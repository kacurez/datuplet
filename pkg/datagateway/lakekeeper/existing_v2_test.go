package lakekeeper

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icebergtable "github.com/apache/iceberg-go/table"

	_ "modernc.org/sqlite"

	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// writeIDFile writes ids as a one-column (id) Parquet file without field ids
// under tb's data/ directory and returns its file:// path.
func writeIDFile(t *testing.T, tb *icebergtable.Table, name string, ids ...int64) string {
	t.Helper()
	path := strings.TrimRight(tb.Location(), "/") + "/data/" + name
	abs := strings.TrimPrefix(path, "file://")
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	sch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), sch)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()
	out, err := os.Create(abs)
	if err != nil {
		t.Fatal(err)
	}
	w, err := pqarrow.NewFileWriter(sch, out, parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.NewArrowWriterProperties())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// An existing v2 table, partitioned or not, is written and read as v2: the
// resolve keeps its version, a commit leaves it v2 without row ids, and a
// scan returns every row.
func TestExistingV2TableStaysV2(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		name := map[bool]string{false: "flat", true: "part"}[partitioned]
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "catalog.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			cat, err := sqlcat.NewCatalog("unit", db, sqlcat.SQLite, iceberg.Properties{"warehouse": "file://" + dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := cat.CreateNamespace(ctx, catalog.ToIdentifier("ns"), nil); err != nil {
				t.Fatal(err)
			}
			sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
			opts := []catalog.CreateTableOpt{catalog.WithProperties(iceberg.Properties{icebergtable.PropertyFormatVersion: "2"})}
			if partitioned {
				spec := idPartitionSpec(t, sc)
				opts = append(opts, catalog.WithPartitionSpec(&spec))
			}
			ident := catalog.ToIdentifier("ns", name)
			if _, err := cat.CreateTable(ctx, ident, sc, opts...); err != nil {
				t.Fatal(err)
			}

			tb, _, err := LoadOrCreateTable(ctx, cat, "ns", name, idSchemaProvider(t), nil)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if v := tb.Metadata().Version(); v != 2 {
				t.Fatalf("resolved version %d, want 2", v)
			}
			var part *icebergjob.PartitionedFiles
			a := writeIDFile(t, tb, "id=1/a.parquet", 1)
			b := writeIDFile(t, tb, "id=2/b.parquet", 2)
			if partitioned {
				part = &icebergjob.PartitionedFiles{Tuples: []map[int]any{{iceberg.PartitionDataIDStart: int64(1)}, {iceberg.PartitionDataIDStart: int64(2)}}}
			}
			if _, err := icebergjob.CommitTableFiles(ctx, cat, ident, []string{a, b}, part, icebergjob.WriteModeAppend, nil, "k1"); err != nil {
				t.Fatalf("commit: %v", err)
			}

			tb, err = cat.LoadTable(ctx, ident)
			if err != nil {
				t.Fatal(err)
			}
			if v := tb.Metadata().Version(); v != 2 {
				t.Fatalf("version after commit %d, want 2", v)
			}
			if s := tb.CurrentSnapshot(); s == nil || s.FirstRowID != nil {
				t.Fatalf("snapshot %+v, want one without first-row-id", s)
			}
			tasks, err := tb.Scan().PlanFiles(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, task := range tasks {
				if task.File.FirstRowID() != nil {
					t.Errorf("%s has first_row_id %d on a v2 table", task.File.FilePath(), *task.File.FirstRowID())
				}
			}
			arrTbl, err := tb.Scan().ToArrowTable(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer arrTbl.Release()
			var ids []int64
			for _, ch := range arrTbl.Column(0).Data().Chunks() {
				ids = append(ids, ch.(*array.Int64).Int64Values()...)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
				t.Fatalf("read ids %v, want [1 2]", ids)
			}
		})
	}
}
