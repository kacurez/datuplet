package icebergjob

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
)

// snapshotDataPaths lists the data file paths of tbl's current snapshot.
func snapshotDataPaths(ctx context.Context, tbl *icebergtable.Table) ([]string, error) {
	files, err := currentSnapshotDataFiles(ctx, tbl)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.FilePath()
	}
	return out, nil
}

var partArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

var partIceSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
)

// partTable creates ns1.<name> partitioned by identity(name) (spec field id
// 1000) in fx's catalog, with props.
func partTable(t *testing.T, fx *commitSharedFixture, name string, partitioned bool, props iceberg.Properties) icebergtable.Identifier {
	t.Helper()
	ident := icebergtable.Identifier{"ns1", name}
	opts := []catalog.CreateTableOpt{catalog.WithProperties(props)}
	if partitioned {
		id := iceberg.PartitionDataIDStart
		spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("name", "name", iceberg.IdentityTransform{}, partIceSchema, &id))
		if err != nil {
			t.Fatal(err)
		}
		opts = append(opts, catalog.WithPartitionSpec(&spec))
	}
	if _, err := fx.cat.CreateTable(context.Background(), ident, partIceSchema, opts...); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return ident
}

// writeRows writes rows (id, name) without field ids under the table's
// data/ directory and returns the file:// path.
func writeRows(t *testing.T, fx *commitSharedFixture, ident icebergtable.Identifier, file string, ids []int64, name string) string {
	t.Helper()
	tbl, err := fx.cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatal(err)
	}
	path := strings.TrimRight(tbl.Location(), "/") + "/data/" + file
	abs := strings.TrimPrefix(path, "file://")
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	b := array.NewRecordBuilder(memory.NewGoAllocator(), partArrowSchema)
	defer b.Release()
	for _, id := range ids {
		b.Field(0).(*array.Int64Builder).Append(id)
		b.Field(1).(*array.StringBuilder).Append(name)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	out, err := os.Create(abs)
	if err != nil {
		t.Fatal(err)
	}
	w, err := pqarrow.NewFileWriter(partArrowSchema, out, parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.NewArrowWriterProperties())
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

func tuples(names ...any) *PartitionedFiles {
	part := &PartitionedFiles{}
	for _, n := range names {
		part.Tuples = append(part.Tuples, map[int]any{iceberg.PartitionDataIDStart: n})
	}
	return part
}

// snapshotFiles returns path -> partition value of field 1000 for the
// current snapshot's data files.
func snapshotFiles(t *testing.T, fx *commitSharedFixture, ident icebergtable.Identifier) map[string]any {
	t.Helper()
	tbl, err := fx.cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatal(err)
	}
	files, err := currentSnapshotDataFiles(context.Background(), tbl)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	for _, f := range files {
		out[f.FilePath()] = f.Partition()[iceberg.PartitionDataIDStart]
	}
	return out
}

// conflictCatalog fails the first `fails` table commits with the REST 409.
type conflictCatalog struct {
	catalog.Catalog
	fails   int
	commits int
}

func (c *conflictCatalog) LoadTable(ctx context.Context, ident icebergtable.Identifier) (*icebergtable.Table, error) {
	t, err := c.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	return icebergtable.New(t.Identifier(), t.Metadata(), t.MetadataLocation(), t.FS, c), nil
}

func (c *conflictCatalog) CommitTable(ctx context.Context, ident icebergtable.Identifier, reqs []icebergtable.Requirement, updates []icebergtable.Update) (icebergtable.Metadata, string, error) {
	c.commits++
	if c.fails > 0 {
		c.fails--
		return nil, "", rest.ErrCommitFailed
	}
	return c.Catalog.CommitTable(ctx, ident, reqs, updates)
}

// TestPartitionedCommitIdempotencyAndRetry: a partitioned APPEND survives a
// 409 (retried, files committed with their explicit tuples), and the same
// key again is an idempotency hit.
func TestPartitionedCommitIdempotencyAndRetry(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, nil)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1, 2}, "a")
	b := writeRows(t, fx, ident, "name=b/f-1.parquet", []int64{3}, "b")
	cat := &conflictCatalog{Catalog: fx.cat, fails: 1}

	res, err := CommitTableFiles(context.Background(), cat, ident, []string{a, b}, tuples("a", "b"), WriteModeAppend, nil, "key-1")
	if err != nil {
		t.Fatalf("CommitTableFiles: %v", err)
	}
	if res.IdempotencyHit || res.SnapshotIDAfter == "" || res.DataFilesAdded != 2 || cat.commits != 2 {
		t.Fatalf("result %+v after %d commit attempts, want a retried commit of 2 files", res, cat.commits)
	}
	if got := snapshotFiles(t, fx, ident); len(got) != 2 || got[a] != "a" || got[b] != "b" {
		t.Fatalf("committed tuples = %v", got)
	}

	res2, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{a, b}, tuples("a", "b"), WriteModeAppend, nil, "key-1")
	if err != nil || !res2.IdempotencyHit || res2.SnapshotIDAfter != res.SnapshotIDAfter {
		t.Fatalf("second commit = %+v, %v; want an idempotency hit on %s", res2, err, res.SnapshotIDAfter)
	}
}

// TestPartitionedCommitSpecChanged: a spec change between write and commit
// fails the commit, naming both spec ids.
func TestPartitionedCommitSpecChanged(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, nil)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1}, "a")

	tbl, err := fx.cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatal(err)
	}
	txn := tbl.NewTransaction()
	if err := txn.UpdateSpec(true).AddField("id", iceberg.BucketTransform{NumBuckets: 4}, "id_bucket_4").Commit(); err != nil {
		t.Fatalf("UpdateSpec: %v", err)
	}
	if _, err := txn.Commit(context.Background()); err != nil {
		t.Fatalf("spec commit: %v", err)
	}

	part := tuples("a")
	part.SpecID = 0
	_, err = CommitTableFiles(context.Background(), fx.cat, ident, []string{a}, part, WriteModeAppend, nil, "key-2")
	if err == nil || !strings.Contains(err.Error(), "partition spec changed during the run (spec id 0 → 1)") {
		t.Fatalf("err = %v, want the spec-changed error", err)
	}
	if got := snapshotFiles(t, fx, ident); len(got) != 0 {
		t.Fatalf("files committed despite the spec change: %v", got)
	}
}

// TestPartitionedFullLoadReplaces: FULL_LOAD on a partitioned table leaves
// exactly the new files, with their tuples.
func TestPartitionedFullLoadReplaces(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, nil)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1}, "a")
	b := writeRows(t, fx, ident, "name=b/f-1.parquet", []int64{2}, "b")
	if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{a, b}, tuples("a", "b"), WriteModeFullLoad, nil, "k1"); err != nil {
		t.Fatalf("first FULL_LOAD: %v", err)
	}
	c := writeRows(t, fx, ident, "name=c/f-2.parquet", []int64{3}, "c")
	n := writeRows(t, fx, ident, "name=null/f-3.parquet", []int64{4}, "")
	if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{c, n}, tuples("c", nil), WriteModeFullLoad, nil, "k2"); err != nil {
		t.Fatalf("second FULL_LOAD: %v", err)
	}
	got := snapshotFiles(t, fx, ident)
	if len(got) != 2 || got[c] != "c" || got[n] != nil {
		t.Fatalf("files after FULL_LOAD = %v, want only %s (c) and %s (null)", got, c, n)
	}
}

// TestFullLoadListsDataFilesOnly: FULL_LOAD replaces only data files; a
// position delete file in the current snapshot is not listed (so the commit
// does not try to delete it as a data file), on both commit paths.
func TestFullLoadListsDataFilesOnly(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		name := map[bool]string{false: "unpart", true: "part"}[partitioned]
		t.Run(name, func(t *testing.T) {
			fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
			ident := partTable(t, fx, name, partitioned, iceberg.Properties{
				icebergtable.PropertyFormatVersion: "2",
				icebergtable.WriteDeleteModeKey:    icebergtable.WriteModeMergeOnRead,
			})
			a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1, 2, 3}, "a")
			var part *PartitionedFiles
			if partitioned {
				part = tuples("a")
			}
			if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{a}, part, WriteModeAppend, nil, "k1"); err != nil {
				t.Fatalf("append: %v", err)
			}
			tbl, err := fx.cat.LoadTable(context.Background(), ident)
			if err != nil {
				t.Fatal(err)
			}
			txn := tbl.NewTransaction()
			if err := txn.Delete(context.Background(), iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if tbl, err = txn.Commit(context.Background()); err != nil {
				t.Fatalf("delete commit: %v", err)
			}
			var deletes int
			fs, _ := tbl.FS(context.Background())
			mfs, err := tbl.CurrentSnapshot().Manifests(fs)
			if err != nil {
				t.Fatal(err)
			}
			for _, mf := range mfs {
				entries, err := mf.FetchEntries(fs, true)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range entries {
					if e.DataFile().ContentType() != iceberg.EntryContentData {
						deletes++
					}
				}
			}
			if deletes == 0 {
				t.Fatal("setup: the delete wrote no delete file")
			}
			if got, err := snapshotDataPaths(context.Background(), tbl); err != nil || !slices.Equal(got, []string{a}) {
				t.Fatalf("data files = %v, %v; want only %s", got, err, a)
			}

			c := writeRows(t, fx, ident, "name=c/f-1.parquet", []int64{9}, "c")
			if partitioned {
				part = tuples("c")
			}
			if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{c}, part, WriteModeFullLoad, nil, "k2"); err != nil {
				t.Fatalf("FULL_LOAD: %v", err)
			}
			if got := snapshotFiles(t, fx, ident); len(got) != 1 {
				t.Fatalf("data files after FULL_LOAD = %v, want only %s", got, c)
			}
		})
	}
}

// TestUnpartitionedStillUsesAddFiles: an unpartitioned APPEND commits through
// AddFiles; a partitioned one does not.
func TestUnpartitionedStillUsesAddFiles(t *testing.T) {
	var calls [][]string
	orig := txnAddFiles
	txnAddFiles = func(ctx context.Context, txn *icebergtable.Transaction, paths []string, props iceberg.Properties) error {
		calls = append(calls, paths)
		return orig(ctx, txn, paths, props)
	}
	t.Cleanup(func() { txnAddFiles = orig })

	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	unpart := partTable(t, fx, "unpart", false, nil)
	a := writeRows(t, fx, unpart, "f-0.parquet", []int64{1}, "a")
	if _, err := CommitTableFiles(context.Background(), fx.cat, unpart, []string{a}, nil, WriteModeAppend, nil, "k1"); err != nil {
		t.Fatalf("unpartitioned append: %v", err)
	}
	if len(calls) != 1 || !slices.Equal(calls[0], []string{a}) {
		t.Fatalf("AddFiles calls = %v, want one with %s", calls, a)
	}

	part := partTable(t, fx, "part", true, nil)
	b := writeRows(t, fx, part, "name=b/f-0.parquet", []int64{2}, "b")
	if _, err := CommitTableFiles(context.Background(), fx.cat, part, []string{b}, tuples("b"), WriteModeAppend, nil, "k2"); err != nil {
		t.Fatalf("partitioned append: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("partitioned append went through AddFiles: %v", calls)
	}
}
