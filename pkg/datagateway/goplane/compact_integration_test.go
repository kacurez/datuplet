//go:build integration

package goplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// TestCompactTable_OutputSchemaMatchesTable: files written by the gateway
// writer into a gateway-created (v3) all-types table, compacted by
// icebergjob.CompactTable, match the table's schema in names, Arrow types and
// field ids, but also carry the two v3 row-lineage columns iceberg-go
// preserves, so the raw-eligibility check refuses them. Lives here because
// goplane imports icebergjob.
func TestCompactTable_OutputSchemaMatchesTable(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch, inputs := allTypesSchema(t)
	const ns, tbl = "gw_it", "compact_raw"
	target := createTable(t, r, ns, tbl, sch)
	for i := 1; i <= 6; i++ {
		files := writeRun(t, p, target, fmt.Sprintf("part-run%05d-w1", i), sch, inputs, 1, target.VendedCreds, 0)
		commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, fmt.Sprintf("key-run%d", i))
	}
	rowsBefore, _ := scanRows(t, r, ns, tbl)

	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ident := catalog.ToIdentifier(ns, tbl)
	res, err := icebergjob.CompactTable(ctx, cat, ident, icebergjob.CompactOpts{}, nil,
		icebergjob.CompactOperationKey("run-1", "compact", ns, tbl))
	if err != nil || res.Status != icebergjob.CompactCompacted || res.FilesAdded == 0 {
		t.Fatalf("CompactTable = %+v, %v", res, err)
	}
	if n, _ := scanRows(t, r, ns, tbl); n != rowsBefore {
		t.Fatalf("rows %d, want %d", n, rowsBefore)
	}

	tb, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	tableArrow, err := table.SchemaToArrowSchema(tb.Schema(), nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	fsys, err := tb.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := tb.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != res.FilesAdded {
		t.Fatalf("%d live files, want the %d compacted ones", len(tasks), res.FilesAdded)
	}
	for _, task := range tasks {
		if !wholeFileTask(task) || hasDeletes(task) {
			t.Fatalf("task %s not whole-file / has deletes", task.File.FilePath())
		}
		f, err := fsys.Open(task.File.FilePath())
		if err != nil {
			t.Fatal(err)
		}
		pr, err := file.NewParquetReader(f)
		if err != nil {
			t.Fatal(err)
		}
		md := pr.MetaData()
		fileSchema, err := pqarrow.FromParquet(md.Schema, &pqarrow.ArrowReadProperties{}, md.KeyValueMetadata())
		_ = pr.Close()
		if err != nil {
			t.Fatal(err)
		}
		if fileMatchesTable(fileSchema, tableArrow, tb.Schema(), tb.NameMapping()) {
			t.Fatalf("compacted file %s is raw-eligible; row lineage no longer materialized?", task.File.FilePath())
		}
		data, lineage := splitLineage(fileSchema)
		if lineage != 2 || !fileMatchesTable(data, tableArrow, tb.Schema(), tb.NameMapping()) {
			t.Fatalf("compacted file %s (%d lineage columns) does not match the table:\nfile:  %v\ntable: %v", task.File.FilePath(), lineage, fileSchema, tableArrow)
		}
	}
}

// splitLineage drops the v3 row-lineage columns from s and counts them.
func splitLineage(s *arrow.Schema) (*arrow.Schema, int) {
	var keep []arrow.Field
	n := 0
	for _, f := range s.Fields() {
		if f.Name == "_row_id" || f.Name == "_last_updated_sequence_number" {
			n++
			continue
		}
		keep = append(keep, f)
	}
	md := s.Metadata()
	return arrow.NewSchema(keep, &md), n
}
