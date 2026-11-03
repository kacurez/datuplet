//go:build integration

package goplane

import (
	"cmp"
	"context"
	"errors"
	"io"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// checkV3Lineage checks that tb is a gateway-created v3 table and that its
// snapshots, oldest first, carry first-row-id / added-rows for rows, with
// next-row-id at their sum.
func checkV3Lineage(t *testing.T, tb *table.Table, rows ...int64) {
	t.Helper()
	if v := tb.Metadata().Version(); v != 3 {
		t.Fatalf("format version %d, want 3", v)
	}
	if got := tb.Properties()[table.CommitNumRetriesKey]; got != "0" {
		t.Fatalf("%s = %q, want \"0\"", table.CommitNumRetriesKey, got)
	}
	snaps := slices.Clone(tb.Metadata().Snapshots()) // catalog order is not commit order
	slices.SortFunc(snaps, func(a, b table.Snapshot) int { return cmp.Compare(a.SequenceNumber, b.SequenceNumber) })
	if len(snaps) != len(rows) {
		t.Fatalf("%d snapshots, want %d", len(snaps), len(rows))
	}
	var next int64
	for i, s := range snaps {
		if s.FirstRowID == nil || s.AddedRows == nil || *s.FirstRowID != next || *s.AddedRows != rows[i] {
			t.Fatalf("snapshot %d: first-row-id %s added-rows %s, want %d and %d", i, fmtPtr(s.FirstRowID), fmtPtr(s.AddedRows), next, rows[i])
		}
		next += rows[i]
	}
	if got := tb.Metadata().NextRowID(); got != next {
		t.Fatalf("next-row-id %d, want %d", got, next)
	}
}

// TestIntegrationServerWriteV3Unpartitioned: a real server creates an
// unpartitioned table as v3 and commits it twice through AddFiles; each
// snapshot carries first-row-id and added-rows, next-row-id advances, and
// the gateway reads every row back.
func TestIntegrationServerWriteV3Unpartitioned(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_v3", "flat"
	srv := startPartitionServer(t, fx, "0123456789abcdef-server-v3flat", ns, tbl, nil)
	serverAppend(t, srv, ns, tbl, []partitionEvent{{0, "a", 0}, {1, "b", 0}, {2, "", 1}})
	serverAppend(t, srv, ns, tbl, []partitionEvent{{3, "a", 0}, {4, "c", 2}})

	tb := loadLK(t, resolver(t, fx), ns, tbl)
	if spec := tb.Spec(); spec.NumFields() != 0 {
		t.Fatalf("spec %v, want unpartitioned", spec)
	}
	checkV3Lineage(t, tb, 3, 2)
	read := serverReadIDs(t, srv, ns, tbl)
	for id := int64(0); id <= 4; id++ {
		if read[id] != 1 {
			t.Fatalf("gateway read: id %d read %d times, want once (%v)", id, read[id], read)
		}
	}
}

// TestIntegrationServerPartitionedWriteV3: a real server creates an
// identity(name) table as v3 and commits it twice; every data file carries
// an explicit first_row_id, the ranges are contiguous and never overlap,
// snapshot lineage and next-row-id match, and the gateway reads every row.
func TestIntegrationServerPartitionedWriteV3(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_v3", "part"
	srv := startPartitionServer(t, fx, "0123456789abcdef-server-v3part", ns, tbl, []datagateway.PartitionFieldConfig{
		{SourceColumn: "name", Transform: "identity"},
	})
	if files := serverAppend(t, srv, ns, tbl, []partitionEvent{{0, "a", 0}, {1, "b", 0}, {2, "a", 1}, {3, "", 0}}); files != 3 {
		t.Fatalf("first run wrote %d files, want 3", files)
	}
	serverAppend(t, srv, ns, tbl, []partitionEvent{{4, "b", 0}, {5, "c", 0}})

	tb := loadLK(t, resolver(t, fx), ns, tbl)
	if spec := tb.Spec(); spec.NumFields() != 1 {
		t.Fatalf("spec %v, want identity(name)", spec)
	}
	checkV3Lineage(t, tb, 4, 2)

	tasks, err := tb.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	type rng struct{ first, count int64 }
	var ranges []rng
	for _, task := range tasks {
		id := task.File.FirstRowID()
		if id == nil {
			t.Fatalf("data file %s has no first_row_id", task.File.FilePath())
		}
		ranges = append(ranges, rng{*id, task.File.Count()})
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].first < ranges[j].first })
	var next int64
	for _, r := range ranges {
		if r.first != next {
			t.Fatalf("row-id ranges %v are not contiguous from 0", ranges)
		}
		next += r.count
	}
	if next != tb.Metadata().NextRowID() {
		t.Fatalf("row-id ranges %v end at %d, next-row-id %d", ranges, next, tb.Metadata().NextRowID())
	}

	read := serverReadIDs(t, srv, ns, tbl)
	for id := int64(0); id <= 5; id++ {
		if read[id] != 1 {
			t.Fatalf("gateway read: id %d read %d times, want once (%v)", id, read[id], read)
		}
	}
}

// TestIntegrationV3DeletionVectorKeepsOtherFilesRaw: on a gateway-created v3
// table of 5 files, an iceberg-go merge-on-read delete in one file writes a
// deletion vector attached to that file's task only, so the read serves the
// other 4 files raw and decodes 1, with the deleted rows gone.
func TestIntegrationV3DeletionVectorKeepsOtherFilesRaw(t *testing.T) {
	fx := sharedFixture(t)
	r := resolver(t, fx)
	p := New(Config{Tables: ResolverTables{Resolver: r}})
	defer p.Close()
	const ns, tbl = "gw_v3", "dv"
	target := createTable(t, r, ns, tbl, testSchema)
	if target.FormatVersion != 3 {
		t.Fatalf("target format version %d, want 3", target.FormatVersion)
	}
	txn := loadLK(t, r, ns, tbl).NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead}); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(context.Background()); err != nil {
		t.Fatalf("set properties: %v", err)
	}
	var files []datagateway.WrittenFile
	for i := 0; i < 5; i++ {
		files = append(files, writeIDs(t, p, target, "part-v3dv0001-w"+string(rune('1'+i)), testSchema, i*100, 100))
	}
	commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, "key-v3dv")
	lkDelete(t, r, ns, tbl, iceberg.NewAnd(
		iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(5)),
		iceberg.LessThan(iceberg.Reference("id"), int64(10))))

	tasks, err := loadLK(t, r, ns, tbl).Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var withDeletes []table.FileScanTask
	for _, task := range tasks {
		if hasDeletes(task) {
			withDeletes = append(withDeletes, task)
		}
	}
	if len(tasks) != 5 || len(withDeletes) != 1 || withDeletes[0].File.FilePath() != files[0].Path ||
		len(withDeletes[0].DeletionVectorFiles) != 1 || len(withDeletes[0].DeleteFiles) != 0 {
		t.Fatalf("%d tasks, %d with deletes (%+v); want 5 tasks and one deletion vector on %s", len(tasks), len(withDeletes), withDeletes, files[0].Path)
	}

	tr := openLKReader(t, p, ns, tbl, true)
	ctx := context.Background()
	var rawFiles, decodedFiles int
	var ids []int64
	for {
		part, err := tr.NextPart(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextPart: %v", err)
		}
		if part.Raw != nil {
			rawFiles++
			rc, err := part.Raw.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			_, recs := readParquetBytes(t, b)
			ids = append(ids, summarize(t, "raw", recs).ids...)
			continue
		}
		decodedFiles++
		var recs []arrow.RecordBatch
		for {
			rec, err := part.Records.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			recs = append(recs, rec)
		}
		_ = part.Records.Close()
		ids = append(ids, summarize(t, "decoded", recs).ids...)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if rawFiles != 4 || decodedFiles != 1 || !equalIDs(ids, idRange(0, 500, 5, 6, 7, 8, 9)) {
		t.Fatalf("raw_files=%d decoded_files=%d, %d ids; want raw_files=4 decoded_files=1 and 495 ids", rawFiles, decodedFiles, len(ids))
	}
	t.Logf("read: raw_files=%d decoded_files=%d rows=%d", rawFiles, decodedFiles, len(ids))
}

func fmtPtr(p *int64) string {
	if p == nil {
		return "<nil>"
	}
	return strconv.FormatInt(*p, 10)
}
