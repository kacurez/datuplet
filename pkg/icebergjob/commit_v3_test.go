package icebergjob

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
)

var v3Props = iceberg.Properties{icebergtable.PropertyFormatVersion: "3", icebergtable.CommitNumRetriesKey: "0"}

// countAssigns counts assignFirstRowIDs calls for the test's duration.
func countAssigns(t *testing.T) *int {
	t.Helper()
	n := new(int)
	orig := assignFirstRowIDs
	assignFirstRowIDs = func(b []*iceberg.DataFileBuilder, f []iceberg.DataFile, next int64) {
		*n++
		orig(b, f, next)
	}
	t.Cleanup(func() { assignFirstRowIDs = orig })
	return n
}

// rowIDs returns path -> first_row_id (-1 when unset) of the current
// snapshot's data files, and the loaded table.
func rowIDs(t *testing.T, fx *commitSharedFixture, ident icebergtable.Identifier) (map[string]int64, *icebergtable.Table) {
	t.Helper()
	tbl, err := fx.cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatal(err)
	}
	files, err := currentSnapshotDataFiles(context.Background(), tbl)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, f := range files {
		out[f.FilePath()] = -1
		if id := f.FirstRowID(); id != nil {
			out[f.FilePath()] = *id
		}
	}
	return out, tbl
}

// checkLineage checks the current snapshot's first-row-id and added-rows and
// the table's next-row-id.
func checkLineage(t *testing.T, tbl *icebergtable.Table, first, added, next int64) {
	t.Helper()
	s := tbl.CurrentSnapshot()
	if s == nil || s.FirstRowID == nil || s.AddedRows == nil || *s.FirstRowID != first || *s.AddedRows != added {
		t.Fatalf("snapshot lineage %+v, want first-row-id %d added-rows %d", s, first, added)
	}
	if got := tbl.Metadata().NextRowID(); got != next {
		t.Fatalf("next-row-id %d, want %d", got, next)
	}
}

// raceCatalog runs race once inside the first table commit and then fails
// that commit with the REST 409.
type raceCatalog struct {
	catalog.Catalog
	race    func()
	commits int
	loads   int
}

func (c *raceCatalog) LoadTable(ctx context.Context, ident icebergtable.Identifier) (*icebergtable.Table, error) {
	c.loads++
	t, err := c.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	return icebergtable.New(t.Identifier(), t.Metadata(), t.MetadataLocation(), t.FS, c), nil
}

func (c *raceCatalog) CommitTable(ctx context.Context, ident icebergtable.Identifier, reqs []icebergtable.Requirement, updates []icebergtable.Update) (icebergtable.Metadata, string, error) {
	c.commits++
	if r := c.race; r != nil {
		c.race = nil
		r()
		return nil, "", rest.ErrCommitFailed
	}
	return c.Catalog.CommitTable(ctx, ident, reqs, updates)
}

func setTableProps(t *testing.T, cat catalog.Catalog, ident icebergtable.Identifier, props iceberg.Properties) {
	t.Helper()
	tbl, err := cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatal(err)
	}
	txn := tbl.NewTransaction()
	if err := txn.SetProperties(props); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(context.Background()); err != nil {
		t.Fatalf("set properties: %v", err)
	}
}

// TestPartitionedV3AppendAssignsRowIDs: two partitioned APPENDs to a v3 table
// give each file first_row_id = next-row-id + the rows of the files before it
// (path order), so the ranges are contiguous and never overlap, and the
// snapshots' lineage and next-row-id match.
func TestPartitionedV3AppendAssignsRowIDs(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, v3Props)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1, 2}, "a")
	b := writeRows(t, fx, ident, "name=b/f-1.parquet", []int64{3, 4, 5}, "b")
	if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{a, b}, tuples("a", "b"), WriteModeAppend, nil, "k1"); err != nil {
		t.Fatalf("first APPEND: %v", err)
	}
	ids, tbl := rowIDs(t, fx, ident)
	if ids[a] != 0 || ids[b] != 2 {
		t.Fatalf("first APPEND row ids %v, want %s=0 %s=2", ids, a, b)
	}
	checkLineage(t, tbl, 0, 5, 5)

	c := writeRows(t, fx, ident, "name=c/f-2.parquet", []int64{6}, "c")
	d := writeRows(t, fx, ident, "name=a/f-3.parquet", []int64{7, 8}, "a")
	if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{c, d}, tuples("c", "a"), WriteModeAppend, nil, "k2"); err != nil {
		t.Fatalf("second APPEND: %v", err)
	}
	ids, tbl = rowIDs(t, fx, ident)
	if ids[a] != 0 || ids[b] != 2 || ids[c] != 5 || ids[d] != 6 {
		t.Fatalf("row ids after both APPENDs %v, want a=0 b=2 c=5 d=6", ids)
	}
	checkLineage(t, tbl, 5, 3, 8)
}

// TestPartitionedV3RetryReassignsRowIDs: a concurrent commit lands and the
// first attempt gets a 409; the retry assigns ids from the fresh table's
// next-row-id, after the concurrent commit's rows.
func TestPartitionedV3RetryReassignsRowIDs(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, v3Props)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1, 2}, "a")
	b := writeRows(t, fx, ident, "name=b/f-1.parquet", []int64{3}, "b")
	c := writeRows(t, fx, ident, "name=c/f-2.parquet", []int64{4, 5, 6}, "c")
	cat := &raceCatalog{Catalog: fx.cat, race: func() {
		if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{c}, tuples("c"), WriteModeAppend, nil, "other"); err != nil {
			t.Errorf("concurrent APPEND: %v", err)
		}
	}}
	assigns := countAssigns(t)
	if _, err := CommitTableFiles(context.Background(), cat, ident, []string{a, b}, tuples("a", "b"), WriteModeAppend, nil, "k1"); err != nil {
		t.Fatalf("CommitTableFiles: %v", err)
	}
	if cat.commits != 2 {
		t.Fatalf("%d commit attempts, want 2", cat.commits)
	}
	if *assigns != 3 { // two attempts + the concurrent commit
		t.Fatalf("%d row-id assignments, want 3", *assigns)
	}
	ids, tbl := rowIDs(t, fx, ident)
	if ids[c] != 0 || ids[a] != 3 || ids[b] != 5 {
		t.Fatalf("row ids %v, want c=0 a=3 b=5", ids)
	}
	checkLineage(t, tbl, 3, 3, 6)
}

// TestPartitionedV3FullLoad: a partitioned FULL_LOAD to a v3 table assigns
// ids from next-row-id, leaves only the new files, and advances next-row-id.
func TestPartitionedV3FullLoad(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, v3Props)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1, 2}, "a")
	b := writeRows(t, fx, ident, "name=b/f-1.parquet", []int64{3}, "b")
	if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{a, b}, tuples("a", "b"), WriteModeFullLoad, nil, "k1"); err != nil {
		t.Fatalf("first FULL_LOAD: %v", err)
	}
	c := writeRows(t, fx, ident, "name=c/f-2.parquet", []int64{4, 5, 6}, "c")
	n := writeRows(t, fx, ident, "name=null/f-3.parquet", []int64{7}, "")
	if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{c, n}, tuples("c", nil), WriteModeFullLoad, nil, "k2"); err != nil {
		t.Fatalf("second FULL_LOAD: %v", err)
	}
	ids, tbl := rowIDs(t, fx, ident)
	if len(ids) != 2 || ids[c] != 3 || ids[n] != 6 {
		t.Fatalf("files after FULL_LOAD %v, want only c=3 and null=6", ids)
	}
	if op := tbl.CurrentSnapshot().Summary.Operation; op != icebergtable.OpOverwrite {
		t.Fatalf("FULL_LOAD operation %s, want overwrite", op)
	}
	checkLineage(t, tbl, 3, 4, 7)
}

// TestPartitionedV3IdempotencyHitAssignsNothing: a repeated key is an
// idempotency hit before any row id is assigned or anything is committed.
func TestPartitionedV3IdempotencyHitAssignsNothing(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, v3Props)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1, 2}, "a")
	if _, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{a}, tuples("a"), WriteModeAppend, nil, "k1"); err != nil {
		t.Fatal(err)
	}
	assigns := countAssigns(t)
	res, err := CommitTableFiles(context.Background(), fx.cat, ident, []string{a}, tuples("a"), WriteModeAppend, nil, "k1")
	if err != nil || !res.IdempotencyHit {
		t.Fatalf("repeat = %+v, %v; want an idempotency hit", res, err)
	}
	if *assigns != 0 {
		t.Fatalf("%d row-id assignments on an idempotency hit, want 0", *assigns)
	}
	_, tbl := rowIDs(t, fx, ident)
	if n := len(tbl.Metadata().Snapshots()); n != 1 {
		t.Fatalf("%d snapshots, want 1", n)
	}
	checkLineage(t, tbl, 0, 2, 2)
}

// TestPartitionedV3RetryPropertyChangedBeforeCommit: commit.retry.num-retries
// set above 0 after the writer resolved fails the commit on its first attempt,
// not retried, with no row ids assigned and nothing committed. An unpartitioned
// v3 table (manifest-inherited row ids) still commits.
func TestPartitionedV3RetryPropertyChangedBeforeCommit(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, v3Props)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1}, "a")
	setTableProps(t, fx.cat, ident, iceberg.Properties{icebergtable.CommitNumRetriesKey: "2"})

	cat := &raceCatalog{Catalog: fx.cat}
	assigns := countAssigns(t)
	_, err := CommitTableFiles(context.Background(), cat, ident, []string{a}, tuples("a"), WriteModeAppend, nil, "k1")
	if err == nil || !strings.Contains(err.Error(), "commit.retry.num-retries=2") || errors.Is(err, rest.ErrCommitFailed) {
		t.Fatalf("err = %v, want a non-retryable error naming commit.retry.num-retries=2", err)
	}
	if cat.commits != 0 || cat.loads != 2 || *assigns != 0 {
		t.Fatalf("commits %d loads %d assigns %d; want 0, 2 (one attempt), 0", cat.commits, cat.loads, *assigns)
	}
	if ids, tbl := rowIDs(t, fx, ident); len(ids) != 0 || tbl.Metadata().NextRowID() != 0 {
		t.Fatalf("committed %v (next-row-id %d) despite the property", ids, tbl.Metadata().NextRowID())
	}

	flat := partTable(t, fx, "flat", false, iceberg.Properties{icebergtable.PropertyFormatVersion: "3", icebergtable.CommitNumRetriesKey: "2"})
	f := writeRows(t, fx, flat, "f-0.parquet", []int64{1, 2}, "a")
	if _, err := CommitTableFiles(context.Background(), fx.cat, flat, []string{f}, nil, WriteModeAppend, nil, "k2"); err != nil {
		t.Fatalf("unpartitioned v3 append: %v", err)
	}
	_, tbl := rowIDs(t, fx, flat)
	checkLineage(t, tbl, 0, 2, 2)
}

// TestPartitionedV3RetryPropertyChangedBetweenAttempts: the property is set
// above 0 while the first attempt gets a 409; the next attempt fails on the
// fresh table without committing.
func TestPartitionedV3RetryPropertyChangedBetweenAttempts(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, v3Props)
	a := writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1}, "a")
	cat := &raceCatalog{Catalog: fx.cat, race: func() {
		setTableProps(t, fx.cat, ident, iceberg.Properties{icebergtable.CommitNumRetriesKey: "1"})
	}}
	_, err := CommitTableFiles(context.Background(), cat, ident, []string{a}, tuples("a"), WriteModeAppend, nil, "k1")
	if err == nil || !strings.Contains(err.Error(), "commit.retry.num-retries=1") {
		t.Fatalf("err = %v, want the num-retries error", err)
	}
	if cat.commits != 1 {
		t.Fatalf("%d commit attempts, want 1", cat.commits)
	}
	if ids, _ := rowIDs(t, fx, ident); len(ids) != 0 {
		t.Fatalf("committed %v", ids)
	}
}

// TestPartitionedV2Unchanged: a partitioned v2 table commits without row ids,
// whatever its commit.retry.num-retries, and stays v2.
func TestPartitionedV2Unchanged(t *testing.T) {
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "base"})
	ident := partTable(t, fx, "part", true, iceberg.Properties{icebergtable.PropertyFormatVersion: "2", icebergtable.CommitNumRetriesKey: "3"})
	files := []string{
		writeRows(t, fx, ident, "name=a/f-0.parquet", []int64{1, 2}, "a"),
		writeRows(t, fx, ident, "name=a/f-1.parquet", []int64{3}, "a"),
	}
	assigns := countAssigns(t)
	for i, mode := range []WriteMode{WriteModeAppend, WriteModeFullLoad} {
		if _, err := CommitTableFiles(context.Background(), fx.cat, ident, files[i:i+1], tuples("a"), mode, nil, string(mode)); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		ids, tbl := rowIDs(t, fx, ident)
		if tbl.Metadata().Version() != 2 || len(ids) != 1 || ids[files[i]] != -1 || tbl.CurrentSnapshot().FirstRowID != nil || len(tbl.Metadata().Snapshots()) != i+1 {
			t.Fatalf("%s: version %d, row ids %v, snapshot %+v", mode, tbl.Metadata().Version(), ids, tbl.CurrentSnapshot())
		}
	}
	if *assigns != 0 {
		t.Fatalf("%d row-id assignments on v2, want 0", *assigns)
	}
}
