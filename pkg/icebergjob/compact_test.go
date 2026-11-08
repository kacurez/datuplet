//go:build integration

package icebergjob

// CompactTable against the Lakekeeper + silo fixture (lakekeeper_fixture_test.go).
//
//	go test -count=1 -tags=integration ./pkg/icebergjob/ -run TestCompactTable -v -timeout 20m

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/url"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/minio/minio-go/v7"
)

const compactNS = "cmp"

// cmpEnv is one test's table in the shared fixture plus the plain catalog
// used for injected commits.
type cmpEnv struct {
	t     *testing.T
	fx    *fixture
	cat   catalog.Catalog
	ident icebergtable.Identifier
}

func newCmpEnv(t *testing.T, name string, partitioned bool, props iceberg.Properties) *cmpEnv {
	t.Helper()
	fx := sharedLakekeeper(t)
	cat := lkCatalog(t, fx)
	ctx := context.Background()
	if err := cat.CreateNamespace(ctx, icebergtable.Identifier{compactNS}, nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		t.Fatal(err)
	}
	all := iceberg.Properties{icebergtable.CommitNumRetriesKey: "0"}
	for k, v := range props {
		all[k] = v
	}
	opts := []catalog.CreateTableOpt{catalog.WithProperties(all)}
	if partitioned {
		id := iceberg.PartitionDataIDStart
		spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("name", "name", iceberg.IdentityTransform{}, partIceSchema, &id))
		if err != nil {
			t.Fatal(err)
		}
		opts = append(opts, catalog.WithPartitionSpec(&spec))
	}
	ident := icebergtable.Identifier{compactNS, name}
	if _, err := cat.CreateTable(ctx, ident, partIceSchema, opts...); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return &cmpEnv{t: t, fx: fx, cat: cat, ident: ident}
}

func (e *cmpEnv) load() *icebergtable.Table {
	e.t.Helper()
	tbl, err := e.cat.LoadTable(context.Background(), e.ident)
	if err != nil {
		e.t.Fatal(err)
	}
	return tbl
}

func idsRecords(ids []int64, name string) array.RecordReader {
	b := array.NewRecordBuilder(memory.DefaultAllocator, partArrowSchema)
	defer b.Release()
	for _, id := range ids {
		b.Field(0).(*array.Int64Builder).Append(id)
		b.Field(1).(*array.StringBuilder).Append(name)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, _ := array.NewRecordReader(partArrowSchema, []arrow.RecordBatch{rec})
	return rdr
}

// appendRows commits one APPEND of rows (id, name) through iceberg-go's writer.
func (e *cmpEnv) appendRows(ids []int64, name string) {
	e.t.Helper()
	if _, err := e.load().Append(context.Background(), idsRecords(ids, name), nil); err != nil {
		e.t.Fatalf("append: %v", err)
	}
}

// writeFile writes rows (id, name) as a Parquet file without field ids under
// the table's data/ prefix and returns its path (an AddFiles input).
func (e *cmpEnv) writeFile(file string, ids []int64, name string) string {
	e.t.Helper()
	tbl := e.load()
	fsys, err := tbl.FS(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	path := strings.TrimRight(tbl.Location(), "/") + "/data/" + file
	f, err := fsys.(iceio.WriteFileIO).Create(path)
	if err != nil {
		e.t.Fatal(err)
	}
	w, err := pqarrow.NewFileWriter(partArrowSchema, f, parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps())
	if err != nil {
		e.t.Fatal(err)
	}
	rdr := idsRecords(ids, name)
	defer rdr.Release()
	for rdr.Next() {
		if err := w.Write(rdr.RecordBatch()); err != nil {
			e.t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		e.t.Fatal(err)
	}
	return path
}

// addFiles commits an APPEND of pre-written files via the plain catalog.
func (e *cmpEnv) addFiles(paths ...string) {
	e.t.Helper()
	txn := e.load().NewTransaction()
	if err := txn.AddFiles(context.Background(), paths, nil, false); err != nil {
		e.t.Fatalf("AddFiles: %v", err)
	}
	if _, err := txn.Commit(context.Background()); err != nil {
		e.t.Fatalf("AddFiles commit: %v", err)
	}
}

func (e *cmpEnv) liveFiles() []iceberg.DataFile {
	e.t.Helper()
	files, err := currentSnapshotDataFiles(context.Background(), e.load())
	if err != nil {
		e.t.Fatal(err)
	}
	return files
}

func (e *cmpEnv) livePaths() map[string]bool {
	out := map[string]bool{}
	for _, f := range e.liveFiles() {
		out[f.FilePath()] = true
	}
	return out
}

// rows scans the current snapshot (deletes applied) and returns its ids.
func (e *cmpEnv) rows() map[int64]bool {
	e.t.Helper()
	tbl, err := e.load().Scan().ToArrowTable(context.Background())
	if err != nil {
		e.t.Fatalf("scan: %v", err)
	}
	defer tbl.Release()
	out := map[int64]bool{}
	col := tbl.Column(0)
	for _, ch := range col.Data().Chunks() {
		a := ch.(*array.Int64)
		for i := 0; i < a.Len(); i++ {
			out[a.Value(i)] = true
		}
	}
	if int64(len(out)) != tbl.NumRows() {
		e.t.Fatalf("duplicate ids: %d distinct of %d rows", len(out), tbl.NumRows())
	}
	return out
}

// referenced returns the data and delete files the current snapshot's scan uses.
func (e *cmpEnv) referenced() map[string]bool {
	e.t.Helper()
	tasks, err := e.load().Scan().PlanFiles(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[string]bool{}
	for _, task := range tasks {
		out[task.File.FilePath()] = true
		for _, d := range task.DeleteFiles {
			out[d.FilePath()] = true
		}
	}
	return out
}

func unreferenced(paths []string, ref map[string]bool) []string {
	var out []string
	for _, p := range paths {
		if !ref[p] {
			out = append(out, p)
		}
	}
	return out
}

func (e *cmpEnv) snapshotID() int64 {
	e.t.Helper()
	s := e.load().CurrentSnapshot()
	if s == nil {
		return 0
	}
	return s.SnapshotID
}

// dataObjects lists every object under the table's data/ prefix.
func (e *cmpEnv) dataObjects() map[string]bool {
	e.t.Helper()
	u, err := url.Parse(strings.TrimRight(e.load().Location(), "/") + "/data/")
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[string]bool{}
	for obj := range e.fx.siloCore(e.t).Client.ListObjects(context.Background(), u.Host, minio.ListObjectsOptions{Prefix: strings.TrimPrefix(u.Path, "/"), Recursive: true}) {
		if obj.Err != nil {
			e.t.Fatal(obj.Err)
		}
		out[u.Scheme+"://"+u.Host+"/"+obj.Key] = true
	}
	return out
}

func (e *cmpEnv) replaceSnapshots() int {
	n := 0
	for _, s := range e.load().Metadata().Snapshots() {
		if s.Summary != nil && s.Summary.Operation == icebergtable.OpReplace {
			n++
		}
	}
	return n
}

// seedPartitioned appends 12 files: 6 to partition a (2 rows each), 6 to b.
func (e *cmpEnv) seedPartitioned() {
	for i := int64(0); i < 6; i++ {
		e.appendRows([]int64{i * 10, i*10 + 1}, "a")
		e.appendRows([]int64{i*10 + 5, i*10 + 6}, "b")
	}
}

// seed appends n two-row files to the unpartitioned table.
func (e *cmpEnv) seed(n int) {
	for i := int64(0); i < int64(n); i++ {
		e.appendRows([]int64{i * 10, i*10 + 1}, "x")
	}
}

// skipTarget is the target size under which seedSkipped's file is right-sized
// (>= 75 %) and seed's files are candidates.
const skipTarget = 16 << 10

// seedSkipped appends one file of 2000 random ids that the planner skips at
// skipTarget, so a concurrent change to it removes no compaction input.
func (e *cmpEnv) seedSkipped() string {
	e.t.Helper()
	before := e.livePaths()
	rng := rand.New(rand.NewPCG(3, 4))
	ids := make([]int64, 2000)
	for i := range ids {
		ids[i] = 1_000_000 + rng.Int64N(1<<62)
	}
	e.appendRows(ids, "x")
	for _, f := range e.liveFiles() {
		if !before[f.FilePath()] {
			if sz := f.FileSizeBytes(); sz < skipTarget*3/4 || sz > skipTarget*9/5 {
				e.t.Fatalf("precondition: skipped file is %d bytes", sz)
			}
			return f.FilePath()
		}
	}
	e.t.Fatal("seedSkipped: no new file")
	return ""
}

// newObjects returns objects under data/ not in before.
func newObjects(before, after map[string]bool) []string {
	var out []string
	for k := range after {
		if !before[k] {
			out = append(out, k)
		}
	}
	return out
}

// hookCatalog wraps the fixture catalog: onLoad runs before every LoadTable
// (1-based count), wrapFS wraps the IO of the returned table, and commit,
// when set, replaces CommitTable (call next to reach the real catalog).
type hookCatalog struct {
	catalog.Catalog
	loads   int
	commits int
	onLoad  func(n int)
	wrapFS  func(n int, fs iceio.IO) iceio.IO
	commit  func(n int, next func() (icebergtable.Metadata, string, error)) (icebergtable.Metadata, string, error)
}

func (c *hookCatalog) LoadTable(ctx context.Context, ident icebergtable.Identifier) (*icebergtable.Table, error) {
	c.loads++
	n := c.loads
	if c.onLoad != nil {
		c.onLoad(n)
	}
	t, err := c.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	fsF := t.FS
	if c.wrapFS != nil {
		fsF = func(ctx context.Context) (iceio.IO, error) {
			fs, err := t.FS(ctx)
			if err != nil {
				return nil, err
			}
			return c.wrapFS(n, fs), nil
		}
	}
	return icebergtable.New(t.Identifier(), t.Metadata(), t.MetadataLocation(), fsF, c), nil
}

func (c *hookCatalog) CommitTable(ctx context.Context, ident icebergtable.Identifier, reqs []icebergtable.Requirement, updates []icebergtable.Update) (icebergtable.Metadata, string, error) {
	c.commits++
	next := func() (icebergtable.Metadata, string, error) { return c.Catalog.CommitTable(ctx, ident, reqs, updates) }
	if c.commit != nil {
		return c.commit(c.commits, next)
	}
	return next()
}

// failingIO fails Open of manifest lists.
type failingIO struct {
	iceio.WriteFileIO
}

func (f failingIO) Open(name string) (iceio.File, error) {
	if strings.Contains(name, "/metadata/snap-") {
		return nil, errors.New("injected: manifest list read failed")
	}
	return f.WriteFileIO.Open(name)
}

var errTransport = errors.New("injected: connection reset by peer")

func audit() iceberg.Properties { return iceberg.Properties{"datuplet.run-id": "run-1"} }

func TestCompactTable_BinPacksPerPartition(t *testing.T) {
	e := newCmpEnv(t, "binpack", true, nil)
	e.seedPartitioned()
	before := e.load().CurrentSnapshot()
	rowsBefore := e.rows()
	key := CompactOperationKey("run-1", "compact", compactNS, "binpack")

	res, err := CompactTable(context.Background(), e.cat, e.ident, CompactOpts{}, audit(), key)
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactCompacted || res.Groups != 2 || res.FilesBefore != 12 || res.FilesRemoved != 12 || res.FilesAdded != 2 || res.FilesAfter != 2 {
		t.Fatalf("result %+v", res)
	}
	if res.SnapshotBefore != before.SnapshotID || res.SnapshotAfter != e.snapshotID() || res.BytesBefore <= 0 || res.BytesAfter <= 0 {
		t.Fatalf("result %+v", res)
	}
	files := e.liveFiles()
	parts := map[any]int{}
	for _, f := range files {
		parts[f.Partition()[iceberg.PartitionDataIDStart]]++
	}
	if len(files) != 2 || parts["a"] != 1 || parts["b"] != 1 {
		t.Fatalf("live files per partition %v", parts)
	}
	if got := e.rows(); len(got) != len(rowsBefore) {
		t.Fatalf("rows %d, want %d", len(got), len(rowsBefore))
	}
	tbl := e.load()
	fsys, _ := tbl.FS(context.Background())
	mfs, err := before.Manifests(fsys)
	if err != nil {
		t.Fatal(err)
	}
	old := 0
	for _, mf := range mfs {
		entries, err := mf.FetchEntries(fsys, true)
		if err != nil {
			t.Fatal(err)
		}
		old += len(entries)
	}
	if old != 12 {
		t.Fatalf("previous snapshot lists %d files, want 12", old)
	}
	cur := tbl.CurrentSnapshot()
	p := cur.Summary.Properties
	if cur.Summary.Operation != icebergtable.OpReplace || p["datuplet.commit-key"] != key || p["datuplet.run-id"] != "run-1" ||
		p["datuplet.compaction.files-before"] != "12" || p["datuplet.compaction.files-after"] != "2" ||
		p["datuplet.compaction.bytes-before"] == "" || p["datuplet.compaction.bytes-after"] == "" {
		t.Fatalf("snapshot %s summary %v", cur.Summary.Operation, p)
	}
}

func TestCompactTable_NothingToDo(t *testing.T) {
	e := newCmpEnv(t, "nothing", false, nil)
	e.seed(2)
	sid := e.snapshotID()
	res, err := CompactTable(context.Background(), e.cat, e.ident, CompactOpts{}, audit(), "k-nothing")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactNothingToDo || res.Groups != 0 || res.FilesBefore != 2 || res.FilesAfter != 2 || res.SnapshotAfter != sid || e.snapshotID() != sid {
		t.Fatalf("result %+v, snapshot %d (was %d)", res, e.snapshotID(), sid)
	}
}

func TestCompactTable_ConcurrentAppendRestaged(t *testing.T) {
	e := newCmpEnv(t, "append_race", false, nil)
	e.seed(6)
	extra := e.writeFile("extra.parquet", []int64{900, 901}, "x")
	cat := &hookCatalog{Catalog: e.cat, commit: func(n int, next func() (icebergtable.Metadata, string, error)) (icebergtable.Metadata, string, error) {
		if n == 1 {
			e.addFiles(extra)
		}
		return next()
	}}
	res, err := CompactTable(context.Background(), cat, e.ident, CompactOpts{}, audit(), "k-append")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactCompacted || cat.commits != 2 || res.FilesRemoved != 6 || res.FilesAfter != 2 {
		t.Fatalf("result %+v after %d commits", res, cat.commits)
	}
	if live := e.livePaths(); !live[extra] || len(live) != 2 {
		t.Fatalf("live files %v, want the compacted file and %s", live, extra)
	}
	if got := e.rows(); len(got) != 14 || !got[900] {
		t.Fatalf("rows %v", got)
	}
}

func TestCompactTable_NonAppendSuperseded(t *testing.T) {
	e := newCmpEnv(t, "replace_race", false, nil)
	e.seed(6)
	big := e.seedSkipped()
	repl := e.writeFile("replacement.parquet", []int64{7000, 7001, 7002}, "x")
	objsBefore := e.dataObjects()
	var replSnap int64
	cat := &hookCatalog{Catalog: e.cat, commit: func(n int, next func() (icebergtable.Metadata, string, error)) (icebergtable.Metadata, string, error) {
		if n == 1 {
			txn := e.load().NewTransaction()
			if err := txn.ReplaceDataFiles(context.Background(), []string{big}, []string{repl}, nil); err != nil {
				t.Fatalf("ReplaceDataFiles: %v", err)
			}
			out, err := txn.Commit(context.Background())
			if err != nil {
				t.Fatalf("replace commit: %v", err)
			}
			replSnap = out.CurrentSnapshot().SnapshotID
		}
		return next()
	}}
	res, err := CompactTable(context.Background(), cat, e.ident, CompactOpts{TargetFileSizeBytes: skipTarget}, audit(), "k-replace")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactSuperseded || res.SnapshotAfter != replSnap || e.snapshotID() != replSnap {
		t.Fatalf("result %+v, current snapshot %d, replacement %d", res, e.snapshotID(), replSnap)
	}
	if left := newObjects(objsBefore, e.dataObjects()); len(left) != 0 {
		t.Fatalf("written files left behind: %v", left)
	}
	if got := e.rows(); len(got) != 12+3 || !got[7000] {
		t.Fatalf("rows %d, want the 12 seeded + the replacement's 3", len(got))
	}
}

func TestCompactTable_DeleteManifestSuperseded(t *testing.T) {
	e := newCmpEnv(t, "delete_race", false, iceberg.Properties{"format-version": "2", icebergtable.WriteDeleteModeKey: icebergtable.WriteModeMergeOnRead})
	e.seed(6)
	objsBefore := e.dataObjects()
	cat := &hookCatalog{Catalog: e.cat, commit: func(n int, next func() (icebergtable.Metadata, string, error)) (icebergtable.Metadata, string, error) {
		if n == 1 {
			if _, err := e.load().Delete(context.Background(), iceberg.EqualTo(iceberg.Reference("id"), int64(21)), nil); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			cur := e.load().CurrentSnapshot()
			if cur.Summary.Properties["added-position-delete-files"] != "1" {
				t.Fatalf("injected delete is not a position delete: %s %v", cur.Summary.Operation, cur.Summary.Properties)
			}
		}
		return next()
	}}
	res, err := CompactTable(context.Background(), cat, e.ident, CompactOpts{}, audit(), "k-delete")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactSuperseded {
		t.Fatalf("result %+v", res)
	}
	got := e.rows()
	if got[21] || len(got) != 11 {
		t.Fatalf("rows %v: deleted row 21 resurrected or rows lost", got)
	}
	if left := unreferenced(newObjects(objsBefore, e.dataObjects()), e.referenced()); len(left) != 0 {
		t.Fatalf("written files left behind: %v", left)
	}
}

func TestCompactTable_RollbackSuperseded(t *testing.T) {
	e := newCmpEnv(t, "rollback", false, nil)
	e.seed(6)
	e.seedSkipped()
	parent := *e.load().CurrentSnapshot().ParentSnapshotID
	cat := &hookCatalog{Catalog: e.cat, commit: func(n int, next func() (icebergtable.Metadata, string, error)) (icebergtable.Metadata, string, error) {
		if n == 1 {
			txn := e.load().NewTransaction()
			if err := txn.RollbackToSnapshot(parent); err != nil {
				t.Fatalf("rollback: %v", err)
			}
			if _, err := txn.Commit(context.Background()); err != nil {
				t.Fatalf("rollback commit: %v", err)
			}
		}
		return next()
	}}
	res, err := CompactTable(context.Background(), cat, e.ident, CompactOpts{TargetFileSizeBytes: skipTarget}, audit(), "k-rollback")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactSuperseded || e.snapshotID() != parent {
		t.Fatalf("result %+v, current %d, want rollback target %d", res, e.snapshotID(), parent)
	}
	if got := e.rows(); len(got) != 12 {
		t.Fatalf("rows %d, want 12 (rolled back state)", len(got))
	}
}

func TestCompactTable_SummaryWithoutCountersRestaged(t *testing.T) {
	e := newCmpEnv(t, "nocounters", false, nil)
	e.seed(6)
	extra := e.writeFile("extra.parquet", []int64{800}, "x")
	cat := &hookCatalog{Catalog: e.cat, onLoad: func(n int) {
		if n == 2 {
			e.addFiles(extra)
			s := e.load().CurrentSnapshot()
			for _, k := range []string{"added-delete-files", "added-position-delete-files", "added-equality-delete-files"} {
				if _, ok := s.Summary.Properties[k]; ok {
					t.Fatalf("precondition: append summary carries %s: %v", k, s.Summary.Properties)
				}
			}
		}
	}}
	res, err := CompactTable(context.Background(), cat, e.ident, CompactOpts{}, audit(), "k-nocounters")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactCompacted || cat.commits != 1 {
		t.Fatalf("result %+v after %d commits", res, cat.commits)
	}
	if live := e.livePaths(); !live[extra] || len(live) != 2 {
		t.Fatalf("live files %v", live)
	}
}

func TestCompactTable_ManifestReadFailure(t *testing.T) {
	e := newCmpEnv(t, "manifest_fail", false, nil)
	e.seed(6)
	extra := e.writeFile("extra.parquet", []int64{800}, "x")
	objsBefore := e.dataObjects()
	cat := &hookCatalog{Catalog: e.cat,
		onLoad: func(n int) {
			if n == 2 {
				e.addFiles(extra)
			}
		},
		wrapFS: func(n int, fs iceio.IO) iceio.IO {
			if n >= 2 {
				return failingIO{fs.(iceio.WriteFileIO)}
			}
			return fs
		},
	}
	res, err := CompactTable(context.Background(), cat, e.ident, CompactOpts{}, audit(), "k-mfail")
	if !errors.Is(err, ErrHistoryUnverifiable) {
		t.Fatalf("CompactTable = %+v, %v; want ErrHistoryUnverifiable", res, err)
	}
	if op := e.load().CurrentSnapshot().Summary.Operation; cat.commits != 0 || op != icebergtable.OpAppend {
		t.Fatalf("%d commits, current snapshot operation %s", cat.commits, op)
	}
	if left := newObjects(objsBefore, e.dataObjects()); len(left) != 0 {
		t.Fatalf("written files left behind: %v", left)
	}
}

func TestCompactTable_LostCommitResponse(t *testing.T) {
	e := newCmpEnv(t, "lost_response", false, nil)
	e.seed(6)
	key := "k-lost"
	cat := &hookCatalog{Catalog: e.cat, commit: func(n int, next func() (icebergtable.Metadata, string, error)) (icebergtable.Metadata, string, error) {
		if _, _, err := next(); err != nil {
			return nil, "", err
		}
		return nil, "", errTransport
	}}
	res, err := CompactTable(context.Background(), cat, e.ident, CompactOpts{}, audit(), key)
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	cur := e.load().CurrentSnapshot()
	if res.Status != CompactCompacted || cat.commits != 1 || res.SnapshotAfter != cur.SnapshotID || cur.Summary.Properties["datuplet.commit-key"] != key {
		t.Fatalf("result %+v after %d commits", res, cat.commits)
	}
	if live := e.livePaths(); len(live) != 1 {
		t.Fatalf("live %v", live)
	}
	if got := e.rows(); len(got) != 12 {
		t.Fatalf("rows %d", len(got))
	}
}

func TestCompactTable_UnknownOutcomeKeepsFiles(t *testing.T) {
	e := newCmpEnv(t, "unknown_outcome", false, nil)
	e.seed(6)
	objsBefore := e.dataObjects()
	sid := e.snapshotID()
	cat := &hookCatalog{Catalog: e.cat, commit: func(int, func() (icebergtable.Metadata, string, error)) (icebergtable.Metadata, string, error) {
		return nil, "", errTransport
	}}
	res, err := CompactTable(context.Background(), cat, e.ident, CompactOpts{}, audit(), "k-unknown")
	if !errors.Is(err, ErrCommitOutcomeUnknown) || !errors.Is(err, errTransport) {
		t.Fatalf("CompactTable = %+v, %v; want ErrCommitOutcomeUnknown wrapping the transport error", res, err)
	}
	if e.snapshotID() != sid {
		t.Fatal("snapshot changed")
	}
	if kept := newObjects(objsBefore, e.dataObjects()); len(kept) != 1 {
		t.Fatalf("written files %v, want the one compacted file kept", kept)
	}
}

func TestCompactTable_OperationKeyIdempotent(t *testing.T) {
	e := newCmpEnv(t, "idempotent", false, nil)
	e.seed(6)
	key := CompactOperationKey("run-1", "compact", compactNS, "idempotent")
	first, err := CompactTable(context.Background(), e.cat, e.ident, CompactOpts{}, audit(), key)
	if err != nil || first.Status != CompactCompacted {
		t.Fatalf("first = %+v, %v", first, err)
	}
	for i := int64(0); i < 6; i++ {
		e.appendRows([]int64{1000 + i}, "x")
	}
	objsBefore := e.dataObjects()
	second, err := CompactTable(context.Background(), e.cat, e.ident, CompactOpts{}, audit(), key)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.Status != CompactAlreadyCommitted || second.SnapshotAfter != first.SnapshotAfter ||
		second.FilesBefore != first.FilesBefore || second.FilesAfter != first.FilesAfter {
		t.Fatalf("second = %+v, first = %+v", second, first)
	}
	if n := e.replaceSnapshots(); n != 1 {
		t.Fatalf("%d replace snapshots, want 1", n)
	}
	if left := newObjects(objsBefore, e.dataObjects()); len(left) != 0 {
		t.Fatalf("second call wrote %v", left)
	}
}

func TestCompactTable_SizeOnlySelection(t *testing.T) {
	e := newCmpEnv(t, "sizeonly", false, iceberg.Properties{"format-version": "2", icebergtable.WriteDeleteModeKey: icebergtable.WriteModeMergeOnRead})
	const target = 64 << 10
	rng := rand.New(rand.NewPCG(1, 2))
	big := make([]int64, 12000)
	for i := range big {
		big[i] = 1_000_000 + rng.Int64N(1<<62)
	}
	e.appendRows(big, "big")
	bigPath := ""
	for p := range e.livePaths() {
		bigPath = p
	}
	for k := int64(0); k < 5; k++ {
		if _, err := e.load().Delete(context.Background(), iceberg.EqualTo(iceberg.Reference("id"), big[k*1000]), nil); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	for i := int64(0); i < 5; i++ {
		e.appendRows([]int64{i}, "small")
	}
	tbl := e.load()
	tasks, err := tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, task := range tasks {
		if task.File.FilePath() == bigPath {
			found = true
			if sz := task.File.FileSizeBytes(); sz < target*3/4 || sz > target*9/5 || len(task.DeleteFiles) < 5 {
				t.Fatalf("precondition: big file %d bytes with %d delete files", sz, len(task.DeleteFiles))
			}
		}
	}
	if !found {
		t.Fatal("precondition: big file not planned")
	}
	// MinInputFiles 1 lets a lone file form a group, so only the delete
	// rules differ between the library defaults and CompactTable.
	def := compaction.DefaultConfig()
	def.TargetFileSizeBytes, def.MinFileSizeBytes, def.MaxFileSizeBytes, def.MinInputFiles = target, target*3/4, target*9/5, 1
	plan, err := def.PlanCompaction(tasks)
	if err != nil {
		t.Fatal(err)
	}
	if !planHas(plan, bigPath) {
		t.Fatal("precondition: the library's default config would not select the big file")
	}

	res, err := CompactTable(context.Background(), e.cat, e.ident, CompactOpts{TargetFileSizeBytes: target, MinInputFiles: 1}, audit(), "k-size")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactCompacted || res.FilesRemoved != 5 || res.FilesSkipped != 1 {
		t.Fatalf("result %+v", res)
	}
	if !e.livePaths()[bigPath] {
		t.Fatal("big file was rewritten")
	}
	if got := e.rows(); len(got) != 12000-5+5 {
		t.Fatalf("rows %d", len(got))
	}
}

func planHas(plan compaction.Plan, path string) bool {
	for _, g := range plan.Groups {
		for _, task := range g.Tasks {
			if task.File.FilePath() == path {
				return true
			}
		}
	}
	return false
}

func TestCompactTable_DryRun(t *testing.T) {
	e := newCmpEnv(t, "dryrun", true, nil)
	e.seedPartitioned()
	sid := e.snapshotID()
	objsBefore := e.dataObjects()
	res, err := CompactTable(context.Background(), e.cat, e.ident, CompactOpts{DryRun: true}, audit(), "k-dry")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactDryRun || res.Groups != 2 || res.FilesBefore != 12 || res.FilesRemoved != 12 || res.SnapshotAfter != sid || e.snapshotID() != sid {
		t.Fatalf("result %+v", res)
	}
	if left := newObjects(objsBefore, e.dataObjects()); len(left) != 0 {
		t.Fatalf("dry run wrote %v", left)
	}
}

func TestCompactTable_MaxInputFilesDefers(t *testing.T) {
	e := newCmpEnv(t, "maxinput", true, nil)
	e.seedPartitioned()
	res, err := CompactTable(context.Background(), e.cat, e.ident, CompactOpts{MaxInputFiles: 6}, audit(), "k-max")
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if res.Status != CompactCompacted || res.Groups != 1 || res.FilesRemoved != 6 || res.FilesDeferred != 6 || res.FilesAfter != 7 {
		t.Fatalf("result %+v", res)
	}
}

func TestCompactTable_TableNotFound(t *testing.T) {
	e := newCmpEnv(t, "present", false, nil)
	_, err := CompactTable(context.Background(), e.cat, icebergtable.Identifier{compactNS, "missing"}, CompactOpts{}, audit(), "k-missing")
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		t.Fatalf("err = %v, want ErrNoSuchTable", err)
	}
}
