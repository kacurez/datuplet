package icebergjob

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icebergtable "github.com/apache/iceberg-go/table"

	_ "modernc.org/sqlite"
)

// commitSharedFixture spins up a real iceberg-go SQLite catalog over a
// tmp warehouse directory + creates a (namespace, table) with a small
// integer schema. Tests use this to drive CommitTableFiles through the local
// SQLite catalog path.
type commitSharedFixture struct {
	t           *testing.T
	tmpDir      string
	warehouse   string
	cat         catalog.Catalog
	sqldb       *sql.DB
	schema      *iceberg.Schema
	arrowSchema *arrow.Schema
	ident       icebergtable.Identifier
}

func newCommitSharedFixture(t *testing.T, ident icebergtable.Identifier) *commitSharedFixture {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "catalog.db")
	warehouse := "file://" + tmpDir
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", dbPath)
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	cat, err := sqlcat.NewCatalog("test", sqldb, sqlcat.SQLite, iceberg.Properties{"warehouse": warehouse})
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	ctx := context.Background()
	ns := icebergtable.Identifier{ident[0]}
	if err := cat.CreateNamespace(ctx, ns, nil); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	icSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	if _, err := cat.CreateTable(ctx, ident, icSchema); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	// Arrow schema for parquet writes. iceberg-go's AddFiles requires
	// the parquet footer to NOT carry PARQUET:field_id annotations
	// (it stamps field-ids itself via the catalog's name mapping); a
	// parquet with field_ids is rejected with "add-files only supports
	// the addition of files without field_ids". We omit the metadata
	// to match production's writer-side contract for AddFiles inputs.
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		},
	}, nil)
	return &commitSharedFixture{
		t:           t,
		tmpDir:      tmpDir,
		warehouse:   warehouse,
		cat:         cat,
		sqldb:       sqldb,
		schema:      icSchema,
		arrowSchema: arrowSchema,
		ident:       ident,
	}
}

// writeParquet builds a one-row parquet at <tmpDir>/<rel> and returns
// the absolute file:// URL iceberg-go expects from AddFiles.
func (f *commitSharedFixture) writeParquet(rel string, val int64) string {
	f.t.Helper()
	abs := filepath.Join(f.tmpDir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		f.t.Fatalf("mkdir parquet parent: %v", err)
	}
	alloc := memory.NewGoAllocator()
	b := array.NewRecordBuilder(alloc, f.arrowSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(val)
	rec := b.NewRecord()
	defer rec.Release()
	var buf bytes.Buffer
	wprops := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	aprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(alloc))
	w, err := pqarrow.NewFileWriter(f.arrowSchema, &buf, wprops, aprops)
	if err != nil {
		f.t.Fatalf("pqarrow.NewFileWriter: %v", err)
	}
	if err := w.WriteBuffered(rec); err != nil {
		f.t.Fatalf("WriteBuffered: %v", err)
	}
	if err := w.Close(); err != nil {
		f.t.Fatalf("parquet Close: %v", err)
	}
	if err := os.WriteFile(abs, buf.Bytes(), 0o644); err != nil {
		f.t.Fatalf("write parquet: %v", err)
	}
	return "file://" + abs
}

// TestCommitTableFiles_AppendHappyPath: 3 paths, APPEND, table has no
// pre-existing data → AddFiles + Commit succeed; result reports
// DataFilesAdded=3 and a non-empty SnapshotIDAfter.
func TestCommitTableFiles_AppendHappyPath(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "events"})

	paths := []string{
		fx.writeParquet("data/a.parquet", 1),
		fx.writeParquet("data/b.parquet", 2),
		fx.writeParquet("data/c.parquet", 3),
	}

	res, err := CommitTableFiles(context.Background(), fx.cat, fx.ident, paths, WriteModeAppend, nil, "")
	if err != nil {
		t.Fatalf("CommitTableFiles: %v", err)
	}
	if res == nil {
		t.Fatalf("nil result")
	}
	if res.WriteMode != WriteModeAppend {
		t.Errorf("WriteMode=%q want APPEND", res.WriteMode)
	}
	if res.DataFilesAdded != 3 {
		t.Errorf("DataFilesAdded=%d want 3", res.DataFilesAdded)
	}
	if res.SnapshotIDBefore != "" {
		t.Errorf("SnapshotIDBefore=%q want empty (fresh table)", res.SnapshotIDBefore)
	}
	if res.SnapshotIDAfter == "" {
		t.Errorf("SnapshotIDAfter empty; want a snapshot ID after append")
	}

	// Verify the table now has the three files.
	tbl, err := fx.cat.LoadTable(context.Background(), fx.ident)
	if err != nil {
		t.Fatalf("post-commit LoadTable: %v", err)
	}
	postFiles, err := listCurrentSnapshotFilePaths(context.Background(), tbl)
	if err != nil {
		t.Fatalf("listCurrentSnapshotFilePaths: %v", err)
	}
	if len(postFiles) != 3 {
		t.Errorf("post-commit file count=%d want 3 (paths=%v)", len(postFiles), postFiles)
	}

	// RFC 025 §4.2 proof obligation: TableInfo will read total-records /
	// total-data-files from the committed snapshot summary instead of
	// walking manifests. Prove iceberg-go actually emits both keys.
	// writeParquet writes exactly ONE row per file (the int64 arg is the
	// row's "id" value, not a row count) — 3 files here means 3 rows.
	const wantTotalRows, wantTotalFiles = 3, 3
	cur := tbl.CurrentSnapshot()
	if cur == nil || cur.Summary == nil || cur.Summary.Properties == nil {
		t.Fatalf("committed snapshot has no summary properties: %+v", cur)
	}
	t.Logf("append snapshot Summary.Properties=%+v", cur.Summary.Properties)
	if got := cur.Summary.Properties["total-records"]; got != fmt.Sprintf("%d", wantTotalRows) {
		t.Fatalf("total-records = %q, want %d — RFC 025 §4.2 relies on this summary key (props=%+v)", got, wantTotalRows, cur.Summary.Properties)
	}
	if got := cur.Summary.Properties["total-data-files"]; got != fmt.Sprintf("%d", wantTotalFiles) {
		t.Fatalf("total-data-files = %q, want %d (props=%+v)", got, wantTotalFiles, cur.Summary.Properties)
	}
}

// TestCommitTableFiles_FullLoadHappyPath: table has 2 existing files (via
// initial APPEND); FULL_LOAD with 1 new path should ReplaceDataFiles
// the existing 2 with the new 1.
func TestCommitTableFiles_FullLoadHappyPath(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "fullload_target"})

	// Seed the table with 2 existing files via an initial APPEND.
	seedPaths := []string{
		fx.writeParquet("data/seed-a.parquet", 10),
		fx.writeParquet("data/seed-b.parquet", 20),
	}
	if _, err := CommitTableFiles(context.Background(), fx.cat, fx.ident, seedPaths, WriteModeAppend, nil, ""); err != nil {
		t.Fatalf("seed APPEND: %v", err)
	}

	// Now FULL_LOAD with one new file — old 2 should be replaced.
	newPaths := []string{
		fx.writeParquet("data/new-x.parquet", 99),
	}

	res, err := CommitTableFiles(context.Background(), fx.cat, fx.ident, newPaths, WriteModeFullLoad, nil, "")
	if err != nil {
		t.Fatalf("CommitTableFiles FULL_LOAD: %v", err)
	}
	if res.WriteMode != WriteModeFullLoad {
		t.Errorf("WriteMode=%q want FULL_LOAD", res.WriteMode)
	}
	if res.DataFilesAdded != 1 {
		t.Errorf("DataFilesAdded=%d want 1", res.DataFilesAdded)
	}
	if res.SnapshotIDBefore == "" {
		t.Errorf("SnapshotIDBefore empty; want the seed snapshot ID")
	}
	if res.SnapshotIDAfter == "" || res.SnapshotIDAfter == res.SnapshotIDBefore {
		t.Errorf("SnapshotIDAfter=%q before=%q; want a new snapshot",
			res.SnapshotIDAfter, res.SnapshotIDBefore)
	}

	// Verify only the new file is present.
	tbl, err := fx.cat.LoadTable(context.Background(), fx.ident)
	if err != nil {
		t.Fatalf("post-commit LoadTable: %v", err)
	}
	postFiles, err := listCurrentSnapshotFilePaths(context.Background(), tbl)
	if err != nil {
		t.Fatalf("listCurrentSnapshotFilePaths: %v", err)
	}
	if len(postFiles) != 1 {
		t.Errorf("post-commit file count=%d want 1 (paths=%v)", len(postFiles), postFiles)
	}

	// RFC 025 §4.2 proof obligation: full-load REPLACES the prior files,
	// so the summary totals must reflect only the new file (1 row, 1
	// file), not the cumulative history (writeParquet writes 1 row/file).
	const wantTotalRows, wantTotalFiles = 1, 1
	cur := tbl.CurrentSnapshot()
	if cur == nil || cur.Summary == nil || cur.Summary.Properties == nil {
		t.Fatalf("committed snapshot has no summary properties: %+v", cur)
	}
	t.Logf("full-load snapshot Summary.Properties=%+v", cur.Summary.Properties)
	if got := cur.Summary.Properties["total-records"]; got != fmt.Sprintf("%d", wantTotalRows) {
		t.Fatalf("total-records = %q, want %d — RFC 025 §4.2 relies on this summary key (props=%+v)", got, wantTotalRows, cur.Summary.Properties)
	}
	if got := cur.Summary.Properties["total-data-files"]; got != fmt.Sprintf("%d", wantTotalFiles) {
		t.Fatalf("total-data-files = %q, want %d (props=%+v)", got, wantTotalFiles, cur.Summary.Properties)
	}
}

// TestCommitTableFiles_FullLoad_FreshTable: FULL_LOAD against a table with
// no current snapshot — iceberg-go's ReplaceDataFiles delegates to
// AddFiles when filesToDelete is empty, so the call should succeed.
func TestCommitTableFiles_FullLoad_FreshTable(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "fresh_fullload"})

	newPaths := []string{
		fx.writeParquet("data/initial.parquet", 7),
	}

	res, err := CommitTableFiles(context.Background(), fx.cat, fx.ident, newPaths, WriteModeFullLoad, nil, "")
	if err != nil {
		t.Fatalf("CommitTableFiles FULL_LOAD on fresh table: %v", err)
	}
	if res.DataFilesAdded != 1 {
		t.Errorf("DataFilesAdded=%d want 1", res.DataFilesAdded)
	}
	if res.SnapshotIDBefore != "" {
		t.Errorf("SnapshotIDBefore=%q want empty (fresh table)", res.SnapshotIDBefore)
	}
	if res.SnapshotIDAfter == "" {
		t.Errorf("SnapshotIDAfter empty; want a snapshot ID after fresh full-load")
	}

	tbl, err := fx.cat.LoadTable(context.Background(), fx.ident)
	if err != nil {
		t.Fatalf("post-commit LoadTable: %v", err)
	}
	postFiles, err := listCurrentSnapshotFilePaths(context.Background(), tbl)
	if err != nil {
		t.Fatalf("listCurrentSnapshotFilePaths: %v", err)
	}
	if len(postFiles) != 1 {
		t.Errorf("post-commit file count=%d want 1 (paths=%v)", len(postFiles), postFiles)
	}
}

// TestCommitTableFiles_NoPaths_NoOp: empty dataPaths is success-zero and no
// commit happens.
func TestCommitTableFiles_NoPaths_NoOp(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "no_paths"})

	res, err := CommitTableFiles(context.Background(), fx.cat, fx.ident, nil, WriteModeAppend, nil, "")
	if err != nil {
		t.Fatalf("CommitTableFiles: %v", err)
	}
	if res.DataFilesAdded != 0 {
		t.Errorf("DataFilesAdded=%d want 0", res.DataFilesAdded)
	}
	if res.SnapshotIDAfter != "" {
		t.Errorf("SnapshotIDAfter=%q want empty (no commit)", res.SnapshotIDAfter)
	}
	if res.WriteMode != WriteModeAppend {
		t.Errorf("WriteMode=%q want APPEND", res.WriteMode)
	}
}

// TestCommitTableFiles_UnsupportedMode_Error: mode "UPSERT" (or any unknown
// literal) returns an error before any commit work runs.
func TestCommitTableFiles_UnsupportedMode_Error(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "unsupported_mode"})

	paths := []string{fx.writeParquet("data/a.parquet", 1)}

	_, err := CommitTableFiles(context.Background(), fx.cat, fx.ident, paths, WriteMode("UPSERT"), nil, "")
	if err == nil {
		t.Fatalf("expected error for unsupported mode UPSERT, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported write mode") {
		t.Errorf("error=%q want 'unsupported write mode' substring", err)
	}
}

// TestCommitTableFiles_StampsSnapshotProps: caller-supplied audit props
// (the BuildSnapshotSummary output the Data Gateway passes) must land on the
// committed snapshot's summary, merged with the idempotency commit-key. This
// is the audit-trail parity restored after RFC 021 (the inline-commit path
// initially passed nil snapshotProps, so snapshots had empty actor/run-id).
func TestCommitTableFiles_StampsSnapshotProps(t *testing.T) {
	t.Parallel()
	ident := icebergtable.Identifier{"ns1", "audit_tbl"}
	fx := newCommitSharedFixture(t, ident)
	path := fx.writeParquet("data/a.parquet", 1)

	props := iceberg.Properties{
		"datuplet.actor":        "user-123",
		"datuplet.run-id":       "run-abc",
		"datuplet.run-mode":     "cluster",
		"datuplet.pipeline-api": "datuplet-api",
	}
	res, err := CommitTableFiles(context.Background(), fx.cat, ident,
		[]string{path}, WriteModeAppend, props, "commit-key-xyz")
	if err != nil {
		t.Fatalf("CommitTableFiles: %v", err)
	}
	if res.SnapshotIDAfter == "" {
		t.Fatal("expected a snapshot after append")
	}

	tbl, err := fx.cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	snaps := tbl.Metadata().Snapshots()
	latest := snaps[len(snaps)-1]
	if latest.Summary == nil || latest.Summary.Properties == nil {
		t.Fatal("latest snapshot summary/properties nil")
	}
	got := latest.Summary.Properties
	for k, want := range map[string]string{
		"datuplet.actor":        "user-123",
		"datuplet.run-id":       "run-abc",
		"datuplet.run-mode":     "cluster",
		"datuplet.pipeline-api": "datuplet-api",
		"datuplet.commit-key":   "commit-key-xyz", // added by CommitTableFiles on top of audit props
	} {
		if got[k] != want {
			t.Errorf("snapshot summary[%q]=%q want %q", k, got[k], want)
		}
	}
}

// TestCommitTableFiles_IdempotencyHit: first commit with key "test-key-abc"
// should succeed; second commit with the same key but different paths should
// short-circuit (idempotency hit) and leave the table with exactly 1 file.
func TestCommitTableFiles_IdempotencyHit(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "idem_tbl"})

	firstPath := fx.writeParquet("data/a.parquet", 1)
	const key = "test-key-abc"

	res1, err := CommitTableFiles(context.Background(), fx.cat, icebergtable.Identifier{"ns1", "idem_tbl"},
		[]string{firstPath}, WriteModeAppend, nil, key)
	if err != nil {
		t.Fatalf("first CommitTableFiles: %v", err)
	}
	if res1.DataFilesAdded != 1 {
		t.Errorf("first commit DataFilesAdded=%d want 1", res1.DataFilesAdded)
	}
	if res1.SnapshotIDAfter == "" {
		t.Errorf("first commit SnapshotIDAfter empty")
	}
	if res1.IdempotencyHit {
		t.Errorf("first commit IdempotencyHit=true; want false (real commit)")
	}

	// Second call: same key, different path — must be idempotency hit.
	shouldNotBeAdded := fx.writeParquet("data/SHOULD_NOT_BE_ADDED.parquet", 999)
	res2, err := CommitTableFiles(context.Background(), fx.cat, icebergtable.Identifier{"ns1", "idem_tbl"},
		[]string{shouldNotBeAdded}, WriteModeAppend, nil, key)
	if err != nil {
		t.Fatalf("second CommitTableFiles: %v", err)
	}
	if res2 == nil {
		t.Fatalf("second commit nil result")
	}
	if res2.SnapshotIDAfter == "" {
		t.Errorf("second commit SnapshotIDAfter empty; want it populated via idempotency hit")
	}
	if !res2.IdempotencyHit {
		t.Errorf("second commit IdempotencyHit=false; want true (skipped commit)")
	}
	// Table must still have exactly 1 data file.
	tbl, err := fx.cat.LoadTable(context.Background(), icebergtable.Identifier{"ns1", "idem_tbl"})
	if err != nil {
		t.Fatalf("post-idempotency LoadTable: %v", err)
	}
	postFiles, err := listCurrentSnapshotFilePaths(context.Background(), tbl)
	if err != nil {
		t.Fatalf("listCurrentSnapshotFilePaths: %v", err)
	}
	if len(postFiles) != 1 {
		t.Errorf("post-idempotency file count=%d want 1 (paths=%v)", len(postFiles), postFiles)
	}
}

// TestCommitTableFiles_RejectsCallerCommitKey: CommitTableFiles must return
// an error immediately when the caller places "datuplet.commit-key" in
// snapshotProps (the idempotency key must only be passed via idempotencyKey).
func TestCommitTableFiles_RejectsCallerCommitKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "tbl1"})
	_, err := CommitTableFiles(ctx, fx.cat, icebergtable.Identifier{"ns1", "tbl1"},
		[]string{"s3://b/data/a.parquet"}, WriteModeAppend,
		iceberg.Properties{"datuplet.commit-key": "x"}, "y")
	if err == nil {
		t.Fatal("expected error when caller sets snapshotProps[datuplet.commit-key]")
	}
}
