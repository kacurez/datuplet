package icebergjob

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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
// integer schema. Tests use this to drive CommitTable through the local
// SQLite catalog path.
type commitSharedFixture struct {
	t          *testing.T
	tmpDir     string
	warehouse  string
	cat        catalog.Catalog
	sqldb      *sql.DB
	schema     *iceberg.Schema
	arrowSchema *arrow.Schema
	ident      icebergtable.Identifier
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

// writeManifest writes a v1-shaped files.json at <tableLocation>/.run-state/<runID>/files.json
// and returns the absolute file:// URL of the manifest.
func (f *commitSharedFixture) writeManifest(runID string, paths []string) string {
	f.t.Helper()
	tbl, err := f.cat.LoadTable(context.Background(), f.ident)
	if err != nil {
		f.t.Fatalf("LoadTable for manifest base: %v", err)
	}
	loc := tbl.Location()
	manifestURL := FilesManifestPath(loc, runID)
	// Convert file:// URL to absolute path for os.WriteFile.
	manifestPath := strings.TrimPrefix(manifestURL, "file://")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		f.t.Fatalf("mkdir manifest parent: %v", err)
	}
	doc := FilesManifest{
		RunID:     runID,
		Namespace: strings.Join(f.ident[:len(f.ident)-1], "."),
		Table:     f.ident[len(f.ident)-1],
		Paths:     paths,
	}
	body, err := json.Marshal(&doc)
	if err != nil {
		f.t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, body, 0o644); err != nil {
		f.t.Fatalf("write manifest: %v", err)
	}
	return manifestURL
}

// writeManifestEmpty writes a manifest with paths=[].
func (f *commitSharedFixture) writeManifestEmpty(runID string) string {
	return f.writeManifest(runID, []string{})
}

// manifestPathFor returns the URL where a manifest WOULD live for runID,
// without actually creating the file (used to test the missing-manifest
// success-zero path).
func (f *commitSharedFixture) manifestPathFor(runID string) string {
	f.t.Helper()
	tbl, err := f.cat.LoadTable(context.Background(), f.ident)
	if err != nil {
		f.t.Fatalf("LoadTable for manifest path: %v", err)
	}
	return FilesManifestPath(tbl.Location(), runID)
}

// TestCommitTable_AppendHappyPath: 3 paths, APPEND, table has no
// pre-existing data → AddFiles + Commit succeed; result reports
// DataFilesAdded=3 and a non-empty SnapshotIDAfter.
func TestCommitTable_AppendHappyPath(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "events"})

	paths := []string{
		fx.writeParquet("data/a.parquet", 1),
		fx.writeParquet("data/b.parquet", 2),
		fx.writeParquet("data/c.parquet", 3),
	}
	manifestPath := fx.writeManifest("run-append-1", paths)

	res, err := CommitTable(context.Background(), fx.cat, fx.ident, manifestPath, WriteModeAppend, nil)
	if err != nil {
		t.Fatalf("CommitTable: %v", err)
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
}

// TestCommitTable_FullLoadHappyPath: table has 2 existing files (via
// initial APPEND); FULL_LOAD with 1 new path should ReplaceDataFiles
// the existing 2 with the new 1.
func TestCommitTable_FullLoadHappyPath(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "fullload_target"})

	// Seed the table with 2 existing files via an initial APPEND.
	seedPaths := []string{
		fx.writeParquet("data/seed-a.parquet", 10),
		fx.writeParquet("data/seed-b.parquet", 20),
	}
	seedManifest := fx.writeManifest("seed-run", seedPaths)
	if _, err := CommitTable(context.Background(), fx.cat, fx.ident, seedManifest, WriteModeAppend, nil); err != nil {
		t.Fatalf("seed APPEND: %v", err)
	}

	// Now FULL_LOAD with one new file — old 2 should be replaced.
	newPaths := []string{
		fx.writeParquet("data/new-x.parquet", 99),
	}
	manifestPath := fx.writeManifest("run-fullload-1", newPaths)

	res, err := CommitTable(context.Background(), fx.cat, fx.ident, manifestPath, WriteModeFullLoad, nil)
	if err != nil {
		t.Fatalf("CommitTable FULL_LOAD: %v", err)
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
}

// TestCommitTable_FullLoad_FreshTable: FULL_LOAD against a table with
// no current snapshot — iceberg-go's ReplaceDataFiles delegates to
// AddFiles when filesToDelete is empty, so the call should succeed.
func TestCommitTable_FullLoad_FreshTable(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "fresh_fullload"})

	newPaths := []string{
		fx.writeParquet("data/initial.parquet", 7),
	}
	manifestPath := fx.writeManifest("run-fresh-fullload", newPaths)

	res, err := CommitTable(context.Background(), fx.cat, fx.ident, manifestPath, WriteModeFullLoad, nil)
	if err != nil {
		t.Fatalf("CommitTable FULL_LOAD on fresh table: %v", err)
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

// TestCommitTable_EmptyManifest_NoOp: manifest exists but paths is [].
// Returns success-zero result, no commit happens.
func TestCommitTable_EmptyManifest_NoOp(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "empty_manifest"})

	manifestPath := fx.writeManifestEmpty("run-empty")

	res, err := CommitTable(context.Background(), fx.cat, fx.ident, manifestPath, WriteModeAppend, nil)
	if err != nil {
		t.Fatalf("CommitTable: %v", err)
	}
	if res == nil {
		t.Fatalf("nil result on empty manifest")
	}
	if res.DataFilesAdded != 0 {
		t.Errorf("DataFilesAdded=%d want 0 (empty manifest)", res.DataFilesAdded)
	}
	if res.SnapshotIDAfter != "" {
		t.Errorf("SnapshotIDAfter=%q want empty (no commit)", res.SnapshotIDAfter)
	}
	if res.WriteMode != WriteModeAppend {
		t.Errorf("WriteMode=%q want APPEND", res.WriteMode)
	}
}

// TestCommitTable_MissingManifest_NoOp: manifestPath does not exist.
// Returns success-zero (matches today's table-commit behavior where a
// run that produced no parquet for a target is a no-op).
func TestCommitTable_MissingManifest_NoOp(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "missing_manifest"})

	manifestPath := fx.manifestPathFor("run-missing") // never written
	res, err := CommitTable(context.Background(), fx.cat, fx.ident, manifestPath, WriteModeAppend, nil)
	if err != nil {
		t.Fatalf("CommitTable: %v", err)
	}
	if res == nil {
		t.Fatalf("nil result on missing manifest")
	}
	if res.DataFilesAdded != 0 {
		t.Errorf("DataFilesAdded=%d want 0 (missing manifest)", res.DataFilesAdded)
	}
	if res.SnapshotIDAfter != "" {
		t.Errorf("SnapshotIDAfter=%q want empty (no commit)", res.SnapshotIDAfter)
	}
}

// TestCommitTable_UnsupportedMode_Error: mode "UPSERT" (or any unknown
// literal) returns an error before any commit work runs.
func TestCommitTable_UnsupportedMode_Error(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"raw", "unsupported_mode"})

	paths := []string{fx.writeParquet("data/a.parquet", 1)}
	manifestPath := fx.writeManifest("run-unsupported", paths)

	_, err := CommitTable(context.Background(), fx.cat, fx.ident, manifestPath, WriteMode("UPSERT"), nil)
	if err == nil {
		t.Fatalf("expected error for unsupported mode UPSERT, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported write mode") {
		t.Errorf("error=%q want 'unsupported write mode' substring", err)
	}
}

// TestParseFilesManifest_V1Migration: a v1 manifest (Paths populated,
// SchemaVersion absent) is migrated to v2 (DataPaths populated).
func TestParseFilesManifest_V1Migration(t *testing.T) {
	t.Parallel()
	v1 := []byte(`{"run_id":"r","namespace":"raw","table":"t","paths":["s3://b/a.parquet","s3://b/b.parquet"]}`)
	m, err := parseFilesManifest(v1)
	if err != nil {
		t.Fatalf("parseFilesManifest v1: %v", err)
	}
	if m.SchemaVersion != 0 {
		t.Errorf("SchemaVersion=%d want 0 (v1)", m.SchemaVersion)
	}
	if len(m.DataPaths) != 2 {
		t.Errorf("DataPaths len=%d want 2 (v1→v2 migration)", len(m.DataPaths))
	}
	if m.DataPaths[0] != "s3://b/a.parquet" {
		t.Errorf("DataPaths[0]=%q want s3://b/a.parquet", m.DataPaths[0])
	}
}

// TestParseFilesManifest_V2: a v2 manifest with SchemaVersion=2 and
// DataPaths is read directly without migration.
func TestParseFilesManifest_V2(t *testing.T) {
	t.Parallel()
	v2 := []byte(`{"schema_version":2,"run_id":"r","namespace":"raw","table":"t","data_paths":["s3://b/a.parquet"],"delete_paths":[]}`)
	m, err := parseFilesManifest(v2)
	if err != nil {
		t.Fatalf("parseFilesManifest v2: %v", err)
	}
	if m.SchemaVersion != 2 {
		t.Errorf("SchemaVersion=%d want 2", m.SchemaVersion)
	}
	if len(m.DataPaths) != 1 {
		t.Errorf("DataPaths len=%d want 1", len(m.DataPaths))
	}
}

// TestCommitTableFiles_SucceedsWithInMemoryPaths: CommitTableFiles with 2
// in-memory paths and WriteModeAppend should succeed and report DataFilesAdded=2.
func TestCommitTableFiles_SucceedsWithInMemoryPaths(t *testing.T) {
	t.Parallel()
	fx := newCommitSharedFixture(t, icebergtable.Identifier{"ns1", "tbl1"})

	paths := []string{
		fx.writeParquet("data/a.parquet", 1),
		fx.writeParquet("data/b.parquet", 2),
	}

	res, err := CommitTableFiles(context.Background(), fx.cat, icebergtable.Identifier{"ns1", "tbl1"},
		paths, WriteModeAppend, nil, "")
	if err != nil {
		t.Fatalf("CommitTableFiles: %v", err)
	}
	if res == nil {
		t.Fatalf("nil result")
	}
	if res.DataFilesAdded != 2 {
		t.Errorf("DataFilesAdded=%d want 2", res.DataFilesAdded)
	}
	if res.SnapshotIDAfter == "" {
		t.Errorf("SnapshotIDAfter empty; want a snapshot ID after append")
	}
	if res.WriteMode != WriteModeAppend {
		t.Errorf("WriteMode=%q want APPEND", res.WriteMode)
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

// TestParseFilesManifest_Empty: an empty paths/data_paths produces
// ErrManifestEmpty.
func TestParseFilesManifest_Empty(t *testing.T) {
	t.Parallel()
	body := []byte(`{"run_id":"r","namespace":"raw","table":"t","paths":[]}`)
	_, err := parseFilesManifest(body)
	if err == nil {
		t.Fatalf("expected ErrManifestEmpty, got nil")
	}
	if err != ErrManifestEmpty {
		t.Errorf("err=%v want ErrManifestEmpty", err)
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
