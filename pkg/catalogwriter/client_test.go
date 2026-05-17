package catalogwriter

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icebergtable "github.com/apache/iceberg-go/table"

	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// Shared fixture helpers
// ---------------------------------------------------------------------------

// newRestClientFixture stands up a minimal httptest.Server with a /v1/config
// handler (required for rest.NewCatalog bootstrap) plus any extra handlers
// registered via registerHandlers. Returns a fully-constructed *Client.
func newRestClientFixture(t *testing.T, token string, registerHandlers func(mux *http.ServeMux)) *Client {
	t.Helper()
	mux := http.NewServeMux()

	// rest.NewCatalog calls GET /v1/config to discover server overrides.
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})

	if registerHandlers != nil {
		registerHandlers(mux)
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cli, err := NewClient(context.Background(), ClientConfig{
		URI:           srv.URL,
		TokenProvider: func(context.Context) (string, error) { return token, nil },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return cli
}

// newSQLiteCatalog creates a real iceberg-go SQLite catalog in a TempDir
// and pre-creates the given namespace + table with a minimal int64 schema.
// Same DSN flags as CLAUDE.md and commit_shared_test.go.
func newSQLiteCatalog(t *testing.T, ns string, ident []string) catalog.Catalog {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "catalog.db")
	warehouse := "file://" + tmpDir
	dsn := fmt.Sprintf(
		"file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)",
		dbPath,
	)
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })

	cat, err := sqlcat.NewCatalog("test", sqldb, sqlcat.SQLite, iceberg.Properties{"warehouse": warehouse})
	if err != nil {
		t.Fatalf("sqlcat.NewCatalog: %v", err)
	}
	ctx := context.Background()
	if err := cat.CreateNamespace(ctx, icebergtable.Identifier{ns}, nil); err != nil {
		t.Fatalf("CreateNamespace %q: %v", ns, err)
	}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	if _, err := cat.CreateTable(ctx, icebergtable.Identifier(ident), schema); err != nil {
		t.Fatalf("CreateTable %v: %v", ident, err)
	}
	return cat
}

// writeParquet writes a one-row Parquet file at path without field_id
// annotations (required for iceberg-go AddFiles/ReplaceDataFiles per
// commit_shared_test.go's note). Returns the file:// URL.
func writeParquet(t *testing.T, path string, val int64) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	alloc := memory.NewGoAllocator()
	b := array.NewRecordBuilder(alloc, arrowSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(val)
	rec := b.NewRecord()
	defer rec.Release()
	var buf bytes.Buffer
	wprops := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	aprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(alloc))
	w, err := pqarrow.NewFileWriter(arrowSchema, &buf, wprops, aprops)
	if err != nil {
		t.Fatalf("pqarrow.NewFileWriter: %v", err)
	}
	if err := w.WriteBuffered(rec); err != nil {
		t.Fatalf("WriteBuffered: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet Close: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("os.WriteFile %s: %v", path, err)
	}
	return "file://" + path
}

// ---------------------------------------------------------------------------
// TestClient_DropTable_NoPurge
// ---------------------------------------------------------------------------

// TestClient_DropTable_NoPurge verifies that Client.DropTable sends
// DELETE /v1/namespaces/{ns}/tables/{tbl}?purgeRequested=false.
func TestClient_DropTable_NoPurge(t *testing.T) {
	t.Parallel()

	var capturedPath atomic.Value
	hits := &atomic.Int64{}

	cli := newRestClientFixture(t, "tok-drop", func(mux *http.ServeMux) {
		// Catch all DELETE calls under /v1/namespaces/.
		mux.HandleFunc("/v1/namespaces/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodDelete {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			p := r.URL.Path
			if r.URL.RawQuery != "" {
				p += "?" + r.URL.RawQuery
			}
			capturedPath.Store(p)
			hits.Add(1)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	if err := cli.DropTable(context.Background(), []string{"public", "events"}); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected 1 DELETE hit, got %d", got)
	}

	path := capturedPath.Load().(string)
	if !strings.Contains(path, "purgeRequested=false") {
		t.Fatalf("expected purgeRequested=false in %q", path)
	}
	if !strings.Contains(path, "tables/events") {
		t.Fatalf("expected tables/events in %q", path)
	}
}

// ---------------------------------------------------------------------------
// TestClient_PurgeTable_RestCatalog
// ---------------------------------------------------------------------------

// TestClient_PurgeTable_RestCatalog verifies that Client.PurgeTable sends
// DELETE .../tables/{tbl}?purgeRequested=true when backed by a REST catalog.
func TestClient_PurgeTable_RestCatalog(t *testing.T) {
	t.Parallel()

	var capturedPath atomic.Value
	hits := &atomic.Int64{}

	cli := newRestClientFixture(t, "tok-purge", func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodDelete {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			p := r.URL.Path
			if r.URL.RawQuery != "" {
				p += "?" + r.URL.RawQuery
			}
			capturedPath.Store(p)
			hits.Add(1)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	if err := cli.PurgeTable(context.Background(), []string{"public", "events"}); err != nil {
		t.Fatalf("PurgeTable: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected 1 DELETE hit, got %d", got)
	}

	path := capturedPath.Load().(string)
	if !strings.Contains(path, "purgeRequested=true") {
		t.Fatalf("expected purgeRequested=true in %q", path)
	}
	if !strings.Contains(path, "tables/events") {
		t.Fatalf("expected tables/events in %q", path)
	}
}

// ---------------------------------------------------------------------------
// TestClient_PurgeTable_NotRestCatalog_Error
// ---------------------------------------------------------------------------

// TestClient_PurgeTable_NotRestCatalog_Error verifies that PurgeTable (both
// the Client method and the package-level helper) returns a clear "requires
// REST catalog" error when the underlying catalog is SQLite.
func TestClient_PurgeTable_NotRestCatalog_Error(t *testing.T) {
	t.Parallel()

	sqliteCat := newSQLiteCatalog(t, "purge_ns", []string{"purge_ns", "t"})
	ctx := context.Background()

	// Method form.
	cli := &Client{Catalog: sqliteCat}
	if err := cli.PurgeTable(ctx, []string{"purge_ns", "t"}); err == nil {
		t.Fatal("PurgeTable on SQLite: expected error, got nil")
	} else if !errors.Is(err, ErrPurgeNotSupported) {
		t.Fatalf("PurgeTable error %v not wrapping ErrPurgeNotSupported", err)
	}

	// Package-level helper form.
	if err := PurgeTable(ctx, sqliteCat, []string{"purge_ns", "t"}); err == nil {
		t.Fatal("package-level PurgeTable on SQLite: expected error, got nil")
	} else if !errors.Is(err, ErrPurgeNotSupported) {
		t.Fatalf("package-level PurgeTable error %v not wrapping ErrPurgeNotSupported", err)
	}
}

// ---------------------------------------------------------------------------
// TestClient_DropNamespace_HappyPath
// ---------------------------------------------------------------------------

// TestClient_DropNamespace_HappyPath verifies that Client.DropNamespace
// sends DELETE /v1/namespaces/{ns} and succeeds on 204.
func TestClient_DropNamespace_HappyPath(t *testing.T) {
	t.Parallel()

	var capturedPath atomic.Value
	hits := &atomic.Int64{}

	cli := newRestClientFixture(t, "tok-ns-drop", func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodDelete {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			capturedPath.Store(r.URL.Path)
			hits.Add(1)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	if err := cli.DropNamespace(context.Background(), []string{"myns"}); err != nil {
		t.Fatalf("DropNamespace: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected 1 DELETE hit, got %d", got)
	}

	path := capturedPath.Load().(string)
	if !strings.HasSuffix(path, "/namespaces/myns") {
		t.Fatalf("DELETE path %q expected to end with /namespaces/myns", path)
	}
}

// ---------------------------------------------------------------------------
// TestClient_DropNamespace_NonEmpty_Error
// ---------------------------------------------------------------------------

// TestClient_DropNamespace_NonEmpty_Error verifies that a 409 response from
// the server (namespace not empty) propagates as a non-nil error.
func TestClient_DropNamespace_NonEmpty_Error(t *testing.T) {
	t.Parallel()

	cli := newRestClientFixture(t, "tok-ns-conflict", func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodDelete {
				w.WriteHeader(http.StatusConflict)
				_, _ = fmt.Fprint(w, `{"error":{"message":"namespace not empty","type":"NamespaceNotEmptyException","code":409}}`)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		})
	})

	if err := cli.DropNamespace(context.Background(), []string{"nonempty"}); err == nil {
		t.Fatal("DropNamespace with 409: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestReplaceDataFiles_DelegatesToTxn
// ---------------------------------------------------------------------------

// TestReplaceDataFiles_DelegatesToTxn verifies that the package-level
// ReplaceDataFiles function delegates correctly to txn.ReplaceDataFiles.
// It uses a real SQLite catalog: writes an initial snapshot with AddFiles
// (one file), then replaces it with two new files via ReplaceDataFiles.
// After commit the table's current snapshot should reference only the two
// new files.
func TestReplaceDataFiles_DelegatesToTxn(t *testing.T) {
	t.Parallel()

	cat := newSQLiteCatalog(t, "rdf_ns", []string{"rdf_ns", "rdf_tbl"})
	ctx := context.Background()
	ident := icebergtable.Identifier{"rdf_ns", "rdf_tbl"}
	tmpDir := t.TempDir()

	// Write three distinct parquet files.
	old1 := writeParquet(t, filepath.Join(tmpDir, "old1.parquet"), 1)
	new1 := writeParquet(t, filepath.Join(tmpDir, "new1.parquet"), 2)
	new2 := writeParquet(t, filepath.Join(tmpDir, "new2.parquet"), 3)

	// Step 1: initial APPEND so the table has a current snapshot.
	{
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("LoadTable (initial): %v", err)
		}
		txn := tbl.NewTransaction()
		if err := txn.AddFiles(ctx, []string{old1}, nil, false); err != nil {
			t.Fatalf("AddFiles (initial): %v", err)
		}
		if _, err := txn.Commit(ctx); err != nil {
			t.Fatalf("Commit (initial): %v", err)
		}
	}

	// Step 2: ReplaceDataFiles — replace old1 with new1+new2.
	{
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("LoadTable (replace): %v", err)
		}
		txn := tbl.NewTransaction()
		if err := ReplaceDataFiles(ctx, txn, []string{old1}, []string{new1, new2}, nil); err != nil {
			t.Fatalf("ReplaceDataFiles: %v", err)
		}
		if _, err := txn.Commit(ctx); err != nil {
			t.Fatalf("Commit (replace): %v", err)
		}
	}

	// Step 3: reload and assert current snapshot contains exactly new1 and new2.
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable (verify): %v", err)
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		t.Fatal("no current snapshot after ReplaceDataFiles commit")
	}

	tasks, err := tbl.Scan(icebergtable.WithSnapshotID(snap.SnapshotID)).PlanFiles(ctx)
	if err != nil {
		t.Fatalf("PlanFiles: %v", err)
	}
	var paths []string
	for _, tf := range tasks {
		u, _ := url.Parse(tf.File.FilePath())
		paths = append(paths, u.Path)
	}
	if len(paths) != 2 {
		t.Fatalf("expected 2 data files after replace, got %d: %v", len(paths), paths)
	}
	for _, p := range paths {
		if strings.HasSuffix(p, "old1.parquet") {
			t.Fatalf("old1.parquet still present after replace: %v", paths)
		}
	}
}
