//go:build duckdb_arrow

// Catalog-write deny matrix on an ATTACHED, locked engine (DuckDB 2.0.0).
//
// TestLockdownDeny proves the posture on an unattached engine, which is blind to
// anything that writes THROUGH the attached catalog: on the production state
// (attach → USE lk."<ns>" → lock) a bare CALL dbgen() created its tables in the
// current catalog lk (stage-create POSTs to lakekeeper), and 2.0's
// iceberg_rollback_to_snapshot / set_iceberg_table_properties reached
// /v1/transactions/commit — metadata-only writes the read-only vended STS
// credentials cannot stop. attachCatalog therefore attaches READ_ONLY and drops
// the bearer secret before lock(); this file proves that DuckDB refuses the listed
// writes before any catalog WRITE request (binding may still issue reads), and
// that the catalog cannot be attached again with the stored token.
//
// The fake iceberg-REST catalog ACCEPTS every mutation with 200: a leak must show
// up as a SUCCESS plus a recorded mutation, never hide behind a 403 that a real
// viewer-scoped lakekeeper would also return. No network or Docker is needed — it
// runs wherever iceberg/httpfs load (plain CI with warmed extensions).
//
// RE-RUN ON EVERY duckdb-go VERSION BUMP and reconcile the deny rows against the
// iceberg extension's function list (duckdb_functions() after LOAD iceberg): a new
// catalog-mutating table function is new write surface.
package queryengine

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeCatalog is a minimal iceberg-REST catalog: one namespace "ns" holding one
// table "t" (single long column id, snapshots 1001 → 2002).
type fakeCatalog struct {
	srv *httptest.Server

	mu    sync.Mutex
	reqs  []string
	auths []string // Authorization header of reqs[i]
	muts  int
}

func fakeTableMetadata() map[string]any {
	snap := func(id, parent, seq int64) map[string]any {
		s := map[string]any{
			"snapshot-id":     id,
			"sequence-number": seq,
			"timestamp-ms":    1700000000000 + seq*1000,
			"manifest-list":   "s3://bucket/ns/t/metadata/snap-" + strconv.FormatInt(id, 10) + ".avro",
			"summary":         map[string]any{"operation": "append"},
			"schema-id":       0,
		}
		if parent != 0 {
			s["parent-snapshot-id"] = parent
		}
		return s
	}
	return map[string]any{
		"format-version":       2,
		"table-uuid":           "11111111-1111-1111-1111-111111111111",
		"location":             "s3://bucket/ns/t",
		"last-sequence-number": 2,
		"last-updated-ms":      1700000002000,
		"last-column-id":       1,
		"current-schema-id":    0,
		"schemas": []any{map[string]any{
			"type": "struct", "schema-id": 0,
			"fields": []any{map[string]any{"id": 1, "name": "id", "required": false, "type": "long"}},
		}},
		"default-spec-id":       0,
		"partition-specs":       []any{map[string]any{"spec-id": 0, "fields": []any{}}},
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders":           []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties":            map[string]any{},
		"current-snapshot-id":   2002,
		"snapshots":             []any{snap(1001, 0, 1), snap(2002, 1001, 2)},
		"snapshot-log": []any{
			map[string]any{"timestamp-ms": 1700000001000, "snapshot-id": 1001},
			map[string]any{"timestamp-ms": 1700000002000, "snapshot-id": 2002},
		},
		"metadata-log": []any{},
		"refs":         map[string]any{"main": map[string]any{"snapshot-id": 2002, "type": "branch"}},
	}
}

func newFakeCatalog(t *testing.T) *fakeCatalog {
	t.Helper()
	fc := &fakeCatalog{}
	loadTable := map[string]any{
		"metadata-location": "s3://bucket/ns/t/metadata/00002.metadata.json",
		"metadata":          fakeTableMetadata(),
		"config":            map[string]any{},
	}
	send := func(w http.ResponseWriter, code int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	}
	fc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fc.mu.Lock()
		fc.reqs = append(fc.reqs, r.Method+" "+r.URL.Path)
		fc.auths = append(fc.auths, r.Header.Get("Authorization"))
		mutating := r.Method != http.MethodGet && r.Method != http.MethodHead
		if mutating {
			fc.muts++
		}
		fc.mu.Unlock()

		p := r.URL.Path
		switch {
		case mutating:
			send(w, http.StatusOK, loadTable)
		case r.Method == http.MethodHead:
			if p == "/v1/namespaces/ns" || p == "/v1/namespaces/ns/tables/t" {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case p == "/v1/config":
			send(w, http.StatusOK, map[string]any{"defaults": map[string]any{}, "overrides": map[string]any{}})
		case p == "/v1/namespaces":
			send(w, http.StatusOK, map[string]any{"namespaces": [][]string{{"ns"}}})
		case p == "/v1/namespaces/ns":
			send(w, http.StatusOK, map[string]any{"namespace": []string{"ns"}, "properties": map[string]any{}})
		case p == "/v1/namespaces/ns/tables":
			send(w, http.StatusOK, map[string]any{"identifiers": []any{map[string]any{"namespace": []string{"ns"}, "name": "t"}}})
		case p == "/v1/namespaces/ns/tables/t":
			send(w, http.StatusOK, loadTable)
		default:
			send(w, http.StatusNotFound, map[string]any{"error": map[string]any{"message": "not found " + p, "type": "NoSuchTableException", "code": 404}})
		}
	}))
	t.Cleanup(fc.srv.Close)
	return fc
}

func (fc *fakeCatalog) mutations() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.muts
}

func (fc *fakeCatalog) requests() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return slices.Clone(fc.reqs)
}

func (fc *fakeCatalog) authorizations() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return slices.Clone(fc.auths)
}

// requireIcebergExtensions mirrors lockedEngineViaAttach's pre-flight: skip when
// iceberg/httpfs cannot load, fail under QE_REQUIRE_WARMED=1.
func requireIcebergExtensions(t *testing.T, e *engine) {
	t.Helper()
	for _, s := range []string{"INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs"} {
		if _, err := e.conn.ExecContext(context.Background(), s); err != nil {
			if os.Getenv("QE_REQUIRE_WARMED") == "1" {
				t.Fatalf("iceberg/httpfs unavailable (%s: %v); attached deny matrix needs them", s, err)
			}
			t.Skipf("iceberg/httpfs unavailable (%s: %v); attached deny matrix needs them", s, err)
		}
	}
}

// lockedEngineAttached reaches the exact production state against the fake
// catalog: openEngine → attachCatalog (which SUCCEEDS and runs USE lk."ns") →
// lock(). Returns the engine, the fake, and the engine's temp dir.
func lockedEngineAttached(t *testing.T) (*engine, *fakeCatalog, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	e, err := openEngine(ctx, Request{TempDir: dir})
	if err != nil {
		t.Fatalf("openEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	requireIcebergExtensions(t, e)

	fc := newFakeCatalog(t)
	if err := attachCatalog(ctx, e, Request{
		LakekeeperURL: fc.srv.URL,
		Warehouse:     "proj/wh",
		CatalogJWT:    dummyAttachJWT,
	}); err != nil {
		t.Fatalf("attachCatalog against the fake catalog: %v", err)
	}
	if err := e.lock(ctx); err != nil {
		t.Fatalf("lock: %v", err)
	}
	return e, fc, dir
}

func queryInt(t *testing.T, conn *sql.Conn, stmt string) int64 {
	t.Helper()
	var n int64
	if err := conn.QueryRowContext(context.Background(), stmt).Scan(&n); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	return n
}

var readOnlyDeny = regexp.MustCompile(`(?i)attached in read-only mode`)

// TestLockdownDenyAttachedReadOnly: each listed catalog write on the attached,
// locked engine must be refused by the READ_ONLY attach (or, for the re-ATTACH
// rows, by the dropped secret), with zero mutating requests reaching the catalog. Do not loosen the error predicate to make a row pass: a
// row that denies for another reason (bind error, 404) proves nothing about the
// read-only guard and belongs out of the matrix.
func TestLockdownDenyAttachedReadOnly(t *testing.T) {
	e, fc, dir := lockedEngineAttached(t)
	ctx := context.Background()

	var ro bool
	if err := e.conn.QueryRowContext(ctx,
		"SELECT readonly FROM duckdb_databases() WHERE database_name = 'lk'").Scan(&ro); err != nil {
		t.Fatalf("read lk readonly flag: %v", err)
	}
	if !ro {
		t.Fatal("SECURITY: catalog lk is attached read-write; attachCatalog must pass READ_ONLY")
	}

	// attachCatalog drops the bearer secret before lock(): post-lock user SQL
	// must find nothing to ATTACH the catalog again with.
	if n := queryInt(t, e.conn, "SELECT count(*) FROM duckdb_secrets() WHERE name = '"+catalogSecretName+"'"); n != 0 {
		t.Errorf("SECURITY: secret %s still exists post-lock (%d); attachCatalog must drop it", catalogSecretName, n)
	}

	// Positive controls: catalog reads and session-local writes stay usable, so
	// the denies below are the read-only guard, not a broken attach.
	postDrop := len(fc.requests())
	if n := queryInt(t, e.conn, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lk'"); n != 1 {
		t.Fatalf("duckdb_tables() on lk = %d, want 1", n)
	}
	if n := queryInt(t, e.conn, "SELECT count(*) FROM iceberg_snapshots('lk.ns.t')"); n != 2 {
		t.Fatalf("iceberg_snapshots('lk.ns.t') = %d rows, want 2", n)
	}
	// Dropping the secret must not drop the token from the live attachment: every
	// catalog request the reads above made (all after the drop) carries the bearer.
	reqs, auths := fc.requests()[postDrop:], fc.authorizations()[postDrop:]
	if len(reqs) == 0 {
		t.Fatal("positive-control reads issued no catalog request; the bearer check below would be vacuous")
	}
	for i, r := range reqs {
		if auths[i] != "Bearer "+dummyAttachJWT {
			t.Fatalf("post-drop catalog request %q carried Authorization %q, want the attach bearer", r, auths[i])
		}
	}
	t.Logf("post-drop catalog requests, all with the attach bearer: %s", strings.Join(reqs, ", "))
	if _, err := e.conn.ExecContext(ctx, "CREATE TEMP TABLE tt AS SELECT 1 AS a"); err != nil {
		t.Fatalf("CREATE TEMP TABLE: %v", err)
	}
	if n := queryInt(t, e.conn, "SELECT count(*) FROM tt"); n != 1 {
		t.Fatalf("temp table count = %d, want 1", n)
	}
	// The accepted generator path: dbgen aimed at the in-memory catalog writes to
	// RAM only, bounded by memory_limit / max_temp_directory_size / the timeout.
	if _, err := e.conn.ExecContext(ctx, "CALL dbgen(sf=0.001, catalog := 'memory', schema := 'main')"); err != nil {
		t.Fatalf("dbgen into memory: %v", err)
	}
	if n := queryInt(t, e.conn, "SELECT count(*) FROM memory.main.region"); n != 5 {
		t.Fatalf("memory.main.region = %d rows, want 5", n)
	}
	if m := fc.mutations(); m != 0 {
		t.Fatalf("positive controls issued %d mutating catalog request(s): %v", m, fc.requests())
	}

	// Re-ATTACH: lock_configuration does not block ATTACH, so with lk_tok stored a
	// user could attach the catalog again as lk2 WITHOUT READ_ONLY and write
	// through it. The ATTACH must fail for want of the dropped secret, and the
	// lk2 write after it must fail (any error) with zero mutations.
	//
	// Not a row: ATTACH ... (AUTHORIZATION_TYPE 'none') binds against this fake,
	// which serves unauthenticated clients. Real lakekeeper rejects them (401), and
	// user SQL cannot read the run's JWT (allow_unredacted_secrets=false, and after
	// the drop the secret no longer exists). A user who already holds their own
	// valid catalog JWT could attach it writable, but they already have that
	// lakekeeper access directly and FGA governs it — out of scope for the engine.
	// iceberg_truncate / iceberg_bucket are absent on purpose: on 2.0.0 they are
	// SCALAR partition transforms (width, value), not catalog writes.
	noSecret := regexp.MustCompile(`(?i)no iceberg secret by the name of 'lk_tok'`)
	anyErr := regexp.MustCompile(`.`)
	deny := []struct {
		name, stmt string
		want       *regexp.Regexp // nil = readOnlyDeny
	}{
		{"reattach-with-stored-secret", "ATTACH 'proj/wh' AS lk2 (TYPE ICEBERG, URI '" + fc.srv.URL + "', SECRET lk_tok, ACCESS_DELEGATION_MODE 'vended_credentials')", noSecret},
		{"reattach-write", "CREATE SCHEMA lk2.evil", anyErr},
		{"dbgen-current-catalog", "CALL dbgen(sf=0.001)", nil},
		{"dbgen-catalog-lk", "CALL dbgen(sf=0.001, catalog := 'lk', schema := 'ns')", nil},
		{"dsdgen-current-catalog", "CALL dsdgen(sf=0.001)", nil},
		{"iceberg_rollback_to_snapshot", "SELECT * FROM iceberg_rollback_to_snapshot('lk.ns.t', 1001)", nil},
		{"set_iceberg_table_properties", "CALL set_iceberg_table_properties('lk.ns.t', MAP {'k': 'v'})", nil},
		{"remove_iceberg_table_properties", "CALL remove_iceberg_table_properties('lk.ns.t', ['k'])", nil},
		{"set_iceberg_schema_properties", "CALL set_iceberg_schema_properties('lk.ns', MAP {'k': 'v'})", nil},
		{"remove_iceberg_schema_properties", "CALL remove_iceberg_schema_properties('lk.ns', ['k'])", nil},
		{"insert", "INSERT INTO lk.ns.t VALUES (1)", nil},
		{"create-table", "CREATE TABLE lk.ns.x (a INT)", nil},
		{"ctas-bare-name", "CREATE TABLE y AS SELECT 1 AS a", nil},
		{"drop-table", "DROP TABLE lk.ns.t", nil},
		{"create-schema", "CREATE SCHEMA lk.newns", nil},
		{"alter-table", "ALTER TABLE lk.ns.t ADD COLUMN b INT", nil},
	}
	for _, p := range deny {
		t.Run(p.name, func(t *testing.T) {
			before := fc.mutations()
			_, err := e.conn.ExecContext(ctx, p.stmt)
			if err == nil {
				t.Fatalf("SECURITY HOLE: %q SUCCEEDED on the read-only attached catalog.\n  stmt: %s", p.name, p.stmt)
			}
			t.Logf("denied: %v", err)
			want := p.want
			if want == nil {
				want = readOnlyDeny
			}
			if !want.MatchString(err.Error()) {
				t.Fatalf("%q must be refused with %s, got a different error: %v", p.name, want, err)
			}
			if after := fc.mutations(); after != before {
				t.Fatalf("SECURITY: %q issued %d mutating catalog request(s) before erroring", p.name, after-before)
			}
		})
	}

	if m := fc.mutations(); m != 0 {
		t.Fatalf("SECURITY: %d mutating catalog request(s) reached the catalog: %v", m, fc.requests())
	}
	reqs = fc.requests()
	paths := []string{}
	for _, r := range reqs {
		if !slices.Contains(paths, r) {
			paths = append(paths, r)
		}
	}
	t.Logf("catalog saw %d request(s), all reads; distinct: %s", len(reqs), strings.Join(paths, ", "))
	assertNoFilesCreated(t, dir)
}

// TestLockdownDenyAttachedReadOnlyHarnessDetectsWrites is the negative control:
// the same fake, the same lock(), only READ_ONLY dropped from the ATTACH — the
// write must reach the catalog, proving the matrix above cannot pass vacuously.
func TestLockdownDenyAttachedReadOnlyHarnessDetectsWrites(t *testing.T) {
	ctx := context.Background()
	e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("openEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	requireIcebergExtensions(t, e)

	fc := newFakeCatalog(t)
	for _, s := range []string{
		"CREATE OR REPLACE SECRET lk_tok (TYPE ICEBERG, TOKEN '" + dummyAttachJWT + "')",
		"ATTACH 'proj/wh' AS lk (TYPE ICEBERG, URI '" + fc.srv.URL + "', SECRET lk_tok, ACCESS_DELEGATION_MODE 'vended_credentials')",
		`USE lk."ns"`,
	} {
		if _, err := e.conn.ExecContext(ctx, s); err != nil {
			t.Fatalf("pre-lock %q: %v", strings.SplitN(s, " (", 2)[0], err)
		}
	}
	if err := e.lock(ctx); err != nil {
		t.Fatalf("lock: %v", err)
	}

	_, err = e.conn.ExecContext(ctx, "CREATE SCHEMA lk.newns")
	t.Logf("CREATE SCHEMA lk.newns on the read-write attach: err=%v", err)
	if fc.mutations() < 1 || !slices.Contains(fc.requests(), "POST /v1/namespaces") {
		t.Fatalf("harness is blind: a read-write attach must reach POST /v1/namespaces, saw %v", fc.requests())
	}
}
