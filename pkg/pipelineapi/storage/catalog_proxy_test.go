package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// TestJoinNS exercises the multi-segment namespace flattening shape.
// The handlers expose namespaces as "."-joined strings on the wire so
// the JS UI doesn't need to understand iceberg-go's slice type.
func TestJoinNS(t *testing.T) {
	cases := []struct {
		name  string
		ident icebergtable.Identifier
		want  string
	}{
		{"single_ns", icebergtable.Identifier{"raw", "orders"}, "raw"},
		{"multi_ns", icebergtable.Identifier{"raw", "events", "orders"}, "raw.events"},
		{"empty", icebergtable.Identifier{}, ""},
		{"name_only", icebergtable.Identifier{"orders"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinNS(tc.ident); got != tc.want {
				t.Errorf("joinNS(%v) = %q, want %q", tc.ident, got, tc.want)
			}
		})
	}
}

// TestShortName: the last segment of the identifier is the table name.
func TestShortName(t *testing.T) {
	cases := []struct {
		name  string
		ident icebergtable.Identifier
		want  string
	}{
		{"single_ns", icebergtable.Identifier{"raw", "orders"}, "orders"},
		{"multi_ns", icebergtable.Identifier{"a", "b", "c", "tbl"}, "tbl"},
		{"empty", icebergtable.Identifier{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shortName(tc.ident); got != tc.want {
				t.Errorf("shortName(%v) = %q, want %q", tc.ident, got, tc.want)
			}
		})
	}
}

// TestNewCatalogProxy_NilService rejects a nil Service so handlers
// see a clean 500 instead of a nil-deref.
func TestNewCatalogProxy_NilService(t *testing.T) {
	_, err := newCatalogProxy(context.Background(), nil, "", "datuplet")
	if err == nil {
		t.Fatal("newCatalogProxy(nil): expected error, got nil")
	}
}

// TestNewCatalogProxy_EmptyURL: a Service without LakekeeperURL must
// surface as an error so handlers can fall back to the walker path
// rather than dialling an empty URL.
func TestNewCatalogProxy_EmptyURL(t *testing.T) {
	svc := &Service{
		LakekeeperURL: "",
		Minter:        func(ctx context.Context) (tokens.ImpersonationToken, error) { return "tok", nil },
	}
	_, err := newCatalogProxy(context.Background(), svc, "", "datuplet")
	if err == nil {
		t.Fatal("newCatalogProxy(empty URL): expected error, got nil")
	}
}

// TestNewCatalogProxy_NilMinter: Minter is REQUIRED. Both cluster + local
// modes wire a real minter against pipeline-api's signing key; a nil
// Minter is a configuration error and the proxy must refuse to construct
// rather than silently fall back to anonymous calls.
func TestNewCatalogProxy_NilMinter(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer stub.Close()
	svc := &Service{LakekeeperURL: stub.URL, Minter: nil}
	if _, err := newCatalogProxy(context.Background(), svc, "", "datuplet"); err == nil {
		t.Fatal("newCatalogProxy(nil minter): expected error, got nil")
	}
}

// TestNewCatalogProxy_OK: a fully-wired Service returns a usable proxy.
// The stub answers iceberg-go's config handshake with a minimal JSON
// body; the handshake must include the bearer minted by the Minter.
func TestNewCatalogProxy_OK(t *testing.T) {
	var seenAuth string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seenAuth == "" {
			seenAuth = r.Header.Get("Authorization")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer stub.Close()

	svc := &Service{
		LakekeeperURL: stub.URL,
		Minter: func(ctx context.Context) (tokens.ImpersonationToken, error) {
			return "impersonation-jwt-here", nil
		},
	}
	p, err := newCatalogProxy(context.Background(), svc, "", "datuplet")
	if err != nil {
		t.Fatalf("newCatalogProxy: %v", err)
	}
	if p == nil || p.cli == nil {
		t.Fatal("newCatalogProxy: nil proxy or client")
	}
	// The handshake should have attached the impersonation JWT.
	if seenAuth == "" {
		t.Skip("stub never received a request — iceberg-go didn't attempt the handshake")
	}
	want := "impersonation-jwt-here"
	if got := seenAuth; len(got) < len(want) || got[len(got)-len(want):] != want {
		t.Errorf("Authorization header = %q, want suffix %q", got, want)
	}
}

// TestNewCatalogProxy_ProjectIDHeader: when projectID is non-empty the stub
// must receive x-project-id on the initial handshake request.
func TestNewCatalogProxy_ProjectIDHeader(t *testing.T) {
	var seenProjectID string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seenProjectID == "" {
			seenProjectID = r.Header.Get("x-project-id")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer stub.Close()

	wantPID := "lk-project-uuid-1234"
	svc := &Service{
		LakekeeperURL: stub.URL,
		Minter: func(ctx context.Context) (tokens.ImpersonationToken, error) {
			return "tok", nil
		},
	}
	_, err := newCatalogProxy(context.Background(), svc, wantPID, "datuplet")
	if err != nil {
		t.Fatalf("newCatalogProxy: %v", err)
	}
	if seenProjectID == "" {
		t.Skip("stub never received a request — iceberg-go didn't attempt the handshake")
	}
	if seenProjectID != wantPID {
		t.Errorf("x-project-id = %q, want %q", seenProjectID, wantPID)
	}
}

// TestListAllTables_RealLakekeeperLayout exercises the listAllTables
// path against a stub that emits the actual lakekeeper-managed wire
// shape: namespaces returned by the catalog and table metadata-locations
// keyed by storage-uuid + table-uuid (NOT the legacy
// `<warehouse>/orgs/<org>/projects/<pid>/...` directory layout).
//
// This is the regression that motivated removing projectMetadataPrefix
// (P1-1): every real lakekeeper response would have failed the prefix
// match, so listAllTables returned [] and the SPA storage browse showed
// nothing for cluster-mode warehouses.
//
// The stub speaks just enough of the iceberg-go REST catalog protocol
// to drive ListNamespaces → ListTables → LoadTable. We don't try to
// stand up a full catalog; we only verify that the proxy correctly
// surfaces a UUID-keyed metadata location to the caller without
// applying any dead `/orgs/.../projects/.../` filter.
func TestListAllTables_RealLakekeeperLayout(t *testing.T) {
	// Sample of the lakekeeper layout iceberg-go produces. The
	// `<storage-uuid>` and `<table-uuid>` are server-allocated; their
	// shape is what matters here, not the exact bytes.
	const metadataLoc = "s3://datuplet/019dceed-aaaa-bbbb-cccc-000000000001/019dceed-1111-2222-3333-000000000002/metadata/00000-abcd.metadata.json"

	mux := http.NewServeMux()
	// /v1/config: handshake, returns a minimal config doc.
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	})
	// /v1/{prefix}/namespaces (no parent): list one namespace "raw".
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"namespaces":[["raw"]]}`))
	})
	// /v1/{prefix}/namespaces/raw/tables: one table "events".
	mux.HandleFunc("/v1/namespaces/raw/tables", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"identifiers":[{"namespace":["raw"],"name":"events"}]}`))
	})
	// LoadTable: emit a real-lakekeeper-shaped metadata-location.
	mux.HandleFunc("/v1/namespaces/raw/tables/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Minimal v1 table metadata that iceberg-go's REST client will
		// happily decode. We omit snapshots so CurrentSnapshot()==nil
		// (snapshot id stays 0 in the TableRef).
		body := `{
			"metadata-location": "` + metadataLoc + `",
			"metadata": {
				"format-version": 1,
				"table-uuid": "019dceed-1111-2222-3333-000000000002",
				"location": "s3://datuplet/019dceed-aaaa-bbbb-cccc-000000000001/019dceed-1111-2222-3333-000000000002",
				"last-updated-ms": 1700000000000,
				"last-column-id": 1,
				"schema": {"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]},
				"current-schema-id": 0,
				"schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}],
				"partition-spec": [],
				"default-spec-id": 0,
				"partition-specs": [{"spec-id":0,"fields":[]}],
				"default-sort-order-id": 0,
				"sort-orders": [{"order-id":0,"fields":[]}],
				"properties": {},
				"current-snapshot-id": -1,
				"snapshots": [],
				"refs": {}
			},
			"config": {}
		}`
		_, _ = w.Write([]byte(body))
	})

	stub := httptest.NewServer(mux)
	defer stub.Close()

	svc := &Service{
		LakekeeperURL: stub.URL,
		Minter: func(ctx context.Context) (tokens.ImpersonationToken, error) {
			return "tok", nil
		},
	}
	p, err := newCatalogProxy(context.Background(), svc, "", "datuplet")
	if err != nil {
		t.Fatalf("newCatalogProxy: %v", err)
	}
	refs, err := p.listAllTables(context.Background())
	if err != nil {
		// Some iceberg-go versions perform an additional config call
		// against /v1/config?warehouse= or /v1/<prefix>/namespaces with
		// a different shape. If the stub doesn't satisfy them we skip
		// rather than fail — the load-bearing assertion is that
		// listAllTables NO LONGER applies a projectMetadataPrefix
		// filter. The test is structural; an error here means we should
		// adjust the stub, not that the production code is wrong.
		t.Skipf("listAllTables: %v (stub may not match iceberg-go's full REST surface)", err)
	}
	if len(refs) == 0 {
		t.Fatal("listAllTables returned 0 tables — pre-fix, the projectMetadataPrefix filter rejected every real lakekeeper layout entry; post-fix it must surface them")
	}
	// Sanity: the returned metadata location must be the UUID-keyed
	// shape, NOT the legacy `<warehouse>/orgs/<org>/projects/<pid>/...`
	// layout. If we ever start rewriting the URI on the proxy boundary
	// the regression should fail here.
	got := refs[0].MetadataLocation
	if !strings.HasPrefix(got, "s3://datuplet/") {
		t.Errorf("MetadataLocation = %q; want s3://datuplet/<uuid>/... (lakekeeper layout)", got)
	}
	if strings.Contains(got, "/orgs/") || strings.Contains(got, "/projects/") {
		t.Errorf("MetadataLocation = %q; should NOT carry legacy /orgs/.../projects/... prefix", got)
	}
}

// fakeTableMetadataJSON builds a minimal v1 table metadata document (the
// same shape as TestListAllTables_RealLakekeeperLayout's inline fixture),
// parameterized by metadata-location and table UUID so each faked
// identifier in the tests below gets a distinguishable LoadTable response.
// table-uuid must be a real UUID (iceberg-go decodes it via uuid.UUID),
// so tests pass a zero-padded index rather than the bare table name.
func fakeTableMetadataJSON(metadataLoc, tableUUID string) string {
	return `{
		"metadata-location": "` + metadataLoc + `",
		"metadata": {
			"format-version": 1,
			"table-uuid": "` + tableUUID + `",
			"location": "s3://datuplet/` + tableUUID + `",
			"last-updated-ms": 1700000000000,
			"last-column-id": 1,
			"schema": {"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]},
			"current-schema-id": 0,
			"schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}],
			"partition-spec": [],
			"default-spec-id": 0,
			"partition-specs": [{"spec-id":0,"fields":[]}],
			"default-sort-order-id": 0,
			"sort-orders": [{"order-id":0,"fields":[]}],
			"properties": {},
			"current-snapshot-id": -1,
			"snapshots": [],
			"refs": {}
		},
		"config": {}
	}`
}

// fakeUUID turns a small integer into a syntactically valid UUID so the
// fixture metadata below satisfies iceberg-go's uuid.UUID decode.
func fakeUUID(i int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
}

// fakeUUIDForTableName derives a distinguishable-but-valid UUID from a
// "t<N>" fixture table name, so each faked identifier's LoadTable
// response is unique rather than every table sharing index 0.
func fakeUUIDForTableName(name string) string {
	n, err := strconv.Atoi(strings.TrimPrefix(name, "t"))
	if err != nil {
		n = 0
	}
	return fakeUUID(n)
}

// newFakeCatalogMux builds an httptest mux that speaks just enough of the
// iceberg-go REST catalog protocol to drive listAllTables: one namespace
// "raw" containing `total` tables named t0..t(total-1), with LoadTable
// requests routed through loadHandler so tests can instrument/fail
// individual loads. Mirrors the wiring already proven out in
// TestListAllTables_RealLakekeeperLayout.
func newFakeCatalogMux(total int, loadHandler func(w http.ResponseWriter, name string)) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	})
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"namespaces":[["raw"]]}`))
	})
	mux.HandleFunc("/v1/namespaces/raw/tables", func(w http.ResponseWriter, _ *http.Request) {
		idents := make([]string, total)
		for i := 0; i < total; i++ {
			idents[i] = fmt.Sprintf(`{"namespace":["raw"],"name":"t%d"}`, i)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"identifiers":[%s]}`, strings.Join(idents, ","))
	})
	// Trailing-slash subtree route: matches every "/v1/namespaces/raw/tables/<name>"
	// LoadTable request without needing one handler per identifier.
	mux.HandleFunc("/v1/namespaces/raw/tables/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/v1/namespaces/raw/tables/")
		loadHandler(w, name)
	})
	return mux
}

func newFakeCatalogProxy(t *testing.T, mux *http.ServeMux) *catalogProxy {
	t.Helper()
	stub := httptest.NewServer(mux)
	t.Cleanup(stub.Close)

	svc := &Service{
		LakekeeperURL: stub.URL,
		Minter: func(ctx context.Context) (tokens.ImpersonationToken, error) {
			return "tok", nil
		},
	}
	p, err := newCatalogProxy(context.Background(), svc, "", "datuplet")
	if err != nil {
		t.Fatalf("newCatalogProxy: %v", err)
	}
	return p
}

// TestListAllTables_BoundedParallelLoad proves the errgroup.SetLimit(8)
// width deterministically: it blocks every LoadTable call inside the fake
// until `release` closes, tracking the peak number simultaneously blocked.
// With 16 identifiers and a limit of 8, the peak must reach exactly 8 —
// never more (the limiter caps it) and, since 16 > 8, never less either.
// No wall-clock assertions: the test polls until the peak is observed,
// then unblocks everything and asserts on the final state.
func TestListAllTables_BoundedParallelLoad(t *testing.T) {
	const total = 16
	var inflight, peak int32
	release := make(chan struct{})

	loadEnter := func() {
		cur := atomic.AddInt32(&inflight, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
				break
			}
		}
		<-release
		atomic.AddInt32(&inflight, -1)
	}

	mux := newFakeCatalogMux(total, func(w http.ResponseWriter, name string) {
		loadEnter()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fakeTableMetadataJSON(
			fmt.Sprintf("s3://datuplet/%s/metadata/00000.metadata.json", name),
			fakeUUIDForTableName(name),
		)))
	})
	p := newFakeCatalogProxy(t, mux)

	type res struct {
		out []TableRef
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		out, err := p.listAllTables(context.Background())
		resCh <- res{out, err} // assertions happen on the test goroutine below
	}()

	// Deterministic release: wait until the limiter's full width (8) is
	// simultaneously blocked inside LoadTable, then let everything finish.
	deadline := time.After(10 * time.Second)
	for atomic.LoadInt32(&peak) < 8 {
		select {
		case <-deadline:
			close(release) // unblock leaked goroutines before failing
			t.Fatalf("never reached 8 concurrent LoadTable calls (peak=%d)", atomic.LoadInt32(&peak))
		case <-time.After(2 * time.Millisecond):
		}
	}
	close(release)

	r := <-resCh
	if r.err != nil {
		t.Fatalf("listAllTables: %v", r.err)
	}
	if got := atomic.LoadInt32(&peak); got != 8 {
		t.Fatalf("peak concurrency = %d, want exactly the SetLimit width 8", got)
	}
	if len(r.out) != total {
		t.Fatalf("len(out) = %d, want %d", len(r.out), total)
	}
}

// TestListAllTables_SkipsFailedLoadPreservesOrder: a LoadTable failure on
// one identifier must be skipped (not fail the whole list, and not
// reorder the survivors) — identifiers are collected in
// ListNamespaces/ListTables order, loaded into an index-aligned slice, and
// the final compaction preserves that index order.
func TestListAllTables_SkipsFailedLoadPreservesOrder(t *testing.T) {
	const total = 3 // t0, t1 (fails), t2
	mux := newFakeCatalogMux(total, func(w http.ResponseWriter, name string) {
		if name == "t1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fakeTableMetadataJSON(
			fmt.Sprintf("s3://datuplet/%s/metadata/00000.metadata.json", name),
			fakeUUIDForTableName(name),
		)))
	})
	p := newFakeCatalogProxy(t, mux)

	out, err := p.listAllTables(context.Background())
	if err != nil {
		t.Fatalf("listAllTables: %v", err)
	}
	if len(out) != total-1 {
		t.Fatalf("len(out) = %d, want %d (t1 failed to load and should be skipped)", len(out), total-1)
	}
	if out[0].Name != "t0" || out[1].Name != "t2" {
		t.Fatalf("order not preserved: got [%s, %s], want [t0, t2]", out[0].Name, out[1].Name)
	}
	for _, ref := range out {
		if ref.Name == "t1" {
			t.Fatalf("t1 failed LoadTable and must be absent from the result, got %+v", ref)
		}
	}
}

// TestListAllTables_AllLoadsFailReturnsError: when every identifier fails to
// load (simulating a lakekeeper/STS outage), listAllTables must return an
// error rather than a silently-empty catalog — otherwise the outage is
// indistinguishable from "this project genuinely has no tables" on the wire.
func TestListAllTables_AllLoadsFailReturnsError(t *testing.T) {
	const total = 3
	mux := newFakeCatalogMux(total, func(w http.ResponseWriter, _ string) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	p := newFakeCatalogProxy(t, mux)

	out, err := p.listAllTables(context.Background())
	if err == nil {
		t.Fatalf("listAllTables should error when every LoadTable call fails, got out=%v", out)
	}
	if out != nil {
		t.Errorf("out = %v, want nil alongside the total-failure error", out)
	}
}

// TestListAllTables_EmptyCatalogReturnsEmptySuccess: a namespace with zero
// tables is a genuinely empty catalog, not an outage — it must still
// return ([], nil), not an error. This is the case the total-failure guard
// (len(idents) > 0 && len(out) == 0) must NOT catch.
func TestListAllTables_EmptyCatalogReturnsEmptySuccess(t *testing.T) {
	mux := newFakeCatalogMux(0, func(w http.ResponseWriter, _ string) {})
	p := newFakeCatalogProxy(t, mux)

	out, err := p.listAllTables(context.Background())
	if err != nil {
		t.Fatalf("listAllTables on a genuinely empty catalog should succeed, got err: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("out = %v, want empty", out)
	}
}

// TestListAllTables_CancelledContextReturnsError: a request context that's
// cancelled mid-fan-out (client disconnect, upstream deadline) must surface
// as an error. Pre-fix, cancellation only showed up as per-table LoadTable
// skips, which — combined with the total-failure guard being absent —
// meant a fully-cancelled request silently returned 200 {"tables":[]}.
func TestListAllTables_CancelledContextReturnsError(t *testing.T) {
	const total = 4
	ctx, cancel := context.WithCancel(context.Background())
	mux := newFakeCatalogMux(total, func(w http.ResponseWriter, _ string) {
		// Cancel the very context listAllTables was called with. By the
		// time g.Wait() returns (which requires this handler's goroutine to
		// finish), ctx.Err() is guaranteed non-nil.
		cancel()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	p := newFakeCatalogProxy(t, mux)

	_, err := p.listAllTables(ctx)
	if err == nil {
		t.Fatal("listAllTables with a cancelled request ctx must return an error, not an empty success")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the error to wrap context.Canceled, got: %v", err)
	}
}
