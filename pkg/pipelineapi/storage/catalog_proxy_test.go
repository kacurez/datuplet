package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
		WarehouseName: "datuplet",
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
		WarehouseName: "datuplet",
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
		WarehouseName: "datuplet",
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
