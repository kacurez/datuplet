package http_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/queryproxy"
	"github.com/datuplet/datuplet/pkg/pipelineapi/storage"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// stubQueryHandler builds a pre-built http.Handler for the query service,
// pointed at the given worker URL. Uses the signer to mint query-scoped JWTs.
func stubQueryHandler(t *testing.T, workerURL string, signer *tokens.Signer) stdhttp.Handler {
	t.Helper()
	h, err := queryproxy.Handler(queryproxy.Config{
		WorkerURL: workerURL,
		Warehouse: "test-project/test-warehouse",
	}, signer)
	if err != nil {
		t.Fatalf("queryproxy.Handler: %v", err)
	}
	return h
}

// stubResolver always resolves to a fixed user. Used by the storage
// route-wiring tests — we only need to prove the route is reachable,
// not that it returns a 2xx.
type stubResolver struct{}

func (stubResolver) UserFor(_ stdhttp.ResponseWriter, _ *stdhttp.Request) (*store.User, bool, error) {
	return &store.User{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001")}, true, nil
}
func (stubResolver) Mode() string        { return "test" }
func (stubResolver) SupportsLogin() bool { return false }

func TestHealthz(t *testing.T) {
	srv := apihttp.NewServer(nil) // nil DB is fine for /healthz
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body.status = %q, want ok", body["status"])
	}
}

// TestServer_StorageRoute_SoftDegrade asserts that a pipeline-api built
// without WithStorage returns 503 on /api/v1/storage/*. The path doesn't
// 404 because the server registers a catch-all handler under
// /api/v1/storage/.
func TestServer_StorageRoute_SoftDegrade(t *testing.T) {
	srv := apihttp.NewServer(nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/storage/projects/anything/tables")
	if err != nil {
		t.Fatalf("GET /api/v1/storage/...: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

// TestServer_StorageRoute_WithServiceWired asserts that once a
// storage.Service is wired via WithStorage AND the auth seam
// (resolver + authzr) is present, the route is registered and no
// longer returns the catch-all 503. We wire a LakekeeperProjectIDFor
// stub that returns a synthetic project ID and an empty fake Authorizer
// (no tuples), so the handler returns 403 — confirming the real handler
// is executing, not the catch-all.
func TestServer_StorageRoute_WithServiceWired(t *testing.T) {
	const syntheticLKID = "bbbbbbbb-0000-0000-0000-000000000002"
	dir := t.TempDir()
	svc := &storage.Service{
		WarehouseURI: "file://" + filepath.Join(dir, "nonexistent"),
		OrgName:      "myorg",
		AllowLocal:   true,
		LakekeeperProjectIDFor: func(_ context.Context, _ uuid.UUID) (string, error) {
			return syntheticLKID, nil
		},
	}
	// Empty fake — Check returns (false, nil) → 403 from the handler.
	fakeAuthz := authztest.New()
	// The gate is built from the same stubs the Service already carries
	// (LakekeeperProjectIDFor) plus the same authzr wired via WithAuthorizer
	// — resolveProject now delegates to h.Gate instead of calling
	// h.Svc.LakekeeperProjectIDFor / h.Authorizer directly.
	gate := &projectgate.Gate{
		LakekeeperProjectIDFor: svc.LakekeeperProjectIDFor,
		Authorizer:             fakeAuthz,
	}
	srv := apihttp.NewServer(nil).
		WithStorage(svc).
		WithProjectGate(gate).
		WithUserResolver(stubResolver{}).
		WithAuthorizer(fakeAuthz)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/storage/projects/00000000-0000-0000-0000-000000000002/tables")
	if err != nil {
		t.Fatalf("GET /api/v1/storage/...: %v", err)
	}
	defer resp.Body.Close()
	// 403 proves the real handler ran (the catch-all returns 503 with a
	// plain-text body containing "not configured").
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "not configured") {
		t.Errorf("storage route returned the catch-all 503 — WithStorage+WithAuthorizer should register the real handler")
	}
	if resp.StatusCode != stdhttp.StatusForbidden {
		t.Errorf("status = %d, want 403 (real handler, empty authz fake denies)", resp.StatusCode)
	}
}

// TestServer_QueryRoute_NotConfigured asserts that POST /api/v1/query
// returns 404 when the query service is not wired.
func TestServer_QueryRoute_NotConfigured(t *testing.T) {
	srv := apihttp.NewServer(nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Post(ts.URL+"/api/v1/query", "application/json", strings.NewReader(`{"sql":"SELECT 1"}`))
	if err != nil {
		t.Fatalf("POST /api/v1/query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestServer_QueryRoute_WithServiceWired asserts that POST /api/v1/query is
// registered behind auth.WithUser when a pre-built query handler + resolver
// are wired. It exercises the full chain: route → auth.WithUser → queryproxy
// → fake worker.
func TestServer_QueryRoute_WithServiceWired(t *testing.T) {
	// Fixed result JSON the fake worker returns verbatim.
	const resultJSON = `{"schema":[{"name":"a","type":"INTEGER"}],"rows":[[1]],"truncated":false,"stats":{"duration_ms":3}}`

	// Spin up a fake query worker that returns a valid 200 result.
	worker := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(stdhttp.StatusOK)
		_, _ = w.Write([]byte(resultJSON))
	}))
	defer worker.Close()

	// Build the signer and the pre-built query handler at config time (mirrors
	// how runServeCluster wires it in main.go).
	signer := testSignerAndKey(t)
	qh := stubQueryHandler(t, worker.URL, signer)

	// Use a configurable resolver so we can test both auth states.
	resolver := &selectiveResolver{allow: false}

	srv := apihttp.NewServer(nil).
		WithSigner(signer).
		WithQueryService(qh).
		WithUserResolver(resolver)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Test 1: Unauthenticated request.
	// auth.WithUser (middleware.go) always returns exactly 401 when the resolver
	// returns (nil, false, nil) — it writes writeJSONError(w, 401, "not authenticated")
	// regardless of resolver Mode. The selectiveResolver.Mode() returns "test" which
	// does not trigger any redirect path.
	resp, err := stdhttp.Post(ts.URL+"/api/v1/query", "application/json", strings.NewReader(`{"sql":"SELECT 1"}`))
	if err != nil {
		t.Fatalf("POST /api/v1/query (unauthenticated): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusUnauthorized {
		t.Errorf("unauthenticated: status = %d, want 401 (auth.WithUser always returns 401 on (nil,false,nil))", resp.StatusCode)
	}

	// Test 2: Authenticated request — full chain: route → auth → queryproxy → fake worker.
	resolver.allow = true
	resp2, err := stdhttp.Post(ts.URL+"/api/v1/query", "application/json", strings.NewReader(`{"sql":"SELECT 1"}`))
	if err != nil {
		t.Fatalf("POST /api/v1/query (authenticated): %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != stdhttp.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("authenticated: status = %d, want 200; body=%s", resp2.StatusCode, body)
	}
	got, _ := io.ReadAll(resp2.Body)
	if strings.TrimRight(string(got), "\n") != resultJSON {
		t.Errorf("authenticated body = %q, want %q", strings.TrimRight(string(got), "\n"), resultJSON)
	}
}

// testSignerAndKey builds a Signer for testing. Returns just the signer.
func testSignerAndKey(t *testing.T) *tokens.Signer {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	dir := t.TempDir()
	privDER, _ := x509.MarshalPKCS8PrivateKey(priv)
	privPath := filepath.Join(dir, "priv.pem")
	_ = os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o400)
	signer, err := tokens.LoadPrivateKeyFromPEMFile(privPath, "test-kid")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return signer
}

// selectiveResolver is a test resolver that can be configured to allow or
// deny authentication. Used for testing auth middleware behavior.
type selectiveResolver struct {
	allow bool
}

func (sr *selectiveResolver) UserFor(_ stdhttp.ResponseWriter, _ *stdhttp.Request) (*store.User, bool, error) {
	if sr.allow {
		return &store.User{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001")}, true, nil
	}
	return nil, false, nil
}

func (sr *selectiveResolver) Mode() string        { return "test" }
func (sr *selectiveResolver) SupportsLogin() bool { return false }

