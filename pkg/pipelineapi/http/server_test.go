package http_test

import (
	"context"
	"encoding/json"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/storage"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

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
	srv := apihttp.NewServer(nil).
		WithStorage(svc).
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
