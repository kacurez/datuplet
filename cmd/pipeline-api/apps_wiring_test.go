package main

// apps_wiring_test.go proves the RFC 028 control plane is actually reachable in
// a real deployment. P2 and P3 deliberately left both route blocks unwired
// because apps.NewIdentityManager()'s methods panicked; P4 fills those in and
// owns the last mile. Without the wiring every author route 404s in production
// and app-worker cannot be exercised end to end — so it is asserted here, not
// assumed, against the SAME function runServeCluster calls.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/apps"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// wiringResolver authenticates every request, so an author route that answers
// 403 (the empty authz fake denies) proves the real handler ran rather than the
// mux 404ing on an unregistered pattern.
type wiringResolver struct{}

func (wiringResolver) UserFor(http.ResponseWriter, *http.Request) (*store.User, bool, error) {
	return &store.User{ID: uuid.New()}, true, nil
}
func (wiringResolver) Mode() string        { return "test" }
func (wiringResolver) SupportsLogin() bool { return false }

type wiringProjects struct{}

func (wiringProjects) ListForUser(context.Context, uuid.UUID) ([]apihttp.ProjectView, error) {
	return nil, nil
}
func (wiringProjects) GetByID(_ context.Context, id uuid.UUID) (*apihttp.ProjectView, error) {
	return &apihttp.ProjectView{ID: id, LakekeeperProjectID: "lk-" + id.String()}, nil
}

// baseServer is the minimum wiring the app route blocks' nil-gates require
// (resolver + authorizer + project reader), with a nil pool — every assertion
// below is answered by a gate that runs before any query.
func baseServer() *apihttp.Server {
	return apihttp.NewServer(nil).
		WithUserResolver(wiringResolver{}).
		WithAuthorizer(authztest.New()).
		WithProjectReader(wiringProjects{})
}

// TestWireUserApps_RegistersAuthorRoutes: with the apps deps attached, the
// author routes must be REGISTERED (403 from the authz gate, never 404).
func TestWireUserApps_RegistersAuthorRoutes(t *testing.T) {
	srv, err := wireUserApps(baseServer(), nil, authztest.New(), nil, wiringProjects{})
	if err != nil {
		t.Fatalf("wireUserApps: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	pid := uuid.NewString()
	for _, path := range []string{
		"/api/v1/projects/" + pid + "/apps",
		"/api/v1/projects/" + pid + "/apps/dash1",
		"/api/v1/projects/" + pid + "/apps/dash1/logs",
	} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			t.Errorf("GET %s: 404 — the author routes are NOT wired into main.go", path)
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s: status = %d, want 403 (handler ran, authz fake denies)", path, resp.StatusCode)
		}
	}
}

// TestWireUserApps_RegistersInternalRoutesWithToken: with the service-token
// file present, the six /internal/v1/* routes must be registered and gated —
// 401 (credential missing on the request), never 404.
func TestWireUserApps_RegistersInternalRoutesWithToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("service-credential-for-app-worker\n"), 0o400); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	t.Setenv(apps.ServiceTokenFileEnv, path)

	srv, err := wireUserApps(baseServer(), nil, authztest.New(), nil, wiringProjects{})
	if err != nil {
		t.Fatalf("wireUserApps: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/internal/v1/impersonate", "application/json",
		nil)
	if err != nil {
		t.Fatalf("POST /internal/v1/impersonate: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("POST /internal/v1/impersonate: 404 — the internal routes are NOT wired into main.go")
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (the service-credential gate ran)", resp.StatusCode)
	}
}

// TestWireUserApps_NoTokenLeavesInternalRoutesUnregistered: an unset env var is
// NOT an error (P3's contract) — it just leaves the internal surface off.
func TestWireUserApps_NoTokenLeavesInternalRoutesUnregistered(t *testing.T) {
	t.Setenv(apps.ServiceTokenFileEnv, "")

	srv, err := wireUserApps(baseServer(), nil, authztest.New(), nil, wiringProjects{})
	if err != nil {
		t.Fatalf("wireUserApps with no service token must not fail: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/internal/v1/impersonate", "application/json", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (no credential ⇒ no internal surface)", resp.StatusCode)
	}
}

// TestWireUserApps_UnreadableOrEmptyTokenFileIsABootError: a misconfigured
// Secret must fail boot loudly. Silently skipping would leave app-worker
// staring at 404s; accepting an empty file would be worse still (P3's
// LoadServiceToken rejects it so "every bearer works" is impossible).
func TestWireUserApps_UnreadableOrEmptyTokenFileIsABootError(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		t.Setenv(apps.ServiceTokenFileEnv, filepath.Join(t.TempDir(), "does-not-exist"))
		if _, err := wireUserApps(baseServer(), nil, authztest.New(), nil, wiringProjects{}); err == nil {
			t.Fatal("an unreadable service-token file must be a boot error, not a silent skip")
		}
	})
	t.Run("empty file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "empty")
		if err := os.WriteFile(path, []byte("   \n"), 0o400); err != nil {
			t.Fatalf("write: %v", err)
		}
		t.Setenv(apps.ServiceTokenFileEnv, path)
		if _, err := wireUserApps(baseServer(), nil, authztest.New(), nil, wiringProjects{}); err == nil {
			t.Fatal("an empty service-token file must be a boot error")
		}
	})
}
