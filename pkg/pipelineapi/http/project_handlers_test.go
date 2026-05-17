package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func seedUserAndLogin(t *testing.T, pool *pgxpool.Pool, baseURL, email, password string) (*stdhttp.Cookie, *store.User) {
	t.Helper()
	seedUser(t, pool, email, password)
	u, err := store.GetUserByEmail(context.Background(), pool, email)
	if err != nil {
		t.Fatalf("getuser: %v", err)
	}
	resp := postJSON(t, baseURL+"/api/v1/auth/login", map[string]string{"email": email, "password": password})
	defer resp.Body.Close()
	for _, c := range resp.Cookies() {
		if c.Name == auth.SessionCookieName {
			return c, u
		}
	}
	t.Fatal("no session cookie after login")
	return nil, nil
}

func TestListProjects_EmptyMemberships(t *testing.T) {
	ts, pool, _, cleanup := freshServer(t)
	defer cleanup()
	cookie, _ := seedUserAndLogin(t, pool, ts.URL, "alice@example.com", "hunter2")

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if len(body) != 0 {
		t.Errorf("expected empty list, got %d projects", len(body))
	}
}

func TestListProjects_OnlyUserMemberships(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "alice@example.com", "x")
	pA, _ := store.CreateProject(ctx, pool, "proj-a")
	_, _ = store.CreateProject(ctx, pool, "proj-unused")

	// Seed FGA membership for alice on proj-a only (P1-2 fix: FGA-backed listing).
	lkID := "lk-test-uuid-proj-a"
	if err := store.SetLakekeeperProjectID(ctx, pool, pA.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(
		authz.UserObject(alice.ID.String()).String(),
		"datuplet_member",
		authz.ProjectObject(lkID),
	)

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	var body []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)

	if len(body) != 1 {
		t.Fatalf("expected 1 project, got %d", len(body))
	}
	if body[0]["name"] != "proj-a" {
		t.Errorf("got name=%v", body[0]["name"])
	}
	ns, _ := body[0]["k8s_namespace"].(string)
	if ns != "datuplet-"+pA.ID.String() {
		t.Errorf("namespace %q wrong", ns)
	}
}

func TestGetProject_ForbiddenWithoutMembership(t *testing.T) {
	ts, pool, _, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, _ := seedUserAndLogin(t, pool, ts.URL, "alice@example.com", "x")
	other, _ := store.CreateProject(ctx, pool, "other")
	// Project has no lakekeeper_project_id and no FGA tuple for alice.
	// mustHaveRelation returns 503 (lakekeeper not yet provisioned).
	_ = other

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+other.ID.String(), nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Errorf("status = %d, expected non-200 (no access)", resp.StatusCode)
	}
}

func TestGetProject_Allowed(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "alice@example.com", "x")
	p, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-test-uuid-get-allowed"
	if err := store.SetLakekeeperProjectID(ctx, pool, p.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	// handleGetProject checks "describe".
	fakeAuthz.Allow(
		authz.UserObject(alice.ID.String()).String(),
		"describe",
		authz.ProjectObject(lkID),
	)

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+p.ID.String(), nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}
