package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
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

// seedProject logs in a@example.com, creates a project bound to the lakekeeper
// project id lkID, and grants that user each of relations on it as an FGA
// tuple. Returns the session cookie, the user and the project.
func seedProject(t *testing.T, ts *httptest.Server, pool *pgxpool.Pool, fakeAuthz *authztest.Fake, lkID string, relations ...string) (*stdhttp.Cookie, *store.User, *store.Project) {
	t.Helper()
	cookie, user := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, err := store.CreateProject(context.Background(), pool, "proj")
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if err := store.SetLakekeeperProjectID(context.Background(), pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	for _, rel := range relations {
		fakeAuthz.Allow(authz.UserObject(user.ID.String()).String(), rel, authz.ProjectObject(lkID))
	}
	return cookie, user, proj
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

	// handleGetProject checks "describe".
	cookie, _, p := seedProject(t, ts, pool, fakeAuthz, "lk-test-uuid-get-allowed", "describe")

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
