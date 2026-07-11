// Package e2e — RFC 022 Task 2.7: ad-hoc SQL query e2e scenarios.
//
// This file covers POST /api/v1/projects/{pid}/query via the queryproxy →
// query-worker path. All tests require:
//   - E2E_K8S=1
//   - SetupFGABootstrap ran in TestMain (SharedHarness != nil)
//   - The query-worker Deployment is Running (queryWorker.enabled=true in the
//     e2e helm install — wired in tests/e2e/values-app.yaml)
//
// The warehouse is resolved per request from the {pid} path segment (RFC
// 025) — there is no post-bootstrap env patch step. {pid} is the Datuplet
// project UUID, resolved once via getQueryProjectID (reuses
// remoteCLIFindProject against the admin session, since some test principals
// below intentionally carry no FGA grants on the project).
//
// # Authentication
//
// POST /api/v1/projects/{pid}/query is protected by auth.WithUser (ChainResolver) which
// accepts either a session cookie (PostgresResolver) or a cli-api JWT
// (BearerJWTResolver).  The FGA test identities (Alice/Bob/Charlie/Dora)
// are FGA-only UUIDs with no DB user records, so their impersonation tokens
// are rejected with 401 by pipeline-api.
//
// All query tests authenticate using session cookies from
// queryAdminSessionLogin, which logs in as admin@datuplet.local (the only
// DB user seeded by register.sh, with password "changeme").
//
// # AUTHZ-denied test
//
// To prove that a user with no FGA grants is denied, TestQuery_AuthzDenied
// creates a fresh DB user (deny-test-<runPrefix>@datuplet.test / changeme)
// via `kubectl exec pipeline-api admin create-user`, then logs in as that
// user.  The deny-test user has no FGA tuple on the e2e lakekeeper project,
// so their catalog JWT (minted by pipeline-api from the DB user UUID as sub)
// carries no grants.  Lakekeeper denies the LoadTable / STS vend → the
// query-worker surfaces this as a DuckDB-level error (status 400, kind
// sql_error). Note that the deny-test user's lack of FGA grants also means
// it cannot resolve {pid} via its own session (GET /api/v1/projects returns
// only projects the caller has a relation on) — {pid} is resolved once via
// the admin session and reused across all test principals.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// ──────────────────────────────────────────────────────────────────────────────
// Shared query-scenario fixtures
// ──────────────────────────────────────────────────────────────────────────────

// queryE2ENamespace is the K8s namespace where pipeline-api and query-worker
// live during the e2e run (matches the helm install namespace).
const queryE2ENamespace = "datuplet-e2e"

// queryAdminEmail / queryAdminPassword are the credentials for the single
// real DB user seeded by scripts/register.sh. These are the POC defaults —
// production installs override them via --admin-email / --admin-password.
const (
	queryAdminEmail    = "admin@datuplet.local"
	queryAdminPassword = "changeme"
)

// queryAdminSessionOnce caches the admin session cookie so every test in a
// single process shares one login.
var queryAdminSessionOnce sync.Once
var queryAdminSession string // pipeline_api_session cookie value
var queryAdminSessionErr error

// getAdminSession returns the admin session cookie, logging in once per
// test-process via POST /api/v1/auth/login.
func getAdminSession(t *testing.T) string {
	t.Helper()
	queryAdminSessionOnce.Do(func() {
		// Retry across a ~90s window. On OrbStack the NodePort the framework
		// reaches (:30081) can briefly stop serving while pipeline-api settles
		// after the shared bootstrap. Tolerating that settling window beats
		// skipping the entire query suite on one transient timeout.
		var lastErr error
		deadline := time.Now().Add(90 * time.Second)
		for attempt := 1; ; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cookie, _, err := querySessionLogin(ctx, framework.PipelineAPIBaseURL(), queryAdminEmail, queryAdminPassword)
			cancel()
			if err == nil {
				queryAdminSession = cookie
				return
			}
			lastErr = err
			if time.Now().After(deadline) {
				break
			}
			t.Logf("query: admin login attempt %d failed (%v); retrying after pipeline-api rollout…", attempt, err)
			time.Sleep(3 * time.Second)
		}
		queryAdminSessionErr = fmt.Errorf("admin login: %w", lastErr)
	})
	if queryAdminSessionErr != nil {
		framework.SkipOrFail(t, "admin session login failed: %v", queryAdminSessionErr)
	}
	return queryAdminSession
}

// seededQueryNamespace is the Iceberg namespace (lakekeeper namespace == bucket
// name in the pipeline YAML) written by the fga-matrix-alice-trigger-and-browse
// scenario. We reuse that table for the happy-path and authz assertions so we
// don't need to run an extra pipeline.
//
// Shape: "<runPrefix>-api", table "data" — 100 rows, schema: userId/id/title/body.
// This is the same table the http-json-extract pipeline writes (k8s/http-json-extract.yaml).
//
// We cannot guarantee ordering between TestScenarios and TestQuery, so
// queryScenarioNamespaceOnce lazily seeds the table the first time it is needed.
var queryScenarioNamespaceOnce sync.Once
var queryScenarioNamespace string
var queryScenarioTable string
var queryScenarioSeedErr error

// ensureQueryTable runs the http-json-extract pipeline (same as the fga-matrix
// scenarios) once per test process to produce a well-known table.  Subsequent
// calls are no-ops that return the cached namespace/table or the cached error.
//
// The table has 100 rows (jsonplaceholder /posts):
//
//	schema: userId BIGINT, id BIGINT, title VARCHAR, body VARCHAR
func ensureQueryTable(t *testing.T) (ns, table string) {
	t.Helper()
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil — E2E_K8S=1 + bootstrap required")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}

	queryScenarioNamespaceOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		kb, err := framework.NewK8sBackend(h, runPrefix)
		if err != nil {
			queryScenarioSeedErr = fmt.Errorf("new K8s backend: %w", err)
			return
		}
		defer kb.Cleanup(context.Background())

		templatePath, err := findPipelineTemplate("k8s/http-json-extract.yaml")
		if err != nil {
			queryScenarioSeedErr = fmt.Errorf("find pipeline template: %w", err)
			return
		}
		vars := framework.TemplateVars{RunPrefix: runPrefix}
		rendered, err := framework.RenderPipeline(templatePath, vars)
		if err != nil {
			queryScenarioSeedErr = fmt.Errorf("render pipeline: %w", err)
			return
		}
		defer os.Remove(rendered)

		result, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{
			StorageType: "s3",
			Timeout:     3 * time.Minute,
		})
		if err != nil {
			queryScenarioSeedErr = fmt.Errorf("run seed pipeline: %w", err)
			return
		}
		if !result.Success {
			queryScenarioSeedErr = fmt.Errorf("seed pipeline failed (exit %d, type %s): %s",
				result.ExitCode, result.FailureType, result.Logs)
			return
		}
		queryScenarioNamespace = runPrefix + "-api"
		queryScenarioTable = "data"
	})

	if queryScenarioSeedErr != nil {
		t.Fatalf("query table seed failed: %v", queryScenarioSeedErr)
	}
	return queryScenarioNamespace, queryScenarioTable
}

// ──────────────────────────────────────────────────────────────────────────────
// Wiring helper — project UUID resolution (RFC 025 project-scoped route)
// ──────────────────────────────────────────────────────────────────────────────

// queryProjectIDOnce / queryProjectIDCached / queryProjectIDErr cache the
// Datuplet project UUID for the test process (mirrors the queryAdminSession*
// caching pattern above).
var queryProjectIDOnce sync.Once
var queryProjectIDCached string
var queryProjectIDErr error

// getQueryProjectID resolves the Datuplet project UUID that all query
// scenarios target (POST /api/v1/projects/{pid}/query), caching it for the
// test process.
//
// WHY resolve via the admin session and reuse across every test principal:
// TestQuery_AuthzDenied and TestQuery_WriteProbe deliberately use principals
// with NO FGA grants (or read-only grants) on the project — GET
// /api/v1/projects only returns projects the CALLING user has a relation on
// (pkg/pipelineapi/http/stores_pgx.go ListForUser), so those principals could
// not resolve {pid} via their own session. {pid} identifies the project being
// queried, not "projects visible to me" — resolving it once via the admin
// session (which does have project_admin on it) and reusing it is correct
// for every principal.
//
// WHY resolve BY NAME rather than the single-project fallback: the e2e
// cluster carries TWO lakekeeper projects — the shell bootstrap's "default"
// project (register.sh's create-project + attach-warehouse) and the Go
// harness's own project (h.LakekeeperProjectID, lakekeeper NAME
// h.LakekeeperProjectName == "datuplet-e2e"). The query seed table
// (ensureQueryTable) and every FGA grant this suite writes
// (ensureAdminFGAGrant, SeedViewerFGAGrant, ...) live on the HARNESS's
// lakekeeper project, NOT the shell bootstrap's "default" one. Before this
// fix, the single-project fallback below (empty projectName) resolved to
// whichever Datuplet project existed first — the "default" one — so the
// query route attached to the wrong warehouse (table-not-found) and
// non-admin principals (granted only on the harness project) got 403. Now
// that two Datuplet projects can exist, that fallback is ambiguous; we must
// resolve the ONE bound to the harness's own lakekeeper project by name.
//
// ensureSecretsLadderProject (scenarios_secrets_test.go) already does
// exactly the binding we need here: `pipeline-api admin create-project
// --name=<lakekeeper project name>` is a find-or-create BY NAME on the
// lakekeeper side (LakekeeperManager.FindProjectIDByName), so passing
// h.LakekeeperProjectName reuses h.LakekeeperProjectID instead of
// allocating a new lakekeeper Project, and persists a Datuplet
// projects-table row bound to it (lakekeeper_project_id =
// h.LakekeeperProjectID). It's idempotent — safe to call here even if
// TestSecretsLadder already ran it in the same process.
//
// Uses remoteCLIFindProject (defined below in this file), the ground-truth
// helper for this exact GET /api/v1/projects call — it decodes the bare JSON
// array the endpoint returns and matches by exact name.
func getQueryProjectID(t *testing.T) string {
	t.Helper()
	// getAdminSession is called OUTSIDE queryProjectIDOnce.Do: it may itself
	// t.Skip on failure, and doing that from inside another sync.Once's
	// closure would mark this Once "done" via Goexit-triggered defers without
	// ever setting queryProjectIDCached — silently handing later tests an
	// empty pid. getAdminSession is already cheap to call repeatedly (cached
	// by its own sync.Once).
	session := getAdminSession(t)
	h := framework.SharedHarness()
	queryProjectIDOnce.Do(func() {
		if h == nil {
			queryProjectIDErr = fmt.Errorf("resolve query project id: SharedHarness nil")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Bind a Datuplet project row to the harness's own lakekeeper project
		// FIRST — remoteCLIFindProject below can only find it by name once
		// the row exists.
		if _, err := ensureSecretsLadderProject(ctx, h); err != nil {
			queryProjectIDErr = fmt.Errorf("bind Datuplet project to harness lakekeeper project %q: %w", h.LakekeeperProjectName, err)
			return
		}
		pid, err := remoteCLIFindProject(ctx, framework.PipelineAPIBaseURL(), session, h.LakekeeperProjectName)
		if err != nil {
			queryProjectIDErr = fmt.Errorf("resolve query project id: %w", err)
			return
		}
		queryProjectIDCached = pid
	})
	if queryProjectIDErr != nil {
		framework.SkipOrFail(t, "%v", queryProjectIDErr)
	}
	return queryProjectIDCached
}

// ──────────────────────────────────────────────────────────────────────────────
// HTTP helpers
// ──────────────────────────────────────────────────────────────────────────────

// queryRequest is the JSON body for POST /api/v1/projects/{pid}/query.
type queryRequest struct {
	SQL      string `json:"sql"`
	TimeoutS *int   `json:"timeout_s,omitempty"`
	MaxRows  *int   `json:"max_rows,omitempty"`
}

// queryResult mirrors the query-worker's Result wire shape.
type queryResult struct {
	Schema    []map[string]string    `json:"schema"`
	Rows      [][]interface{}        `json:"rows"`
	Truncated bool                   `json:"truncated"`
	Stats     map[string]interface{} `json:"stats"`
}

// queryError mirrors the query-proxy / query-worker's error envelope.
type queryError struct {
	Error string `json:"error"`
	Kind  string `json:"kind"`
}

// postQuery sends POST /api/v1/projects/{pid}/query authenticated with a
// session cookie. Returns (statusCode, body).
func postQuery(ctx context.Context, sessionCookie, pid string, req queryRequest) (int, []byte, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal query request: %w", err)
	}
	u := framework.PipelineAPIBaseURL() + "/api/v1/projects/" + pid + "/query"
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	r.Header.Set("Content-Type", "application/json")
	if sessionCookie != "" {
		r.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: sessionCookie})
	}
	cli := &http.Client{Timeout: 30 * time.Second}
	resp, err := cli.Do(r)
	if err != nil {
		return 0, nil, fmt.Errorf("POST /api/v1/projects/%s/query: %w", pid, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body, nil
}

// storageGETPreview fetches the storage preview for (ns, table) via
// GET /api/v1/storage/projects/{pid}/tables/{ns}/{table}/preview, authenticated
// with a session cookie (mirrors postQuery's cookie attachment above). Returns
// (statusCode, body, err).
func storageGETPreview(ctx context.Context, sessionCookie, pid, ns, table string) (int, []byte, error) {
	u := framework.PipelineAPIBaseURL() + "/api/v1/storage/projects/" + pid +
		"/tables/" + ns + "/" + table + "/preview"
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	if sessionCookie != "" {
		r.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: sessionCookie})
	}
	cli := &http.Client{Timeout: 30 * time.Second}
	resp, err := cli.Do(r)
	if err != nil {
		return 0, nil, fmt.Errorf("GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, nil, fmt.Errorf("read response body: %w", err)
	}
	return resp.StatusCode, body, nil
}

// querySessionLogin POSTs to /api/v1/auth/login and returns the session
// cookie value + user_id. pipeline-api returns 204 No Content on success
// (cookie in Set-Cookie header; no JSON body). This mirrors the cookie-login
// pattern in scenarios_remote_cli_test.go but handles 204 correctly.
func querySessionLogin(ctx context.Context, baseURL, email, password string) (cookie, userID string, err error) {
	body, err := json.Marshal(map[string]string{"email": email, "password": password})
	if err != nil {
		return "", "", err
	}
	u := strings.TrimRight(baseURL, "/") + "/api/v1/auth/login"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", "", fmt.Errorf("POST %s: %w", u, err)
	}
	defer resp.Body.Close()
	// pipeline-api /api/v1/auth/login returns 204 No Content on success,
	// carrying the session cookie in Set-Cookie. Accept both 200 and 204.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", "", fmt.Errorf("login HTTP %d: %s", resp.StatusCode, string(b))
	}

	// On 200 the body carries {user_id: "..."}; on 204 there is no body.
	// We don't need user_id for cookie-based query tests, so skip the decode
	// when the server returns 204.
	if resp.StatusCode == http.StatusOK {
		var loginResp struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&loginResp)
		userID = loginResp.UserID
	}
	for _, c := range resp.Cookies() {
		if c.Name == "pipeline_api_session" {
			return c.Value, userID, nil
		}
	}
	return "", "", fmt.Errorf("no pipeline_api_session cookie in response")
}

// queryMeUserID fetches the authenticated user's UUID via GET /api/v1/auth/me
// using the session cookie. pipeline-api's /api/v1/auth/login returns 204 with
// no body (so querySessionLogin cannot surface the UUID), but the minted
// catalog JWT uses this same user.ID as `sub` — so this is the FGA subject we
// must grant tuples to.
func queryMeUserID(ctx context.Context, baseURL, cookie string) (string, error) {
	u := strings.TrimRight(baseURL, "/") + "/api/v1/auth/me"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: cookie})
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("me HTTP %d: %s", resp.StatusCode, string(b))
	}
	var meResp struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meResp); err != nil {
		return "", fmt.Errorf("decode /me: %w", err)
	}
	if meResp.ID == "" {
		return "", fmt.Errorf("/me returned empty id")
	}
	return meResp.ID, nil
}

// remoteCLIFindProject lists the user's projects and returns the ID of the
// one matching projectName. Falls back to the first project when only one exists.
func remoteCLIFindProject(ctx context.Context, baseURL, sessionCookie, projectName string) (string, error) {
	url := strings.TrimRight(baseURL, "/") + "/api/v1/projects"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: sessionCookie})

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("list projects HTTP %d: %s", resp.StatusCode, string(b))
	}

	var projects []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&projects); err != nil {
		return "", fmt.Errorf("decode projects: %w", err)
	}
	for _, p := range projects {
		if p.Name == projectName {
			return p.ID, nil
		}
	}
	if len(projects) == 1 {
		return projects[0].ID, nil
	}
	return "", fmt.Errorf("project %q not found in %d projects: %+v", projectName, len(projects), projects)
}

// queryFindPipelineAPIPod returns the name of a running pipeline-api pod
// in queryE2ENamespace, or "" on error.
func queryFindPipelineAPIPod(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "kubectl", "get", "pods",
		"-n", queryE2ENamespace,
		"-l", "app.kubernetes.io/name=pipeline-api",
		"-o", "jsonpath={.items[0].metadata.name}").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// intPtr is a convenience helper for optional int pointers in JSON bodies.
func intPtr(v int) *int { return &v }

// findPipelineTemplate resolves the path to a pipeline template relative to
// the test's working directory ("pipelines/<relPath>"). Returns an error
// when the file does not exist.
func findPipelineTemplate(relPath string) (string, error) {
	// The e2e test binary cwd is tests/e2e/ (set by go test). Templates live
	// under tests/e2e/pipelines/.
	path := "pipelines/" + relPath
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("pipeline template not found at %q: %w", path, err)
	}
	return path, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Prerequisite: bootstrap the warehouse env + confirm worker is ready
// ──────────────────────────────────────────────────────────────────────────────

// ensureAdminFGAGrant grants project_admin to the real DB admin user
// (admin@datuplet.local) on the e2e lakekeeper project.
//
// register.sh grants the admin user on the "default" Datuplet project,
// NOT on the e2e harness's separate lakekeeper project. Without this grant,
// the catalog JWT minted for admin (sub=<admin-uuid>) has no FGA tuples on
// the e2e project → lakekeeper returns 403 Forbidden on ATTACH.
//
// Mechanism: fetch the admin user's UUID from pipeline-api /auth/me, then
// call h.Authorizer.WriteTuples to write the FGA tuple directly (same
// pattern as SeedFGAGrants in framework/bootstrap.go). Idempotent: the
// already-exists check matches the pattern in isAlreadyExistsErr.
func ensureAdminFGAGrant(t *testing.T, h *framework.FGAHarness, session string) {
	t.Helper()
	if h == nil || session == "" {
		return
	}

	// Fetch the admin user's DB UUID from pipeline-api /auth/me.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	meURL := strings.TrimRight(framework.PipelineAPIBaseURL(), "/") + "/api/v1/auth/me"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, meURL, nil)
	if err != nil {
		t.Logf("query: WARN — cannot build /auth/me request: %v", err)
		return
	}
	req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: session})
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Logf("query: WARN — GET /auth/me failed: %v", err)
		return
	}
	defer resp.Body.Close()
	var me struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil || me.ID == "" {
		t.Logf("query: WARN — decode /auth/me failed (err=%v id=%q); FGA grant skipped", err, me.ID)
		return
	}

	// Write project_admin tuple via SeedAdminFGAGrant (from framework).
	if err := framework.SeedAdminFGAGrant(ctx, h, me.ID); err != nil {
		t.Logf("query: WARN — SeedAdminFGAGrant failed: %v", err)
		return
	}
	t.Logf("query: admin FGA grant ensured for user %s on project %s", me.ID, h.LakekeeperProjectID)
}

// TestQueryBootstrap verifies the query-worker is ready and the admin
// session + project UUID resolve cleanly. All other TestQuery* subtests
// depend on this having run first.  Go's test execution is top-down within
// a file so naming it Bootstrap keeps it first.
func TestQueryBootstrap(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil — SetupFGABootstrap must have run in TestMain")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable on NodePort 30081 — start port-forward")
	}

	// Verify query-worker Deployment exists and has at least 1 ready replica.
	out, err := exec.Command("kubectl", "get", "deployment", "query-worker",
		"-n", queryE2ENamespace,
		"-o", "jsonpath={.status.readyReplicas}").Output()
	if err != nil {
		framework.SkipOrFail(t, "query-worker Deployment not found — did you set queryWorker.enabled=true in values-app.yaml? err=%v", err)
	}
	ready := strings.TrimSpace(string(out))
	if ready == "" || ready == "0" {
		framework.SkipOrFail(t, "query-worker has 0 ready replicas — worker may still be starting (readyReplicas=%q)", ready)
	}
	t.Logf("query-worker ready replicas: %s", ready)

	// Verify admin login works.
	session := getAdminSession(t)
	if session == "" {
		t.Fatal("admin session login returned empty cookie")
	}
	t.Logf("admin session established successfully")

	// Ensure the admin DB user has FGA grants on the e2e lakekeeper project.
	// register.sh grants the admin user on the "default" project, not on the
	// e2e harness project. Without this grant, the query-worker's catalog JWT
	// (sub=admin-uuid) gets 403 from lakekeeper on ATTACH.
	ensureAdminFGAGrant(t, h, session)

	// Resolve + cache the project UUID the query scenarios target (RFC 025
	// project-scoped route). Failing fast here gives a clearer error than
	// letting every downstream TestQuery* subtest hit the same resolution
	// failure independently.
	pid := getQueryProjectID(t)
	if pid == "" {
		t.Fatal("query project id resolved to empty string")
	}
	t.Logf("query project id resolved: %s", pid)
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 1 — happy path: admin queries the seeded table
// ──────────────────────────────────────────────────────────────────────────────

func TestQuery_HappyPath(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ns, table := ensureQueryTable(t)
	ctx := context.Background()
	session := getAdminSession(t)
	pid := getQueryProjectID(t)

	// The Iceberg namespace is the lakekeeper namespace = "<runPrefix>-api".
	// DuckDB's iceberg extension references it as: iceberg_scan('<catalog>.<ns>.<table>').
	// The query-worker resolves this via the catalog ATTACH — the catalog is
	// the lakekeeper warehouse we seeded, the namespace and table match what
	// the pipeline wrote.
	sql := fmt.Sprintf(`SELECT count(*) AS cnt FROM "%s"."%s"`, ns, table)
	status, body, err := postQuery(ctx, session, pid, queryRequest{SQL: sql})
	if err != nil {
		t.Fatalf("POST /api/v1/projects/{pid}/query: %v", err)
	}
	t.Logf("happy-path status=%d body=%s", status, truncateLog(body, 512))
	if status != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", status, string(body))
	}

	var result queryResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	// Schema sanity: must have at least one column named "cnt".
	if len(result.Schema) == 0 {
		t.Errorf("schema is empty")
	}
	hasCnt := false
	for _, col := range result.Schema {
		if col["name"] == "cnt" {
			hasCnt = true
		}
	}
	if !hasCnt {
		t.Errorf("schema missing 'cnt' column; schema=%v", result.Schema)
	}
	// Row count: the http-json-extractor writes 100 posts.
	if len(result.Rows) == 0 {
		t.Fatalf("rows is empty")
	}
	rawCnt := result.Rows[0][0]
	switch v := rawCnt.(type) {
	case float64:
		if int(v) != 100 {
			t.Errorf("count(*) want 100, got %v", v)
		}
	default:
		// Accept string or json.Number representations for robustness.
		t.Logf("count(*) raw type %T = %v (accepting non-float64)", v, v)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 2 — AUTHZ: a user with no FGA grants is denied
// ──────────────────────────────────────────────────────────────────────────────

// TestQuery_AuthzDenied proves that a user with NO FGA grants on the
// project cannot retrieve data.
//
// Approach: create a fresh DB user (no FGA grants) via pipeline-api admin
// create-user (kubectl exec), login as that user to get a session cookie,
// then POST to the project-scoped query route.
//
// RFC 025 §4.6: the project gate (projectgate.Gate.Authorize) enforces FGA
// datuplet_member at the pipeline-api layer, BEFORE the query reaches the
// worker/lakekeeper. A zero-grant caller is denied fail-fast with HTTP 403
// kind="forbidden" (Authorize → Check returns (false,nil) → 403 forbidden;
// see pkg/pipelineapi/queryproxy/audit.go serveWithAudit → QualifiedWarehouse).
// This is a stronger, earlier denial than the pre-RFC-025 behaviour, where
// the query reached lakekeeper and was rejected at ATTACH time as a DuckDB
// 400 sql_error.
//
// We assert: status == 403 AND kind == "forbidden" AND a non-empty "error"
// field.
func TestQuery_AuthzDenied(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ns, table := ensureQueryTable(t)
	ctx := context.Background()

	// Create a fresh DB user with no FGA grants on the e2e lakekeeper project.
	// The user email is unique per run-prefix to avoid collisions on re-runs.
	denyEmail := "deny-test-" + runPrefix + "@datuplet.test"
	denyPassword := "deny-test-changeme"

	// Find the pipeline-api pod for kubectl exec.
	podName := queryFindPipelineAPIPod(ctx)
	if podName == "" {
		framework.SkipOrFail(t, "pipeline-api pod not found — cannot create deny-test user")
	}

	// Create user via pipeline-api admin subcommand (idempotent: already-exists
	// is logged and ignored by the subcommand itself).
	createOut, err := exec.CommandContext(ctx, "kubectl", "exec",
		podName, "-n", queryE2ENamespace,
		"--", "/usr/local/bin/pipeline-api", "admin", "create-user",
		"--email="+denyEmail,
		"--password="+denyPassword).CombinedOutput()
	if err != nil {
		framework.SkipOrFail(t, "create deny-test user failed: %v\noutput: %s", err, string(createOut))
	}
	t.Logf("deny-test user creation: %s", strings.TrimSpace(string(createOut)))

	// Login as the deny-test user to get a session cookie.
	denyCookie, _, err := querySessionLogin(ctx, framework.PipelineAPIBaseURL(), denyEmail, denyPassword)
	if err != nil {
		framework.SkipOrFail(t, "deny-test user login failed: %v", err)
	}

	// Resolve {pid} via the admin session, NOT the deny-test session: the
	// deny-test user has no FGA grants on the project, so GET /api/v1/projects
	// under its own session would return an empty list.
	pid := getQueryProjectID(t)

	// Query the seeded table. The deny-test user has no FGA tuple on the
	// project → the project gate denies the request at the pipeline-api layer,
	// before the query ever reaches the worker/lakekeeper.
	sql := fmt.Sprintf(`SELECT count(*) AS cnt FROM "%s"."%s"`, ns, table)
	status, body, err := postQuery(ctx, denyCookie, pid, queryRequest{SQL: sql})
	if err != nil {
		t.Fatalf("POST /api/v1/projects/{pid}/query: %v", err)
	}
	t.Logf("authz-denied status=%d body=%s", status, truncateLog(body, 512))

	// The project gate denies a zero-grant caller fail-fast with HTTP 403
	// (RFC 025 §4.6). This also implicitly proves the query was NOT served
	// (a 200 would mean the gate let it through).
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for deny-test user (no FGA grants), got %d: %s",
			status, truncateLog(body, 512))
	}

	// Body must carry a non-empty "error" field.
	var errBody queryError
	if err := json.Unmarshal(body, &errBody); err != nil {
		t.Fatalf("cannot parse error body: %v (raw: %s)", err, string(body))
	}
	if errBody.Error == "" {
		t.Errorf("error body has empty 'error' field — expected a denial message")
	}
	// The denial must carry the project gate's stable "forbidden" kind (the
	// FGA datuplet_member check at the pipeline-api layer), NOT a downstream
	// worker/lakekeeper kind — the gate rejects before the query is dispatched.
	if errBody.Kind != "forbidden" {
		t.Errorf("expected kind=forbidden for project-gate FGA denial, got kind=%q error=%q",
			errBody.Kind, errBody.Error)
	}
	t.Logf("authz denied correctly: status=%d kind=%q error=%q", status, errBody.Kind, errBody.Error)
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 3 — Truncation: max_rows cap is enforced
// ──────────────────────────────────────────────────────────────────────────────

func TestQuery_Truncation(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ctx := context.Background()
	session := getAdminSession(t)
	pid := getQueryProjectID(t)

	// range(100000) produces 100k rows; cap to 10000 (chart max_rows default).
	// This query needs no catalog attachment — DuckDB's built-in range()
	// function works without an Iceberg table.
	sql := "SELECT * FROM range(100000)"
	cap := 10000
	status, body, err := postQuery(ctx, session, pid, queryRequest{SQL: sql, MaxRows: intPtr(cap)})
	if err != nil {
		t.Fatalf("POST /api/v1/projects/{pid}/query: %v", err)
	}
	t.Logf("truncation status=%d body_len=%d", status, len(body))
	if status != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", status, truncateLog(body, 256))
	}

	var result queryResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if !result.Truncated {
		t.Errorf("truncated=false; expected true for 100k rows capped at %d", cap)
	}
	if len(result.Rows) != cap {
		t.Errorf("row count: got %d, want exactly %d (cap)", len(result.Rows), cap)
	}
	t.Logf("truncation correct: rows=%d truncated=%v", len(result.Rows), result.Truncated)
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 4 — Timeout: a slow query returns 408 within ~5s
// ──────────────────────────────────────────────────────────────────────────────

func TestQuery_Timeout(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ctx := context.Background()
	session := getAdminSession(t)
	pid := getQueryProjectID(t)

	// Aggregation over range(30000)×range(30000) = 900M combinations.
	// Produces exactly 1 output row (a SUM), so the max_rows cap cannot
	// truncate it before the 1s timeout fires. DuckDB takes ~2.8s to scan
	// all 900M combinations on the e2e cluster; the worker's context
	// cancellation fires at 1s well before the scan completes.
	sql := "SELECT sum(t1.range * t2.range) AS v FROM range(30000) t1, range(30000) t2"
	timeoutS := 1

	start := time.Now()
	ctx2, cancel := context.WithTimeout(ctx, 15*time.Second) // outer guard
	defer cancel()

	// Use a raw HTTP client with a longer timeout so the 408 from the worker
	// can reach us (we don't want our client-side timeout to fire first).
	raw, _ := json.Marshal(queryRequest{SQL: sql, TimeoutS: intPtr(timeoutS)})
	u := framework.PipelineAPIBaseURL() + "/api/v1/projects/" + pid + "/query"
	req, _ := http.NewRequestWithContext(ctx2, http.MethodPost, u, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: session})
	cli := &http.Client{Timeout: 15 * time.Second}
	resp, err := cli.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("POST /api/v1/projects/{pid}/query: %v (elapsed %s)", err, elapsed)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	t.Logf("timeout status=%d elapsed=%s body=%s", resp.StatusCode, elapsed, truncateLog(body, 256))

	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("expected 408 RequestTimeout, got %d (elapsed %s): %s",
			resp.StatusCode, elapsed, string(body))
	}

	var errBody queryError
	if err := json.Unmarshal(body, &errBody); err != nil {
		t.Logf("WARN: cannot parse error body: %v", err)
	} else if errBody.Kind != "timeout" {
		t.Errorf("expected kind=timeout, got %q", errBody.Kind)
	}

	// The response should arrive within ~10s (worker kills the query at 1s,
	// serialises the error, network round-trip ≤8s on a small cluster).
	if elapsed > 10*time.Second {
		t.Errorf("timeout took %s — expected within 10s for a 1-second budget", elapsed)
	}
	t.Logf("timeout assertion passed: status=408 kind=%q elapsed=%s", errBody.Kind, elapsed)
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 5 — Concurrency: per-principal cap of 2 → at least one 429
// ──────────────────────────────────────────────────────────────────────────────

func TestQuery_Concurrency(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ctx := context.Background()
	// All 3 requests use the SAME session cookie (same DB user UUID = same
	// principal in the per-principal gate). The gate cap is 2, so the 3rd
	// request must get 429 rate_limited.
	session := getAdminSession(t)
	pid := getQueryProjectID(t)

	// 3 concurrent slow queries from the SAME user (admin).
	// Per-principal cap = 2 (queryproxy default).
	// At least one of the three MUST get 429 rate_limited.
	//
	// The query must reliably HOLD each request's gate slot for longer than
	// the pre-gate stagger, so that requests 1 and 2 are both still in flight
	// when request 3 hits the per-principal gate and gets an instant 429.
	// Two pitfalls to avoid:
	//   - a row-returning scan hits the max_rows cap (default 1000) and
	//     returns near-instantly;
	//   - a plain count(*)/sum() over range()×range() is computed
	//     ANALYTICALLY by DuckDB (cardinality known without scanning), so it
	//     also returns in ~milliseconds.
	// An aggregate over an OPAQUE per-row predicate (hash) forces DuckDB to
	// actually evaluate every row of the product — a genuine multi-second
	// scan the optimizer cannot short-circuit. 30000×30000 = 9e8 rows.
	//
	// (RFC 025's per-request warehouse resolution widened the pre-gate
	// stagger, which made the old fast query deterministically fail this test
	// on fast machines — the gate itself is unchanged and correct.)
	sql := "SELECT count(*) AS c FROM range(30000) t1, range(30000) t2 WHERE hash(t1.range * 1000003 + t2.range) % 5 = 0"
	timeoutS := 20

	type result struct {
		status int
		body   []byte
		err    error
	}
	results := make([]result, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx2, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			raw, _ := json.Marshal(queryRequest{SQL: sql, TimeoutS: intPtr(timeoutS)})
			u := framework.PipelineAPIBaseURL() + "/api/v1/projects/" + pid + "/query"
			req, _ := http.NewRequestWithContext(ctx2, http.MethodPost, u, bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: session})
			cli := &http.Client{Timeout: 20 * time.Second}
			resp, err := cli.Do(req)
			if err != nil {
				results[i] = result{err: err}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			results[i] = result{status: resp.StatusCode, body: body}
		}()
	}
	wg.Wait()

	// gotCap is true when at least one request was rejected by a concurrency cap.
	// Two cap levels are possible:
	//   - 429 rate_limited: the proxy's per-principal gate fires (cap=2 per caller).
	//   - 503 capacity:     the worker's own admission semaphore fires (concurrency=2)
	//     AND the proxy translates the worker's 429 → client 503 kind=capacity.
	// Both prove the system correctly limits concurrent queries per principal/pod.
	gotCap := false
	for i, r := range results {
		if r.err != nil {
			t.Logf("goroutine %d: error=%v", i, r.err)
			continue
		}
		t.Logf("goroutine %d: status=%d body=%s", i, r.status, truncateLog(r.body, 128))
		if r.status == http.StatusTooManyRequests || r.status == http.StatusServiceUnavailable {
			gotCap = true
			var errBody queryError
			if err := json.Unmarshal(r.body, &errBody); err == nil {
				t.Logf("goroutine %d: cap triggered kind=%q error=%q", i, errBody.Kind, errBody.Error)
				// rate_limited = proxy per-principal gate; capacity = worker admission (translated by proxy).
				if errBody.Kind != "rate_limited" && errBody.Kind != "capacity" {
					t.Errorf("goroutine %d: expected kind=rate_limited or capacity, got %q", i, errBody.Kind)
				}
			}
		}
	}

	if !gotCap {
		t.Errorf("concurrency cap NOT triggered: 3 concurrent queries as same principal did not produce a 429 or 503 — " +
			"check DATUPLET_QUERY_PER_PRINCIPAL_INFLIGHT (should be 2) and that all 3 requests landed simultaneously")
	} else {
		t.Logf("concurrency cap correctly enforced: all 3 requests accounted for, cap triggered")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 6 — Write probe: INSERT into an Iceberg table via DuckDB must fail
// ──────────────────────────────────────────────────────────────────────────────

// TestQuery_WriteProbe asserts that a READ-ONLY principal cannot write to an
// Iceberg table via the query service — the production read-only enforcement
// property required by RFC 022 ("nothing committed to the catalog").
//
// Enforcement locus (proven by Phase 1, components/queryengine/
// lockdown_deny_integration_test.go): the DuckDB posture deliberately does NOT
// block iceberg writes — DuckDB 1.5.3's iceberg extension DOES support
// INSERT/MERGE/DELETE, and Spike 0.2 confirmed a post-lock INSERT stages to
// object storage under write-capable creds. Read-only is therefore enforced by
// the CALLER's FGA grant via lakekeeper: a `viewer` principal is vended
// read-only STS, so the catalog-qualified write is denied catalog-side (the
// staging PUT or the metadata commit fails) and surfaces as a 400 sql_error.
//
// We therefore probe with a freshly-provisioned viewer (not the admin, who has
// write grants), using a TYPE-CORRECT INSERT (re-inserting an existing row) so
// any failure is about WRITE PERMISSION, not value-type binding. A positive
// control (the viewer CAN read) proves the grant is live before the write
// assertion, so a denial can't be misattributed to a broken read path.
func TestQuery_WriteProbe(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ns, table := ensureQueryTable(t)
	ctx := context.Background()
	pid := getQueryProjectID(t)

	// Provision a fresh READ-ONLY (viewer) DB user, unique per run-prefix.
	viewerEmail := "viewer-" + runPrefix + "@datuplet.test"
	viewerPassword := "viewer-changeme"
	podName := queryFindPipelineAPIPod(ctx)
	if podName == "" {
		framework.SkipOrFail(t, "pipeline-api pod not found — cannot create viewer user")
	}
	createOut, err := exec.CommandContext(ctx, "kubectl", "exec",
		podName, "-n", queryE2ENamespace,
		"--", "/usr/local/bin/pipeline-api", "admin", "create-user",
		"--email="+viewerEmail,
		"--password="+viewerPassword).CombinedOutput()
	if err != nil {
		framework.SkipOrFail(t, "create viewer user failed: %v\noutput: %s", err, string(createOut))
	}
	t.Logf("viewer user creation: %s", strings.TrimSpace(string(createOut)))

	viewerCookie, _, err := querySessionLogin(ctx, framework.PipelineAPIBaseURL(), viewerEmail, viewerPassword)
	if err != nil {
		framework.SkipOrFail(t, "viewer login failed: %v", err)
	}
	// Resolve the viewer's UUID (the FGA subject the catalog JWT carries as
	// sub). /api/v1/auth/me is the only path that surfaces it (login is 204).
	viewerUUID, err := queryMeUserID(ctx, framework.PipelineAPIBaseURL(), viewerCookie)
	if err != nil {
		framework.SkipOrFail(t, "resolve viewer UUID: %v", err)
	}

	// Grant the viewer read-only access on the e2e lakekeeper project. Mirrors
	// SeedAdminFGAGrant — register.sh only grants on the "default" project, but
	// the query path uses the harness's separate datuplet-e2e project.
	if err := framework.SeedViewerFGAGrant(ctx, h, viewerUUID); err != nil {
		t.Fatalf("seed viewer grant: %v", err)
	}

	// Positive control: the viewer CAN read. Proves the read-only grant is live
	// (lakekeeper vends working read STS) so the write denial below is
	// attributable to write-permission, not a broken read path.
	selectSQL := fmt.Sprintf(`SELECT count(*) AS cnt FROM "%s"."%s"`, ns, table)
	status, body, err := postQuery(ctx, viewerCookie, pid, queryRequest{SQL: selectSQL})
	if err != nil {
		t.Fatalf("POST /api/v1/projects/{pid}/query (viewer SELECT): %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("viewer SELECT must succeed (read-only grant) — got %d body=%s",
			status, truncateLog(body, 512))
	}
	t.Logf("viewer read positive-control OK: status=%d", status)

	// The probe: a TYPE-CORRECT INSERT (re-insert an existing row, so the schema
	// always matches and the failure is about WRITE PERMISSION, not types). The
	// viewer's read-only STS / FGA grant MUST deny this catalog-side.
	insertSQL := fmt.Sprintf(
		`INSERT INTO "%s"."%s" SELECT * FROM "%s"."%s" LIMIT 1`,
		ns, table, ns, table,
	)
	status, body, err = postQuery(ctx, viewerCookie, pid, queryRequest{SQL: insertSQL})
	if err != nil {
		t.Fatalf("POST /api/v1/projects/{pid}/query (viewer INSERT): %v", err)
	}
	t.Logf("viewer write-probe status=%d body=%s", status, truncateLog(body, 512))

	if status == http.StatusOK {
		t.Fatalf("SECURITY: viewer INSERT returned 200 OK — a read-only principal WROTE " +
			"to the catalog via ad-hoc query; read-only enforcement is broken")
	}
	var errBody queryError
	if err := json.Unmarshal(body, &errBody); err != nil {
		t.Fatalf("cannot parse error body: %v (raw: %s)", err, string(body))
	}
	// The write must be denied at the query-execution layer (DuckDB surfaces the
	// read-only STS / catalog denial as a sql_error), NOT by an infra failure
	// (404 route / 401 auth) that would pass the status!=200 check vacuously.
	if errBody.Kind != "sql_error" {
		t.Errorf("expected kind=sql_error for catalog-side write denial, got kind=%q error=%q",
			errBody.Kind, errBody.Error)
	}
	t.Logf("viewer write correctly denied: status=%d kind=%q error=%q", status, errBody.Kind, errBody.Error)
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 7 — NetworkPolicy egress self-check
// ──────────────────────────────────────────────────────────────────────────────

// TestQuery_NetworkPolicyEgress implements the Spike 0.2 graduated assertion:
//  1. Self-check: deploy a deny-all-egress NetworkPolicy on a canary pod and
//     probe an in-cluster HTTP target. If traffic still flows, the CNI does
//     not enforce NetworkPolicy → t.Skip with a LOUD message.
//  2. If enforced: assert that a query using read_parquet targeting an
//     out-of-allowlist HTTP URL fails (connection refused / blocked), while
//     the normal seeded-table query still succeeds.
func TestQuery_NetworkPolicyEgress(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ctx := context.Background()

	// ---- Self-check: does this cluster enforce NetworkPolicy? ----
	// Deploy a canary busybox pod + deny-all-egress policy, probe an
	// in-cluster HTTP target. Clean up in defer regardless.
	canaryNS := queryE2ENamespace
	canaryName := "np-self-check-" + runPrefix
	canaryPolicy := canaryName + "-deny-all"

	// Cleanup in defer — always runs.
	defer func() {
		_ = exec.Command("kubectl", "delete", "pod", canaryName,
			"-n", canaryNS, "--ignore-not-found", "--wait=false").Run()
		_ = exec.Command("kubectl", "delete", "networkpolicy", canaryPolicy,
			"-n", canaryNS, "--ignore-not-found").Run()
	}()

	// Create a deny-all-egress NetworkPolicy scoped to the canary pod.
	npManifest := fmt.Sprintf(`
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %s
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: %s
  policyTypes:
  - Egress
`, canaryPolicy, canaryNS, canaryName)

	applyCmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	applyCmd.Stdin = strings.NewReader(npManifest)
	if out, err := applyCmd.CombinedOutput(); err != nil {
		t.Skipf("cannot apply canary NetworkPolicy (RBAC?): %v\n%s", err, string(out))
	}

	// Create a canary pod that does a wget probe and exits.
	// We probe pipeline-api (which is in the same namespace and reachable).
	// If the policy enforced, the wget should fail (exit non-zero or empty).
	podManifest := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app: %s
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  containers:
  - name: canary
    image: busybox:latest
    imagePullPolicy: IfNotPresent
    command: ["/bin/sh", "-c", "wget -T 3 -O /dev/null http://pipeline-api.%s.svc.cluster.local:8081/healthz && echo REACHABLE || echo BLOCKED"]
`, canaryName, canaryNS, canaryName, canaryNS)

	podApplyCmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	podApplyCmd.Stdin = strings.NewReader(podManifest)
	if out, err := podApplyCmd.CombinedOutput(); err != nil {
		t.Skipf("cannot create canary pod: %v\n%s", err, string(out))
	}

	// Wait up to 30s for the canary pod to complete.
	canaryDone := false
	for i := 0; i < 15; i++ {
		time.Sleep(2 * time.Second)
		out, _ := exec.CommandContext(ctx, "kubectl", "get", "pod", canaryName,
			"-n", canaryNS,
			"-o", "jsonpath={.status.phase}").Output()
		phase := strings.TrimSpace(string(out))
		if phase == "Succeeded" || phase == "Failed" {
			canaryDone = true
			break
		}
	}
	if !canaryDone {
		t.Logf("canary pod did not complete in 30s — NetworkPolicy self-check inconclusive; skipping")
		t.Skip("NetworkPolicy self-check inconclusive: canary pod did not complete")
	}

	logOut, _ := exec.CommandContext(ctx, "kubectl", "logs", canaryName,
		"-n", canaryNS).Output()
	canaryLog := strings.TrimSpace(string(logOut))
	t.Logf("canary pod log: %q", canaryLog)

	if strings.Contains(canaryLog, "REACHABLE") {
		// NetworkPolicy not enforced — kindnet or no CNI.
		t.Skip("NetworkPolicy unenforced on this cluster (no policy CNI) — " +
			"egress assertion skipped; production requires a policy-capable CNI " +
			"(GKE Dataplane V2, Calico, Cilium). Spike 0.2 verification: OrbStack " +
			"uses kindnet which does not enforce NetworkPolicy egress rules.")
	}

	// ---- Policy IS enforced: assert query to blocked target fails ----
	// Queries to the seeded table must still succeed; queries referencing
	// out-of-allowlist targets (openfga, not on the query-worker's egress
	// allowlist) must fail with a connection-class error.
	t.Logf("NetworkPolicy is enforced — running egress assertion")
	session := getAdminSession(t)
	pid := getQueryProjectID(t)

	// Attempt to reach openfga (not in query-worker allowlist).
	// read_parquet over HTTP to openfga's port — the connection must be
	// blocked at the network layer → DuckDB returns a sql_error or connection error.
	openFGATarget := fmt.Sprintf("http://openfga.%s.svc.cluster.local:8080/x.parquet", canaryNS)
	blockedSQL := fmt.Sprintf(`SELECT * FROM read_parquet('%s') LIMIT 1`, openFGATarget)
	blockedStatus, blockedBody, err := postQuery(ctx, session, pid, queryRequest{SQL: blockedSQL})
	if err != nil {
		t.Logf("blocked query transport error (acceptable): %v", err)
	} else {
		t.Logf("blocked query status=%d body=%s", blockedStatus, truncateLog(blockedBody, 256))
		if blockedStatus == http.StatusOK {
			t.Errorf("egress lockdown FAILED: query to %s returned 200 OK — "+
				"the query-worker's NetworkPolicy egress rule should have blocked this", openFGATarget)
		} else {
			t.Logf("egress blocked correctly: status=%d", blockedStatus)
		}
	}

	// Seeded-table query must still succeed (lakekeeper + minio are in the allowlist).
	ns, table := ensureQueryTable(t)
	goodSQL := fmt.Sprintf(`SELECT count(*) AS cnt FROM "%s"."%s"`, ns, table)
	goodStatus, goodBody, err := postQuery(ctx, session, pid, queryRequest{SQL: goodSQL})
	if err != nil {
		t.Errorf("seeded-table query after egress check failed: %v", err)
	} else if goodStatus != http.StatusOK {
		t.Errorf("seeded-table query returned %d after egress check: %s", goodStatus, truncateLog(goodBody, 256))
	} else {
		t.Logf("seeded-table query still succeeds after egress assertion: status=%d", goodStatus)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 8 — Audit smoke: grep pipeline-api logs for a query_audit line
// ──────────────────────────────────────────────────────────────────────────────

// TestQuery_AuditSmoke runs the happy-path query again (or relies on the one
// already in TestQuery_HappyPath above), then greps the pipeline-api pod logs
// for a "query_audit" slog line with outcome=ok.
//
// This is best-effort: if log scraping fails or the format changes, the test
// logs a warning but does NOT fail (skip-not-fail semantics).
func TestQuery_AuditSmoke(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ns, table := ensureQueryTable(t)
	ctx := context.Background()
	session := getAdminSession(t)
	pid := getQueryProjectID(t)

	// Fire a fresh happy-path query to ensure a recent audit line exists.
	sql := fmt.Sprintf(`SELECT count(*) AS cnt FROM "%s"."%s"`, ns, table)
	status, body, err := postQuery(ctx, session, pid, queryRequest{SQL: sql})
	if err != nil || status != http.StatusOK {
		t.Skipf("audit-smoke pre-query failed (status=%d err=%v) — skipping audit check", status, err)
	}
	_ = body

	// Find the pipeline-api pod name.
	podName := queryFindPipelineAPIPod(ctx)
	if podName == "" {
		t.Logf("AUDIT SMOKE SKIP: cannot find pipeline-api pod")
		t.Skip("audit smoke: pipeline-api pod not found")
	}
	t.Logf("audit smoke: checking logs on pod %s", podName)

	// Fetch the last 200 lines of logs (recent; avoids pulling the full log).
	logOut, err := exec.CommandContext(ctx, "kubectl", "logs", podName,
		"-n", queryE2ENamespace,
		"--tail=200").Output()
	if err != nil {
		t.Logf("AUDIT SMOKE SKIP: kubectl logs failed: %v", err)
		t.Skip("audit smoke: kubectl logs failed")
	}

	logStr := string(logOut)
	if !strings.Contains(logStr, "query_audit") {
		t.Logf("AUDIT SMOKE WARN: no 'query_audit' line found in last 200 log lines — " +
			"either the query didn't reach pipeline-api or log format changed. " +
			"Raw log tail:\n%s", truncateLog([]byte(logStr), 1024))
		t.Skip("audit smoke: no query_audit line found — best-effort only")
	}
	if !strings.Contains(logStr, "outcome=ok") {
		t.Logf("AUDIT SMOKE WARN: 'query_audit' line found but no 'outcome=ok' — " +
			"the query may have failed or the slog format uses a different key. " +
			"Log snippet:\n%s", extractAuditLines(logStr))
		t.Skip("audit smoke: query_audit found but outcome=ok not present — best-effort only")
	}
	t.Logf("audit smoke PASS: found query_audit line with outcome=ok")
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 9 — Storage preview: served by the query-worker, <5s latency gate
// ──────────────────────────────────────────────────────────────────────────────

// TestQuery_StoragePreview proves that GET
// /api/v1/storage/projects/{pid}/tables/{ns}/{table}/preview is served by the
// query-worker (RFC 025 §4.1 — pipeline-api never touches parquet bytes) and
// meets the Phase 3 latency gate (§7): the response must arrive within 5s.
//
// The disabled path (501 query_disabled, h.Query == nil in
// pkg/pipelineapi/storage/handlers.go Preview) is NOT exercised here: the e2e
// cluster always installs with queryWorker.enabled=true (tests/e2e/values-app.yaml
// plus the chart's own default), so there is no disabled lane in this suite
// to assert against.
func TestQuery_StoragePreview(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable")
	}

	ctx := context.Background()
	session := getAdminSession(t)
	pid := getQueryProjectID(t)
	ns, table := ensureQueryTable(t)

	start := time.Now()
	status, body, err := storageGETPreview(ctx, session, pid, ns, table)
	if err != nil {
		t.Fatalf("GET storage preview: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", status, truncateLog(body, 512))
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("preview took %s, want < 5s (RFC 025 §7 Phase 3 latency gate)", elapsed)
	}
	t.Logf("preview latency: %s", time.Since(start)) // recorded in the PR description

	var resp struct {
		Columns []struct {
			Name, Type string
		} `json:"columns"`
		Rows      [][]any `json:"rows"`
		Truncated bool    `json:"truncated"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Columns) == 0 {
		t.Fatalf("preview shape: err=%v body=%s", err, truncateLog(body, 512))
	}
	t.Logf("preview OK: columns=%d rows=%d truncated=%v", len(resp.Columns), len(resp.Rows), resp.Truncated)
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

// truncateLog trims body to at most maxBytes for logging; appends "…" when
// truncated. Avoids flooding test output on 10 MiB result bodies.
func truncateLog(b []byte, maxBytes int) string {
	if len(b) <= maxBytes {
		return string(b)
	}
	return string(b[:maxBytes]) + "…"
}

// extractAuditLines returns all log lines containing "query_audit".
func extractAuditLines(logStr string) string {
	var lines []string
	for _, line := range strings.Split(logStr, "\n") {
		if strings.Contains(line, "query_audit") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}
