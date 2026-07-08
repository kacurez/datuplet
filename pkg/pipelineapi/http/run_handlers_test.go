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
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/runbackend"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// freshServerWithK8s spins up a Postgres-backed handler stack with a fake
// k8s client + a fake FGA authorizer. The returned *authztest.Fake is the
// caller's seam to grant per-user `viewer` / `editor` tuples on the
// lakekeeper project so mustHaveRelation lets the request through.
func freshServerWithK8s(t *testing.T) (*httptest.Server, *pgxpool.Pool, *authztest.Fake, func()) {
	t.Helper()
	return freshServerWithK8sClient(t, fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build())
}

// freshServerWithK8sClient is freshServerWithK8s parameterised on the K8s
// client, so tests can inject an interceptor client (e.g. Forbidden on the
// managed-Secret Get). The run-trigger backend AND WithSecrets share this one
// client, mirroring main.go's wiring.
func freshServerWithK8sClient(t *testing.T, c client.Client) (*httptest.Server, *pgxpool.Pool, *authztest.Fake, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if _, err := pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
		pool.Close()
		t.Fatalf("reset: %v", err)
	}
	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}

	// In-test keypair for the signer.
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		pool.Close()
		t.Fatalf("gen key: %v", err)
	}
	dir := t.TempDir()
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		pool.Close()
		t.Fatalf("marshal key: %v", err)
	}
	kp := filepath.Join(dir, "priv.pem")
	if err := os.WriteFile(kp, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o400); err != nil {
		pool.Close()
		t.Fatalf("write key: %v", err)
	}
	signer, err := tokens.LoadPrivateKeyFromPEMFile(kp, "kid-test")
	if err != nil {
		pool.Close()
		t.Fatalf("load signer: %v", err)
	}

	authzr := authztest.New()
	backend := runbackend.NewK8sBackend(runbackend.K8sOpts{
		Client:      c,
		RunInserter: runbackend.NewStoreInserter(pool),
		ProjectNS:   runbackend.NewStoreProjectNS(pool, c),
		Minter:      runbackend.NewTokenMinter(signer),
		DB:          pool,
		Authorizer:  authzr,
	})
	srv := apihttp.NewServer(pool).
		WithSigner(signer).
		WithK8sClient(c).
		WithRunBackend(backend).
		WithUserResolver(auth.NewPostgresResolver(pool, false)).
		WithAuthorizer(authzr).
		WithProjectReader(apihttp.NewPgxProjectReader(pool, authzr)).
		WithPipelineStore(apihttp.NewPgxPipelineStore(pool)).
		WithRunReader(apihttp.NewPgxRunReader(pool)).
		// Reuses the same fake k8s client as run-trigger — mirrors main.go's
		// wiring (WithSecrets shares WithK8sClient's client). Existing
		// fixtures (validPipelineYAML, tlYAML) reference no secrets, so the
		// S7 ladder no-ops for every test above; only the secret-ladder
		// tests below exercise it.
		WithSecrets(c, time.Now)
	ts := httptest.NewServer(srv.Handler())
	cleanup := func() { ts.Close(); pool.Close() }
	return ts, pool, authzr, cleanup
}

// seedProjectAuthz provisions a synthetic lakekeeper_project_id on the
// project + seeds the FGA tuples that mustHaveRelation expects. Returns
// the lakekeeper UUID for assertions.
//
// The fake Authorizer is exact-match-only (no inheritance), so we write
// the concrete relations the handlers actually check:
//   - write handlers check "data_admin"
//   - read handlers check "describe"
//
// "editor" seeds data_admin (write) + describe (read).
// "viewer"  seeds describe only.
func seedProjectAuthz(t *testing.T, pool *pgxpool.Pool, fakeAuthz *authztest.Fake, userID, projectID uuid.UUID, role string) string {
	t.Helper()
	lkID := uuid.NewString()
	if err := store.SetLakekeeperProjectID(context.Background(), pool, projectID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	userStr := authz.UserObject(userID.String()).String()
	projObj := authz.ProjectObject(lkID)
	// Read access: handlers check "describe".
	fakeAuthz.Allow(userStr, "describe", projObj)
	if role == "editor" {
		// Write access: handlers check "data_admin".
		fakeAuthz.Allow(userStr, "data_admin", projObj)
	}
	return lkID
}

func TestTriggerRun_HappyPath(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", validPipelineYAML)

	resp := postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl/runs", map[string]any{}, cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["id"] == "" {
		t.Error("missing id in response")
	}

	// DB row created.
	runs, _ := store.ListRunsForProject(ctx, pool, proj.ID, 10)
	if len(runs) != 1 {
		t.Errorf("runs len = %d, want 1", len(runs))
	}
}

// TestTriggerRun_ViewerForbidden confirms a user seeded with the
// `viewer` role (read-only — only `describe` is granted) is rejected
// from the trigger handler. Trigger requires `data_admin`; the fake
// Authorizer does not chain, so a viewer never satisfies it. Anchors
// the negative path of seedProjectAuthz so the read/write split is
// regression-tested.
func TestTriggerRun_ViewerForbidden(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "viewer")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", validPipelineYAML)

	resp := postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl/runs", map[string]any{}, cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Errorf("viewer trigger: status = %d, want 403", resp.StatusCode)
	}
}

func TestTriggerRun_PipelineNotFound(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")

	resp := postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/nope/runs", map[string]any{}, cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestListAndGetRun(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", validPipelineYAML)

	postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl/runs", map[string]any{}, cookie).Body.Close()

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("list: status = %d", resp.StatusCode)
	}
	var list struct {
		Runs       []map[string]any `json:"runs"`
		NextCursor *string          `json:"next_cursor"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&list)
	if len(list.Runs) != 1 {
		t.Fatalf("list len = %d", len(list.Runs))
	}
	runID := list.Runs[0]["id"].(string)

	req2, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs/"+runID, nil)
	req2.AddCookie(cookie)
	resp2, err := stdhttp.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Errorf("get: status = %d", resp2.StatusCode)
	}
}

func TestCancelRun(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", validPipelineYAML)

	trg := postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl/runs", map[string]any{}, cookie)
	var body map[string]string
	_ = json.NewDecoder(trg.Body).Decode(&body)
	trg.Body.Close()
	runID := body["id"]

	resp := postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs/"+runID+"/cancel", map[string]any{}, cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}
	run, _ := store.GetRunByID(ctx, pool, parseUUID(t, runID))
	if run.Phase != "Cancelled" {
		t.Errorf("phase = %q, want Cancelled", run.Phase)
	}
}

func TestListRuns_EnvelopeAndFilters(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")

	daily, _ := store.CreatePipeline(ctx, pool, proj.ID, "daily-orders", validPipelineYAML)
	sync, _ := store.CreatePipeline(ctx, pool, proj.ID, "customer-sync", validPipelineYAML)
	rA, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: daily.ID})
	_, _ = store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: sync.ID})
	_, _ = store.UpdateRunPhase(ctx, pool, rA.ID, store.UpdateRunPhaseOpts{Phase: "Succeeded"})

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs?pipeline=daily", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("list: status = %d", resp.StatusCode)
	}
	var got struct {
		Runs []struct {
			ID           string `json:"id"`
			PipelineName string `json:"pipeline_name"`
			StartedAt    string `json:"started_at"`
		} `json:"runs"`
		NextCursor *string `json:"next_cursor"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Runs) != 1 || got.Runs[0].PipelineName != "daily-orders" {
		t.Fatalf("runs = %+v, want 1 daily-orders", got.Runs)
	}
	if got.NextCursor != nil {
		t.Errorf("next_cursor = %v, want null on last page", *got.NextCursor)
	}

	// Unknown phase -> 400.
	reqBadPhase, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs?phase=Bogus", nil)
	reqBadPhase.AddCookie(cookie)
	respBadPhase, err := stdhttp.DefaultClient.Do(reqBadPhase)
	if err != nil {
		t.Fatalf("phase filter: %v", err)
	}
	defer respBadPhase.Body.Close()
	if respBadPhase.StatusCode != 400 {
		t.Errorf("phase=Bogus: status = %d, want 400", respBadPhase.StatusCode)
	}

	// Malformed cursor -> 400.
	reqBadCursor, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs?cursor=!!!", nil)
	reqBadCursor.AddCookie(cookie)
	respBadCursor, err := stdhttp.DefaultClient.Do(reqBadCursor)
	if err != nil {
		t.Fatalf("cursor filter: %v", err)
	}
	defer respBadCursor.Body.Close()
	if respBadCursor.StatusCode != 400 {
		t.Errorf("cursor=!!!: status = %d, want 400", respBadCursor.StatusCode)
	}
}

const tlYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: daily-orders
spec:
  stages:
    - name: extract
      components:
        - name: api
          component: x
          inputs: {buckets: [api]}
          outputs:
            tables: [{name: orders, bucket: raw, writeMode: FULL_LOAD}]
`

func TestGetRun_AssemblesTimeline(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")

	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "daily-orders", tlYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID})
	snap := []byte(`[{"name":"extract","phase":"Succeeded","startTime":"2026-06-16T14:02:12Z","completionTime":"2026-06-16T14:03:40Z"}]`)
	_, _ = store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{Phase: "Succeeded", StageStatuses: snap})

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs/"+run.ID.String(), nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("get: status = %d", resp.StatusCode)
	}
	var got struct {
		Timeline []struct {
			Name     string `json:"name"`
			Exported []struct {
				Label string `json:"label"`
			} `json:"exported"`
		} `json:"timeline"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Timeline) != 1 || got.Timeline[0].Name != "extract" {
		t.Fatalf("timeline = %+v, want 1 extract stage", got.Timeline)
	}
	if len(got.Timeline[0].Exported) != 1 || got.Timeline[0].Exported[0].Label != "raw.orders" {
		t.Errorf("exported = %+v, want raw.orders", got.Timeline[0].Exported)
	}

	// A run with no snapshot returns "timeline": null.
	run2, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID})
	_, _ = store.UpdateRunPhase(ctx, pool, run2.ID, store.UpdateRunPhaseOpts{Phase: "Succeeded"})

	req2, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs/"+run2.ID.String(), nil)
	req2.AddCookie(cookie)
	resp2, err := stdhttp.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("get run2: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("get run2: status = %d", resp2.StatusCode)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&raw); err != nil {
		t.Fatalf("decode run2: %v", err)
	}
	if v, ok := raw["timeline"]; !ok || v != nil {
		t.Errorf("timeline = %v, want null", v)
	}
}

// TestTriggerRun_UnknownSecretKey_Returns400 covers the S7 trigger-reject
// rung: a pipeline referencing "$[api_token]" is rejected hard (400) at
// trigger time when the project's managed Secret doesn't have that key.
// The controller admission check (S3) remains the authoritative gate for
// the pod itself; this is the fast user-facing reject before a run row is
// even inserted.
func TestTriggerRun_UnknownSecretKey_Returns400(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl-secret", pipelineYAMLWithSecretRef)

	resp := postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl-secret/runs", map[string]any{}, cookie)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "api_token") {
		t.Errorf("body = %s, want it to name the missing key api_token", body)
	}

	// No run row must have been inserted — the reject happens before any
	// DB write.
	runs, _ := store.ListRunsForProject(ctx, pool, proj.ID, 10)
	if len(runs) != 0 {
		t.Errorf("runs len = %d, want 0 (reject must happen before insert)", len(runs))
	}
}

// TestTriggerRun_KnownSecretKey_Returns201 proves the ladder's happy path:
// once the referenced key exists in the project's managed Secret, the same
// pipeline triggers normally (201, as today).
func TestTriggerRun_KnownSecretKey_Returns201(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl-secret", pipelineYAMLWithSecretRef)

	putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/api_token", "shh", cookie).Body.Close()

	resp := postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl-secret/runs", map[string]any{}, cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201; body=%s", resp.StatusCode, body)
	}
}

// TestTriggerRun_FreshProjectForbiddenSecretRead_Returns400 proves the trigger
// hard-reject ladder survives a BRAND-NEW project whose managed-Secret Get is
// RBAC-Forbidden (S6 removed pipeline-api's cluster-wide Secret verbs; the
// per-namespace `datuplet-secrets` Role is created lazily only on the first
// secret PUT). The ladder must reject the missing $[api_token] with a 400,
// NEVER a 500 — and insert no run row. (The NotFound/absent variant is covered
// by TestTriggerRun_UnknownSecretKey_Returns400; the fake client can't model
// RBAC, so this test injects Forbidden via an interceptor client.)
func TestTriggerRun_FreshProjectForbiddenSecretRead_Returns400(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8sClient(t, forbiddenSecretGetClient(t))
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl-secret", pipelineYAMLWithSecretRef)

	resp := postJSON(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl-secret/runs", map[string]any{}, cookie)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400 (fresh-project ladder, not 500); body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "api_token") {
		t.Errorf("body = %s, want it to name the missing key api_token", body)
	}
	runs, _ := store.ListRunsForProject(ctx, pool, proj.ID, 10)
	if len(runs) != 0 {
		t.Errorf("runs len = %d, want 0 (reject must happen before insert)", len(runs))
	}
}

func parseUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}
	return id
}
