package http_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
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

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
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
		WithRunReader(apihttp.NewPgxRunReader(pool))
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
	var list []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&list)
	if len(list) != 1 {
		t.Fatalf("list len = %d", len(list))
	}
	runID := list[0]["id"].(string)

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

func parseUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}
	return id
}
