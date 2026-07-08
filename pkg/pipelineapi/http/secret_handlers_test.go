package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// testClock is the injected-clock seam secret_handlers.go uses instead of
// reading time.Now() itself — lets tests assert exact "datuplet.io/updated-
// <key>" annotation values and prove the handler never reads wall-clock time.
type testClock struct{ t time.Time }

func (c *testClock) now() time.Time { return c.t }

// freshServerWithSecrets spins up a Postgres-backed handler stack with a
// fake k8s client + fake FGA authorizer, wired via WithSecrets. Returns the
// fake k8s client and clock too, so tests can inspect the managed Secret
// directly and drive deterministic timestamps.
func freshServerWithSecrets(t *testing.T) (ts *httptest.Server, pool *pgxpool.Pool, fakeAuthz *authztest.Fake, k8sClient client.Client, clock *testClock, cleanup func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	p, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if _, err := p.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
		p.Close()
		t.Fatalf("reset: %v", err)
	}
	if err := pipelineapidb.Migrate(context.Background(), p); err != nil {
		p.Close()
		t.Fatalf("migrate: %v", err)
	}

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	authzr := authztest.New()
	clk := &testClock{t: time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)}
	srv := apihttp.NewServer(p).
		WithUserResolver(auth.NewPostgresResolver(p, false)).
		WithAuthorizer(authzr).
		WithProjectReader(apihttp.NewPgxProjectReader(p, authzr)).
		WithSecrets(c, clk.now)
	server := httptest.NewServer(srv.Handler())
	return server, p, authzr, c, clk, func() { server.Close(); p.Close() }
}

func putSecretReq(t *testing.T, url, value string, cookie *stdhttp.Cookie) *stdhttp.Response {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"value": value})
	req, _ := stdhttp.NewRequest("PUT", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	return resp
}

func TestPutSecret_CreatesManagedSecretAndAnnotation(t *testing.T) {
	ts, pool, fakeAuthz, k8sClient, clock, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-put-secret"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/api_token", "s3cr3t", cookie)
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusNoContent {
		body, _ := readAll(resp)
		t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, body)
	}

	ns := pkg8s.NamespaceForProject(proj.ID)
	sec := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: pkg8s.ProjectSecretsName, Namespace: ns}, sec); err != nil {
		t.Fatalf("get managed secret: %v", err)
	}
	if string(sec.Data["api_token"]) != "s3cr3t" {
		t.Errorf("data[api_token] = %q, want s3cr3t", sec.Data["api_token"])
	}
	wantTS := clock.now().UTC().Format(time.RFC3339)
	if got := sec.Annotations["datuplet.io/updated-api_token"]; got != wantTS {
		t.Errorf("annotation = %q, want %q", got, wantTS)
	}
}

func TestPutSecret_SecondKeyPreservesFirst(t *testing.T) {
	ts, pool, fakeAuthz, k8sClient, clock, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-preserve"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp1 := putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/first", "one", cookie)
	resp1.Body.Close()
	if resp1.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("first put status = %d, want 204", resp1.StatusCode)
	}

	// Advance the clock so the second key's timestamp differs, proving each
	// PUT reads the injected clock at call time (not a value baked in once).
	clock.t = clock.t.Add(time.Hour)

	resp2 := putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/second", "two", cookie)
	resp2.Body.Close()
	if resp2.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("second put status = %d, want 204", resp2.StatusCode)
	}

	ns := pkg8s.NamespaceForProject(proj.ID)
	sec := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: pkg8s.ProjectSecretsName, Namespace: ns}, sec); err != nil {
		t.Fatalf("get managed secret: %v", err)
	}
	if string(sec.Data["first"]) != "one" {
		t.Errorf("data[first] = %q, want one (must survive the second PUT — merge-PATCH, not replace)", sec.Data["first"])
	}
	if string(sec.Data["second"]) != "two" {
		t.Errorf("data[second] = %q, want two", sec.Data["second"])
	}
	if sec.Annotations["datuplet.io/updated-first"] == sec.Annotations["datuplet.io/updated-second"] {
		t.Errorf("expected distinct timestamps, both = %q", sec.Annotations["datuplet.io/updated-first"])
	}
}

func TestListSecrets_NeverReturnsValues(t *testing.T) {
	ts, pool, fakeAuthz, _, clock, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-list"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	userStr := authz.UserObject(alice.ID.String()).String()
	fakeAuthz.Allow(userStr, "data_admin", authz.ProjectObject(lkID))
	fakeAuthz.Allow(userStr, "datuplet_member", authz.ProjectObject(lkID))

	putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/api_token", "s3cr3t-value-xyz", cookie).Body.Close()

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	rawBody, _ := readAll(resp)
	if strings.Contains(rawBody, "s3cr3t-value-xyz") {
		t.Fatalf("response body leaked the secret value: %s", rawBody)
	}

	var got []struct {
		Key       string `json:"key"`
		UpdatedAt string `json:"updatedAt"`
	}
	if err := json.Unmarshal([]byte(rawBody), &got); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rawBody)
	}
	if len(got) != 1 || got[0].Key != "api_token" {
		t.Fatalf("got = %+v, want one entry named api_token", got)
	}
	wantTS := clock.now().UTC().Format(time.RFC3339)
	if got[0].UpdatedAt != wantTS {
		t.Errorf("updatedAt = %q, want %q", got[0].UpdatedAt, wantTS)
	}
}

func TestListSecrets_AsViewer_Works(t *testing.T) {
	ts, pool, fakeAuthz, _, _, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-viewer-read"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	// Only datuplet_member — no data_admin.
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "datuplet_member", authz.ProjectObject(lkID))

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestPutSecret_AsViewer_Forbidden(t *testing.T) {
	ts, pool, fakeAuthz, _, _, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-viewer-write"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	// Only datuplet_member — no data_admin — must not be able to write.
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "datuplet_member", authz.ProjectObject(lkID))

	resp := putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/api_token", "s3cr3t", cookie)
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
}

func TestPutSecret_InvalidKey(t *testing.T) {
	ts, pool, fakeAuthz, _, _, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-invalid-key"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/bad%20key!", "x", cookie)
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestDeleteSecret_RemovesOneKeyOnly(t *testing.T) {
	ts, pool, fakeAuthz, k8sClient, _, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-delete"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/keep", "keepval", cookie).Body.Close()
	putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/drop", "dropval", cookie).Body.Close()

	req, _ := stdhttp.NewRequest("DELETE", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/drop", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}

	ns := pkg8s.NamespaceForProject(proj.ID)
	sec := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: pkg8s.ProjectSecretsName, Namespace: ns}, sec); err != nil {
		t.Fatalf("get managed secret: %v", err)
	}
	if _, exists := sec.Data["drop"]; exists {
		t.Errorf("data[drop] still present: %v", sec.Data)
	}
	if string(sec.Data["keep"]) != "keepval" {
		t.Errorf("data[keep] = %q, want keepval (delete must not touch other keys)", sec.Data["keep"])
	}
	if _, exists := sec.Annotations["datuplet.io/updated-drop"]; exists {
		t.Errorf("annotation for dropped key still present: %v", sec.Annotations)
	}

	// Deleting the already-absent key again must 404.
	req2, _ := stdhttp.NewRequest("DELETE", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/drop", nil)
	req2.AddCookie(cookie)
	resp2, err := stdhttp.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("second delete: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != stdhttp.StatusNotFound {
		t.Errorf("second delete status = %d, want 404", resp2.StatusCode)
	}
}

func TestDeleteSecret_AsViewer_Forbidden(t *testing.T) {
	ts, pool, fakeAuthz, _, _, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-viewer-delete"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "datuplet_member", authz.ProjectObject(lkID))

	req, _ := stdhttp.NewRequest("DELETE", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/anything", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
}

func readAll(resp *stdhttp.Response) (string, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(resp.Body)
	return buf.String(), err
}
