package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// freshServer spins up a Postgres-backed handler stack with a fake FGA
// authorizer. The returned *authztest.Fake is the caller's seam to seed
// per-user FGA tuples so the project/pipeline/run handlers let requests through.
func freshServer(t *testing.T) (*httptest.Server, *pgxpool.Pool, *authztest.Fake, func()) {
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
	authzr := authztest.New()
	srv := apihttp.NewServer(pool).
		WithUserResolver(auth.NewPostgresResolver(pool, false)).
		WithAuthorizer(authzr).
		WithProjectReader(apihttp.NewPgxProjectReader(pool, authzr)).
		WithPipelineStore(apihttp.NewPgxPipelineStore(pool)).
		WithRunReader(apihttp.NewPgxRunReader(pool))
	ts := httptest.NewServer(srv.Handler())
	cleanup := func() {
		ts.Close()
		pool.Close()
	}
	return ts, pool, authzr, cleanup
}

func seedUser(t *testing.T, pool *pgxpool.Pool, email, password string) {
	t.Helper()
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if _, err := store.CreateUser(context.Background(), pool, email, hash); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func postJSON(t *testing.T, url string, body any, cookies ...*stdhttp.Cookie) *stdhttp.Response {
	t.Helper()
	buf, _ := json.Marshal(body)
	req, _ := stdhttp.NewRequest("POST", url, bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	for _, c := range cookies {
		req.AddCookie(c)
	}
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func TestLogin_Success(t *testing.T) {
	ts, pool, _, cleanup := freshServer(t)
	defer cleanup()
	seedUser(t, pool, "alice@example.com", "hunter2")

	resp := postJSON(t, ts.URL+"/api/v1/auth/login",
		map[string]string{"email": "alice@example.com", "password": "hunter2"})
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}
	var found bool
	for _, c := range resp.Cookies() {
		if c.Name == auth.SessionCookieName {
			found = true
			if c.Value == "" {
				t.Error("session cookie is empty")
			}
			if !c.HttpOnly {
				t.Error("session cookie must be HttpOnly")
			}
		}
	}
	if !found {
		t.Error("session cookie not set")
	}
}

func TestLogin_WrongPassword(t *testing.T) {
	ts, pool, _, cleanup := freshServer(t)
	defer cleanup()
	seedUser(t, pool, "alice@example.com", "hunter2")

	resp := postJSON(t, ts.URL+"/api/v1/auth/login",
		map[string]string{"email": "alice@example.com", "password": "wrong"})
	defer resp.Body.Close()

	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
	for _, c := range resp.Cookies() {
		if c.Name == auth.SessionCookieName {
			t.Error("session cookie must not be set on login failure")
		}
	}
}

func TestLogin_UnknownEmail_UniformError(t *testing.T) {
	ts, _, _, cleanup := freshServer(t)
	defer cleanup()

	resp := postJSON(t, ts.URL+"/api/v1/auth/login",
		map[string]string{"email": "nobody@example.com", "password": "x"})
	defer resp.Body.Close()

	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401 (uniform with wrong-password)", resp.StatusCode)
	}
}

func TestMe_RequiresSession(t *testing.T) {
	ts, _, _, cleanup := freshServer(t)
	defer cleanup()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/auth/me")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestMe_ReturnsLoggedInUser(t *testing.T) {
	ts, pool, _, cleanup := freshServer(t)
	defer cleanup()
	seedUser(t, pool, "alice@example.com", "hunter2")

	loginResp := postJSON(t, ts.URL+"/api/v1/auth/login",
		map[string]string{"email": "alice@example.com", "password": "hunter2"})
	loginResp.Body.Close()
	var sessionCookie *stdhttp.Cookie
	for _, c := range loginResp.Cookies() {
		if c.Name == auth.SessionCookieName {
			sessionCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("login did not set a session cookie")
	}

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/auth/me", nil)
	req.AddCookie(sessionCookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("me: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["email"] != "alice@example.com" {
		t.Errorf("email = %q, want alice@example.com", body["email"])
	}
}

func TestLogout_DestroysSession(t *testing.T) {
	ts, pool, _, cleanup := freshServer(t)
	defer cleanup()
	seedUser(t, pool, "alice@example.com", "hunter2")

	loginResp := postJSON(t, ts.URL+"/api/v1/auth/login",
		map[string]string{"email": "alice@example.com", "password": "hunter2"})
	loginResp.Body.Close()
	var sessionCookie *stdhttp.Cookie
	for _, c := range loginResp.Cookies() {
		if c.Name == auth.SessionCookieName {
			sessionCookie = c
		}
	}

	logoutResp := postJSON(t, ts.URL+"/api/v1/auth/logout", nil, sessionCookie)
	logoutResp.Body.Close()
	if logoutResp.StatusCode != 204 {
		t.Errorf("logout status = %d, want 204", logoutResp.StatusCode)
	}

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/auth/me", nil)
	req.AddCookie(sessionCookie)
	meResp, _ := stdhttp.DefaultClient.Do(req)
	meResp.Body.Close()
	if meResp.StatusCode != 401 {
		t.Errorf("me after logout status = %d, want 401", meResp.StatusCode)
	}
}
