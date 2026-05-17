package http_test

import (
	"bytes"
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
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// freshCLITokenServer builds a pipeline-api Server with everything the
// cli-token handler needs: a signer (the route is gated on signer
// presence), a Postgres pool (the handler reads users from it), and the
// deploy-time cluster info the response embeds. Returns an httptest
// server, the pool (so tests can seed users / disable users without
// re-resolving via env), and a cleanup.
//
// `requireDB=true` — the test exercises a code path that reaches the
// DB; we skip cleanly when TEST_DATABASE_URL is unset (matching
// auth_handlers_test.go's convention).
//
// `requireDB=false` — the test only exercises pre-DB paths (rate limit,
// body validation). We still need a non-nil *pgxpool.Pool because the
// route is gated on `s.db != nil`, but pgxpool.New is lazy — it doesn't
// connect until a query runs. So we hand it a never-reachable DSN; if
// any test in this branch accidentally tries to query, the test fails
// with a connection error instead of silently passing on a stubbed DB.
func freshCLITokenServer(t *testing.T, requireDB bool) (ts *httptest.Server, pool *pgxpool.Pool, cleanup func()) {
	t.Helper()
	signer := mustNewSigner(t)

	dsn := os.Getenv("TEST_DATABASE_URL")
	useFakePool := dsn == "" && !requireDB
	if dsn == "" && requireDB {
		t.Skip("TEST_DATABASE_URL not set")
	}

	if useFakePool {
		// A DSN pgxpool.New accepts but never reaches; the lazy pool
		// stays valid for non-DB tests (rate limit, body validation)
		// and any actual query will surface a clear connection error.
		dsn = "postgres://x:x@127.0.0.1:1/x?sslmode=disable&connect_timeout=1"
	}

	var err error
	pool, err = pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if !useFakePool {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
			pool.Close()
			t.Fatalf("reset: %v", err)
		}
		if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
			pool.Close()
			t.Fatalf("migrate: %v", err)
		}
	}

	srv := apihttp.NewServer(pool).
		WithCookieSecure(false).
		WithSigner(signer).
		WithCLIClusterInfo("http://lakekeeper.test/catalog", "datuplet")
	ts = httptest.NewServer(srv.Handler())
	cleanup = func() {
		ts.Close()
		pool.Close()
	}
	return ts, pool, cleanup
}

// mustNewSigner returns a fresh in-memory RSA signer — convenient for
// handler tests that only need *something* signable, not a particular
// keypair.
func mustNewSigner(t *testing.T) *tokens.Signer {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	dir := t.TempDir()
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	path := filepath.Join(dir, "priv.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o400); err != nil {
		t.Fatalf("write: %v", err)
	}
	signer, err := tokens.LoadPrivateKeyFromPEMFile(path, "kid-cli-test")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return signer
}

func seedCLIUser(t *testing.T, pool *pgxpool.Pool, email, password string) {
	t.Helper()
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if _, err := store.CreateUser(context.Background(), pool, email, hash); err != nil {
		t.Fatalf("create user: %v", err)
	}
}

func disableUser(t *testing.T, pool *pgxpool.Pool, email string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE users SET disabled_at = NOW() WHERE email = $1`, email); err != nil {
		t.Fatalf("disable user: %v", err)
	}
}

// postCLIToken issues a POST against the cli-token endpoint with a JSON
// body. Returns the raw response so tests can inspect status + body.
func postCLIToken(t *testing.T, url string, body any) *stdhttp.Response {
	t.Helper()
	buf, _ := json.Marshal(body)
	req, _ := stdhttp.NewRequest("POST", url+"/api/v1/auth/token", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

// --- happy path ---

func TestHandleCLIToken_Success(t *testing.T) {
	ts, pool, cleanup := freshCLITokenServer(t, true)
	defer cleanup()
	seedCLIUser(t, pool, "alice@example.com", "hunter2")

	resp := postCLIToken(t, ts.URL, map[string]string{
		"email":    "alice@example.com",
		"password": "hunter2",
	})
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
		UserID    string `json:"user_id"`
		Cluster   struct {
			LakekeeperURL string `json:"lakekeeper_url"`
			WarehouseName string `json:"warehouse_name"`
		} `json:"cluster"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Token == "" {
		t.Error("token is empty")
	}
	if body.UserID == "" {
		t.Error("user_id is empty")
	}
	if body.Cluster.LakekeeperURL != "http://lakekeeper.test/catalog" {
		t.Errorf("cluster.lakekeeper_url = %q", body.Cluster.LakekeeperURL)
	}
	if body.Cluster.WarehouseName != "datuplet" {
		t.Errorf("cluster.warehouse_name = %q", body.Cluster.WarehouseName)
	}
	if body.ExpiresAt == "" {
		t.Error("expires_at is empty")
	}

	// Pin the JWT claim shape for the integration check between the HTTP
	// handler and MintLocalCLIToken.
	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(body.Token, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse JWT: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)
	if got, _ := claims["aud"].(string); got != tokens.TableTokenAudience {
		t.Errorf("aud = %q, want %q", got, tokens.TableTokenAudience)
	}
	if got, _ := claims["token_kind"].(string); got != tokens.TokenKindLocalCLI {
		t.Errorf("token_kind = %q, want %q", got, tokens.TokenKindLocalCLI)
	}
	if got, _ := claims["sub"].(string); got != body.UserID {
		t.Errorf("sub = %q, want %q (user_id)", got, body.UserID)
	}
}

// --- credential-failure paths return uniform 401 ---

func TestHandleCLIToken_WrongPassword(t *testing.T) {
	ts, pool, cleanup := freshCLITokenServer(t, true)
	defer cleanup()
	seedCLIUser(t, pool, "alice@example.com", "hunter2")

	resp := postCLIToken(t, ts.URL, map[string]string{
		"email":    "alice@example.com",
		"password": "wrong",
	})
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestHandleCLIToken_UnknownEmail(t *testing.T) {
	ts, _, cleanup := freshCLITokenServer(t, true)
	defer cleanup()

	resp := postCLIToken(t, ts.URL, map[string]string{
		"email":    "ghost@example.com",
		"password": "anything",
	})
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401 (uniform with wrong-password)", resp.StatusCode)
	}
}

func TestHandleCLIToken_DisabledUser(t *testing.T) {
	ts, pool, cleanup := freshCLITokenServer(t, true)
	defer cleanup()
	seedCLIUser(t, pool, "alice@example.com", "hunter2")
	disableUser(t, pool, "alice@example.com")

	resp := postCLIToken(t, ts.URL, map[string]string{
		"email":    "alice@example.com",
		"password": "hunter2",
	})
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401 (disabled users uniform with wrong-pw)", resp.StatusCode)
	}
}

// --- request-shape validation ---

// TestHandleCLIToken_BodyValidation asserts 400 for bodies missing
// required fields or malformed JSON. Distinct from 401 so a CLI client
// can distinguish "you forgot a field" from "those credentials are
// wrong".
//
// This test exercises the validation path that fires BEFORE the DB
// lookup, so it runs without TEST_DATABASE_URL.
func TestHandleCLIToken_BodyValidation(t *testing.T) {
	ts, _, cleanup := freshCLITokenServer(t, false)
	defer cleanup()

	cases := []map[string]string{
		{"email": "", "password": "x"},
		{"email": "x@y.z", "password": ""},
		{"email": "", "password": ""},
	}
	for _, body := range cases {
		resp := postCLIToken(t, ts.URL, body)
		_ = resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("body=%v: status = %d, want 400", body, resp.StatusCode)
		}
	}

	// Malformed JSON also returns 400.
	req, _ := stdhttp.NewRequest("POST", ts.URL+"/api/v1/auth/token", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("malformed JSON: status = %d, want 400", resp.StatusCode)
	}

	// Body > 4 KiB must return 400 (MaxBytesReader cap; F2 security fix).
	// The handler wraps r.Body in http.MaxBytesReader(4096) before readJSON,
	// so an oversize body causes json.Decode to return a *http.MaxBytesError
	// which the handler maps to a 400.
	oversizeBody := strings.Repeat("x", 4097)
	req2, _ := stdhttp.NewRequest("POST", ts.URL+"/api/v1/auth/token", strings.NewReader(oversizeBody))
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := stdhttp.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("oversize post: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != 400 {
		t.Errorf("oversize body: status = %d, want 400", resp2.StatusCode)
	}
}

// TestHandleCLIToken_RateLimit drives 11 requests against the same IP
// (httptest.Server with no proxy → all loopback) and asserts that the
// 11th gets 429. This is the documented per-IP fixed-window cap (10/min).
//
// The rate limiter is keyed on the IP portion of RemoteAddr — port
// stripped — so all requests in this test share a bucket regardless of
// ephemeral-port differences.
//
// Rate-limit fires BEFORE the DB lookup (P1 security requirement: argon2
// CPU must not be wasted on brute-force attempts), so this test runs
// without TEST_DATABASE_URL.
func TestHandleCLIToken_RateLimit(t *testing.T) {
	ts, _, cleanup := freshCLITokenServer(t, false)
	defer cleanup()

	// 10 valid-shape requests (status doesn't matter — they hit the rate
	// limiter BEFORE the DB lookup, then 401 on unknown email or 400 on
	// empty password). The 11th MUST be 429.
	body := map[string]string{"email": "x@y.z", "password": "x"}
	for i := 0; i < 10; i++ {
		resp := postCLIToken(t, ts.URL, body)
		_ = resp.Body.Close()
		if resp.StatusCode == 429 {
			t.Fatalf("request #%d unexpectedly rate-limited (want 429 only on #11+)", i+1)
		}
	}
	resp := postCLIToken(t, ts.URL, body)
	defer resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Errorf("11th request status = %d, want 429", resp.StatusCode)
	}
}
