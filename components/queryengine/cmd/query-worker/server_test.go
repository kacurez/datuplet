// No build tag: server.go + server_test.go are CGO-free (they depend only on
// untagged types.go via the Runner interface, not on engine.go which is
// duckdb_arrow-tagged).

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto/rand"
	"crypto/rsa"

	"github.com/golang-jwt/jwt/v5"

	"github.com/datuplet/datuplet/components/queryengine"
)

// ---- test helpers ----

// fakeRunner is a RunnerFunc stub that records whether it was called and
// returns a preconfigured result/error.
type fakeRunner struct {
	mu      sync.Mutex
	called  bool
	req     queryengine.Request
	result  *queryengine.Result
	err     error
	blocked chan struct{} // if non-nil, Run blocks until closed
	onCall  func()       // if non-nil, called at the very start of Run (before blocking)
}

func (f *fakeRunner) Run(ctx context.Context, r queryengine.Request) (*queryengine.Result, error) {
	f.mu.Lock()
	f.called = true
	f.req = r
	f.mu.Unlock()
	if f.onCall != nil {
		f.onCall()
	}
	if f.blocked != nil {
		<-f.blocked
	}
	return f.result, f.err
}

func (f *fakeRunner) wasCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

func (f *fakeRunner) lastReq() queryengine.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.req
}

// echoResult is a convenient fixed result for success tests.
var echoResult = &queryengine.Result{
	Schema: []queryengine.Column{{Name: "n", Type: "BIGINT"}},
	Rows:   [][]any{{int64(42)}},
	Stats:  queryengine.Stats{DurationMs: 1},
}

// genTestKey generates an RSA key for tests.
func genTestKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return priv
}

// stubKeys is a KeyProvider backed by a simple map for test servers.
type stubKeys struct {
	keys map[string]*rsa.PublicKey
}

func (s *stubKeys) KeyFor(_ context.Context, kid string) (*rsa.PublicKey, error) {
	k, ok := s.keys[kid]
	if !ok {
		return nil, errors.New("stub: kid not found: " + kid)
	}
	return k, nil
}

const testServerKID = "srv-test-kid"

// mintTestToken mints a valid internal-query JWT signed with priv.
func mintTestToken(t *testing.T, priv *rsa.PrivateKey) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        queryWorkerAudience,
		"sub":        "test-sub-uuid",
		"token_kind": tokenKindInternalQuery,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(5 * time.Minute).Unix(),
		"jti":        fmt.Sprintf("test-jti-%d", now.UnixNano()),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = testServerKID
	s, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign test token: %v", err)
	}
	return s
}

// workerCfg returns a minimal workerConfig for tests.
func testWorkerCfg() workerConfig {
	return workerConfig{
		LakekeeperURL:  "http://lakekeeper.test",
		MaxConcurrency: 2,
		MaxTimeoutS:    300,
		MaxRows:        10000,
		MaxBytes:       10 * 1024 * 1024,
	}
}

// newTestServer builds a *queryServer with a stub runner and stub key provider.
func newTestServer(t *testing.T, priv *rsa.PrivateKey, runner Runner, cfg workerConfig) *queryServer {
	t.Helper()
	keys := &stubKeys{keys: map[string]*rsa.PublicKey{testServerKID: &priv.PublicKey}}
	verifier := newTokenVerifier(keys)
	return newQueryServer(verifier, runner, cfg)
}

// doPost sends a POST /internal/query with the given body and bearer token.
func doPost(t *testing.T, srv *queryServer, body any, token string) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/query", &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

// decodeError decodes a {"error":...,"kind":...} response body.
func decodeError(t *testing.T, body string) (errMsg, kind string) {
	t.Helper()
	var v struct {
		Error string `json:"error"`
		Kind  string `json:"kind"`
	}
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("decode error body %q: %v", body, err)
	}
	return v.Error, v.Kind
}

// ---- tests ----

// Test 1: valid token + fake runner → 200 with schema/rows.
func TestServer_ValidToken_Returns200(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := map[string]any{
		"sql":         "SELECT 42",
		"catalog_jwt": "hdr.pld.sig",
		"warehouse":   "proj/wh",
	}
	resp := doPost(t, srv, body, token)

	if resp.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.Code, resp.Body.String())
	}

	var result queryengine.Result
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if len(result.Schema) != 1 || result.Schema[0].Name != "n" {
		t.Errorf("unexpected schema: %+v", result.Schema)
	}
	if len(result.Rows) != 1 {
		t.Errorf("unexpected rows: %+v", result.Rows)
	}
	if !runner.wasCalled() {
		t.Error("runner was not called")
	}
}

// Test 2a: missing Authorization header → 401, runner NOT called.
func TestServer_MissingToken_Returns401(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	body := map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}
	resp := doPost(t, srv, body, "" /* no token */)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.Code)
	}
	_, kind := decodeError(t, resp.Body.String())
	if kind != "unauthorized" {
		t.Errorf("kind = %q, want unauthorized", kind)
	}
	if runner.wasCalled() {
		t.Error("runner should NOT have been called on 401")
	}
}

// Test 2b: garbage token → 401, runner NOT called.
func TestServer_GarbageToken_Returns401(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	body := map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}
	resp := doPost(t, srv, body, "not-a-jwt-at-all")

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d: %s", resp.Code, resp.Body.String())
	}
	if runner.wasCalled() {
		t.Error("runner should NOT have been called on bad token")
	}
}

// Test 3: semaphore — concurrency=1, in-flight request blocks second → 429.
func TestServer_SemaphoreFull_Returns429(t *testing.T) {
	priv := genTestKey(t)
	gate := make(chan struct{})
	running := make(chan struct{}) // closed by onCall when the first Run starts

	blocker := &fakeRunner{
		blocked: gate,
		result:  echoResult,
		onCall: func() {
			// Signal that the first request has entered Run (semaphore is held).
			close(running)
		},
	}

	cfg := testWorkerCfg()
	cfg.MaxConcurrency = 1
	srv := newTestServer(t, priv, blocker, cfg)

	token := mintTestToken(t, priv)
	body := map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}

	// First request: in flight (runner blocks on gate).
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		resp := doPost(t, srv, body, token)
		firstDone <- resp
	}()

	// Wait deterministically for the first request to hold the semaphore.
	<-running

	// Second request: should get 429 immediately.
	resp2 := doPost(t, srv, body, token)
	if resp2.Code != http.StatusTooManyRequests {
		t.Errorf("want 429, got %d: %s", resp2.Code, resp2.Body.String())
	}
	_, kind := decodeError(t, resp2.Body.String())
	if kind != "capacity" {
		t.Errorf("kind = %q, want capacity", kind)
	}
	retryAfter := resp2.Header().Get("Retry-After")
	if retryAfter == "" {
		t.Error("missing Retry-After header on 429")
	}

	// Unblock the first request.
	close(gate)
	<-firstDone
}

// Test 4a: ErrTimeout from runner → 408 kind=timeout.
func TestServer_RunnerTimeout_Returns408(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{err: fmt.Errorf("timeout: %w", queryengine.ErrTimeout)}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}
	resp := doPost(t, srv, body, token)

	if resp.Code != http.StatusRequestTimeout {
		t.Fatalf("want 408, got %d: %s", resp.Code, resp.Body.String())
	}
	_, kind := decodeError(t, resp.Body.String())
	if kind != "timeout" {
		t.Errorf("kind = %q, want timeout", kind)
	}
}

// Test 4b: ErrResultTooLarge from runner → 413 kind=result_too_large.
func TestServer_RunnerResultTooLarge_Returns413(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{err: fmt.Errorf("big: %w", queryengine.ErrResultTooLarge)}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}
	resp := doPost(t, srv, body, token)

	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d: %s", resp.Code, resp.Body.String())
	}
	_, kind := decodeError(t, resp.Body.String())
	if kind != "result_too_large" {
		t.Errorf("kind = %q, want result_too_large", kind)
	}
}

// Test 4c: plain error from runner → 400 kind=sql_error.
func TestServer_RunnerPlainError_Returns400(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{err: errors.New("syntax error near 'SELEC'")}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := map[string]any{"sql": "SELEC 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}
	resp := doPost(t, srv, body, token)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", resp.Code, resp.Body.String())
	}
	_, kind := decodeError(t, resp.Body.String())
	if kind != "sql_error" {
		t.Errorf("kind = %q, want sql_error", kind)
	}
}

// Test 5a: body timeout_s=9999 → runner receives Timeout==cfg.MaxTimeoutS.
func TestServer_TimeoutClamped(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	cfg := testWorkerCfg()
	cfg.MaxTimeoutS = 300
	srv := newTestServer(t, priv, runner, cfg)

	token := mintTestToken(t, priv)
	body := map[string]any{
		"sql":         "SELECT 1",
		"catalog_jwt": "a.b.c",
		"warehouse":   "p/w",
		"timeout_s":   9999,
	}
	resp := doPost(t, srv, body, token)
	if resp.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.Code, resp.Body.String())
	}
	req := runner.lastReq()
	wantTimeout := time.Duration(cfg.MaxTimeoutS) * time.Second
	if req.Timeout != wantTimeout {
		t.Errorf("Timeout = %v, want %v", req.Timeout, wantTimeout)
	}
}

// Test 5b: max_rows/max_bytes clamped to cfg ceilings.
func TestServer_MaxRowsMaxBytesClamped(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	cfg := testWorkerCfg()
	cfg.MaxRows = 1000
	cfg.MaxBytes = 1024 * 1024
	srv := newTestServer(t, priv, runner, cfg)

	token := mintTestToken(t, priv)
	body := map[string]any{
		"sql":         "SELECT 1",
		"catalog_jwt": "a.b.c",
		"warehouse":   "p/w",
		"max_rows":    999999,
		"max_bytes":   999999999,
	}
	resp := doPost(t, srv, body, token)
	if resp.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.Code, resp.Body.String())
	}
	req := runner.lastReq()
	if req.MaxRows != cfg.MaxRows {
		t.Errorf("MaxRows = %d, want %d (ceiling)", req.MaxRows, cfg.MaxRows)
	}
	if req.MaxBytes != cfg.MaxBytes {
		t.Errorf("MaxBytes = %d, want %d (ceiling)", req.MaxBytes, cfg.MaxBytes)
	}
}

// Test 5c: body values below ceiling pass through unchanged.
func TestServer_MaxRowsMaxBytesUnderCeiling(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	cfg := testWorkerCfg()
	cfg.MaxRows = 10000
	cfg.MaxBytes = 10 * 1024 * 1024
	srv := newTestServer(t, priv, runner, cfg)

	token := mintTestToken(t, priv)
	body := map[string]any{
		"sql":         "SELECT 1",
		"catalog_jwt": "a.b.c",
		"warehouse":   "p/w",
		"max_rows":    50,
		"max_bytes":   512,
	}
	resp := doPost(t, srv, body, token)
	if resp.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.Code)
	}
	req := runner.lastReq()
	if req.MaxRows != 50 {
		t.Errorf("MaxRows = %d, want 50", req.MaxRows)
	}
	if req.MaxBytes != 512 {
		t.Errorf("MaxBytes = %d, want 512", req.MaxBytes)
	}
}

// Test 6: body >1MiB → 413 kind=request_too_large.
func TestServer_BodyTooLarge(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)

	// Build a body larger than 1MiB. Stuff the padding into the sql field.
	padding := strings.Repeat("x", 2*1024*1024) // 2MiB
	body := fmt.Sprintf(`{"sql":%q,"catalog_jwt":"a.b.c","warehouse":"p/w"}`, padding)

	req := httptest.NewRequest(http.MethodPost, "/internal/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// MaxBytesReader returns *http.MaxBytesError; we map it to 413 request_too_large.
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413 for oversized body, got %d: %s", w.Code, w.Body.String())
	}
	_, kind := decodeError(t, w.Body.String())
	if kind != "request_too_large" {
		t.Errorf("kind = %q, want request_too_large", kind)
	}
	if runner.wasCalled() {
		t.Error("runner should not be called for oversized body")
	}
}

// Test 7a: missing sql → 400.
func TestServer_MissingSQL_Returns400(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := map[string]any{"catalog_jwt": "a.b.c", "warehouse": "p/w"}
	resp := doPost(t, srv, body, token)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", resp.Code, resp.Body.String())
	}
	_, kind := decodeError(t, resp.Body.String())
	if kind != "bad_request" {
		t.Errorf("kind = %q, want bad_request", kind)
	}
}

// Test 7b: missing catalog_jwt → 400.
func TestServer_MissingCatalogJWT_Returns400(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := map[string]any{"sql": "SELECT 1", "warehouse": "p/w"}
	resp := doPost(t, srv, body, token)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", resp.Code, resp.Body.String())
	}
}

// Test 7c: missing warehouse → 400.
func TestServer_MissingWarehouse_Returns400(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c"}
	resp := doPost(t, srv, body, token)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", resp.Code, resp.Body.String())
	}
}

// Test 8: GET /healthz → 200 "ok" (no auth required).
func TestServer_Healthz(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ok") {
		t.Errorf("want body containing 'ok', got %q", w.Body.String())
	}
}

// Test 9: malformed JSON body → 400 kind=bad_request.
func TestServer_MalformedJSON_Returns400(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	req := httptest.NewRequest(http.MethodPost, "/internal/query", strings.NewReader("{not valid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	_, kind := decodeError(t, w.Body.String())
	if kind != "bad_request" {
		t.Errorf("kind = %q, want bad_request", kind)
	}
	if runner.wasCalled() {
		t.Error("runner should not be called on malformed JSON")
	}
}

// Test 10: timeout_s=0 in body → runner receives cfg.MaxTimeoutS (default applies).
func TestServer_ZeroTimeoutUsesDefault(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{result: echoResult}
	cfg := testWorkerCfg()
	cfg.MaxTimeoutS = 120
	srv := newTestServer(t, priv, runner, cfg)

	token := mintTestToken(t, priv)
	body := map[string]any{
		"sql":         "SELECT 1",
		"catalog_jwt": "a.b.c",
		"warehouse":   "p/w",
		"timeout_s":   0, // zero → use max
	}
	resp := doPost(t, srv, body, token)
	if resp.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.Code)
	}
	req := runner.lastReq()
	wantTimeout := time.Duration(cfg.MaxTimeoutS) * time.Second
	if req.Timeout != wantTimeout {
		t.Errorf("Timeout = %v, want %v", req.Timeout, wantTimeout)
	}
}
