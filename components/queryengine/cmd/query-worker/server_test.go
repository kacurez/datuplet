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
	onCall  func()        // if non-nil, called at the very start of Run (before blocking)
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

// postRaw sends a POST /internal/query with a raw string body.
func postRaw(t *testing.T, srv *queryServer, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/internal/query", strings.NewReader(body))
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

// Missing / garbage Authorization → 401 kind=unauthorized, runner NOT called.
// Security boundary: neither token shape may reach the runner.
func TestServer_BadToken_Returns401(t *testing.T) {
	cases := []struct {
		name  string
		token string
	}{
		{name: "missing token", token: ""},
		{name: "garbage token", token: "not-a-jwt-at-all"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			priv := genTestKey(t)
			runner := &fakeRunner{result: echoResult}
			srv := newTestServer(t, priv, runner, testWorkerCfg())

			body := map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}
			resp := doPost(t, srv, body, tc.token)

			if resp.Code != http.StatusUnauthorized {
				t.Fatalf("want 401, got %d: %s", resp.Code, resp.Body.String())
			}
			_, kind := decodeError(t, resp.Body.String())
			if kind != "unauthorized" {
				t.Errorf("kind = %q, want unauthorized", kind)
			}
			if runner.wasCalled() {
				t.Error("runner should NOT have been called on 401")
			}
		})
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

// Runner errors map to HTTP status + kind: ErrTimeout → 408, ErrResultTooLarge
// → 413, any other error → 400 sql_error.
func TestServer_RunnerErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		sql        string
		wantStatus int
		wantKind   string
	}{
		{"timeout", fmt.Errorf("timeout: %w", queryengine.ErrTimeout), "SELECT 1", http.StatusRequestTimeout, "timeout"},
		{"result too large", fmt.Errorf("big: %w", queryengine.ErrResultTooLarge), "SELECT 1", http.StatusRequestEntityTooLarge, "result_too_large"},
		{"plain error", errors.New("syntax error near 'SELEC'"), "SELEC 1", http.StatusBadRequest, "sql_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			priv := genTestKey(t)
			runner := &fakeRunner{err: tc.err}
			srv := newTestServer(t, priv, runner, testWorkerCfg())

			token := mintTestToken(t, priv)
			body := map[string]any{"sql": tc.sql, "catalog_jwt": "a.b.c", "warehouse": "p/w"}
			resp := doPost(t, srv, body, token)

			if resp.Code != tc.wantStatus {
				t.Fatalf("want %d, got %d: %s", tc.wantStatus, resp.Code, resp.Body.String())
			}
			_, kind := decodeError(t, resp.Body.String())
			if kind != tc.wantKind {
				t.Errorf("kind = %q, want %q", kind, tc.wantKind)
			}
		})
	}
}

// Request limits: body values are clamped to the cfg ceilings; values under
// the ceiling and absent/zero values behave as documented.
func TestServer_RequestLimits(t *testing.T) {
	cases := []struct {
		name        string
		cfg         func(c *workerConfig)
		extra       map[string]any
		wantTimeout time.Duration
		wantRows    int
		wantBytes   int
	}{
		{
			name:        "timeout_s above ceiling clamped",
			cfg:         func(c *workerConfig) { c.MaxTimeoutS = 300 },
			extra:       map[string]any{"timeout_s": 9999},
			wantTimeout: 300 * time.Second,
			wantRows:    10000,
			wantBytes:   10 * 1024 * 1024,
		},
		{
			name:        "max_rows/max_bytes above ceilings clamped",
			cfg:         func(c *workerConfig) { c.MaxRows = 1000; c.MaxBytes = 1024 * 1024 },
			extra:       map[string]any{"max_rows": 999999, "max_bytes": 999999999},
			wantTimeout: 300 * time.Second,
			wantRows:    1000,
			wantBytes:   1024 * 1024,
		},
		{
			name:        "values under ceilings pass through",
			cfg:         func(c *workerConfig) { c.MaxRows = 10000; c.MaxBytes = 10 * 1024 * 1024 },
			extra:       map[string]any{"max_rows": 50, "max_bytes": 512},
			wantTimeout: 300 * time.Second,
			wantRows:    50,
			wantBytes:   512,
		},
		{
			name:        "timeout_s=0 uses default (cfg max)",
			cfg:         func(c *workerConfig) { c.MaxTimeoutS = 120 },
			extra:       map[string]any{"timeout_s": 0},
			wantTimeout: 120 * time.Second,
			wantRows:    10000,
			wantBytes:   10 * 1024 * 1024,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			priv := genTestKey(t)
			runner := &fakeRunner{result: echoResult}
			cfg := testWorkerCfg()
			tc.cfg(&cfg)
			srv := newTestServer(t, priv, runner, cfg)

			body := map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}
			for k, v := range tc.extra {
				body[k] = v
			}
			resp := doPost(t, srv, body, mintTestToken(t, priv))
			if resp.Code != http.StatusOK {
				t.Fatalf("want 200, got %d: %s", resp.Code, resp.Body.String())
			}
			req := runner.lastReq()
			if req.Timeout != tc.wantTimeout {
				t.Errorf("Timeout = %v, want %v", req.Timeout, tc.wantTimeout)
			}
			if req.MaxRows != tc.wantRows {
				t.Errorf("MaxRows = %d, want %d", req.MaxRows, tc.wantRows)
			}
			if req.MaxBytes != tc.wantBytes {
				t.Errorf("MaxBytes = %d, want %d", req.MaxBytes, tc.wantBytes)
			}
		})
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

// Missing required fields and malformed JSON → 400 kind=bad_request, runner
// NOT called.
func TestServer_InvalidRequest_Returns400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "missing sql", body: `{"catalog_jwt":"a.b.c","warehouse":"p/w"}`},
		{name: "missing catalog_jwt", body: `{"sql":"SELECT 1","warehouse":"p/w"}`},
		{name: "missing warehouse", body: `{"sql":"SELECT 1","catalog_jwt":"a.b.c"}`},
		{name: "malformed JSON", body: "{not valid json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			priv := genTestKey(t)
			runner := &fakeRunner{result: echoResult}
			srv := newTestServer(t, priv, runner, testWorkerCfg())

			w := postRaw(t, srv, tc.body, mintTestToken(t, priv))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", w.Code, w.Body.String())
			}
			_, kind := decodeError(t, w.Body.String())
			if kind != "bad_request" {
				t.Errorf("kind = %q, want bad_request", kind)
			}
			if runner.wasCalled() {
				t.Error("runner should not be called on an invalid request")
			}
		})
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

// Test 11: GET /metrics → 200, body exposes inflight + capacity gauges.
func TestMetricsEndpoint(t *testing.T) {
	priv := genTestKey(t)
	ok := RunnerFunc(func(_ context.Context, _ queryengine.Request) (*queryengine.Result, error) {
		return &queryengine.Result{Rows: [][]any{}}, nil
	})
	srv := newTestServer(t, priv, ok, workerConfig{MaxConcurrency: 3})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "query_worker_capacity_slots 3") {
		t.Fatalf("metrics missing capacity gauge:\n%s", body)
	}
	if !strings.Contains(body, "query_worker_inflight 0") {
		t.Fatalf("metrics missing inflight gauge:\n%s", body)
	}
}

// queryBody is the minimal valid /internal/query body (the fake runner
// never attaches, so the JWT/warehouse just need to be non-empty).
var queryBody = map[string]any{"sql": "SELECT 1", "catalog_jwt": "a.b.c", "warehouse": "p/w"}

// Test 12a: bounded-wait admission — a slot that frees inside the window
// admits the queued request instead of rejecting it immediately.
func TestAdmission_BoundedWaitAdmitsWhenSlotFrees(t *testing.T) {
	priv := genTestKey(t)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	slow := RunnerFunc(func(_ context.Context, _ queryengine.Request) (*queryengine.Result, error) {
		started <- struct{}{}
		<-release
		return &queryengine.Result{Rows: [][]any{}}, nil
	})
	srv := newTestServer(t, priv, slow, workerConfig{MaxConcurrency: 1, AdmissionWait: 500 * time.Millisecond})
	tok := mintTestToken(t, priv)

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- doPost(t, srv, queryBody, tok) }()
	<-started // first request now holds the only slot

	// Second request arrives while full; the slot frees inside the window.
	time.AfterFunc(100*time.Millisecond, func() { close(release) })
	rec := doPost(t, srv, queryBody, tok)
	if rec.Code != http.StatusOK {
		t.Fatalf("second request = %d, want 200 (admitted after bounded wait); body=%s", rec.Code, rec.Body.String())
	}
	<-firstDone
}

// Test 12b: bounded-wait admission — a slot that never frees within the
// window still 429s, with a Retry-After header.
func TestAdmission_BoundedWaitExpires429(t *testing.T) {
	priv := genTestKey(t)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	slow := RunnerFunc(func(_ context.Context, _ queryengine.Request) (*queryengine.Result, error) {
		started <- struct{}{}
		<-release
		return &queryengine.Result{Rows: [][]any{}}, nil
	})
	srv := newTestServer(t, priv, slow, workerConfig{MaxConcurrency: 1, AdmissionWait: 50 * time.Millisecond})
	tok := mintTestToken(t, priv)

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- doPost(t, srv, queryBody, tok) }()
	<-started

	rec := doPost(t, srv, queryBody, tok) // window expires — slot never frees
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 after AdmissionWait expiry; body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("Retry-After header missing on 429")
	}
	close(release)
	<-firstDone
}

// TestSanitizeEngineError: DuckDB's native error message appends a
// "\nLINE N: <SQL>\n   ^" echo of the offending statement, which may carry
// sensitive literals (RFC 025 Codex review F1). sanitizeEngineError must
// keep the leading diagnostic (used by callers to distinguish error kinds)
// and drop everything from the LINE echo onward.
func TestSanitizeEngineError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "line_echo_stripped",
			err: errors.New("Catalog Error: Table with name secret_table does not exist.\n" +
				"\nLINE 1: SELECT secret_column FROM \"ns\".\"secret_table\"\n" +
				"                                     ^"),
			want: "Catalog Error: Table with name secret_table does not exist.",
		},
		{
			name: "no_line_echo_passthrough",
			err:  errors.New("connection refused"),
			want: "connection refused",
		},
		{
			name: "wrapped_timeout_no_line_echo",
			err:  fmt.Errorf("timeout: %w", queryengine.ErrTimeout),
			want: "timeout: query timed out",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeEngineError(tc.err); got != tc.want {
				t.Errorf("sanitizeEngineError(%q) = %q, want %q", tc.err.Error(), got, tc.want)
			}
			if strings.Contains(sanitizeEngineError(tc.err), "secret_column") {
				t.Errorf("sanitized error must not contain the SQL echo: %q", sanitizeEngineError(tc.err))
			}
		})
	}
}

// TestServer_RunnerError_LogAndBodyStripSQLEcho: a DuckDB-shaped error
// containing a LINE echo of the SQL text must not leak that echo into the
// HTTP error body (the worker log isn't observable from this test, but
// sanitizeEngineError is the single choke point both use).
func TestServer_RunnerError_LogAndBodyStripSQLEcho(t *testing.T) {
	priv := genTestKey(t)
	duckdbErr := errors.New("Catalog Error: Table with name nonexistent_t does not exist.\n" +
		"\nLINE 1: SELECT * FROM \"ns\".\"nonexistent_t\" WHERE ssn = '123-45-6789'\n" +
		"                       ^")
	runner := &fakeRunner{err: duckdbErr}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := map[string]any{"sql": "SELECT * FROM \"ns\".\"nonexistent_t\" WHERE ssn = '123-45-6789'", "catalog_jwt": "a.b.c", "warehouse": "p/w"}
	resp := doPost(t, srv, body, token)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", resp.Code, resp.Body.String())
	}
	errMsg, kind := decodeError(t, resp.Body.String())
	if kind != "sql_error" {
		t.Errorf("kind = %q, want sql_error", kind)
	}
	if !strings.Contains(errMsg, "nonexistent_t") {
		t.Errorf("error body should still name the offending table, got: %q", errMsg)
	}
	if strings.Contains(errMsg, "LINE ") || strings.Contains(errMsg, "123-45-6789") {
		t.Errorf("error body must not echo the SQL statement, got: %q", errMsg)
	}
}

// ---- RFC 028 Q2: bound parameters ----

// RFC 028 Q2: a well-formed `params` object passes validation and reaches the
// engine Request verbatim, with JSON numbers decoded as json.Number (UseNumber)
// so the §6.1 integral/non-integral distinction survives the decode. A body
// without `params` behaves exactly as before: a nil Params map and untouched
// limit clamping.
func TestServer_Params(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		check func(t *testing.T, resp *httptest.ResponseRecorder, req queryengine.Request)
	}{
		{
			name: "forwarded to runner",
			body: `{"sql":"SELECT $country AS c, $days AS d, $ratio AS r, $flag AS f, $nul AS n",` +
				`"catalog_jwt":"a.b.c","warehouse":"p/w",` +
				`"params":{"country":"DE","days":30,"ratio":1.5,"flag":true,"nul":null}}`,
			check: func(t *testing.T, _ *httptest.ResponseRecorder, req queryengine.Request) {
				got := req.Params
				if len(got) != 5 {
					t.Fatalf("Params = %#v, want 5 entries", got)
				}
				if got["country"] != "DE" {
					t.Errorf("params[country] = %v (%T), want string \"DE\"", got["country"], got["country"])
				}
				if n, ok := got["days"].(json.Number); !ok || n.String() != "30" {
					t.Errorf("params[days] = %v (%T), want json.Number(\"30\") — the decoder must UseNumber()", got["days"], got["days"])
				}
				if n, ok := got["ratio"].(json.Number); !ok || n.String() != "1.5" {
					t.Errorf("params[ratio] = %v (%T), want json.Number(\"1.5\")", got["ratio"], got["ratio"])
				}
				if got["flag"] != true {
					t.Errorf("params[flag] = %v (%T), want true", got["flag"], got["flag"])
				}
				if v, ok := got["nul"]; !ok || v != nil {
					t.Errorf("params[nul] = %v (present=%v), want an explicit nil entry", v, ok)
				}
			},
		},
		{
			name: "no params regression",
			body: `{"sql":"SELECT 42","catalog_jwt":"hdr.pld.sig","warehouse":"proj/wh"}`,
			check: func(t *testing.T, resp *httptest.ResponseRecorder, req queryengine.Request) {
				var result queryengine.Result
				if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
					t.Fatalf("decode result: %v", err)
				}
				if len(result.Schema) != 1 || result.Schema[0].Name != "n" || len(result.Rows) != 1 {
					t.Errorf("unexpected result: %+v", result)
				}
				if req.Params != nil {
					t.Errorf("Params = %#v, want nil when the body carries no params field", req.Params)
				}
				// Timeout/rows/bytes clamping is untouched by the params wiring.
				if req.Timeout != 300*time.Second || req.MaxRows != 10000 || req.MaxBytes != 10*1024*1024 {
					t.Errorf("clamped limits drifted: %+v", req)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			priv := genTestKey(t)
			runner := &fakeRunner{result: echoResult}
			srv := newTestServer(t, priv, runner, testWorkerCfg())

			w := postRaw(t, srv, tc.body, mintTestToken(t, priv))
			if w.Code != http.StatusOK {
				t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
			}
			tc.check(t, w, runner.lastReq())
		})
	}
}

// TestServer_ParamsShapeViolations_Return400BadRequest: every ValidateParams
// failure (missing / unreferenced / non-scalar / out-of-safe-range) is a 400
// with kind=bad_request, and the runner is never called.
func TestServer_ParamsShapeViolations_Return400BadRequest(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "missing param",
			body: `{"sql":"SELECT $a, $b","catalog_jwt":"a.b.c","warehouse":"p/w","params":{"a":1}}`,
		},
		{
			name: "unreferenced param",
			body: `{"sql":"SELECT $a","catalog_jwt":"a.b.c","warehouse":"p/w","params":{"a":1,"typo":2}}`,
		},
		{
			name: "no params supplied for a referenced placeholder",
			body: `{"sql":"SELECT $a","catalog_jwt":"a.b.c","warehouse":"p/w"}`,
		},
		{
			name: "non-scalar param (object)",
			body: `{"sql":"SELECT $a","catalog_jwt":"a.b.c","warehouse":"p/w","params":{"a":{"k":1}}}`,
		},
		{
			name: "non-scalar param (array)",
			body: `{"sql":"SELECT $a","catalog_jwt":"a.b.c","warehouse":"p/w","params":{"a":[1,2]}}`,
		},
		{
			name: "integral number beyond MAX_SAFE_INTEGER",
			body: `{"sql":"SELECT $a","catalog_jwt":"a.b.c","warehouse":"p/w","params":{"a":9007199254740992}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			priv := genTestKey(t)
			runner := &fakeRunner{result: echoResult}
			srv := newTestServer(t, priv, runner, testWorkerCfg())
			token := mintTestToken(t, priv)

			req := httptest.NewRequest(http.MethodPost, "/internal/query", strings.NewReader(tc.body))
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
				t.Error("runner must not be called when parameter validation fails")
			}
		})
	}
}

// TestServer_ParamsBindError_Returns400SQLError: params that pass shape
// validation but blow up at DuckDB bind/execute time come back from the
// engine as a plain error — which the worker maps to 400 kind=sql_error, not
// bad_request. (The engine-side proof that a real DuckDB bind failure IS a
// plain error lives in TestBindParams_DuckDBBindErrorIsPlainError.)
func TestServer_ParamsBindError_Returns400SQLError(t *testing.T) {
	priv := genTestKey(t)
	runner := &fakeRunner{err: errors.New(
		"Conversion Error: Could not convert string 'not-a-number' to INT32")}
	srv := newTestServer(t, priv, runner, testWorkerCfg())

	token := mintTestToken(t, priv)
	body := `{"sql":"SELECT $a::INT","catalog_jwt":"a.b.c","warehouse":"p/w","params":{"a":"not-a-number"}}`
	req := httptest.NewRequest(http.MethodPost, "/internal/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	_, kind := decodeError(t, w.Body.String())
	if kind != "sql_error" {
		t.Errorf("kind = %q, want sql_error (bind failures are not shape violations)", kind)
	}
	if !runner.wasCalled() {
		t.Error("runner should have been called — the params shape was valid")
	}
}
