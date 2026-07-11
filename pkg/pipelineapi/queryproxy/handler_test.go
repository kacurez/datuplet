package queryproxy

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate/projectgatetest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// testSigner builds a pipeline-api Signer from a fresh keypair, mirroring
// tokens/mint_test.go: sharedSigner.
func testSigner(t *testing.T) *tokens.Signer {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	dir := t.TempDir()
	privDER, _ := x509.MarshalPKCS8PrivateKey(priv)
	privPath := filepath.Join(dir, "priv.pem")
	if err := os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o400); err != nil {
		t.Fatalf("write key: %v", err)
	}
	signer, err := tokens.LoadPrivateKeyFromPEMFile(privPath, "key-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return signer
}

// authedRequest builds a POST /api/v1/query request whose context carries
// an authenticated user, the same way auth.WithUser does at runtime. The
// minters (MintQueryToken/MintInternalQueryToken) read the subject from
// this ctx, so without it they would refuse to mint.
func authedRequest(t *testing.T, sub uuid.UUID, jsonBody string) *http.Request {
	t.Helper()
	pid := uuid.NewString()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/projects/"+pid+"/query", strings.NewReader(jsonBody))
	r.SetPathValue("pid", pid)
	u := &store.User{ID: sub, Email: "test@example.com"}
	return r.WithContext(auth.WithCtxUser(r.Context(), u))
}

// newHandler builds a queryproxy handler pointed at the given worker URL.
func newHandler(t *testing.T, cfg Config) http.Handler {
	t.Helper()
	if cfg.WorkerURL == "" {
		t.Fatal("test bug: cfg.WorkerURL must be set")
	}
	h, err := Handler(cfg, testSigner(t))
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	return h
}

func TestHandler_RequiresWorkerURL(t *testing.T) {
	// A valid Gate is supplied so the Gate-nil guard does not short-circuit
	// before newWorkerClient's WorkerURL-empty check — this test must exercise
	// the WorkerURL requirement specifically.
	if _, err := Handler(Config{Gate: projectgatetest.AllowAll("p", "w")}, testSigner(t)); err == nil {
		t.Fatal("expected error when WorkerURL is empty")
	}
}

func TestHandler_RequiresSigner(t *testing.T) {
	if _, err := Handler(Config{WorkerURL: "http://worker"}, nil); err == nil {
		t.Fatal("expected error when signer is nil")
	}
}

func TestHandler_HappyPath_PassthroughBytes(t *testing.T) {
	// The fake worker echoes a fixed queryengine Result; the handler must
	// stream those exact bytes back with a JSON content type, NOT re-encode.
	const resultJSON = `{"schema":[{"name":"a","type":"INTEGER"}],"rows":[[1]],"truncated":false,"stats":{"duration_ms":3}}`

	var gotAuth, gotCatalogJWT, gotWarehouse string
	var gotTimeout, gotRows, gotBytes int
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		var body workerRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotCatalogJWT = body.CatalogJWT
		gotWarehouse = body.Warehouse
		gotTimeout, gotRows, gotBytes = body.TimeoutS, body.MaxRows, body.MaxBytes
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(resultJSON))
	}))
	defer worker.Close()

	h := newHandler(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("proj-id", "wh")})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(t, uuid.New(), `{"sql":"SELECT 1"}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q, want application/json", ct)
	}
	if rec.Body.String() != resultJSON {
		t.Fatalf("body passthrough mismatch:\n got %q\nwant %q", rec.Body.String(), resultJSON)
	}
	// Defaults applied (no limits in the request).
	if gotTimeout != 60 || gotRows != 1000 || gotBytes != 1*1024*1024 {
		t.Fatalf("worker got timeout=%d rows=%d bytes=%d, want 60/1000/1MiB", gotTimeout, gotRows, gotBytes)
	}
	if gotWarehouse != "proj-id/wh" {
		t.Fatalf("worker warehouse = %q, want proj-id/wh", gotWarehouse)
	}
	// Real (non-redacted) tokens must reach the worker — proves Reveal() is wired.
	if !strings.HasPrefix(gotAuth, "Bearer ") || len(gotAuth) <= len("Bearer ") {
		t.Fatalf("worker Authorization = %q, want a real Bearer token", gotAuth)
	}
	if gotCatalogJWT == "" || gotCatalogJWT == "[redacted query token]" {
		t.Fatalf("worker catalog_jwt = %q, want a real JWT", gotCatalogJWT)
	}
	// The two tokens must be distinct (catalog vs internal-query).
	gotInternal := strings.TrimPrefix(gotAuth, "Bearer ")
	if gotInternal == gotCatalogJWT {
		t.Fatal("internal-query token and catalog token must differ")
	}
	// Sanity: both are real RS256 JWTs (three dot-separated segments).
	for name, tok := range map[string]string{"internal": gotInternal, "catalog": gotCatalogJWT} {
		if strings.Count(tok, ".") != 2 {
			t.Fatalf("%s token is not a JWT: %q", name, tok)
		}
	}
}

func TestHandler_Clamping(t *testing.T) {
	var got workerRequest
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`))
	}))
	defer worker.Close()

	cfg := Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")}

	cases := []struct {
		name                          string
		body                          string
		wantTimeout, wantRows, wantBy int
	}{
		{"unset uses defaults", `{"sql":"SELECT 1"}`, 60, 1000, 1 * 1024 * 1024},
		{"in range honoured", `{"sql":"SELECT 1","timeout_s":120,"max_rows":500,"max_bytes":2048}`, 120, 500, 2048},
		{"zero falls to default", `{"sql":"SELECT 1","timeout_s":0,"max_rows":0,"max_bytes":0}`, 60, 1000, 1 * 1024 * 1024},
		{"negative falls to default", `{"sql":"SELECT 1","timeout_s":-5,"max_rows":-1,"max_bytes":-100}`, 60, 1000, 1 * 1024 * 1024},
		{"huge clamped to max", `{"sql":"SELECT 1","timeout_s":99999,"max_rows":9999999,"max_bytes":999999999}`, 300, 10000, 10 * 1024 * 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got = workerRequest{}
			h := newHandler(t, cfg)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, authedRequest(t, uuid.New(), tc.body))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
			}
			if got.TimeoutS != tc.wantTimeout {
				t.Errorf("timeout_s = %d, want %d", got.TimeoutS, tc.wantTimeout)
			}
			if got.MaxRows != tc.wantRows {
				t.Errorf("max_rows = %d, want %d", got.MaxRows, tc.wantRows)
			}
			if got.MaxBytes != tc.wantBy {
				t.Errorf("max_bytes = %d, want %d", got.MaxBytes, tc.wantBy)
			}
		})
	}
}

func TestHandler_MalformedAndEmptySQL(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker should not be called for a bad request body")
		w.WriteHeader(http.StatusOK)
	}))
	defer worker.Close()
	h := newHandler(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")})

	for _, body := range []string{`{not json`, `{"sql":""}`, `{}`} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, authedRequest(t, uuid.New(), body))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %q: status = %d, want 400", body, rec.Code)
		}
		if k := decodeKind(rec.Body.Bytes()); k != "bad_request" {
			t.Fatalf("body %q: kind = %q, want bad_request", body, k)
		}
	}
}

func TestHandler_Unauthenticated(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker should not be called for an unauthenticated request")
	}))
	defer worker.Close()
	h := newHandler(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")})

	// No auth.WithCtxUser on this request → no user in ctx.
	r := httptest.NewRequest(http.MethodPost, "/api/v1/query", strings.NewReader(`{"sql":"SELECT 1"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestHandler_PerPrincipal429WhenCapExhausted(t *testing.T) {
	// Deterministic concurrency: the fake worker blocks on `release` once
	// it is in-flight, signalling via `entered`. We drive the gate to its
	// cap with blocked queries, then assert the next concurrent query from
	// the SAME principal gets 429 rate_limited — no sleeps.
	entered := make(chan struct{})
	release := make(chan struct{})
	// Deferred once-close so a t.Fatalf on any assertion below cannot leak
	// the blocked worker goroutines past the test.
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	defer closeRelease()
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`))
	}))
	defer worker.Close()

	cfg := Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w"), PerPrincipalInflight: 2}
	h := newHandler(t, cfg)
	sub := uuid.New()

	// Launch `cap` (2) concurrent queries; each blocks inside the worker.
	for i := 0; i < 2; i++ {
		go func() {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, authedRequest(t, sub, `{"sql":"SELECT 1"}`))
		}()
	}
	// Wait until both are genuinely in-flight (gate holds 2 slots).
	<-entered
	<-entered

	// Third concurrent query from the same principal: gate is full → 429.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(t, sub, `{"sql":"SELECT 1"}`))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if k := decodeKind(rec.Body.Bytes()); k != "rate_limited" {
		t.Fatalf("kind = %q, want rate_limited", k)
	}
	if ra := rec.Header().Get("Retry-After"); ra != "2" {
		t.Fatalf("Retry-After = %q, want 2", ra)
	}

	// A DIFFERENT principal is independent — should not be gated.
	recOther := httptest.NewRecorder()
	otherDone := make(chan struct{})
	go func() {
		h.ServeHTTP(recOther, authedRequest(t, uuid.New(), `{"sql":"SELECT 1"}`))
		close(otherDone)
	}()
	<-entered // the other principal's query entered the worker (was admitted)

	// Drain everything.
	closeRelease()
	<-otherDone
}

func TestHandler_TranslatesWorkerOutcomes(t *testing.T) {
	cases := []struct {
		name        string
		workerCode  int
		workerBody  string
		wantStatus  int
		wantKind    string
		wantRetry   bool
		wantMsgPart string // substring expected in the error message (passthrough cases)
	}{
		{"timeout", http.StatusRequestTimeout, `{"error":"INTERRUPT Error: Interrupted!","kind":"timeout"}`, http.StatusRequestTimeout, "timeout", false, ""},
		{"result_too_large", http.StatusRequestEntityTooLarge, `{"error":"too big","kind":"result_too_large"}`, http.StatusRequestEntityTooLarge, "result_too_large", false, ""},
		{"capacity_to_503", http.StatusTooManyRequests, `{"error":"server at capacity","kind":"capacity"}`, http.StatusServiceUnavailable, "capacity", true, ""},
		{"sql_error_passthrough", http.StatusBadRequest, `{"error":"Binder Error: no such table foo","kind":"sql_error"}`, http.StatusBadRequest, "sql_error", false, "Binder Error: no such table foo"},
		{"bad_request_passthrough", http.StatusBadRequest, `{"error":"warehouse is required","kind":"bad_request"}`, http.StatusBadRequest, "bad_request", false, "warehouse is required"},
		{"worker_401_to_502", http.StatusUnauthorized, `{"error":"unauthorized","kind":"unauthorized"}`, http.StatusBadGateway, "internal", false, ""},
		{"unexpected_5xx_to_502", http.StatusInternalServerError, `boom`, http.StatusBadGateway, "internal", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.workerCode)
				_, _ = w.Write([]byte(tc.workerBody))
			}))
			defer worker.Close()

			h := newHandler(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, authedRequest(t, uuid.New(), `{"sql":"SELECT 1"}`))

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if k := decodeKind(rec.Body.Bytes()); k != tc.wantKind {
				t.Fatalf("kind = %q, want %q", k, tc.wantKind)
			}
			if tc.wantRetry && rec.Header().Get("Retry-After") == "" {
				t.Fatalf("expected Retry-After header")
			}
			if tc.wantMsgPart != "" && !strings.Contains(rec.Body.String(), tc.wantMsgPart) {
				t.Fatalf("body %q does not contain worker error text %q", rec.Body.String(), tc.wantMsgPart)
			}
		})
	}
}

func TestHandler_TransportErrorTo502(t *testing.T) {
	// Worker URL points at a closed port → transport error.
	h := newHandler(t, Config{WorkerURL: "http://127.0.0.1:1/", Gate: projectgatetest.AllowAll("p", "w")})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(t, uuid.New(), `{"sql":"SELECT 1"}`))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if k := decodeKind(rec.Body.Bytes()); k != "internal" {
		t.Fatalf("kind = %q, want internal", k)
	}
}

func TestHandler_NeverLogsTokens(t *testing.T) {
	// Capture slog output and assert no real JWT material leaks. We force
	// the worker-401 path (which logs) and the transport-error path.
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	defer slog.SetDefault(prev)

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized","kind":"unauthorized"}`))
	}))
	defer worker.Close()

	h := newHandler(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(t, uuid.New(), `{"sql":"SECRET SQL 12345"}`))

	logs := logBuf.String()
	// The redacted Stringer text is acceptable; raw JWT material is not.
	if strings.Contains(logs, ".") && strings.Contains(logs, "eyJ") {
		t.Fatalf("logs appear to contain a raw JWT: %s", logs)
	}
	// SQL text must never be logged.
	if strings.Contains(logs, "SECRET SQL 12345") {
		t.Fatalf("logs leaked SQL text: %s", logs)
	}
}

func TestHandler_GateForbidden(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker must not be called when the gate denies")
	}))
	defer worker.Close()
	g := projectgatetest.AllowAll("p", "w")
	g.Authorizer = projectgatetest.FakeAuthorizer{Allow: false}
	h := newHandler(t, Config{WorkerURL: worker.URL, Gate: g})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(t, uuid.New(), `{"sql":"SELECT 1"}`))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandler_GateInvalidPID(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer worker.Close()
	h := newHandler(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")})
	r := httptest.NewRequest(http.MethodPost, "/api/v1/projects/not-a-uuid/query", strings.NewReader(`{"sql":"SELECT 1"}`))
	r.SetPathValue("pid", "not-a-uuid")
	r = r.WithContext(auth.WithCtxUser(r.Context(), &store.User{ID: uuid.New(), Email: "t@e.c"}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHandler_RequiresGate(t *testing.T) {
	if _, err := Handler(Config{WorkerURL: "http://worker"}, testSigner(t)); err == nil {
		t.Fatal("expected error when Gate is nil")
	}
}
