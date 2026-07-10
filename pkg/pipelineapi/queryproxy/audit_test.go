package queryproxy

// Tests for RFC 022 §5.5 structured audit emission. All tests run inside
// the same package so they can reach unexported helpers.
//
// Design contract under test:
//   - Exactly one "query_audit" slog.Info line per request that passed
//     authentication (i.e. has a principal sub). Unauthenticated requests
//     (no sub) do NOT emit an audit line — there is nothing to correlate.
//   - Raw SQL text must NEVER appear in the log output.
//   - Raw JWT material (base64url segments starting "eyJ") must NEVER
//     appear in the log output.
//   - The catalog-token jti is extracted by unverified base64url decode of
//     the token's payload segment (we minted it ourselves — verification
//     would be circular and adds latency for no gain). jti is non-empty
//     whenever a catalog token was successfully minted, empty otherwise
//     (rate_limited, bad_request before mint).
//   - statement_hash is the first 16 hex chars of SHA-256(raw SQL), or ""
//     for empty/missing SQL.
//   - outcome is one of ok / bad_request / rate_limited / timeout /
//     result_too_large / capacity / sql_error / internal.
//   - truncated is true only when the worker returns 200 and the result
//     body carries {"truncated":true}; false in all other cases.
//   - The Prometheus counter pipelineapi_query_requests_total is labeled
//     solely by "outcome" (low cardinality).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate/projectgatetest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// ---- helpers ---------------------------------------------------------------

// captureLogger replaces the global slog default with a buffer-backed text
// handler for the duration of t, then restores the original. Returns the
// buffer.
func captureLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// auditLines returns every line from buf that contains "query_audit".
func auditLines(buf *bytes.Buffer) []string {
	var lines []string
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, "query_audit") {
			lines = append(lines, line)
		}
	}
	return lines
}

// extractField returns the value of key= from a slog text-format line, or "".
// Text format: key=value or key="value with spaces".
func extractField(line, key string) string {
	needle := key + "="
	idx := strings.Index(line, needle)
	if idx < 0 {
		return ""
	}
	rest := line[idx+len(needle):]
	if strings.HasPrefix(rest, `"`) {
		end := strings.Index(rest[1:], `"`)
		if end < 0 {
			return rest[1:]
		}
		return rest[1 : end+1]
	}
	// unquoted: up to next space
	end := strings.IndexByte(rest, ' ')
	if end < 0 {
		return strings.TrimRight(rest, "\n\r")
	}
	return rest[:end]
}

// newHandlerWithCounter builds a handler that uses the supplied Prometheus
// CounterVec for its query-audit counter, rather than the package-level
// promauto singleton. This lets each test own its counter and measure
// increments in isolation.
func newHandlerWithCounter(t *testing.T, cfg Config, counter *prometheus.CounterVec) http.Handler {
	t.Helper()
	if cfg.WorkerURL == "" {
		t.Fatal("test bug: cfg.WorkerURL must be set")
	}
	h, err := HandlerWithAudit(cfg, testSigner(t), counter)
	if err != nil {
		t.Fatalf("HandlerWithAudit: %v", err)
	}
	return h
}

// freshCounter builds an isolated CounterVec on a private registry so tests
// don't share state with the package-level promauto singleton.
func freshCounter() *prometheus.CounterVec {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pipelineapi_query_requests_total",
		Help: "test counter",
	}, []string{"outcome"})
	reg.MustRegister(c)
	return c
}

// counterDelta returns counter.WithLabelValues(outcome).Get() minus the
// supplied before snapshot.
func counterDelta(c *prometheus.CounterVec, outcome string, before float64) float64 {
	return testutil.ToFloat64(c.WithLabelValues(outcome)) - before
}

// workerThatReturns is a one-shot httptest.Server that returns status+body.
func workerThatReturns(status int, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	}))
}

// authedReq is authedRequest from handler_test.go (same package). It sets
// the {pid} path value the same way authedRequest does (Task 0.3) since the
// gate step reads r.PathValue("pid") before the body decode.
func authedReq(t *testing.T, sub uuid.UUID, body string) *http.Request {
	t.Helper()
	pid := uuid.NewString()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/projects/"+pid+"/query", strings.NewReader(body))
	r.SetPathValue("pid", pid)
	u := &store.User{ID: sub, Email: "test@example.com"}
	return r.WithContext(auth.WithCtxUser(r.Context(), u))
}

// ---- extractJTIFromToken ---------------------------------------------------

func TestExtractJTIFromToken_Valid(t *testing.T) {
	// Build a synthetic three-part JWT whose payload carries a jti claim.
	// We use base64url-encoded JSON to exercise the full decode path.
	header := "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9" // {"alg":"RS256","typ":"JWT"}
	payload := base64URLEncode([]byte(`{"jti":"test-jti-1234","sub":"abc"}`))
	sig := "fakesig"
	tok := header + "." + payload + "." + sig

	got := extractJTIFromToken(tok)
	if got != "test-jti-1234" {
		t.Errorf("extractJTIFromToken = %q, want test-jti-1234", got)
	}
}

func TestExtractJTIFromToken_Missing(t *testing.T) {
	payload := base64URLEncode([]byte(`{"sub":"abc"}`))
	tok := "header." + payload + ".sig"
	if got := extractJTIFromToken(tok); got != "" {
		t.Errorf("want empty jti when absent, got %q", got)
	}
}

func TestExtractJTIFromToken_MalformedToken(t *testing.T) {
	for _, bad := range []string{"", "onlyone", "two.parts"} {
		if got := extractJTIFromToken(bad); got != "" {
			t.Errorf("extractJTIFromToken(%q) = %q, want empty", bad, got)
		}
	}
}

// ---- statementHash ---------------------------------------------------------

func TestStatementHash_NonEmpty(t *testing.T) {
	h := statementHash("SELECT 1")
	if len(h) != 16 {
		t.Errorf("len = %d, want 16", len(h))
	}
	// Must be hex digits only.
	for _, c := range h {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("non-hex char %q in hash %q", c, h)
		}
	}
}

func TestStatementHash_Empty(t *testing.T) {
	if got := statementHash(""); got != "" {
		t.Errorf("statementHash(\"\") = %q, want empty", got)
	}
}

func TestStatementHash_Deterministic(t *testing.T) {
	if statementHash("SELECT 1") != statementHash("SELECT 1") {
		t.Error("statementHash not deterministic")
	}
	if statementHash("SELECT 1") == statementHash("SELECT 2") {
		t.Error("different SQL produced the same hash (collision unlikely)")
	}
}

// ---- integration: audit line per outcome -----------------------------------

// TestAudit_ExactlyOneLinePerRequest runs one request per outcome variant and
// asserts exactly one "query_audit" slog line per request, correct outcome
// field, jti presence/absence, truncated field, and that neither raw SQL nor
// JWT material appears in logs.
func TestAudit_ExactlyOneLinePerRequest(t *testing.T) {
	cases := []struct {
		name         string
		workerStatus int
		workerBody   string
		reqBody      string
		wantOutcome  string
		wantJTI      bool // true = jti must be non-empty
		wantTruncated bool
	}{
		{
			name:          "ok_truncated_true",
			workerStatus:  http.StatusOK,
			workerBody:    `{"schema":[],"rows":[],"truncated":true,"stats":{"duration_ms":1}}`,
			reqBody:       `{"sql":"SELECT 1"}`,
			wantOutcome:   "ok",
			wantJTI:       true,
			wantTruncated: true,
		},
		{
			name:          "ok_truncated_false",
			workerStatus:  http.StatusOK,
			workerBody:    `{"schema":[],"rows":[[1]],"truncated":false,"stats":{"duration_ms":1}}`,
			reqBody:       `{"sql":"SELECT 2"}`,
			wantOutcome:   "ok",
			wantJTI:       true,
			wantTruncated: false,
		},
		{
			name:         "sql_error",
			workerStatus: http.StatusBadRequest,
			workerBody:   `{"error":"Binder Error: no such table foo","kind":"sql_error"}`,
			reqBody:      `{"sql":"SELECT * FROM missing_table"}`,
			wantOutcome:  "sql_error",
			wantJTI:      true,
		},
		{
			name:         "timeout",
			workerStatus: http.StatusRequestTimeout,
			workerBody:   `{"error":"timeout","kind":"timeout"}`,
			reqBody:      `{"sql":"SELECT sleep(999)"}`,
			wantOutcome:  "timeout",
			wantJTI:      true,
		},
		{
			name:         "result_too_large",
			workerStatus: http.StatusRequestEntityTooLarge,
			workerBody:   `{"error":"too big","kind":"result_too_large"}`,
			reqBody:      `{"sql":"SELECT * FROM huge"}`,
			wantOutcome:  "result_too_large",
			wantJTI:      true,
		},
		{
			name:         "capacity",
			workerStatus: http.StatusTooManyRequests,
			workerBody:   `{"error":"busy","kind":"capacity"}`,
			reqBody:      `{"sql":"SELECT 1"}`,
			wantOutcome:  "capacity",
			wantJTI:      true,
		},
		{
			name:         "internal_worker401",
			workerStatus: http.StatusUnauthorized,
			workerBody:   `{"error":"unauthorized","kind":"unauthorized"}`,
			reqBody:      `{"sql":"SELECT 1"}`,
			wantOutcome:  "internal",
			wantJTI:      true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogger(t)
			counter := freshCounter()

			worker := workerThatReturns(tc.workerStatus, tc.workerBody)
			defer worker.Close()

			h := newHandlerWithCounter(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")}, counter)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, authedReq(t, uuid.New(), tc.reqBody))

			lines := auditLines(buf)
			if len(lines) != 1 {
				t.Fatalf("want 1 query_audit line, got %d:\n%s", len(lines), buf.String())
			}
			line := lines[0]

			// outcome field
			if got := extractField(line, "outcome"); got != tc.wantOutcome {
				t.Errorf("outcome = %q, want %q\nline: %s", got, tc.wantOutcome, line)
			}

			// jti field
			jti := extractField(line, "jti")
			if tc.wantJTI && jti == "" {
				t.Errorf("jti must be non-empty for outcome %q\nline: %s", tc.wantOutcome, line)
			}

			// truncated field (only relevant for ok path)
			if tc.wantTruncated {
				if got := extractField(line, "truncated"); got != "true" {
					t.Errorf("truncated = %q, want true\nline: %s", got, line)
				}
			}

			// statement_hash must be a 16-char hex string (not raw SQL)
			hash := extractField(line, "statement_hash")
			if hash == "" {
				t.Errorf("statement_hash missing\nline: %s", line)
			}
			if len(hash) != 16 {
				t.Errorf("statement_hash len = %d, want 16\nline: %s", len(hash), line)
			}

			// duration_ms must be present (≥0)
			if dur := extractField(line, "duration_ms"); dur == "" {
				t.Errorf("duration_ms missing\nline: %s", line)
			}

			// principal must be present
			if prin := extractField(line, "principal"); prin == "" {
				t.Errorf("principal missing\nline: %s", line)
			}

			// No raw SQL in log
			reqBody := struct{ SQL string }{}
			_ = json.Unmarshal([]byte(tc.reqBody), &reqBody)
			if reqBody.SQL != "" && strings.Contains(buf.String(), reqBody.SQL) {
				t.Errorf("raw SQL %q leaked into logs:\n%s", reqBody.SQL, buf.String())
			}

			// No JWT material (eyJ...) in log
			if strings.Contains(buf.String(), "eyJ") {
				t.Errorf("JWT material (eyJ...) leaked into logs:\n%s", buf.String())
			}

			// Counter incremented by 1 for the right outcome
			if delta := counterDelta(counter, tc.wantOutcome, 0); delta != 1 {
				t.Errorf("counter[%q] delta = %v, want 1", tc.wantOutcome, delta)
			}
		})
	}
}

// TestAudit_BadRequest_BeforeMint covers the bad_request path where no catalog
// token is minted (empty SQL), so jti must be empty.
func TestAudit_BadRequest_BeforeMint(t *testing.T) {
	buf := captureLogger(t)
	counter := freshCounter()

	// Worker should never be called.
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker must not be called for bad_request before mint")
	}))
	defer worker.Close()

	h := newHandlerWithCounter(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")}, counter)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(t, uuid.New(), `{"sql":""}`)) // empty SQL → bad_request

	lines := auditLines(buf)
	if len(lines) != 1 {
		t.Fatalf("want 1 query_audit line, got %d:\n%s", len(lines), buf.String())
	}
	line := lines[0]

	if got := extractField(line, "outcome"); got != "bad_request" {
		t.Errorf("outcome = %q, want bad_request", got)
	}
	if jti := extractField(line, "jti"); jti != "" {
		t.Errorf("jti must be empty before mint, got %q", jti)
	}
	if hash := extractField(line, "statement_hash"); hash != "" {
		t.Errorf("statement_hash must be empty for empty SQL, got %q", hash)
	}

	if delta := counterDelta(counter, "bad_request", 0); delta != 1 {
		t.Errorf("counter[bad_request] delta = %v, want 1", delta)
	}
}

// TestAudit_RateLimited_EmptyJTI covers the per-principal gate path: no token
// is minted when the gate is full, so jti must be empty.
func TestAudit_RateLimited_EmptyJTI(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	defer closeRelease()

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`)
	}))
	defer worker.Close()

	counter := freshCounter()
	cfg := Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w"), PerPrincipalInflight: 1}
	h := newHandlerWithCounter(t, cfg, counter)
	sub := uuid.New()

	// First query: blocks inside worker. done is closed when it fully
	// returns (including its deferred audit emit) so the test can await it
	// before returning — otherwise the late emit fires after the NEXT test
	// has swapped the global slog default (slog.SetDefault also redirects the
	// stdlib log package), leaking this query's audit line into that test's
	// captured buffer.
	done := make(chan struct{})
	go func() {
		defer close(done)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, authedReq(t, sub, `{"sql":"SELECT 1"}`))
	}()
	<-entered // first query is in-flight

	// Second query from same principal: gate full → rate_limited.
	buf := captureLogger(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(t, sub, `{"sql":"SELECT 2"}`))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}

	lines := auditLines(buf)
	if len(lines) != 1 {
		t.Fatalf("want 1 query_audit line, got %d:\n%s", len(lines), buf.String())
	}
	line := lines[0]

	if got := extractField(line, "outcome"); got != "rate_limited" {
		t.Errorf("outcome = %q, want rate_limited", got)
	}
	if jti := extractField(line, "jti"); jti != "" {
		t.Errorf("jti must be empty for rate_limited (no mint), got %q", jti)
	}

	if delta := counterDelta(counter, "rate_limited", 0); delta != 1 {
		t.Errorf("counter[rate_limited] delta = %v, want 1", delta)
	}

	closeRelease()
	<-done // ensure the in-flight query finishes (and emits) before returning
}

// TestAudit_TransportError_InternalOutcome covers the path where the worker
// is unreachable — should produce outcome=internal with jti non-empty (the
// catalog token was minted before the transport error).
func TestAudit_TransportError_InternalOutcome(t *testing.T) {
	buf := captureLogger(t)
	counter := freshCounter()

	h := newHandlerWithCounter(t, Config{WorkerURL: "http://127.0.0.1:1/", Gate: projectgatetest.AllowAll("p", "w")}, counter)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(t, uuid.New(), `{"sql":"SELECT 1"}`))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}

	lines := auditLines(buf)
	if len(lines) != 1 {
		t.Fatalf("want 1 query_audit line, got %d:\n%s", len(lines), buf.String())
	}
	line := lines[0]

	if got := extractField(line, "outcome"); got != "internal" {
		t.Errorf("outcome = %q, want internal", got)
	}
	// Catalog token was minted before the transport failure → jti non-empty.
	if jti := extractField(line, "jti"); jti == "" {
		t.Errorf("jti must be non-empty for transport error (catalog token was minted)\nline: %s", line)
	}

	if delta := counterDelta(counter, "internal", 0); delta != 1 {
		t.Errorf("counter[internal] delta = %v, want 1", delta)
	}
}

// TestAudit_Unauthenticated_NoLine asserts that a request with no principal
// (unauthenticated) produces ZERO audit lines — there is no sub to correlate
// with lakekeeper logs, so emitting an audit record would be noise.
func TestAudit_Unauthenticated_NoLine(t *testing.T) {
	buf := captureLogger(t)
	counter := freshCounter()

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker must not be called for unauthenticated request")
	}))
	defer worker.Close()

	h := newHandlerWithCounter(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")}, counter)
	// No auth.WithCtxUser → no user in ctx.
	r := httptest.NewRequest(http.MethodPost, "/api/v1/query", strings.NewReader(`{"sql":"SELECT 1"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}

	lines := auditLines(buf)
	if len(lines) != 0 {
		t.Fatalf("want 0 query_audit lines for unauthenticated request, got:\n%s", buf.String())
	}
}

// TestAudit_JTIMatchesMintedToken verifies that the jti in the audit line
// actually matches the jti embedded in the catalog token that was minted.
// We capture the catalog JWT sent to the worker and compare its payload jti.
func TestAudit_JTIMatchesMintedToken(t *testing.T) {
	buf := captureLogger(t)
	counter := freshCounter()

	var capturedCatalogJWT string
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body workerRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		capturedCatalogJWT = body.CatalogJWT
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`)
	}))
	defer worker.Close()

	h := newHandlerWithCounter(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")}, counter)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(t, uuid.New(), `{"sql":"SELECT 1"}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	// Extract jti from captured catalog JWT.
	wantJTI := extractJTIFromToken(capturedCatalogJWT)
	if wantJTI == "" {
		t.Fatal("could not extract jti from captured catalog JWT")
	}

	lines := auditLines(buf)
	if len(lines) != 1 {
		t.Fatalf("want 1 audit line, got %d", len(lines))
	}
	gotJTI := extractField(lines[0], "jti")
	if gotJTI != wantJTI {
		t.Errorf("audit jti = %q, want %q (from minted catalog token)", gotJTI, wantJTI)
	}
}

// TestAudit_StatementHashMatchesSHA256 verifies the hash value matches
// the expected sha256 prefix of the SQL.
func TestAudit_StatementHashMatchesSHA256(t *testing.T) {
	buf := captureLogger(t)
	counter := freshCounter()

	const sql = "SELECT 42 FROM dual"
	worker := workerThatReturns(http.StatusOK,
		`{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`)
	defer worker.Close()

	h := newHandlerWithCounter(t, Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")}, counter)
	rec := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	h.ServeHTTP(rec, authedReq(t, uuid.New(), string(body)))

	lines := auditLines(buf)
	if len(lines) != 1 {
		t.Fatalf("want 1 audit line, got %d", len(lines))
	}

	wantHash := statementHash(sql)
	gotHash := extractField(lines[0], "statement_hash")
	if gotHash != wantHash {
		t.Errorf("statement_hash = %q, want %q", gotHash, wantHash)
	}
}
