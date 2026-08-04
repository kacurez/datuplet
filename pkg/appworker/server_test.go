package appworker

// server_test.go drives the W6 HTTP layer against a full httptest server with
// a fake pipeline-api (resolve + auth + render) and the REAL appengine.Engine
// (shared package-wide via testEngine — a fake engine cannot exercise the
// guest ABI). It covers the brief's failing-test list item by item: routing +
// `@draft` + sub-path/`..`, resolve-before-verify order, the §4.2 response
// matrix, §6.5 input normalization, the §9 access log + Prometheus counter,
// the health/readiness gate, and the five hand-offs.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// ---------------------------------------------------------------------------
// Bundles
// ---------------------------------------------------------------------------

// serverProbeBundle echoes ctx.path and ctx.params back into the OutputDoc so
// routing + normalization are observable from the rendered response. It never
// calls datuplet.query, so Bundle is the only client method the happy path
// touches.
const serverProbeBundle = `var __dtp_app = { render: (ctx) => {
	const keys = Object.keys(ctx.params).sort().join(",");
	return {
		outputDoc: 1,
		title: "Probe",
		blocks: [
			{ id: "kpis", type: "metric", items: [{ label: "n", value: 1 }] },
			{ id: "path", type: "markdown", text: "path=" + ctx.path },
			{ id: "params", type: "markdown", text: "keys=" + keys },
			{ id: "x", type: "markdown", text: "x=" + (ctx.params.x ?? "") }
		]
	};
}};`

// serverQueryBundle awaits one query, so a blocking Query fake holds the
// render inside the engine — how the capacity gate is exercised at HTTP level.
const serverQueryBundle = `var __dtp_app = { render: async () => {
	await datuplet.query("SELECT 1");
	return { outputDoc: 1, title: "q", blocks: [{ id: "a", type: "markdown", text: "ok" }] };
}};`

// serverInvalidDocBundle returns a non-OutputDoc → outputdoc.Validate fails →
// render_error.
const serverInvalidDocBundle = `var __dtp_app = { render: () => ({ nope: 1 }) };`

// serverHotLoopBundle never yields → the wall-clock backstop fires → timeout.
const serverHotLoopBundle = `var __dtp_app = { render: () => { while (true) {} } };`

// serverInjectionBundle emits app-controlled text containing a literal
// `</script>` breakout — the exact payload that would inject markup into the
// trusted shell if the doc were embedded unescaped (Finding 1).
const serverInjectionBundle = `var __dtp_app = { render: () => ({
	outputDoc: 1,
	title: "t",
	blocks: [{ id: "a", type: "markdown", text: "</script><img src=x onerror=boom>" }]
}) };`

// serverPlainRowTableBundle has a table with plain-ARRAY rows (a valid W1
// tableRow shape). Pre-fix, `block=tbl` 400'd because the array row failed the
// whole-block unmarshal (Finding 2).
const serverPlainRowTableBundle = `var __dtp_app = { render: () => ({
	outputDoc: 1, title: "t", blocks: [
		{ id: "tbl", type: "table", columns: ["a", "b"], rows: [["1", "2"], ["3", "4"]] },
		{ id: "m", type: "markdown", text: "hi" }
	]
}) };`

// serverObjectRowModalBundle has an OBJECT-row table whose row carries a modal
// with a nested block — the recursion that must keep working (Finding 2).
const serverObjectRowModalBundle = `var __dtp_app = { render: () => ({
	outputDoc: 1, title: "t", blocks: [
		{ id: "tbl2", type: "table", columns: ["a"], rows: [
			{ cells: ["x"], modal: { title: "d", blocks: [{ id: "deep", type: "markdown", text: "hi" }] } }
		] }
	]
}) };`

// ---------------------------------------------------------------------------
// Fake pipeline-api client: resolveAPI + authAPI + renderAPI in one value
// ---------------------------------------------------------------------------

type fakeServerAPI struct {
	mu sync.Mutex

	bundle    []byte
	bundleErr error

	resolveErr      error
	resolveNotFound bool

	member     bool
	sessionErr error

	verifyOK  bool
	verifyErr error

	queryFn func(ctx context.Context, pid, jwt string, body []byte) (*http.Response, error)

	// calls records the ordered sequence of upstream calls, so a test can
	// assert resolve precedes verify (spec §4.2).
	calls              []string
	verifyTokenCalls   int
	resolveCalls       int
	lastResolveChannel string
	impersonateSeq     int
	logs               []RenderLogRecord
}

func (f *fakeServerAPI) Resolve(_ context.Context, _, name, channel string) (Resolved, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "resolve")
	f.resolveCalls++
	f.lastResolveChannel = channel
	err, nf := f.resolveErr, f.resolveNotFound
	f.mu.Unlock()
	if err != nil {
		return Resolved{}, err
	}
	if nf || name == "missing" {
		return Resolved{}, &APIError{StatusCode: http.StatusNotFound, Kind: "app_not_found", Message: "no such app"}
	}
	return Resolved{AppID: "app-" + name, VersionHash: testVersionHash}, nil
}

func (f *fakeServerAPI) VerifyToken(_ context.Context, _, _, _ string) (bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "verify_token")
	f.verifyTokenCalls++
	ok, err := f.verifyOK, f.verifyErr
	f.mu.Unlock()
	return ok, err
}

func (f *fakeServerAPI) CheckTokenActive(_ context.Context, _, _ string) (bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "active")
	f.mu.Unlock()
	return true, nil
}

func (f *fakeServerAPI) VerifySession(_ context.Context, _, _, _ string) (SessionInfo, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "session")
	err, member := f.sessionErr, f.member
	f.mu.Unlock()
	if err != nil {
		return SessionInfo{}, err
	}
	return SessionInfo{UserID: "user-1", ProjectMember: member}, nil
}

func (f *fakeServerAPI) Bundle(_ context.Context, _ string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bundleErr != nil {
		return nil, f.bundleErr
	}
	return f.bundle, nil
}

func (f *fakeServerAPI) Impersonate(_ context.Context, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.impersonateSeq++
	return fmt.Sprintf("jwt-%d", f.impersonateSeq), nil
}

func (f *fakeServerAPI) Query(ctx context.Context, pid, jwt string, body []byte) (*http.Response, error) {
	f.mu.Lock()
	fn := f.queryFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, pid, jwt, body)
	}
	return jsonResponse(http.StatusOK, `{"schema":[{"name":"n","type":"BIGINT"}],"rows":[[7]],"truncated":false,"stats":{}}`), nil
}

func (f *fakeServerAPI) AppendLog(_ context.Context, rec RenderLogRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, rec)
	return nil
}

func (f *fakeServerAPI) callSeq() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// ---------------------------------------------------------------------------
// syncBuffer — concurrency-safe log sink (the capacity test logs from two
// goroutines).
// ---------------------------------------------------------------------------

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type serverHarness struct {
	t      *testing.T
	api    *fakeServerAPI
	srv    *Server
	ts     *httptest.Server
	reg    *prometheus.Registry
	logBuf *syncBuffer
}

func newServerHarness(t *testing.T) *serverHarness {
	return newServerHarnessWith(t, Config{Render: renderConfig()}, 0)
}

// newServerHarnessWith builds a ready-to-render harness. The server clock is
// CONSTANT (testClockBase) so render-rate buckets do not refill mid-test — the
// engine's own wall clock is real regardless, so timeout tests still fire.
func newServerHarnessWith(t *testing.T, cfg Config, poolWait time.Duration) *serverHarness {
	t.Helper()
	api := &fakeServerAPI{bundle: []byte(serverProbeBundle), member: true, verifyOK: true}
	reg := prometheus.NewRegistry()
	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	clock := func() time.Time { return testClockBase }

	opts := []ServerOption{
		WithResolveAPI(api), WithAuthAPI(api), WithRenderAPI(api),
		WithCookieKey(testCookieKey), WithServerClock(clock),
		WithLogger(logger), WithMetricsRegistry(reg),
	}
	if poolWait > 0 {
		opts = append(opts, WithPoolAcquireWait(poolWait))
	}
	srv := NewServer(cfg, testEngine(t), opts...)
	srv.markEngineReady()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return &serverHarness{t: t, api: api, srv: srv, ts: ts, reg: reg, logBuf: logBuf}
}

func (h *serverHarness) url(suffix string) string {
	return h.ts.URL + "/apps/" + testProjectID + "/" + testAppName + suffix
}

// client never follows redirects so the 302 exchange is observable.
func (h *serverHarness) client() *http.Client {
	c := h.ts.Client()
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return c
}

func (h *serverHarness) do(req *http.Request) *http.Response {
	h.t.Helper()
	resp, err := h.client().Do(req)
	if err != nil {
		h.t.Fatalf("request %s %s: %v", req.Method, req.URL, err)
	}
	return resp
}

// bearerGet issues an authenticated (platform bearer) GET; accept selects the
// response-matrix branch ("" = navigation).
func (h *serverHarness) bearerGet(rawurl, accept string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawurl, nil)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer platform-token")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	return h.do(req)
}

// readBody and decodeEnvelope are shared with auth_test.go (same package).

// scriptDoc extracts and parses the OutputDoc embedded in the shell's
// <script type="application/json"> element.
func scriptDoc(t *testing.T, html string) map[string]any {
	t.Helper()
	const marker = `type="application/json">`
	i := strings.Index(html, marker)
	if i < 0 {
		t.Fatalf("no application/json script in shell:\n%s", html)
	}
	rest := html[i+len(marker):]
	j := strings.Index(rest, "</script>")
	if j < 0 {
		t.Fatalf("unterminated script element in shell")
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(rest[:j]), &doc); err != nil {
		t.Fatalf("embedded doc is not valid JSON: %v\n%s", err, rest[:j])
	}
	return doc
}

// blockTextByID finds a block's text field in a parsed doc's block list.
func blockTextByID(doc map[string]any, id string) (string, bool) {
	blocks, _ := doc["blocks"].([]any)
	for _, raw := range blocks {
		b, _ := raw.(map[string]any)
		if b["id"] == id {
			s, _ := b["text"].(string)
			return s, true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Routing, @draft, sub-path → ctx.path, `..` rejection
// ---------------------------------------------------------------------------

func TestRoute_ProductionAndDraftChannel(t *testing.T) {
	t.Run("production", func(t *testing.T) {
		h := newServerHarness(t)
		resp := h.bearerGet(h.url(""), "application/json")
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, readBody(t, resp))
		}
		resp.Body.Close()
		if h.api.lastResolveChannel != channelProduction {
			t.Errorf("resolve channel = %q, want %q", h.api.lastResolveChannel, channelProduction)
		}
	})

	t.Run("draft suffix", func(t *testing.T) {
		h := newServerHarness(t)
		// @draft only accepts a platform credential — the bearer supplies it.
		resp := h.bearerGet(h.ts.URL+"/apps/"+testProjectID+"/"+testAppName+draftSuffix, "application/json")
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, readBody(t, resp))
		}
		resp.Body.Close()
		if h.api.lastResolveChannel != channelDraft {
			t.Errorf("resolve channel = %q, want %q", h.api.lastResolveChannel, channelDraft)
		}
	})
}

func TestRoute_SubPathBecomesCtxPath(t *testing.T) {
	h := newServerHarness(t)
	resp := h.bearerGet(h.url("/reports/q4"), "")
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, body)
	}
	doc := scriptDoc(t, body)
	got, ok := blockTextByID(doc, "path")
	if !ok || got != "path=/reports/q4" {
		t.Errorf("ctx.path block = %q (ok=%v), want %q", got, ok, "path=/reports/q4")
	}
}

func TestRoute_BareAppHasRootCtxPath(t *testing.T) {
	h := newServerHarness(t)
	body := readBody(t, h.bearerGet(h.url(""), ""))
	if got, _ := blockTextByID(scriptDoc(t, body), "path"); got != "path=/" {
		t.Errorf("ctx.path = %q, want %q", got, "path=/")
	}
}

func TestRoute_TraversalIsBadRequest(t *testing.T) {
	h := newServerHarness(t)
	cases := []string{
		"/apps/" + testProjectID + "/" + testAppName + "/a/../b",
		"/apps/" + testProjectID + "/" + testAppName + "/%2e%2e/etc",
		"/apps/" + testProjectID + "/" + testAppName + "/a%2Fb",
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, h.ts.URL+raw, nil)
			req.Header.Set("Authorization", "Bearer platform-token")
			req.Header.Set("Accept", "application/json")
			resp := h.do(req)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (%s)", resp.StatusCode, readBody(t, resp))
			}
			if e := decodeEnvelope(t, resp); e.Kind != errKindBadRequest {
				t.Errorf("kind = %q, want bad_request", e.Kind)
			}
			// A traversal must never have reached resolve.
			if h.api.resolveCalls != 0 {
				t.Errorf("resolve called %d times for a rejected path", h.api.resolveCalls)
			}
			h.api.mu.Lock()
			h.api.resolveCalls = 0
			h.api.mu.Unlock()
		})
	}
}

func TestRoute_CtxPathTooLongIsBadRequest(t *testing.T) {
	h := newServerHarness(t)
	long := "/" + strings.Repeat("a", maxCtxPathLen+10)
	resp := h.bearerGet(h.url(long), "application/json")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (%s)", resp.StatusCode, readBody(t, resp))
	}
	if e := decodeEnvelope(t, resp); e.Kind != errKindBadRequest {
		t.Errorf("kind = %q, want bad_request", e.Kind)
	}
}

// ---------------------------------------------------------------------------
// Resolve-first order (spec §4.2)
// ---------------------------------------------------------------------------

func TestResolveIsCalledBeforeVerify(t *testing.T) {
	h := newServerHarness(t)
	resp := h.bearerGet(h.url(""), "application/json")
	resp.Body.Close()

	seq := h.api.callSeq()
	if len(seq) < 2 || seq[0] != "resolve" {
		t.Fatalf("call sequence = %v, want resolve first", seq)
	}
	verifyIdx := -1
	for i, c := range seq {
		if c == "session" {
			verifyIdx = i
			break
		}
	}
	if verifyIdx <= 0 {
		t.Fatalf("verify (session) not found after resolve in %v", seq)
	}
}

// A bad token on a NONEXISTENT app must be indistinguishable from one on a
// real app: resolve fails first, so the token is never verified (spec §4.2).
func TestResolveFailureShortCircuitsBeforeTokenVerify(t *testing.T) {
	h := newServerHarness(t)
	req, _ := http.NewRequest(http.MethodGet,
		h.ts.URL+"/apps/"+testProjectID+"/missing?token=vw_abc.secret", nil)
	req.Header.Set("Accept", "application/json")
	resp := h.do(req)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (%s)", resp.StatusCode, readBody(t, resp))
	}
	if e := decodeEnvelope(t, resp); e.Kind != errKindAppNotFound {
		t.Errorf("kind = %q, want app_not_found", e.Kind)
	}
	if h.api.verifyTokenCalls != 0 {
		t.Errorf("VerifyToken called %d times on a nonexistent app — resolve must short-circuit", h.api.verifyTokenCalls)
	}
	if seq := h.api.callSeq(); len(seq) != 1 || seq[0] != "resolve" {
		t.Errorf("call sequence = %v, want exactly [resolve]", seq)
	}
}

// ---------------------------------------------------------------------------
// Response matrix (spec §4.2)
// ---------------------------------------------------------------------------

func TestResponseMatrix_NavigationServesShell(t *testing.T) {
	h := newServerHarness(t)
	// `block` is present but MUST be ignored on navigation.
	resp := h.bearerGet(h.url("?block=kpis"), "")
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	if !strings.HasPrefix(strings.TrimSpace(body), "<!doctype html>") {
		t.Errorf("shell does not start with <!doctype html>:\n%s", body)
	}
	if !strings.Contains(body, `id="`+shellRootID+`"`) {
		t.Errorf("shell missing <div id=%q>", shellRootID)
	}
	if !strings.Contains(body, `id="`+shellDocScriptID+`"`) {
		t.Errorf("shell missing the doc island's <script id=%q>", shellDocScriptID)
	}
	if resp.Header.Get("Content-Security-Policy") != shellCSP {
		t.Errorf("CSP = %q, want the shell CSP", resp.Header.Get("Content-Security-Policy"))
	}
	// The full doc is embedded (block ignored → all blocks present).
	doc := scriptDoc(t, body)
	if doc["title"] != "Probe" {
		t.Errorf("embedded doc title = %v, want Probe", doc["title"])
	}
	if blocks, _ := doc["blocks"].([]any); len(blocks) != 4 {
		t.Errorf("embedded doc has %d blocks, want 4 (block param must be ignored)", len(blocks))
	}
}

func TestResponseMatrix_JSONFullDoc(t *testing.T) {
	h := newServerHarness(t)
	resp := h.bearerGet(h.url(""), "application/json")
	body := readBody(t, resp)
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("status/type = %d/%q (%s)", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if doc["title"] != "Probe" {
		t.Errorf("title = %v, want Probe", doc["title"])
	}
	if blocks, _ := doc["blocks"].([]any); len(blocks) != 4 {
		t.Errorf("full doc has %d blocks, want 4", len(blocks))
	}
}

func TestResponseMatrix_JSONSingleBlock(t *testing.T) {
	h := newServerHarness(t)
	resp := h.bearerGet(h.url("?block=kpis"), "application/json")
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, body)
	}
	var block map[string]any
	if err := json.Unmarshal([]byte(body), &block); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if block["id"] != "kpis" || block["type"] != "metric" {
		t.Errorf("block = %v, want the kpis metric block", block)
	}
	if _, hasTitle := block["title"]; hasTitle {
		t.Error("single-block response leaked the doc title — it must be the block only")
	}
	if _, hasBlocks := block["blocks"]; hasBlocks {
		t.Error("single-block response contains a blocks array — it must be one block")
	}
}

func TestResponseMatrix_UnknownBlockIsBadRequest(t *testing.T) {
	h := newServerHarness(t)
	resp := h.bearerGet(h.url("?block=does-not-exist"), "application/json")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (%s)", resp.StatusCode, readBody(t, resp))
	}
	e := decodeEnvelope(t, resp)
	if e.Kind != errKindBadRequest {
		t.Errorf("kind = %q, want bad_request", e.Kind)
	}
	if e.RequestID == "" {
		t.Error("error envelope missing request_id")
	}
}

func TestResponseMatrix_ErrorEnvelopeByAccept(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		h := newServerHarness(t)
		req, _ := http.NewRequest(http.MethodGet, h.ts.URL+"/apps/"+testProjectID+"/missing", nil)
		req.Header.Set("Authorization", "Bearer platform-token")
		req.Header.Set("Accept", "application/json")
		resp := h.do(req)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("Content-Type = %q, want json", ct)
		}
		e := decodeEnvelope(t, resp)
		if e.Kind != errKindAppNotFound || e.Error == "" || e.RequestID == "" {
			t.Errorf("envelope = %+v, want app_not_found + error + request_id", e)
		}
	})

	t.Run("html", func(t *testing.T) {
		h := newServerHarness(t)
		req, _ := http.NewRequest(http.MethodGet, h.ts.URL+"/apps/"+testProjectID+"/missing", nil)
		req.Header.Set("Authorization", "Bearer platform-token")
		resp := h.do(req)
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Errorf("Content-Type = %q, want text/html", ct)
		}
		if !strings.Contains(body, string(errKindAppNotFound)) || !strings.Contains(body, "request_id") {
			t.Errorf("HTML error page missing kind/request_id:\n%s", body)
		}
		if resp.Header.Get("Content-Security-Policy") != shellCSP {
			t.Error("HTML error page missing the shell CSP")
		}
	})
}

// ---------------------------------------------------------------------------
// §6.5 input normalization — HTTP-level
// ---------------------------------------------------------------------------

func TestParams_DuplicateKeyLastWins_HTTP(t *testing.T) {
	h := newServerHarness(t)
	body := readBody(t, h.bearerGet(h.url("?x=1&x=2"), ""))
	if got, _ := blockTextByID(scriptDoc(t, body), "x"); got != "x=2" {
		t.Errorf("duplicate key: app saw %q, want x=2 (last wins)", got)
	}
}

func TestParams_ReservedBlockStrippedFromCtxParams_HTTP(t *testing.T) {
	h := newServerHarness(t)
	body := readBody(t, h.bearerGet(h.url("?block=kpis&only=1"), ""))
	got, _ := blockTextByID(scriptDoc(t, body), "params")
	if got != "keys=only" {
		t.Errorf("ctx.params keys = %q, want keys=only (block must be stripped)", got)
	}
}

// ---------------------------------------------------------------------------
// §6.5 input normalization — readParams unit tests (the authoritative proof;
// the caps and the token/block strip live here where every branch is
// reachable without entangling the auth exchange, which consumes `?token=`).
// ---------------------------------------------------------------------------

func reqGet(rawurl string) *http.Request {
	return httptest.NewRequest(http.MethodGet, rawurl, nil)
}

func TestReadParams_StripsReservedTokenAndBlock(t *testing.T) {
	params, block, err := readParams(reqGet("/apps/p/n?token=SECRET&block=kpis&keep=1"))
	if err != nil {
		t.Fatalf("readParams: %v", err)
	}
	if block != "kpis" {
		t.Errorf("block = %q, want kpis", block)
	}
	if _, ok := params[tokenQueryParam]; ok {
		t.Error("token leaked into ctx.params")
	}
	if _, ok := params[blockQueryParam]; ok {
		t.Error("block leaked into ctx.params")
	}
	if params["keep"] != "1" || len(params) != 1 {
		t.Errorf("params = %v, want only {keep:1}", params)
	}
}

func TestReadParams_DuplicateKeyLastWins(t *testing.T) {
	params, _, err := readParams(reqGet("/apps/p/n?x=1&x=2&x=3"))
	if err != nil {
		t.Fatalf("readParams: %v", err)
	}
	if params["x"] != "3" {
		t.Errorf("x = %q, want 3 (last wins)", params["x"])
	}
}

func TestReadParams_Caps(t *testing.T) {
	t.Run("too many keys", func(t *testing.T) {
		var b strings.Builder
		for i := 0; i < maxParamKeys+1; i++ {
			if i > 0 {
				b.WriteByte('&')
			}
			fmt.Fprintf(&b, "k%d=v", i)
		}
		if _, _, err := readParams(reqGet("/apps/p/n?" + b.String())); err == nil {
			t.Error("want error for >32 keys")
		}
	})
	t.Run("key too long", func(t *testing.T) {
		key := strings.Repeat("k", maxParamKeyLen+1)
		if _, _, err := readParams(reqGet("/apps/p/n?" + key + "=v")); err == nil {
			t.Error("want error for a key >64 chars")
		}
	})
	t.Run("value too long", func(t *testing.T) {
		val := strings.Repeat("v", maxParamValueBytes+1)
		if _, _, err := readParams(reqGet("/apps/p/n?k=" + val)); err == nil {
			t.Error("want error for a value >1 KiB")
		}
	})
	t.Run("url too long", func(t *testing.T) {
		val := strings.Repeat("v", maxRequestURIBytes+10)
		if _, _, err := readParams(reqGet("/apps/p/n?k=" + val)); err == nil {
			t.Error("want error for a URL >8 KiB")
		}
	})
	t.Run("just under the key cap passes", func(t *testing.T) {
		var b strings.Builder
		for i := 0; i < maxParamKeys; i++ {
			if i > 0 {
				b.WriteByte('&')
			}
			fmt.Fprintf(&b, "k%d=v", i)
		}
		if _, _, err := readParams(reqGet("/apps/p/n?" + b.String())); err != nil {
			t.Errorf("32 keys should pass: %v", err)
		}
	})
}

func TestReadParams_POSTRequiresJSON(t *testing.T) {
	t.Run("missing content-type", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/apps/p/n", strings.NewReader(`{"a":"b"}`))
		if _, _, err := readParams(r); err == nil {
			t.Error("POST without application/json must be rejected")
		}
	})
	t.Run("wrong content-type", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/apps/p/n", strings.NewReader(`{"a":"b"}`))
		r.Header.Set("Content-Type", "text/plain")
		if _, _, err := readParams(r); err == nil {
			t.Error("POST with text/plain must be rejected")
		}
	})
	t.Run("string body merges over query", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/apps/p/n?a=fromquery&q=1",
			strings.NewReader(`{"a":"frombody"}`))
		r.Header.Set("Content-Type", "application/json")
		params, _, err := readParams(r)
		if err != nil {
			t.Fatalf("readParams: %v", err)
		}
		if params["a"] != "frombody" {
			t.Errorf("body must override query: a = %q, want frombody", params["a"])
		}
		if params["q"] != "1" {
			t.Errorf("query-only key lost: q = %q", params["q"])
		}
	})
	t.Run("nested values rejected", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/apps/p/n", strings.NewReader(`{"a":[1,2]}`))
		r.Header.Set("Content-Type", "application/json")
		if _, _, err := readParams(r); err == nil {
			t.Error("arrays/objects must be rejected (no nesting)")
		}
	})
}

// §6.5: ctx.params is string→string with NO type coercion. A POST body with a
// non-string scalar must be REJECTED (the client sends `{"n":"42"}`), matching
// the inherently-string GET query path — not coerced to "42"/"true".
func TestReadParams_POSTRejectsNonStringValues(t *testing.T) {
	reject := []string{
		`{"n":42}`,      // number
		`{"b":true}`,    // bool
		`{"z":null}`,    // null
		`{"o":{"k":1}}`, // object
	}
	for _, body := range reject {
		t.Run("reject "+body, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/apps/p/n", strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			if _, _, err := readParams(r); err == nil {
				t.Errorf("body %s must be rejected — no coercion (§6.5)", body)
			}
		})
	}
	t.Run("accept string", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/apps/p/n", strings.NewReader(`{"n":"42"}`))
		r.Header.Set("Content-Type", "application/json")
		params, _, err := readParams(r)
		if err != nil {
			t.Fatalf("string value must be accepted: %v", err)
		}
		if params["n"] != "42" {
			t.Errorf("n = %q, want \"42\"", params["n"])
		}
	})
}

// The 16 KiB cap is enforced PRE-PARSE: an oversized body is refused by size,
// never handed to the JSON decoder.
func TestReadParams_POSTBodyCapIsPreParse(t *testing.T) {
	// Deliberately-huge body; also invalid JSON past the cap. The size error
	// must win, proving nothing tried to parse it.
	big := `{"k":"` + strings.Repeat("a", maxPostBodyBytes+2000) + `"}`
	r := httptest.NewRequest(http.MethodPost, "/apps/p/n", strings.NewReader(big))
	r.Header.Set("Content-Type", "application/json")
	_, _, err := readParams(r)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("err = %v, want a pre-parse size rejection", err)
	}
}

func TestParams_PostBodyTooLargeIsBadRequest_HTTP(t *testing.T) {
	h := newServerHarness(t)
	big := `{"k":"` + strings.Repeat("a", maxPostBodyBytes+2000) + `"}`
	req, _ := http.NewRequest(http.MethodPost, h.url(""), strings.NewReader(big))
	req.Header.Set("Authorization", "Bearer platform-token")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp := h.do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if e := decodeEnvelope(t, resp); e.Kind != errKindBadRequest {
		t.Errorf("kind = %q, want bad_request", e.Kind)
	}
}

// ---------------------------------------------------------------------------
// Headers + access log (spec §5.3, §9)
// ---------------------------------------------------------------------------

func TestHeaders_ReferrerPolicyOnEveryResponse(t *testing.T) {
	h := newServerHarness(t)
	// success
	ok := h.bearerGet(h.url(""), "application/json")
	ok.Body.Close()
	if ok.Header.Get("Referrer-Policy") != "no-referrer" {
		t.Error("success response missing Referrer-Policy: no-referrer")
	}
	// error
	req, _ := http.NewRequest(http.MethodGet, h.ts.URL+"/apps/"+testProjectID+"/missing", nil)
	req.Header.Set("Authorization", "Bearer platform-token")
	req.Header.Set("Accept", "application/json")
	er := h.do(req)
	er.Body.Close()
	if er.Header.Get("Referrer-Policy") != "no-referrer" {
		t.Error("error response missing Referrer-Policy: no-referrer")
	}
}

// The access log carries structured fields ONLY — never the request URL, its
// query string, or the Referer. A request with a plaintext token in the query
// and a Referer header must leak neither into the log.
func TestAccessLog_NoURLOrReferer(t *testing.T) {
	h := newServerHarness(t)
	const secret = "SEKRIT-token-value"
	const referer = "https://evil.example/leak"
	req, _ := http.NewRequest(http.MethodGet, h.url("?block=kpis&sensitive="+secret), nil)
	req.Header.Set("Authorization", "Bearer platform-token")
	req.Header.Set("Referer", referer)
	// navigation → shell (block ignored); the point is what the log captures.
	h.do(req).Body.Close()

	logged := h.logBuf.String()
	if !strings.Contains(logged, "app_render_access") {
		t.Fatalf("no access-log line emitted:\n%s", logged)
	}
	for _, banned := range []string{secret, referer, "evil.example", "block=kpis", "sensitive="} {
		if strings.Contains(logged, banned) {
			t.Errorf("access log leaked %q:\n%s", banned, logged)
		}
	}
}

// params_hash is computed AFTER the reserved names are stripped, and is a
// hash (never plaintext values).
func TestAccessLog_ParamsHashIsPostStripping(t *testing.T) {
	h := newServerHarness(t)
	req, _ := http.NewRequest(http.MethodGet, h.url("?block=kpis&x=1"), nil)
	req.Header.Set("Authorization", "Bearer platform-token")
	h.do(req).Body.Close()

	logged := h.logBuf.String()
	wantHash := paramsHash(map[string]string{"x": "1"}) // block stripped
	wrongHash := paramsHash(map[string]string{"x": "1", "block": "kpis"})
	if !strings.Contains(logged, "params_hash="+wantHash) {
		t.Errorf("params_hash not computed post-stripping; log:\n%s", logged)
	}
	if wantHash == wrongHash {
		t.Fatal("test bug: stripped and unstripped hashes coincide")
	}
	if strings.Contains(logged, "params_hash="+wrongHash) {
		t.Error("params_hash was computed BEFORE stripping the reserved block param")
	}
}

func TestAccessLog_CarriesPrincipalFields(t *testing.T) {
	h := newServerHarness(t)
	h.bearerGet(h.url(""), "application/json").Body.Close()
	logged := h.logBuf.String()
	for _, want := range []string{
		"principal_kind=" + principalKindPlatformUser,
		"principal_id=user-1",
		"params_hash=",
		"outcome=" + outcomeOK,
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("access log missing %q:\n%s", want, logged)
		}
	}
}

// The client-IP field uses s.clientIP(r), so it agrees with the rate-limit
// bucket: an untrusted X-Forwarded-For must NOT appear (default config trusts
// no proxy).
func TestAccessLog_ClientIPUsesResolver(t *testing.T) {
	h := newServerHarness(t)
	req, _ := http.NewRequest(http.MethodGet, h.url(""), nil)
	req.Header.Set("Authorization", "Bearer platform-token")
	req.Header.Set("X-Forwarded-For", "9.9.9.9")
	req.Header.Set("Accept", "application/json")
	h.do(req).Body.Close()

	logged := h.logBuf.String()
	if !strings.Contains(logged, "client_ip=127.0.0.1") {
		t.Errorf("client_ip should be the loopback peer (via s.clientIP), log:\n%s", logged)
	}
	if strings.Contains(logged, "9.9.9.9") {
		t.Error("client_ip took an untrusted X-Forwarded-For — must route through s.clientIP")
	}
}

// ---------------------------------------------------------------------------
// Rate limiting is wired (hand-off #2): allowRender runs once per render.
// ---------------------------------------------------------------------------

func TestRateLimit_AllowRenderWired(t *testing.T) {
	h := newServerHarness(t) // constant clock → buckets do not refill
	var got429 *http.Response
	for i := 0; i < renderRatePerPrincipalBurst+1; i++ {
		resp := h.bearerGet(h.url(""), "application/json")
		if resp.StatusCode == http.StatusTooManyRequests {
			got429 = resp
			break
		}
		if resp.StatusCode != 200 {
			t.Fatalf("request %d status = %d, want 200 (%s)", i, resp.StatusCode, readBody(t, resp))
		}
		resp.Body.Close()
	}
	if got429 == nil {
		t.Fatalf("no 429 after %d renders — allowRender is not wired", renderRatePerPrincipalBurst+1)
	}
	defer got429.Body.Close()
	ra := got429.Header.Get("Retry-After")
	if ra == "" {
		t.Error("429 missing Retry-After")
	}
	if e := decodeEnvelope(t, got429); e.Kind != errKindRateLimited {
		t.Errorf("kind = %q, want rate_limited", e.Kind)
	}
}

// ---------------------------------------------------------------------------
// Health + readiness (hand-off #5)
// ---------------------------------------------------------------------------

func TestReadyz_GatedOnEngineCompile(t *testing.T) {
	api := &fakeServerAPI{bundle: []byte(serverProbeBundle), member: true}
	srv := NewServer(Config{Render: renderConfig()}, testEngine(t),
		WithResolveAPI(api), WithAuthAPI(api), WithRenderAPI(api),
		WithCookieKey(testCookieKey))
	ts := httptest.NewServer(srv)
	defer ts.Close()
	c := ts.Client()

	// Before markEngineReady: /readyz is 503, /healthz still 200.
	if resp, _ := c.Get(ts.URL + "/readyz"); resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before compile: got %v, want 503", statusOf(resp))
	}
	if resp, _ := c.Get(ts.URL + "/healthz"); resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz before compile: got %v, want 200", statusOf(resp))
	}

	srv.markEngineReady()

	if resp, _ := c.Get(ts.URL + "/readyz"); resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("/readyz after compile: got %v, want 200", statusOf(resp))
	}
	if resp, _ := c.Get(ts.URL + "/healthz"); resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz after compile: got %v, want 200", statusOf(resp))
	}
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return -1
	}
	resp.Body.Close()
	return resp.StatusCode
}

// ---------------------------------------------------------------------------
// Metrics (spec §9): datuplet_appworker_render_requests_total{outcome}
// ---------------------------------------------------------------------------

func TestMetrics_EndpointExposesRenderCounter(t *testing.T) {
	h := newServerHarness(t)
	h.bearerGet(h.url(""), "application/json").Body.Close()

	resp, err := h.ts.Client().Get(h.ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("/metrics: %v", err)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, renderRequestsOpts.Name) {
		t.Errorf("/metrics does not expose %q:\n%s", renderRequestsOpts.Name, body)
	}
	if !strings.Contains(body, outcomeLabel+`="`+outcomeOK+`"`) {
		t.Errorf("/metrics missing the ok outcome series:\n%s", body)
	}
}

// The counter moves per outcome label across the WHOLE §8/§9 label set. Each
// row drives a fresh harness (fresh registry) so the counts are isolated.
func TestMetrics_CounterMovesPerOutcome(t *testing.T) {
	labelSet := []string{
		outcomeOK, "render_error", "timeout", "rate_limited",
		"capacity", "unauthorized", "bad_request", "unavailable",
	}

	drive := map[string]func(t *testing.T) *serverHarness{
		outcomeOK: func(t *testing.T) *serverHarness {
			h := newServerHarness(t)
			h.bearerGet(h.url(""), "application/json").Body.Close()
			return h
		},
		"render_error": func(t *testing.T) *serverHarness {
			h := newServerHarness(t)
			h.api.bundle = []byte(serverInvalidDocBundle)
			h.bearerGet(h.url(""), "application/json").Body.Close()
			return h
		},
		"timeout": func(t *testing.T) *serverHarness {
			cfg := Config{Render: renderConfig()}
			cfg.Render.TimeoutS = 1 // real ~1s wall-clock backstop
			h := newServerHarnessWith(t, cfg, 0)
			h.api.bundle = []byte(serverHotLoopBundle)
			h.bearerGet(h.url(""), "application/json").Body.Close()
			return h
		},
		"rate_limited": func(t *testing.T) *serverHarness {
			h := newServerHarness(t)
			for i := 0; i < renderRatePerPrincipalBurst+1; i++ {
				h.bearerGet(h.url(""), "application/json").Body.Close()
			}
			return h
		},
		"capacity":     driveCapacity,
		"unauthorized": driveUnauthorized,
		"bad_request": func(t *testing.T) *serverHarness {
			h := newServerHarness(t)
			h.bearerGet(h.url("?block=nope"), "application/json").Body.Close()
			return h
		},
		"unavailable": func(t *testing.T) *serverHarness {
			h := newServerHarness(t)
			h.api.mu.Lock()
			h.api.resolveErr = errors.New("pipeline-api down")
			h.api.mu.Unlock()
			h.bearerGet(h.url(""), "application/json").Body.Close()
			return h
		},
	}

	for _, outcome := range labelSet {
		t.Run(outcome, func(t *testing.T) {
			h := drive[outcome](t)
			if got := testutil.ToFloat64(h.srv.renderRequests.WithLabelValues(outcome)); got < 1 {
				t.Errorf("counter{outcome=%q} = %v, want >= 1", outcome, got)
			}
		})
	}
}

func driveUnauthorized(t *testing.T) *serverHarness {
	h := newServerHarness(t)
	// No credential at all → authenticate writes 401 unauthorized.
	req, _ := http.NewRequest(http.MethodGet, h.url(""), nil)
	req.Header.Set("Accept", "application/json")
	h.do(req).Body.Close()
	return h
}

// driveCapacity holds the single pool slot with a blocking query on app A,
// then a render for app B is shed as `capacity` after the bounded wait.
func driveCapacity(t *testing.T) *serverHarness {
	cfg := Config{Render: renderConfig()}
	cfg.Render.Concurrency = 1
	cfg.Render.PerAppInflight = 2
	h := newServerHarnessWith(t, cfg, 30*time.Millisecond)
	h.api.bundle = []byte(serverQueryBundle)
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	h.api.mu.Lock()
	h.api.queryFn = blockingQuery(arrived, release)
	h.api.mu.Unlock()

	holdURL := h.ts.URL + "/apps/" + testProjectID + "/app-hold"
	otherURL := h.ts.URL + "/apps/" + testProjectID + "/app-other"

	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequest(http.MethodGet, holdURL, nil)
		req.Header.Set("Authorization", "Bearer platform-token")
		req.Header.Set("Accept", "application/json")
		resp, err := h.client().Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()

	<-arrived // render #1 now holds the pool slot inside its query

	req, _ := http.NewRequest(http.MethodGet, otherURL, nil)
	req.Header.Set("Authorization", "Bearer platform-token")
	req.Header.Set("Accept", "application/json")
	resp := h.do(req)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("capacity: status = %d, want 503 (%s)", resp.StatusCode, readBody(t, resp))
	}
	if e := decodeEnvelope(t, resp); e.Kind != errKindCapacity {
		t.Errorf("capacity: kind = %q, want capacity", e.Kind)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("capacity 503 missing Retry-After")
	}
	close(release)
	<-done
	return h
}

// ---------------------------------------------------------------------------
// Method handling
// ---------------------------------------------------------------------------

func TestRoute_UnsupportedMethodIsRejected(t *testing.T) {
	h := newServerHarness(t)
	req, _ := http.NewRequest(http.MethodDelete, h.url(""), nil)
	req.Header.Set("Authorization", "Bearer platform-token")
	req.Header.Set("Accept", "application/json")
	resp := h.do(req)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 (%s)", resp.StatusCode, readBody(t, resp))
	}
	if got := resp.Header.Get("Allow"); got != "GET, POST" {
		t.Errorf("Allow = %q, want \"GET, POST\"", got)
	}
}

// ---------------------------------------------------------------------------
// Boot wiring + loud failure (hand-off #1)
// ---------------------------------------------------------------------------

func TestServe_FailsLoudlyOnMissingBootSecrets(t *testing.T) {
	goodCookie := writeTempSecret(t, "cookie", "hmac-key")
	goodToken := writeTempSecret(t, "svc", "svc-token")
	emptyCookie := writeTempSecret(t, "empty-cookie", "   ")

	cases := []struct {
		name    string
		cfg     Config
		wantSub string
	}{
		{
			name:    "no API URL",
			cfg:     Config{CookieKeyFile: goodCookie, ServiceTokenFile: goodToken, Render: renderConfig()},
			wantSub: EnvAPIURL,
		},
		{
			name:    "no cookie key file",
			cfg:     Config{APIURL: "http://api", ServiceTokenFile: goodToken, Render: renderConfig()},
			wantSub: "cookie",
		},
		{
			name:    "cookie key file missing on disk",
			cfg:     Config{APIURL: "http://api", CookieKeyFile: "/no/such/file", ServiceTokenFile: goodToken, Render: renderConfig()},
			wantSub: "cannot read",
		},
		{
			name:    "cookie key file empty",
			cfg:     Config{APIURL: "http://api", CookieKeyFile: emptyCookie, ServiceTokenFile: goodToken, Render: renderConfig()},
			wantSub: "empty",
		},
		{
			name:    "no service token file",
			cfg:     Config{APIURL: "http://api", CookieKeyFile: goodCookie, Render: renderConfig()},
			wantSub: "service token",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engineCalled := false
			newEngine := func(context.Context, uint32) (Engine, error) {
				engineCalled = true
				return nil, errors.New("engine should never be built on a boot-config error")
			}
			err := Serve(context.Background(), tc.cfg, newEngine)
			if err == nil {
				t.Fatal("Serve returned nil, want a loud boot error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %q, want it to mention %q", err.Error(), tc.wantSub)
			}
			if engineCalled {
				t.Error("engine was constructed despite a boot-config error — boot must fail before that")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fix round (RFC 028 W6 fix): shell escaping, block lookup, bundle cap,
// non-string POST params
// ---------------------------------------------------------------------------

// Finding 1 (BLOCKER): app-controlled doc content must not break out of the
// shell's <script type="application/json"> element.
//
// Since Part 4 (V0) replaced the W6 stub with the real ui/appshell/index.html
// asset, the legitimate response now carries SEVERAL <script> elements (the
// doc island, the vendored libraries, the module loader) — a fixed
// `</script>` count of 1 is no longer the right invariant. Instead:
//  1. every "<script" open has a matching "</script>" close (an unescaped
//     payload injects a BARE close with no matching open, which this
//     balance check catches regardless of how many legitimate script tags
//     the template has);
//  2. the raw injected markup never appears verbatim in the response; and
//  3. the doc still round-trips (truncated/broken JSON would fail here).
func TestShell_EscapesScriptBreakout(t *testing.T) {
	h := newServerHarness(t)
	h.api.bundle = []byte(serverInjectionBundle)
	body := readBody(t, h.bearerGet(h.url(""), "")) // navigation → shell

	if opens, closes := strings.Count(body, "<script"), strings.Count(body, "</script>"); opens != closes {
		t.Fatalf("found %d <script but %d </script> in the shell response — "+
			"app content broke out of the doc script element:\n%s", opens, closes, body)
	}
	if strings.Contains(body, "<img src=x onerror=boom>") {
		t.Fatalf("the raw injected payload appears unescaped in the shell response:\n%s", body)
	}
	doc := scriptDoc(t, body) // truncated/broken JSON would fail here
	got, ok := blockTextByID(doc, "a")
	if !ok || got != "</script><img src=x onerror=boom>" {
		t.Errorf("round-tripped block text = %q (ok=%v), want the original payload", got, ok)
	}
}

// escapeJSONForScript neutralizes the breakout bytes AND the two JS line
// terminators while preserving the value semantically (JSON.parse round-trip).
func TestEscapeJSONForScript_RoundTripsAndNeutralizes(t *testing.T) {
	ls := string(rune(0x2028)) // U+2028 LINE SEPARATOR
	ps := string(rune(0x2029)) // U+2029 PARAGRAPH SEPARATOR
	orig := json.RawMessage(`{"t":"</script>&` + ls + ps + `x"}`)
	escaped := escapeJSONForScript(orig)

	if strings.Contains(escaped, "</script>") {
		t.Error("escaped output still contains a raw </script>")
	}
	if strings.ContainsRune(escaped, rune(0x2028)) || strings.ContainsRune(escaped, rune(0x2029)) {
		t.Error("escaped output still contains a raw JS line terminator")
	}
	if strings.ContainsRune(escaped, '<') || strings.ContainsRune(escaped, '>') || strings.ContainsRune(escaped, '&') {
		t.Errorf("escaped output still contains a raw <, >, or &: %q", escaped)
	}
	// Semantic round-trip: the escaped bytes are valid JSON parsing to the
	// identical value (what the browser's JSON.parse sees).
	var a, b any
	if err := json.Unmarshal([]byte(escaped), &a); err != nil {
		t.Fatalf("escaped output is not valid JSON: %v (%q)", err, escaped)
	}
	if err := json.Unmarshal(orig, &b); err != nil {
		t.Fatalf("original is not valid JSON: %v", err)
	}
	if fmt.Sprintf("%v", a) != fmt.Sprintf("%v", b) {
		t.Errorf("round-trip mismatch: escaped→%v, original→%v", a, b)
	}
}

// Finding 2: a table with plain-ARRAY rows must be findable by its own id.
func TestFindBlock_PlainRowTableIsFoundByID(t *testing.T) {
	h := newServerHarness(t)
	h.api.bundle = []byte(serverPlainRowTableBundle)
	resp := h.bearerGet(h.url("?block=tbl"), "application/json")
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 — a plain-row table must be findable by id (%s)", resp.StatusCode, body)
	}
	var blk map[string]any
	if err := json.Unmarshal([]byte(body), &blk); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if blk["id"] != "tbl" || blk["type"] != "table" {
		t.Errorf("block = %v, want the tbl table block", blk)
	}
}

// Finding 2: an object-row table's modal-nested block still resolves, and the
// table itself resolves.
func TestFindBlock_ObjectRowModalStillResolves(t *testing.T) {
	h := newServerHarness(t)
	h.api.bundle = []byte(serverObjectRowModalBundle)

	if resp := h.bearerGet(h.url("?block=tbl2"), "application/json"); resp.StatusCode != 200 {
		t.Errorf("block=tbl2 status = %d, want 200", resp.StatusCode)
		resp.Body.Close()
	} else {
		resp.Body.Close()
	}

	resp := h.bearerGet(h.url("?block=deep"), "application/json")
	body := readBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("block=deep status = %d, want 200 — row-modal nesting must resolve (%s)", resp.StatusCode, body)
	}
	var blk map[string]any
	if err := json.Unmarshal([]byte(body), &blk); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if blk["id"] != "deep" {
		t.Errorf("block = %v, want the deep block", blk)
	}
}

// Finding 3: an operator-configured bundle cap BELOW the 5 MB hard default must
// be wired into the pipeline-api client (not silently overridden).
func TestServe_ConfiguredBundleCapIsWiredIntoClient(t *testing.T) {
	cfg := Config{APIURL: "http://api", Render: RenderConfig{BundleMaxBytes: 1 << 20}}
	c := newConfiguredAPIClient(cfg, "tok")
	defer c.Close()
	if c.maxBundleBytes != int64(1<<20) {
		t.Errorf("client maxBundleBytes = %d, want %d (configured lower cap must win)",
			c.maxBundleBytes, int64(1<<20))
	}
}
