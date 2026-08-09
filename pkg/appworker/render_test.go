package appworker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/appengine"
	"github.com/datuplet/datuplet/pkg/appengine/outputdoc"
)

// ---------------------------------------------------------------------------
// Shared real engine
//
// The brief mandates the REAL appengine.Engine (with a fake APIClient) so the
// tests exercise the actual guest ABI — the query envelope shape, the
// settled-flag protocol, the wall-clock preemption. NewEngine costs ~0.25s, so
// one instance is shared across the whole package's render tests; Engine is
// documented safe for concurrent Render calls, which is exactly what the
// admission-gate tests need.
// ---------------------------------------------------------------------------

var (
	sharedEngineOnce sync.Once
	sharedEngine     *appengine.Engine
	sharedEngineErr  error
)

func testEngine(t *testing.T) *appengine.Engine {
	t.Helper()
	sharedEngineOnce.Do(func() {
		sharedEngine, sharedEngineErr = appengine.NewEngine(context.Background(), 2048)
	})
	if sharedEngineErr != nil {
		t.Fatalf("appengine.NewEngine: %v", sharedEngineErr)
	}
	return sharedEngine
}

// ---------------------------------------------------------------------------
// Fake pipeline-api client (renderAPI)
// ---------------------------------------------------------------------------

type queryCall struct {
	PID  string
	JWT  string
	Body []byte
}

type fakeRenderAPI struct {
	mu sync.Mutex

	bundles   map[string][]byte
	bundleErr error

	impersonateCalls int
	impersonateErr   error
	tokenSeq         int

	queryCalls []queryCall
	// queryFn, when set, produces the response for each Query call. The
	// default returns one row.
	queryFn func(ctx context.Context, pid, jwt string, body []byte) (*http.Response, error)

	logs   []RenderLogRecord
	logErr error
}

func newFakeRenderAPI(bundle []byte) *fakeRenderAPI {
	return &fakeRenderAPI{bundles: map[string][]byte{testVersionHash: bundle}}
}

func (f *fakeRenderAPI) Bundle(_ context.Context, hash string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bundleErr != nil {
		return nil, f.bundleErr
	}
	b, ok := f.bundles[hash]
	if !ok {
		return nil, &APIError{StatusCode: http.StatusNotFound, Kind: "app_not_found", Message: "no such bundle"}
	}
	return b, nil
}

func (f *fakeRenderAPI) Impersonate(_ context.Context, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.impersonateErr != nil {
		return "", f.impersonateErr
	}
	f.impersonateCalls++
	f.tokenSeq++
	return fmt.Sprintf("jwt-%d", f.tokenSeq), nil
}

func (f *fakeRenderAPI) Query(ctx context.Context, pid, jwt string, body []byte) (*http.Response, error) {
	f.mu.Lock()
	f.queryCalls = append(f.queryCalls, queryCall{PID: pid, JWT: jwt, Body: append([]byte(nil), body...)})
	fn := f.queryFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, pid, jwt, body)
	}
	return jsonResponse(http.StatusOK, `{"schema":[{"name":"n","type":"BIGINT"}],"rows":[[7]],"truncated":false,"stats":{}}`), nil
}

func (f *fakeRenderAPI) AppendLog(_ context.Context, rec RenderLogRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.logErr != nil {
		return f.logErr
	}
	f.logs = append(f.logs, rec)
	return nil
}

func (f *fakeRenderAPI) snapshot() (impersonate int, queries []queryCall, logs []RenderLogRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.impersonateCalls,
		append([]queryCall(nil), f.queryCalls...),
		append([]RenderLogRecord(nil), f.logs...)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

// testProjectID / testAppID are declared in auth_test.go and reused here so
// the two halves of the render path share one fixture identity.
const testVersionHash = "0000000000000000000000000000000000000000000000000000000000000001"

func testResolved(appID string) resolvedApp {
	return resolvedApp{
		ProjectID:   testProjectID,
		Name:        "sales",
		Channel:     channelProduction,
		AppID:       appID,
		VersionHash: testVersionHash,
	}
}

func testPrincipal() principal {
	return principal{Kind: principalKindViewerToken, ID: "tok-1"}
}

// renderConfig is the default render-limit set for these tests: small wall
// clock so a timeout test doesn't dominate the suite, spec defaults elsewhere.
func renderConfig() RenderConfig {
	return RenderConfig{
		TimeoutS:          5,
		MaxTimeoutS:       HardCapTimeoutS,
		MemoryMiB:         DefaultMemoryMiB,
		MaxMemoryMiB:      HardCapMemoryMiB,
		QueriesPerRender:  DefaultQueriesPerRender,
		OutputDocMaxBytes: DefaultOutputDocMaxBytes,
		BundleMaxBytes:    DefaultBundleMaxBytes,
		PerAppInflight:    DefaultPerAppInflight,
		Concurrency:       DefaultConcurrency,
	}
}

func newRenderServer(t *testing.T, api *fakeRenderAPI, opts ...ServerOption) *Server {
	t.Helper()
	cfg := Config{Render: renderConfig()}
	all := append([]ServerOption{WithRenderAPI(api)}, opts...)
	return NewServer(cfg, testEngine(t), all...)
}

func newRenderServerCfg(t *testing.T, cfg Config, api *fakeRenderAPI, opts ...ServerOption) *Server {
	t.Helper()
	all := append([]ServerOption{WithRenderAPI(api)}, opts...)
	return NewServer(cfg, testEngine(t), all...)
}

// stepClock advances by step on every read. Monotonic and deterministic, so
// deadline arithmetic is asserted without sleeping.
type stepClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.now
	c.now = c.now.Add(c.step)
	return t
}

// blockingQuery returns a query function that signals arrival on `arrived` and
// then waits until `release` closes (or ctx dies). It is how the gate tests
// hold a render in flight.
func blockingQuery(arrived chan<- struct{}, release <-chan struct{}) func(context.Context, string, string, []byte) (*http.Response, error) {
	return func(ctx context.Context, _, _ string, _ []byte) (*http.Response, error) {
		select {
		case arrived <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return jsonResponse(http.StatusOK, `{"schema":[],"rows":[],"truncated":false,"stats":{}}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// ---------------------------------------------------------------------------
// Bundles
// ---------------------------------------------------------------------------

// appendixABundle mirrors spec Appendix A's worked example: one bound-param
// query, a metric + chart + table doc, and a refreshInterval.
const appendixABundle = `var __dtp_app = { render: async (ctx) => {
	const r = await datuplet.query(
		"SELECT day, revenue FROM sales WHERE region = $region",
		{ region: ctx.params.region ?? "EU" },
		{ maxRows: 500, timeoutS: 60 });
	console.log("rows=" + r.rows.length);
	return {
		outputDoc: 1,
		title: "Sales overview",
		refreshInterval: 60,
		blocks: [
			{ id: "kpi", type: "metric", items: [{ label: "Rows", value: r.rows.length }] },
			{ id: "chart", type: "chart", library: "vega-lite", spec: {
				mark: "bar",
				data: { values: r.rows.map((row) => ({ day: row[0], revenue: row[1] })) },
				encoding: { x: { field: "day", type: "nominal" }, y: { field: "revenue", type: "quantitative" } } } },
			{ id: "note", type: "markdown", text: "region: " + (ctx.params.region ?? "EU") }
		]
	};
}};`

// kindEchoBundle catches a query rejection and renders the kind, proving the
// guest sees the mapped {kind, message} shape and can degrade gracefully.
const kindEchoBundle = `var __dtp_app = { render: async () => {
	let kind = "none", msg = "";
	try { await datuplet.query("SELECT 1"); }
	catch (e) { kind = e.kind || "?"; msg = String(e.message || ""); }
	return { outputDoc: 1, title: "t", blocks: [
		{ id: "k", type: "markdown", text: "kind=" + kind },
		{ id: "m", type: "markdown", text: "msg=" + msg } ] };
}};`

// docOnlyBundle renders without touching datuplet.query at all.
const docOnlyBundle = `var __dtp_app = { render: () => ({
	outputDoc: 1, title: "t", blocks: [{ id: "a", type: "markdown", text: "hi" }] }) };`

func refreshBundle(interval string) []byte {
	return []byte(fmt.Sprintf(`var __dtp_app = { render: () => ({
		outputDoc: 1, title: "t", refreshInterval: %s,
		blocks: [{ id: "a", type: "markdown", text: "hi" }] }) };`, interval))
}

// ---------------------------------------------------------------------------
// Helpers for reading a rendered doc back
// ---------------------------------------------------------------------------

type renderedDoc struct {
	OutputDoc       int             `json:"outputDoc"`
	Title           string          `json:"title"`
	RefreshInterval *int            `json:"refreshInterval"`
	Blocks          []renderedBlock `json:"blocks"`
	Raw             json.RawMessage `json:"-"`
}

type renderedBlock struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Text string          `json:"text"`
	Spec json.RawMessage `json:"spec"`
}

func decodeDoc(t *testing.T, doc json.RawMessage) renderedDoc {
	t.Helper()
	var d renderedDoc
	if err := json.Unmarshal(doc, &d); err != nil {
		t.Fatalf("decode rendered doc: %v\ndoc: %s", err, doc)
	}
	d.Raw = doc
	return d
}

func blockText(t *testing.T, d renderedDoc, id string) string {
	t.Helper()
	for _, b := range d.Blocks {
		if b.ID == id {
			return b.Text
		}
	}
	t.Fatalf("no block %q in doc: %s", id, d.Raw)
	return ""
}

// ---------------------------------------------------------------------------
// 1. Happy path
// ---------------------------------------------------------------------------

func TestRenderHappyPath(t *testing.T) {
	api := newFakeRenderAPI([]byte(appendixABundle))
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/",
		map[string]string{"region": "NA"}, testPrincipal())
	if rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}

	// The returned doc must independently satisfy the OutputDoc validator.
	if err := outputdoc.Validate(doc); err != nil {
		t.Fatalf("returned doc fails outputdoc.Validate: %v\ndoc: %s", err, doc)
	}
	d := decodeDoc(t, doc)
	if d.Title != "Sales overview" {
		t.Errorf("title = %q", d.Title)
	}
	if got := blockText(t, d, "note"); got != "region: NA" {
		t.Errorf("params did not reach the guest: note = %q", got)
	}

	impersonations, queries, logs := api.snapshot()

	// Exactly one fresh mint per render (spec §5.4) — never zero (the render
	// queried) and never one per query.
	if impersonations != 1 {
		t.Errorf("Impersonate calls = %d, want 1", impersonations)
	}
	if len(queries) != 1 {
		t.Fatalf("Query calls = %d, want 1", len(queries))
	}
	if queries[0].PID != testProjectID {
		t.Errorf("query pid = %q, want %q", queries[0].PID, testProjectID)
	}
	if queries[0].JWT != "jwt-1" {
		t.Errorf("query jwt = %q, want the minted impersonation token", queries[0].JWT)
	}

	var wire struct {
		SQL      string         `json:"sql"`
		TimeoutS *int           `json:"timeout_s"`
		MaxRows  *int           `json:"max_rows"`
		Params   map[string]any `json:"params"`
	}
	if err := json.Unmarshal(queries[0].Body, &wire); err != nil {
		t.Fatalf("decode query body: %v (%s)", err, queries[0].Body)
	}
	if !strings.Contains(wire.SQL, "$region") {
		t.Errorf("sql = %q, want the guest's placeholder SQL", wire.SQL)
	}
	if wire.Params["region"] != "NA" {
		t.Errorf("bound params = %v, want region=NA", wire.Params)
	}
	if wire.MaxRows == nil || *wire.MaxRows != 500 {
		t.Errorf("max_rows = %s, want the guest's 500", intPtr(wire.MaxRows))
	}
	// The guest asked for 60s; the render budget is 5s, so the effective value
	// is min(60, floor(remaining)) — never above the render's own budget.
	if wire.TimeoutS == nil || *wire.TimeoutS > 5 || *wire.TimeoutS < 1 {
		t.Errorf("timeout_s = %s, want clamped into [1,5]", intPtr(wire.TimeoutS))
	}

	if len(logs) != 1 {
		t.Fatalf("AppendLog records = %d, want 1", len(logs))
	}
	rec := logs[0]
	if rec.Outcome != outcomeOK {
		t.Errorf("outcome = %q, want %q", rec.Outcome, outcomeOK)
	}
	if rec.AppID != testAppID || rec.VersionHash != testVersionHash || rec.Channel != channelProduction {
		t.Errorf("log identity = %+v", rec)
	}
	if rec.PrincipalKind != principalKindViewerToken || rec.PrincipalID != "tok-1" {
		t.Errorf("log principal = %q/%q", rec.PrincipalKind, rec.PrincipalID)
	}
	if rec.RequestID == "" {
		t.Error("log record has no request_id")
	}
	if rec.StartedAt.IsZero() {
		t.Error("log record has no started_at")
	}
	if !strings.Contains(rec.LogText, "rows=1") {
		t.Errorf("log_text = %q, want the guest's console.log", rec.LogText)
	}
	if rec.Error != "" {
		t.Errorf("error = %q, want empty on success", rec.Error)
	}
}

func TestRenderUsesRequestIDFromContext(t *testing.T) {
	api := newFakeRenderAPI([]byte(docOnlyBundle))
	s := newRenderServer(t, api)

	ctx := withRequestID(context.Background(), "req-42")
	if _, rerr := s.render(ctx, testResolved(testAppID), "/", nil, testPrincipal()); rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	_, _, logs := api.snapshot()
	if len(logs) != 1 || logs[0].RequestID != "req-42" {
		t.Fatalf("log request_id = %+v, want req-42", logs)
	}
}

// A render that never queries must not mint an impersonation JWT: the mint is
// audited server-side, so minting one nobody uses pollutes the audit trail
// (and costs a control-plane round trip inside the render deadline).
func TestRenderWithoutQueriesDoesNotImpersonate(t *testing.T) {
	api := newFakeRenderAPI([]byte(docOnlyBundle))
	s := newRenderServer(t, api)

	if _, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal()); rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	impersonations, queries, _ := api.snapshot()
	if impersonations != 0 {
		t.Errorf("Impersonate calls = %d, want 0 for a query-less render", impersonations)
	}
	if len(queries) != 0 {
		t.Errorf("Query calls = %d, want 0", len(queries))
	}
}

// Two renders must mint two DIFFERENT tokens — never a cached one (P4 made the
// jti cryptographically random precisely so concurrent renders are
// individually attributable in query_audit).
func TestRenderMintsAFreshTokenPerRender(t *testing.T) {
	api := newFakeRenderAPI([]byte(appendixABundle))
	s := newRenderServer(t, api)

	for i := 0; i < 2; i++ {
		if _, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal()); rerr != nil {
			t.Fatalf("render %d error: %+v", i, rerr)
		}
	}
	impersonations, queries, _ := api.snapshot()
	if impersonations != 2 {
		t.Errorf("Impersonate calls = %d, want 2 (one per render)", impersonations)
	}
	if len(queries) != 2 || queries[0].JWT == queries[1].JWT {
		t.Errorf("query JWTs = %q/%q, want two distinct fresh mints", queries[0].JWT, queries[1].JWT)
	}
}

// A render making several queries mints ONCE — the token is scoped to this
// render and lives 60s, so re-minting per query would multiply audited mints
// for no security gain.
func TestRenderMintsOncePerRenderAcrossQueries(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: async () => {
		for (let i = 0; i < 3; i++) { await datuplet.query("SELECT " + i); }
		return { outputDoc: 1, title: "t", blocks: [{ id: "a", type: "markdown", text: "ok" }] };
	}};`)
	api := newFakeRenderAPI(bundle)
	s := newRenderServer(t, api)

	if _, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal()); rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	impersonations, queries, _ := api.snapshot()
	if impersonations != 1 {
		t.Errorf("Impersonate calls = %d, want 1", impersonations)
	}
	if len(queries) != 3 {
		t.Errorf("Query calls = %d, want 3", len(queries))
	}
}

// ---------------------------------------------------------------------------
// 2. Query error caught by the app → the doc still renders
// ---------------------------------------------------------------------------

func TestRenderQueryErrorCaughtByApp(t *testing.T) {
	api := newFakeRenderAPI([]byte(kindEchoBundle))
	api.queryFn = func(context.Context, string, string, []byte) (*http.Response, error) {
		return jsonResponse(http.StatusBadRequest, `{"error":"Binder Error: no such column","kind":"sql_error"}`), nil
	}
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr != nil {
		t.Fatalf("render error: %+v (the app caught the query error, so the render must succeed)", rerr)
	}
	d := decodeDoc(t, doc)
	if got := blockText(t, d, "k"); got != "kind=sql_error" {
		t.Errorf("guest kind = %q, want the query service's wire kind", got)
	}
	if got := blockText(t, d, "m"); !strings.Contains(got, "Binder Error") {
		t.Errorf("guest message = %q, want the query service's message", got)
	}
	_, _, logs := api.snapshot()
	if len(logs) != 1 || logs[0].Outcome != outcomeOK {
		t.Errorf("outcome = %+v, want ok", logs)
	}
}

func TestRenderQueryTransportFailureIsUnavailable(t *testing.T) {
	api := newFakeRenderAPI([]byte(kindEchoBundle))
	api.queryFn = func(context.Context, string, string, []byte) (*http.Response, error) {
		return nil, fmt.Errorf("dial tcp: connection refused")
	}
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	d := decodeDoc(t, doc)
	if got := blockText(t, d, "k"); got != "kind=unavailable" {
		t.Errorf("guest kind = %q, want unavailable", got)
	}
	// The guest must not learn app-worker's internal topology from the error.
	if got := blockText(t, d, "m"); strings.Contains(got, "dial tcp") {
		t.Errorf("guest message leaks transport detail: %q", got)
	}
}

func TestRenderImpersonateFailureIsUnavailable(t *testing.T) {
	api := newFakeRenderAPI([]byte(kindEchoBundle))
	api.impersonateErr = &APIError{StatusCode: http.StatusUnauthorized, Kind: "unauthorized", Message: "bad service credential"}
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	d := decodeDoc(t, doc)
	if got := blockText(t, d, "k"); got != "kind=unavailable" {
		t.Errorf("guest kind = %q, want unavailable (a 401 means OUR credential is wrong)", got)
	}
	_, queries, _ := api.snapshot()
	if len(queries) != 0 {
		t.Errorf("Query calls = %d, want 0 — no credential, no call", len(queries))
	}
}

// ---------------------------------------------------------------------------
// 3. Uncaught guest throw → render_error, stack captured in the log record
// ---------------------------------------------------------------------------

func TestRenderUncaughtGuestErrorCapturesStack(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: () => {
		console.log("before boom");
		throw new Error("kaboom");
	}};`)
	api := newFakeRenderAPI(bundle)
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr == nil {
		t.Fatalf("want a render error, got doc: %s", doc)
	}
	if rerr.Kind != string(errKindRenderError) {
		t.Errorf("kind = %q, want render_error", rerr.Kind)
	}
	if rerr.Stack == "" {
		t.Error("RenderError.Stack is empty — the guest stack was dropped")
	}

	_, _, logs := api.snapshot()
	if len(logs) != 1 {
		t.Fatalf("AppendLog records = %d, want 1", len(logs))
	}
	rec := logs[0]
	if rec.Outcome != string(errKindRenderError) {
		t.Errorf("outcome = %q, want render_error", rec.Outcome)
	}
	if !strings.Contains(rec.Error, "kaboom") {
		t.Errorf("log error = %q, want the guest message", rec.Error)
	}
	if !strings.Contains(rec.Error, "stack:") {
		t.Errorf("log error = %q, want the guest stack captured", rec.Error)
	}
	if !strings.Contains(rec.LogText, "before boom") {
		t.Errorf("log_text = %q, want console.log output up to the throw", rec.LogText)
	}
}

// ---------------------------------------------------------------------------
// 4. Deadline expiry cancels the in-flight query
// ---------------------------------------------------------------------------

func TestRenderDeadlineCancelsInFlightQuery(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: async () => {
		await datuplet.query("SELECT sleep()");
		return { outputDoc: 1, title: "t", blocks: [{ id: "a", type: "markdown", text: "never" }] };
	}};`)
	api := newFakeRenderAPI(bundle)

	var canceled atomic.Bool
	entered := make(chan struct{}, 1)
	api.queryFn = func(ctx context.Context, _, _ string, _ []byte) (*http.Response, error) {
		entered <- struct{}{}
		<-ctx.Done() // the render context is the only thing that can free us
		canceled.Store(ctx.Err() != nil)
		return nil, ctx.Err()
	}

	cfg := Config{Render: renderConfig()}
	// 2s, not 1s: floor(remaining) with a 1s budget is already 0 by the time
	// the guest's query() is serviced, so a 1s render can never ISSUE a query
	// (it takes the <1s local-timeout path instead — see
	// TestRenderSubSecondBudgetMakesNoHTTPCall). 2s is the smallest budget
	// that reaches the wire and can then be cut off mid-flight.
	cfg.Render.TimeoutS = 2
	s := newRenderServerCfg(t, cfg, api)

	start := time.Now()
	_, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	elapsed := time.Since(start)

	select {
	case <-entered:
	default:
		t.Fatal("the fake query was never entered")
	}
	if !canceled.Load() {
		t.Error("the in-flight query's context was not canceled on render-deadline expiry")
	}
	if rerr == nil || rerr.Kind != string(errKindTimeout) {
		t.Fatalf("rerr = %+v, want kind timeout", rerr)
	}
	if elapsed > 8*time.Second {
		t.Errorf("render took %v, want the 2s deadline to bound it", elapsed)
	}
	_, _, logs := api.snapshot()
	if len(logs) != 1 || logs[0].Outcome != string(errKindTimeout) {
		t.Errorf("logged outcome = %+v, want timeout", logs)
	}
}

// ---------------------------------------------------------------------------
// 5. <1s remaining → NO HTTP call at all
// ---------------------------------------------------------------------------

func TestRenderSubSecondBudgetMakesNoHTTPCall(t *testing.T) {
	api := newFakeRenderAPI([]byte(kindEchoBundle))
	s := newRenderServer(t, api,
		// Every clock read jumps 10s while the render budget is 5s, so by the
		// time the guest's query() is serviced the remaining budget is
		// negative — floor(remaining) < 1.
		WithServerClock((&stepClock{now: time.Unix(1753228800, 0), step: 10 * time.Second}).Now))

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr != nil {
		t.Fatalf("render error: %+v (the app caught the timeout, so the render must succeed)", rerr)
	}
	d := decodeDoc(t, doc)
	if got := blockText(t, d, "k"); got != "kind=timeout" {
		t.Errorf("guest kind = %q, want timeout raised locally", got)
	}

	impersonations, queries, _ := api.snapshot()
	if len(queries) != 0 {
		t.Errorf("Query calls = %d, want 0 — the host must not call the service with <1s left", len(queries))
	}
	if impersonations != 0 {
		t.Errorf("Impersonate calls = %d, want 0 — no HTTP call of any kind", impersonations)
	}
}

// ---------------------------------------------------------------------------
// 6. Query budget
// ---------------------------------------------------------------------------

func TestRenderQueryBudgetExhaustedIsBadRequest(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: async () => {
		let kind = "none", n = 0;
		for (let i = 0; i < 11; i++) {
			try { await datuplet.query("SELECT " + i); n++; }
			catch (e) { kind = e.kind || "?"; break; }
		}
		return { outputDoc: 1, title: "t", blocks: [
			{ id: "k", type: "markdown", text: "kind=" + kind },
			{ id: "n", type: "markdown", text: "n=" + n } ] };
	}};`)
	api := newFakeRenderAPI(bundle)
	cfg := Config{Render: renderConfig()}
	cfg.Render.QueriesPerRender = 10
	s := newRenderServerCfg(t, cfg, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	d := decodeDoc(t, doc)
	if got := blockText(t, d, "k"); got != "kind=bad_request" {
		t.Errorf("11th query kind = %q, want bad_request", got)
	}
	if got := blockText(t, d, "n"); got != "n=10" {
		t.Errorf("successful queries = %q, want n=10", got)
	}
	_, queries, _ := api.snapshot()
	if len(queries) != 10 {
		t.Errorf("Query calls = %d, want exactly the 10-query budget", len(queries))
	}
}

// The runner enforces the same bound at the seam the spec assigns it, so the
// limit survives even if appengine's own MaxQueries wiring ever drifts.
func TestQueryRunnerEnforcesBudgetIndependently(t *testing.T) {
	api := newFakeRenderAPI(nil)
	r := &queryRunner{
		api: api, pid: testProjectID, appID: testAppID,
		now:        func() time.Time { return time.Unix(1753228800, 0) },
		deadline:   time.Unix(1753228800, 0).Add(10 * time.Second),
		maxQueries: 1,
	}
	first, err := r.run(context.Background(), []byte(`{"sql":"SELECT 1"}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(string(first), `"result"`) {
		t.Fatalf("first query envelope = %s, want a result", first)
	}
	second, err := r.run(context.Background(), []byte(`{"sql":"SELECT 2"}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if kind := envelopeKind(t, second); kind != string(errKindBadRequest) {
		t.Errorf("over-budget kind = %q, want bad_request", kind)
	}
}

func TestQueryRunnerClampsTimeoutToRemainingBudget(t *testing.T) {
	base := time.Unix(1753228800, 0)
	cases := []struct {
		name      string
		remaining time.Duration
		optsJSON  string
		want      int
	}{
		{"guest asks more than the render budget", 5 * time.Second, `,"opts":{"timeoutS":60}`, 5},
		{"guest asks less than the render budget", 5 * time.Second, `,"opts":{"timeoutS":2}`, 2},
		{"guest asks nothing", 7 * time.Second, ``, 7},
		{"remaining floors down", 3900 * time.Millisecond, `,"opts":{"timeoutS":60}`, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeRenderAPI(nil)
			r := &queryRunner{
				api: api, pid: testProjectID, appID: testAppID,
				now:        func() time.Time { return base },
				deadline:   base.Add(tc.remaining),
				maxQueries: 10,
			}
			if _, err := r.run(context.Background(), []byte(`{"sql":"SELECT 1"`+tc.optsJSON+`}`)); err != nil {
				t.Fatalf("run: %v", err)
			}
			_, queries, _ := api.snapshot()
			if len(queries) != 1 {
				t.Fatalf("Query calls = %d, want 1", len(queries))
			}
			var wire struct {
				TimeoutS *int `json:"timeout_s"`
			}
			if err := json.Unmarshal(queries[0].Body, &wire); err != nil {
				t.Fatal(err)
			}
			if wire.TimeoutS == nil || *wire.TimeoutS != tc.want {
				t.Errorf("timeout_s = %s, want %d", intPtr(wire.TimeoutS), tc.want)
			}
		})
	}
}

func TestQueryRunnerRejectsMalformedGuestRequest(t *testing.T) {
	api := newFakeRenderAPI(nil)
	base := time.Unix(1753228800, 0)
	r := &queryRunner{
		api: api, pid: testProjectID, appID: testAppID,
		now: func() time.Time { return base }, deadline: base.Add(10 * time.Second), maxQueries: 10,
	}
	for _, req := range []string{`not json`, `{"sql":""}`, `{"sql":"   "}`} {
		env, err := r.run(context.Background(), []byte(req))
		if err != nil {
			t.Fatalf("run(%q): %v", req, err)
		}
		if kind := envelopeKind(t, env); kind != string(errKindBadRequest) {
			t.Errorf("run(%q) kind = %q, want bad_request", req, kind)
		}
	}
	_, queries, _ := api.snapshot()
	if len(queries) != 0 {
		t.Errorf("Query calls = %d, want 0", len(queries))
	}
}

// intPtr renders an optional wire int for failure messages ("<unset>" rather
// than a pointer address).
func intPtr(p *int) string {
	if p == nil {
		return "<unset>"
	}
	return strconv.Itoa(*p)
}

func envelopeKind(t *testing.T, env []byte) string {
	t.Helper()
	var e struct {
		Error struct {
			Kind    string `json:"kind"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(env, &e); err != nil {
		t.Fatalf("decode envelope %s: %v", env, err)
	}
	return e.Error.Kind
}

func TestQueryRunnerMapsStatusToKind(t *testing.T) {
	base := time.Unix(1753228800, 0)
	cases := []struct {
		status int
		body   string
		want   string
	}{
		{http.StatusTooManyRequests, `{"error":"slow down","kind":"rate_limited"}`, "rate_limited"},
		{http.StatusServiceUnavailable, `{"error":"full","kind":"capacity"}`, "capacity"},
		{http.StatusBadRequest, `{"error":"bad sql","kind":"sql_error"}`, "sql_error"},
		// No wire kind at all: fall back to a status-derived kind rather than
		// handing the guest an empty string.
		{http.StatusBadGateway, `<html>oops</html>`, "unavailable"},
		{http.StatusUnauthorized, ``, "unauthorized"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			api := newFakeRenderAPI(nil)
			api.queryFn = func(context.Context, string, string, []byte) (*http.Response, error) {
				return jsonResponse(tc.status, tc.body), nil
			}
			r := &queryRunner{
				api: api, pid: testProjectID, appID: testAppID,
				now: func() time.Time { return base }, deadline: base.Add(10 * time.Second), maxQueries: 10,
			}
			env, err := r.run(context.Background(), []byte(`{"sql":"SELECT 1"}`))
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if kind := envelopeKind(t, env); kind != tc.want {
				t.Errorf("kind = %q, want %q", kind, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 7. Oversized doc
// ---------------------------------------------------------------------------

func TestRenderOversizedDocIsRenderError(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: () => ({
		outputDoc: 1, title: "t",
		blocks: [{ id: "a", type: "markdown", text: "x".repeat(4096) }] }) };`)
	api := newFakeRenderAPI(bundle)
	cfg := Config{Render: renderConfig()}
	cfg.Render.OutputDocMaxBytes = 1024
	s := newRenderServerCfg(t, cfg, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr == nil {
		t.Fatalf("want render error, got doc of %d bytes", len(doc))
	}
	if rerr.Kind != string(errKindRenderError) {
		t.Errorf("kind = %q, want render_error", rerr.Kind)
	}
	if !strings.Contains(rerr.Msg, "exceeds") {
		t.Errorf("msg = %q, want it to name the size violation", rerr.Msg)
	}
}

func TestRenderInvalidDocIsRenderError(t *testing.T) {
	// Two blocks sharing an id: schema-legal per block, rejected document-wide
	// (the `block=<id>` partial-render key must be unambiguous).
	bundle := []byte(`var __dtp_app = { render: () => ({
		outputDoc: 1, title: "t", blocks: [
			{ id: "dup", type: "markdown", text: "a" },
			{ id: "dup", type: "markdown", text: "b" }] }) };`)
	api := newFakeRenderAPI(bundle)
	s := newRenderServer(t, api)

	_, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr == nil || rerr.Kind != string(errKindRenderError) {
		t.Fatalf("rerr = %+v, want render_error", rerr)
	}
	if !strings.Contains(rerr.Msg, "dup") {
		t.Errorf("msg = %q, want it to name the offending id", rerr.Msg)
	}
}

// ---------------------------------------------------------------------------
// 8. Chart specs: vegaspec.Validate must run on EVERY chart block
// ---------------------------------------------------------------------------

func TestRenderRejectsChartDataURLByPointer(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: () => ({
		outputDoc: 1, title: "t", blocks: [
			{ id: "c", type: "chart", library: "vega-lite",
			  spec: { mark: "bar", data: { url: "https://evil.example/x.json" } } }] }) };`)
	api := newFakeRenderAPI(bundle)
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr == nil {
		t.Fatalf("data.url chart was ACCEPTED — the Vega subset is a security boundary\ndoc: %s", doc)
	}
	if rerr.Kind != string(errKindRenderError) {
		t.Errorf("kind = %q, want render_error", rerr.Kind)
	}
	if !strings.Contains(rerr.Msg, "/blocks/0/spec") {
		t.Errorf("msg = %q, want the offending JSON pointer", rerr.Msg)
	}
	if !strings.Contains(rerr.Msg, "url") {
		t.Errorf("msg = %q, want the offending key named", rerr.Msg)
	}
}

// A chart nested inside a tab, and one inside a modal, must be validated too —
// otherwise a one-line nesting change smuggles an unvalidated spec through.
func TestRenderValidatesNestedChartSpecs(t *testing.T) {
	cases := map[string]struct {
		bundle  string
		pointer string
	}{
		"inside a tab": {
			bundle: `var __dtp_app = { render: () => ({
				outputDoc: 1, title: "t", blocks: [
					{ id: "tabs", type: "tabs", tabs: [
						{ label: "one", blocks: [
							{ id: "c", type: "chart", library: "vega-lite",
							  spec: { mark: "bar", data: { url: "https://evil.example/x.json" } } }]}]}]}) };`,
			pointer: "/blocks/0/tabs/0/blocks/0/spec",
		},
		"inside a block modal": {
			bundle: `var __dtp_app = { render: () => ({
				outputDoc: 1, title: "t", blocks: [
					{ id: "m", type: "markdown", text: "open", modal: { title: "x", blocks: [
						{ id: "c", type: "chart", library: "vega-lite",
						  spec: { mark: "bar", data: { url: "https://evil.example/x.json" } } }]}}]}) };`,
			pointer: "/blocks/0/modal/blocks/0/spec",
		},
		"inside a table-row modal": {
			bundle: `var __dtp_app = { render: () => ({
				outputDoc: 1, title: "t", blocks: [
					{ id: "tbl", type: "table", columns: ["a"], rows: [
						{ cells: [1], modal: { title: "x", blocks: [
							{ id: "c", type: "chart", library: "vega-lite",
							  spec: { mark: "bar", data: { url: "https://evil.example/x.json" } } }]}}]}]}) };`,
			pointer: "/blocks/0/rows/0/modal/blocks/0/spec",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			api := newFakeRenderAPI([]byte(tc.bundle))
			s := newRenderServer(t, api)
			doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
			if rerr == nil {
				t.Fatalf("nested data.url chart was ACCEPTED\ndoc: %s", doc)
			}
			if !strings.Contains(rerr.Msg, tc.pointer) {
				t.Errorf("msg = %q, want pointer %q", rerr.Msg, tc.pointer)
			}
		})
	}
}

func TestRenderAcceptsValidChartSpec(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: () => ({
		outputDoc: 1, title: "t", blocks: [
			{ id: "c", type: "chart", library: "vega-lite", spec: {
				mark: "line",
				data: { values: [{ x: 1, y: 2 }] },
				encoding: { x: { field: "x", type: "quantitative" },
				            y: { field: "y", type: "quantitative" } } } }] }) };`)
	api := newFakeRenderAPI(bundle)
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr != nil {
		t.Fatalf("valid chart rejected: %+v", rerr)
	}
	if !strings.Contains(string(doc), `"vega-lite"`) {
		t.Errorf("doc lost the chart block: %s", doc)
	}
}

// ---------------------------------------------------------------------------
// refreshInterval CLAMPS (spec §6.3) — it must never fail the whole render
// ---------------------------------------------------------------------------

func TestRenderClampsRefreshInterval(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
	}{
		{"below the floor", "5", refreshIntervalMinS},
		{"zero", "0", refreshIntervalMinS},
		{"negative", "-30", refreshIntervalMinS},
		{"above the ceiling", "99999", refreshIntervalMaxS},
		{"in range is untouched", "60", 60},
		{"at the floor", "15", 15},
		{"at the ceiling", "3600", 3600},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeRenderAPI(refreshBundle(tc.in))
			s := newRenderServer(t, api)
			doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
			if rerr != nil {
				t.Fatalf("refreshInterval %s failed the render (spec says CLAMP): %+v", tc.in, rerr)
			}
			d := decodeDoc(t, doc)
			if d.RefreshInterval == nil {
				t.Fatalf("refreshInterval missing from doc: %s", doc)
			}
			if *d.RefreshInterval != tc.want {
				t.Errorf("refreshInterval = %d, want %d", *d.RefreshInterval, tc.want)
			}
			// The clamped doc must still be a valid OutputDoc.
			if err := outputdoc.Validate(doc); err != nil {
				t.Errorf("clamped doc fails validation: %v\ndoc: %s", err, doc)
			}
		})
	}
}

func TestRenderLeavesOtherFieldsIntactWhenClamping(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: () => ({
		outputDoc: 1, title: "Ünïcode <b>&</b> title", refreshInterval: 1,
		blocks: [{ id: "a", type: "markdown", text: "a<b>&c" }] }) };`)
	api := newFakeRenderAPI(bundle)
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	d := decodeDoc(t, doc)
	if d.Title != "Ünïcode <b>&</b> title" {
		t.Errorf("title mangled by the clamp rewrite: %q", d.Title)
	}
	if got := blockText(t, d, "a"); got != "a<b>&c" {
		t.Errorf("block text mangled by the clamp rewrite: %q", got)
	}
	if d.RefreshInterval == nil || *d.RefreshInterval != refreshIntervalMinS {
		t.Errorf("refreshInterval = %v, want %d", d.RefreshInterval, refreshIntervalMinS)
	}
}

// A non-integer refreshInterval cannot be clamped; it must reach the validator
// rather than being silently coerced.
func TestRenderNonIntegerRefreshIntervalIsRejected(t *testing.T) {
	api := newFakeRenderAPI(refreshBundle(`"soon"`))
	s := newRenderServer(t, api)
	_, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr == nil || rerr.Kind != string(errKindRenderError) {
		t.Fatalf("rerr = %+v, want render_error", rerr)
	}
}

// ---------------------------------------------------------------------------
// 9. + 10. The two admission gates — DIFFERENT policies, both observed
// ---------------------------------------------------------------------------

// Per-app in-flight is the app's OWN concurrency ceiling: acquired
// NON-BLOCKING, so the third concurrent render for the same app is refused
// immediately with `rate_limited` (the caller should back off) rather than
// queueing behind the other two.
func TestPerAppInflightGateReturnsRateLimitedNonBlocking(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: async () => {
		await datuplet.query("SELECT 1");
		return { outputDoc: 1, title: "t", blocks: [{ id: "a", type: "markdown", text: "ok" }] };
	}};`)
	api := newFakeRenderAPI(bundle)
	arrived := make(chan struct{}, 4)
	release := make(chan struct{})
	api.queryFn = blockingQuery(arrived, release)

	cfg := Config{Render: renderConfig()}
	cfg.Render.PerAppInflight = 2
	cfg.Render.Concurrency = 8 // plenty of pool room: the ONLY binding gate is per-app
	// A deliberately LONG pool wait: if the third render were (wrongly) routed
	// through the pool's bounded wait, it could not return this fast.
	s := newRenderServerCfg(t, cfg, api, WithPoolAcquireWait(2*time.Second))

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
		}()
	}
	// Wait until both holders are genuinely inside their query (a real
	// concurrent hold cannot be faked with a clock).
	for i := 0; i < 2; i++ {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			close(release)
			wg.Wait()
			t.Fatal("timed out waiting for two in-flight renders")
		}
	}

	start := time.Now()
	_, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	elapsed := time.Since(start)

	close(release)
	wg.Wait()

	if rerr == nil || rerr.Kind != string(errKindRateLimited) {
		t.Fatalf("third concurrent render for the same app: rerr = %+v, want rate_limited", rerr)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("per-app gate took %v — it must be NON-BLOCKING (TryAcquire), not a bounded wait", elapsed)
	}
	// A refused render must not be recorded in the author's ring buffer: the
	// render never executed and the buffer is the author's debugging window.
	_, _, logs := api.snapshot()
	for _, rec := range logs {
		if rec.Outcome == string(errKindRateLimited) {
			t.Errorf("admission refusal was written to the render log: %+v", rec)
		}
	}
}

// The pool semaphore is the whole-pod ceiling: acquired with a SHORT BOUNDED
// WAIT, and a render that cannot get in within that window is shed as
// `capacity` (the pod is saturated; another replica should take it) — never an
// unbounded queue.
func TestPoolGateReturnsCapacityAfterBoundedWait(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: async () => {
		await datuplet.query("SELECT 1");
		return { outputDoc: 1, title: "t", blocks: [{ id: "a", type: "markdown", text: "ok" }] };
	}};`)
	api := newFakeRenderAPI(bundle)
	arrived := make(chan struct{}, 4)
	release := make(chan struct{})
	api.queryFn = blockingQuery(arrived, release)

	const wait = 50 * time.Millisecond
	cfg := Config{Render: renderConfig()}
	cfg.Render.Concurrency = 1    // whole-pod ceiling of one render
	cfg.Render.PerAppInflight = 2 // per-app gate must NOT be what bites here
	s := newRenderServerCfg(t, cfg, api, WithPoolAcquireWait(wait))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.render(context.Background(), testResolved("app-A"), "/", nil, testPrincipal())
	}()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		close(release)
		<-done
		t.Fatal("timed out waiting for the first render to occupy the pool slot")
	}

	// A DIFFERENT app: its own per-app budget is untouched, so the only gate
	// left is the pool.
	start := time.Now()
	_, rerr := s.render(context.Background(), testResolved("app-B"), "/", nil, testPrincipal())
	elapsed := time.Since(start)

	close(release)
	<-done

	if rerr == nil || rerr.Kind != string(errKindCapacity) {
		t.Fatalf("second render on a full pod: rerr = %+v, want capacity", rerr)
	}
	if elapsed < wait {
		t.Errorf("pool gate returned after %v, want at least the %v bounded wait", elapsed, wait)
	}
	if elapsed > 10*wait+time.Second {
		t.Errorf("pool gate waited %v — the wait must be BOUNDED, not an unbounded queue", elapsed)
	}
}

// Once a render finishes, both gates must release: the same app renders again.
func TestGatesReleaseAfterRender(t *testing.T) {
	api := newFakeRenderAPI([]byte(docOnlyBundle))
	cfg := Config{Render: renderConfig()}
	cfg.Render.PerAppInflight = 1
	cfg.Render.Concurrency = 1
	s := newRenderServerCfg(t, cfg, api, WithPoolAcquireWait(20*time.Millisecond))

	for i := 0; i < 3; i++ {
		if _, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal()); rerr != nil {
			t.Fatalf("sequential render %d refused: %+v", i, rerr)
		}
	}
}

func TestPerAppGateIsKeyedPerApp(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: async () => {
		await datuplet.query("SELECT 1");
		return { outputDoc: 1, title: "t", blocks: [{ id: "a", type: "markdown", text: "ok" }] };
	}};`)
	api := newFakeRenderAPI(bundle)
	arrived := make(chan struct{}, 4)
	release := make(chan struct{})
	api.queryFn = blockingQuery(arrived, release)

	cfg := Config{Render: renderConfig()}
	cfg.Render.PerAppInflight = 1
	cfg.Render.Concurrency = 8
	s := newRenderServerCfg(t, cfg, api, WithPoolAcquireWait(time.Second))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.render(context.Background(), testResolved("app-A"), "/", nil, testPrincipal())
	}()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		close(release)
		<-done
		t.Fatal("timed out waiting for app-A's render")
	}

	// app-A is at its ceiling; app-B must be unaffected.
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(release)
	}()
	if _, rerr := s.render(context.Background(), testResolved("app-B"), "/", nil, testPrincipal()); rerr != nil {
		t.Errorf("app-B refused while only app-A was at its ceiling: %+v", rerr)
	}
	<-done
}

// ---------------------------------------------------------------------------
// Bundle fetch + configuration failures
// ---------------------------------------------------------------------------

func TestRenderBundleFetchFailureIsUnavailable(t *testing.T) {
	api := newFakeRenderAPI([]byte(docOnlyBundle))
	api.bundleErr = fmt.Errorf("boom")
	s := newRenderServer(t, api)

	_, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr == nil || rerr.Kind != string(errKindUnavailable) {
		t.Fatalf("rerr = %+v, want unavailable (spec §8: bundle fetch failure)", rerr)
	}
	_, _, logs := api.snapshot()
	if len(logs) != 1 || logs[0].Outcome != string(errKindUnavailable) {
		t.Errorf("logged outcome = %+v, want unavailable", logs)
	}
}

func TestRenderWithoutAPIIsUnavailable(t *testing.T) {
	s := NewServer(Config{Render: renderConfig()}, testEngine(t))
	_, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr == nil || rerr.Kind != string(errKindUnavailable) {
		t.Fatalf("rerr = %+v, want unavailable when the render path is unwired", rerr)
	}
}

// AppendLog is async/drop-on-full by design: a dropped audit breadcrumb must
// never fail (or delay) a render.
func TestRenderSucceedsWhenAppendLogFails(t *testing.T) {
	api := newFakeRenderAPI([]byte(docOnlyBundle))
	api.logErr = ErrLogQueueFull
	s := newRenderServer(t, api)

	doc, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal())
	if rerr != nil {
		t.Fatalf("a dropped log record failed the render: %+v", rerr)
	}
	if len(doc) == 0 {
		t.Error("empty doc")
	}
}

// The 64 KiB per-render log cap (spec §6.6) is applied before the record is
// handed to the client (which does not pre-truncate — task-W3-report.md).
func TestRenderCapsAuthorLog(t *testing.T) {
	bundle := []byte(`var __dtp_app = { render: () => {
		for (let i = 0; i < 400; i++) { console.log("y".repeat(1000)); }
		return { outputDoc: 1, title: "t", blocks: [{ id: "a", type: "markdown", text: "hi" }] };
	}};`)
	api := newFakeRenderAPI(bundle)
	s := newRenderServer(t, api)

	if _, rerr := s.render(context.Background(), testResolved(testAppID), "/", nil, testPrincipal()); rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	_, _, logs := api.snapshot()
	if len(logs) != 1 {
		t.Fatalf("AppendLog records = %d", len(logs))
	}
	if n := len(logs[0].LogText); n > renderMaxLogBytes {
		t.Errorf("log_text = %d bytes, want <= %d", n, renderMaxLogBytes)
	}
	if len(logs[0].LogText) == 0 {
		t.Error("log_text is empty — the cap must truncate, not discard")
	}
}
