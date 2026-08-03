// render.go is app-worker's render pipeline (RFC 028 spec §4.2 steps 3-5,
// §6.2, §6.3, §7): the place where the WASM engine, the per-render query
// budget, the deadline coupling, the two output validators and the two
// admission gates come together.
//
// Three things in here are easy to get wrong and are therefore called out at
// their definitions rather than only here:
//
//  1. The two admission gates have DELIBERATELY DIFFERENT acquisition
//     policies — per-app in-flight is non-blocking (`rate_limited`), the pool
//     semaphore is a short bounded wait (`capacity`). See render's gate
//     section.
//  2. `outputdoc.Validate` does NOT validate chart specs — the OutputDoc
//     schema types `chartBlock.spec` as a bare object. `validateChartSpecs`
//     runs `vegaspec.Validate` on EVERY chart block, at every nesting depth.
//     Skipping it puts unvalidated Vega-Lite in front of the viewer, which is
//     a security hole (spec §6.4/§9), not a cosmetic gap.
//  3. `refreshInterval` CLAMPS, it does not reject (spec §6.3). The clamp runs
//     BEFORE validation so an author's `refreshInterval: 5` degrades instead
//     of failing the whole render.
package appworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/appengine"
	"github.com/datuplet/datuplet/pkg/appengine/outputdoc"
	"github.com/datuplet/datuplet/pkg/appengine/vegaspec"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	// defaultPoolAcquireWait is how long a render waits for a whole-pod render
	// slot before being shed as `capacity`. Short and BOUNDED on purpose: a
	// saturated pod must shed load so the HPA / another replica takes it,
	// never grow an unbounded queue whose members all time out anyway. Mirrors
	// query-worker's own `AdmissionWait` shape (RFC 025 §5.2).
	defaultPoolAcquireWait = 250 * time.Millisecond

	// renderMaxLogBytes is spec §6.6's per-render author-log cap (64 KiB). It
	// is passed to the engine (which truncates the guest's captured output)
	// AND applied to the log record, because the record also carries the
	// engine-side error text.
	renderMaxLogBytes = 64 << 10

	// refreshIntervalMinS / refreshIntervalMaxS are spec §7's clamp bounds for
	// `refreshInterval`. CLAMP, not reject — see clampRefreshInterval.
	refreshIntervalMinS = 15
	refreshIntervalMaxS = 3600

	// queryResponseMaxBytes bounds what the host will buffer from one query
	// response. Spec §7's memory model budgets "one buffered query response
	// (≤max_bytes, 10 MiB cap)" per render slot; the slack above 10 MiB covers
	// the JSON envelope around the rows. Without a bound here a misbehaving
	// upstream could blow the per-render memory budget the pool semaphore is
	// sized against.
	queryResponseMaxBytes = 12 << 20

	// outcomeSuccess is the render log's `outcome` for a successful render.
	// Every other outcome is the §8 error kind verbatim.
	outcomeSuccess = "success"
)

// ---------------------------------------------------------------------------
// renderAPI
// ---------------------------------------------------------------------------

// renderAPI is the subset of *APIClient (W3) the render path depends on.
//
// It is deliberately SEPARATE from authAPI rather than a single union
// interface: W4's auth tests inject doubles that implement only the three
// auth methods, and a union would force every one of them to grow four
// irrelevant stubs. Production wiring (W6) passes the same *APIClient to
// WithAuthAPI and WithRenderAPI; *APIClient satisfies both structurally.
type renderAPI interface {
	Bundle(ctx context.Context, hash string) ([]byte, error)
	Impersonate(ctx context.Context, appID string) (string, error)
	Query(ctx context.Context, pid string, jwt string, body []byte) (*http.Response, error)
	AppendLog(ctx context.Context, rec RenderLogRecord) error
}

// ---------------------------------------------------------------------------
// Request id
// ---------------------------------------------------------------------------

type requestIDKey struct{}

// withRequestID stamps the §8 request id on ctx. W6's HTTP middleware calls
// this once per request so the id in the error envelope, the render log
// record, and app-worker's own structured log are the SAME value — that
// identity is what makes `datuplet apps logs --request-id` (§5.5) work.
func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// requestIDFromContext returns the request id stamped by withRequestID, or ""
// when there is none.
func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// ---------------------------------------------------------------------------
// Gate 1: per-app in-flight (NON-BLOCKING)
// ---------------------------------------------------------------------------

// inflightGate counts concurrent renders per app_id, admitting only up to max.
// Acquisition is strictly non-blocking (tryAcquire): this is the app's OWN
// concurrency ceiling (spec §7, default 2, aligned with the query service's
// per-principal in-flight cap), so exceeding it is a signal to the CALLER to
// back off — `rate_limited` — not a reason to hold a connection open.
//
// Shared mutable state across concurrent renders; every access is
// mutex-guarded and the map is pruned back to empty so a long-lived pod's key
// space cannot grow without bound.
type inflightGate struct {
	mu       sync.Mutex
	max      int
	inflight map[string]int
}

func newInflightGate(max int) *inflightGate {
	if max <= 0 {
		max = DefaultPerAppInflight
	}
	return &inflightGate{max: max, inflight: make(map[string]int)}
}

// tryAcquire takes one slot for key, or reports ok=false immediately. The
// returned release func is idempotent-safe to call exactly once (deferred by
// the caller).
func (g *inflightGate) tryAcquire(key string) (release func(), ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inflight[key] >= g.max {
		return nil, false
	}
	g.inflight[key]++
	return func() { g.release(key) }, true
}

func (g *inflightGate) release(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if n := g.inflight[key] - 1; n > 0 {
		g.inflight[key] = n
	} else {
		delete(g.inflight, key)
	}
}

// ---------------------------------------------------------------------------
// render
// ---------------------------------------------------------------------------

// render executes one app render end to end and returns the validated
// OutputDoc.
//
// Sequence (the order is load-bearing):
//
//  1. per-app in-flight gate — NON-BLOCKING → `rate_limited`
//  2. pool semaphore — SHORT BOUNDED WAIT → `capacity`
//  3. bundle fetch (hash-verified inside the client) → `unavailable` on failure
//  4. engine render under the wall clock, with a QueryFunc that mints the
//     impersonation JWT lazily-once, enforces the query budget, and clamps
//     each query's timeout to the remaining render budget
//  5. refreshInterval clamp → output-size cap → outputdoc.Validate →
//     vegaspec.Validate per chart block
//  6. AppendLog (async, drop-on-full, never fails a render)
//
// The gates come FIRST and in this order: the per-app check is a map lookup,
// so refusing a burst from one app costs nothing and never consumes a pod-wide
// slot; only a render that has already cleared its app's own ceiling competes
// for pod capacity.
//
// rerr's Kind carries the §8 envelope vocabulary — which is WIDER than
// appengine.RenderError's own three kinds (render_error|timeout|bad_request),
// since the gates and the control-plane failures produce
// rate_limited|capacity|unavailable. The type is reused (per this task's
// interface contract) rather than duplicated; W6 maps Kind → HTTP status.
func (s *Server) render(
	ctx context.Context,
	resolved resolvedApp,
	path string,
	params map[string]string,
	p principal,
) (doc json.RawMessage, rerr *appengine.RenderError) {
	if s.engine == nil || s.rapi == nil {
		// A pod whose render path is not wired must never pretend to render.
		return nil, renderErr(errKindUnavailable, "app-worker: render path is not configured")
	}

	// --- Gate 1: per-app in-flight. NON-BLOCKING. -------------------------
	//
	// TryAcquire semantics, by design (contract-and-constraints.md / the
	// round-6 finding this ordering exists to preserve): the app is already
	// running its full allowance, so the answer is "back off", delivered
	// immediately. Blocking here would conflate the app's own ceiling with
	// pod saturation and hand the caller a `capacity` story that is not true.
	releaseApp, ok := s.perAppInflight.tryAcquire(resolved.AppID)
	if !ok {
		return nil, renderErr(errKindRateLimited,
			fmt.Sprintf("app already has %d renders in flight", s.perAppInflight.max))
	}
	defer releaseApp()

	// --- Gate 2: pool semaphore. SHORT BOUNDED WAIT. ----------------------
	//
	// A bounded wait (never unbounded) smooths the transient collision where
	// two requests land on the same pod microseconds apart, while still
	// shedding load when the pod is genuinely full — same 429-vs-503
	// distinction the query service draws (spec §7).
	acquireCtx, cancelAcquire := context.WithTimeout(ctx, s.poolAcquireWait)
	acquireErr := s.pool.Acquire(acquireCtx, 1)
	cancelAcquire()
	if acquireErr != nil {
		if ctx.Err() != nil {
			// The CALLER went away / its own deadline expired — not our
			// capacity story to tell.
			return nil, renderErr(errKindTimeout, "render deadline expired before admission")
		}
		return nil, renderErr(errKindCapacity, "app-worker is at render capacity")
	}
	defer s.pool.Release(1)

	// Everything from here on is a real render attempt, so it gets a render
	// log record (spec §9 audit layer 2). Admission refusals deliberately do
	// NOT: nothing executed, and the per-app ring buffer (200 records) is the
	// author's debugging window — filling it with 429s would evict the render
	// failures the author actually needs. W6's Prometheus counters +
	// structured log carry refusal volume instead.
	started := s.now()
	var authorLog []byte
	defer func() { s.appendRenderLog(ctx, resolved, p, started, authorLog, rerr) }()

	bundle, err := s.rapi.Bundle(ctx, resolved.VersionHash)
	if err != nil {
		// Spec §8: bundle fetch/resolve failure → `unavailable`. A 401 here
		// means app-worker's OWN service credential is wrong
		// (task-P3-report.md) — still `unavailable` toward the viewer, never a
		// per-viewer denial.
		return nil, renderErr(errKindUnavailable, "app-worker: cannot load app bundle")
	}

	wall := s.renderWallClock()
	runner := &queryRunner{
		api:        s.rapi,
		pid:        resolved.ProjectID,
		appID:      resolved.AppID,
		now:        s.now,
		deadline:   started.Add(wall),
		maxQueries: s.renderMaxQueries(),
	}

	res, engErr := s.engine.Render(ctx, appengine.RenderInput{
		Bundle: bundle,
		Path:   path,
		Params: params,
		Now:    started,
		Query:  runner.run,
		Limits: appengine.Limits{
			WallClock:      wall,
			MaxQueries:     runner.maxQueries,
			MaxOutputBytes: s.renderMaxOutputBytes(),
			MaxLogBytes:    renderMaxLogBytes,
		},
	})
	if engErr != nil {
		authorLog = engErr.Log
		return nil, engErr
	}
	authorLog = res.Log

	// refreshInterval CLAMPS (spec §6.3/§7), and must do so BEFORE validation:
	// W1's schema states the [15,3600] bounds as a hard range, so an
	// unclamped `refreshInterval: 5` would fail the whole render instead of
	// degrading to the floor. Clamping here is the runtime behaviour the spec
	// asks for; W1's schema is left as the authoritative statement of the
	// legal range.
	doc, err = clampRefreshInterval(res.Doc)
	if err != nil {
		return nil, renderErr(errKindRenderError, "invalid output document: "+err.Error())
	}

	// Output-size cap, re-checked AFTER the clamp rewrite (which can add
	// digits, e.g. 5 → 15). The engine applies the same limit to the raw doc;
	// this is the post-rewrite guard, not a duplicate.
	if max := s.renderMaxOutputBytes(); max > 0 && len(doc) > max {
		return nil, renderErr(errKindRenderError,
			fmt.Sprintf("output document is %d bytes, exceeds limit %d", len(doc), max))
	}

	if err := outputdoc.Validate(doc); err != nil {
		return nil, renderErr(errKindRenderError, "invalid output document: "+err.Error())
	}
	// MANDATORY second validator: outputdoc's schema types a chart block's
	// `spec` as a bare object, so nothing above has looked inside it.
	if err := validateChartSpecs(doc); err != nil {
		return nil, renderErr(errKindRenderError, err.Error())
	}

	return doc, nil
}

// renderErr builds a RenderError carrying a §8 envelope kind.
func renderErr(kind errorKind, msg string) *appengine.RenderError {
	return &appengine.RenderError{Kind: string(kind), Msg: msg}
}

// renderWallClock is the per-render wall-clock budget (spec §7: 10 s default,
// 30 s cap — LoadConfig already clamped it).
func (s *Server) renderWallClock() time.Duration {
	n := s.cfg.Render.TimeoutS
	if n <= 0 {
		n = DefaultTimeoutS
	}
	return time.Duration(n) * time.Second
}

func (s *Server) renderMaxQueries() int {
	if n := s.cfg.Render.QueriesPerRender; n > 0 {
		return n
	}
	return DefaultQueriesPerRender
}

func (s *Server) renderMaxOutputBytes() int {
	if n := s.cfg.Render.OutputDocMaxBytes; n > 0 {
		return n
	}
	return DefaultOutputDocMaxBytes
}

// appendRenderLog writes one render-access-log record (spec §6.6/§9). It can
// never fail or delay a render: AppendLog is async and drops on a full queue
// by design (task-W3-report.md), and a lost audit breadcrumb is not a render
// defect.
func (s *Server) appendRenderLog(
	ctx context.Context,
	resolved resolvedApp,
	p principal,
	started time.Time,
	authorLog []byte,
	rerr *appengine.RenderError,
) {
	// request_id is the render-log table's primary key (P2's migration), so a
	// record must always carry one. W6's middleware stamps the id it also puts
	// in the §8 envelope; a render invoked without one (a test, or a future
	// non-HTTP caller) still gets an id rather than an unwritable record.
	requestID := requestIDFromContext(ctx)
	if requestID == "" {
		requestID = uuid.NewString()
	}
	rec := RenderLogRecord{
		RequestID:     requestID,
		AppID:         resolved.AppID,
		VersionHash:   resolved.VersionHash,
		Channel:       resolved.Channel,
		PrincipalKind: p.Kind,
		PrincipalID:   p.ID,
		StartedAt:     started,
		DurationMS:    s.now().Sub(started).Milliseconds(),
		Outcome:       outcomeSuccess,
		LogText:       truncate(string(authorLog), renderMaxLogBytes),
	}
	if rerr != nil {
		rec.Outcome = rerr.Kind
		rec.Error = truncate(renderErrorText(rerr), renderMaxLogBytes)
	}
	// The context is passed for symmetry only — AppendLog enqueues and returns.
	_ = s.rapi.AppendLog(ctx, rec)
}

// renderErrorText is the author-facing rendition of a failed render: the
// message plus the guest stack when there is one. The viewer only ever sees
// the generic §8 envelope (spec §8: "full error + stack to the author log,
// generic failure to the viewer").
func renderErrorText(rerr *appengine.RenderError) string {
	if rerr.Stack == "" {
		return rerr.Msg
	}
	return rerr.Msg + "\n\nstack:\n" + rerr.Stack
}

func truncate(s string, max int) string {
	if max > 0 && len(s) > max {
		return s[:max]
	}
	return s
}

// ---------------------------------------------------------------------------
// refreshInterval clamp
// ---------------------------------------------------------------------------

// clampRefreshInterval rewrites an out-of-range integer `refreshInterval` into
// spec §7's [15, 3600] window, leaving the rest of the document byte-identical.
//
// Spec §6.3 says "clamped", not "rejected": an author who writes
// `refreshInterval: 5` should get a 15 s refresh, not a failed dashboard. The
// rewrite happens before validation precisely so the clamped value is what the
// schema sees.
//
// A refreshInterval that is not an integer (a string, a float, null) is left
// alone: it cannot be meaningfully clamped, and silently coercing it would
// hide an authoring bug. outputdoc.Validate rejects it with a precise message.
// The re-encode uses SetEscapeHTML(false) so untouched values keep their
// original bytes.
func clampRefreshInterval(doc json.RawMessage) (json.RawMessage, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(doc, &top); err != nil || top == nil {
		// Not a JSON object: nothing to clamp. outputdoc.Validate produces the
		// precise complaint.
		return doc, nil
	}
	raw, ok := top["refreshInterval"]
	if !ok {
		return doc, nil
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return doc, nil
	}
	got, err := n.Int64()
	if err != nil {
		return doc, nil
	}
	want := got
	if want < refreshIntervalMinS {
		want = refreshIntervalMinS
	}
	if want > refreshIntervalMaxS {
		want = refreshIntervalMaxS
	}
	if want == got {
		return doc, nil
	}

	top["refreshInterval"] = json.RawMessage(strconv.FormatInt(want, 10))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(top); err != nil {
		return nil, err
	}
	return json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

// ---------------------------------------------------------------------------
// Chart-spec validation — the hand-off nothing else wires up
// ---------------------------------------------------------------------------

// validateChartSpecs runs vegaspec.Validate on every chart block's `spec`, at
// every nesting depth the block vocabulary allows (root, inside a `tabs`
// block's tabs, inside a block-level modal, inside a table-row modal).
//
// This is NOT redundant with outputdoc.Validate: that schema types
// `chartBlock.spec` as `{"type": "object"}` and looks no further. The
// restricted Vega-Lite subset is what prevents `data.url` exfiltration and
// remote image loads from the trusted shell (spec §6.4), so an unvalidated
// spec reaching the browser is a security failure. Reported errors name the
// document JSON pointer of the offending spec so an author can find it.
//
// Runs AFTER outputdoc.Validate, so the structure walked here is already
// schema-valid; the walk is still defensive about shapes (it skips anything
// unexpected rather than panicking).
func validateChartSpecs(doc json.RawMessage) error {
	var data any
	if err := json.Unmarshal(doc, &data); err != nil {
		return nil // already rejected by outputdoc.Validate
	}
	m, ok := data.(map[string]any)
	if !ok {
		return nil
	}
	blocks, ok := m["blocks"].([]any)
	if !ok {
		return nil
	}
	return walkChartBlocks(blocks, "")
}

func walkChartBlocks(blocks []any, path string) error {
	for i, b := range blocks {
		block, ok := b.(map[string]any)
		if !ok {
			continue
		}
		blockPath := fmt.Sprintf("%s/blocks/%d", path, i)
		blockType, _ := block["type"].(string)

		if blockType == "chart" {
			raw, err := json.Marshal(block["spec"])
			if err != nil {
				return fmt.Errorf("chart spec at %s/spec: %w", blockPath, err)
			}
			if err := vegaspec.Validate(raw); err != nil {
				return fmt.Errorf("chart spec at %s/spec: %w", blockPath, err)
			}
		}

		if err := walkChartModal(block["modal"], blockPath+"/modal"); err != nil {
			return err
		}

		switch blockType {
		case "tabs":
			tabs, ok := block["tabs"].([]any)
			if !ok {
				continue
			}
			for j, t := range tabs {
				tab, ok := t.(map[string]any)
				if !ok {
					continue
				}
				nested, ok := tab["blocks"].([]any)
				if !ok {
					continue
				}
				if err := walkChartBlocks(nested, fmt.Sprintf("%s/tabs/%d", blockPath, j)); err != nil {
					return err
				}
			}
		case "table":
			rows, ok := block["rows"].([]any)
			if !ok {
				continue
			}
			for j, r := range rows {
				row, ok := r.(map[string]any)
				if !ok {
					continue
				}
				if err := walkChartModal(row["modal"], fmt.Sprintf("%s/rows/%d/modal", blockPath, j)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func walkChartModal(v any, path string) error {
	modal, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	blocks, ok := modal["blocks"].([]any)
	if !ok {
		return nil
	}
	return walkChartBlocks(blocks, path)
}

// ---------------------------------------------------------------------------
// queryRunner — the guest's only host capability
// ---------------------------------------------------------------------------

// guestQuery is the request the prelude hands the host:
// `{sql, params, opts}` (pkg/appengine/prelude.js). `opts` field names are the
// guest-facing camelCase spelling of spec §6.2's 1:1 mapping onto the query
// service's snake_case fields.
type guestQuery struct {
	SQL    string         `json:"sql"`
	Params map[string]any `json:"params"`
	Opts   *guestOpts     `json:"opts"`
}

type guestOpts struct {
	MaxRows  *int `json:"maxRows"`
	MaxBytes *int `json:"maxBytes"`
	TimeoutS *int `json:"timeoutS"`
}

// wireQuery is pipeline-api's app-query body
// (pkg/pipelineapi/queryproxy.queryRequest). timeout_s is always set: the
// proxy's own default (60 s) is far outside a render's budget, and "a query can
// never outlive its render" (spec §7) is only true if the host states the
// bound explicitly on every call.
type wireQuery struct {
	SQL      string         `json:"sql"`
	TimeoutS *int           `json:"timeout_s,omitempty"`
	MaxRows  *int           `json:"max_rows,omitempty"`
	MaxBytes *int           `json:"max_bytes,omitempty"`
	Params   map[string]any `json:"params,omitempty"`
}

// queryRunner services `datuplet.query()` for one render. Not shared across
// renders: it owns that render's query budget and its single impersonation
// JWT.
//
// Guest queries are sequential (QuickJS is single-threaded and appengine
// serializes host calls), so mu guards against nothing in practice — it is
// kept because the budget and the lazily-minted token are the two pieces of
// per-render mutable state a future parallel query API would race on, and
// -race must stay clean either way.
type queryRunner struct {
	api        renderAPI
	pid        string
	appID      string
	now        func() time.Time
	deadline   time.Time
	maxQueries int

	mu     sync.Mutex
	count  int
	jwt    string
	minted bool
}

// run is the appengine.QueryFunc. It NEVER returns a non-nil error: every
// failure is mapped onto the guest error envelope `{error:{kind,message}}`
// here, so the guest always sees a §8 kind it can switch on. Returning a Go
// error instead would make appengine substitute its own generic
// "query_error" kind and lose the mapping.
func (r *queryRunner) run(ctx context.Context, reqJSON []byte) ([]byte, error) {
	return r.execute(ctx, reqJSON), nil
}

func (r *queryRunner) execute(ctx context.Context, reqJSON []byte) []byte {
	var gq guestQuery
	dec := json.NewDecoder(bytes.NewReader(reqJSON))
	// UseNumber keeps the integral / non-integral distinction in bound params
	// intact all the way to query-worker's ValidateParams (Part 1), which
	// rejects integral numbers above 2^53−1.
	dec.UseNumber()
	if err := dec.Decode(&gq); err != nil {
		return guestErr(errKindBadRequest, "malformed query request")
	}
	if strings.TrimSpace(gq.SQL) == "" {
		return guestErr(errKindBadRequest, "query: sql is required")
	}

	// Per-render query budget (spec §7: 10/render default, 25 cap).
	// appengine enforces the same MaxQueries one layer up and therefore
	// normally answers first; this check keeps the bound standing at the seam
	// the spec assigns it to, so the two can never drift into a hole.
	if !r.take() {
		return guestErr(errKindBadRequest,
			fmt.Sprintf("query budget exhausted: at most %d queries per render", r.maxQueries))
	}

	// Deadline coupling (spec §6.2/§7). floor() because the wire field is
	// whole seconds: rounding up would let a query outlive its render.
	remaining := r.remainingSeconds()
	if remaining < 1 {
		// Local `timeout`, with NO HTTP CALL AT ALL — not even the
		// impersonation mint below. Issuing a request that is certain to be
		// canceled costs the query service a DuckDB admission slot for nothing.
		return guestErr(errKindTimeout, "render deadline exhausted: query not issued")
	}
	timeoutS := remaining
	if gq.Opts != nil && gq.Opts.TimeoutS != nil && *gq.Opts.TimeoutS > 0 && *gq.Opts.TimeoutS < timeoutS {
		timeoutS = *gq.Opts.TimeoutS
	}

	body := wireQuery{SQL: gq.SQL, Params: gq.Params, TimeoutS: &timeoutS}
	if gq.Opts != nil {
		body.MaxRows = gq.Opts.MaxRows
		body.MaxBytes = gq.Opts.MaxBytes
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return guestErr(errKindBadRequest, "query: unencodable request")
	}

	jwt, err := r.token(ctx)
	if err != nil {
		// pipeline-api unavailable, or app-worker's own service credential is
		// wrong (a 401 from Impersonate means the latter — task-P3-report.md).
		// Either way it fails CLOSED as `unavailable` (spec §8), and the guest
		// learns nothing about which.
		return guestErr(errKindUnavailable, "query service credential unavailable")
	}

	// ctx is the ENGINE's render context (appengine wraps the caller's ctx
	// with the wall clock and hands it to the QueryFunc), so render-deadline
	// expiry cancels this HTTP request in flight — spec §7's "the render
	// context cancels in-flight queries on expiry". It is deliberately not
	// re-wrapped here: the injected clock used for the timeout arithmetic
	// above is a test seam, and deriving a cancellation deadline from it would
	// make a fake clock able to break (or fake) real cancellation.
	resp, err := r.api.Query(ctx, r.pid, jwt, raw)
	if err != nil {
		if ctx.Err() != nil {
			return guestErr(errKindTimeout, "query canceled: render deadline expired")
		}
		// Fixed message: a transport error carries app-worker's internal
		// topology (the internal API's URL), which untrusted guest code has no
		// business learning. The detail belongs in app-worker's own structured
		// log (W6).
		return guestErr(errKindUnavailable, "query service unreachable")
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, queryResponseMaxBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return guestErr(errKindTimeout, "query canceled: render deadline expired")
		}
		return guestErr(errKindUnavailable, "query response truncated")
	}
	if len(payload) > queryResponseMaxBytes {
		return guestErr(errKindBadRequest,
			fmt.Sprintf("query response exceeds the %d-byte per-render buffer", queryResponseMaxBytes))
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return guestErr(parseQueryError(resp.StatusCode, payload))
	}
	return resultEnvelope(payload)
}

// take consumes one unit of the render's query budget.
func (r *queryRunner) take() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.maxQueries > 0 && r.count >= r.maxQueries {
		return false
	}
	r.count++
	return true
}

// remainingSeconds is floor(seconds left in the render budget). Negative when
// the budget is already blown.
func (r *queryRunner) remainingSeconds() int {
	return int(math.Floor(r.deadline.Sub(r.now()).Seconds()))
}

// token returns this render's impersonation JWT, minting it on FIRST USE.
//
// One fresh mint per render (spec §5.4) — never cached across renders (there
// is no such cache: APIClient.Impersonate deliberately has none, because P4
// made the `jti` cryptographically random so concurrent renders are
// individually attributable in query_audit). Minting lazily rather than
// eagerly means a render that never queries mints nothing at all: every mint
// is audited server-side, so an unused one is noise in the audit trail plus a
// control-plane round trip spent inside the render deadline.
//
// A failed mint is NOT memoized, so a later query in the same render retries.
func (r *queryRunner) token(ctx context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.minted {
		return r.jwt, nil
	}
	tok, err := r.api.Impersonate(ctx, r.appID)
	if err != nil {
		return "", err
	}
	r.jwt, r.minted = tok, true
	return tok, nil
}

// parseQueryError maps the query service's error envelope onto the guest's
// `{kind, message}` (spec §6.2). The wire kind is forwarded VERBATIM when
// present — `sql_error`, `timeout`, `rate_limited`, `capacity`, … are exactly
// what an app is documented to switch on. A response with no usable envelope
// (an HTML 502 from an intermediary, an empty body) falls back to a
// status-derived kind rather than handing the guest an empty string.
func parseQueryError(status int, payload []byte) (kind errorKind, msg string) {
	var body struct {
		Error string `json:"error"`
		Kind  string `json:"kind"`
	}
	_ = json.Unmarshal(payload, &body)
	if body.Kind != "" {
		if body.Error == "" {
			body.Error = fmt.Sprintf("query failed with status %d", status)
		}
		return errorKind(body.Kind), body.Error
	}
	switch status {
	case http.StatusBadRequest:
		kind = errKindBadRequest
	case http.StatusUnauthorized, http.StatusForbidden:
		kind = errKindUnauthorized
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		kind = errKindTimeout
	case http.StatusTooManyRequests:
		kind = errKindRateLimited
	default:
		kind = errKindUnavailable
	}
	if body.Error != "" {
		return kind, body.Error
	}
	return kind, fmt.Sprintf("query failed with status %d", status)
}

// guestErr builds the guest-visible error envelope the prelude turns into a
// rejected `datuplet.query` promise carrying `e.kind`.
func guestErr(kind errorKind, msg string) []byte {
	env := struct {
		Error struct {
			Message string `json:"message"`
			Kind    string `json:"kind"`
		} `json:"error"`
	}{}
	env.Error.Message = msg
	env.Error.Kind = string(kind)
	b, err := json.Marshal(env)
	if err != nil { // unreachable: two string fields
		return []byte(`{"error":{"message":"internal error","kind":"render_error"}}`)
	}
	return b
}

// resultEnvelope wraps a successful query response verbatim as
// `{"result": <body>}`. A body that is not valid JSON is reported as a service
// failure rather than injected into the guest's JSON.parse.
func resultEnvelope(payload []byte) []byte {
	env := struct {
		Result json.RawMessage `json:"result"`
	}{Result: payload}
	b, err := json.Marshal(env)
	if err != nil {
		return guestErr(errKindUnavailable, "query service returned a malformed response")
	}
	return b
}
