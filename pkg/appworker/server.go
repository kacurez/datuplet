package appworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/semaphore"

	"github.com/datuplet/datuplet/pkg/appengine"
	"github.com/datuplet/datuplet/ui/appshell"
)

// Engine is the subset of *appengine.Engine's behavior app-worker depends on.
// Defining it as an interface (rather than importing *appengine.Engine
// directly) lets tests inject a fake engine constructor and assert Serve's
// boot-time wiring without paying the real ~0.25s WASM compile cost
// (task-E1-report.md's "NewEngine is expensive-ish" note).
// *appengine.Engine satisfies this interface structurally.
//
// Render was added by the render pipeline (W5); the render tests use the REAL
// engine (a fake engine cannot exercise the guest ABI), so this interface
// exists purely for the boot/shutdown seam.
type Engine interface {
	Render(ctx context.Context, in appengine.RenderInput) (*appengine.Result, *appengine.RenderError)
	Close(ctx context.Context) error
}

// EngineConstructor builds the render engine. Production wiring
// (cmd/app-worker/main.go) passes a thin adapter over appengine.NewEngine;
// tests inject a fake to assert the boot-time call.
type EngineConstructor func(ctx context.Context, memoryPages uint32) (Engine, error)

// errorKind enumerates the app-worker error envelope's `kind` values
// (contract-and-constraints.md, spec §8) — the complete, fixed vocabulary.
type errorKind string

const (
	errKindBadRequest   errorKind = "bad_request"
	errKindUnauthorized errorKind = "unauthorized"
	errKindAppNotFound  errorKind = "app_not_found"
	errKindRenderError  errorKind = "render_error"
	errKindTimeout      errorKind = "timeout"
	errKindRateLimited  errorKind = "rate_limited"
	errKindCapacity     errorKind = "capacity"
	errKindUnavailable  errorKind = "unavailable"
)

// errorBody is the JSON error envelope shape: {error, kind, request_id}
// (contract-and-constraints.md).
type errorBody struct {
	Error     string    `json:"error"`
	Kind      errorKind `json:"kind"`
	RequestID string    `json:"request_id"`
}

// ---------------------------------------------------------------------------
// Outcome taxonomy — ONE vocabulary for the render log, the access log, and
// the Prometheus label (spec §9; contract-and-constraints.md)
// ---------------------------------------------------------------------------

const (
	// outcomeOK is the successful-render outcome. It is spelled `ok` — not
	// `success` — because it is the SAME label value the query service's
	// query_audit / pipelineapi_query_requests_total already uses, and because
	// the render log's `outcome` column, this worker's access log, and the
	// Prometheus label must be one taxonomy rather than three near-misses.
	// Every other value is a spec §8 error kind verbatim.
	outcomeOK = "ok"

	// outcomeExchange marks the one-time `?token=` exchange (spec §5.3): a 302
	// that is NOT a render, so it is recorded in the access log but never
	// counted in the render counter. It is deliberately the only access-log
	// outcome outside the §8 kind vocabulary + `ok`.
	outcomeExchange = "exchange"
)

// renderRequestsOpts describes the §9 render counter. Declared once so the
// package-level (default-registry) counter and a test-injected registry's
// counter cannot drift in name, help text, or label set.
var renderRequestsOpts = prometheus.CounterOpts{
	Name: "datuplet_appworker_render_requests_total",
	Help: "Total app render requests served by app-worker, labeled by outcome.",
}

const outcomeLabel = "outcome"

// renderRequestsTotal is the package-level counter registered on the default
// registry via promauto — mirroring pkg/pipelineapi/queryproxy's
// pipelineapi_query_requests_total exactly (same single low-cardinality
// `outcome` label, same promauto/default-registry pattern, same
// promhttp exposure). Tests use WithMetricsRegistry to get an isolated
// registry instead of racing this singleton.
var renderRequestsTotal = promauto.NewCounterVec(renderRequestsOpts, []string{outcomeLabel})

// ---------------------------------------------------------------------------
// Response-shaping constants (spec §4.2, §6.4, §6.5)
// ---------------------------------------------------------------------------

const (
	// shellCSP is spec §6.4's shell Content-Security-Policy, verbatim. Set on
	// every HTML response this worker produces (the shell AND the minimal HTML
	// error page), so even a validator miss cannot reach the network.
	// `frame-ancestors 'self'` (not 'none') is deliberate — see spec §6.4.
	shellCSP = "default-src 'none'; script-src 'self'; style-src 'self'; " +
		"connect-src 'self'; img-src 'self' data:; base-uri 'none'; " +
		"form-action 'self'; frame-ancestors 'self'"

	// shellDocScriptID is the element id ui/appshell/index.html's doc island
	// carries, and shell.js reads the embedded OutputDoc from. No longer
	// interpolated at render time (Part 4 made index.html a real static
	// asset with this id hardcoded) — kept as a named constant purely so
	// tests assert against one source of truth instead of a repeated
	// literal.
	shellDocScriptID = "dtp-doc"
	// shellRootID is the mount point id ui/appshell/index.html hardcodes and
	// shell.js renders into. See shellDocScriptID's comment on why this is a
	// constant despite not being interpolated into the template anymore.
	shellRootID = "dtp-root"

	// shellAssetPrefix is where app-worker mounts the embedded
	// ui/appshell.Assets subtree (shell.js, theme.css, the vendored
	// third-party libraries, the vendored vegaspec schema copy) — the
	// same-origin assets ui/appshell/index.html's <script src>/<link href>
	// tags reference. "-" is a reserved pid sentinel: a real {pid} is a
	// lakekeeper project id and can never literally be "-", so this literal
	// subtree route can never collide with a real app's
	// /apps/{pid}/{name}/{path...} URL, and Go 1.22 ServeMux resolves the
	// ambiguity in this route's favor (a literal path segment is more
	// specific than a wildcard at the same position) — proven by
	// TestShellAssets_ServedAtReservedPath's resolveCalls assertion.
	shellAssetPrefix = "/apps/-/shell/"

	// shellTitlePlaceholder and shellDocPlaceholder are the two tokens
	// ui/appshell/index.html carries for writeShell to substitute. They are
	// HTML comments so the template renders inertly (e.g. in an editor
	// preview) before substitution.
	shellTitlePlaceholder = "<!--DTP:TITLE-->"
	shellDocPlaceholder   = "<!--DTP:DOC-->"

	// blockQueryParam selects a single block for a partial re-render
	// (spec §4.2). One of the reserved param names (with `token` and
	// modalStateParam) stripped before the guest sees ctx.params (spec §6.5).
	blockQueryParam = "block"

	// modalStateParam is the shell's reserved modal deep-link key
	// (ui/appshell/interact.js MODAL_PARAM). The shell writes `?__dtp_modal=<id>`
	// to deep-link an open modal; it is platform-owned URL bookkeeping, NOT an
	// app filter param, so — like `token` and `block` — it is stripped before
	// the guest sees ctx.params. The `__dtp_` prefix is reserved for the shell
	// and app filter params must not use it. (The POST body already omits it via
	// the shell's getParams(); stripping here also covers the GET nav query.)
	modalStateParam = "__dtp_modal"

	// draftSuffix marks the draft channel in the app name: `{name}@draft`
	// (spec §4.1).
	draftSuffix = "@draft"
)

// shellTemplate is ui/appshell/index.html (RFC 028 Part 4), read once at
// package init from the embedded asset. writeShell substitutes
// shellTitlePlaceholder/shellDocPlaceholder into a copy of this string per
// request; the template itself is immutable package state.
var shellTemplate = string(appshell.IndexHTML)

// Request-input normalization limits — spec §6.5/§7 verbatim.
const (
	maxParamKeys       = 32
	maxParamKeyLen     = 64
	maxParamValueBytes = 1 << 10  // 1 KiB
	maxRequestURIBytes = 8 << 10  // 8 KiB
	maxPostBodyBytes   = 16 << 10 // 16 KiB, enforced PRE-PARSE
	maxCtxPathLen      = 256
)

// capacityRetryAfterS is the Retry-After for a pod-saturation shed. Mirrors
// the query proxy's own `Retry-After: 2` on its capacity path — a saturated
// pod recovers on the order of one render, not one rate-limit window.
const capacityRetryAfterS = 2

// Server is app-worker's HTTP handler: the routing, the spec §4.2 response
// matrix, the §6.5 input normalization, the §9 access log + render counter,
// and the health/readiness endpoints, over W3's pipeline-api client (`api` /
// `rapi` / `rsapi`), W4's viewer auth, and W5's render pipeline.
type Server struct {
	cfg    Config
	engine Engine
	mux    *http.ServeMux

	// api is the pipeline-api client (W3) viewer auth calls into. Nil until
	// wired: authenticate fails closed with `unavailable` rather than
	// panicking or — far worse — waving requests through.
	api authAPI
	// rapi is the SAME pipeline-api client, seen through the render path's
	// method set (renderAPI in render.go). Separate fields, not one union
	// interface: W4's auth doubles implement only the three auth methods, and
	// a union would force each to grow four irrelevant stubs. Nil until wired:
	// render fails closed with `unavailable`.
	rapi renderAPI
	// rsapi is the SAME pipeline-api client again, seen through the resolve
	// method set. Nil until wired: the route fails closed with `unavailable`.
	rsapi resolveAPI
	// cookieKey is the HMAC key for the viewer session cookie (spec §5.3,
	// mounted via Config.CookieKeyFile). An empty key never authenticates a
	// cookie; see authenticate. Serve refuses to boot without it.
	cookieKey string
	// now is the injected clock: cookie expiry and every rate bucket read it,
	// so tests drive them deterministically instead of sleeping.
	now func() time.Time

	// logger receives the §9 access log. Structured fields ONLY — never a
	// request URL, query string, or Referer (spec §5.3 token log-redaction).
	logger *slog.Logger

	// renderRequests is the §9 counter; metrics is what /metrics serves. Both
	// default to the package-level promauto counter on the default registry;
	// WithMetricsRegistry swaps in an isolated registry for tests.
	renderRequests *prometheus.CounterVec
	metrics        prometheus.Gatherer

	// engineReady gates /readyz. It is flipped by markEngineReady, which
	// Serve calls only after the engine constructor has RETURNED — readiness
	// is an explicit assertion made by the boot path, never inferred from a
	// non-nil field, so a future refactor that builds the Server before
	// compiling cannot silently flip the gate early (spec §7 rollout
	// requirement: "readiness only after engine compile").
	engineReady atomic.Bool

	// Render-rate buckets (spec §7) and the verify-failure anti-hammering
	// bucket (spec §5.3). Shared mutable state across concurrent renders —
	// each registry is mutex-guarded.
	renderPrincipalLimits *limiterRegistry
	renderAppLimits       *limiterRegistry
	verifyFailLimits      *limiterRegistry

	// The two admission gates (spec §7, render.go). They have DELIBERATELY
	// DIFFERENT acquisition policies and must not be conflated:
	//   - perAppInflight is the app's own concurrency ceiling, acquired
	//     NON-BLOCKING → `rate_limited` (the caller should back off);
	//   - pool is the whole-pod render-slot semaphore, acquired with a SHORT
	//     BOUNDED WAIT (poolAcquireWait) → `capacity` (the pod is saturated;
	//     another replica should take it). Never an unbounded block.
	// Both are shared mutable state across concurrent renders.
	perAppInflight  *inflightGate
	pool            *semaphore.Weighted
	poolAcquireWait time.Duration
}

// resolveAPI is the subset of *APIClient (W3) the route's resolve step depends
// on. Separate from authAPI/renderAPI for the same reason those two are
// separate (see the rapi field); *APIClient satisfies all three structurally.
type resolveAPI interface {
	Resolve(ctx context.Context, pid, name, channel string) (Resolved, error)
}

// ServerOption configures a Server at construction. Production wiring
// (Serve) passes the real *APIClient to all three API options and the cookie
// key read from Config.CookieKeyFile; tests inject doubles and a fake clock.
type ServerOption func(*Server)

// WithAuthAPI injects the pipeline-api client viewer auth uses.
func WithAuthAPI(api authAPI) ServerOption { return func(s *Server) { s.api = api } }

// WithRenderAPI injects the pipeline-api client the render path uses (bundle
// fetch, impersonation mint, query proxy, render-log append). Production
// wiring passes the SAME *APIClient given to WithAuthAPI.
func WithRenderAPI(api renderAPI) ServerOption { return func(s *Server) { s.rapi = api } }

// WithResolveAPI injects the pipeline-api client the route's resolve step
// uses. Production wiring passes the SAME *APIClient given to WithAuthAPI.
func WithResolveAPI(api resolveAPI) ServerOption { return func(s *Server) { s.rsapi = api } }

// WithPoolAcquireWait overrides how long a render waits for a whole-pod render
// slot before being shed as `capacity` (default 250 ms). Tests shrink it so the
// bounded-wait outcome is observable without a quarter-second sleep per case.
func WithPoolAcquireWait(d time.Duration) ServerOption {
	return func(s *Server) {
		if d > 0 {
			s.poolAcquireWait = d
		}
	}
}

// WithCookieKey injects the viewer session cookie's HMAC key.
func WithCookieKey(key string) ServerOption { return func(s *Server) { s.cookieKey = key } }

// WithServerClock injects the clock used for cookie expiry and rate limiting.
func WithServerClock(now func() time.Time) ServerOption {
	return func(s *Server) {
		if now != nil {
			s.now = now
		}
	}
}

// WithLogger injects the logger the §9 access log is written to.
func WithLogger(l *slog.Logger) ServerOption {
	return func(s *Server) {
		if l != nil {
			s.logger = l
		}
	}
}

// WithMetricsRegistry registers the §9 render counter on reg instead of the
// default registry, and serves /metrics from it. Tests pass a fresh
// prometheus.Registry so counter deltas are measurable without racing the
// package-level promauto singleton (the same seam
// queryproxy.HandlerWithAudit provides).
func WithMetricsRegistry(reg *prometheus.Registry) ServerOption {
	return func(s *Server) {
		if reg == nil {
			return
		}
		s.renderRequests = promauto.With(reg).NewCounterVec(renderRequestsOpts, []string{outcomeLabel})
		s.metrics = reg
	}
}

// NewServer builds the Server and its route table.
func NewServer(cfg Config, engine Engine, opts ...ServerOption) *Server {
	s := &Server{
		cfg:             cfg,
		engine:          engine,
		now:             time.Now,
		poolAcquireWait: defaultPoolAcquireWait,
		logger:          slog.Default(),
		renderRequests:  renderRequestsTotal,
		metrics:         prometheus.DefaultGatherer,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.perAppInflight = newInflightGate(cfg.Render.PerAppInflight)
	concurrency := cfg.Render.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	s.pool = semaphore.NewWeighted(int64(concurrency))
	s.renderPrincipalLimits = newLimiterRegistry(renderRatePerPrincipalPerMin, renderRatePerPrincipalBurst, s.now)
	s.renderAppLimits = newLimiterRegistry(renderRatePerAppPerMin, renderRatePerAppPerMin, s.now)
	s.verifyFailLimits = newLimiterRegistry(verifyFailuresPerMin, verifyFailuresPerMin, s.now)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics, promhttp.HandlerOpts{}))
	// The trusted-shell static asset subtree (RFC 028 Part 4). Registered
	// under the reserved "-" pid sentinel so it out-specificities the
	// wildcard app route below for any request under shellAssetPrefix (see
	// that constant's doc comment); registration order here does not matter
	// to Go 1.22 ServeMux's specificity resolution, but the route is placed
	// first for readability, next to the other non-app-render endpoints.
	mux.Handle("GET "+shellAssetPrefix, shellAssetHandler())
	// Two patterns, one handler: the bare app URL and any sub-path (which
	// becomes ctx.path, spec §6.5).
	mux.HandleFunc("/apps/{pid}/{name}", s.handleApp)
	mux.HandleFunc("/apps/{pid}/{name}/{path...}", s.handleApp)
	mux.HandleFunc("/", s.handleNotFound)
	s.mux = mux
	return s
}

// markEngineReady flips the /readyz gate. Serve calls it after the engine
// constructor returns; nothing else may.
func (s *Server) markEngineReady() { s.engineReady.Store(true) }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Every /apps/* response carries Referrer-Policy: no-referrer (spec §5.3
	// token log-redaction), including the 302 exchange. Applied worker-wide
	// from here so no route can forget it.
	w.Header().Set("Referrer-Policy", "no-referrer")

	// ctx.path hygiene must be decided BEFORE http.ServeMux sees the request:
	// the mux 301-redirects any path needing cleaning, so a `..` traversal
	// attempt would otherwise become a redirect instead of the spec §6.5
	// `bad_request` it is. Checked on the raw (pre-cleaning) URL.
	if isAppPath(r.URL.Path) && !appPathIsClean(r.URL) {
		s.rejectAppRequest(w, r, http.StatusBadRequest, errKindBadRequest,
			"invalid app path")
		return
	}
	s.mux.ServeHTTP(w, r)
}

// isAppPath reports whether the request targets the public app surface.
func isAppPath(p string) bool { return p == "/apps" || strings.HasPrefix(p, "/apps/") }

// appPathIsClean rejects the two path shapes spec §6.5 forbids: `..`
// traversal (in any encoding — the DECODED segments are inspected, so
// `%2e%2e` is caught too) and encoded path separators (`%2F`, `%5C`), which
// would smuggle an extra segment past the router's own splitting.
func appPathIsClean(u *url.URL) bool {
	escaped := u.EscapedPath()
	for _, enc := range []string{"%2f", "%2F", "%5c", "%5C"} {
		if strings.Contains(escaped, enc) {
			return false
		}
	}
	for _, seg := range strings.Split(u.Path, "/") {
		if seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleReadyz reports 200 only once the render engine has compiled (spec §7:
// "readiness only after engine compile"). A fresh pod must never be routed
// traffic it would have to fail.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.engineReady.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready: render engine still compiling"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleNotFound answers anything outside the documented surface. `/apps/…`
// shapes that do not name a project AND an app land here too, so the kind is
// app_not_found rather than a generic 404.
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	if isAppPath(r.URL.Path) {
		s.rejectAppRequest(w, r, http.StatusNotFound, errKindAppNotFound, "no such app")
		return
	}
	writeError(w, r, http.StatusNotFound, errKindAppNotFound, "not found")
}

// ---------------------------------------------------------------------------
// Access log (spec §9 audit layer 2) — structured fields ONLY
// ---------------------------------------------------------------------------

// accessLogRecord is one app-worker access-log line. The field set is spec
// §9's audit layer 2 verbatim: app, version hash, channel, principal kind +
// id, path, params hash, outcome, duration, client IP.
//
// SECURITY INVARIANT, and the reason this is a struct rather than ad-hoc
// slog args at each call site: there is NO field for the request URL, the
// query string, or the Referer, and there never may be. The one-time
// `?token=` exchange puts a plaintext viewer token in a URL (spec §5.3), so a
// URL in this log is a credential in this log. `Path` is the normalized
// ctx.path — the sub-path only, never a query string — and `ParamsHash` is
// computed AFTER the reserved `token`/`block` names are stripped, so `token`
// cannot enter even in hashed form.
type accessLogRecord struct {
	RequestID     string
	Method        string
	ProjectID     string
	AppName       string
	Channel       string
	AppID         string
	VersionHash   string
	Path          string
	ParamsHash    string
	PrincipalKind string
	PrincipalID   string
	Outcome       string
	Status        int
	DurationMS    int64
	ClientIP      string
}

// emitAccessLog writes the record and moves the §9 counter. Exactly one call
// per /apps/* request, from a deferred closure, so exactly-one-emission is
// structural rather than per-branch discipline (the pattern
// queryproxy.serveWithAudit established).
func (s *Server) emitAccessLog(rec *accessLogRecord) {
	logger := s.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("app_render_access",
		"request_id", rec.RequestID,
		"method", rec.Method,
		"project_id", rec.ProjectID,
		"app_name", rec.AppName,
		"app_id", rec.AppID,
		"version_hash", rec.VersionHash,
		"channel", rec.Channel,
		"path", rec.Path,
		"params_hash", rec.ParamsHash,
		"principal_kind", rec.PrincipalKind,
		"principal_id", rec.PrincipalID,
		"outcome", rec.Outcome,
		"status", rec.Status,
		"duration_ms", rec.DurationMS,
		"client_ip", rec.ClientIP,
	)
	// The exchange 302 is not a render, so it is logged but never counted.
	if rec.Outcome != "" && rec.Outcome != outcomeExchange && s.renderRequests != nil {
		s.renderRequests.WithLabelValues(rec.Outcome).Inc()
	}
}

// paramsHash is the §9 params fingerprint: sha256 over the sorted key/value
// pairs, first 16 hex chars (bounded cardinality, same convention as
// queryproxy.statementHash). Computed on the ALREADY-STRIPPED map, so
// `token` can never contribute to it. "" for no params.
func paramsHash(params map[string]string) string {
	if len(params) == 0 {
		return ""
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(params[k])
		b.WriteByte(0)
	}
	return sha256Hex([]byte(b.String()))[:16]
}

// ---------------------------------------------------------------------------
// appResponseWriter — carries the outcome back out of the response writers
// ---------------------------------------------------------------------------

// appResponseWriter records the status and the §8 error kind actually written,
// so the deferred access-log emit can name the outcome without every branch
// having to remember to set it. writeError stamps `kind`; that is what keeps
// the envelope's kind, the log's outcome, and the metric label identical by
// construction.
type appResponseWriter struct {
	http.ResponseWriter
	status int
	kind   errorKind
}

func (w *appResponseWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *appResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// outcome derives the access-log outcome from what was actually written.
func (w *appResponseWriter) outcome() string {
	if w.kind != "" {
		return string(w.kind)
	}
	switch {
	case w.status >= 300 && w.status < 400:
		return outcomeExchange
	case w.status == 0 || (w.status >= 200 && w.status < 300):
		return outcomeOK
	default:
		// Unreachable: every non-2xx this worker writes goes through
		// writeError, which stamps a kind. Named rather than left blank so a
		// future branch that forgets cannot mint an empty metric label.
		return string(errKindUnavailable)
	}
}

// ---------------------------------------------------------------------------
// The app route (spec §4.2)
// ---------------------------------------------------------------------------

// handleApp serves GET/POST /apps/{pid}/{name}[/{sub-path}].
//
// The step order is the spec's and is load-bearing:
//
//  1. route parse (`@draft` suffix → channel; sub-path → ctx.path)
//  2. RESOLVE — spec §4.2 step 1, BEFORE authentication: viewer-token
//     verification and the cookie↔app binding are keyed by the resolved
//     app_id, never by anything the client supplied.
//  3. authenticate (W4) — writes its own response when it denies
//  4. allowRender (W4's rate buckets) — once per render, keyed on the
//     principal, so it can only run after step 3
//  5. §6.5 input normalization — after the authz gate, so an unauthenticated
//     caller learns nothing about the validation surface (the same ordering
//     queryproxy applies to its body decode)
//  6. render (W5)
//  7. the §4.2 response matrix
func (s *Server) handleApp(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.NewString()
	// One id for the envelope, the render-log record, and this access log —
	// that identity is what makes `datuplet apps logs --request-id` work.
	r = r.WithContext(withRequestID(r.Context(), requestID))

	arw := &appResponseWriter{ResponseWriter: w}
	started := s.now()
	rec := &accessLogRecord{
		RequestID: requestID,
		Method:    r.Method,
		// s.clientIP — NOT RemoteAddr or XFF directly — so the log and the
		// rate-limit bucket can never disagree about who the client was
		// (task-W4-report.md, fix round 1).
		ClientIP: s.clientIP(r),
	}
	defer func() {
		rec.Status = arw.status
		rec.Outcome = arw.outcome()
		rec.DurationMS = s.now().Sub(started).Milliseconds()
		s.emitAccessLog(rec)
	}()

	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		arw.Header().Set("Allow", "GET, POST")
		s.fail(arw, r, http.StatusMethodNotAllowed, errKindBadRequest, "method not allowed")
		return
	}

	pid := r.PathValue("pid")
	name, channel := parseAppName(r.PathValue("name"))
	if pid == "" || name == "" {
		s.fail(arw, r, http.StatusNotFound, errKindAppNotFound, "no such app")
		return
	}
	ctxPath := normalizeCtxPath(r.PathValue("path"))
	if len(ctxPath) > maxCtxPathLen {
		s.fail(arw, r, http.StatusBadRequest, errKindBadRequest, "path too long")
		return
	}
	rec.ProjectID, rec.AppName, rec.Channel, rec.Path = pid, name, channel, ctxPath

	resolved, ok := s.resolveApp(arw, r, pid, name, channel)
	if !ok {
		return
	}
	rec.AppID, rec.VersionHash = resolved.AppID, resolved.VersionHash

	p, ok := s.authenticate(arw, r, resolved)
	if !ok {
		return
	}
	rec.PrincipalKind, rec.PrincipalID = p.Kind, p.ID

	// Render RATE buckets (spec §7): per-principal 60/min burst 10, per-app
	// 300/min. Exactly once per render, and only here — render() owns the
	// separate CONCURRENCY gates, not these.
	principalKey, appKey := rateKeys(p, resolved.AppID)
	if allowed, retryAfter := s.allowRender(principalKey, appKey); !allowed {
		s.failRateLimited(arw, r, retryAfter)
		return
	}

	params, block, err := readParams(r)
	if err != nil {
		s.fail(arw, r, http.StatusBadRequest, errKindBadRequest, err.Error())
		return
	}
	rec.ParamsHash = paramsHash(params)

	doc, rerr := s.render(r.Context(), resolved, ctxPath, params, p)
	if rerr != nil {
		s.failRender(arw, r, rerr)
		return
	}

	s.writeRenderResponse(arw, r, doc, block)
}

// parseAppName splits `{name}` into the bare app name and its channel. The
// bare name is what the cookie Path is scoped to, so both channels of one app
// MUST yield the identical name (spec §5.3).
func parseAppName(raw string) (name, channel string) {
	if bare, found := strings.CutSuffix(raw, draftSuffix); found {
		return bare, channelDraft
	}
	return raw, channelProduction
}

// normalizeCtxPath turns the router's `{path...}` value into ctx.path: always
// rooted, no trailing slash beyond the root. Traversal and encoded separators
// were already rejected in ServeHTTP (appPathIsClean); the length bound is
// checked by the caller, which owns the error envelope.
func normalizeCtxPath(sub string) string {
	sub = strings.TrimPrefix(sub, "/")
	if sub == "" {
		return "/"
	}
	return "/" + sub
}

// resolveApp performs spec §4.2 step 1. A 404/app_not_found from pipeline-api
// is the viewer's 404; anything else (including a 401, which means
// app-worker's OWN service credential is wrong — task-P3-report.md) fails
// closed as `unavailable`, never as a per-viewer denial.
func (s *Server) resolveApp(w http.ResponseWriter, r *http.Request, pid, name, channel string) (resolvedApp, bool) {
	if s.rsapi == nil {
		s.fail(w, r, http.StatusServiceUnavailable, errKindUnavailable,
			"app-worker: app resolution is not configured")
		return resolvedApp{}, false
	}
	res, err := s.rsapi.Resolve(r.Context(), pid, name, channel)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) &&
			(apiErr.StatusCode == http.StatusNotFound || apiErr.Kind == string(errKindAppNotFound)) {
			s.fail(w, r, http.StatusNotFound, errKindAppNotFound, "no such app")
			return resolvedApp{}, false
		}
		s.fail(w, r, http.StatusServiceUnavailable, errKindUnavailable,
			"app-worker: cannot resolve app")
		return resolvedApp{}, false
	}
	return resolvedApp{
		ProjectID:   pid,
		Name:        name,
		Channel:     channel,
		AppID:       res.AppID,
		VersionHash: res.VersionHash,
	}, true
}

// failRender maps a render outcome's §8 kind onto its HTTP status (spec §8;
// task-W5-report.md §11.3). The viewer sees only the generic envelope — the
// real error and the guest stack went to the author's render log.
func (s *Server) failRender(w http.ResponseWriter, r *http.Request, rerr *appengine.RenderError) {
	kind := errorKind(rerr.Kind)
	switch kind {
	case errKindBadRequest:
		s.fail(w, r, http.StatusBadRequest, kind, "the app rejected the request input")
	case errKindTimeout:
		s.fail(w, r, http.StatusGatewayTimeout, kind, "the app took too long to render")
	case errKindRateLimited:
		s.failRateLimited(w, r, 1)
	case errKindCapacity:
		w.Header().Set("Retry-After", fmt.Sprint(capacityRetryAfterS))
		s.fail(w, r, http.StatusServiceUnavailable, kind, "app-worker is at render capacity")
	case errKindUnavailable:
		s.fail(w, r, http.StatusServiceUnavailable, kind, "the app could not be served")
	default:
		// render_error and anything the engine might grow: 500 + the generic
		// message. Never rerr.Msg — it can carry guest-authored text.
		s.fail(w, r, http.StatusInternalServerError, errKindRenderError, "the app failed to render")
	}
}

// ---------------------------------------------------------------------------
// §6.5 request-input normalization
// ---------------------------------------------------------------------------

// readParams builds ctx.params and pulls out the reserved `block` selector.
//
// Order (each step's failure is a 400 `bad_request`):
//
//  1. total request URI ≤8 KiB
//  2. the query string parses
//  3. POST only: `Content-Type: application/json`, body ≤16 KiB enforced
//     PRE-PARSE (a 17 KiB body is refused without ever reaching the JSON
//     decoder), then a flat JSON object
//  4. merge — body overrides query; duplicate keys last-wins
//  5. strip the reserved `token` and `block`
//  6. ≤32 keys, key ≤64 chars, value ≤1 KiB
//
// The limits are checked AFTER stripping: the two reserved names are consumed
// by the platform, so an author's 32-key budget is not spent on them.
//
// Every error message is a fixed literal. Echoing an offending key or value
// back would put client-controlled text in a response body (and, worse, in a
// minimal HTML error page).
func readParams(r *http.Request) (params map[string]string, block string, err error) {
	if len(r.URL.RequestURI()) > maxRequestURIBytes {
		return nil, "", errors.New("request URL too long")
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, "", errors.New("malformed query string")
	}

	params = make(map[string]string, len(query))
	for k, vs := range query {
		if len(vs) > 0 {
			// Duplicate key: last wins (spec §6.5).
			params[k] = vs[len(vs)-1]
		}
	}

	if r.Method == http.MethodPost {
		body, berr := readJSONBody(r)
		if berr != nil {
			return nil, "", berr
		}
		for k, v := range body {
			params[k] = v
		}
	}

	// Reserved names, stripped before the guest ever sees ctx.params.
	block = params[blockQueryParam]
	delete(params, blockQueryParam)
	delete(params, tokenQueryParam)
	// The shell's modal deep-link key is platform-owned URL bookkeeping, not an
	// app filter param — strip it so it never reaches ctx.params (it can arrive
	// in a GET nav query string; the POST body already omits it shell-side).
	delete(params, modalStateParam)

	if len(params) > maxParamKeys {
		return nil, "", fmt.Errorf("too many parameters (max %d)", maxParamKeys)
	}
	for k, v := range params {
		if len(k) > maxParamKeyLen {
			return nil, "", fmt.Errorf("parameter name too long (max %d characters)", maxParamKeyLen)
		}
		if len(v) > maxParamValueBytes {
			return nil, "", fmt.Errorf("parameter value too long (max %d bytes)", maxParamValueBytes)
		}
	}
	return params, block, nil
}

// readJSONBody enforces spec §6.5's re-render POST rules and flattens the
// body into string→string.
//
// The 16 KiB cap is enforced PRE-PARSE by reading one byte past it: an
// oversized body is refused before any JSON decoding happens, so a hostile
// caller cannot make app-worker parse megabytes to discover it should have
// said no.
//
// Values: strings pass through verbatim. Numbers and booleans are taken as
// their JSON literal text — NOT coerced (spec §6.5's "no type coercion …
// apps parse their own numbers": the app receives "42" and parses it
// itself). Arrays, objects, and null are rejected: "no arrays, no nesting".
func readJSONBody(r *http.Request) (map[string]string, error) {
	ct := r.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil || mediaType != "application/json" {
		return nil, errors.New("re-render POST requires Content-Type: application/json")
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, maxPostBodyBytes+1))
	if err != nil {
		return nil, errors.New("could not read request body")
	}
	if len(raw) > maxPostBodyBytes {
		return nil, fmt.Errorf("request body too large (max %d bytes)", maxPostBodyBytes)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return map[string]string{}, nil
	}

	var obj map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, errors.New("request body must be a JSON object")
	}

	// §6.5: ctx.params is a flat string→string map with NO type coercion —
	// "apps parse their own numbers". A JSON string value passes through; a
	// number, bool, null, array, or object is REJECTED (the client must send
	// `{"n":"42"}`, matching the inherently-string GET query path). Coercing
	// a number to "42" here would silently diverge the two input channels and
	// hand the app a value the spec says it must have parsed itself.
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		s, ok := v.(string)
		if !ok {
			return nil, errors.New("request body values must be JSON strings (apps parse their own numbers)")
		}
		out[k] = s
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// §4.2 response matrix
// ---------------------------------------------------------------------------

// writeRenderResponse applies spec §4.2's normative response matrix:
//
//	navigation (no `Accept: application/json`) → shell HTML embedding the full
//	                                            doc; a `block` param is IGNORED
//	Accept: application/json                  → the full OutputDoc
//	Accept: application/json + block=<id>     → that single block
//	unknown block id                          → 400 `bad_request`
func (s *Server) writeRenderResponse(w http.ResponseWriter, r *http.Request, doc json.RawMessage, block string) {
	if !wantsJSON(r) {
		// Navigation. `block` is deliberately ignored rather than validated:
		// the shell re-fetches partials itself, so a stale `block` in a
		// bookmarked URL must not break the page.
		s.writeShell(w, doc)
		return
	}

	if block == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(doc)
		return
	}

	one, found := findBlock(doc, block)
	if !found {
		s.fail(w, r, http.StatusBadRequest, errKindBadRequest, "unknown block id")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(one)
}

// wantsJSON is the response matrix's discriminator.
func wantsJSON(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "application/json")
}

// shellAssetHandler serves the trusted-viewer-shell static subtree embedded
// in ui/appshell (shell.js, theme.css, the vendored third-party libraries,
// and the client-side Vega-schema copy) at shellAssetPrefix.
//
// Deliberately NOT behind handleApp: these are platform-owned, public,
// tenant-agnostic files (no OutputDoc, no query data ever touches them), and
// the viewer session cookie is scoped to Path=/apps/{pid}/{name} (spec
// §5.3) — it would never even be attached to a request here, so the shell's
// own assets cannot be gated behind the same per-app auth without breaking
// every app that loads them. Bypassing handleApp also means these requests
// are not access-logged or rate-limited, matching /healthz, /readyz, and
// /metrics, which are the same kind of infrastructure endpoint.
//
// index.html is excluded on purpose (see appshell.IndexHTML's doc comment):
// it is a server-side template, never a static file.
func shellAssetHandler() http.Handler {
	fileServer := http.FileServerFS(appshell.Assets)
	return http.StripPrefix(shellAssetPrefix, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Refuse directory-style requests (the bare prefix, or any
		// subdirectory with a trailing slash): the embedded FS has no
		// index.html of its own, so http.FileServer would otherwise
		// auto-generate a directory listing instead of a 404.
		if r.URL.Path == "" || strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		fileServer.ServeHTTP(w, r)
	}))
}

// writeShell emits the trusted viewer shell: ui/appshell/index.html (RFC 028
// Part 4) with the OutputDoc's title and JSON substituted into its two
// placeholders. The template's own <script src>/<link href> tags reference
// ONLY same-origin assets served by shellAssetHandler — no CDN, no external
// fonts (TestShell_ReferencesOnlySameOriginAssets enforces this).
func (s *Server) writeShell(w http.ResponseWriter, doc json.RawMessage) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", shellCSP)
	w.WriteHeader(http.StatusOK)
	page := strings.Replace(shellTemplate, shellTitlePlaceholder, html.EscapeString(docTitle(doc)), 1)
	page = strings.Replace(page, shellDocPlaceholder, escapeJSONForScript(doc), 1)
	_, _ = io.WriteString(w, page)
}

// docTitle reads the OutputDoc's title for the <title> element, falling back
// to a neutral one. The value is HTML-escaped by the caller.
func docTitle(doc json.RawMessage) string {
	var top struct {
		Title string `json:"title"`
	}
	if err := json.Unmarshal(doc, &top); err != nil || strings.TrimSpace(top.Title) == "" {
		return "Datuplet app"
	}
	return top.Title
}

// escapeJSONForScript makes a JSON document safe to inline in a
// `<script type="application/json">` element by replacing the bytes that could
// break out of it with their JSON `\uXXXX` escapes:
//
//	<  → <    >  → >    &  → &
//	U+2028 →      U+2029 →     (JS line terminators, legal raw in
//	                                      JSON strings but not in JS source)
//
// The escaped forms are valid JSON that parses back to the byte-identical
// string, so the embedded doc is unchanged SEMANTICALLY while being inert as
// markup: a doc whose text contains `</script>` can no longer close the element
// early and inject into the platform-owned trusted shell.
//
// This is load-bearing precisely because the render path re-encodes the
// OutputDoc with `SetEscapeHTML(false)` to keep an author's bytes intact
// (task-W5-report.md §4), so `json.Encoder`'s own HTML escaping never ran —
// this function is the only thing standing between app-controlled output and
// the shell response.
//
// Byte-level replacement is sound because in well-formed JSON these bytes can
// only appear inside a string literal (the structural bytes are `{}[],:"` plus
// number/keyword bytes), and inside a string literal each maps to an
// exactly-equivalent `\uXXXX` encoding. None of the replacement strings
// introduces a new `<`, `>`, or `&`, so the order of the replacements does not
// matter.
func escapeJSONForScript(doc json.RawMessage) string {
	var b strings.Builder
	b.Grow(len(doc))
	for _, r := range string(doc) {
		switch r {
		case '<':
			b.WriteString(`\u003c`)
		case '>':
			b.WriteString(`\u003e`)
		case '&':
			b.WriteString(`\u0026`)
		case ' ':
			b.WriteString(`\u2028`)
		case ' ':
			b.WriteString(`\u2029`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// findBlock locates a block by id anywhere in the document — root blocks,
// `tabs` block tabs, block-level modals, and table-row modals — mirroring the
// walk validateChartSpecs uses. Ids are unique document-wide (W1's validator
// enforces it), so the lookup is unambiguous at any depth, which is what lets
// the shell re-fetch a block nested inside a tab.
func findBlock(doc json.RawMessage, id string) (json.RawMessage, bool) {
	var top struct {
		Blocks []json.RawMessage `json:"blocks"`
	}
	if err := json.Unmarshal(doc, &top); err != nil {
		return nil, false
	}
	return searchBlocks(top.Blocks, id)
}

func searchBlocks(blocks []json.RawMessage, id string) (json.RawMessage, bool) {
	for _, raw := range blocks {
		// The id check comes FIRST and tolerates any block shape: a table with
		// plain-array rows (`rows: [["a","b"], …]`, a valid W1 tableRow form)
		// must be findable by its own id even though those rows do not fit the
		// modal-carrying struct below. Decoding into a single big struct up
		// front — as the pre-fix code did — made a plain-array row fail the
		// whole-block unmarshal, so the block was skipped before its id was
		// ever compared, and `block=<that-table-id>` wrongly 400'd.
		var idOnly struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &idOnly); err == nil && idOnly.ID == id {
			return raw, true
		}

		// Recurse into the nesting sites. Rows are decoded as raw messages so a
		// plain-array row does not blow up the block decode; only object rows
		// (`{cells, modal?}`) can carry a modal, and only those are reached
		// into. `modal`'s `{param}` form has no `blocks` and simply recurses
		// over an empty slice.
		var nest struct {
			Modal *struct {
				Blocks []json.RawMessage `json:"blocks"`
			} `json:"modal"`
			Tabs []struct {
				Blocks []json.RawMessage `json:"blocks"`
			} `json:"tabs"`
			Rows []json.RawMessage `json:"rows"`
		}
		if err := json.Unmarshal(raw, &nest); err != nil {
			continue
		}
		if nest.Modal != nil {
			if found, ok := searchBlocks(nest.Modal.Blocks, id); ok {
				return found, true
			}
		}
		for _, tab := range nest.Tabs {
			if found, ok := searchBlocks(tab.Blocks, id); ok {
				return found, true
			}
		}
		for _, rawRow := range nest.Rows {
			var row struct {
				Modal *struct {
					Blocks []json.RawMessage `json:"blocks"`
				} `json:"modal"`
			}
			if err := json.Unmarshal(rawRow, &row); err != nil || row.Modal == nil {
				continue // plain-array row, or an object row with no modal
			}
			if found, ok := searchBlocks(row.Modal.Blocks, id); ok {
				return found, true
			}
		}
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// Error responses
// ---------------------------------------------------------------------------

// rejectAppRequest writes an /apps/* error that never entered handleApp (a
// dirty path, an unroutable app URL), still producing an access-log line and
// a counter increment so those refusals are observable.
func (s *Server) rejectAppRequest(w http.ResponseWriter, r *http.Request, status int, kind errorKind, msg string) {
	requestID := uuid.NewString()
	r = r.WithContext(withRequestID(r.Context(), requestID))
	arw := &appResponseWriter{ResponseWriter: w}
	writeError(arw, r, status, kind, msg)
	s.emitAccessLog(&accessLogRecord{
		RequestID: requestID,
		Method:    r.Method,
		ClientIP:  s.clientIP(r),
		Status:    arw.status,
		Outcome:   arw.outcome(),
	})
}

// writeError renders the error envelope: JSON for `Accept:
// application/json`, a minimal HTML page otherwise (spec §8).
//
// request_id comes from the request context when the app route's middleware
// stamped one (withRequestID), so the id in this envelope, the render log
// record, and app-worker's access log are the SAME value — that identity is
// what makes `datuplet apps logs --request-id` work (spec §6.6). A fresh UUID
// is minted only when nothing stamped the context.
//
// It also stamps the kind on an *appResponseWriter, which is how the access
// log's outcome and the Prometheus label stay identical to the envelope's
// kind by construction rather than by discipline.
func writeError(w http.ResponseWriter, r *http.Request, status int, kind errorKind, msg string) {
	requestID := requestIDFromContext(r.Context())
	if requestID == "" {
		requestID = uuid.NewString()
	}
	if arw, ok := w.(*appResponseWriter); ok {
		arw.kind = kind
	}

	if wantsJSON(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(errorBody{Error: msg, Kind: kind, RequestID: requestID})
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The HTML error page is HTML served from this origin, so it carries the
	// shell's CSP too (spec §6.4 defence in depth).
	w.Header().Set("Content-Security-Policy", shellCSP)
	w.WriteHeader(status)
	fmt.Fprintf(w, "<!doctype html><html><head><title>%s</title></head>"+
		"<body><h1>%s</h1><p>request_id: %s</p></body></html>",
		kind, html.EscapeString(msg), html.EscapeString(requestID))
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

// Serve boots app-worker: validates the boot configuration, builds the
// pipeline-api client, compiles the render engine (pkg/appengine) via
// newEngine, marks readiness, THEN serves cfg.ListenAddr until ctx is
// canceled.
//
// The ordering is the load-bearing contract: /readyz cannot report 200 before
// the engine has compiled (spec §7), so a fresh pod never attempts to render
// on an uncompiled engine.
//
// Boot FAILS LOUDLY on a missing/unreadable/empty cookie-key or
// service-token file, or an unset DATUPLET_API_URL. Each of those leaves the
// worker able to serve nothing but `unavailable` — authenticate and render
// both fail closed without them — and a pod that answers every request with
// 503 is indistinguishable from an outage. Crash-looping with a precise
// message is the only honest behaviour, and it matches how the platform
// treats every other missing critical secret.
//
// newEngine is production-wired to appengine.NewEngine by
// cmd/app-worker/main.go; tests inject a fake so the boot wiring is assertable
// without paying the real WASM compile cost.
// newConfiguredAPIClient builds the pipeline-api client with the operator's
// configured bundle-size ceiling wired in. Serve calls this so an operator who
// sets appWorker.render.bundleMaxBytes BELOW the 5 MB hard cap is honored:
// NewAPIClient otherwise defaults maxBundleBytes to the hard cap, silently
// overriding a lower configured value. Split out from Serve so the wiring is
// unit-testable at the Serve seam without binding a listener. A non-positive
// BundleMaxBytes (never produced by LoadConfig, which clamps) leaves the
// client default rather than setting a zero cap that would reject every
// bundle.
func newConfiguredAPIClient(cfg Config, serviceToken string) *APIClient {
	var opts []Option
	if cfg.Render.BundleMaxBytes > 0 {
		opts = append(opts, WithMaxBundleBytes(int64(cfg.Render.BundleMaxBytes)))
	}
	return NewAPIClient(cfg.APIURL, serviceToken, opts...)
}

func Serve(ctx context.Context, cfg Config, newEngine EngineConstructor) error {
	if strings.TrimSpace(cfg.APIURL) == "" {
		return fmt.Errorf("appworker: %s is not set — app-worker cannot reach pipeline-api", EnvAPIURL)
	}
	cookieKey, err := readSecretFile(cfg.CookieKeyFile, "viewer session cookie key", EnvCookieKeyFile)
	if err != nil {
		return err
	}
	serviceToken, err := readSecretFile(cfg.ServiceTokenFile, "pipeline-api service token", EnvServiceTokenFile)
	if err != nil {
		return err
	}

	client := newConfiguredAPIClient(cfg, serviceToken)
	defer client.Close()

	engine, err := newEngine(ctx, cfg.MemoryPages())
	if err != nil {
		return fmt.Errorf("appworker: engine boot: %w", err)
	}
	defer func() { _ = engine.Close(context.Background()) }()

	srv := NewServer(cfg, engine,
		WithAuthAPI(client),
		WithRenderAPI(client),
		WithResolveAPI(client),
		WithCookieKey(cookieKey),
	)
	// The engine is compiled and every dependency is wired: only now may
	// /readyz report 200.
	srv.markEngineReady()

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("appworker: shutdown: %w", err)
		}
		<-errCh
		return nil
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("appworker: listen: %w", err)
		}
		return nil
	}
}

// readSecretFile reads a mounted secret file, trimming the trailing newline a
// Secret volume or `echo` leaves behind. Unset path, unreadable file, and
// empty contents are all boot errors naming both the credential and the env
// var that configures it — see Serve's doc for why this is fatal rather than
// degraded.
func readSecretFile(path, what, envVar string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("appworker: %s is not configured (set %s)", what, envVar)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("appworker: cannot read %s from %s (%s): %w", what, path, envVar, err)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", fmt.Errorf("appworker: %s file %s (%s) is empty", what, path, envVar)
	}
	return value, nil
}
