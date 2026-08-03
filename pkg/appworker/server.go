package appworker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"

	"github.com/datuplet/datuplet/pkg/appengine"
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

// Server is app-worker's HTTP handler. This skeleton (W0) is a stub: every
// request gets a 503 `unavailable` envelope. W1-W6 fill in the real
// `/apps/{pid}/{name}` render routes, viewer-token/cookie auth, and rate
// limiting against this same Server value.
type Server struct {
	cfg    Config
	engine Engine
	mux    *http.ServeMux

	// api is the pipeline-api client (W3) viewer auth calls into. Nil until
	// wired: authenticate fails closed with `unavailable` rather than
	// panicking or — far worse — waving requests through.
	api authAPI
	// rapi is the SAME pipeline-api client, seen through the render path's
	// method set (renderAPI in render.go). Two fields, not one union
	// interface: W4's auth doubles implement only the three auth methods, and
	// a union would force each to grow four irrelevant stubs. Nil until wired:
	// render fails closed with `unavailable`.
	rapi renderAPI
	// cookieKey is the HMAC key for the viewer session cookie (spec §5.3,
	// mounted via Config.CookieKeyFile). An empty key never authenticates a
	// cookie; see authenticate.
	cookieKey string
	// now is the injected clock: cookie expiry and every rate bucket read it,
	// so tests drive them deterministically instead of sleeping.
	now func() time.Time

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

// ServerOption configures a Server at construction. Production wiring
// (cmd/app-worker/main.go, W6) passes the real APIClient and the cookie key
// read from Config.CookieKeyFile; tests inject doubles and a fake clock.
type ServerOption func(*Server)

// WithAuthAPI injects the pipeline-api client viewer auth uses.
func WithAuthAPI(api authAPI) ServerOption { return func(s *Server) { s.api = api } }

// WithRenderAPI injects the pipeline-api client the render path uses (bundle
// fetch, impersonation mint, query proxy, render-log append). Production
// wiring passes the SAME *APIClient given to WithAuthAPI.
func WithRenderAPI(api renderAPI) ServerOption { return func(s *Server) { s.rapi = api } }

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

// NewServer builds the Server. engine is stored for later tasks (orderly
// shutdown, and eventually Render calls); W0 does not call it.
func NewServer(cfg Config, engine Engine, opts ...ServerOption) *Server {
	s := &Server{cfg: cfg, engine: engine, now: time.Now, poolAcquireWait: defaultPoolAcquireWait}
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
	mux.HandleFunc("/", s.handleUnavailable)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Every /apps/* response carries Referrer-Policy: no-referrer (spec
	// §5.3 token log-redaction); applied worker-wide since W0 has no routes
	// yet that would need to differ.
	w.Header().Set("Referrer-Policy", "no-referrer")
	s.mux.ServeHTTP(w, r)
}

// handleUnavailable is the W0 stub handler: render routes aren't wired yet,
// so every request gets the `unavailable` error envelope (spec §8: "bundle
// fetch/resolve failure" and "pipeline-api unavailable" both map here; an
// unimplemented route is a stricter subset of "unavailable").
func (s *Server) handleUnavailable(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusServiceUnavailable, errKindUnavailable, "app-worker: not yet implemented")
}

// writeError renders the error envelope: JSON for `Accept:
// application/json`, a minimal HTML page otherwise (spec §8).
//
// request_id comes from the request context when W6's middleware stamped one
// (withRequestID), so the id in this envelope, the render log record, and
// app-worker's structured log are the SAME value — that identity is what makes
// `datuplet apps logs --request-id` work (spec §6.6). A fresh UUID is minted
// only when nothing stamped the context.
func writeError(w http.ResponseWriter, r *http.Request, status int, kind errorKind, msg string) {
	requestID := requestIDFromContext(r.Context())
	if requestID == "" {
		requestID = uuid.NewString()
	}

	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(errorBody{Error: msg, Kind: kind, RequestID: requestID})
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<!doctype html><html><head><title>%s</title></head>"+
		"<body><h1>%s</h1><p>request_id: %s</p></body></html>",
		kind, msg, requestID)
}

// Serve boots app-worker: compiles the render engine (pkg/appengine) via
// newEngine, THEN starts serving cfg.ListenAddr until ctx is canceled. This
// ordering is the load-bearing contract: W6 wires the actual readiness
// gate, but the engine must already be compiled before that gate can ever
// flip, so a fresh pod never attempts to render on an uncompiled engine.
//
// newEngine is production-wired to appengine.NewEngine by
// cmd/app-worker/main.go; tests inject a fake (see
// TestServePassesMemoryPagesToEngine) so the boot-time
// cfg.MemoryPages()-to-NewEngine wiring is assertable without paying the
// real WASM compile cost.
func Serve(ctx context.Context, cfg Config, newEngine EngineConstructor) error {
	engine, err := newEngine(ctx, cfg.MemoryPages())
	if err != nil {
		return fmt.Errorf("appworker: engine boot: %w", err)
	}
	defer func() { _ = engine.Close(context.Background()) }()

	srv := NewServer(cfg, engine)
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
