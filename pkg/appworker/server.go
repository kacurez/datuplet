package appworker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Engine is the subset of *appengine.Engine's behavior app-worker's boot
// path depends on. Defining it as an interface (rather than importing
// *appengine.Engine directly) lets tests inject a fake engine constructor
// and assert Serve's boot-time wiring without paying the real ~0.25s WASM
// compile cost (task-E1-report.md's "NewEngine is expensive-ish" note).
// *appengine.Engine satisfies this interface structurally.
//
// W1-W6 will extend server.go's actual render path against
// *appengine.Engine.Render directly (or grow this interface) once the
// render request plumbing (bundle resolution, query client, OutputDoc
// validation) exists; this skeleton only needs the boot call + orderly
// shutdown.
type Engine interface {
	Close(ctx context.Context) error
}

// EngineConstructor builds the render engine. Production wiring
// (cmd/app-worker/main.go) passes a thin adapter over appengine.NewEngine;
// tests inject a fake to assert the boot-time call.
type EngineConstructor func(ctx context.Context, memoryPages uint32) (Engine, error)

// errorKind enumerates the app-worker error envelope's `kind` values
// (contract-and-constraints.md, spec §8). Only errKindUnavailable is used by
// this skeleton; later tasks will produce the rest.
type errorKind string

const (
	errKindUnavailable errorKind = "unavailable"
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
}

// NewServer builds the stub Server. engine is stored for later tasks
// (orderly shutdown, and eventually Render calls); W0 does not call it.
func NewServer(cfg Config, engine Engine) *Server {
	s := &Server{cfg: cfg, engine: engine}
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
// application/json`, a minimal HTML page otherwise (spec §8). request_id is
// a fresh UUID per response (this skeleton does not thread a request-scoped
// ID through middleware yet).
func writeError(w http.ResponseWriter, r *http.Request, status int, kind errorKind, msg string) {
	requestID := uuid.NewString()

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
