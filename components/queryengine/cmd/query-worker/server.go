// No build tag: server.go depends only on untagged types (queryengine.Request,
// queryengine.Result, queryengine.ErrTimeout, queryengine.ErrResultTooLarge from
// errors.go + types.go) and the Runner interface defined here. The actual
// queryengine.Run function (duckdb_arrow-tagged) is wired only in main.go.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/datuplet/datuplet/components/queryengine"
)

// Runner is the engine seam: lets server.go be tested with a fake without
// importing duckdb_arrow-tagged code.
type Runner interface {
	Run(context.Context, queryengine.Request) (*queryengine.Result, error)
}

// RunnerFunc adapts a plain function to the Runner interface.
type RunnerFunc func(context.Context, queryengine.Request) (*queryengine.Result, error)

// Run implements Runner.
func (f RunnerFunc) Run(ctx context.Context, r queryengine.Request) (*queryengine.Result, error) {
	return f(ctx, r)
}

// workerConfig holds all configuration values read from env by main.go.
// Zero values apply the defaults noted in each field comment.
type workerConfig struct {
	ListenAddr     string
	JWKSUrl        string
	LakekeeperURL  string
	MaxConcurrency int // default 2
	MemoryLimit    string
	TempDir        string // default /scratch
	MaxTempSize    string
	Threads        int
	MaxTimeoutS    int // default 300
	MaxRows        int // default 10000
	MaxBytes       int // default 10MiB
}

// queryServer is the HTTP handler for the query-worker. It owns the semaphore,
// the token verifier, and the Runner.
type queryServer struct {
	verifier *tokenVerifier
	runner   Runner
	cfg      workerConfig
	sem      chan struct{} // buffered-channel semaphore, size = MaxConcurrency
	mux      *http.ServeMux
}

// newQueryServer constructs a queryServer wired with the given verifier, runner,
// and config. The semaphore is sized by cfg.MaxConcurrency (must be > 0).
func newQueryServer(verifier *tokenVerifier, runner Runner, cfg workerConfig) *queryServer {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 2
	}
	if cfg.MaxTimeoutS <= 0 {
		cfg.MaxTimeoutS = 300
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 10000
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10 * 1024 * 1024
	}
	if cfg.TempDir == "" {
		cfg.TempDir = "/scratch"
	}

	srv := &queryServer{
		verifier: verifier,
		runner:   runner,
		cfg:      cfg,
		sem:      make(chan struct{}, cfg.MaxConcurrency),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealthz)
	mux.HandleFunc("POST /internal/query", srv.handleQuery)
	srv.mux = mux

	return srv
}

// ServeHTTP implements http.Handler, routing via the internal ServeMux.
func (s *queryServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleHealthz returns 200 "ok" unconditionally (no auth).
func (s *queryServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// queryRequest is the JSON body for POST /internal/query.
type queryRequest struct {
	SQL        string `json:"sql"`
	CatalogJWT string `json:"catalog_jwt"`
	Warehouse  string `json:"warehouse"`
	TimeoutS   int    `json:"timeout_s"`
	MaxRows    int    `json:"max_rows"`
	MaxBytes   int    `json:"max_bytes"`
}

// errorResponse is the JSON body for all error responses.
type errorResponse struct {
	Error string `json:"error"`
	Kind  string `json:"kind"`
}

// writeError writes a JSON error response. kind is the machine-readable kind field.
func writeError(w http.ResponseWriter, status int, kind, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: message, Kind: kind})
}

const maxBodyBytes = 1 * 1024 * 1024 // 1MiB

// handleQuery handles POST /internal/query.
func (s *queryServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// 1. Verify the internal-query JWT.
	sub, jti, err := s.extractAndVerify(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}

	// 2. Admission: acquire semaphore non-blocking.
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	default:
		log.Printf("query-worker: capacity exhausted sub=%s jti=%s", sub, jti)
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "capacity", "server at capacity, try again shortly")
		return
	}

	// 3. Parse and validate the request body.
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// MaxBytesReader returns *http.MaxBytesError when the request body
		// exceeds maxBodyBytes. Return 413 with kind="request_too_large" so
		// callers can distinguish an oversized body from a malformed one.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large",
				fmt.Sprintf("request body exceeds %d bytes", maxBodyBytes))
			return
		}
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid request body: %v", err))
		return
	}

	// Required fields.
	if req.SQL == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "sql is required")
		return
	}
	if req.CatalogJWT == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "catalog_jwt is required")
		return
	}
	if req.Warehouse == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "warehouse is required")
		return
	}

	// 4. Build queryengine.Request: clamp/apply worker config limits.
	//    Timeout: body value (>0) clamped to MaxTimeoutS; 0 or negative → max.
	timeoutS := req.TimeoutS
	if timeoutS <= 0 || timeoutS > s.cfg.MaxTimeoutS {
		timeoutS = s.cfg.MaxTimeoutS
	}
	//    max_rows: body value (>0) clamped to ceiling; 0 → ceiling.
	maxRows := req.MaxRows
	if maxRows <= 0 || maxRows > s.cfg.MaxRows {
		maxRows = s.cfg.MaxRows
	}
	//    max_bytes: body value (>0) clamped to ceiling; 0 → ceiling.
	maxBytes := req.MaxBytes
	if maxBytes <= 0 || maxBytes > s.cfg.MaxBytes {
		maxBytes = s.cfg.MaxBytes
	}

	engineReq := queryengine.Request{
		SQL:           req.SQL,
		CatalogJWT:    req.CatalogJWT,
		Warehouse:     req.Warehouse,
		LakekeeperURL: s.cfg.LakekeeperURL,
		Timeout:       time.Duration(timeoutS) * time.Second,
		MaxRows:       maxRows,
		MaxBytes:      maxBytes,
		MemoryLimit:   s.cfg.MemoryLimit,
		TempDir:       s.cfg.TempDir,
		MaxTempSize:   s.cfg.MaxTempSize,
		Threads:       s.cfg.Threads,
	}

	// 5. Run the query.
	// SECURITY: never log the SQL or catalog_jwt. Audit carries sub+jti+kind+duration.
	result, runErr := s.runner.Run(r.Context(), engineReq)
	duration := time.Since(start)

	if runErr != nil {
		var status int
		var kind string
		switch {
		case errors.Is(runErr, queryengine.ErrTimeout):
			status = http.StatusRequestTimeout
			kind = "timeout"
		case errors.Is(runErr, queryengine.ErrResultTooLarge):
			status = http.StatusRequestEntityTooLarge
			kind = "result_too_large"
		default:
			status = http.StatusBadRequest
			kind = "sql_error"
		}
		log.Printf("query-worker: sub=%s jti=%s kind=%s duration=%s err=%v", sub, jti, kind, duration, runErr)
		writeError(w, status, kind, runErr.Error())
		return
	}

	log.Printf("query-worker: sub=%s jti=%s kind=ok duration=%s rows=%d", sub, jti, duration, len(result.Rows))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}

// extractAndVerify pulls the Bearer token from the Authorization header and
// verifies it. Returns sub+jti on success, error on failure.
func (s *queryServer) extractAndVerify(r *http.Request) (sub, jti string, err error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", "", errors.New("missing Authorization header")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(authHeader, prefix) {
		return "", "", errors.New("Authorization header is not Bearer")
	}
	tokenStr := strings.TrimPrefix(authHeader, prefix)
	if tokenStr == "" {
		return "", "", errors.New("empty bearer token")
	}
	return s.verifier.Verify(r.Context(), tokenStr)
}
