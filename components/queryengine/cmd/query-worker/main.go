//go:build duckdb_arrow

// Package main is the query-worker binary entrypoint (RFC 022 Task 2.3).
//
// The query-worker is a small HTTP service:
//
//	pipeline-api → POST /internal/query (internal-query JWT)
//	            → queryengine.Run (embedded DuckDB + lakekeeper catalog)
//	            → JSON queryengine.Result
//
// All configuration is via environment variables; see workerConfig and the env
// names in run() for the full list.  The binary fails fast at boot if required
// variables are absent — matching the Data Gateway sidecar's fail-closed posture.
//
// Signal handling mirrors cmd/pipeline-observer: SIGTERM or SIGINT triggers
// graceful HTTP shutdown with a 15s drain, then the process exits.
//
// Metrics: GET /metrics (default Prometheus registry via promhttp) exposes
// query_worker_inflight, query_worker_capacity_slots, and
// query_worker_admission_total{outcome} alongside /healthz (RFC 025 §5.3).
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/datuplet/datuplet/components/queryengine"
	"github.com/datuplet/datuplet/pkg/datagateway/jwks"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "query-worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// ---- config from environment -----------------------------------------
	// Env naming convention: DATUPLET_* prefix, matching Data Gateway sidecar
	// (DATUPLET_LAKEKEEPER_URL is shared with pkg/pipeline and pkg/pipelineapi)
	// and the DATUPLET_QUERY_WORKER_* namespace for worker-specific knobs.
	cfg := workerConfig{
		ListenAddr:     envOr("DATUPLET_QUERY_WORKER_LISTEN", ":8090"),
		LakekeeperURL:  os.Getenv("DATUPLET_LAKEKEEPER_URL"),
		JWKSUrl:        os.Getenv("DATUPLET_QUERY_WORKER_JWKS_URL"),
		MemoryLimit:    os.Getenv("DATUPLET_QUERY_WORKER_MEMORY_LIMIT"),
		TempDir:        envOr("DATUPLET_QUERY_WORKER_TEMP_DIR", "/scratch"),
		MaxTempSize:    os.Getenv("DATUPLET_QUERY_WORKER_MAX_TEMP_SIZE"),
		MaxConcurrency: envInt("DATUPLET_QUERY_WORKER_CONCURRENCY", 2),
		Threads:        envInt("DATUPLET_QUERY_WORKER_THREADS", 0), // 0 = engine default (2)
		MaxTimeoutS:    envInt("DATUPLET_QUERY_WORKER_MAX_TIMEOUT_S", 300),
		MaxRows:        envInt("DATUPLET_QUERY_WORKER_MAX_ROWS", 10000),
		MaxBytes:       envInt("DATUPLET_QUERY_WORKER_MAX_BYTES", 10*1024*1024),
	}

	// Required env vars: fail fast with a clear message.
	if cfg.JWKSUrl == "" {
		return fmt.Errorf("DATUPLET_QUERY_WORKER_JWKS_URL is required")
	}
	if cfg.LakekeeperURL == "" {
		return fmt.Errorf("DATUPLET_LAKEKEEPER_URL is required")
	}

	// ---- print startup banner (mirrors pipeline-observer) -----------------
	fmt.Println("query-worker starting...")
	fmt.Printf("  Listen:       %s\n", cfg.ListenAddr)
	fmt.Printf("  JWKS URL:     %s\n", cfg.JWKSUrl)
	fmt.Printf("  Lakekeeper:   %s\n", cfg.LakekeeperURL)
	fmt.Printf("  Concurrency:  %d\n", cfg.MaxConcurrency)
	fmt.Printf("  MaxTimeoutS:  %d\n", cfg.MaxTimeoutS)
	fmt.Printf("  MaxRows:      %d\n", cfg.MaxRows)
	fmt.Printf("  MaxBytes:     %d\n", cfg.MaxBytes)
	fmt.Printf("  Metrics:      /metrics\n")

	// ---- wire JWKS client + token verifier --------------------------------
	// jwks.Client.KeyFor holds a mutex across HTTP re-fetches, which makes it
	// unsafe for per-request use at concurrency.  Wrap it in a cachingKeyProvider
	// (keycache.go) so only kid-misses ever reach the JWKS endpoint; all
	// subsequent requests for the same kid are served from an in-memory map under
	// an RLock.
	jwksClient := jwks.NewClient(cfg.JWKSUrl, nil)
	keyCache := newCachingKeyProvider(jwksClient)
	verifier := newTokenVerifier(keyCache)

	// ---- wire Runner: RunnerFunc adapts queryengine.Run -------------------
	// main.go is tagged duckdb_arrow so this import is safe.
	runner := RunnerFunc(queryengine.Run)

	// ---- build HTTP server ------------------------------------------------
	srv := newQueryServer(verifier, runner, cfg)
	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// ---- signal handling (mirrors pipeline-observer) ---------------------
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	httpErrCh := make(chan error, 1)
	go func() {
		fmt.Printf("  HTTP: listening on %s (/healthz, /metrics, /internal/query)\n", cfg.ListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			httpErrCh <- fmt.Errorf("http listen: %w", err)
			return
		}
		httpErrCh <- nil
	}()

	select {
	case <-sigCh:
		fmt.Println("\nReceived shutdown signal...")
		// Graceful shutdown: drain in-flight requests, then exit.
		// After Shutdown returns, ListenAndServe exits with http.ErrServerClosed,
		// which the goroutine filters to nil and sends on httpErrCh — drain it.
		const drainTimeout = 15 * time.Second
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), drainTimeout)
		defer shutdownCancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "query-worker: shutdown: %v\n", err)
		}
		// Drain the http goroutine (sends nil via the ErrServerClosed filter).
		<-httpErrCh

	case err := <-httpErrCh:
		// Server stopped on its own. Non-nil means a startup/listen error.
		// Nil means ListenAndServe returned cleanly (e.g. already closed) —
		// nothing left to shut down; return immediately to avoid blocking forever.
		if err != nil {
			return err
		}
	}

	fmt.Println("query-worker shut down cleanly.")
	return nil
}

// envOr returns the value of key, or fallback if the key is unset/empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envInt parses key as a decimal integer, returning fallback on missing/invalid.
func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query-worker: env %s=%q is not a valid integer, using default %d\n", key, v, fallback)
		return fallback
	}
	return n
}
