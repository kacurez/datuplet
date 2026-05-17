// Package main is the pipeline-observer server entrypoint.
//
// pipeline-observer is the single-writer to the pipeline-api `runs` table.
// It runs a controller-runtime informer watching PipelineRun CRDs and
// mirrors observed status into Postgres via the coalesce + DB-updater
// chain (pkg/pipelineapi/k8s/{observer,coalesce,db_updater}.go).
//
// Extracted from pipeline-api so pipeline-api can scale to N replicas (HTTP
// only, no shared informer state). Pipeline-observer runs at exactly 1 replica
// — no leader election. If the observer Pod is down, run-status DB rows drift
// from K8s truth; pipeline runs themselves still execute (operator + DG are
// independent). The `pipelineapi_observer_lag_seconds` metric gives operators
// an objective signal for this state.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/datuplet/datuplet/pkg/pipelineapi"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/metrics"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "pipeline-observer: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("pipeline-observer", flag.ExitOnError)
	addr := fs.String("addr", "", "HTTP listen address for /healthz + /metrics (also PIPELINE_OBSERVER_ADDR; default :8081)")
	cacheSyncTimeout := fs.Duration("cache-sync-timeout", 2*time.Minute, "Max time to wait for initial informer cache sync")
	shutdownGrace := fs.Duration("shutdown-grace", 10*time.Second, "Graceful shutdown timeout on SIGTERM")
	_ = fs.Parse(os.Args[1:])

	cfg := pipelineapi.LoadConfig()
	if *addr == "" {
		*addr = envOr("PIPELINE_OBSERVER_ADDR", ":8081")
	} else {
		// CLI flag wins over env.
	}

	if cfg.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("pipeline-observer starting...")
	fmt.Printf("  Addr: %s\n", *addr)
	fmt.Printf("  In-cluster: %v\n", cfg.InCluster)
	if !cfg.InCluster && cfg.KubeconfigPath != "" {
		fmt.Printf("  Kubeconfig: %s\n", cfg.KubeconfigPath)
	}

	// Postgres pool — ping only; migrations are owned by the
	// pipeline-api-migrate pre-install Job. Same pool semantics as the
	// pipeline-api binary's pool (defaults applied in pipelineapidb.Open).
	pool, err := pipelineapidb.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}
	fmt.Println("  DB: connected")

	// Coalesce wraps the DB-writing updater: identity-write drops happen
	// in memory before any DB round-trip. DELETE events from the
	// informer call coalesce.Forget synchronously.
	coalesce := pkg8s.NewCoalescedUpdater(pkg8s.NewDBRunUpdater(pool))

	// Build a rest.Config; controller-runtime's Manager owns the informer
	// cache + watch lifecycle.
	restCfg, err := pkg8s.NewRESTConfig(pkg8s.ClientOpts{
		InCluster:      cfg.InCluster,
		KubeconfigPath: cfg.KubeconfigPath,
	})
	if err != nil {
		return fmt.Errorf("rest config: %w", err)
	}
	obs, err := pkg8s.NewObserver(restCfg, coalesce, coalesce.Forget)
	if err != nil {
		return fmt.Errorf("observer: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 48h TTL backstop for missed DELETE events across informer
	// reconnects; sweep every 10m.
	go coalesce.RunJanitor(ctx, 10*time.Minute, 48*time.Hour)

	// Observer goroutine. Exit causes the process to exit via run() returning.
	observerErrCh := make(chan error, 1)
	go func() {
		err := obs.Start(ctx)
		if err != nil && err != context.Canceled {
			observerErrCh <- err
			return
		}
		observerErrCh <- nil
	}()

	// Block on initial cache sync (bootstrap ordering: observer must be
	// cache-synced before handling events). The readiness probe (/healthz)
	// returns 503 until this completes; the metrics endpoint is fine to
	// serve during sync.
	syncCtx, syncCancel := context.WithTimeout(ctx, *cacheSyncTimeout)
	cacheSynced := obs.WaitForCacheSync(syncCtx)
	syncCancel()
	if !cacheSynced {
		return fmt.Errorf("informer cache failed to sync within %s", *cacheSyncTimeout)
	}
	fmt.Println("  Observer: informer cache synced")

	// Seed observer-lag with "alive at cache-sync" so the metric stays
	// small even in an idle cluster where no reconcile events fire.
	// Without this, ObserverLag() returns 0 until the first reconcile;
	// a quiet observer would look unhealthy.
	metrics.RecordReconcile()

	// pipelineapi_informer_cache_size gauge — sampled every 15s from a
	// background goroutine. Not event-driven: per-event counters cover
	// reconciles; this gauge spots cache-growth trends.
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := obs.CacheSize(ctx)
				if err == nil {
					metrics.InformerCacheSize.Set(float64(n))
				}
			}
		}
	}()

	// pipelineapi_observer_lag_seconds gauge — sampled every 30s.
	// Updated by reconcileOne (event-driven) AND once at cache sync (liveness
	// seed). The sampler converts the stored timestamp to a seconds delta so
	// the metric grows during outages.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				metrics.ObserverLagSeconds.Set(metrics.ObserverLag().Seconds())
			}
		}
	}()

	// HTTP server for /healthz + /metrics.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		// Informer cache synced is the readiness signal. The DB pool
		// connectivity was verified at startup; pgxpool reconnects on
		// its own under transient outage.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	httpErrCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			httpErrCh <- fmt.Errorf("http listen: %w", err)
			return
		}
		httpErrCh <- nil
	}()
	fmt.Printf("  HTTP: listening on %s (/healthz, /metrics)\n", *addr)

	// Wait for either: SIGTERM, observer error, or HTTP error.
	select {
	case <-sigCh:
		fmt.Println("\nReceived shutdown signal...")
	case err := <-observerErrCh:
		if err != nil {
			log.Printf("pipeline-observer: manager exited with error: %v", err)
		}
	case err := <-httpErrCh:
		if err != nil {
			log.Printf("pipeline-observer: http server exited with error: %v", err)
		}
	}

	// Graceful shutdown.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), *shutdownGrace)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	cancel()
	// Drain remaining goroutines.
	<-observerErrCh
	<-httpErrCh

	fmt.Println("pipeline-observer shut down cleanly.")
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
