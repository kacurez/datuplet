// Command app-worker is the RFC 028 user-apps render service: a stateless
// HTTP service that renders user-authored dashboard apps inside per-request
// WASM sandboxes (pkg/appengine) and serves the resulting OutputDoc.
//
// All configuration is via environment variables (pkg/appworker.LoadConfig);
// see that package for the full list and their spec §7 defaults/caps. The
// render engine (pkg/appengine) is compiled once at boot, before the process
// is considered ready to serve — a fresh pod must never attempt to render on
// an uncompiled engine (spec §7 "Rollout requirements").
//
// Signal handling mirrors cmd/pipeline-observer and
// components/queryengine/cmd/query-worker: SIGTERM/SIGINT trigger graceful
// HTTP shutdown.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/datuplet/datuplet/pkg/appengine"
	"github.com/datuplet/datuplet/pkg/appworker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "app-worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := appworker.LoadConfig()

	fmt.Println("app-worker starting...")
	fmt.Printf("  Listen:              %s\n", cfg.ListenAddr)
	fmt.Printf("  API URL:             %s\n", cfg.APIURL)
	fmt.Printf("  Render timeout:      %ds (cap %ds)\n", cfg.Render.TimeoutS, cfg.Render.MaxTimeoutS)
	fmt.Printf("  Render memory:       %d MiB (cap %d MiB) = %d wazero pages\n",
		cfg.Render.MemoryMiB, cfg.Render.MaxMemoryMiB, cfg.MemoryPages())
	fmt.Printf("  Queries/render:      %d (cap %d)\n", cfg.Render.QueriesPerRender, cfg.Render.MaxQueriesPerRender)
	fmt.Printf("  OutputDoc max bytes: %d\n", cfg.Render.OutputDocMaxBytes)
	fmt.Printf("  Bundle max bytes:    %d\n", cfg.Render.BundleMaxBytes)
	fmt.Printf("  Per-app in-flight:   %d\n", cfg.Render.PerAppInflight)
	fmt.Printf("  Concurrency:         %d\n", cfg.Render.Concurrency)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// newEngine adapts appengine.NewEngine (which returns the concrete
	// *appengine.Engine) to appworker.EngineConstructor's interface return
	// type. Compiling the engine happens inside appworker.Serve, before the
	// HTTP listener starts — see that function's doc comment for the
	// readiness-ordering contract W6 wires up.
	newEngine := func(ctx context.Context, memoryPages uint32) (appworker.Engine, error) {
		return appengine.NewEngine(ctx, memoryPages)
	}

	fmt.Println("  Compiling render engine...")
	if err := appworker.Serve(ctx, cfg, newEngine); err != nil {
		return err
	}

	fmt.Println("app-worker shut down cleanly.")
	return nil
}
