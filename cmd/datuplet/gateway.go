package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
)

func runGateway(mode, configPath, dataDir, addr, runTokenPath, podAnnotationsPath string) error {
	iterationID := os.Getenv("DATUPLET_ITERATION_ID")
	if iterationID == "" {
		iterationID = "(none)"
	}
	runIDEnv := os.Getenv("RUN_ID")
	if runIDEnv == "" {
		runIDEnv = "(none)"
	}
	log.Printf("datuplet-gateway: starting %s iteration_id=%s run_id=%s",
		datagateway.GatewayBuildInfo(), iterationID, runIDEnv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived shutdown signal...")
		cancel()
	}()

	cfg, err := datagateway.LoadGatewayConfigFile(configPath)
	if err != nil {
		return err
	}

	var be backend.StorageBackend
	var chunkSize int64

	switch mode {
	case "local":
		// Apply defaults for local mode
		if dataDir != "" && dataDir != "./data" {
			cfg.DataDir = dataDir
		}
		if cfg.DataDir == "" {
			cfg.DataDir = "./data"
		}
		if cfg.Local.InputFormat == "" {
			cfg.Local.InputFormat = "auto"
		}
		if cfg.Local.OutputFormat == "" {
			cfg.Local.OutputFormat = "csv"
		}

		// Use mode-specific chunk_size if set (backward compat), otherwise datagateway.chunk_size
		chunkSize = cfg.Gateway.ChunkSize
		if cfg.Local.ChunkSize > 0 {
			chunkSize = cfg.Local.ChunkSize
		}
		be = backend.NewLocalBackend(backend.LocalConfig{
			DataDir:      cfg.DataDir,
			InputFormat:  cfg.Local.InputFormat,
			OutputFormat: cfg.Local.OutputFormat,
			ChunkSize:    cfg.Local.ChunkSize,
		})

		fmt.Println("Data Gateway starting in LOCAL mode")
		fmt.Printf("Data directory: %s\n", cfg.DataDir)

	case "minio":
		// Try to get credentials from environment first
		if cfg.MinIO.Endpoint == "" {
			cfg.MinIO.Endpoint = os.Getenv("MINIO_ENDPOINT")
		}
		if cfg.MinIO.Bucket == "" {
			cfg.MinIO.Bucket = os.Getenv("MINIO_BUCKET")
		}
		if cfg.MinIO.AccessKey == "" {
			cfg.MinIO.AccessKey = os.Getenv("MINIO_ACCESS_KEY")
			if cfg.MinIO.AccessKey == "" {
				cfg.MinIO.AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
			}
		}
		if cfg.MinIO.SecretKey == "" {
			cfg.MinIO.SecretKey = os.Getenv("MINIO_SECRET_KEY")
			if cfg.MinIO.SecretKey == "" {
				cfg.MinIO.SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
			}
		}

		// When lakekeeper is configured, storage paths come from the
		// catalog and credentials are vended STS — DG instantiates per-
		// table minio backends itself, so the static `be` stays nil.
		//
		// Local-file mode: when neither LakekeeperURL nor MinIO endpoint is
		// set AND DATUPLET_STORAGE_TYPE=filesystem, leave be=nil. NewServerV2
		// detects the env var and wires the SQLite resolver instead. A non-nil
		// static backend would cause a fatal in NewServerV2 when it also finds
		// a resolver.
		if cfg.LakekeeperURL != "" {
			fmt.Printf("Lakekeeper mode enabled: %s\n", cfg.LakekeeperURL)
			be = nil
			chunkSize = cfg.Gateway.ChunkSize
		} else if cfg.MinIO.Endpoint == "" && os.Getenv("DATUPLET_STORAGE_TYPE") == "filesystem" {
			// Local-file mode: SQLite resolver is wired by NewServerV2.
			be = nil
			chunkSize = cfg.Gateway.ChunkSize
			fmt.Printf("Data Gateway starting in LOCAL-FILE mode (SQLite catalog)\n")
		} else {
			// Direct MinIO mode - validate config
			if cfg.MinIO.Endpoint == "" || cfg.MinIO.Bucket == "" {
				return fmt.Errorf("MinIO endpoint and bucket are required when lakekeeper is not configured")
			}

			// Use mode-specific chunk_size if set (backward compat), otherwise datagateway.chunk_size
			chunkSize = cfg.Gateway.ChunkSize
			if cfg.MinIO.ChunkSize > 0 {
				chunkSize = cfg.MinIO.ChunkSize
			}
			minioBe, err := backend.NewMinIOBackend(backend.MinIOConfig{
				Endpoint:  cfg.MinIO.Endpoint,
				Bucket:    cfg.MinIO.Bucket,
				AccessKey: cfg.MinIO.AccessKey,
				SecretKey: cfg.MinIO.SecretKey,
				Region:    cfg.MinIO.Region,
				UseSSL:    cfg.MinIO.UseSSL,
				ChunkSize: cfg.MinIO.ChunkSize,
			})
			if err != nil {
				return fmt.Errorf("failed to create MinIO backend: %w", err)
			}
			be = minioBe

			fmt.Println("Data Gateway starting in MINIO mode")
			fmt.Printf("Endpoint: %s, Bucket: %s\n", cfg.MinIO.Endpoint, cfg.MinIO.Bucket)
		}

	default:
		return fmt.Errorf("unknown mode: %s", mode)
	}

	// Convert the parsed YAML into the gateway Config (shared loader).
	gatewayCfg := cfg.ToConfig(runTokenPath, podAnnotationsPath)

	// Determine run ID (from config or generate)
	if gatewayCfg.RunID == "" {
		gatewayCfg.RunID = fmt.Sprintf("%s-%d", mode, time.Now().Unix())
	}
	gatewayCfg.ComponentName = fmt.Sprintf("%s-component", mode)
	gatewayCfg.ChunkSize = chunkSize
	gatewayCfg.Backend = be

	// Start Pyroscope continuous profiling when DATUPLET_GATEWAY_PROFILING is set.
	// Returns nil when disabled. Defer the stop so it runs on graceful shutdown.
	if stopProfiling := datagateway.StartProfilingIfEnabled(); stopProfiling != nil {
		defer func() {
			if err := stopProfiling(); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: pyroscope stop error: %v\n", err)
			}
		}()
	}

	// Create and start v2 server
	server := datagateway.NewServerV2(gatewayCfg)

	if len(cfg.Component.Inputs) > 0 {
		fmt.Printf("Inputs: %v\n", cfg.Component.Inputs)
	}

	// Run server (blocks until shutdown or error)
	// ServeWithHTTP starts both gRPC (for control) and HTTP (for data transfer)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeWithHTTP(addr)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		fmt.Println("Shutting down datagateway...")
		if cerr := server.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "Warning: server Close error: %v\n", cerr)
		}
		// Wait for Serve to return after graceful stop so we don't drop in-flight RPCs.
		<-errCh
		return nil
	}
}
