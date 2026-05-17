package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	"gopkg.in/yaml.v3"
)

// GatewayConfig is the configuration file format for the datagateway.
type GatewayConfig struct {
	Mode          string `yaml:"mode"`
	RunID         string `yaml:"run_id"`
	DataDir       string `yaml:"data_dir"`
	LakekeeperURL string `yaml:"lakekeeper_url,omitempty"` // Lakekeeper REST catalog base URL
	// PipelineAPIJWKSURL is the JWKS endpoint pipeline-api serves.
	// The operator injects this whenever LakekeeperURL is set; DG validates
	// the mounted run-token JWT against it at boot. NewServerV2 fail-closes
	// when this is empty but RunTokenPath is set.
	PipelineAPIJWKSURL string `yaml:"pipeline_api_jwks_url,omitempty"`
	// SecretsDir is the directory where $[name] secret references are resolved from.
	// One file per name; file contents == value. Leave empty to disable resolution.
	SecretsDir string `yaml:"secrets_dir,omitempty"`

	// S3 credentials for StorageBootstrap (native S3 components like DuckDB)
	S3Endpoint   string `yaml:"s3_endpoint,omitempty"`
	S3AccessKey  string `yaml:"s3_access_key,omitempty"`
	S3SecretKey  string `yaml:"s3_secret_key,omitempty"`
	S3Region     string `yaml:"s3_region,omitempty"`
	S3BucketName string `yaml:"s3_bucket_name,omitempty"`

	// Bucket-based access control (top-level)
	InputBuckets  []string                        `yaml:"input_buckets,omitempty"`
	OutputBuckets []string                        `yaml:"output_buckets,omitempty"`
	InputTables   []GatewayInputTableConfig       `yaml:"input_tables,omitempty"`
	OutputTables  []datagateway.OutputTableConfig `yaml:"output_tables,omitempty"`
	DefaultBucket    string `yaml:"default_bucket,omitempty"`
	DefaultWriteMode string `yaml:"default_write_mode,omitempty"`

	// Gateway-level settings (shared between modes)
	Gateway struct {
		ChunkSize      int64 `yaml:"chunk_size"`       // Component chunk size (default: 1MB)
		BufferSize     int64 `yaml:"buffer_size"`      // Memory buffer before flush (default: 10MB)
		RowGroupSize   int64 `yaml:"row_group_size"`   // Parquet row group size (default: BufferSize)
		TargetFileSize int64 `yaml:"target_file_size"` // Parquet file rotation size (default: 128MB)
	} `yaml:"gateway"`

	Component struct {
		Inputs  map[string]string `yaml:"inputs"`
		Config  map[string]any    `yaml:"config"`

		// Bucket-based outputs (new API)
		Outputs struct {
			DefaultBucket    string                    `yaml:"default_bucket,omitempty"`
			DefaultWriteMode string                    `yaml:"default_write_mode,omitempty"`
			Buckets          []GatewayBucketConfig     `yaml:"buckets,omitempty"`
			Tables           []GatewayTableConfig      `yaml:"tables,omitempty"`
			Processors       []datagateway.ProcessorConfig `yaml:"processors,omitempty"`
		} `yaml:"outputs,omitempty"`

		// Legacy outputs (deprecated)
		LegacyOutputs map[string]GatewayOutputConfig `yaml:"legacy_outputs,omitempty"`
	} `yaml:"component"`

	Local struct {
		InputFormat  string `yaml:"input_format"`
		OutputFormat string `yaml:"output_format"`
		ChunkSize    int64  `yaml:"chunk_size"` // Deprecated: use datagateway.chunk_size
	} `yaml:"local"`

	MinIO struct {
		Endpoint  string `yaml:"endpoint"`
		Bucket    string `yaml:"bucket"`
		AccessKey string `yaml:"access_key"`
		SecretKey string `yaml:"secret_key"`
		Region    string `yaml:"region"`
		UseSSL    bool   `yaml:"use_ssl"`
		ChunkSize int64  `yaml:"chunk_size"` // Deprecated: use datagateway.chunk_size
	} `yaml:"minio"`
}

// GatewayOutputConfig supports both string and full output config (legacy format).
type GatewayOutputConfig struct {
	Path       string                        `yaml:"path"`
	Processors []datagateway.ProcessorConfig `yaml:"processors,omitempty"`
}

// GatewayBucketConfig defines a bucket output with dynamic table creation.
type GatewayBucketConfig struct {
	Name      string `yaml:"name"`
	WriteMode string `yaml:"write_mode"`
}

// GatewayInputTableConfig defines a specific table input.
type GatewayInputTableConfig struct {
	Bucket           string `yaml:"bucket"`
	Table            string `yaml:"table"`
	SinceTimestampMs int64  `yaml:"since_timestamp_ms,omitempty"`
	SinceSnapshot    int64  `yaml:"since_snapshot,omitempty"`
}

// GatewayTableConfig defines a fixed table output.
type GatewayTableConfig struct {
	Name        string `yaml:"name"`
	Bucket      string `yaml:"bucket"`
	WriteMode   string `yaml:"write_mode"`
	LogicalName string `yaml:"logical_name,omitempty"`
}

// UnmarshalYAML handles both string and full config formats.
func (o *GatewayOutputConfig) UnmarshalYAML(node *yaml.Node) error {
	// Try simple string first
	if node.Kind == yaml.ScalarNode {
		o.Path = node.Value
		return nil
	}

	// Otherwise, decode as full spec
	type rawConfig GatewayOutputConfig
	return node.Decode((*rawConfig)(o))
}

func runGateway(mode, configPath, dataDir, addr, runTokenPath, podAnnotationsPath string) error {
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

	var cfg GatewayConfig

	// Load config file if provided
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return fmt.Errorf("failed to read config file: %w", err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// Apply gateway defaults
	if cfg.Gateway.ChunkSize <= 0 {
		cfg.Gateway.ChunkSize = 32 * 1024 * 1024 // 32MB
	}
	if cfg.Gateway.BufferSize <= 0 {
		cfg.Gateway.BufferSize = 64 * 1024 * 1024 // 64MB
	}
	if cfg.Gateway.RowGroupSize <= 0 {
		cfg.Gateway.RowGroupSize = cfg.Gateway.BufferSize // Default to BufferSize
	}
	if cfg.Gateway.TargetFileSize <= 0 {
		cfg.Gateway.TargetFileSize = 128 * 1024 * 1024 // 128MB
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

	// Encode component config as JSON
	componentCfgJSON, err := json.Marshal(cfg.Component.Config)
	if err != nil {
		componentCfgJSON = []byte("{}")
	}

	// Determine run ID (from config or generate)
	runID := cfg.RunID
	if runID == "" {
		runID = fmt.Sprintf("%s-%d", mode, time.Now().Unix())
	}

	// Create gateway config
	gatewayCfg := &datagateway.Config{
		RunID:            runID,
		ComponentName:    fmt.Sprintf("%s-component", mode),
		Inputs:           cfg.Component.Inputs,
		ComponentCfg:     componentCfgJSON,
		ChunkSize:        chunkSize,
		BufferSize:       cfg.Gateway.BufferSize,
		RowGroupSize:     cfg.Gateway.RowGroupSize,
		TargetFileSize:   cfg.Gateway.TargetFileSize,
		Backend:            be,
		LakekeeperURL:      cfg.LakekeeperURL,
		PipelineAPIJWKSURL: cfg.PipelineAPIJWKSURL,
		RunTokenPath:       runTokenPath,
		PodAnnotationsPath: podAnnotationsPath,
		SecretsDir:         cfg.SecretsDir,

		// S3 credentials for StorageBootstrap (native S3 components)
		S3Endpoint:   cfg.S3Endpoint,
		S3AccessKey:  cfg.S3AccessKey,
		S3SecretKey:  cfg.S3SecretKey,
		S3Region:     cfg.S3Region,
		S3BucketName: cfg.S3BucketName,

		// Bucket-based access control
		InputBuckets:  cfg.InputBuckets,
		OutputBuckets: cfg.OutputBuckets,
		DefaultBucket:    cfg.DefaultBucket,
		DefaultWriteMode: cfg.DefaultWriteMode,
	}

	// Convert input tables
	for _, t := range cfg.InputTables {
		gatewayCfg.InputTables = append(gatewayCfg.InputTables, datagateway.InputTableConfig{
			Bucket:           t.Bucket,
			Table:            t.Table,
			SinceTimestampMs: t.SinceTimestampMs,
			SinceSnapshot:    t.SinceSnapshot,
		})
	}

	// Convert output tables
	for _, t := range cfg.OutputTables {
		gatewayCfg.OutputTables = append(gatewayCfg.OutputTables, t)
	}

	// Populate bucket-based outputs (new API) or legacy outputs
	// Note: Top-level fields (cfg.InputBuckets, cfg.OutputBuckets, etc.) were already copied above.
	// This section handles nested Component.Outputs for backward compatibility.
	if cfg.Component.Outputs.DefaultBucket != "" || len(cfg.Component.Outputs.Buckets) > 0 || len(cfg.Component.Outputs.Tables) > 0 || len(cfg.Component.Outputs.Processors) > 0 {
		// Use bucket-based API from Component.Outputs (only if top-level not set)
		if gatewayCfg.DefaultBucket == "" {
			gatewayCfg.DefaultBucket = cfg.Component.Outputs.DefaultBucket
		}
		if gatewayCfg.DefaultWriteMode == "" {
			gatewayCfg.DefaultWriteMode = cfg.Component.Outputs.DefaultWriteMode
		}
		if len(cfg.Component.Outputs.Processors) > 0 {
			gatewayCfg.Processors = cfg.Component.Outputs.Processors
		}

		// Convert explicit bucket configs (only if not already set at top level)
		for _, b := range cfg.Component.Outputs.Buckets {
			gatewayCfg.OutputBuckets = append(gatewayCfg.OutputBuckets, b.Name)
		}

		// Convert explicit table configs (only if not already set at top level)
		for _, t := range cfg.Component.Outputs.Tables {
			gatewayCfg.OutputTables = append(gatewayCfg.OutputTables, datagateway.OutputTableConfig{
				Name:        t.Name,
				Bucket:      t.Bucket,
				WriteMode:   t.WriteMode,
				LogicalName: t.LogicalName,
			})
		}

		fmt.Printf("Bucket-based outputs: default_bucket=%s, default_write_mode=%s\n",
			gatewayCfg.DefaultBucket, gatewayCfg.DefaultWriteMode)
	} else if len(cfg.Component.LegacyOutputs) > 0 {
		// Fall back to legacy outputs
		gatewayOutputs := make(map[string]datagateway.OutputConfig)
		for name, out := range cfg.Component.LegacyOutputs {
			gatewayOutputs[name] = datagateway.OutputConfig{
				Path:       out.Path,
				Processors: out.Processors,
			}
		}
		gatewayCfg.Outputs = gatewayOutputs
		fmt.Printf("Legacy outputs: %v\n", gatewayOutputs)
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
