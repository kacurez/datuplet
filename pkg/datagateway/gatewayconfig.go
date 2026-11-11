package datagateway

import (
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// GatewayConfig is the configuration file format for the
type GatewayConfig struct {
	RunID         string `yaml:"run_id"`
	ComponentName string `yaml:"component_name,omitempty"` // pipeline component instance name
	// TableMaintenance opens the maintenance RPCs (rewrite manifests, expire
	// snapshots, delete orphan files). Written by the controller only for
	// iceberg-maintenance stages; never read from user config.
	TableMaintenance bool   `yaml:"table_maintenance,omitempty"`
	LakekeeperURL    string `yaml:"lakekeeper_url,omitempty"` // Lakekeeper REST catalog base URL
	// PipelineAPIJWKSURL is the JWKS endpoint pipeline-api serves.
	// The operator injects this whenever LakekeeperURL is set; DG validates
	// the mounted run-token JWT against it at boot. NewServerV2 fail-closes
	// when this is empty but RunTokenPath is set.
	PipelineAPIJWKSURL string `yaml:"pipeline_api_jwks_url,omitempty"`
	// SecretsDir is the directory where $[name] secret references are resolved from.
	// One file per name; file contents == value. Leave empty to disable resolution.
	SecretsDir string `yaml:"secrets_dir,omitempty"`

	// Preview is set only for a preview run (see Config.Preview).
	Preview *PreviewConfig `yaml:"preview,omitempty"`

	// Bucket-based access control (top-level)
	InputBuckets     []string                  `yaml:"input_buckets,omitempty"`
	OutputBuckets    []string                  `yaml:"output_buckets,omitempty"`
	InputTables      []GatewayInputTableConfig `yaml:"input_tables,omitempty"`
	OutputTables     []OutputTableConfig       `yaml:"output_tables,omitempty"`
	DefaultBucket    string                    `yaml:"default_bucket,omitempty"`
	DefaultWriteMode string                    `yaml:"default_write_mode,omitempty"`

	// Gateway-level settings (shared between modes)
	Gateway struct {
		ChunkSize      int64 `yaml:"chunk_size"`       // Component chunk size (default: 1MB)
		BufferSize     int64 `yaml:"buffer_size"`      // Memory buffer before flush (default: 10MB)
		RowGroupSize   int64 `yaml:"row_group_size"`   // Parquet row group size (default: BufferSize)
		TargetFileSize int64 `yaml:"target_file_size"` // Parquet file rotation size (default: 128MB)
	} `yaml:"gateway"`

	Component struct {
		Inputs map[string]string `yaml:"inputs"`
		Config map[string]any    `yaml:"config"`

		// Bucket-based outputs (new API)
		Outputs struct {
			DefaultBucket    string                `yaml:"default_bucket,omitempty"`
			DefaultWriteMode string                `yaml:"default_write_mode,omitempty"`
			Buckets          []GatewayBucketConfig `yaml:"buckets,omitempty"`
			Tables           []GatewayTableConfig  `yaml:"tables,omitempty"`
			Processors       []ProcessorConfig     `yaml:"processors,omitempty"`
		} `yaml:"outputs,omitempty"`

		// Legacy outputs (deprecated)
		LegacyOutputs map[string]GatewayOutputConfig `yaml:"legacy_outputs,omitempty"`
	} `yaml:"component"`
}

// GatewayOutputConfig supports both string and full output config (legacy format).
type GatewayOutputConfig struct {
	Path       string            `yaml:"path"`
	Processors []ProcessorConfig `yaml:"processors,omitempty"`
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
	Name        string         `yaml:"name"`
	Bucket      string         `yaml:"bucket"`
	WriteMode   string         `yaml:"write_mode"`
	LogicalName string         `yaml:"logical_name,omitempty"`
	Columns     []ColumnConfig `yaml:"columns,omitempty"`
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

// LoadGatewayConfigFile reads and parses the gateway YAML at path and applies
// the gateway size defaults. An empty path yields a zero config with defaults.
func LoadGatewayConfigFile(path string) (*GatewayConfig, error) {
	var cfg GatewayConfig

	// Load config file if provided
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
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
	return &cfg, nil
}

// LoadConfigFile loads the gateway YAML into a Config (the datagateway
// binary's loader). The caller defaults an empty RunID or ComponentName.
func LoadConfigFile(configPath, runTokenPath, podAnnotationsPath string) (*Config, error) {
	cfg, err := LoadGatewayConfigFile(configPath)
	if err != nil {
		return nil, err
	}
	return cfg.ToConfig(runTokenPath, podAnnotationsPath), nil
}

// ToConfig converts the parsed YAML into the gateway Config.
func (cfg *GatewayConfig) ToConfig(runTokenPath, podAnnotationsPath string) *Config {
	// Encode component config as JSON
	componentCfgJSON, err := json.Marshal(cfg.Component.Config)
	if err != nil {
		componentCfgJSON = []byte("{}")
	}

	// Create gateway config
	gatewayCfg := &Config{
		RunID:              cfg.RunID,
		ComponentName:      cfg.ComponentName,
		TableMaintenance:   cfg.TableMaintenance,
		Inputs:             cfg.Component.Inputs,
		ComponentCfg:       componentCfgJSON,
		ChunkSize:          cfg.Gateway.ChunkSize,
		BufferSize:         cfg.Gateway.BufferSize,
		RowGroupSize:       cfg.Gateway.RowGroupSize,
		TargetFileSize:     cfg.Gateway.TargetFileSize,
		LakekeeperURL:      cfg.LakekeeperURL,
		PipelineAPIJWKSURL: cfg.PipelineAPIJWKSURL,
		RunTokenPath:       runTokenPath,
		PodAnnotationsPath: podAnnotationsPath,
		SecretsDir:         cfg.SecretsDir,
		Preview:            cfg.Preview,

		// Bucket-based access control
		InputBuckets:     cfg.InputBuckets,
		OutputBuckets:    cfg.OutputBuckets,
		DefaultBucket:    cfg.DefaultBucket,
		DefaultWriteMode: cfg.DefaultWriteMode,
	}

	// Convert input tables
	for _, t := range cfg.InputTables {
		gatewayCfg.InputTables = append(gatewayCfg.InputTables, InputTableConfig{
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
			gatewayCfg.OutputTables = append(gatewayCfg.OutputTables, OutputTableConfig{
				Name:        t.Name,
				Bucket:      t.Bucket,
				WriteMode:   t.WriteMode,
				LogicalName: t.LogicalName,
				Columns:     t.Columns,
			})
		}

		fmt.Printf("Bucket-based outputs: default_bucket=%s, default_write_mode=%s\n",
			gatewayCfg.DefaultBucket, gatewayCfg.DefaultWriteMode)
	} else if len(cfg.Component.LegacyOutputs) > 0 {
		// Fall back to legacy outputs
		gatewayOutputs := make(map[string]OutputConfig)
		for name, out := range cfg.Component.LegacyOutputs {
			gatewayOutputs[name] = OutputConfig{
				Path:       out.Path,
				Processors: out.Processors,
			}
		}
		gatewayCfg.Outputs = gatewayOutputs
		fmt.Printf("Legacy outputs: %v\n", gatewayOutputs)
	}

	return gatewayCfg
}
