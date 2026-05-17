package datagateway

import "github.com/datuplet/datuplet/pkg/datagateway/backend"

// Config contains the gateway server configuration.
type Config struct {
	RunID string `yaml:"run_id,omitempty"` // Unique run identifier for this pipeline execution
	ComponentName  string `yaml:"component_name,omitempty"`

	// Bucket-based access control
	InputBuckets  []string `yaml:"input_buckets,omitempty"`  // Buckets component can read from
	OutputBuckets []string `yaml:"output_buckets,omitempty"` // Buckets component can write to

	// Table-level access (for explicit table references)
	InputTables  []InputTableConfig  `yaml:"input_tables,omitempty"`  // Specific tables for read access
	OutputTables []OutputTableConfig `yaml:"output_tables,omitempty"` // Specific tables for write access

	// Default bucket for writes (if component uses defaultBucket mode)
	DefaultBucket    string `yaml:"default_bucket,omitempty"`
	DefaultWriteMode string `yaml:"default_write_mode,omitempty"` // APPEND or FULL_LOAD

	// Processors apply to all outputs
	Processors []ProcessorConfig `yaml:"processors,omitempty"`

	// Legacy fields for backward compatibility (deprecated)
	Inputs  map[string]string       `yaml:"inputs,omitempty"`  // logical name -> table path
	Outputs map[string]OutputConfig `yaml:"outputs,omitempty"` // logical name -> output config

	ComponentCfg     []byte                 `yaml:"config,omitempty"` // Component-specific JSON config
	ChunkSize        int64                  `yaml:"chunk_size,omitempty"`
	BufferSize       int64                  `yaml:"buffer_size,omitempty"`
	RowGroupSize     int64                  `yaml:"row_group_size,omitempty"`
	TargetFileSize   int64                  `yaml:"target_file_size,omitempty"`
	Backend  backend.StorageBackend `yaml:"-"` // Not serialized
	HTTPAddr string                 `yaml:"http_addr,omitempty"`

	// RunTokenPath is the filesystem path to the mounted run-token Secret.
	// When non-empty and the file exists, the gateway reads it at boot.
	// The file contains a single raw JWT string (the per-run token).
	// K8s controllers set this to /var/run/secrets/datuplet-runtoken/token.
	RunTokenPath string `yaml:"run_token_path,omitempty"`

	// PodAnnotationsPath is the filesystem path to the kubelet downward-API
	// projection of the pod's annotations. When non-empty, ServerV2 starts a
	// goroutine on boot that polls this file every CancelPollInterval and
	// triggers a graceful shutdown on `datuplet.io/cancel="true"`. This
	// short-circuits the ≤15-min STS-cred leak window after a cancel.
	// Empty path = no cancel watcher (Docker + non-K8s deployments).
	PodAnnotationsPath string `yaml:"pod_annotations_path,omitempty"`

	// LakekeeperURL is the base URL of the Iceberg REST catalog (lakekeeper).
	// When set, the gateway constructs a `pkg/datagateway/lakekeeper.Resolver`
	// at boot and routes per-write/per-read requests through it for
	// LoadOrCreate + vended-creds rotation. Empty = static-backend mode
	// (tests + dev only).
	LakekeeperURL string `yaml:"lakekeeper_url,omitempty"`

	// PipelineAPIJWKSURL is the JWKS endpoint URL pipeline-api serves
	// (e.g. http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json).
	// Required whenever LakekeeperURL is set (because the same condition that
	// triggers lakekeeper routing also requires a validated run-token).
	// The operator injects this into the DG sidecar's configMap;
	// tests + dev paths leave it empty.
	PipelineAPIJWKSURL string `yaml:"pipeline_api_jwks_url,omitempty"`

	// SecretsDir is the directory from which the gateway resolves $[name] references
	// in ComponentCfg at startup. One file per secret; file contents == value. When
	// empty, no resolution is attempted and any $[name] reference in ComponentCfg
	// causes NewServerV2 to fail.
	SecretsDir string `yaml:"secrets_dir,omitempty"`

	// Storage credentials for native S3 components (StorageBootstrap)
	S3Endpoint   string `yaml:"s3_endpoint,omitempty"`
	S3AccessKey  string `yaml:"s3_access_key,omitempty"`
	S3SecretKey  string `yaml:"s3_secret_key,omitempty"`
	S3Region     string `yaml:"s3_region,omitempty"`
	S3BucketName string `yaml:"s3_bucket_name,omitempty"`
}

// InputTableConfig defines a specific table input.
type InputTableConfig struct {
	Bucket           string `yaml:"bucket"`                       // Bucket name (e.g., "raw")
	Table            string `yaml:"table"`                        // Table name (e.g., "orders")
	As               string `yaml:"as,omitempty"`                 // Optional logical SQL name (defaults to table name)
	SinceTimestampMs int64  `yaml:"since_timestamp_ms,omitempty"` // >0 = read delta since this epoch ms
	SinceSnapshot    int64  `yaml:"since_snapshot,omitempty"`     // >0 = read delta since this snapshot ID
	// TimestampColumn is the iceberg column DG uses to apply SinceTimestampMs.
	// Defaults to "created" when SinceTimestampMs is set and this is empty.
	// Used by the sinceDays/since filter pushdown in sql-transform.
	TimestampColumn string `yaml:"timestamp_column,omitempty"`
}

// OutputTableConfig defines a specific table output.
type OutputTableConfig struct {
	Name            string                   `yaml:"name"`                      // Output name (iceberg target table)
	Bucket          string                   `yaml:"bucket"`                    // Bucket name
	WriteMode       string                   `yaml:"write_mode"`                // APPEND or FULL_LOAD
	LogicalName     string                   `yaml:"logical_name,omitempty"`    // SDK-facing identifier (defaults to Name when empty)
	PartitionFields []PartitionFieldConfig   `yaml:"partition_fields,omitempty"` // Optional partition spec
}

// PartitionFieldConfig defines a single partition field.
type PartitionFieldConfig struct {
	SourceColumn string `yaml:"source_column"`
	Transform    string `yaml:"transform"`
}

// GetRunID returns the run ID.
func (c *Config) GetRunID() string {
	return c.RunID
}

// OutputConfig contains output path and optional processors.
// Deprecated: Use bucket-based outputs instead.
type OutputConfig struct {
	Path       string            // Table path for output
	Processors []ProcessorConfig // Optional processors to apply
}

// ProcessorConfig defines a processor operation.
type ProcessorConfig struct {
	Type    string   `yaml:"type"`    // Processor type: "drop"
	Columns []string `yaml:"columns"` // For drop: columns to remove
}
