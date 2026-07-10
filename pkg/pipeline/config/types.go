// Package config provides types and parsing for pipeline YAML configuration.
package config

// Pipeline represents a complete data pipeline definition.
type Pipeline struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	Metadata   Metadata `yaml:"metadata"`
	Spec       Spec     `yaml:"spec"`
}

// Metadata contains pipeline metadata.
type Metadata struct {
	Name   string            `yaml:"name"`
	Labels map[string]string `yaml:"labels,omitempty"`
}

// Spec defines the pipeline specification.
// DataLake credentials are not carried here — they come from
// lakekeeper-vended STS at the data plane.
type Spec struct {
	Gateway GatewayConfig `yaml:"gateway,omitempty"`
	Stages  []Stage       `yaml:"stages"`
}

// GatewayConfig holds data gateway settings.
type GatewayConfig struct {
	// ChunkSize is the default chunk size for component read/write operations.
	// Default: 32MB
	ChunkSize int64 `yaml:"chunkSize,omitempty"`

	// BufferSize is the memory buffer size before flushing to a Parquet row group.
	// Default: 64MB
	BufferSize int64 `yaml:"bufferSize,omitempty"`

	// RowGroupSize is the target size for Parquet row groups.
	// Default: same as BufferSize
	RowGroupSize int64 `yaml:"rowGroupSize,omitempty"`

	// TargetFileSize is the target Parquet file size before rotation.
	// Default: 128MB
	TargetFileSize int64 `yaml:"targetFileSize,omitempty"`
}

// Stage represents a pipeline stage containing one or more components.
// Components within a stage run in parallel.
type Stage struct {
	Name       string      `yaml:"name"`
	Components []Component `yaml:"components"`
}

// Component defines a single pipeline component.
// Note: Type field is removed - component behavior is determined by inputs/outputs.
type Component struct {
	Name      string `yaml:"name"`
	Component string `yaml:"component"`
	Version   string `yaml:"version,omitempty"`
	// Image is the container image. On the K8s surface it is resolved from
	// the registry at admission and not carried here; retained for the
	// legacy local orchestrator path only.
	Image   string         `yaml:"image,omitempty"`
	Config  map[string]any `yaml:"config,omitempty"`
	Inputs  *InputSpec     `yaml:"inputs,omitempty"`
	Outputs *OutputSpec    `yaml:"outputs,omitempty"`

	// Optional resource limits
	Resources *ResourceSpec `yaml:"resources,omitempty"`
}

// InputSpec defines input configuration for a component.
// Components can read from:
//   - Specific tables (explicit bucket.table references)
//   - Entire buckets (access to all tables in bucket)
type InputSpec struct {
	// Buckets grants read access to all tables in these buckets.
	// SDK uses: OpenReaderFromBucket(bucket, table)
	Buckets []string `yaml:"buckets,omitempty"`

	// Tables grants read access to specific tables.
	// SDK uses: OpenReaderFromBucket(bucket, table)
	Tables []InputTableSpec `yaml:"tables,omitempty"`
}

// InputTableSpec defines a specific table input.
type InputTableSpec struct {
	Bucket          string `yaml:"bucket"`                     // Bucket name (e.g., "raw")
	Table           string `yaml:"table"`                      // Table name (e.g., "orders")
	As              string `yaml:"as,omitempty"`               // Optional logical SQL name (defaults to table name)
	Since           string `yaml:"since,omitempty"`            // Relative duration for incremental reads: "30m", "12h", "3d", "1w"
	SinceSnapshot   *int64 `yaml:"sinceSnapshot,omitempty"`    // Explicit snapshot ID for incremental reads
	SinceDays       *int   `yaml:"sinceDays,omitempty"`        // Sugar over Since: rows where TimestampColumn >= NOW - SinceDays days
	TimestampColumn string `yaml:"timestampColumn,omitempty"`  // Column for SinceDays / Since filter (default: "created")
}

// OutputSpec defines output configuration for a component.
// Two modes are supported:
//  1. DefaultBucket mode (exclusive): All writes go to one bucket with dynamic table names
//  2. Explicit mode: Specific buckets and/or tables declared
type OutputSpec struct {
	// DefaultBucket mode (exclusive - cannot be combined with Buckets/Tables)
	// Enables dynamic table creation: SDK calls WriteChunk(table, data)
	DefaultBucket    string `yaml:"defaultBucket,omitempty"`
	DefaultWriteMode string `yaml:"defaultWriteMode,omitempty"` // APPEND or FULL_LOAD (default: FULL_LOAD)

	// Explicit mode: declare specific buckets and/or tables
	// SDK uses: WriteChunkToBucket(bucket, table, data) for buckets
	// SDK uses: WriteChunk(tableName, data) for tables
	Buckets []OutputBucketSpec `yaml:"buckets,omitempty"`
	Tables  []OutputTableSpec  `yaml:"tables,omitempty"`

	// Processors apply transformations to output data (e.g., drop columns)
	// Applied to all outputs from this component
	Processors []Processor `yaml:"processors,omitempty"`
}

// OutputBucketSpec defines a bucket output with dynamic table creation.
type OutputBucketSpec struct {
	Name      string `yaml:"name"`      // Bucket name (e.g., "raw")
	WriteMode string `yaml:"writeMode"` // APPEND or FULL_LOAD
}

// OutputTableSpec defines a fixed table output.
type OutputTableSpec struct {
	Name          string               `yaml:"name"`                    // Iceberg target table (e.g., "daily_summary")
	Bucket        string               `yaml:"bucket"`                  // Bucket name (e.g., "curated")
	WriteMode     string               `yaml:"writeMode"`               // APPEND or FULL_LOAD
	LogicalName   string               `yaml:"logicalName,omitempty"`   // SDK identifier (defaults to Name when empty)
	PartitionSpec []PartitionFieldSpec `yaml:"partitionSpec,omitempty"` // Optional partition specification
}

// PartitionFieldSpec defines a single partition field.
type PartitionFieldSpec struct {
	SourceColumn string `yaml:"source_column"` // Column name in the data
	Transform    string `yaml:"transform"`     // Transform: identity, day, month, year, hour
}

// Processor defines a data processor operation applied by the gateway.
type Processor struct {
	Type    string   `yaml:"type"`              // Processor type: "drop"
	Columns []string `yaml:"columns,omitempty"` // For drop: columns to remove
}

// ResourceSpec defines resource limits for a component.
type ResourceSpec struct {
	Memory string `yaml:"memory,omitempty"`
	CPU    string `yaml:"cpu,omitempty"`
}

// Write modes for table outputs
const (
	WriteModeAppend   = "APPEND"
	WriteModeFullLoad = "FULL_LOAD"
)

// Defaults
const (
	DefaultAPIVersion     = "datuplet.io/v1"
	DefaultKind           = "Pipeline"
	DefaultChunkSize      = 32 * 1024 * 1024  // 32MB - component chunk size
	DefaultBufferSize     = 64 * 1024 * 1024  // 64MB - gateway buffer before flush
	DefaultTargetFileSize = 128 * 1024 * 1024 // 128MB - Parquet file rotation
	DefaultWriteMode      = WriteModeFullLoad // Default write mode for outputs
)
