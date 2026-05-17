// Package orchestrator provides abstractions for running components in Docker.
//
// The Orchestrator interface is used by the Docker execution path
// (`datuplet run`). Kubernetes execution does NOT use this interface: the
// operators in pkg/k8s/controllers/ drive Job/Pod lifecycle directly via
// controller-runtime and client-go.
package orchestrator

import (
	"context"
	"time"
)

// Orchestrator abstracts how components are executed.
// Implemented only by the Docker orchestrator; Kubernetes execution bypasses
// this interface (see package doc).
type Orchestrator interface {
	// ExecuteComponent runs a component to completion.
	// It blocks until the component finishes (success or failure).
	ExecuteComponent(ctx context.Context, spec ComponentSpec) (*ExecutionResult, error)

	// ExecuteTableCommit runs a TableCommit job to commit a write session to an Iceberg table.
	ExecuteTableCommit(ctx context.Context, spec TableCommitSpec) error

	// ForceStop kills and removes a container by ID. Used by LocalBackend on
	// run cancel — ctx cancel alone only stops the Go wait, not the Docker
	// container. Idempotent: not-found errors are swallowed.
	ForceStop(ctx context.Context, containerID string) error

	// Cleanup releases any resources held by the orchestrator.
	Cleanup(ctx context.Context) error
}

// ComponentSpec defines the specification for running a component.
type ComponentSpec struct {
	// Name is the component name (used for container naming)
	Name string

	// Image is the container image to run
	Image string

	// ExecutionID is the unique identifier for this execution
	ExecutionID string

	// RunID is the unique execution identifier for this pipeline run.
	// All components in a pipeline execution share the same RunID.
	RunID string

	// LakekeeperURL is the catalog REST base URL the gateway sidecar
	// uses for per-table vended-creds + reads.
	LakekeeperURL string

	// WarehouseName is the lakekeeper warehouse name (the `warehouse`
	// query param iceberg-go's REST client appends to /v1/config). Without
	// it lakekeeper returns `GetConfigNoWarehouseProvided`. The gateway
	// reads this from `lakekeeper_warehouse_name` in its config YAML; DG's
	// resolver falls back to `s3_bucket_name` for back-compat.
	WarehouseName string

	// LakekeeperProjectID is the lakekeeper Project UUID forwarded as
	// `x-project-id` on every lakekeeper REST call. Empty disables the header.
	LakekeeperProjectID string

	// Config contains component-specific configuration
	Config map[string]any

	// DataLake contains data lake connection settings
	DataLake DataLakeConfig

	// InputBuckets lists buckets this component can read from (bucket-level access)
	InputBuckets []string

	// OutputBuckets lists buckets this component can write to (bucket-level access)
	OutputBuckets []string

	// InputTables lists specific tables this component can read
	InputTables []InputTable

	// OutputConfig contains the bucket-based output configuration
	OutputConfig BucketOutputConfig

	// Legacy: Inputs maps logical names to data lake paths (deprecated)
	Inputs map[string]string

	// Legacy: Outputs maps logical names to output configurations (deprecated)
	Outputs map[string]OutputConfig

	// ChunkSize is the recommended chunk size for streaming
	ChunkSize int64

	// BufferSize is the memory buffer size before flushing to a row group
	BufferSize int64

	// RowGroupSize is the target size for Parquet row groups
	RowGroupSize int64

	// TargetFileSize is the target Parquet file size before rotation
	TargetFileSize int64

	// Volumes maps host paths to container paths (for local files)
	Volumes map[string]string

	// ExtraEnv contains additional environment variables to pass to the component
	ExtraEnv map[string]string

	// DirectMode runs the component without a gateway sidecar.
	// Config is passed via DATUPLET_CONFIG environment variable.
	// Used for sample mode where components read directly from sources.
	DirectMode bool

	// StorageType is "filesystem" for local storage, empty or "s3" for S3
	StorageType string

	// StorageRoot is the host path to the warehouse directory (for filesystem mode volume mounts)
	StorageRoot string

	// SecretsDir is an absolute host path to bind-mount read-only at
	// /var/run/secrets/datuplet on the gateway container. When set, the
	// orchestrator also writes secrets_dir into gateway.yaml so the gateway
	// binary resolves $[name] references from that mount. Empty = not mounted.
	SecretsDir string

	// RunTokenPath is an absolute host path to the per-run JWT. When set,
	// the orchestrator bind-mounts the file read-only at
	// /var/run/secrets/datuplet-runtoken/token on the gateway sidecar
	// and passes --run-token-path to the gateway binary. Empty = no mount
	// (DG runs without credentials; OK against allowall-mode lakekeeper).
	RunTokenPath string

	// Labels are applied to every container this spec spawns (component and
	// gateway sidecar). LocalBackend uses them to scope its orphan-reap to
	// the correct --dir.
	Labels map[string]string

	// OnContainerStarted, if non-nil, is invoked with each container ID the
	// orchestrator creates for this spec (gateway sidecar AND component).
	// Called after ContainerCreate and before ContainerStart, so LocalBackend
	// can track the ID for cancel-time ForceStop before any long-running work
	// begins. Must not block or the pipeline stalls.
	OnContainerStarted func(containerID string)

}

// InputTable represents a specific table input.
type InputTable struct {
	Bucket           string
	Table            string
	As               string // Optional logical SQL name (defaults to table name)
	SinceTimestampMs int64  // >0 = read delta since this epoch ms
	SinceSnapshot    int64  // >0 = read delta since this snapshot ID
	TimestampColumn  string // Column DG uses for SinceDays/Since filter (default: "created")
}

// BucketOutputConfig contains the bucket-based output configuration.
type BucketOutputConfig struct {
	// DefaultBucket mode (exclusive): all writes go to this bucket
	DefaultBucket    string
	DefaultWriteMode string

	// Explicit bucket outputs
	Buckets []BucketConfig

	// Explicit table outputs
	Tables []TableConfig

	// Processors apply transformations to all outputs
	Processors []ProcessorSpec
}

// BucketConfig defines a bucket output with dynamic table creation.
type BucketConfig struct {
	Name      string
	WriteMode string
}

// TableConfig defines a fixed table output.
type TableConfig struct {
	Name            string                 // Iceberg target table
	Bucket          string                 // Bucket name
	WriteMode       string                 // APPEND or FULL_LOAD
	LogicalName     string                 // SDK identifier (defaults to Name when empty)
	PartitionFields []PartitionFieldConfig // Optional partition specification
}

// PartitionFieldConfig defines a single partition field for orchestrator threading.
type PartitionFieldConfig struct {
	SourceColumn string
	Transform    string
}

// TableCommitSpec defines the specification for a TableCommit job.
type TableCommitSpec struct {
	// RunID is the run to commit
	RunID string

	// Bucket is the bucket to commit (discovers tables dynamically)
	Bucket string

	// WriteMode is APPEND or FULL_LOAD (applied to all tables in bucket)
	WriteMode string

	// LakekeeperURL is the catalog REST base URL the spawned tablecommit
	// container points at (e.g.
	// http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog or, in
	// local mode, http://localhost:8181/catalog). Required.
	LakekeeperURL string

	// WarehouseName is the lakekeeper warehouse name used as the
	// `warehouse` query param. Constant `datuplet` in cluster + local
	// mode today; kept on the spec so a future multi-warehouse deploy
	// can override per-run.
	WarehouseName string

	// LakekeeperProjectID is the lakekeeper Project UUID forwarded as
	// `x-project-id` on every lakekeeper REST call.
	LakekeeperProjectID string

	// WarehouseRoot is the storage root URL where DG dropped
	// `.run-state/<runID>/files.json`. Always a fully-qualified URL —
	// `s3://<bucket>` for S3 mode or `file:///<root>` for filesystem mode.
	WarehouseRoot string

	// StorageType is "filesystem" for local storage, empty or "s3" for S3
	StorageType string

	// StorageRoot is the host path to the warehouse directory (for filesystem mode volume mounts)
	StorageRoot string

	// Legacy: Table is the logical table identifier (deprecated, use Bucket)
	Table string

	// Legacy: WarehousePath is the warehouse location (deprecated)
	WarehousePath string

	// DataLake contains data lake connection settings
	DataLake DataLakeConfig

	// Labels are applied to the TableCommit container. LocalBackend scopes
	// its orphan-reap by these labels.
	Labels map[string]string

	// OnContainerStarted, if non-nil, is invoked with the container ID the
	// orchestrator creates for this spec. Called after ContainerCreate and
	// before ContainerStart (same contract as ComponentSpec.OnContainerStarted).
	// Must not block.
	OnContainerStarted func(containerID string)

	// RunTokenPath is an absolute host path to the per-run JWT. When set,
	// the orchestrator bind-mounts the file read-only at
	// /var/run/secrets/datuplet-runtoken/token inside the tablecommit
	// container and forwards RUN_TOKEN_PATH so the binary attaches
	// `Authorization: Bearer <jwt>` on its lakekeeper calls.
	RunTokenPath string
}

// OutputConfig contains the staging and final paths for an output.
type OutputConfig struct {
	StagingPath string          // Where component writes data
	FinalPath   string          // Final table/path (for reference in Iceberg mode)
	IsIceberg   bool            // If true, StagingPath is within the Iceberg table's data directory
	Processors  []ProcessorSpec // Optional data processors applied by gateway
	WriteMode   string          // "APPEND" or "FULL_LOAD"
}

// ProcessorSpec defines a processor operation for the gateway.
type ProcessorSpec struct {
	Type    string   // Processor type: "drop"
	Columns []string // For drop: columns to remove
}

// DataLakeConfig contains data lake connection configuration.
type DataLakeConfig struct {
	Endpoint     string
	Bucket       string
	AccessKey    string
	SecretKey    string
	Region       string
	UseSSL       bool
	UsePathStyle bool
}

// ExecutionResult contains the result of a component execution.
type ExecutionResult struct {
	// Success indicates if the component completed successfully
	Success bool

	// Error contains the error message if the component failed
	Error string

	// Logs contains the component's stdout/stderr output
	Logs string

	// StartTime is when the component started
	StartTime time.Time

	// EndTime is when the component finished
	EndTime time.Time

	// ExitCode is the container's exit code
	ExitCode int

	// FailureType classifies the failure: "FailedUser" (exit 1) or "FailedApplication" (exit >= 20).
	// Empty string if succeeded.
	FailureType string

	// StatusMessage is the status message extracted from component logs via the
	// DUPLET_STATUS_MESSAGE: protocol, or a fallback message.
	StatusMessage string
}
