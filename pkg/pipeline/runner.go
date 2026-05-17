// Package pipeline provides the pipeline execution engine.
package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/datuplet/datuplet/pkg/lib/datalake"
	"github.com/datuplet/datuplet/pkg/lib/orchestrator"
	"github.com/datuplet/datuplet/pkg/lib/secrets"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/google/uuid"
)

// Phase is the state a ProgressEvent reports. Typed string so misspellings
// fail at compile time. LocalBackend persists this verbatim into SQLite.
type Phase string

const (
	PhaseRunning           Phase = "Running"
	PhaseSucceeded         Phase = "Succeeded"
	PhaseFailedUser        Phase = "FailedUser"
	PhaseFailedApplication Phase = "FailedApplication"
)

// ProgressEvent is emitted on every pipeline phase transition by Controller.Run.
// Emitted synchronously from the Controller goroutine in order;
// the callback must not block. nil ProgressFn is a no-op.
type ProgressEvent struct {
	// Phase is one of PhaseRunning / PhaseSucceeded / PhaseFailedUser /
	// PhaseFailedApplication.
	Phase Phase
	// CurrentStage names the stage being entered; empty on the initial
	// PhaseRunning event and on the terminal PhaseSucceeded event.
	CurrentStage string
	// Message carries human-readable status: on PhaseSucceeded a fixed
	// success string, on failure the error text. Empty on PhaseRunning
	// events.
	Message string
}

// ProgressFn is the progress callback. nil is a no-op.
type ProgressFn func(ProgressEvent)

// Controller orchestrates pipeline execution.
type Controller struct {
	orchestrator orchestrator.Orchestrator
	dataLake     datalake.DataLake
	config       *config.Pipeline

	// Infrastructure config (from environment, not pipeline spec)
	infraConfig InfraConfig

	// secretsDir is the host directory bind-mounted on each gateway sidecar at
	// /var/run/secrets/datuplet. Always empty in the current local-run mode;
	// the pre-flight check in Run rejects pipelines that reference $[name]
	// secrets so the empty value never reaches a running container.
	secretsDir string

	// progress is an optional phase-transition callback. nil is a no-op.
	// Set via WithProgress. Called synchronously from Run; callbacks must not
	// block. Used by LocalBackend (pipeline-api local mode) to mirror phase
	// transitions into SQLite.
	progress ProgressFn

	// Runtime state (set during Run)
	runID string // Unique run ID for all components (full UUID)
}

// WithProgress registers a callback for pipeline phase transitions.
// See ProgressFn documentation for contract. Returns the Controller to
// allow chaining.
func (c *Controller) WithProgress(fn ProgressFn) *Controller {
	c.progress = fn
	return c
}

// emit invokes the progress callback if one is registered. Nil-safe.
func (c *Controller) emit(e ProgressEvent) {
	if c.progress != nil {
		c.progress(e)
	}
}

// SetRunID pre-seeds the run identifier used for all containers in this run
// and embedded in iceberg snapshot summaries. If not called, Run() generates
// a fresh UUID. `datuplet run --remote` calls this so the run-id printed on
// success matches the id in the snapshot audit trail.
func (c *Controller) SetRunID(id string) {
	c.runID = id
}

// InfraConfig holds infrastructure configuration loaded from environment.
// This is separate from the Pipeline spec and contains storage credentials.
type InfraConfig struct {
	// StorageType is "filesystem" for local storage, empty or "s3" for S3/MinIO
	StorageType string
	// StorageRoot is the absolute path to the warehouse directory (for filesystem mode)
	StorageRoot string

	// S3/MinIO settings (used when StorageType != "filesystem")
	Endpoint     string
	Bucket       string
	AccessKey    string
	SecretKey    string
	Region       string
	UseSSL       bool
	UsePathStyle bool

	// LakekeeperURL is the catalog REST base URL TableCommit talks to.
	// Wired from DATUPLET_LAKEKEEPER_URL. Required for K8s; empty in
	// unit tests.
	LakekeeperURL string

	// LakekeeperWarehouseName is the lakekeeper warehouse name
	// (constant `datuplet` in cluster + local mode today). Wired from
	// DATUPLET_LAKEKEEPER_WAREHOUSE; defaults to `datuplet` when unset
	// so the common case is zero-config.
	LakekeeperWarehouseName string

	// LakekeeperProjectID is the lakekeeper Project UUID forwarded as
	// `x-project-id` on every catalog/STS call. Wired from
	// DATUPLET_LAKEKEEPER_PROJECT_ID; empty disables the header for
	// single-project deploys.
	LakekeeperProjectID string
}

// BucketCommitInfo holds information needed to commit a bucket.
type BucketCommitInfo struct {
	Bucket    string
	WriteMode string
}

// ExecutionContext holds state for a pipeline execution.
type ExecutionContext struct {
	Executions map[string]*ComponentExecution // component name -> execution
}

// ComponentExecution tracks a single component's execution.
type ComponentExecution struct {
	ExecutionID string
	// OutputBuckets maps bucket name to commit info
	OutputBuckets map[string]BucketCommitInfo
}

// New creates a new controller with the given orchestrator.
func New(orch orchestrator.Orchestrator) *Controller {
	return &Controller{
		orchestrator: orch,
	}
}

// LoadPipeline loads and validates a pipeline configuration.
func (c *Controller) LoadPipeline(configPath string) error {
	cfg, err := config.ParseFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to load pipeline: %w", err)
	}
	return c.initFromConfig(cfg)
}

// initFromConfig is the shared post-parse initialization: stores the
// config, loads infrastructure config from env, and constructs the data
// lake client.
func (c *Controller) initFromConfig(cfg *config.Pipeline) error {
	c.config = cfg

	// Load infrastructure config from environment
	c.infraConfig = loadInfraConfigFromEnv()

	// Create data lake client for local operations (bucket creation, etc.)
	switch c.infraConfig.StorageType {
	case "filesystem":
		if c.infraConfig.StorageRoot == "" {
			return fmt.Errorf("DATUPLET_STORAGE_ROOT is required when DATUPLET_STORAGE_TYPE=filesystem")
		}
		fsDL := datalake.NewFilesystemDataLake(c.infraConfig.StorageRoot)
		if err := fsDL.EnsureRoot(); err != nil {
			return fmt.Errorf("failed to ensure warehouse directory: %w", err)
		}
		c.dataLake = fsDL
	default:
		if c.infraConfig.Endpoint != "" {
			dlConfig := datalake.Config{
				Type:         "minio",
				Endpoint:     c.infraConfig.Endpoint,
				Bucket:       c.infraConfig.Bucket,
				AccessKey:    c.infraConfig.AccessKey,
				SecretKey:    c.infraConfig.SecretKey,
				Region:       c.infraConfig.Region,
				UseSSL:       c.infraConfig.UseSSL,
				UsePathStyle: c.infraConfig.UsePathStyle,
			}

			dl, err := datalake.NewMinIODataLake(dlConfig)
			if err != nil {
				return fmt.Errorf("failed to create data lake client: %w", err)
			}
			c.dataLake = dl
		}
	}

	return nil
}

// loadInfraConfigFromEnv loads infrastructure configuration from environment variables.
func loadInfraConfigFromEnv() InfraConfig {
	cfg := InfraConfig{
		StorageType:             os.Getenv("DATUPLET_STORAGE_TYPE"),
		StorageRoot:             os.Getenv("DATUPLET_STORAGE_ROOT"),
		Endpoint:                getEnvOrDefault("DATUPLET_STORAGE_ENDPOINT", "localhost:9000"),
		Bucket:                  getEnvOrDefault("DATUPLET_STORAGE_BUCKET", "datuplet"),
		AccessKey:               getEnvOrDefault("DATUPLET_STORAGE_ACCESS_KEY", "minioadmin"),
		SecretKey:               getEnvOrDefault("DATUPLET_STORAGE_SECRET_KEY", "minioadmin"),
		Region:                  getEnvOrDefault("DATUPLET_STORAGE_REGION", ""),
		UseSSL:                  os.Getenv("DATUPLET_STORAGE_USE_SSL") == "true",
		UsePathStyle:            getEnvOrDefault("DATUPLET_STORAGE_USE_PATH_STYLE", "true") == "true",
		LakekeeperURL:           os.Getenv("DATUPLET_LAKEKEEPER_URL"),
		LakekeeperWarehouseName: getEnvOrDefault("DATUPLET_LAKEKEEPER_WAREHOUSE", "datuplet"),
		LakekeeperProjectID:     os.Getenv("DATUPLET_LAKEKEEPER_PROJECT_ID"),
	}

	// In filesystem mode, clear S3 defaults so they don't get used accidentally
	if cfg.StorageType == "filesystem" {
		cfg.Endpoint = ""
		cfg.Bucket = ""
		cfg.AccessKey = ""
		cfg.SecretKey = ""
	}

	return cfg
}

func getEnvOrDefault(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

// Run executes the config.
func (c *Controller) Run(ctx context.Context) error {
	if c.config == nil {
		return fmt.Errorf("no pipeline loaded")
	}

	// Pre-flight: if any component config references $[name] and no secrets dir
	// is configured, fail before starting any container. Exit before containers
	// start and list referenced names.
	if c.secretsDir == "" {
		var missing []string
		for _, stage := range c.config.Spec.Stages {
			for _, comp := range stage.Components {
				if comp.Config == nil {
					continue
				}
				refs, err := secrets.Validate(comp.Config)
				if err != nil {
					// Parser already caught syntax errors; treat any late failure here as fatal.
					return fmt.Errorf("component %s: %w", comp.Name, err)
				}
				missing = append(missing, refs...)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("pipeline references secrets %v but --secrets-dir was not provided", missing)
		}
	}

	fmt.Printf("Starting pipeline: %s\n", c.config.Metadata.Name)
	if c.infraConfig.StorageType == "filesystem" {
		fmt.Printf("Storage: filesystem (%s)\n", c.infraConfig.StorageRoot)
	} else {
		fmt.Printf("Storage: S3 (%s/%s)\n", c.infraConfig.Endpoint, c.infraConfig.Bucket)
	}
	fmt.Printf("Using Iceberg table format (TableCommit jobs)\n")

	// Bucket provisioning is owned by lakekeeper at chart-install time.
	// The Controller's only remaining caller is `datuplet run --remote`,
	// which talks to a remote pipeline-api / lakekeeper / MinIO — there
	// is nothing for the laptop CLI to "ensure" here. DG fails loudly if
	// the warehouse is genuinely missing.

	// Generate run ID (shared by all components in this run).
	// Preserve a pre-seeded run ID when SetRunID was called — the local-
	// CLI path seeds the UUID before Run so the printed run-id matches the
	// iceberg snapshot audit trail.
	if c.runID == "" {
		c.runID = uuid.New().String()
	}

	// Create execution context
	execCtx := &ExecutionContext{
		Executions: make(map[string]*ComponentExecution),
	}

	fmt.Printf("Run ID: %s\n", c.runID)

	// Initial Running event — pipeline accepted and about to bring up gateway.
	c.emit(ProgressEvent{Phase: PhaseRunning})

	// Lakekeeper is the catalog of record, brought up out-of-band (local
	// mode: local infra compose; K8s: lakekeeper Deployment). The
	// Controller does not own the lakekeeper lifecycle.

	// Execute stages sequentially
	for _, stage := range c.config.Spec.Stages {
		fmt.Printf("\nStarting stage: %s (%d components)\n", stage.Name, len(stage.Components))
		c.emit(ProgressEvent{Phase: PhaseRunning, CurrentStage: stage.Name})

		if err := c.executeStage(ctx, &stage, execCtx); err != nil {
			fmt.Printf("Stage %s failed: %v\n", stage.Name, err)
			c.rollbackStage(ctx, &stage, execCtx)
			// All stage failures emit PhaseFailedApplication. Distinguishing
			// PhaseFailedUser vs PhaseFailedApplication would require threading
			// orchestrator.ExecutionResult.FailureType through the error chain
			// wrapped at runner.go `executeComponent`. LocalBackend can still
			// read the exit-code classification from Message, which embeds
			// "(exit N, FailureType)".
			c.emit(ProgressEvent{Phase: PhaseFailedApplication, CurrentStage: stage.Name, Message: err.Error()})
			return fmt.Errorf("stage %s failed: %w", stage.Name, err)
		}

		// Commit stage outputs (per bucket)
		if err := c.commitStage(ctx, &stage, execCtx); err != nil {
			fmt.Printf("Failed to commit stage %s: %v\n", stage.Name, err)
			c.rollbackStage(ctx, &stage, execCtx)
			// Commit failures are always infrastructure — Iceberg snapshot errors.
			c.emit(ProgressEvent{Phase: PhaseFailedApplication, CurrentStage: stage.Name, Message: err.Error()})
			return fmt.Errorf("failed to commit stage %s: %w", stage.Name, err)
		}

		fmt.Printf("Stage %s completed\n", stage.Name)
	}

	fmt.Printf("\nPipeline %s completed successfully\n", c.config.Metadata.Name)
	c.emit(ProgressEvent{Phase: PhaseSucceeded, Message: "pipeline completed successfully"})
	return nil
}

// executeStage runs all components in a stage.
func (c *Controller) executeStage(ctx context.Context, stage *config.Stage, execCtx *ExecutionContext) error {
	for _, comp := range stage.Components {
		fmt.Printf("  Starting component: %s\n", comp.Name)

		if err := c.executeComponent(ctx, &comp, execCtx); err != nil {
			return fmt.Errorf("component %s failed: %w", comp.Name, err)
		}

		fmt.Printf("  Component %s completed\n", comp.Name)
	}
	return nil
}

// executeComponent runs a single component.
func (c *Controller) executeComponent(ctx context.Context, comp *config.Component, execCtx *ExecutionContext) error {
	executionID := uuid.New().String()

	// Create component execution record
	compExec := &ComponentExecution{
		ExecutionID:   executionID,
		OutputBuckets: make(map[string]BucketCommitInfo),
	}
	execCtx.Executions[comp.Name] = compExec

	// Extract volume mounts from config (look for "source" field with file paths)
	volumes := c.extractVolumeMounts(comp.Config)

	// Build output bucket permissions for the component
	// Components will get credentials from lakekeeper at runtime
	outputBuckets := comp.GetOutputBuckets()
	inputBuckets := comp.GetInputBuckets()

	// Track buckets for commit
	for _, bucket := range outputBuckets {
		writeMode := config.DefaultWriteMode
		if comp.Outputs != nil {
			writeMode = comp.Outputs.GetWriteModeForBucket(bucket)
		}
		compExec.OutputBuckets[bucket] = BucketCommitInfo{
			Bucket:    bucket,
			WriteMode: writeMode,
		}
	}

	// Build component spec with bucket-based authorization
	spec := orchestrator.ComponentSpec{
		Name:                comp.Name,
		Image:               comp.Image,
		ExecutionID:         executionID,
		RunID:               c.runID,
		LakekeeperURL:       c.infraConfig.LakekeeperURL,
		WarehouseName:       c.infraConfig.LakekeeperWarehouseName,
		LakekeeperProjectID: c.infraConfig.LakekeeperProjectID,
		Config:              comp.Config,
		StorageType:      c.infraConfig.StorageType,
		StorageRoot:      c.infraConfig.StorageRoot,
		SecretsDir:       c.secretsDir,
		// Bucket-based authorization
		InputBuckets:  inputBuckets,
		OutputBuckets: outputBuckets,
		// Output configuration for bucket mode
		OutputConfig:   buildOutputConfig(comp.Outputs),
		ChunkSize:      c.config.Spec.Gateway.ChunkSize,
		BufferSize:     c.config.Spec.Gateway.BufferSize,
		RowGroupSize:   c.config.Spec.Gateway.RowGroupSize,
		TargetFileSize: c.config.Spec.Gateway.TargetFileSize,
		Volumes:        volumes,
	}

	// Add DataLake config for S3 mode (filesystem mode resolves paths
	// via lakekeeper).
	if c.infraConfig.StorageType != "filesystem" {
		spec.DataLake = orchestrator.DataLakeConfig{
			Endpoint:     c.translateEndpointForContainer(c.infraConfig.Endpoint),
			Bucket:       c.infraConfig.Bucket,
			AccessKey:    c.infraConfig.AccessKey,
			SecretKey:    c.infraConfig.SecretKey,
			Region:       c.infraConfig.Region,
			UseSSL:       c.infraConfig.UseSSL,
			UsePathStyle: c.infraConfig.UsePathStyle,
		}
	}

	// Add input tables if specified
	if comp.Inputs != nil {
		spec.InputTables = buildInputTables(comp.Inputs)
	}

	// Execute the component
	result, err := c.orchestrator.ExecuteComponent(ctx, spec)
	if err != nil {
		return fmt.Errorf("component execution failed: %w", err)
	}

	if !result.Success {
		if result.StatusMessage != "" {
			return fmt.Errorf("component failed (exit %d, %s): %s", result.ExitCode, result.FailureType, result.StatusMessage)
		}
		return fmt.Errorf("component failed: %s", result.Error)
	}

	return nil
}

// buildOutputConfig builds the output configuration for the component.
func buildOutputConfig(out *config.OutputSpec) orchestrator.BucketOutputConfig {
	if out == nil {
		return orchestrator.BucketOutputConfig{}
	}

	cfg := orchestrator.BucketOutputConfig{
		Processors: convertProcessors(out.Processors),
	}

	if out.DefaultBucket != "" {
		cfg.DefaultBucket = out.DefaultBucket
		cfg.DefaultWriteMode = out.DefaultWriteMode
	}

	// Add explicit bucket configs
	for _, b := range out.Buckets {
		cfg.Buckets = append(cfg.Buckets, orchestrator.BucketConfig{
			Name:      b.Name,
			WriteMode: b.WriteMode,
		})
	}

	// Add explicit table configs
	for _, t := range out.Tables {
		tc := orchestrator.TableConfig{
			Name:        t.Name,
			Bucket:      t.Bucket,
			WriteMode:   t.WriteMode,
			LogicalName: t.LogicalName,
		}
		for _, pf := range t.PartitionSpec {
			tc.PartitionFields = append(tc.PartitionFields, orchestrator.PartitionFieldConfig{
				SourceColumn: pf.SourceColumn,
				Transform:    pf.Transform,
			})
		}
		cfg.Tables = append(cfg.Tables, tc)
	}

	return cfg
}

// buildInputTables builds the input table list for the component.
func buildInputTables(in *config.InputSpec) []orchestrator.InputTable {
	var tables []orchestrator.InputTable

	for _, t := range in.Tables {
		it := orchestrator.InputTable{
			Bucket:          t.Bucket,
			Table:           t.Table,
			As:              t.As, // Pass through logical name
			TimestampColumn: t.TimestampColumn,
		}

		// Resolve incremental read config (mutually exclusive: SinceDays > Since > SinceSnapshot)
		switch {
		case t.SinceDays != nil && *t.SinceDays > 0:
			cutoff := time.Now().UTC().Add(-time.Duration(*t.SinceDays) * 24 * time.Hour)
			it.SinceTimestampMs = cutoff.UnixMilli()
		case t.Since != "":
			d, _ := config.ParseSinceDuration(t.Since) // already validated
			it.SinceTimestampMs = time.Now().Add(-d).UnixMilli()
		case t.SinceSnapshot != nil:
			it.SinceSnapshot = *t.SinceSnapshot
		}

		tables = append(tables, it)
	}

	return tables
}

// commitStage commits all outputs from a stage using TableCommit jobs.
// In the new model, commits are per-bucket, not per-table.
func (c *Controller) commitStage(ctx context.Context, stage *config.Stage, execCtx *ExecutionContext) error {
	// Collect unique buckets to commit.
	bucketsToCommit := make(map[string]BucketCommitInfo)
	for _, comp := range stage.Components {
		exec := execCtx.Executions[comp.Name]
		if exec == nil {
			continue
		}
		for bucket, info := range exec.OutputBuckets {
			// If bucket already seen, keep the most conservative write mode
			if existing, ok := bucketsToCommit[bucket]; ok {
				// APPEND is more conservative than FULL_LOAD
				if existing.WriteMode == config.WriteModeAppend {
					continue
				}
			}
			bucketsToCommit[bucket] = info
		}
	}

	// Commit each bucket using TableCommit job
	// TableCommit will discover all tables written under the bucket
	for _, info := range bucketsToCommit {
		fmt.Printf("  Committing bucket: %s (mode: %s)\n", info.Bucket, info.WriteMode)

		spec := orchestrator.TableCommitSpec{
			RunID:               c.runID,
			Bucket:              info.Bucket,
			WriteMode:           info.WriteMode,
			StorageType:         c.infraConfig.StorageType,
			StorageRoot:         c.infraConfig.StorageRoot,
			LakekeeperURL:       c.infraConfig.LakekeeperURL,
			WarehouseName:       c.infraConfig.LakekeeperWarehouseName,
			LakekeeperProjectID: c.infraConfig.LakekeeperProjectID,
			WarehouseRoot:       c.deriveWarehouseRoot(),
		}

		// Add DataLake config for S3 mode
		if c.infraConfig.StorageType != "filesystem" {
			spec.DataLake = orchestrator.DataLakeConfig{
				Endpoint:     c.translateEndpointForContainer(c.infraConfig.Endpoint),
				Bucket:       c.infraConfig.Bucket,
				AccessKey:    c.infraConfig.AccessKey,
				SecretKey:    c.infraConfig.SecretKey,
				Region:       c.infraConfig.Region,
				UseSSL:       c.infraConfig.UseSSL,
				UsePathStyle: c.infraConfig.UsePathStyle,
			}
		}

		if err := c.orchestrator.ExecuteTableCommit(ctx, spec); err != nil {
			return fmt.Errorf("failed to commit bucket %s: %w", info.Bucket, err)
		}
	}

	return nil
}

// rollbackStage cleans up data for a failed stage.
// With Iceberg, data files are orphaned and can be garbage collected later.
func (c *Controller) rollbackStage(ctx context.Context, stage *config.Stage, execCtx *ExecutionContext) {
	fmt.Printf("  Rolling back stage %s\n", stage.Name)

	for _, comp := range stage.Components {
		exec := execCtx.Executions[comp.Name]
		if exec == nil {
			continue
		}

		// Iceberg mode: data files are orphaned, can be garbage collected later
		for bucket := range exec.OutputBuckets {
			fmt.Printf("  Orphaned data (will be garbage collected): bucket=%s, execution=%s\n", bucket, exec.ExecutionID)
		}
	}
}

// deriveWarehouseRoot computes the WAREHOUSE_ROOT URL passed to the
// spawned table-commit container. It must match the placement DG uses for
// `<warehouseRoot>/.run-state/<runID>/files.json` (see
// pkg/datagateway/files_manifest.go ResolveFilesManifestPath, which strips
// the deterministic suffix down to the warehouse prefix). For S3 mode
// that's `s3://<bucket>`; for filesystem mode it's `file://<storage-root>`.
func (c *Controller) deriveWarehouseRoot() string {
	if c.infraConfig.StorageType == "filesystem" {
		if c.infraConfig.StorageRoot == "" {
			return ""
		}
		return "file://" + c.infraConfig.StorageRoot
	}
	if c.infraConfig.Bucket == "" {
		return ""
	}
	return "s3://" + c.infraConfig.Bucket
}

// translateEndpointForContainer converts localhost endpoints to host.docker.internal
// so components running in containers can reach services on the host.
func (c *Controller) translateEndpointForContainer(endpoint string) string {
	endpoint = strings.Replace(endpoint, "localhost:", "host.docker.internal:", 1)
	endpoint = strings.Replace(endpoint, "127.0.0.1:", "host.docker.internal:", 1)
	return endpoint
}

// extractVolumeMounts scans component config for file paths and creates volume mounts.
func (c *Controller) extractVolumeMounts(cfg map[string]any) map[string]string {
	volumes := make(map[string]string)

	filePathKeys := []string{"source", "file", "path", "input_file"}

	for _, key := range filePathKeys {
		if val, ok := cfg[key]; ok {
			if strVal, ok := val.(string); ok {
				if strings.HasPrefix(strVal, "/") || strings.Contains(strVal, string(os.PathSeparator)) {
					absPath, err := filepath.Abs(strVal)
					if err != nil {
						continue
					}

					if _, err := os.Stat(absPath); os.IsNotExist(err) {
						continue
					}

					parentDir := filepath.Dir(absPath)
					volumes[parentDir] = "/data"

					// Update config to use container path
					cfg[key] = "/data/" + filepath.Base(absPath)
					break
				}
			}
		}
	}

	return volumes
}

// convertProcessors converts pipeline processors to orchestrator format.
func convertProcessors(procs []config.Processor) []orchestrator.ProcessorSpec {
	if len(procs) == 0 {
		return nil
	}
	result := make([]orchestrator.ProcessorSpec, len(procs))
	for i, p := range procs {
		result[i] = orchestrator.ProcessorSpec{
			Type:    p.Type,
			Columns: p.Columns,
		}
	}
	return result
}

