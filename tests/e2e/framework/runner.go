package framework

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// RunOpts configures a pipeline run.
type RunOpts struct {
	StorageType string        // "filesystem" or "s3"
	StorageRoot string        // absolute path for filesystem mode
	Timeout     time.Duration // per-pipeline timeout
	Env         map[string]string

	// SecretsDir is an absolute host path passed to `datuplet run --secrets-dir`.
	// Empty means no secrets are wired in (K8s tier creates a Secret object
	// out-of-band via the framework before RunPipeline is called).
	SecretsDir string

	// OnRunID, when non-nil, is invoked by K8sBackend.RunPipeline with the
	// minted run UUID immediately after POST /runs returns — before the run is
	// polled to a terminal phase. It lets a caller that drives RunPipeline in a
	// goroutine (e.g. the mid-run freeze / clamp scenarios) observe the run's
	// Jobs/Pods/PipelineRun (all labelled datuplet.io/run-id=<id>) while the run
	// is still in flight, without waiting for RunResult.RunID at completion.
	// Called at most once per RunPipeline invocation.
	OnRunID func(runID uuid.UUID)
}

// RunResult captures pipeline execution results.
type RunResult struct {
	Success       bool
	ExitCode      int
	FailureType   string // "FailedUser", "FailedApplication", ""
	StatusMessage string
	Logs          string
	Duration      time.Duration

	// RunID is the UUID minted for this run. Populated by K8sBackend.
	// Used by audit-trail assertions to verify datuplet.run-id in the
	// iceberg snapshot summary.
	RunID uuid.UUID
}

// Backend abstracts pipeline execution.
type Backend interface {
	RunPipeline(ctx context.Context, pipelineYAML string, opts RunOpts) (*RunResult, error)
	Cleanup(ctx context.Context) error
}
