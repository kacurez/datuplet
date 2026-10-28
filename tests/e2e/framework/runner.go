package framework

import (
	"time"

	"github.com/google/uuid"
)

// RunOpts configures a pipeline run.
type RunOpts struct {
	StorageType string        // "filesystem" or "s3"
	Timeout     time.Duration // per-pipeline timeout

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
