// Package v1 — commit-related enums. The PipelineRun controller schedules
// commit Jobs directly (one per stage, per bucket); the TableCommit CRD
// has been removed. The enums are reused as convenience labels in
// PipelineRun status.
package v1

// TableCommitPhase represents the phase of an in-flight commit Job.
// Mirrors the old TableCommit CRD's status field shape so existing
// PipelineRun status consumers (UI, pipeline-api) keep working without
// a migration.
type TableCommitPhase string

const (
	// TableCommitPhasePending indicates the commit Job has not yet been
	// created or its Pod has not started.
	TableCommitPhasePending TableCommitPhase = "Pending"
	// TableCommitPhaseRunning indicates the commit Job's Pod is running.
	TableCommitPhaseRunning TableCommitPhase = "Running"
	// TableCommitPhaseSucceeded indicates the commit Job completed
	// successfully.
	TableCommitPhaseSucceeded TableCommitPhase = "Succeeded"
	// TableCommitPhaseFailedApplication indicates the commit Job failed
	// due to an application error. Commit failures are always classified
	// as application errors by design — there's no user-facing exit-code
	// path through the catalog write.
	TableCommitPhaseFailedApplication TableCommitPhase = "FailedApplication"
)

// WriteMode specifies how data should be written to the table.
// Used by the PipelineRun controller when scheduling commit Jobs and by
// the Docker/local orchestrator path (`pkg/lib/orchestrator`).
type WriteMode string

const (
	// WriteModeAppend adds new data files to the table
	WriteModeAppend WriteMode = "APPEND"
	// WriteModeFullLoad replaces all existing data in the table
	WriteModeFullLoad WriteMode = "FULL_LOAD"
)
