// Package v1 — commit-related enums. These are retained as convenience
// labels in PipelineRun status, mirroring the old TableCommit CRD's status
// shape so UI + pipeline-api keep working without a migration. Since RFC 021
// the Data Gateway sidecar commits Iceberg tables inline — there are no
// separate commit Jobs.
package v1

// TableCommitPhase represents the phase of a per-(stage, bucket) commit as
// recorded in PipelineRun status. Mirrors the old TableCommit CRD's status
// field shape so existing PipelineRun status consumers (UI, pipeline-api)
// keep working without a migration.
type TableCommitPhase string

const (
	// TableCommitPhasePending indicates the commit has not yet started.
	TableCommitPhasePending TableCommitPhase = "Pending"
	// TableCommitPhaseRunning indicates the commit is in progress.
	TableCommitPhaseRunning TableCommitPhase = "Running"
	// TableCommitPhaseSucceeded indicates the commit completed successfully.
	TableCommitPhaseSucceeded TableCommitPhase = "Succeeded"
	// TableCommitPhaseFailedApplication indicates the commit failed due to an
	// application error. Commit failures are always classified as application
	// errors by design — there's no user-facing exit-code path through the
	// catalog write.
	TableCommitPhaseFailedApplication TableCommitPhase = "FailedApplication"
)
