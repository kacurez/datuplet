-- 010_run_stage_snapshot.sql — persist the per-stage timeline snapshot.
-- The pipeline-observer marshals PipelineRun.Status.StageStatuses into this
-- column so the run-detail timeline survives CRD deletion. NULL = no timeline
-- recorded yet (pre-feature run, or a run still Pending).
ALTER TABLE runs ADD COLUMN stage_statuses jsonb NULL;
