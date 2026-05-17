-- 002_pipelines_runs.sql — pipeline + run schema additions.

-- Pipelines: one row per (project, name). The YAML text is the source of
-- truth — when a run is triggered, it's re-parsed and server-side-applied
-- as a Pipeline CRD in the project namespace.
-- UNIQUE(id, project_id) is required so runs can use a composite FK that
-- ties a run's project_id to the pipeline's project_id, preventing
-- cross-project leaks.
CREATE TABLE pipelines (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id   UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    yaml         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name),
    UNIQUE (id, project_id)
);

-- Runs: index/mirror of PipelineRun CRD status. Created by POST /runs,
-- updated by the reconciler goroutine. Not authoritative while the CRD
-- lives; authoritative after the CRD is reaped.
--
-- The (pipeline_id, project_id) composite FK ties the run's project to
-- the pipeline's project so an inserted row pointing at a mismatched
-- project is rejected by Postgres. The individual project_id FK is
-- retained for ON DELETE CASCADE behaviour.
CREATE TABLE runs (
    id            UUID PRIMARY KEY,
    project_id    UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    pipeline_id   UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    phase         TEXT NOT NULL DEFAULT 'Pending',
    current_stage TEXT NOT NULL DEFAULT '',
    message       TEXT NOT NULL DEFAULT '',
    started_at    TIMESTAMPTZ NULL,
    completed_at  TIMESTAMPTZ NULL,
    triggered_by  UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (pipeline_id, project_id) REFERENCES pipelines(id, project_id)
);
CREATE INDEX idx_runs_project_created ON runs(project_id, created_at DESC);
CREATE INDEX idx_runs_pipeline_created ON runs(pipeline_id, created_at DESC);
CREATE INDEX idx_runs_phase ON runs(phase);
