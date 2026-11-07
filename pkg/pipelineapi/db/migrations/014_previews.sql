-- Pipeline preview mode: capped, scratch-namespace dry runs of a PipelineDoc.
CREATE TABLE previews (
    id                   UUID PRIMARY KEY,
    project_id           UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    triggered_by         UUID REFERENCES users(id) ON DELETE SET NULL,
    workspace            TEXT NOT NULL,
    doc                  JSONB NOT NULL,
    row_limit            INT NOT NULL,
    timeout_seconds      INT NOT NULL,
    ttl_seconds          INT NOT NULL,
    until_stage          TEXT,
    scratch_namespaces   TEXT[] NOT NULL DEFAULT '{}',
    lakekeeper_project_id TEXT NOT NULL DEFAULT '',
    warehouse            TEXT NOT NULL DEFAULT '',
    k8s_namespace        TEXT NOT NULL,
    phase                TEXT NOT NULL DEFAULT 'Pending',
    current_stage        TEXT NOT NULL DEFAULT '',
    message              TEXT NOT NULL DEFAULT '',
    stage_statuses       JSONB,
    observed_rv          BIGINT NOT NULL DEFAULT 0,
    fga_tuple_deleted_at TIMESTAMPTZ,
    result               JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    expires_at           TIMESTAMPTZ NOT NULL,
    cleaned_at           TIMESTAMPTZ
);

CREATE INDEX previews_project_workspace_created_idx
    ON previews (project_id, workspace, created_at DESC);
CREATE INDEX previews_expires_uncleaned_idx
    ON previews (expires_at) WHERE cleaned_at IS NULL;
