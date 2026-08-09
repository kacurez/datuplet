-- 013_user_apps.sql — RFC 028: user-apps control plane (Part 1/2).
--
-- Apps are project-scoped serverless dashboards: an immutable,
-- content-addressed bundle (SHA-256 of the raw bytes, stored gzip-compressed)
-- plus a manifest row. app_channels maps the two channels ('production',
-- 'draft') to a version; promotion is a compare-and-swap repoint of
-- 'production', upload always repoints 'draft'. app_viewer_tokens backs the
-- opaque `vw_<token_id>.<secret>` viewer-token scheme (§5.3): only the salted
-- hash is stored, never the secret. app_render_logs is the per-app render
-- ring buffer (§6.6), trimmed by both a newest-200 count bound and a
-- retention-age bound (both enforced in Go, not by a DB trigger, so the
-- retention window can be operator-tuned without a migration).
--
-- Additive only (POC greenfield, no back-compat needed).

CREATE TABLE apps (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    fga_registered  BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

-- hash is the hex SHA-256 of the RAW (uncompressed) bundle; bundle stores the
-- gzip-compressed bytes; size_bytes is the RAW size (what quota accounting
-- and the 5 MB cap are measured against — NOT the compressed size).
CREATE TABLE app_versions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    app_id      UUID NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    hash        CHAR(64) NOT NULL,
    bundle      BYTEA NOT NULL,
    size_bytes  BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (app_id, hash)
);
CREATE INDEX idx_app_versions_app_created ON app_versions (app_id, created_at DESC);
CREATE INDEX idx_app_versions_hash ON app_versions (hash);

CREATE TABLE app_channels (
    app_id      UUID NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    channel     TEXT NOT NULL CHECK (channel IN ('production', 'draft')),
    version_id  UUID NOT NULL REFERENCES app_versions(id),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (app_id, channel)
);

CREATE TABLE app_viewer_tokens (
    token_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    app_id       UUID NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    salt         BYTEA NOT NULL,
    secret_hash  BYTEA NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at   TIMESTAMPTZ NULL
);
CREATE INDEX idx_app_viewer_tokens_app ON app_viewer_tokens (app_id);

CREATE TABLE app_render_logs (
    request_id      UUID PRIMARY KEY,
    app_id          UUID NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    version_hash    CHAR(64) NOT NULL,
    channel         TEXT NOT NULL,
    principal_kind  TEXT NOT NULL,
    principal_id    TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    duration_ms     BIGINT NOT NULL,
    outcome         TEXT NOT NULL,
    log_text        TEXT NOT NULL DEFAULT '',
    error           TEXT NULL
);
CREATE INDEX idx_app_render_logs_app_started ON app_render_logs (app_id, started_at DESC);
