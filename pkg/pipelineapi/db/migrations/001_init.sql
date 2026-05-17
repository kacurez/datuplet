-- 001_init.sql — base schema for pipeline-api.

-- UUID generation via gen_random_uuid() (pgcrypto).
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Tracks which migrations have been applied.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Users: one row per human.
CREATE TABLE users (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email          TEXT UNIQUE NOT NULL,
    password_hash  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at    TIMESTAMPTZ NULL
);

-- Server-side sessions backing opaque HTTP cookies.
CREATE TABLE sessions (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL,
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_sessions_user ON sessions(user_id);
CREATE INDEX idx_sessions_expires ON sessions(expires_at);

-- Projects: one-to-one with a K8s namespace. The namespace is conventionally
-- "datuplet-" || id::text; the CHECK enforces that.
CREATE TABLE projects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT UNIQUE NOT NULL,
    k8s_namespace   TEXT UNIQUE NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT k8s_namespace_matches_id CHECK (k8s_namespace = 'datuplet-' || id::text)
);

-- Users can belong to multiple projects, with a role.
CREATE TABLE project_memberships (
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    role        TEXT NOT NULL CHECK (role IN ('admin', 'user')),
    PRIMARY KEY (user_id, project_id)
);
