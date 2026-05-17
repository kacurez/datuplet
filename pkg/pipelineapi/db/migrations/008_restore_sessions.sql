-- 008_restore_sessions.sql — restore the sessions table dropped by 005.
--
-- Background: 005_retire_legacy_auth.sql DROPPED the sessions table on
-- the assumption that an RFC 006 follow-up slice would also rip out the
-- session-cookie code path in pkg/pipelineapi/auth/session.go +
-- pkg/pipelineapi/http/auth_handlers.go. That cleanup never landed, so
-- POST /api/v1/auth/login still tries to INSERT INTO sessions and 500s
-- with "could not create session". This migration restores the table so
-- the cookie-auth path works again, matching the production behaviour
-- documented in CLAUDE.md ("User auth is session-cookie").
--
-- Schema is byte-for-byte the same as 001_init.sql's original, so a
-- fresh-deploy install ends up in the same shape regardless of whether
-- the user has 001+005+008 history or a future-cleaned 001 that never
-- creates the table at all.

CREATE TABLE IF NOT EXISTS sessions (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL,
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sessions_user    ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
