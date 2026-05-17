-- 005_retire_legacy_auth.sql — RFC 006 Slice 2 stop-the-world cutover.
--
-- Pre-FGA auth tables are retired. OpenFGA tuple deletion is now the
-- cancellation mechanism (no separate denylist needed). Session cookies
-- and project_memberships are replaced by FGA-based auth (Slices 4+7).
--
-- The DROP TABLE...IF EXISTS form is safe for:
--   - fresh deploys  (tables never created — 003 is now a no-op; sessions
--     + project_memberships are dropped as soon as this migration runs
--     after 001 creates them — see comment below)
--   - existing deploys (tables exist and are dropped here)
--
-- NOTE: On fresh deploys, 001_init.sql still creates sessions and
-- project_memberships (we preserve the file verbatim to avoid schema
-- divergence with existing Postgres DBs). This migration immediately
-- drops them. The tables exist only transiently in a fresh install.
-- 001 is preserved unchanged; Slice 7 will clean up the dead DDL.
--
-- revoked_tokens: may or may not exist depending on when the deploy was
-- originally created (003 was a no-op on fresh deploys from Slice 2
-- onward, but may have been created before Slice 2 on older installs).

DROP TABLE IF EXISTS revoked_tokens CASCADE;
DROP TABLE IF EXISTS sessions CASCADE;
DROP TABLE IF EXISTS project_memberships CASCADE;
