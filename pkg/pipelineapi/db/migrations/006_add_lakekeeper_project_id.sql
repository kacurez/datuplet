-- 006_add_lakekeeper_project_id.sql — RFC 006 Slice 3.
--
-- Reverses RFC 007's "single shared lakekeeper Project" shortcut. Each Datuplet
-- project now maps 1:1 to a lakekeeper Project; the ID is allocated by
-- lakekeeper at create-project time (POST /management/v1/project) and stored
-- here.
--
-- Default '' is the "not yet provisioned" sentinel. The admin create-project
-- subcommand fills this in immediately after the lakekeeper round-trip; the
-- ensure-project-authz subcommand back-fills any rows that lost their
-- companion lakekeeper Project (e.g. crashed mid-create).
--
-- Empty string (rather than NULL) keeps queries simple — no NULLIF dance —
-- and makes the "missing" case grep-able. We deliberately avoid a uuid type
-- so the column can carry the empty-string default without a NOT NULL
-- violation on the migration step itself.

ALTER TABLE projects
    ADD COLUMN IF NOT EXISTS lakekeeper_project_id text NOT NULL DEFAULT '';
