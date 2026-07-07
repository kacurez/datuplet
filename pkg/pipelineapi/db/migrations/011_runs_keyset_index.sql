-- 011_runs_keyset_index.sql — composite index for keyset-paginated runs list.
-- (project_id, created_at DESC, id DESC) makes the keyset scan + id tiebreaker
-- fully index-ordered; the existing (project_id, created_at DESC) leaves the id
-- sort to a heap step.
CREATE INDEX idx_runs_project_created_id ON runs(project_id, created_at DESC, id DESC);
