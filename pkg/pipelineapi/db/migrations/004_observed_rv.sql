-- 004_observed_rv.sql — RFC 003: monotonic rv guard for the observer.
--
-- observed_rv holds metadata.resourceVersion of the most recent
-- PipelineRun snapshot the observer has mirrored into this row. The
-- store.UpdateRunPhase SQL guard `($7 = 0 OR $7 > observed_rv)` drops
-- out-of-order reconciles that arrive with a stale rv. Default 0 lets
-- the first observer write for an existing row succeed.
ALTER TABLE runs
    ADD COLUMN observed_rv BIGINT NOT NULL DEFAULT 0;
