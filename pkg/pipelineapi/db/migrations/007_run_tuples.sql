-- 007_run_tuples.sql — RFC 006 Slice 4+5.
--
-- run_tuples is a crash-recovery log for FGA tuples written at run-trigger
-- time. The compensating sequence (see runbackend/k8s.go::TriggerRun and
-- runbackend/local.go::TriggerRun) is:
--
--   1. INSERT a run_tuples row recording intent (committed=false).
--   2. WriteTuples to OpenFGA.
--   3. INSERT the runs row + create the K8s PipelineRun CR.
--   4. UPDATE the run_tuples row to committed=true.
--
-- Crash modes:
--   - Crash between (1) and (2): run_tuples row exists with committed=false
--     and no FGA tuples. Reaper (Slice 8) deletes the row.
--   - Crash between (2) and (3): FGA tuples exist + run_tuples is
--     committed=false; reaper deletes both.
--   - Crash between (3) and (4): everything live, just committed=false;
--     reaper observes the live runs row and self-heals committed=true.
--
-- The completion flow (Succeeded / Failed / Cancelled) reads the tuples
-- column to know which FGA tuples to DELETE before nulling the row.
--
-- tuples is jsonb so we can store an array of arbitrary tuple shapes
-- (user, relation, object) without committing to a schema for them.
-- The reaper interpretation is: each top-level array element is one
-- authz.Tuple in {"user":"<sub>","relation":"<rel>","object":"<typ>:<id>"}.

CREATE TABLE IF NOT EXISTS run_tuples (
    run_id     uuid PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    tuples     jsonb NOT NULL,
    committed  boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Reaper sweep predicate: "WHERE committed = false AND created_at < now() - 5m".
-- Index supports the planner's choice of a sequential or partial scan
-- depending on data volume.
CREATE INDEX IF NOT EXISTS idx_run_tuples_committed_created_at
    ON run_tuples (committed, created_at);
