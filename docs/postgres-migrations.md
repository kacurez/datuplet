# Postgres Migrations

Every Datuplet Postgres consumer (lakekeeper, openfga, pipeline-api) runs its migrations
before its Pod becomes Ready. The invariant: by the time `helm install --wait --wait-for-jobs`
returns for the owning layer, all three databases are at their current schema. Migrations are
idempotent and multi-replica-safe via DB-side advisory locks, so concurrent runners and Pod
restarts are safe — the second runner just finds an up-to-date schema and exits 0.

## Library mechanisms per service

| Service | Mechanism | Who runs it |
|---|---|---|
| pipeline-api | `pg_advisory_lock(20260419)` in `pipeline-api admin migrate`; chart pre-install hook Job `<release>-pipeline-api-migrate` | Chart-managed (our code) |
| openfga | golang-migrate; subchart's `datastore.migrationType: initContainer` (init container on the openfga Pod, before main container starts) | openfga subchart |
| lakekeeper | sqlx + `_sqlx_migrations` table; upstream chart's revision-named migration Job + `check-db -dm` init container on the catalog Pod | lakekeeper-charts subchart |

The pipeline-api migration Job is a helm `pre-install,pre-upgrade` hook — it completes before
main resources apply, so the pipeline-api Deployment never races with an unmigrated DB. Openfga
and lakekeeper use their subcharts' own ordering patterns (init container or revision-named Job);
the result is the same: consuming Pods don't start until migration is done.

## Conventions for pipeline-api migrations (the one we own)

**Idempotent.** Every migration is safe to re-run. The advisory lock + version table ensure
concurrent runs don't double-apply; the `migrate` subcommand exits 0 if schema is current.

**Multi-replica safe.** `pg_advisory_lock(20260419)` serializes concurrent runners at the DB
level. A second runner blocks until the first finishes, then finds the schema up-to-date and
exits 0 without applying anything.

**Append-only.** Never edit a committed migration file. The version table records applied
checksums; a changed file fails with a checksum mismatch on the next run. Add a new migration
instead.

**Deprecate-then-drop.** Never drop a column in the same release that stops writing to it:

- Release N: ship code that stops reading/writing the column (backward-compatible change).
- Release N+1: ship migration that drops the column.

This allows a rolling upgrade during N+1: old N Pods (still running during the rollout) won't
crash because the column they expect still exists.

**Split long-locking migrations.** Avoid migrations that hold table locks for a long time.
Break them into three releases:

1. `ADD COLUMN ... NULLABLE` (cheap, near-instant lock).
2. Backfill data in batches (no DDL lock).
3. `ALTER ... SET NOT NULL` or add index (once backfill complete).

**Forward-tolerant runners.** A pipeline-api Pod that restarts during an upgrade may see a DB
at schema version N+1 while the binary was compiled against N. The `migrate` subcommand
detects "schema is ahead of my version" and warns rather than crashes, so the Pod starts
successfully. Backward-compatibility from the deprecate-then-drop discipline ensures the Pod
functions correctly at runtime until the rolling upgrade terminates it.

## Further reading

- `pkg/pipelineapi/store/migrations/` — migration SQL files and the advisory-lock wrapper.

