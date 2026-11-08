# Iceberg table compaction — design

Date: 2026-11-08. Status: revision 4 after three Codex rounds (final round approved the architecture). Scope: v1, deliberately small.
Author: architecture agent (Fable 5.1); revised by the maintainer's session. Maintainer decision: on-demand runs use a hidden saved pipeline (§8), not runs without a pipeline.

## 0. Revision 2 changes (Codex round 1)

| # | Finding | Resolution |
|---|---|---|
| 1 | Re-staging after 409 can resurrect deleted rows | §6 "Retry rule": reuse written files only if every snapshot since the planning baseline is a pure append; otherwise discard and report `SUPERSEDED` |
| 2 | Cleanup can delete committed data | §6 "Cleanup rule": delete written files only before any commit is submitted or after a definitive 409 rejection; every other post-submission outcome keeps them |
| 3 | `RewriteFiles.Commit` only stages | §5/§6: each attempt calls `RewriteFiles.Commit` then `Transaction.Commit` |
| 4 | Idempotency key unstable across Job retries | §6: key = hash(run ID, stage, component, bucket, table); looked up before planning |
| 5 | Output check is bucket-wide | §5/§7: `CompactTable` requires an exact `(bucket, table)` entry in `output_tables` |
| 6 | Nullable `runs.pipeline_id` is underspecified | §8 rewritten: hidden saved pipeline per table, triggered through the existing `TriggerRun` |
| 7 | Memory bound false with concurrency 2 | §4/§5/R1: scan concurrency 1; bound restated; spike covers skewed compression |
| 8 | Wrong references; preview wording | §3, §7 fixed |

Revision 3 (Codex round 2: 3 Important, 2 Nit):

| # | Finding | Resolution |
|---|---|---|
| R2-1 | Gateway `ComponentName` is `<mode>-component`, not the instance name | §6: controller writes `component_name: <comp.Name>` into the gateway YAML; loader reads it; boot keeps it when set |
| R2-2 | Append-only rule needs ancestry proof; summaries omit zero counters | §6: `table.AncestorsBetween` with `baseFound` required; per-snapshot manifest inspection, not summary counters |
| R2-3 | "One at a time" racy; ordinary trigger route bypasses it | §8: per-pipeline Postgres advisory lock around check → doc replace → `TriggerRun`; ordinary route refuses system pipelines |
| R2-4 | Delete rules select large files, breaking the memory bound | §4: v1 disables delete-based selection; size-only candidates |
| R2-5 | Wrong PipelineRun length | §8: defer to `PipelineRunName()` |

Revision 4 (Codex round 3, final: 2 Important, 1 Nit; architecture approved):

| # | Finding | Resolution |
|---|---|---|
| R3-1 | Blocking advisory lock can exhaust the pool `TriggerRun` needs | §8: non-blocking `pg_try_advisory_lock` (busy → 409); detached-context unlock, connection destroyed if unlock fails |
| R3-2 | Manifest-read failure reported as `SUPERSEDED` | §6: proven-unsafe history → `SUPERSEDED` (0); unverifiable history → ≥20; neither commits |
| R3-3 | Naming fallback claim wrong | §8: claim removed |

## 1. Intent

An incremental (APPEND) pipeline leaves one Parquet file per run per table (`CloseWriter` dispatches each writer's files to the commit pool, `pkg/datagateway/commit_pool.go:95-171`; a partitioned writer can leave thousands, `docs/known-limitations.md:235-242`). Reads then open one footer per file and every raw-eligible file is a separate HTTP stream. Compaction bin-packs the small live data files of the current snapshot into target-size files and commits one `replace` snapshot. Users run it (1) from the table detail page on demand and (2) as a pipeline stage after an APPEND load.

## 2. Goals / non-goals / anti-goals

**Goals**
- G1. Bin-pack small data files per partition into ~target-size files; one atomic snapshot per run; row count and data unchanged (deletes applied, if any).
- G2. Zero new credential surface: all storage I/O and the commit happen in the Data Gateway sidecar under the run token's lakekeeper-vended credentials, exactly like today's commits.
- G3. Nothing staged on local disk; memory bounded by one input task being decoded (inputs are small files by definition, < 75 % of target size) plus one open Parquet writer, because tasks are read with scan concurrency 1 (§5, R1).
- G4. Safe under concurrent appends (retry) and concurrent replaces (give up cleanly, no duplicate data, no half-committed state).
- G5. Zero-op is a success: a table with nothing to compact costs one plan and no snapshot.
- G6. Pipeline stage is an ordinary component in a PipelineDoc; on-demand is an ordinary run visible in Runs, cancellable, with the standard exit-code contract.

**Non-goals (v1)**: expire-snapshots, orphan-file cleanup, manifest rewrite, sort/z-order, partial-progress (multi-snapshot) rewrites, scheduling/auto-compaction, compaction across spec evolution policy, a dry-run endpoint in pipeline-api.

**Anti-goals**: credentials in the component; pipeline-api doing storage I/O; a new long-running service; K8s object names carrying table names.

## 3. Constraints verified in code

- iceberg-go v0.7.0 (`go.mod:7`) ships a compaction API: `compaction.Config` / `DefaultConfig` / `PlanCompaction` / `Analyze` (`table/compaction/compaction.go:31-191`, `analyze.go:29`), `ExecuteCompactionGroup` (reads tasks with deletes applied, writes via `WriteRecords` with the clustered writer, `table/rewrite_data_files.go:427-544`), `Transaction.NewRewrite` + `RewriteFiles.ApplyResult` + `RewriteFiles.Commit` (`table/rewrite_files.go:90,314,342`), `Table.RewriteDataFiles` / `Transaction.RewriteDataFiles` (`rewrite_data_files.go:95,336`). The rewrite commit registers a conflict validator rejecting new delete files on rewritten data files (`rewrite_data_files.go:815-838`) and the staging step requires the files to delete to exist in the current snapshot (`table/transaction.go:1716,2041`). The rewrite validator only compares against the transaction's own base, so a fresh transaction after a 409 does NOT detect intervening deletes (`table/table.go:634,710`) — hence the §6 retry rule. `RewriteFiles.Commit` only stages; `Transaction.Commit` must follow (`table/rewrite_files.go:85,332`). REST transport, decode and metadata-parse errors after submission are returned unchanged, not as `rest.ErrCommitStateUnknown` (`catalog/rest/rest.go:537,562,1637`); the library itself suppresses cleanup on non-conflict commit errors (`table/table.go:756`). `ExpireSnapshots` (`transaction.go:735`) and orphan cleanup (`table/orphan_cleanup.go`) exist too — out of scope here.
- iceberg-go's rewrite does NOT clean up written files when an atomic commit fails; cleanup is only in partial-progress mode (`rewrite_data_files.go:336-408` vs `cleanupCompactionOutputs` at `:776`). The library documents the coordinator pattern (execute groups yourself, stage with `NewRewrite`, commit) for callers who need control (`rewrite_data_files.go:134-144,426-432`).
- Output files stream through `fs.Create` → gocloud blob writer (`table/internal/parquet_files.go:600`, `io/gocloud/blobfs/blob.go:335-374`); nothing touches local disk. Reads stream as Arrow batches (`Scan.ReadTasks`, `table/scanner.go:2172`; `WithMaxConcurrency`/`WithArrowBatchSize`, `table/table.go:1270,1330`).
- iceberg-go's commit retry loop honours `commit.retry.num-retries` (`table/table.go:640-700`); gateway-created tables set it to 0 (`docs/known-limitations.md:186-190`, `pkg/icebergjob/commit_shared.go:274-278`), so a 409 surfaces immediately as `rest.ErrCommitFailed` and the outer `catalogwriter.RetryOnConflict` (`pkg/catalogwriter/retry.go:66`) is the retry envelope, as for `CommitTableFiles` (`commit_shared.go:259`).
- Rewrite staging does not require `first_row_id` on added files (`transaction.go:1665,1819`: `requireFirstRowID = !cfg.rewriteSemantics`). Row lineage is preserved only when every source file carries it; mixed groups drop `_row_id` with a warning (`rewrite_data_files.go:454-476`).
- The gateway already holds everything compaction needs: a per-run `Resolver` with `LoadTableForScan` (`pkg/datagateway/lakekeeper/lakekeeper.go:218-236`) and `Catalog` (`:243-250`); the commit pool reads footers and writes manifests through `tbl.FS()` (`commit_shared.go:443,641`) under the same credentials.
- The gateway enforces that a component only writes declared outputs (`pkg/datagateway/server_v2_writing.go:140-196`). `outputs.tables[].writeMode` may be empty (`pkg/pipeline/validate/validate.go:566-573`). An `outputs` declaration is what earns the run identity its `editor` tuple (`pkg/pipelineapi/runbackend/k8s.go:502-520`, `pkg/pipelineapi/tokens/capabilities.go:42-64`).
- Run machinery: a Job always has a `component` container plus the gateway init-sidecar; exit code is read from the `component` container (`pkg/k8s/controllers/pipelinerun_jobs.go:48-330`, `pipelinerun_status.go:52`). `PipelineRunSpec.PipelineSpec` (inline doc, no Pipeline CR) is supported (`pkg/k8s/api/v1/pipelinerun_types.go:59-62`, `pipelinerun_controller.go:364-368`) and used by previews (`pkg/pipelineapi/k8s/preview_create.go:52-63`).
- `runs.pipeline_id` is `NOT NULL ... ON DELETE CASCADE` (`pkg/pipelineapi/db/migrations/002_pipelines_runs.sql:31`); every runs query inner-joins `pipelines` (`pkg/pipelineapi/store/run.go:77-81,180-184`; `pkg/pipelineapi/k8s/db_updater.go:37-57`). Using a saved pipeline keeps all of this unchanged (§8).
- The gateway's existing output check authorizes any table in a declared bucket (`server_v2_writing.go:113,194`, pre-existing); the editor tuple is project-wide (`runbackend/k8s.go:502`). Compaction must therefore enforce the exact target itself (§5).
- iceberg-go scans deliver tasks in order; with concurrency > 1 a fast task can be held fully decoded while an earlier one is read (`table/scanner.go:2164`; memory formula `rewrite_data_files.go:262`).
- Storage routes: `data_admin` gate pattern (`pkg/pipelineapi/storage/delete_table.go:80-124`), catalog proxy (`catalog_proxy.go:54-92`), routes in `pkg/pipelineapi/http/server.go:566-590`. UI table header with Delete: `ui/product/pages/storage-table.js:60-91`; run detail polls `/runs/{id}` (`ui/product/pages/run-detail.js:1-11`).
- The in-flight naming plan (`docs/superpowers/plans/2026-11-07-k8s-object-names-from-ids.md`) makes PipelineRuns `run-<uuid>`.
- The query engine refuses `iceberg_rewrite_data_files` commits and leaves orphan metadata files (`docs/known-limitations.md:362`); sql-transform is credentials-clean.

## 4. What compaction means here

- **Operation**: bin-pack rewrite of the current snapshot's live data files, per partition, using `compaction.Config` with Datuplet defaults: `TargetFileSizeBytes` = the pipeline's `gateway.targetFileSize` (128 MiB default, `pkg/pipeline/config/types.go:161`, forwarded as `target_file_size`, `pkg/datagateway/config.go:39`) unless overridden; `MinFileSizeBytes` = 75 %, `MaxFileSizeBytes` = 180 %, `MinInputFiles` = 5 (library defaults, `compaction.go:104-116`). **Size-only selection in v1**: `DeleteFileThreshold = math.MaxInt` and `DeleteRatioThreshold = 0`, which disables the two delete-based rules that pick files regardless of size (`compaction.go:288,300`). Every candidate is then < 75 % of target size, which the memory bound (G3, R1) relies on. Delete-driven compaction is a follow-up (Datuplet writes no deletes today).
- **Partition-aware**: groups are single-partition (`compaction.go:149-160`); output under the table's current spec.
- **Delete files**: Datuplet writes none today. Foreign position deletes / DVs are applied while reading and removed when dead (`rewrite_data_files.go:840-900`). Equality-delete cleanup is NOT done in v1.
- **Schema evolution**: tasks read projected to the current schema; compacted files may become raw-eligible. UNVERIFIED: writer physical types vs `pkg/datagateway/goplane/eligibility.go`; Task 1 asserts it.
- **Row lineage (v3)**: preserved only when every source file has `first_row_id`.
- **Expire snapshots / orphan cleanup: out of scope.** v1 reduces the current snapshot's file count but does not reclaim storage. The UI/CLI message says so.
- **Bounded work per run**: at most `max_input_files` (default 2000) input files per run; status reports files deferred to a later run.

## 5. Where it executes

| Option | Creds | Disk/memory | Surface | Verdict |
|---|---|---|---|---|
| **A. Data Gateway operation (`CompactTable` RPC) driven by a thin built-in component `iceberg-compactor`** | Sidecar already holds run token + vended creds; component credentials-clean | Streams S3→Arrow→Parquet→S3; no local disk | +1 RPC, +1 SDK method, +1 `icebergjob` function, +1 ~120-LOC component, +1 chart template | **Pick** |
| B. Gateway-only Job (no component container) | Same as A | Same as A | Controller + CRD changes; exit code read from `component` (`pipelinerun_status.go:52`) | Reject |
| C. pipeline-api in-process | 60 s impersonation JWTs; pipeline-api holds no storage creds | Heavy I/O in the HTTP service; no cancel/retry/isolation | — | Reject |
| D. DuckDB `iceberg_rewrite_data_files` | sql-transform credentials-clean; query-worker read-only | — | — | Reject (`known-limitations.md:362`) |

**Shape**
- `pkg/icebergjob/compact.go` — `CompactTable(ctx, cat, ident, CompactOpts, auditProps, operationKey) (*CompactResult, error)`: load → if a snapshot carries `datuplet.commit-key == operationKey`, return `ALREADY_COMMITTED` with its summary (no planning) → record the baseline snapshot ID → `PlanCompaction(tbl.Scan().PlanFiles())` → truncate to caps → per group `ExecuteCompactionGroup` (target size, **scan concurrency 1**, Arrow batch 32768) → commit loop per §6 (each attempt: `txn := fresh.NewTransaction()`; `txn.NewRewrite(props).ApplyResult(gr)…Commit(ctx)` to stage; then `txn.Commit(ctx)`, whose returned table gives the new snapshot) → cleanup only per the §6 cleanup rule.
- `pkg/datagateway/server_v2_compact.go` — RPC handler: **exact target check** — the request's `(bucket, table)` must equal an entry of the component's explicit `output_tables`; a `defaultBucket`/`buckets`-only declaration or a non-matching table is `PermissionDenied` (fail closed; does not reuse the bucket-wide writer check). Plus `isCancelled()` gate, plane-lifetime context, `Resolver.Catalog`, metrics, logs.
- `api/proto/gateway/v2/gateway.proto` — `rpc CompactTable(CompactTableRequest) returns (CompactTableResponse)`; generated Go checked in.
- `sdk/go/client.go` — `func (c *Client) Compact(ctx, bucket, table string, opts CompactOptions) (*CompactResult, error)`.
- `components/iceberg-compactor/` — `main.go`, `config.go`, `schema.json`, `Dockerfile`, `go.mod`.

The rolling Parquet writer runs in the sidecar, which has no explicit resource requests (`pipelinerun_jobs.go:300-303`). See R1.

## 6. Commit semantics

- **Atomic replace**: one `replace` snapshot per run (`transaction.go:1727-1729`). Summary carries standard audit keys (`icebergjob.BuildSnapshotSummary`, `pkg/datagateway/server_v2.go:263`) plus `datuplet.commit-key` and `datuplet.compaction.{files-before,files-after,bytes-before,bytes-after}`.
- **Operation key (idempotency)**: `operationKey = sha256(runID, componentName, bucket, table)` — stable across Job retries (`BackoffLimit 1`, `pipelinerun_jobs.go:167`) and distinct for two compactions of the same table in one run (component names are unique across a pipeline, `pkg/pipeline/validate/validate.go:233`). Today the gateway's `ComponentName` (`pkg/datagateway/config.go:10`) is NOT the instance name: the controller does not emit it (`pkg/k8s/controllers/pipelinerun_jobs.go:528`), the loader leaves it unset (`pkg/datagateway/gatewayconfig.go:145`) and boot assigns `opts.mode + "-component"` (`cmd/datuplet/gateway.go:221`). This design adds the wiring: the controller writes `component_name: <comp.Name>` into the gateway YAML, the loader reads it, and boot only falls back to `<mode>-component` when it is empty. The key uses that instance name; if it is empty at `CompactTable` time the RPC fails ≥20 rather than computing a shared key. Looked up with `findSnapshotByCommitKey` (`commit_shared.go:332-341`) **before planning** and before every commit attempt; a hit means the operation already landed → `ALREADY_COMMITTED`, exit 0, no second snapshot.
- **Retry rule (fixes data resurrection)**: on 409 (`rest.ErrCommitFailed`, definitive rejection), reload the table once; that same fresh table is used both for the check below and to build the next transaction. Proof of a pure-append history: `table.AncestorsBetween(currentID, baselineID, metadata.SnapshotByID)` (`table/snapshot_ancestry.go:83-103`) must return `baseFound == true` — a rollback past the baseline, a branch reset, or expired intermediate snapshots all fail this and count as unsafe. For each snapshot in that chain, safety is decided from its manifests, not its summary counters (iceberg-go omits zero counters, `table/snapshots.go:151-167`): the snapshot is a pure append iff `Summary.Operation == append` and none of the manifests it added (`added_snapshot_id == snapshot.ID`) has content type `deletes`. If every snapshot passes, re-stage the same group results on that fresh transaction (inputs unchanged, appended files untouched). Two failure outcomes, neither of which commits: (a) history **proven** incompatible (`baseFound == false`, a non-append operation, or a deletes manifest added) → the `SUPERSEDED` path below, exit 0; (b) history **unverifiable** (`Snapshot.Manifests` or table reload returns an I/O/credential error, `table/snapshots.go:407`) → fail ≥20 "could not verify concurrent changes", written files deleted (no submission could have succeeded). For case (a), do not reuse the results: delete the written files (safe — every submission so far was rejected) and report `SUPERSEDED`, exit 0. The same check runs before the first attempt (between planning and the first transaction). Bounded by the default `RetryOpts` (`retry.go:20-23`); exhaustion → ≥20.
- **Missing inputs**: if staging fails because an input file is gone (`transaction.go:1716,2041`), report `SUPERSEDED` only when that specific error is recognised; any other staging error is ≥20.
- **Cleanup rule (fixes deletion of committed data)**: `CompactTable` tracks whether any `txn.Commit` was submitted. Written files are deleted only when (a) no commit was submitted (planning/writing failed, cancel before submission, zero-op), or (b) every submission ended in a definitive 409 and the result is `SUPERSEDED` or retry exhaustion. Any other outcome after a submission — transport error, response decode error, metadata parse error, `ErrCommitStateUnknown`, cancellation while a commit is in flight — keeps all written files: reload once and check the operation key; if found, success; else fail ≥20 with "commit outcome unknown; written files kept". Orphans in that case are accepted (R3).
- **Zero-op**: no groups → `NOTHING_TO_DO`, exit 0.
- **Cancellation**: `Interrupt()` cancels the plane context; files are removed only under the cleanup rule. Hard kill leaves orphans (same class as R3b, `known-limitations.md:264-266`).

## 7. Pipeline integration

Component `iceberg-compactor`, `io: {inputs: none, outputs: required}` (`pkg/k8s/api/v1/component_types.go:60-75`). The target table is the declared output: the `editor` tuple derivation applies unchanged, and the gateway's `CompactTable` enforces the exact `(bucket, table)` (§5). `writeMode` omitted and ignored.

```yaml
stages:
  - name: load
    components:
      - name: extract
        component: http-json-extractor
        config: { url: https://…, table_name: events }
        outputs: { tables: [ { name: events, bucket: raw, writeMode: APPEND } ] }
  - name: maintain
    components:
      - name: compact
        component: iceberg-compactor
        config: { target_file_size_mb: 128 }
        outputs: { tables: [ { name: events, bucket: raw } ] }
```

`components/iceberg-compactor/schema.json` (Form Subset):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Iceberg Compactor",
  "description": "Bin-packs the small data files of each declared output table into target-size files in one atomic snapshot.",
  "type": "object", "additionalProperties": false,
  "properties": {
    "target_file_size_mb": { "type": "integer", "minimum": 16, "maximum": 2048, "default": 128,
      "description": "Target size of a compacted Parquet file in MiB.",
      "x-datuplet-doc": "Files smaller than 75% of this are compaction candidates; files larger than 180% are never rewritten. Defaults to the pipeline's gateway.targetFileSize." },
    "min_input_files": { "type": "integer", "minimum": 2, "default": 5, "x-datuplet-advanced": true,
      "description": "Minimum number of small files in one partition before it is worth rewriting." },
    "max_input_files": { "type": "integer", "minimum": 1, "default": 2000, "x-datuplet-advanced": true,
      "description": "Upper bound on input files rewritten in one run; the rest is left for a later run." },
    "dry_run": { "type": "boolean", "default": false,
      "description": "Plan only: report what would be compacted and commit nothing." }
  }
}
```

Component: read `client.RawConfig().Outputs`; require explicit `tables` (else exit 1); call `client.Compact` per table; one `DUPLET_STATUS_MESSAGE:` line; exit per §9. Registration: chart template `charts/datuplet-app/templates/components/iceberg-compactor.yaml`, synced schema, Makefile image targets (`Makefile:54-84`), release image list (`.github/workflows/_release-components.yml:16`; `release.yml:62` delegates there), e2e bootstrap (`tests/e2e/framework/components_bootstrap.go:150-155`). Previews: preview rewrites outputs to a scratch namespace without copying existing tables (`pkg/pipelineapi/preview/rewrite.go:96`), so a compactor stage only does something in a preview if an earlier stage of the same preview wrote that table; a compactor-only preview ends FailedUser "table not found". Documented.

## 8. On-demand from the UI

**Route**: `POST /api/v1/storage/projects/{pid}/tables/{ns}/{t}/compact` (optional body `{"target_file_size_mb": 128}`), next to `DELETE` (`server.go:588`). Handler `pkg/pipelineapi/storage/compact_table.go`, modelled on `DeleteTable`: `data_admin` via `Gate.AuthorizeRelation` (`delete_table.go:90`), identifier regex, lakekeeper required (501), load table for a clean 404, then:

1. **Hidden saved pipeline, one per table.** Name `sys-compact-<hex16>`, where `hex16 = sha256(bucket + "\x00" + table)[:16]` (a valid DNS-1123 label whatever the table identifier contains). Doc: one stage `compact`, one component `compact` of type `iceberg-compactor`, `outputs.tables: [{bucket: ns, name: t}]`, `description: "Compaction of ns.t"`, config from the request body. Validate with `validate.ValidatePipelineDoc` (503 `compactor_unavailable` if the component is not registered) before taking the lock.
2. **Atomic admission, one at a time per table.** Acquire one pooled connection and call the **non-blocking** `pg_try_advisory_lock(hashtextextended(project_id || '/' || name, 0))`. `false` → release the connection and return 409 `compaction_running` immediately, so waiting requests never hold connections and at most one connection per table is pinned while `TriggerRun` uses the pool for its own queries (requires pool `MaxConns ≥ 2`; pgxpool's default is `max(4, NumCPU)`). Release in a `defer`: `pg_advisory_unlock` with a detached context (`context.WithoutCancel` + short timeout); if the unlock fails, close the underlying connection (`conn.Hijack()` then `Close`) instead of returning it to the pool, because releasing a pgx pooled connection does not drop session locks (`pgxpool/conn.go:20`). While holding the lock, the handler, in this order: (a) checks for a Pending/Running run of that pipeline and returns 409 `{"error":"compaction_running","run_id"}` **without touching the saved doc**; (b) upserts the pipeline row with `system = true` and the new doc; (c) calls `backend.TriggerRun`, which applies the Pipeline CR and inserts the run row (`runbackend/k8s.go:216,273`). The lock is held until `TriggerRun` returns, so a second request cannot replace the CR between another request's apply and its operator admission's read of a just-inserted run; the second request then sees the first run as Pending and gets 409. The run is reserved (row inserted) before the lock is released.
3. **Single entry point.** The ordinary `POST /api/v1/projects/{pid}/pipelines/{name}/runs` (`run_handlers.go:73,116`) refuses system pipelines with 409 `{"error":"system_pipeline","hint":"use POST …/tables/{ns}/{t}/compact"}`, so every compaction run goes through the lock. Cancel uses the ordinary run cancel route unchanged.
4. **Trigger through the existing `backend.TriggerRun`** — same tuple/token/K8s ordering, cancel path, timeline handling and `db_updater` behaviour as any run. No change to `runs`.

Response `202 {"run_id","pipeline":"sys-compact-…","target":"ns.t"}`.

**Schema + API changes for hiding** (fresh installs only, new migration file `015_pipelines_system.sql`): `ALTER TABLE pipelines ADD COLUMN system BOOLEAN NOT NULL DEFAULT false`.
- `GET /pipelines` excludes `system = true` rows (no opt-in flag in v1).
- `PUT /pipelines/{name}` rejects names starting with `sys-` (400 `reserved_name`), so users can neither create nor overwrite a system pipeline; `GET /pipelines/{name}` still returns it (read-only view from a run's link).
- `DELETE /pipelines/{name}` on a system pipeline → 409 `system_pipeline` (deleting would cascade-delete its run history, `002_pipelines_runs.sql:31`).
- Runs list and run detail are unchanged; they show the pipeline name `sys-compact-…`, and the run JSON additionally carries the pipeline `description` so the UI can show "Compaction of ns.t".
- PipelineRun naming: whatever `PipelineRunName()` produces (`pkg/pipelineapi/k8s/run_create.go:54`); after the naming plan (`docs/superpowers/plans/2026-11-07-k8s-object-names-from-ids.md`) it is `run-<uuid>`. No dependency on that plan.

**UI**: `Compact` button beside `Delete` in `storage-table.js` (`:70-73`); confirm → `api.js compactTable()` → redirect to `/ui/runs/<run_id>`. Runs list shows the description for system pipelines instead of the raw name.

**CLI**: `datuplet storage compact <ns.table> [--wait] [--timeout] [--json] [--target-file-size-mb]` in `cmd/datuplet/storage.go`, reusing `pollUntilTerminal`/`emitResult` (`cmd/datuplet/trigger.go:291,320`).

## 9. Observability

- `CompactTableResponse`: `status` (`COMPACTED|ALREADY_COMMITTED|NOTHING_TO_DO|SUPERSEDED|DRY_RUN`), `files_before`, `files_removed`, `files_added`, `files_after`, `files_skipped`, `files_deferred`, `groups`, `bytes_before`, `bytes_after`, `snapshot_before`, `snapshot_after`, `duration_ms`.
- Component stdout: one `DUPLET_STATUS_MESSAGE:` summary line (`pkg/lib/status/status.go:47`).
- Gateway logs per group + summary; Prometheus `datagateway_compaction_*` metrics in `commit_metrics.go` style.
- **Exit codes**: `0` COMPACTED / ALREADY_COMMITTED / NOTHING_TO_DO / SUPERSEDED / DRY_RUN; `1` bad config, non-explicit outputs, table not found, not an exact declared output, unsupported file; `>=20` conflict exhaustion, storage/catalog errors, cancellation, unknown commit outcome.

## 10. Failure modes, risks, out of scope

- **R1 Sidecar memory/CPU** — gateway container has no explicit resources (`pipelinerun_jobs.go:191-215,300-303`). Scan concurrency 1 removes out-of-order buffering (`scanner.go:2164`); the remaining bound is one decoded input task + one writer row group + materialized deletes (none today). With size-only selection (§4) every input file is < 75 % of target size, but a highly compressed small file can decode large: the Task 1 spike measures peak RSS on skewed inputs (one 90 MiB high-compression file among small ones) and sets a documented sidecar memory expectation. `GOMEMLIMIT` is not relied on as a bound. UNVERIFIED whether LimitRange covers init containers.
- **R2 Vended-credential TTL** (`known-limitations.md:50-63`) — `max_input_files` cap; stop taking groups past 50 % TTL. Follow-up: renewing `VendedCreds` (`pkg/datagateway/goplane/storage.go:139-208`).
- **R3 Orphans on crash/cancel** — no orphan cleanup (out of scope).
- **R4 No storage reclaim** without expire-snapshots.
- **R5 Duplicate work** — loser ends `SUPERSEDED`; API 409 gate covers on-demand only.
- **R6 Library maturity** — iceberg-go compaction unexercised against Lakekeeper; Task 1 starts with a spike.
- **R7 Mixed row lineage** drops `_row_id`.
- **R8 Long unary RPC** — SDK must not set a short deadline.
- Out of scope: expire-snapshots, orphan cleanup, rewrite-manifests, sort orders, eq-delete cleanup, partial-progress rewrites, dry-run API, scheduling/auto-compaction, "compact after every run" sugar, builder picker for compactor outputs.

## 11. Task breakdown

1. **`icebergjob.CompactTable` + spike** — `pkg/icebergjob/compact.go`, `compact_test.go` (integration tag, Lakekeeper fixture). Tests: 12 small files → 1 per partition, row count equal, old snapshot still lists 12; concurrent `AddFiles` between plan and commit → re-staged, appended file survives; concurrent non-append snapshot (`ReplaceDataFiles`, or a snapshot adding a position-delete file) between plan and commit → `SUPERSEDED`, written files deleted, no rows resurrected; rollback of the table to the baseline's parent between plan and commit (no new snapshot) → `SUPERSEDED` via `baseFound == false`; an append snapshot whose summary lacks the delete counters but whose manifests are data-only → re-staged; manifest read failing during the check (injected FS error) → ≥20, nothing committed, written files deleted; a file with many deletes but ≥ min size is not selected (size-only config); same operation key on a second call (simulated Job retry, different plan) → `ALREADY_COMMITTED`, exactly one replace snapshot; injected transport error after submission (catalog wrapper whose commit succeeds then returns an error) → written files kept, result ≥20 or success via key lookup; the plan's call sequence includes `txn.Commit` (snapshot ID changes); compacted files pass raw eligibility; peak RSS on skewed inputs recorded.
2. **Gateway RPC + SDK + component-name wiring** — proto (+generated), `server_v2_compact.go`, `commit_metrics.go`, `sdk/go/client.go`, `pkg/k8s/controllers/pipelinerun_jobs.go` (emit `component_name`), `pkg/datagateway/gatewayconfig.go` (read it), `cmd/datuplet/gateway.go:221` (fallback only when empty). Tests: gateway YAML built for components `compact-a` and `compact-b` carries each instance name and the loaded config keeps it; two compactor components on the same table in one run get different operation keys (second one compacts, not `ALREADY_COMMITTED`); empty component name → RPC fails ≥20; `(raw, other)` when only `raw.events` is declared → PermissionDenied; `defaultBucket`-only declaration → PermissionDenied; exact `(raw, events)` → seam called with right ident, `target_file_size` and operation key derived from run ID + component name; cancel → `errRunCancelled`.
3. **Component** — `components/iceberg-compactor/*`, Makefile, workflows, chart template + synced schema, e2e bootstrap, `docs/components.md`. Tests: schemalint; exit-code table against a fake gateway; `make sync-component-schemas` clean.
4. **Pipeline e2e** — `tests/e2e/pipelines/k8s/compaction.yaml`, `scenarios_compaction_test.go`, `AssertDataFileCount`. Test: row count unchanged, file count ≤ 8, latest snapshot `replace` with commit key.
5. **On-demand route via hidden pipeline** — migration `015_pipelines_system.sql`, `store` pipeline upsert with `system`, list filter, `pipeline_handlers.go` (reserved `sys-` prefix on PUT, 409 on DELETE), run JSON `description`, `storage/compact_table.go`, server.go. Tests: handler 403 (no `data_admin`) / 404 / 503 / 409 running / 202; second compaction of the same table reuses the same `sys-compact-<hex16>` pipeline; `GET /pipelines` hides it; `PUT /pipelines/sys-x` → 400; `DELETE` of a system pipeline → 409; `POST /pipelines/sys-compact-…/runs` → 409 `system_pipeline`; two concurrent compact requests for the same table (DB-backed, fake backend that blocks in `TriggerRun`) → exactly one 202 and one 409, and the saved doc equals the winner's config; with a pool of `MaxConns = 2` and 10 concurrent requests, the call completes (no stall) and 9 get 409; lock is released after a handler error (next request succeeds); a 409 request leaves the saved doc unchanged; DB-backed test that the triggered run lists and cancels like any run.
6. **UI + CLI** — api.js, storage-table.js, runs.js, run-detail.js, `cmd/datuplet/storage.go` + test, agent-quickstart. Test: CLI posts + polls; UI contrast test green; manual check.
7. **Docs + on-demand e2e** — pipeline-api.md, known-limitations.md, CHANGELOG, architecture.md, `TestCompaction_OnDemand`.
