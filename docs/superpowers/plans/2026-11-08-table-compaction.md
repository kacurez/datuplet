# Iceberg Table Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Users can compact a table's small data files into target-size files, on demand from the table detail page (or `datuplet storage compact`) and as a pipeline stage, in one atomic Iceberg `replace` snapshot.

**Architecture:** A new Data Gateway RPC `CompactTable` does all storage I/O and the commit in the sidecar with the run's vended credentials, using iceberg-go v0.7.0's compaction planner and rewrite API. A thin built-in component `iceberg-compactor` calls it for each declared output table. On-demand compaction saves a hidden per-table pipeline (`sys-compact-<hex16>`, `pipelines.system = true`) and triggers it through the existing `TriggerRun`, guarded by a non-blocking Postgres advisory lock.

**Tech Stack:** Go, iceberg-go v0.7.0 (`table/compaction`, `Transaction.NewRewrite`, `table.AncestorsBetween`), gRPC (gateway v2 proto), pgx/Postgres, controller-runtime, vanilla ES modules UI.

**Spec:** `docs/superpowers/specs/2026-11-08-table-compaction.md` (revision 4). Section references (§N) below point into it; executors read both.

## Global Constraints

- iceberg-go stays at v0.7.0. No new Go module dependencies outside the new component's own module.
- Compaction config: `TargetFileSizeBytes` = component `target_file_size_mb` or the pipeline's `gateway.targetFileSize` (128 MiB default); `MinFileSizeBytes` 75 %, `MaxFileSizeBytes` 180 %, `MinInputFiles` 5, `DeleteFileThreshold = math.MaxInt`, `DeleteRatioThreshold = 0`, scan concurrency 1, Arrow batch 32768, `max_input_files` default 2000.
- Operation key: `sha256(runID, componentInstanceName, bucket, table)`, stored as snapshot summary `datuplet.commit-key`, looked up before planning and before every commit attempt.
- Statuses: `COMPACTED | ALREADY_COMMITTED | NOTHING_TO_DO | SUPERSEDED | DRY_RUN` → exit 0. FailedUser (1): bad config, non-explicit outputs, table not found, not an exact declared output, unsupported file. FailedApplication (≥20): retry exhaustion, unverifiable history, storage/catalog errors, cancellation, unknown commit outcome, empty component name.
- Cleanup: written files are deleted only if no `txn.Commit` was submitted or every submission ended in a definitive 409 (§6). Never otherwise.
- `CompactTable` authorizes only an exact `(bucket, table)` in the component's explicit `output_tables`.
- Hidden pipeline name: `"sys-compact-" + hex(sha256(bucket + "\x00" + table))[:16]`. Public `PUT /pipelines/{name}` rejects the `sys-` prefix (400 `reserved_name`); `DELETE` of a system pipeline → 409 `system_pipeline`; `POST /pipelines/{name}/runs` on a system pipeline → 409 `system_pipeline`; `GET /pipelines` omits system pipelines.
- New migration file only (`015_pipelines_system.sql`); installs are always fresh, no upgrade path.
- Component schema stays in the Form Subset; `make sync-component-schemas` clean.
- No CRD changes. Don't touch `charts/datuplet-app/crds/`.
- `go build ./... && go test ./...` (touched packages) before each commit; `make tidy` for module changes; `make lint` before the PR; `make e2e-k8s` at the end (controller + chart change).

## Review Focus

1. **Concurrent change between plan and commit:** a pure append must be re-staged and survive; any non-append, a rollback, or expired history must not commit; an unreadable manifest must fail ≥20, not report success. Pinned in Task 1.
2. **Ambiguous commit outcome:** after a submitted commit whose response is lost, written files must be kept and the operation key decides success. Pinned in Task 1.
3. **Job retry / two compactors on one table:** a retried pod reports `ALREADY_COMMITTED`; a second compactor component on the same table in the same run compacts again. Pinned in Tasks 1 and 2.
4. **Out-of-scope table:** a component that declared `raw.events` cannot compact `raw.other`. Pinned in Task 2.
5. **Concurrent on-demand requests:** exactly one run starts, the saved doc is the winner's, and a small pool never stalls. Pinned in Task 5.

---

### Task 1: `icebergjob.CompactTable` (library + spike)

**Files:**
- Create: `pkg/icebergjob/compact.go`
- Test: `pkg/icebergjob/compact_test.go` (`//go:build integration`, Lakekeeper fixture as in `commit_partition_test.go`)

**Interfaces:**
- Produces:
  - `type CompactOpts struct { TargetFileSizeBytes int64; MinInputFiles uint; MaxInputFiles int; DryRun bool }`
  - `type CompactStatus string` with constants `CompactCompacted`, `CompactAlreadyCommitted`, `CompactNothingToDo`, `CompactSuperseded`, `CompactDryRun`
  - `type CompactResult struct { Status CompactStatus; FilesBefore, FilesRemoved, FilesAdded, FilesAfter, FilesSkipped, FilesDeferred, Groups int; BytesBefore, BytesAfter int64; SnapshotBefore, SnapshotAfter int64; Duration time.Duration }`
  - `var ErrHistoryUnverifiable = errors.New(...)`, `var ErrCommitOutcomeUnknown = errors.New(...)`
  - `func CompactTable(ctx context.Context, cat catalog.Catalog, ident table.Identifier, opts CompactOpts, auditProps iceberg.Properties, operationKey string) (*CompactResult, error)`
  - `func CompactOperationKey(runID, componentName, bucket, tableName string) string`

- [ ] **Step 1: Spike (time-box one day).** Before the TDD loop, run iceberg-go's `PlanCompaction` → `ExecuteCompactionGroup` → `NewRewrite().ApplyResult().Commit()` → `txn.Commit()` against the Lakekeeper integration fixture on a 12-file table. Confirm Lakekeeper accepts the `replace` snapshot, and record peak RSS on skewed input (one ~90 MiB highly compressible file among small ones). Write the result into the spec's R1/R6 as a short paragraph. If Lakekeeper rejects the snapshot, stop and report to the maintainer.
- [ ] **Step 2: Write the failing tests** in `compact_test.go`:
  - `TestCompactTable_BinPacksPerPartition`: 12 small files over 2 partitions → `Status == CompactCompacted`, 1 file per partition, row count equal, previous snapshot still lists 12 files, latest snapshot operation `replace` with `datuplet.commit-key == key`.
  - `TestCompactTable_NothingToDo`: 2 files → `CompactNothingToDo`, snapshot ID unchanged.
  - `TestCompactTable_ConcurrentAppendRestaged`: an `AddFiles` commit injected between execute and commit (catalog wrapper hook) → `CompactCompacted`, the appended file is still live.
  - `TestCompactTable_NonAppendSuperseded`: a `ReplaceDataFiles` commit injected → `CompactSuperseded`, no written file remains under the table's data prefix, row count equals the replacement's.
  - `TestCompactTable_DeleteManifestSuperseded`: an injected snapshot that adds a position-delete manifest → `CompactSuperseded`, deleted row stays deleted.
  - `TestCompactTable_RollbackSuperseded`: table rolled back to the baseline's parent before commit → `CompactSuperseded`.
  - `TestCompactTable_SummaryWithoutCountersRestaged`: injected append whose summary omits delete counters, data-only manifests → re-staged, `CompactCompacted`.
  - `TestCompactTable_ManifestReadFailure`: FS wrapper fails manifest-list reads during the check → `errors.Is(err, ErrHistoryUnverifiable)`, nothing committed, written files deleted.
  - `TestCompactTable_LostCommitResponse`: catalog wrapper commits then returns a transport error → written files kept; result is success via key lookup (`CompactCompacted`).
  - `TestCompactTable_UnknownOutcomeKeepsFiles`: commit never lands but returns a transport error → `errors.Is(err, ErrCommitOutcomeUnknown)`, written files still present.
  - `TestCompactTable_OperationKeyIdempotent`: second call with the same key after more appends → `CompactAlreadyCommitted`, exactly one `replace` snapshot.
  - `TestCompactTable_SizeOnlySelection`: a file ≥ min size with many deletes is not an input.
  - `TestCompactTable_DryRun`: `CompactDryRun`, groups reported, snapshot unchanged, no files written.
  - `TestCompactTable_OutputRawEligible`: every added file passes the goplane raw-eligibility check.
- [ ] **Step 3: Run** `go test -tags=integration ./pkg/icebergjob/ -run TestCompactTable -v`. Expected: FAIL (undefined `CompactTable`).
- [ ] **Step 4: Implement** `CompactTable` per spec §5 "Shape" and §6. Order: key lookup → baseline snapshot → plan (size-only config) → cap to `MaxInputFiles` group by group → execute groups sequentially → commit loop wrapped in `catalogwriter.RetryOnConflict`, each attempt doing the ancestry + manifest check on the freshly loaded table, staging with `NewRewrite`, then `txn.Commit`. Track `submitted` and whether every submission was a 409 to drive cleanup. Summary props: `auditProps` + `datuplet.commit-key` + `datuplet.compaction.{files-before,files-after,bytes-before,bytes-after}`.
- [ ] **Step 5: Run** the Step 3 command, then `go test ./pkg/icebergjob/...`. Expected: PASS.
- [ ] **Step 6: Commit** `feat(icebergjob): add CompactTable bin-pack rewrite`.

### Task 2: Gateway `CompactTable` RPC, SDK method, component-name wiring

**Files:**
- Modify: `api/proto/gateway/v2/gateway.proto` (+ regenerate checked-in Go per `api/proto/buf.yaml`)
- Create: `pkg/datagateway/server_v2_compact.go`, `pkg/datagateway/server_v2_compact_test.go`
- Modify: `pkg/datagateway/commit_metrics.go` (compaction metrics, §9)
- Modify: `sdk/go/client.go`
- Modify: `pkg/k8s/controllers/pipelinerun_jobs.go` (~line 528, emit `component_name`), `pkg/datagateway/gatewayconfig.go` (~145, read it), `cmd/datuplet/gateway.go:221` (fallback only when empty) and their tests

**Interfaces:**
- Consumes: Task 1's `CompactTable`, `CompactOpts`, `CompactResult`, `CompactOperationKey`.
- Produces:
  - proto `rpc CompactTable(CompactTableRequest) returns (CompactTableResponse)`; request `{bucket, table, target_file_size_bytes, min_input_files, max_input_files, dry_run}`; response fields exactly as spec §9.
  - `func (c *Client) Compact(ctx context.Context, bucket, table string, opts CompactOptions) (*CompactResult, error)` in `sdk/go` (no client-side deadline).
  - Gateway config field `component_name` populated from `comp.Name`.

- [ ] **Step 1: Write the failing tests:**
  - `TestGatewayConfig_CarriesComponentInstanceName` (controllers): gateway YAML for components `compact-a` and `compact-b` contains each name; loader round-trip keeps it; boot fallback to `<mode>-component` only when empty.
  - In `server_v2_compact_test.go`, using a `compactFn` seam like `CommitPoolConfig.CommitFn`:
    - `TestCompactTable_RejectsOtherTableInDeclaredBucket`: declared `raw.events`, request `raw.other` → `codes.PermissionDenied`, seam not called.
    - `TestCompactTable_RejectsBucketOnlyDeclaration`: `defaultBucket`-only outputs → `PermissionDenied`.
    - `TestCompactTable_ExactTargetCallsSeam`: request `raw.events` → seam gets ident `raw.events`, pipeline `target_file_size`, and `CompactOperationKey(runID, "compact-a", "raw", "events")`.
    - `TestCompactTable_DistinctKeysPerComponent`: same table from `compact-a` and `compact-b` → different keys.
    - `TestCompactTable_EmptyComponentName`: → `codes.Internal`, seam not called.
    - `TestCompactTable_Cancelled`: cancel annotation set → `errRunCancelled` mapping.
- [ ] **Step 2: Run** `go test ./pkg/datagateway/ ./pkg/k8s/controllers/ ./cmd/datuplet/ -run 'CompactTable|ComponentInstanceName' -v`. Expected: FAIL.
- [ ] **Step 3: Implement** the proto change and regenerate; the handler (exact-target check against explicit `output_tables`, cancellation gate, plane-lifetime context, `Resolver.Catalog`, metrics, one log line per run); the SDK method; the component-name wiring.
- [ ] **Step 4: Run** the Step 2 command, then `go build ./... && go test ./pkg/datagateway/... ./pkg/k8s/... ./sdk/go/...`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(gateway): add CompactTable RPC with exact-target authorization`.

### Task 3: `iceberg-compactor` component

**Files:**
- Create: `components/iceberg-compactor/{main.go,config.go,config_test.go,schema.json,Dockerfile,go.mod,README.md}`
- Create: `charts/datuplet-app/templates/components/iceberg-compactor.yaml` (pattern: `sql-transform.yaml`), synced `charts/datuplet-app/files/component-schemas/iceberg-compactor.json`
- Modify: `Makefile` (image targets), `.github/workflows/_release-components.yml`, `.github/workflows/pr.yml` if it lists component modules, `tests/e2e/framework/components_bootstrap.go`, `docs/components.md`

**Interfaces:**
- Consumes: Task 2's `client.Compact`.
- Produces: registered component `iceberg-compactor`, `io: {inputs: none, outputs: required}`; schema exactly as spec §7.

- [ ] **Step 1: Write the failing tests** in `config_test.go` against a fake gateway:
  - `TestRun_ExitCodes` table: explicit `tables` + `COMPACTED` → 0 and one `DUPLET_STATUS_MESSAGE:` line naming files before/after; `NOTHING_TO_DO`, `SUPERSEDED`, `ALREADY_COMMITTED`, `DRY_RUN` → 0; `defaultBucket`-only outputs → 1; gateway `NotFound` → 1; `PermissionDenied` → 1; `Internal` / retry exhaustion → ≥20.
  - `TestSchema_FormSubset`: `pkg/pipeline/schemalint` accepts `schema.json`.
- [ ] **Step 2: Run** `(cd components/iceberg-compactor && go test ./... -v)`. Expected: FAIL.
- [ ] **Step 3: Implement** the component (~120 LOC): read config + `RawConfig().Outputs`, loop tables, aggregate results, print the status line (wording from spec §9, including "old files stay until snapshots are expired"), exit per Global Constraints. Add chart template, Makefile/release wiring, e2e bootstrap entry, docs.
- [ ] **Step 4: Run** the Step 2 command, `make sync-component-schemas && git diff --exit-code charts/datuplet-app/files/component-schemas/`, `make tidy`, `helm lint charts/datuplet-app`. Expected: PASS / no diff.
- [ ] **Step 5: Commit** `feat(components): add iceberg-compactor`.

### Task 4: Pipeline-stage e2e

**Files:**
- Create: `tests/e2e/pipelines/k8s/compaction.yaml`, `tests/e2e/scenarios_compaction_test.go`
- Modify: `tests/e2e/framework/assertions.go` (`AssertDataFileCount`)

**Interfaces:**
- Consumes: Task 3's registered component.
- Produces: `func AssertDataFileCount(t *testing.T, env *Env, bucket, table string, maxFiles int)`.

- [ ] **Step 1: Write** `TestCompaction_PipelineStage`: data-generator `bucket[8]`-partitioned APPEND run twice (≥ 16 files), then a pipeline with a compactor stage on that table. Assert: run Succeeded, row count unchanged, data-file count ≤ 8, latest snapshot operation `replace` with `datuplet.commit-key`.
- [ ] **Step 2: Run** against a deployed e2e stack (`scripts/e2e-port-forward.sh datuplet-e2e` invocation + `-run TestCompaction_PipelineStage`, after `duckdb --version`). Expected: PASS once Tasks 1-3 are deployed (`make k8s-rebuild-all`).
- [ ] **Step 3: Commit** `test(e2e): compaction as a pipeline stage`.

### Task 5: On-demand route via hidden saved pipeline

**Files:**
- Create: `pkg/pipelineapi/db/migrations/015_pipelines_system.sql`
- Modify: `pkg/pipelineapi/store/` pipeline upsert/list (add `system`), run JSON (pipeline `description`)
- Modify: `pkg/pipelineapi/http/pipeline_handlers.go` (PUT reserved prefix, DELETE refusal, list filter), `pkg/pipelineapi/http/run_handlers.go:73` (refuse system pipelines), `pkg/pipelineapi/http/server.go` (route)
- Create: `pkg/pipelineapi/storage/compact_table.go`, `compact_table_test.go`

**Interfaces:**
- Consumes: Task 3's component (via registry validation); existing `backend.TriggerRun`.
- Produces: `POST /api/v1/storage/projects/{pid}/tables/{ns}/{t}/compact` → `202 {"run_id","pipeline","target"}`; `func SystemCompactPipelineName(bucket, table string) string`.

- [ ] **Step 1: Write the failing tests** (DB-backed where noted; `TEST_DATABASE_URL`, `-p 1`):
  - `TestSystemCompactPipelineName`: deterministic, valid DNS-1123 label, differs for `("a","b.c")` vs `("a.b","c")`.
  - `TestCompactTable_Handler` table: no `data_admin` → 403; unknown table → 404; compactor not registered → 503; running compaction → 409 `compaction_running` with the doc unchanged; happy path → 202 and a `system = true` pipeline exists.
  - `TestCompactTable_ReusesPipeline` (DB): two sequential compactions of one table → one pipeline row, two runs.
  - `TestCompactTable_ConcurrentRequests` (DB): fake backend blocks in `TriggerRun`; two requests → one 202, one 409; saved doc equals the winner's config.
  - `TestCompactTable_SmallPoolNoStall` (DB): pool `MaxConns = 2`, 10 concurrent requests → all return within 5 s, nine 409s.
  - `TestCompactTable_LockReleasedOnError` (DB): backend returns an error → 5xx; the next request succeeds.
  - `TestPipelines_SystemHidden` (DB): `GET /pipelines` omits it; `GET /pipelines/{sys-name}` returns it; `PUT /pipelines/sys-x` → 400 `reserved_name`; `DELETE` → 409 `system_pipeline`; `POST /pipelines/{sys-name}/runs` → 409 `system_pipeline`.
  - `TestCompactTable_RunCancellable` (DB): the triggered run lists and cancels like any run.
- [ ] **Step 2: Run** `TEST_DATABASE_URL=… go test -p 1 ./pkg/pipelineapi/storage/ ./pkg/pipelineapi/http/ ./pkg/pipelineapi/store/ -run 'Compact|SystemHidden' -v`. Expected: FAIL.
- [ ] **Step 3: Implement** per spec §8 steps 1-4: validate doc before locking; `pg_try_advisory_lock` on one pooled connection; detached-context unlock in `defer`, hijack+close the connection if unlock fails; under the lock check running → upsert → `TriggerRun`. Add migration, store and handler changes.
- [ ] **Step 4: Run** the Step 2 command, then `go build ./... && TEST_DATABASE_URL=… go test -p 1 ./pkg/pipelineapi/...`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(api): on-demand table compaction via hidden system pipeline`.

### Task 6: UI button and CLI command

**Files:**
- Modify: `ui/product/api.js` (`compactTable`), `ui/product/pages/storage-table.js` (button beside Delete, `:70-73`), `ui/product/pages/runs.js` and `run-detail.js` (show `description` for system pipelines)
- Modify: `cmd/datuplet/storage.go`, `cmd/datuplet/storage_test.go`, `docs/agent-quickstart.md`

**Interfaces:**
- Consumes: Task 5's route.
- Produces: `datuplet storage compact <ns.table> [--wait] [--timeout] [--json] [--target-file-size-mb]`.

- [ ] **Step 1: Write the failing test** `TestStorageCompact` in `cmd/datuplet/storage_test.go`: fake server; the command POSTs to `/api/v1/storage/projects/p/tables/raw/t/compact` with the size body, and with `--wait` polls `/runs/{id}` to a terminal phase; a 409 prints the running run ID and exits non-zero.
- [ ] **Step 2: Run** `go test ./cmd/datuplet/ -run TestStorageCompact -v`. Expected: FAIL.
- [ ] **Step 3: Implement** the CLI (reuse `pollUntilTerminal` / `emitResult` from `trigger.go:291,320`) and the UI: confirm text from spec §8, redirect to `/ui/runs/<run_id>`, 409 shows a link to the running run.
- [ ] **Step 4: Run** `go test ./cmd/datuplet/... ./pkg/pipelineapi/http/ -run 'TestStorageCompact|Contrast'` (keeps `pkg/pipelineapi/http/ui_contrast_test.go` green). Expected: PASS. Manually click the button on the e2e stack.
- [ ] **Step 5: Commit** `feat(ui,cli): compact a table on demand`.

### Task 7: Docs and on-demand e2e

**Files:**
- Modify: `docs/pipeline-api.md` (route, system pipelines, reserved prefix), `docs/known-limitations.md` (R1-R5, R7; no storage reclaim; preview behaviour), `docs/architecture.md` (one paragraph under the sidecar), `CHANGELOG.md`
- Modify: `tests/e2e/scenarios_compaction_test.go`, `tests/e2e/framework/pipeline_api_client.go` (compact call)

- [ ] **Step 1: Write** `TestCompaction_OnDemand`: create a fragmented table, call the compact route → 202, wait for Succeeded, assert file count dropped and row count unchanged; a second immediate call while running → 409.
- [ ] **Step 2: Run** `make lint`, then `make e2e-k8s` (after `duckdb --version`). Expected: all scenarios PASS. Spot-check `kubectl -n datuplet-e2e get pipelines` shows the `sys-compact-…` pipeline and the UI pipeline list does not.
- [ ] **Step 3: Commit** `docs: table compaction` (and the e2e test).
