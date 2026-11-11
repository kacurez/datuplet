# Iceberg Table Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `iceberg-compactor` with one `iceberg-maintenance` component that runs compaction, manifest rewrite, snapshot expiry and orphan-file deletion on its declared output tables through new Data Gateway RPCs.

**Architecture:** The component stays a thin SDK program; every storage operation runs in the Data Gateway sidecar under lakekeeper-vended credentials. Three new gateway RPCs (`RewriteManifests`, `ExpireSnapshots`, `DeleteOrphanFiles`) call new `pkg/icebergjob` functions built on iceberg-go v0.7.0 and are served only when the controller marks the pod as a maintenance stage. `gcsIO` gains `WalkDir` so orphan listing works on GCS.

**Tech Stack:** Go, iceberg-go v0.7.0 (`Transaction.ExpireSnapshots`, `Transaction.RewriteManifests`, `Table.PlanOrphanFiles`/`ExecuteOrphanCleanup`), gocloud `blob`/`memblob`, gRPC gateway v2 proto (protoc v5.26.1, protoc-gen-go v1.36.11), controller-runtime, Helm.

**Spec:** `docs/superpowers/specs/2026-11-10-iceberg-maintenance-design.md` (revision 3). Executors read both.

## Global Constraints

- Registry/image/component name: `iceberg-maintenance`; `io: {inputs: none, outputs: required}`.
- Task order per table, fixed: compact → rewrite_manifests → expire_snapshots → delete_orphan_files. A failed task skips the remaining tasks for that table only.
- Defaults: `compact.enabled=true`, others `false`; `rewrite_manifests.target_manifest_size_mb=8`; `expire_snapshots.older_than_hours=168`, `retain_last=10`; `delete_orphan_files.older_than_hours=72`, `max_files=10000`.
- Gateway-enforced floors (`InvalidArgument`, exit 1): expire `older_than_hours >= 24`, `retain_last >= 1`; orphans `older_than_hours >= 25`.
- The three new RPCs are served only when gateway YAML has `table_maintenance: true`, written by the controller iff `comp.Component == "iceberg-maintenance"`; otherwise `PermissionDenied`. Never read from user config. `CompactTable` keeps today's check.
- Exit codes: `0` success statuses (incl. `NOTHING_TO_DO`, `DRY_RUN`, `EXPIRED_PARTIAL`, `SUPERSEDED`, `ALREADY_COMMITTED`); `1` for `NotFound|PermissionDenied|InvalidArgument|FailedPrecondition`; `20` otherwise (reuse `exitCodeFor`).
- Schema stays in the Form Subset (nested objects ok; no `oneOf`/`$ref`/…), every property has a `description`; run `make sync-component-schemas`.
- Never `path.Join`/`filepath.Join` on storage URLs.
- Fresh installs only: no migration code. Multi-module repo: `make tidy`, never bare `go mod tidy`.
- Integration tests go in `pkg/datagateway/goplane/*_integration_test.go` (the only `icebergjob`-consuming package CI runs with `-tags=integration`), reusing `sharedFixture`, `resolver`, `createTable`, `writeRun`, `commit` from the compaction tests.
- Reviewer gates: a fresh Claude reviewer subagent per task (Codex only as fallback); STOP and present Critical/Important findings to the maintainer before any fix.

## Review Focus

1. **A listing that spells paths differently from metadata** (bare keys, `s3a://`, trailing slash) must never delete a live file → the live-file guard compares by object key, not the raw string (Task 6, `TestLiveFileGuard_BareKeyListing`).
2. **A table whose `main` ref carries `max-snapshot-age-ms`/`min-snapshots-to-keep`** must not have recent snapshots expired → keep-set guard refuses (Task 4, `TestExpire_RefRetentionOverride_Refused`).
3. **An append that lands between expire planning and commit** must not have its files deleted → no replay, recompute (Task 4, `TestExpire_ConcurrentAppend_Recomputes`).
4. **A table with only one snapshot / an empty table / a table never compacted** → all four tasks report `NOTHING_TO_DO`, exit 0 (Task 4 + Task 6 unit tests, Task 7 e2e).
5. **A sibling table whose location shares a string prefix** (`…/events` vs `…/events_v2`) is never listed or touched (Task 5 `TestGCSWalkDir_SiblingPrefix`, Task 6 `TestOrphans_SiblingTableUntouched`).

---

### Task 0: Verify vended credentials can list and delete (gate)

**Files:**
- Create: `pkg/datagateway/goplane/maintenance_creds_integration_test.go`
- Create: `docs/superpowers/specs/2026-11-10-iceberg-maintenance-creds-check.md` (findings, ≤1 page)

**Interfaces:** Produces only a go/no-go finding. Every later task depends on it.

- [ ] **Step 1: Write `TestVendedCreds_ListAndDelete`** (integration tag). Using `sharedFixture`/`resolver`/`createTable`/`writeRun`, write one file without committing it, then via the table's `FS(ctx)` (credentials vended by the fixture's Lakekeeper): assert `fs.(iceio.ListableIO)` holds, `WalkDir(table location)` returns the file, and `Remove(file)` succeeds and a second `WalkDir` no longer returns it.
- [ ] **Step 2: Run** `go test -tags=integration -run TestVendedCreds_ListAndDelete ./pkg/datagateway/goplane/ -v`. Expected: PASS. Record in the findings doc whether the fixture's warehouse vends **STS** credentials (session policy) or static ones; if static, the test does not prove the STS policy.
- [ ] **Step 3: Check the real policies.** From Lakekeeper v0.14.2 docs/release notes, record (a) the S3 STS session policy's actions on the table prefix (`s3:ListBucket` with prefix condition, `s3:DeleteObject`) and (b) the GCS downscoped-token roles for read-write access. If a GKE deployment is reachable, run a maintenance dry-run against it after Task 6; otherwise mark GCS as "documented, not live-verified".
- [ ] **Step 4: STOP and report** the findings doc to the maintainer. If list or delete is not granted on either backend, do not start Task 1.
- [ ] **Step 5: Commit** `test(goplane): verify vended credentials allow list and delete`.

### Task 1: Rename `iceberg-compactor` → `iceberg-maintenance` with the nested config

**Files:**
- Move: `components/iceberg-compactor/` → `components/iceberg-maintenance/` (`main.go`, `config.go`, `config_test.go`, `schema.json`, `Dockerfile`, `go.mod`, `README.md`)
- Move: `charts/datuplet-app/templates/components/iceberg-compactor.yaml` → `iceberg-maintenance.yaml`; regenerate `charts/datuplet-app/files/component-schemas/iceberg-maintenance.json`
- Modify: `Makefile:7,57,69,78-79,111`, `.github/workflows/{pr.yml:42-43,e2e.yml:90,_release-components.yml:4,38-42,release.yml:63}`, `tests/e2e/framework/components_bootstrap.go:150-215`, `tests/e2e/pipelines/k8s/compaction.yaml`, `tests/e2e/scenarios_compaction_test.go`, `pkg/pipelineapi/storage/compact_table.go:26,118`, `pkg/pipelineapi/storage/compact_table_test.go`, `pkg/pipelineapi/http/compact_route_test.go`, `pkg/k8s/controllers/pipelinerun_jobs_test.go:790`
- Docs: `docs/components.md` (section rename + config), `docs/architecture.md:209`, `docs/pipeline-api.md:883,905`, `CLAUDE.md` component list

**Interfaces:**
- Produces (in `components/iceberg-maintenance/config.go`):
  ```go
  type Config struct {
      DryRun            bool                   `json:"dry_run"`
      Compact           CompactTask            `json:"compact"`
      RewriteManifests  RewriteManifestsTask   `json:"rewrite_manifests"`
      ExpireSnapshots   ExpireSnapshotsTask    `json:"expire_snapshots"`
      DeleteOrphanFiles DeleteOrphanFilesTask  `json:"delete_orphan_files"`
  }
  type CompactTask struct { Enabled *bool `json:"enabled"`; TargetFileSizeMB, MinInputFiles, MaxInputFiles int /* json tags as today */ }
  type RewriteManifestsTask struct { Enabled bool `json:"enabled"`; TargetManifestSizeMB int `json:"target_manifest_size_mb"` }
  type ExpireSnapshotsTask struct { Enabled bool `json:"enabled"`; OlderThanHours, RetainLast int /* older_than_hours, retain_last */ }
  type DeleteOrphanFilesTask struct { Enabled bool `json:"enabled"`; OlderThanHours, MaxFiles int /* older_than_hours, max_files */ }
  func (c Config) compactEnabled() bool // nil Enabled → true
  type maintainer interface { Compact(ctx context.Context, bucket, table string, opts sdk.CompactOptions) (*sdk.CompactResult, error) } // grows in Tasks 3,4,6
  func run(ctx context.Context, m maintainer, cfg Config, defaultBucket string, tables []sdk.OutputTableRef) (int, string)
  ```
- Hidden Compact-button pipeline config becomes `{"compact": {"enabled": true, "target_file_size_mb": N}}` when N is set, else no config.

- [ ] **Step 1: Write failing tests** in `config_test.go`: `TestRun_EmptyConfig_CompactsOnly` (empty `Config{}` → one `Compact` call per table, exit 0); `TestRun_AllTasksDisabled_ExitUser` (`compact.enabled=false`, nothing else → exit 1, message contains `no maintenance task enabled`); `TestRun_StatusLinePrefix` (message starts with `maintenance: `). Update existing compactor tests to the nested shape. In `compact_table_test.go`, assert the stored stage `Component == "iceberg-maintenance"` and config `{"compact":{"enabled":true,"target_file_size_mb":256}}`.
- [ ] **Step 2: Run** `(cd components/iceberg-maintenance && go test ./... -v)` and `go test ./pkg/pipelineapi/storage/ -run Compact -v`. Expected: FAIL.
- [ ] **Step 3: Implement** the move, the config types, `schema.json` with the four nested objects (defaults per Global Constraints; `min_input_files`, `max_input_files`, `target_manifest_size_mb`, `max_files` are `x-datuplet-advanced`), and every rename listed under Files. `run` keeps today's per-table loop for compact only; tasks 3/4/6 extend it.
- [ ] **Step 4: Run** the two commands above, `make sync-component-schemas`, `go build ./... && go test ./...`, `helm template charts/datuplet-app >/dev/null`, `grep -rn iceberg-compactor --exclude-dir=docs/superpowers .` (expect only CHANGELOG). Expected: PASS / no hits.
- [ ] **Step 5: Commit** `refactor(components): rename iceberg-compactor to iceberg-maintenance`.

### Task 2: Proto, SDK, maintenance gate and controller flag

**Files:**
- Modify: `api/proto/gateway/v2/gateway.proto` (+ regenerate `api/proto/gateway/v2/*.pb.go` and `pkg/datagateway/proto/v2/*.pb.go` exactly as the compaction change did)
- Modify: `pkg/datagateway/gatewayconfig.go:12-35` (field), `pkg/k8s/controllers/pipelinerun_jobs.go:546` (write key), `sdk/go/client.go` (after `Compact`, ~line 1166)
- Create: `pkg/datagateway/server_v2_maintenance.go`, `pkg/datagateway/server_v2_maintenance_test.go`
- Modify: `pkg/k8s/controllers/pipelinerun_jobs_test.go`

**Interfaces:**
- Proto:
  ```proto
  rpc RewriteManifests(RewriteManifestsRequest) returns (RewriteManifestsResponse);
  rpc ExpireSnapshots(ExpireSnapshotsRequest) returns (ExpireSnapshotsResponse);
  rpc DeleteOrphanFiles(DeleteOrphanFilesRequest) returns (DeleteOrphanFilesResponse);
  message RewriteManifestsRequest { string bucket=1; string table=2; int64 target_manifest_size_bytes=3; bool dry_run=4; }
  message RewriteManifestsResponse { string status=1; int32 manifests_before=2; int32 manifests_after=3; int64 snapshot_before=4; int64 snapshot_after=5; int64 duration_ms=6; }
  message ExpireSnapshotsRequest { string bucket=1; string table=2; int32 older_than_hours=3; int32 retain_last=4; bool dry_run=5; }
  message ExpireSnapshotsResponse { string status=1; int32 snapshots_expired=2; int32 files_deleted=3; int64 bytes_deleted=4; int32 files_left=5; int64 current_snapshot=6; int64 duration_ms=7; }
  message DeleteOrphanFilesRequest { string bucket=1; string table=2; int32 older_than_hours=3; int32 max_files=4; bool dry_run=5; }
  message DeleteOrphanFilesResponse { string status=1; int32 orphan_files=2; int64 orphan_bytes=3; int32 files_deleted=4; int64 duration_ms=5; }
  ```
- `GatewayConfig.TableMaintenance bool \`yaml:"table_maintenance,omitempty"\``
- Server seams (nil until Tasks 3/4/6 wire them; nil → `codes.Unimplemented`):
  `rewriteManifestsFn func(ctx, icebergtable.Identifier, icebergjob.RewriteManifestsOpts) (*icebergjob.RewriteManifestsResult, error)`, `expireFn func(ctx, icebergtable.Identifier, icebergjob.ExpireOpts) (*icebergjob.ExpireResult, error)`, `orphansFn func(ctx, icebergtable.Identifier, icebergjob.OrphanOpts) (*icebergjob.OrphanResult, error)` — the opts/result types (`RewriteManifestsOpts/Result`, `ExpireOpts/Result`, `OrphanOpts/Result`, exactly as in the Interfaces blocks of Tasks 3, 4 and 6) and `ErrRefused` are declared in this task in `pkg/icebergjob/maintenance.go`; the functions come later.
- `func (s *ServerV2) maintenancePreflight(bucket, table string) error` — order: empty bucket/table → `InvalidArgument`; `!s.config.TableMaintenance` → `PermissionDenied` ("maintenance RPCs are only served to iceberg-maintenance stages"); `!s.isDeclaredOutputTable` → `PermissionDenied`; `s.isCancelled()` → `cancelledStatus`.
- `func maintenanceStatusError(op, bucket, table string, err error) error` — like `compactStatusError` plus `errors.Is(err, icebergjob.ErrRefused)` → `FailedPrecondition`.
- SDK: `RewriteManifests`, `ExpireSnapshots`, `DeleteOrphanFiles` methods with `RewriteManifestsOptions{TargetManifestSizeBytes int64; DryRun bool}`, `ExpireOptions{OlderThanHours, RetainLast int; DryRun bool}`, `OrphanOptions{OlderThanHours, MaxFiles int; DryRun bool}` and result structs mirroring the responses (`Duration time.Duration`).
- `icebergjob.ErrRefused = errors.New("maintenance refused")` — every safety refusal wraps it.

- [ ] **Step 1: Write failing tests:** `TestMaintenanceRPCs_RequireTableMaintenance` (each RPC with `TableMaintenance=false` and a declared table → `PermissionDenied`); `TestMaintenanceRPCs_RequireDeclaredTable`; `TestMaintenanceRPCs_UnwiredIsUnimplemented`; `TestMaintenanceStatusError_RefusedIsFailedPrecondition`; controller `TestGatewayConfig_TableMaintenanceOnlyForMaintenanceComponent` (component `iceberg-maintenance` → YAML has `table_maintenance: true`; component `sql-transform` with user config `{"table_maintenance": true}` → key absent from gateway YAML).
- [ ] **Step 2: Run** `go test ./pkg/datagateway/ ./pkg/k8s/controllers/ -run 'Maintenance|TableMaintenance' -v`. Expected: FAIL (compile errors first).
- [ ] **Step 3: Implement** proto + regenerate, config field, controller line `if comp.Component == "iceberg-maintenance" { configMap["table_maintenance"] = true }` next to `component_name`, preflight, status mapping, three handlers (preflight → floors check → seam → metrics `datagateway_maintenance_{runs,duration_seconds,files}` labelled `task`,`status` in `commit_metrics.go` → response), SDK methods. Floors live in the handlers; their tests land in the tasks that wire each seam.
- [ ] **Step 4: Run** the Step 2 command, then `go vet ./... && go test ./...`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(gateway): maintenance RPC surface gated to iceberg-maintenance stages`.

### Task 3: RewriteManifests

**Files:**
- Create: `pkg/icebergjob/rewrite_manifests.go`, `pkg/icebergjob/rewrite_manifests_test.go`
- Modify: `pkg/datagateway/server_v2.go:~285` (wire seam), `components/iceberg-maintenance/config.go` (+ test)
- Test: `pkg/datagateway/goplane/maintenance_integration_test.go` (create)

**Interfaces:**
- Consumes: Task 2 seam, SDK method, `ErrRefused`.
- Produces: `type RewriteManifestsOpts struct { TargetManifestSizeBytes int64; DryRun bool }`; `type RewriteManifestsResult struct { Status string /* REWRITTEN|NOTHING_TO_DO|DRY_RUN */; ManifestsBefore, ManifestsAfter int; SnapshotBefore, SnapshotAfter int64; Duration time.Duration }`; `func RewriteTableManifests(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, opts RewriteManifestsOpts) (*RewriteManifestsResult, error)`.

- [ ] **Step 1: Write failing tests:** integration `TestRewriteManifests_MergesAndKeepsRows` (10 APPEND commits → `REWRITTEN`, `ManifestsAfter < ManifestsBefore`, `scanRows` unchanged); `TestRewriteManifests_DryRunCommitsNothing` (snapshot id unchanged, status `DRY_RUN`); `TestRewriteManifests_EmptyTable_NothingToDo`. Component: `TestRun_OrderAndSkipAfterFailure` (fake maintainer records calls; compact error with `codes.Unavailable` on table A → A gets no rewrite call, table B gets compact then rewrite; exit 20).
- [ ] **Step 2: Run** `go test -tags=integration -run TestRewriteManifests ./pkg/datagateway/goplane/ -v` and the component test. Expected: FAIL.
- [ ] **Step 3: Implement** with `tbl.NewTransaction().RewriteManifests(ctx, WithManifestTargetSize(n))`; `IsNoOp()` → `NOTHING_TO_DO`; dry run returns before `Commit`; no gateway retry and no `noReplay` (iceberg-go replays this one safely). Default size 8 MiB. Component: per-table task loop in the fixed order with skip-after-failure, summary `maintenance: <bucket>.<table>: compact COMPACTED files 12 -> 2; rewrite_manifests REWRITTEN 12 -> 1; …`.
- [ ] **Step 4: Run** Step 2 commands + `go test ./pkg/icebergjob/ ./pkg/datagateway/`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(maintenance): rewrite manifests`.

### Task 4: ExpireSnapshots

**Files:**
- Create: `pkg/icebergjob/expire.go`, `pkg/icebergjob/expire_test.go`
- Modify: `pkg/datagateway/server_v2.go` (wire; `noReplay` from `pkg/icebergjob/compact.go:463` is reused in-package), `pkg/datagateway/server_v2_maintenance.go` (floors), `components/iceberg-maintenance/config.go`
- Test: `pkg/datagateway/goplane/maintenance_integration_test.go`, `pkg/datagateway/server_v2_maintenance_test.go`

**Interfaces:**
- Produces: `type ExpireOpts struct { OlderThan time.Duration; RetainLast int; DryRun bool; Now func() time.Time /* nil → time.Now */ }`; `type ExpireResult struct { Status string /* EXPIRED|EXPIRED_PARTIAL|NOTHING_TO_DO|DRY_RUN */; SnapshotsExpired, FilesDeleted, FilesLeft int; BytesDeleted int64; CurrentSnapshot int64; Duration time.Duration }`; `func ExpireTableSnapshots(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, opts ExpireOpts) (*ExpireResult, error)`; unexported `effectiveRetainLast(md icebergtable.Metadata, cutoffMs int64, retainLast int) int`, `keepSet(md icebergtable.Metadata, cutoffMs int64, effective int) map[int64]struct{}`, `expireDeletionSet(ctx context.Context, fs iceio.IO, retained, expired []icebergtable.Snapshot) (paths []string, bytes map[string]int64, err error)`.

Algorithms the signatures do not determine:
```text
effectiveRetainLast: chain = ancestors of main head, newest first: S0, S1, …
  count = #{ i >= 1 : chain[i-1].TimestampMs > cutoffMs }   // Si superseded after cutoff
  return max(retainLast, count+1)
keepSet: {S0} ∪ {chain[i] : i < effective} ∪ {ref.SnapshotID : every ref != main}
expireDeletionSet:
  candidates = manifest lists of expired ∪ their manifests ∪ every entry's file path (ADDED, EXISTING, DELETED)
  keep       = manifest lists of retained ∪ their manifests ∪ entry paths whose status != DELETED
  return candidates − keep
attempt loop (max catalogwriter.DefaultMaxRetries):
  tbl = LoadTable; if gc.enabled=false → ErrRefused
  eff = effectiveRetainLast; ks = keepSet
  txn = noReplay(tbl, cat).NewTransaction()
  txn.ExpireSnapshots(WithOlderThan(opts.OlderThan), WithRetainLast(eff), WithPostCommit(false))
  removed = snapshot ids in tbl.Metadata() missing from txn.StagedTable().Metadata()
  removed ∩ ks ≠ ∅ → ErrRefused ("keep-set guard: snapshot %d")
  removed = ∅ → NOTHING_TO_DO;  DryRun → compute expireDeletionSet against staged metadata, DRY_RUN
  committed, err = txn.Commit; 409 → next attempt (recompute everything)
  paths = expireDeletionSet(committed.FS, committed snapshots, removed snapshots from tbl)
  delete with bounded concurrency (8); failures counted → EXPIRED_PARTIAL with FilesLeft
```

- [ ] **Step 1: Write failing tests.** Unit (`expire_test.go`, synthetic metadata): `TestEffectiveRetainLast_SupersededRecently` (head committed 1 h ago over a 30-day-old parent, older_than 24 h, retain_last 1 → 2); `TestEffectiveRetainLast_AllOld` (→ retain_last); `TestKeepSet_IncludesOtherRefs`; `TestExpireDeletionSet_DeletedEntriesAreCandidates` (file F ADDED in expired manifest, DELETED in retained manifest → F deleted); `TestExpireDeletionSet_LiveFileKept`. Integration: `TestExpire_CompactedFilesFreed` (6 appends, compact, `Now` = +200 h, retain_last 1 → `EXPIRED`, pre-compaction files gone from storage, rows unchanged); `TestExpire_RefRetentionOverride_Refused` (set `main`'s `max-snapshot-age-ms=1` via a catalog `SetSnapshotRef` update, recent snapshots → `ErrRefused`, snapshot list unchanged); `TestExpire_ConcurrentAppend_Recomputes` (catalog wrapper commits one APPEND before the first expire commit → second attempt succeeds, appended file still present and scannable); `TestExpire_SingleSnapshot_NothingToDo`; `TestExpire_DryRun_DeletesNothing`; `TestExpire_GCDisabled_Refused`. Gateway: `TestExpireSnapshots_Floors` (older_than 23 → `InvalidArgument`; retain_last 0 → `InvalidArgument`; zero values → defaults 168/10 applied).
- [ ] **Step 2: Run** `go test ./pkg/icebergjob/ -run 'Expire|EffectiveRetain|KeepSet' -v`, `go test -tags=integration -run TestExpire ./pkg/datagateway/goplane/ -v`, `go test ./pkg/datagateway/ -run Expire -v`. Expected: FAIL.
- [ ] **Step 3: Implement** per the algorithm block; wire the seam and component task (`EXPIRED_PARTIAL` → exit 0 with `files_left` in the summary).
- [ ] **Step 4: Run** Step 2 commands. Expected: PASS.
- [ ] **Step 5: Commit** `feat(maintenance): expire snapshots with reader protection and keep-set guard`.

### Task 5: `gcsIO.WalkDir`

**Files:**
- Modify: `pkg/datupleticeio/gcs_factory.go:318-428`
- Test: `pkg/datupleticeio/gcs_factory_test.go`

**Interfaces:**
- Produces: `func (g *gcsIO) WalkDir(root string, fn fs.WalkDirFunc) error` so `gcsIO` satisfies `iceio.ListableIO` (assert with `var _ iceio.ListableIO = (*gcsIO)(nil)`). Paths passed to `fn` are `gs://<bucketName>/<key>`; `DirEntry.Info()` returns size and `ModTime` = object `ModTime`. Listing prefix = `preprocess(root)` with exactly one trailing `/`.

- [ ] **Step 1: Write failing tests** on a `memblob`-backed `gcsIO{bucketName: "b"}`: `TestGCSWalkDir_FullURIs` (objects `wh/t/data/a.parquet`, `wh/t/metadata/v1.json` → paths exactly `gs://b/wh/t/data/a.parquet`, `gs://b/wh/t/metadata/v1.json`); `TestGCSWalkDir_SiblingPrefix` (object `wh/t_v2/data/x.parquet` not returned for root `gs://b/wh/t`); `TestGCSWalkDir_SizeAndModTime`; `TestGCSWalkDir_RootWithTrailingSlash` (same result).
- [ ] **Step 2: Run** `go test ./pkg/datupleticeio/ -run GCSWalkDir -v`. Expected: FAIL.
- [ ] **Step 3: Implement** with `g.bucket.List(&blob.ListOptions{Prefix: prefix})`, iterating `Next(g.ctx)` until `io.EOF`.
- [ ] **Step 4: Run** Step 2 + `go test ./pkg/datupleticeio/`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(datupleticeio): list objects in gcsIO for orphan cleanup`.

### Task 6: DeleteOrphanFiles

**Files:**
- Create: `pkg/icebergjob/orphans.go`, `pkg/icebergjob/orphans_test.go`
- Modify: `pkg/datagateway/server_v2.go` (wire), `pkg/datagateway/server_v2_maintenance.go` (floor), `components/iceberg-maintenance/config.go`
- Test: `pkg/datagateway/goplane/maintenance_integration_test.go`, `pkg/datupleticeio/gcs_orphans_test.go` (create)

**Interfaces:**
- Consumes: Task 5 `gcsIO.WalkDir`.
- Produces: `type OrphanOpts struct { OlderThan time.Duration; MaxFiles int; DryRun bool }`; `type OrphanResult struct { Status string /* DELETED|NOTHING_TO_DO|DRY_RUN */; OrphanFiles, FilesDeleted int; OrphanBytes int64; Duration time.Duration }`; `func DeleteTableOrphanFiles(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, opts OrphanOpts) (*OrphanResult, error)`; `func DeleteOrphansOnTable(ctx context.Context, tbl *icebergtable.Table, opts OrphanOpts) (*OrphanResult, error)` (the catalog-free core, used by the GCS test); unexported `liveFiles(ctx context.Context, tbl *icebergtable.Table) (map[string]struct{}, error)` keyed by `objectKey`, `objectKey(p string) string` (strip `scheme://authority/`, trim leading `/`), `liveFileGuard(planned []string, live map[string]struct{}) error`.

Flow: `gc.enabled=false` → `ErrRefused`; `plan := tbl.PlanOrphanFiles(ctx, WithFilesOlderThan(opts.OlderThan))` (location = table location, `PrefixMismatchError` default); empty → `NOTHING_TO_DO`; `len > MaxFiles` → `ErrRefused` with the count; `liveFileGuard` (live = current metadata location, current snapshot's manifest list and manifests, every non-DELETED entry path) → `ErrRefused` naming the first offending path; `DryRun` → `DRY_RUN`; `ctx` cancelled → stop before delete; `tbl.ExecuteOrphanCleanup(ctx, plan)`; a partial delete error → returned error (exit 20) with counts in the message.

- [ ] **Step 1: Write failing tests.** Unit: `TestObjectKey` (`s3://b/k`, `gs://b/k`, `s3a://b/k`, `k` → `k`); `TestLiveFileGuard_BareKeyListing` (planned `wh/t/data/a.parquet`, live key same → refused); `TestLiveFileGuard_CleanPlan`. Integration (S3 fixture): `TestOrphans_DeletesOldUncommitted` (write a file without committing, `OlderThan` = 1 ns via `OrphanOpts` directly → that file deleted; every committed file and the current metadata still readable; rows unchanged); `TestOrphans_YoungFileKept` (`OlderThan` 1 h); `TestOrphans_SiblingTableUntouched` (tables `t` and `t_v2` under the same namespace; uncommitted file in `t_v2` survives a run on `t`); `TestOrphans_MaxFilesRefused` (3 orphans, `MaxFiles` 2 → `ErrRefused`, all 3 present); `TestOrphans_DryRun`; `TestOrphans_GCDisabled_Refused`; `TestOrphans_RerunNoop`. GCS (`gcs_orphans_test.go`, no Docker): build a `gs://b/wh/t` table on `memblob` through `gcsIO` (iceberg-go `catalog/hadoop` or `table.New` over metadata written with that IO), commit two files, upload one uncommitted file → `DeleteOrphansOnTable` deletes exactly that file, guard not tripped (positive case). Gateway: `TestDeleteOrphanFiles_Floor` (older_than 24 → `InvalidArgument`; 0 → default 72).
- [ ] **Step 2: Run** `go test ./pkg/icebergjob/ -run 'ObjectKey|LiveFileGuard' -v`, `go test -tags=integration -run TestOrphans ./pkg/datagateway/goplane/ -v`, `go test ./pkg/datupleticeio/ -run Orphans -v`, `go test ./pkg/datagateway/ -run Orphan -v`. Expected: FAIL.
- [ ] **Step 3: Implement** per the flow; wire seam and component task.
- [ ] **Step 4: Run** Step 2 commands. Expected: PASS.
- [ ] **Step 5: Commit** `feat(maintenance): delete orphan files with live-file guard`.

### Task 7: e2e scenario and docs

**Files:**
- Modify: `tests/e2e/pipelines/k8s/compaction.yaml` (or add `maintenance.yaml`), `tests/e2e/scenarios_compaction_test.go`
- Modify: `docs/components.md`, `docs/known-limitations.md` (lines ~264, ~493-509), `docs/architecture.md`, `components/iceberg-maintenance/README.md`

**Interfaces:** Consumes everything above.

- [ ] **Step 1: Write the e2e scenario** `TestMaintenance_PipelineStage`: seed pipeline (existing `compaction-seed.yaml`), then a stage with all four tasks enabled at their defaults. Assert run `Succeeded`, status line contains `compact COMPACTED`, `expire_snapshots NOTHING_TO_DO`, `delete_orphan_files NOTHING_TO_DO`, and `duckdb` row count unchanged. Check `duckdb --version` locally first.
- [ ] **Step 2: Update docs:** components.md config table and example; known-limitations: replace "Orphaned files … No cleanup exists" and "No storage reclaim" with pointers to the component, add the 25 h assumption (pipeline runs only; CLI catalog tokens up to 30 days and foreign writers unprotected), the stage-config retention drift, the unauthenticated `:50051`/missing run-pod NetworkPolicy, and concurrent maintenance stages ending `>=20`.
- [ ] **Step 3: Run** `go vet ./... && go test ./...`, `make lint`, `(cd tests/e2e && go vet ./...)`. Expected: PASS. e2e itself runs in CI on the PR (maintainer rule: no local `make e2e-k8s`).
- [ ] **Step 4: Commit** `test(e2e): maintenance stage scenario; docs for iceberg-maintenance`.
