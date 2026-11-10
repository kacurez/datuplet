# Iceberg table maintenance component — design

Date: 2026-11-10. Status: revision 3 for maintainer review. Builder check
done; GCS and age-floor questions answered by the maintainer; design review
round 1 (4 Important, 4 Nit) applied, see §10.
Builds on: `docs/superpowers/specs/2026-11-08-table-compaction.md` (merged in PR #51).

## 1. Intent

Compaction (PR #51) rewrites small files into big ones, but frees no storage:
the replaced files stay referenced by older snapshots. Crashed, cancelled or
unknown-outcome runs also leave uploaded Parquet files that no snapshot
references (`docs/known-limitations.md` "Orphaned files", R3/R3b). Nothing
deletes either kind today.

This design generalises the `iceberg-compactor` component into one
maintenance component, `iceberg-maintenance`, that runs up to four table
maintenance tasks on each declared output table, in a fixed order:

1. **compact** — today's `CompactTable`, unchanged.
2. **rewrite_manifests** — merge small manifests (iceberg-go `Transaction.RewriteManifests`).
3. **expire_snapshots** — drop old snapshots and delete the files only they
   referenced (iceberg-go `Transaction.ExpireSnapshots`). This is the task
   that actually reclaims the space compaction leaves behind.
4. **delete_orphan_files** — delete files under the table location that no
   snapshot or metadata file references (iceberg-go `Table.PlanOrphanFiles` +
   `Table.ExecuteOrphanCleanup`).

Users run maintenance as an ordinary pipeline stage. There is no new UI.

## 2. Goals / non-goals / anti-goals

**Goals**
- G1. One component and one image for all table maintenance; enabling a task
  is a config flag.
- G2. Same trust model as compaction: all storage I/O and commits happen in
  the Data Gateway sidecar under the run token's lakekeeper-vended
  credentials; the component holds no credentials.
- G3. Never delete a file a live snapshot, a retained snapshot, or an
  in-flight run can still need (§5).
- G4. Each task is idempotent; re-running a stage is always safe.
- G5. The table detail page's existing **Compact** button keeps working.

**Non-goals**: UI buttons for the new tasks; scheduling (users wire the
stage into a pipeline); per-table retention properties editing; branch/tag
management; sort or z-order; position-delete rewrite; cleanup outside a
table's own location (e.g. deleted tables, warehouse-wide sweeps).

**Retention policy (maintainer decision, 2026-11-10):** retention lives in
the stage config (`expire_snapshots.older_than_hours`, `retain_last`),
applied per run within the gateway floors. Iceberg table properties
(`history.expire.max-snapshot-age-ms`, `history.expire.min-snapshots-to-keep`)
are not read: the explicit call options win over them in iceberg-go.
Known drift: two pipelines maintaining one table with different values —
the last run wins. **Follow-up:** table-level retention stored as those
standard properties, set via a storage API/CLI, with the stage config as an
optional override.

**Anti-goals**: credentials in the component; pipeline-api doing storage
I/O; a long-running maintenance service; any upgrade/migration path
(Datuplet is always installed fresh).

## 3. Verified facts this relies on

- iceberg-go v0.7.0 (pinned in `go.mod`) ships all three new operations:
  `table/orphan_cleanup.go` (`PlanOrphanFiles`, `ExecuteOrphanCleanup`,
  options `WithFilesOlderThan`, `WithDryRun`, `WithPrefixMismatchMode`,
  default mode `PrefixMismatchError`, default age 72 h),
  `table/rewrite_manifests.go` (`Transaction.RewriteManifests`,
  `WithManifestTargetSize`), `table/transaction.go:735`
  (`Transaction.ExpireSnapshots`, `WithOlderThan`, `WithRetainLast`,
  `WithPostCommit`; refuses when `gc.enabled=false`; asserts each ref's
  snapshot id). **Caveats (design review):** on a 409, iceberg-go's
  `doCommit` reloads the table, re-points `main`'s ref assertion at the new
  head and resubmits the stale update list (`table/table.go:681,851-878`),
  so a concurrent commit on `main` does *not* surface as a conflict unless
  the replay is disabled, which compaction already does with `noReplay`
  (`pkg/icebergjob/compact.go:247,463`). Per-ref retention fields
  (`MinSnapshotsToKeep`, `MaxSnapshotAgeMs`) take precedence over the
  call options (`table/transaction.go:817-818`).
- iceberg-go's orphan planning does not check `gc.enabled`
  (`table/orphan_cleanup.go:367-436`); Java's DeleteOrphanFiles refuses.
- `isFileOrphan` marks a listed path as an orphan whenever it matches no
  referenced path; the prefix-mismatch check only runs when the bare path
  matches (`table/orphan_cleanup.go:637-668`). The listing's path spelling
  is therefore load-bearing. S3 blobfs lists `prefix/` and re-adds
  `scheme://bucket` (`io/gocloud/blobfs/blob.go:400,432-457`).
- Orphan planning needs a filesystem implementing `iceio.ListableIO`
  (`WalkDir`). S3 uses the gocloud blob FS, which does
  (`io/gocloud/blobfs/blob.go:220`). **GCS does not**: Datuplet replaces
  the `gs://` factory with its own `gcsIO`
  (`pkg/datupleticeio/gcs_factory.go:318-428`; Open/Remove/Create/WriteFile
  only, no `WalkDir`, no bulk delete). See §5.3 "GCS".
- Orphan planning treats as referenced: the current and previous metadata
  files (metadata log), statistics files, every manifest list, manifest and
  data/delete file of every snapshot (`orphan_cleanup.go:473`).
- The run token lives 24 h (`RunTokenLifetime`,
  `pkg/pipelineapi/runbackend/k8s.go:26`). A gateway cannot load a table or
  commit once its token has expired.
- `datuplet login` also mints a `local-cli` token with
  `aud=datuplet-catalog`, default 24 h, operator-configurable up to 30 days
  (`pkg/pipelineapi/http/cli_token_handler.go:57-72`). See §5.3 "Age floor".
- iceberg-go's expire post-commit deletion returns only an error, joined
  onto the successful commit's return (`table/transaction.go:3374-3384`),
  and ages snapshots by their own commit time, not by when they stopped
  being current (`table/transaction.go:820-826`).
- Compaction's gateway path (`pkg/datagateway/server_v2_compact.go`) already
  provides the pattern every new RPC copies: exact declared-output-table
  check, run-lifetime context, cancel check, metrics, gRPC status mapping.
- Nested `type: object` config is within the Form Subset (used by
  `http-json-extractor` and `data-generator` schemas).

**Unverified (plan Task 1 must settle first):** lakekeeper's vended
credentials allow **listing** the table prefix and **deleting** objects
under it, for both S3 (STS session policy) and GCS (downscoped token).
Expire-snapshots also needs delete. If listing is not granted, orphan
deletion cannot work as designed and the maintainer decides the next step
before any further task runs.

## 4. Component

`components/iceberg-compactor/` is renamed to `components/iceberg-maintenance/`
(registry name, image, chart template, Makefile targets, release and PR
workflows, e2e bootstrap, docs, `pkg/pipelineapi/storage/compact_table.go`'s
`compactorComponent`). `io: {inputs: none, outputs: required}` as today.

Config (Form Subset, nested objects, every field has a `description`):

```yaml
- name: maintain
  component: iceberg-maintenance
  config:
    dry_run: false
    compact:
      enabled: true
      target_file_size_mb: 128     # optional; else gateway.targetFileSize
      min_input_files: 5           # advanced
      max_input_files: 2000        # advanced
    rewrite_manifests:
      enabled: false
      target_manifest_size_mb: 8   # advanced
    expire_snapshots:
      enabled: false
      older_than_hours: 168
      retain_last: 10
    delete_orphan_files:
      enabled: false
      older_than_hours: 72         # gateway enforces >= 25
      max_files: 10000             # advanced
  outputs: { tables: [ { name: events, bucket: raw } ] }
```

- Defaults: `compact.enabled: true`, everything else `false`, so an empty
  config behaves exactly like today's compactor.
- A config with every task disabled is a user error (exit 1).
- `dry_run` applies to every enabled task: plan and report, change nothing.
- Per table, the component runs the enabled tasks in the fixed order above.
  If a task fails for a table, the remaining tasks for **that table** are
  skipped (a failed compaction must not be followed by deletes on a table in
  an unknown state); other tables continue. Exit code is the worst across
  all tables (`>=20` beats `1` beats `0`), as today.
- One `DUPLET_STATUS_MESSAGE:` line summarises every table and task.

**Compact button**: `compact_table.go` keeps creating the hidden
`sys-compact-<hex16>` pipeline; its stage now uses `iceberg-maintenance` with
`{compact: {enabled: true, target_file_size_mb: …}}`. Route, lock, 409 rules
and the CLI `datuplet storage compact` are unchanged.

## 5. Gateway RPCs and safety rules

Three new RPCs in `api/proto/gateway/v2/gateway.proto`, beside
`CompactTable`, with matching SDK methods in `sdk/go/client.go`. Each takes
`bucket`, `table`, its own options and `dry_run`, and enforces the same
exact-declared-output-table check as `CompactTable`. Logic lives in
`pkg/icebergjob/` (`rewrite_manifests.go`, `expire.go`, `orphans.go`) so it
is testable against the existing lakekeeper fixture. Typed RPCs, not one
generic "maintain" RPC: each has different safety rules and result fields.

**Who may call them (design review, Important 1).** Today the only check is
"declared output table", so any component, or any pod reaching the
gateway's unauthenticated `:50051`, could expire snapshots or delete files
permanently. The three new RPCs are therefore served **only** when the
gateway's YAML config carries `table_maintenance: true`. The PipelineRun
controller (`pkg/k8s/controllers/`) writes that key iff the stage's
component resolves to the ComponentDefinition named `iceberg-maintenance`
*(my call: name match, not a new CRD field; registering
ComponentDefinitions is already superadmin-only)*. It is never read from
user config. Otherwise the RPCs return `PermissionDenied` (exit 1).
`CompactTable` keeps today's check (it is not destructive). The open
gateway port and missing run-pod NetworkPolicy are pre-existing and out of
scope; `known-limitations.md` records them.

### 5.1 RewriteManifests
- One `RewriteManifests` transaction + `Commit`. iceberg-go already
  replays a rewrite-manifests commit internally on conflict
  (`table/snapshot_producers.go:1648-1655`), which is safe because a
  manifest rewrite has no data effect; the gateway adds no retry of its
  own and does not use `noReplay` here.
- Result: `status` (`REWRITTEN | NOTHING_TO_DO | DRY_RUN`), manifests
  before/after, snapshot before/after.

### 5.2 ExpireSnapshots
- `ExpireSnapshots(WithOlderThan(…), WithRetainLast(effective), WithPostCommit(false))`,
  then the gateway deletes files itself *(my call — iceberg-go's post-commit
  cannot report counts and blurs "commit failed" with "commit landed,
  delete partly failed")*. Commit first, delete second; a delete failure
  never undoes the commit.
- **Deletion set (design review, Nit 2):** computed only from the `*Table`
  that `Commit` returns, never from the staged view, mirroring
  iceberg-go's own post-commit (`table/updates.go:698-756`):
  - candidates: the manifest lists and manifests of the expired
    snapshots, and every data/delete file entry in those manifests,
    including entries with status DELETED (a compacted-away file is only
    ever an ADDED/EXISTING entry in old manifests and a DELETED entry in
    the replacing one; leaving DELETED entries out means compaction never
    frees space);
  - keep: anything a retained snapshot references through a manifest
    list, a manifest, or a manifest entry whose status is **not** DELETED.
  - delete = candidates − keep.
- **No replay (design review, Important 3):** the expire transaction runs on
  a `noReplay` table, exactly like compaction. A 409 means a concurrent
  commit landed; the gateway reloads the table, recomputes the reader
  protection and the keep-set from scratch, and stages a new transaction
  (bounded attempts, same bound as the commit pool; exhaustion → `>=20`).
- **Keep-set guard (design review, Important 2):** before `Commit`, diff the
  staged metadata (`Transaction.StagedTable()`) against the base to get the
  snapshot ids iceberg-go will remove. Abort with `FailedPrecondition`
  (exit 1, nothing committed) if any of them is in the gateway's own
  keep-set: the current snapshot, every snapshot superseded less than
  `older_than_hours` ago, the last effective `retain_last` of `main`, and every
  snapshot a non-`main` ref points to. This catches per-ref retention
  settings written by a foreign engine that override the call options.
- **Reader protection** *(my call)*: a snapshot is only expired once it
  has been **superseded** for longer than `older_than_hours`, so a query
  that started on the previous snapshot just before this stage's
  compaction keeps its files. Implemented without forking iceberg-go:
  on `main`'s ancestor chain, count the snapshots whose child was
  committed after the cutoff, and pass
  `effective = max(retain_last, that count + 1)`.
- Gateway floors: `older_than_hours >= 24`, `retain_last >= 1`; lower
  values are `InvalidArgument` (exit 1). The current snapshot is never
  expired (iceberg-go guarantees this).
- Dry run *(my call)*: stage the transaction, compute the same deletion set
  (manifest reads included), report it, commit and delete nothing.
- `gc.enabled=false` on the table → `FailedPrecondition` (exit 1).
- If the commit succeeds but post-commit deletion partially fails, the run
  still succeeds with status `EXPIRED_PARTIAL` and the count of files left;
  those files are now orphans and the next orphan pass removes them.
- Result: `status` (`EXPIRED | EXPIRED_PARTIAL | NOTHING_TO_DO | DRY_RUN`),
  snapshots expired, files deleted, bytes deleted, files left.

### 5.3 DeleteOrphanFiles — the data-loss-sensitive task
- **Age floor, enforced in the gateway, not only the schema:**
  `older_than_hours >= 25`. Reason: a file is only ever committed by the run
  that uploaded it, under a run token that expires 24 h after mint; a file
  older than 25 h by object `LastModified` therefore belongs to a run that
  can no longer commit it. Default 72 h.
- **Scan only the table's own location** (`metadata.Location()`); never a
  user-supplied location. `PrefixMismatchError` stays on: a referenced path
  that differs from the listed one only in scheme or authority fails the
  run instead of being treated as an orphan.
- **Plan, then execute exactly that plan.** If the plan has more than
  `max_files` files, refuse (`FailedPrecondition`, exit 1) and delete
  nothing; the message reports the count so the user can raise the cap or
  investigate. A sudden huge orphan count usually means something is wrong.
- **Two checks before any delete (design review, Important 4; Task 6
  review, maintainer decision):** both abort with `FailedPrecondition`
  (exit 1), nothing deleted.
  1. *Table-location prefix check:* every planned path must literally start
     with the table location (one trailing `/` trimmed) plus `/`. This
     rejects any listing that respells paths (bare keys, `bucket/key`,
     another scheme such as `s3a://`, percent-encoding, interior `//`) and
     any sibling table whose location merely shares the prefix string.
  2. *Live-file guard:* no planned path may be a file the table metadata
     references, compared by object key (`scheme://authority/` and leading
     `/` stripped). The live set is read independently of the orphan
     planner: the current metadata file, every metadata-log entry,
     statistics and partition-statistics files, and for **every retained
     snapshot** its manifest list, its manifests and every non-DELETED entry
     (data and delete files).

  A path-spelling bug in a listing makes live files look orphaned, so these
  checks catch it even on small tables where `max_files` would not. No
  orphan-ratio guard (maintainer decision, review round 2): iceberg-go's
  plan carries no scanned-file count, and a ratio would permanently block
  small or often-replaced tables whose leftovers are exactly what this task
  deletes.
- `gc.enabled=false` on the table → `FailedPrecondition` (exit 1), as
  Java's DeleteOrphanFiles does (design review, Nit 1).
- Planning reads every snapshot's manifests; it must finish before any
  delete. A read failure aborts with nothing deleted (exit `>=20`).
- Deletes run with bounded concurrency; partial delete failure → exit
  `>=20` with counts; re-running is safe.
- Result: `status` (`DELETED | NOTHING_TO_DO | DRY_RUN`), orphan files,
  bytes, files deleted. (No scanned-file count: iceberg-go's plan does not
  carry one.)

**Assumption stated in docs (maintainer decision)**: only pipeline runs
write to a Datuplet warehouse. Any other writer that keeps uploaded files
uncommitted for more than 25 h is not protected: a `datuplet login`
catalog token (configurable up to 30 days), or a foreign engine with its
own credentials.

**GCS (maintainer decision): add listing to `gcsIO`.** Implement
`iceio.ListableIO.WalkDir` on `pkg/datupleticeio` `gcsIO`, plus bulk
delete if cheap. Contract, matching S3 blobfs:
- list `prefix + "/"` (never the bare prefix, which would also match a
  sibling table whose location starts with the same string);
- report every path as a full `gs://<bucket>/<key>` URI, spelled exactly
  as Lakekeeper writes locations into metadata;
- report size and `Updated` as mod time.
Tests: `gcsIO` wraps a gocloud `blob.Bucket`, so the contract above
(sibling-prefix table, URI spelling) is unit-tested against `memblob`
(the fake-gcs-server harness in `gcs_factory_test.go:311-325` is disabled).
The orphan path runs on both the S3 Lakekeeper fixture and a `gs://` table
whose IO is `gcsIO` over `memblob`, so plan → guard → delete is exercised
with the GCS listing too.

### 5.4 Shared behaviour
- All tasks run on the gateway's run-lifetime context (as compaction does),
  so a client disconnect does not abort a commit in flight; the cancel
  annotation does, and a cancel between plan and delete deletes nothing more.
- Vended-credential TTL: orphan planning on a very large table can outlast
  the vended credentials (`known-limitations.md`). Use the existing
  `VendedCreds` re-fetch path; if a task cannot finish within one
  credential lifetime it fails `>=20` and is safe to re-run.
- Metrics: `datagateway_maintenance_{runs,duration,files}` labelled by task
  and status, alongside the existing compaction metrics.
- Concurrency *(my call)*: no new per-table lock. Within one stage the
  tasks run in sequence per table, so they cannot race each other. Two
  maintenance stages on the same table in different pipelines can: an
  expire may drop the baseline an in-flight compaction checks with
  `AncestorsBetween` (`pkg/icebergjob/compact.go:349`); that compaction
  then ends with unverifiable history (exit `>=20`, nothing committed,
  safe to re-run). Documented in `known-limitations.md`.

## 6. Exit codes

- `0`: every enabled task ended in a success status (including
  `NOTHING_TO_DO`, `DRY_RUN`, `EXPIRED_PARTIAL`, and compaction's
  `SUPERSEDED`/`ALREADY_COMMITTED`).
- `1`: bad or empty config, non-explicit outputs, table not found, not a
  declared output, floor violated, `gc.enabled=false`, orphan plan over
  `max_files`, prefix mismatch, maintenance RPC called without
  `table_maintenance`, expire keep-set guard tripped, orphan live-file
  guard tripped.
- `>=20`: storage/catalog errors, conflict-retry exhaustion, cancellation,
  partial orphan deletion, credential expiry.

## 7. Testing

- Unit: component config parsing, defaults, task ordering, skip-after-
  failure, exit-code folding (extends `config_test.go`).
- Gateway unit: floors, declared-output check, `table_maintenance` gate
  (RPCs refused without it), status mapping per RPC.
- Controller unit: `table_maintenance: true` written for an
  `iceberg-maintenance` stage only, never for another component, never
  from user config.
- `icebergjob` integration (`//go:build integration`, lakekeeper fixture):
  - expire: deletes only unreferenced files; a compacted table frees the
    replaced files (DELETED-entry rule); a concurrent append between plan
    and commit forces a recompute (no replay); a `main` ref carrying
    `max-snapshot-age-ms` small enough to override the options trips the
    keep-set guard and commits nothing;
  - orphans: deletes an uncommitted file older than the cutoff, keeps a
    younger one and every referenced file; a sibling table whose location
    shares the prefix is untouched; the live-file guard aborts with nothing
    deleted (simulate with a listing that returns bare keys); on GCS, a
    correctly spelled listing passes the guard and deletes at least one
    real orphan (positive case, review round 2); `max_files`
    refusal, `gc.enabled=false` and dry run delete nothing; re-run is a
    no-op;
  - run the orphan cases on both S3 and GCS (§5.3 "GCS"); the GCS run
    drives `icebergjob` with a `gs://` table whose IO is `gcsIO` over
    `memblob`.
  - Ages are simulated with `WithFilesOlderThan` against fixture files
    (the 25 h floor is a gateway check, tested at that layer).
- **Real-credential delete (design review, Nit 4):** after plan Task 1
  confirms the vended-credential policy, add an integration case that
  lists and deletes using credentials vended by the real Lakekeeper
  fixture (not static fixture credentials), for expire and orphans.
- e2e (CI, `e2e.yml`): extend the compaction scenario: seed → compact →
  expire (older_than 24 h is too old for a fresh table, so the scenario
  asserts `NOTHING_TO_DO` and row count unchanged) and an orphan dry run;
  the destructive paths are covered by the integration tests.

## 8. Docs to update

`docs/components.md` (rename + new config), `docs/known-limitations.md`
("Orphaned files" and "No storage reclaim" entries now point to the
component; add the foreign-writer assumption), `docs/architecture.md`,
`docs/pipeline-api.md` (component name in the compact route section),
`components/iceberg-maintenance/README.md`, CLAUDE.md component list.

## 9. Left to the builder

Exact proto field names; retry bounds (reuse the commit pool's);
delete concurrency; how the summary line is formatted; whether
`rewrite_manifests` reuses compaction's operation-key idempotency (not
required: the operation is idempotent).

## 10. Design review round 1 (2026-11-10)

| # | Finding | Resolution |
|---|---|---|
| I1 | New RPCs make permanent deletion reachable from any component / any pod on `:50051` | §5: RPCs served only with controller-written `table_maintenance: true` for `iceberg-maintenance` stages |
| I2 | Per-ref retention fields override the gateway floors and reader protection | §5.2: keep-set guard on the staged removal list before commit |
| I3 | iceberg-go replays expire on 409 with a re-pointed `main` assertion | §3 corrected; §5.2: `noReplay`, recompute on conflict, deletion set from the committed table |
| I4 | GCS `WalkDir` path spelling decides whether live data is deleted | §5.3: WalkDir contract, live-file guard, orphan integration test on GCS too |
| N1 | Orphan cleanup ignores `gc.enabled` | §5.3: refuse like Java |
| N2 | Deletion-set rule ambiguous re DELETED entries | §5.2: precise candidates/keep rule |
| N3 | Gateway retry duplicates iceberg-go's rewrite-manifests replay | §5.1: no gateway retry |
| N4 | Vended-credential delete never exercised | §7: real-credential integration case after Task 1 |
| P1 | Gateway `:50051` unauthenticated, no run-pod NetworkPolicy | Pre-existing; recorded in `known-limitations.md`, out of scope |

## 11. Design review round 2 (2026-11-10)

All round-1 findings closed.

| # | Finding | Resolution |
|---|---|---|
| R2-I1 | 50 % orphan-ratio guard needs a scanned count iceberg-go's plan lacks, and permanently blocks small / often-replaced tables | Dropped (maintainer decision); the live-file guard covers the path-spelling failure |
| R2-N1 | Bare-key test proves the guard trips, not that a correct GCS listing passes | §7: positive GCS case |
| R2-N2 | Keep-set guard should say *effective* `retain_last` | §5.2 wording fixed |

## 12. Task 6 review (2026-11-10)

| # | Finding | Resolution |
|---|---|---|
| T6-I1 | Task 6 review: guard covered only current snapshot / objectKey misses spellings | Prefix check + all-snapshot live set (maintainer decision); §5.3 updated |
