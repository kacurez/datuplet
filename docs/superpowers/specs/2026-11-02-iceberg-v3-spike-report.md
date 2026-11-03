# Spike: Iceberg format v3 on the all-Go gateway, and a Lakekeeper upgrade

Date: 2026-11-02. Branch `spike/iceberg-v3` (from `public/main` `b3fca0d5`).
This is a time-boxed, throwaway spike. Its code lives in the session scratchpad
(`v3spike/gw`, a scratch Go module that `replace`s this repo) and is not
committed. Only this report is committed.

## Verdict

- **v3 works end to end today on Lakekeeper v0.12.1.** We checked create,
  commit, row lineage, deletion vectors, DuckDB 2.0 reads and VARIANT. None of
  it needs a Lakekeeper upgrade.
- **Unpartitioned v3 works through the unchanged gateway.** If the table is
  pre-created as v3, the real `ServerV2` write path, the commit pool and
  `AddFiles` / `ReplaceDataFiles` all work with no code change. The snapshots
  carry `first-row-id` and `added-rows`, and `next-row-id` advances.
- **Partitioned v3 needs a small patch.** Today it is blocked twice: by our own
  `CheckWriteSpec` and by iceberg-go's `first_row_id` requirement. A patch of
  about 20 lines (below) makes APPEND and FULL_LOAD work, and the row-id ranges
  do not overlap.
- **The main expected benefit is confirmed.** A merge-on-read delete on v3
  writes one Puffin deletion vector, and that vector is attached to exactly one
  file. The gateway then serves every other file raw. Measured on 2 M rows in 7
  files:

  | Version | raw / decoded files | Read time |
  |---|---|---|
  | v3 | 6 / 1 | 137 ms |
  | v2 | 0 / 7 | 411 ms |

  On v2 the position-delete file is planned against all 7 files, so every file
  is decoded.
- **The Lakekeeper upgrade is unrelated to v3.** It is helpful only for a
  per-warehouse default-version knob. It is a medium-sized change, mostly
  because Lakekeeper v0.14 has a breaking OpenFGA model change (4.4 → 4.13)
  that Datuplet's forked model and its `describe` checks depend on.

## Setup and versions

- Fixture: a copy of the perf fixture with `LK_IMAGE` parametrised (containers
  `v3s-pg`, `v3s-lk` and `v3s-silo`, host network, PG 55532, LK 58181, S3
  59000, authn off). The original perf fixture was not touched.
- Images: `quay.io/lakekeeper/catalog:v0.12.1` and `:v0.14.2`, `postgres:16`,
  and `pgsty/silo:RELEASE.2026-10-26T00-00-00Z`.
- Libraries: iceberg-go v0.7.0 and arrow-go v18.8.0, the same as `go.mod` at
  `b3fca0d5`.
- DuckDB CLI `v2.0.0 (Cyanoptera) d54cbbe3df`. Extension versions: iceberg
  `4c0c0874c4`, httpfs `77392a7207`, avro `345c3f4e82`.
- DuckDB attaches the way `components/queryengine/attach.go` does: an ICEBERG
  secret holding a TOKEN, then `ATTACH '<project>/<warehouse>' (TYPE ICEBERG,
  URI …, ACCESS_DELEGATION_MODE 'vended_credentials', READ_ONLY)`.
- Gateway path: a real `datagateway.NewServerV2(…, goplane.Factory)`, the same
  way `TestIntegrationServerWriteEndToEnd` does it.
  - The run token is RS256 and validated against a JWKS stub.
  - Arrow IPC chunks are POSTed over HTTP, followed by `CloseWriter` and
    `Commit`, which goes through the commit pool and
    `icebergjob.CommitTableFiles`.
  - Reads use `OpenReader` with `FORMAT_PARQUET` over HTTP, and the
    `read summary:` log line is captured.

## Q1: Lakekeeper v0.12.1 and v3 (works)

We called iceberg-go `CreateTable` with the property `format-version: 3`, then
did a native `Append` and `Commit`:

```
CreateTable q1_default format-version="":  OK -> metadata format-version=2
CreateTable q1_v3      format-version="3": OK -> metadata format-version=3
CreateTable q1_v3_part format-version="3": OK -> metadata format-version=3   (identity partition)
Append+Commit: OK
table [v3 q1_v3]: format-version=3 next-row-id=10 snapshots=1
  snapshot … op=append first-row-id=0 added-rows=10
scan rows=10
```

- **v0.12.1 accepts v3.** Its create path parses `"v3" | "3"`
  (`crates/lakekeeper/src/server/tables/create_table.rs:435` at tag v0.12.1).
- **The NVIDIA cudf-spark error is not reproducible on v0.12.1 or v0.14.2.**
  The error text is "Invalid format version specified in table_properties: 3",
  and it is the fallback arm of that same `match`. It must come from an older
  Lakekeeper whose `match` lacked the `"3"` arm.
- **v0.14.2 gives the same results, with no policy setting needed.**
  - The default warehouse shows `"allowed-format-versions":[1,2,3]`.
  - The unchanged fixture `bootstrap.sh` (bootstrap plus warehouse create)
    works on v0.14.2.
  - `pipeline-api admin lakekeeper-bootstrap` uses the same endpoints:
    `/management/v1/bootstrap`, `/info` and `/warehouse`. Their shapes are
    unchanged; we checked this by reading the code, not by running it.
    Its OpenFGA tuple writes are the risk; see Q7.
- **The policy works as documented on v0.14.2.**
  - We set it with `POST /management/v1/warehouse/{id}/format-version-policy`
    `{"allowed-format-versions":[1,2]}`. A v3 create then fails with
    `FormatVersionNotAllowed: Table format version 'v3' is not allowed in this
    warehouse. Allowed versions: [1, 2]`.
  - With `{"allowed-format-versions":[1,2,3],"default-format-version":3}`,
    the gateway itself creates an unpartitioned table as **v3**
    (`auto_unpart: format-version=3 next-row-id=20000`).
  - The partitioned table stays **v2**, because `partition.go` pins it
    (`auto_part: format-version=2`).

## Q2: Unpartitioned v3 through the real gateway write path (works, no code change)

The table was pre-created as v3. Then each run wrote 5×10 000 rows through
`ServerV2` with `TargetFileSize` 64 KiB, which gives 5 files per run. We ran
two APPEND runs, then one FULL_LOAD.

```
run 0: write rows=50000 files=5 commit=true
  format-version=3 next-row-id=50000
  snapshot … op=append first-row-id=0 added-rows=50000
  files first_row_id 0,10000,20000,30000,40000 (inherited from manifest first_row_id=0)
run 1: write rows=50000 files=5 commit=true
  format-version=3 next-row-id=100000
  snapshot … op=append first-row-id=50000 added-rows=50000
  files first_row_id 50000..90000
FULL_LOAD: write rows=20000 files=2 commit=true
  next-row-id=120000; snapshot … op=overwrite first-row-id=100000 added-rows=20000
  iceberg-go scan rows=20000
```

- `AddFiles` (unpartitioned) does not require an explicit `first_row_id`. The
  manifest-list writer assigns the manifest's `first_row_id`, and the files
  inherit theirs from it, as the spec allows.
- DuckDB reads `_row_id` on the FULL_LOAD result as min 100000, max 119999,
  20000 distinct values, which is consistent.
- Results on v0.14.2 are identical.

## Q3: Partitioned v3 with an explicit `first_row_id` (works with a ~20-line patch)

Without the patch it is blocked in two places.

1. Our guard, at `pkg/datagateway/lakekeeper/partition.go:152`:
   `rpc error: code = InvalidArgument desc = lakekeeper open writer: table
   v3.q3_partunpatched: invalid partitioning: partitioned writes to
   format-version 3 tables are not supported`
2. With only the guard relaxed, iceberg-go v0.7.0 rejects the commit at
   `table/transaction.go:1166`, the `validateDataFilesToAdd` check with
   `requireFirstRowID=true` for `AddDataFiles`:
   `data file s3://…/data/k=k0/part-61a2ac74-w1-00000.parquet is missing
   first_row_id which is required for v3 tables for AddDataFiles: use
   DataFileBuilder.FirstRowID() to set it explicitly`

The patch below was applied temporarily, tested, then reverted. It keeps the
`DataFileBuilder`s, and on every retry attempt assigns
`first_row_id = fresh.Metadata().NextRowID() + rows of earlier files in this
commit`:

```diff
--- a/pkg/datagateway/lakekeeper/partition.go
+++ b/pkg/datagateway/lakekeeper/partition.go
@@ -149,7 +149,7 @@ func CheckWriteSpec(t *icebergtable.Table) error {
-	if v := t.Metadata().Version(); v >= 3 {
+	if v := t.Metadata().Version(); v >= 4 { // V3 SPIKE: was >= 3
--- a/pkg/icebergjob/commit_shared.go
+++ b/pkg/icebergjob/commit_shared.go
@@ CommitTableFiles
 	var dataFiles []iceberg.DataFile
+	var builders []*iceberg.DataFileBuilder
 	if part != nil {
-		if dataFiles, err = buildDataFiles(ctx, tbl, dataPaths, part); err != nil {
+		if dataFiles, builders, err = buildDataFiles(ctx, tbl, dataPaths, part); err != nil {
@@ inside RetryOnConflict, after the spec-id check
+			// v3: AddDataFiles requires first_row_id; assign it per attempt
+			// from the fresh table's next-row-id (V3 SPIKE).
+			if fresh.Metadata().Version() >= 3 {
+				next := fresh.Metadata().NextRowID()
+				for i, b := range builders {
+					b.FirstRowID(next)
+					next += dataFiles[i].Count()
+				}
+			}
@@ buildDataFiles: also return the builders
-func buildDataFiles(...) ([]iceberg.DataFile, error) {
+func buildDataFiles(...) ([]iceberg.DataFile, []*iceberg.DataFileBuilder, error) {
+	builders := make([]*iceberg.DataFileBuilder, len(paths))
 			out[i] = b.Build()
+			builders[i] = b
-	return out, nil
+	return out, builders, nil
```

`DataFileBuilder.Build()` returns the builder's own pointer, so calling
`FirstRowID` again on a later attempt updates the same `DataFile` in place.

Result, on a table partitioned by `identity(k)`, with 3 files per run (one per
`k`):

```
APPEND run 0: next-row-id=30000; snapshot first-row-id=0 added-rows=30000
  k=k0 first_row_id=0, k=k1 10000, k=k2 20000
APPEND run 1: next-row-id=60000; snapshot first-row-id=30000 added-rows=30000
  k=k0 30000, k=k1 40000, k=k2 50000          -> no overlap with run 0
FULL_LOAD (ReplaceDataFilesWithDataFiles): next-row-id=80000;
  snapshot op=overwrite first-row-id=60000 added-rows=20000
  files 60000 / 66667 / 73334 (6667+6667+6666 rows); scan rows=20000
```

Results on v0.14.2 are identical. The gateway read of the FULL_LOAD result
logs `raw_files=3 decoded_files=0 rows=20000`.

### What productionizing it would take

- **Code: about 20 lines** in `commit_shared.go`, as above, plus removing the
  v3 rejection in `CheckWriteSpec`. The v2 pin in `LoadOrCreateTable` can then
  go, so the warehouse default applies. Update `TestPartitionValidationErrors`;
  with the patch it fails with "err = <nil>, want the v3 rejection". Add a unit
  test for the assignment and a partitioned-v3 case to the goplane integration
  tests. Rough size: half a day to a day, including e2e.
- **Retry is safe.** The IDs are computed inside the `RetryOnConflict` attempt
  from the table that attempt just loaded. A 409 reloads the table and
  recomputes them, and a stale base is rejected by the commit's ref
  requirement.
- **Idempotency is unaffected.** The commit-key check runs first, so an
  idempotency hit never writes.
- **Hazard: iceberg-go's own commit retry.** With table property
  `commit.retry.num-retries` > 0 (default 0, see `table/properties.go:160`),
  iceberg-go rebuilds the manifest list internally against fresh metadata
  (`snapshot_producers.go` ~1706). The snapshot's first-row-id is moved, but
  the explicit per-file `first_row_id`s are not, so ranges could overlap. A
  production version should force that property to 0 for these commits, or
  assert it is unset.
- **Alternative: change iceberg-go upstream.** `AddDataFiles` could accept a
  nil `first_row_id` and let manifest inheritance assign it, which is what
  `AddFiles` already relies on. Its comment justifies the requirement with
  "cannot fabricate row IDs for files it did not write", but inheritance would
  work here.

## Q4: Deletion vectors and raw passthrough (works; main benefit confirmed)

Each table had 5 files of 10 000 rows, written through the gateway with
`write.delete.mode=merge-on-read`. The delete was iceberg-go
`Transaction.Delete(id >= 5 AND id < 10)`, which touches only the first file.
Metrics were left at their defaults: we did not set
`write.metadata.metrics.default=full`.

```
v3:
  Delete summary: added-position-delete-files:1 added-position-deletes:5 added-files-size:405
  delete entry: 00000-0-…-deletes.puffin content=Positional_Deletes fmt=PUFFIN rows=5 ref=part-…-00000.parquet
  snapshot op=delete first-row-id=50000 added-rows=0   (next-row-id unchanged)
  PlanFiles: tasks=5 tasks_with_deletes=1   (part-…-00000: DVs=1)
  iceberg-go scan rows=49995, gateway rows=49995
  read summary: raw_files=4 decoded_files=1 rows=49995 bytes=408046 chunks=5 duration=52ms
v2 (control):
  delete entry: …-00001.parquet content=Positional_Deletes fmt=PARQUET rows=5 ref=-
  PlanFiles: tasks=5 tasks_with_deletes=5   (every task: pos-deletes=1)
  iceberg-go scan rows=49995, gateway rows=49995
  read summary: raw_files=0 decoded_files=5 rows=49995 bytes=463336 chunks=1 duration=78ms
```

- **v3 writes a deletion vector, not a position-delete file.** It is a Puffin
  blob whose `referenced_data_file` is set, so `PlanFiles` attaches it to
  exactly one file.
- **v2 attaches its delete everywhere.** The position-delete file has no
  `referenced_data_file`, and its `file_path` bounds are truncated, so it is
  attached to every file. On v2 every file is decoded.

Scaled run: 40×50 000 = 2 000 000 rows, `TargetFileSize` 2 MiB, which gave 7
files. The same 5 rows were deleted.

| | v3 (DV) | v2 (position delete) |
|---|---|---|
| tasks with deletes | 1 / 7 | 7 / 7 |
| `raw_files` / `decoded_files` | 6 / 1 | 0 / 7 |
| server read duration (log) | 137 ms | 411 ms |
| client wall time | 156 ms | 424 ms |
| rows (gateway, iceberg-go, DuckDB) | 1 999 995 (all three) | 1 999 995 (all three) |

The goplane reader already counts `DeletionVectorFiles` in `hasDeletes`
(`goplane/eligibility.go`), so no reader change was needed. Results on v0.14.2
are identical: the small run logged `raw_files=4 decoded_files=1`.

## Q5: DuckDB 2.0 reads (works)

DuckDB was attached to the REST catalog with vended credentials.

| Table | Result |
|---|---|
| `q1_v3` (plain v3) | 10 |
| `q2_ordersb` (gateway-written v3 after FULL_LOAD) | 20000 |
| `q4_v3` (with DV) | 49995; `count(*) FILTER (WHERE id BETWEEN 5 AND 9)` = 0 |
| `q4_v2` (control) | 49995 |
| `q4_v3big` / `q4_v2big` | 1999995 / 1999995 |
| `q3_part` (partitioned v3, patched commit) | 20000 |

For `q3_part`, `WHERE k='k1'` returns 6667, and `EXPLAIN ANALYZE` shows
`Total Files Read: 1` (`…/data/k=k1/part-…parquet`), so partitions are pruned.

The row-lineage column works: `SELECT min(_row_id), max(_row_id),
count(DISTINCT _row_id)` returns `100000 | 119999 | 20000`.

We ran the same checks against v0.14.2 and got the same counts.

## Q6: VARIANT, quick check (works up to the gateway write path)

- Creating a v3 table with `v: variant` works on v0.12.1 and v0.14.2
  (`2: v: optional variant`).
- iceberg-go `Append` of an Arrow `extension<arrow.parquet.variant>` column
  (`extensions.NewDefaultVariantType`, filled from JSON) works.
- A Parquet file written by pqarrow with that column, followed by
  `txn.AddFiles` and commit, also works. The iceberg-go scan returns
  `{"a":100,"nested":{"x":[1,2,100]},"tag":"t1"}`.
- DuckDB 2.0 returns `typeof(v) = VARIANT` and the values
  `{'a': 0, 'nested': {'x': [1, 2, 0]}, 'tag': t0}`, and `v.a` / `v.tag`
  extraction works.
- A gateway read of this table works on the raw path (`raw_files=2
  decoded_files=0 rows=10`).
- **First blocker, found by reading the code (not run):** gateway writes. The
  Arrow EXTENSION type is not mapped in `pkg/datagateway/schema/types.go:252`
  (`unsupported Arrow type`). We did not implement gateway support.

## Q7: Lakekeeper v0.12.1 → v0.14.2 (not needed for v3; helpful; medium-sized, with one breaking item)

Sources: `gh release view` for v0.12.2 … v0.14.2. Only v0.13.0 and v0.14.0
contain a BREAKING section. We also read the Lakekeeper source at tags v0.12.1
and v0.14.2.

### Breaking or behaviour changes that touch us

1. **The OpenFGA model went from 4.4 to 4.13. This one is breaking for us.**
   - **What changed upstream.** v0.14.0 split granted from effective
     privileges (#2029), so `describe`, `select` and `modify` now mean
     *granted here*, and `*_effective` means *implied*. v0.13.0 also removed
     the intermediate model directories: v0.14.2 ships only `v2.1`, `v3.4`,
     `v4.0` and `v4.13`.
   - **Datuplet pins the old model.** `charts/datuplet-app/files/fga/` is a
     fork of v4.4 with 4 `DATUPLET EXTENSION`s (such as `explicit_reader` and
     project `editor`/`viewer`). The lakekeeper-config Secret sets
     `LAKEKEEPER__OPENFGA__CONFIGURED_MODEL_VERSION=4.4`.
   - **The pin stops working on v0.14.2.** `get_active_auth_model_id`
     (`crates/authz-openfga/src/migration.rs:113-116` at v0.14.2) logs the
     configured version but looks up the authorization model id for the
     hard-coded `ACTIVE_MODEL_VERSION` (4.13). With 4.4 configured, migration
     is skipped (`:147`) and no 4.13 model exists, so this ends in
     `ActiveAuthModelNotFound`. We inferred this from the code; we did not run
     it against OpenFGA.
   - **Our checks change meaning.** pipeline-api checks `describe` on
     `project` in `pipeline_handlers.go:481,498`, `run_handlers.go:148,201`,
     `apps/handlers.go:35` and `project_handlers.go:73`. In 4.13 that relation
     is granted-only (`define describe: [user, role#assignee]`), so these
     checks must move to `describe_effective`.
   - **Work needed.** Re-fork 4.13 (12 component files, against our 9), rebase
     the 4 extensions onto the split, and bump `fgaModel.version` and
     `platform.fgaModelVersion` together. This is a *breaking* model change
     under `docs/fga-model-upgrades.md`, so the procedure is either a DB wipe
     or a tuple-rewrite migration. Re-run the FGA integration test and e2e.
2. **New default storage layout (v0.13.0).** New namespaces use
   `<base>/<tabular-uuid>` instead of `<base>/<ns-uuid>/<tabular-uuid>`. We
   treat storage paths as opaque, and the spike's writes on v0.14.2 worked.
   Existing tables keep their paths.
3. **Migrations are one-way (v0.13.0).** "refuse to start against a newer
   (downgraded) database" means a Lakekeeper rollback after upgrade needs a DB
   restore. Note this in the upgrade runbook and in `upgrade-e2e`.
4. **The unhealthy health endpoint now returns 503 (v0.13.0).** Our init
   containers and the fixtures poll for 200, which is fine. Check the
   subchart's probes.
5. **authn (v0.14.0).** `LAKEKEEPER__OPENID_SCOPE` is now one word and
   requires `PROVIDER_URI`, and unknown `OPENID_PROVIDERS__*` keys abort
   startup. **We do not set either.** `config-secret.yaml` sets only
   `OPENID_PROVIDER_URI`, `OPENID_ADDITIONAL_ISSUERS` and `OPENID_AUDIENCE`.
   Admission-gate changes do not touch us; we use none.
6. **`update_storage_credential` was removed from the permissions/check API
   (v0.14.0).** We found no callers in `cmd/` or `pkg/`.
7. **Tables can no longer set `write.object-storage.path` or
   `write.folder-storage.path` (v0.14.x).** We do not set them.
8. **Helm subchart.** We vendor `lakekeeper-0.10.0.tgz`. The latest
   lakekeeper-charts release is `lakekeeper-0.13.0` (2026-10-29), and its
   release notes are empty. We did not check whether v0.14.2 is compatible
   with chart 0.10.0's migration Job, `helmWait` and `check-db`. Verify it on
   OrbStack.

### Upgrade verdict

- **Not needed for v3.** v0.12.1 creates, commits and serves v3 tables, DVs
  and VARIANT.
- **Merely helpful.** It adds the per-warehouse format-version policy (v0.13.0)
  and its enforcement on registerTable (v0.14.0). With `default-format-version:
  3` it would flip gateway-created unpartitioned tables to v3 with zero gateway
  code. Without it, the gateway can set `format-version: 3` itself on create,
  which works on v0.12.1 as Q1 shows.
- **Size: medium, about 2-4 days with e2e**, dominated by the FGA 4.13 re-fork
  and the `describe` → `describe_effective` call-site change, plus the subchart
  bump and an OrbStack e2e / upgrade-e2e pass. **Do it as its own PR, decoupled
  from v3.**

## Recommendations

**v3:**

1. **Unpartitioned v3 now.** It works through the unchanged gateway.
   - The only change needed is to pass `format-version: 3` when
     `LoadOrCreateTable` creates an unpartitioned table, or to make it a
     configuration knob.
   - Benefits: row lineage for free, and DV deletes that keep raw passthrough
     for untouched files (about 3× on the measured read).
   - DuckDB 2.0 reads it.
   - Size: a few lines plus a test.
2. **Partitioned v3 with `first_row_id`: yes, as a follow-up.** It is the
   ~20-line patch above, plus removing the guard and the v2 pin and adding
   tests. It is safe across our retry and idempotency loop if
   `commit.retry.num-retries` is kept at 0. Alternatively, propose upstream
   that iceberg-go `AddDataFiles` accept a nil `first_row_id` and rely on
   inheritance. Size: about 1 day including integration and e2e.
3. **Keep v2 only for existing tables.** iceberg-go v0.7.0 has a
   format-version upgrade path, but we did not test upgrading in place.
4. **VARIANT later.** It needs an Arrow extension-type mapping in
   `pkg/datagateway/schema` and on the gateway write path. The catalog,
   iceberg-go and DuckDB already handle it.

**Lakekeeper:**

- **Upgrade to v0.14.2 separately, not as a v3 prerequisite.** Start with the
  FGA 4.13 re-fork.
- If we want to stay lower-risk, v0.13.8 (the 0.13 line) still has the
  format-version policy. It has the storage-layout and one-way-migration
  changes, but the OpenFGA model split arrived in v0.14.0. We did not test
  v0.13.8, or which model version it ships.

## Follow-up work size

| Item | Size |
|---|---|
| Unpartitioned v3 default (create with `format-version: 3`, test) | ~0.5 day |
| Partitioned v3 (`first_row_id` patch, guard and pin removal, tests, retry-property guard) | ~1 day |
| Lakekeeper v0.14.2 (FGA 4.13 re-fork, `describe_effective`, subchart, runbook, e2e) | ~2-4 days |
| VARIANT on the gateway write path | not sized (schema mapping plus golden types) |

## Side finding (pre-existing, not changed)

The goplane integration fixture (`pkg/datagateway/goplane/fixture_test.go`,
`removeContainers`) runs `docker rm -f` without `-v`. `postgres:16` and the
silo image both declare anonymous volumes; during this spike both `v3s-pg` and
`v3s-silo` had one. So every `-tags=integration` run of that package leaks two
volumes. That is a likely source of the leftover GBs. The fix is
`docker rm -f -v`. The spike's own fixture copy uses `-v`.
