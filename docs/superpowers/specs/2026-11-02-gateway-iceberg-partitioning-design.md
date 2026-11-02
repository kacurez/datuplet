# Iceberg partitioned writes in the Go Data Gateway — design (addendum)

Addendum to `2026-11-01-all-go-gateway-design.md`. It replaces that spec's non-goal "Iceberg partition specs" and its
Write path 3 clause "`partition_fields` → one warning per writer, ignored". Everything else in that spec stands.

## Intent

Maintainer, 2026-11-02: implement partitioning support on the all-Go gateway before the live verification (Task 9),
subagent-driven. Investigation showed `main` never created Iceberg partition specs: `partition_fields` only chose a
Hive-style directory for each file, and tables were committed unpartitioned, so engines got no pruning. The maintainer
chose **real Iceberg partitioning** over restoring that directory layout.

## Goals

1. Tables written by the gateway carry an Iceberg partition spec from the output's `partition_fields`. Every data file
   holds exactly one partition tuple and is committed with that tuple, so engines that filter (query-worker DuckDB,
   Trino, Spark, iceberg-go scans with a row filter) prune files.
2. Transforms: everything `iceberg.ParseTransform` accepts for the source type — `identity`, `year`, `month`, `day`,
   `hour`, `bucket[N]`, `truncate[W]`, `void`. `main`'s router allowed only identity (string/int/bool) and
   year/month/day/hour.
3. Unpartitioned tables keep exactly today's code path and performance (no routing, commit via `AddFiles`).
4. Memory stays bounded however many partition values a run produces.

## Non-goals

- Partition spec evolution (changing the spec of an existing table), sort orders, read-side filters or partition
  pruning in the gateway's own read API (the proto has no filter; reads stay full scans).
- Hive-style directory compatibility with `main` (nothing reads it).
- Partitioned position/equality delete writing (the gateway writes no deletes).

## Decisions (controller's calls unless marked)

1. **Config.** The existing `OutputTableConfig.PartitionFields []{source_column, transform}` is the input; no CRD,
   proto or SDK change. `transform` is parsed with `iceberg.ParseTransform`. The partition field name is iceberg's
   default (`<source>` for identity, `<source>_<transform>` otherwise, e.g. `event_time_day`, `id_bucket_16`).
2. **Validation at `OpenWriter`.** Unknown source column, unparseable transform, or a transform that cannot apply to
   the column's Iceberg type (`Transform.CanTransform`) → `InvalidArgument` naming the field (component sees a user
   error → exit 1 contract). Validated against the post-drop schema the table is created with.
3. **Table creation.** New tables (including FULL_LOAD's post-drop re-create) are created with
   `catalog.WithPartitionSpec(spec)` bound to the created schema.
4. **Existing tables: the catalog wins.** When the table exists, the writer partitions by the table's **current**
   spec — also for a table partitioned outside Datuplet with no `partition_fields` configured. If configured fields
   differ from the table's spec, log one warning per writer naming both and use the table's spec. No evolution.
5. **Routing (goplane).** For a partitioned spec, each incoming batch is split by partition tuple: compute each row's
   tuple with iceberg-go's transform functions on the source column, group row indices by tuple (first-seen order),
   and build one sub-batch per tuple (Arrow `compute.Take`, or zero-copy slices when the rows of a tuple are one
   contiguous run). Each sub-batch goes to that tuple's file writer, which is the Task 3 writer unchanged (same
   queue, encoder, constants, rotation, credentials rule, upload and abort cleanup). Null source values give a null
   partition value. Unpartitioned specs bypass routing entirely.
6. **Bounded open partitions.** At most `MaxOpenPartitions` tuple writers are open per table writer — default 32,
   env `DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS` (1–1024; invalid → default with a warning). A batch whose tuple is not
   open while at the cap first closes the least-recently-written tuple writer (its file completes and is kept for
   commit; a later batch for that tuple opens a new file). Memory scales with open writers, not with distinct
   values; very high cardinality yields more, smaller files (documented).
7. **File paths.** `<DataPrefix><PartitionToPath(tuple)>/<FilePrefix>-<seq>.parquet`, using iceberg-go's
   `PartitionSpec.PartitionToPath` for the segment, joined by string concatenation (opaque storage paths). `seq` is
   unique per table writer across all tuples.
8. **Commit.** `WrittenFile` carries the partition spec id and the tuple (`map[fieldID]any`). The commit pool and
   `icebergjob.CommitTableFiles` keep today's `AddFiles` / `ReplaceDataFiles` for unpartitioned tables. For a
   partitioned table they build each `iceberg.DataFile` from the file's Parquet footer metrics (as `AddFiles` does,
   via `table.FileToDataFile` with the unpartitioned spec or equivalent) **plus the explicit partition tuple**
   (`iceberg.NewDataFileBuilder`), and commit with `Transaction.AddDataFiles` (APPEND) or
   `ReplaceDataFilesWithDataFiles` (FULL_LOAD). Inferring tuples from statistics is **not** used: iceberg-go rejects it
   for `bucket` and it is unsafe for truncated string bounds. Idempotency key, 409 retry, success-zero, and the
   session barrier are unchanged. If the table's default spec id at commit differs from the files' spec id
   (concurrent spec change), the commit fails with a clear error (FailedApplication).
9. **Reads unchanged.** `PlanFiles` already handles partitioned tables; raw eligibility is unchanged.

## Verification

- Unit (local catalog): identity string + `day(ts)` table; rows spanning several tuples in one batch and across
  batches → one file per tuple (until rotation), every committed `DataFile`'s partition tuple matches its rows, and
  a row-filter scan (`iceberg.EqualTo("country","DE")`) plans only that tuple's files; `bucket[16]` and
  `truncate[4]` on strings longer than 16 chars; null partition values; open-partition cap eviction (cap 2, 3
  tuples interleaved → files closed and reopened, all rows committed exactly once); existing-table-spec-wins
  warning; validation errors; FULL_LOAD replace on a partitioned table; unpartitioned table still commits via
  `AddFiles` (spy/fake).
- Integration (Lakekeeper + silo fixture): server end-to-end write to a new partitioned table, `Commit`, then an
  iceberg-go scan with a row filter returns only matching rows and plans fewer files than the unfiltered scan;
  APPEND twice; the gateway's own read returns all rows.
- Perf (Task 9 harness): one identity-partitioned run with low cardinality next to the unpartitioned baseline;
  report wall time and peak RSS.

## Risks

- Memory per open partition (~row group under encode + upload part buffers) — bounded by decision 6; the cap's
  default is a guess to be checked in Task 9.
- Per-row transform evaluation is scalar Go; CPU cost on high-cardinality splits is measured, not assumed.
