# Iceberg partitioned writes in the Go Data Gateway — design (addendum)

Addendum to `2026-11-01-all-go-gateway-design.md`. It replaces that spec's non-goal "Iceberg partition specs" and its
Write path 3 clause "`partition_fields` → one warning per writer, ignored". Everything else in that spec stands.
Revision 2 applies the plan review (`codex-plan-review-8b.txt`, Claude stand-in for Codex; maintainer: "Yes, apply and
go").

## Intent

Maintainer, 2026-11-02: implement partitioning support on the all-Go gateway before the live verification (Task 9),
subagent-driven. `main` never created Iceberg partition specs: `partition_fields` only chose a Hive-style directory per
file and tables were committed unpartitioned, so engines got no pruning. The maintainer chose **real Iceberg
partitioning** over restoring that layout.

## Goals

1. Tables written by the gateway carry an Iceberg partition spec from the output's `partition_fields`. Every data file
   holds exactly one partition tuple and is committed with it, so filtering engines (query-worker DuckDB, Trino, Spark,
   iceberg-go row-filter scans) prune files.
2. Transforms: `identity`, `year`, `month`, `day`, `hour`, `bucket[N]`, `truncate[W]`, `void`, on the source types
   Iceberg allows for each.
3. Unpartitioned tables keep today's code path and performance (no routing, commit via `AddFiles`), except the
   decision 11 fix that FULL_LOAD replaces data files only.
4. The number of open files per table writer is bounded; the memory that bound implies is measured in Task 9.

## Non-goals

- Partition spec evolution, sort orders, read-side filters/pruning in the gateway's own read API (no filter in the
  proto; reads stay full scans), Hive-directory compatibility with `main`, partitioned delete files.
- Partitioned writes to **format-version 3** tables: iceberg-go v0.7.0's `AddDataFiles` /
  `ReplaceDataFilesWithDataFiles` require `first_row_id` on every v3 DataFile (`table/transaction.go:1167-1171`), which
  the gateway cannot assign. Such a table fails the writer (decision 3). Unpartitioned v3 tables keep today's
  `AddFiles` path.
- (Rev 4 removes the former non-goal "byte budget for open partition files": Task 9 measured it as needed.)

## Decisions

1. **Config.** The existing `OutputTableConfig.PartitionFields []{source_column, transform}`; no CRD, proto or SDK
   change. Field names come from `iceberg.GeneratePartitionFieldName` (e.g. `event_time_day`, `id_bucket_16`,
   `name_trunc_4`).
2. **Spec resolution and validation — where.** Both happen where the table is loaded or created:
   `LoadOrCreateForWrite`, reached at `OpenWriter` when the component supplies a schema and at the first chunk
   (`ensureTableWriter`, `server_v2_http.go` and the gRPC equivalent) when the schema is inferred. A failure there is
   returned as gRPC `InvalidArgument` (HTTP 400) from whichever RPC triggered it, and nothing is written. (The gRPC
   code is what the gateway controls; how the SDK maps it to an exit code is unchanged.)
3. **Validation — what.** Rejected with `InvalidArgument`, naming the field:
   - a `source_column` not in the schema the table is created with (post-drop schema);
   - a transform string `iceberg.ParseTransform` rejects **or returns `UnknownTransform`** (it returns
     `UnknownTransform{}` with a nil error for strings like `days` or `bucket[-1]`, `transforms.go:47-97`);
   - a transform whose `CanTransform` is false for the source column's Iceberg type;
   - any error from building the spec (`iceberg.NewPartitionSpecOpts`, e.g. two time transforms on one column,
     duplicate partition names, a partition name colliding with a different schema column);
   - an existing table whose current spec contains an `UnknownTransform`;
   - a partitioned table with format version ≥ 3.
4. **Table creation.** A new table is created with `catalog.WithPartitionSpec(spec)` bound to the post-drop schema.
   After create, the writer uses the spec and schema **as returned by the catalog** (field ids may be reassigned,
   `catalog/rest/rest.go:1507-1515`), never the locally built spec. Nothing re-creates an existing table (FULL_LOAD
   replaces files, not the table), so a changed `partition_fields` keeps the old spec until the table is dropped.
5. **Existing tables: the catalog wins.** For an existing table the writer partitions by its **current default
   spec**, also when no `partition_fields` is configured. "Differ" = the configured list, resolved to
   (source column name, transform string) pairs, is not equal as a sequence to the table spec's pairs; then one
   warning per writer names both and the table's spec is used. A spec source column missing from the table schema →
   `InvalidArgument`; present in the table schema but absent from the writer's Arrow schema → the partition value is
   null for every row.
6. **Tuple computation.** The source type is the **table schema's** Iceberg type for the source field (looked up by
   name in the writer's Arrow record), and Arrow values convert to the **Go type of that Iceberg type** — the type
   iceberg-go's transformers assert against the table column (`transforms.go:482-490, 573-627, 731-812, 994-1245`):
   - Iceberg `int` → `int32` (from int8/int16/int32/uint8/uint16 Arrow); `long` → `int64` (from any signed int,
     uint8/16/32; uint64 > MaxInt64 → write error, as R3c); `float` → `float32`; `double` → `float64` (from
     float32/float64); `boolean` → `bool`; `string` → `string`; `binary`/`fixed` → `[]byte`; `decimal` →
     `iceberg.Decimal`;
   - `date` → `iceberg.Date` (date32 days; date64 ms floor-divided by 86 400 000);
   - `timestamp` / `timestamptz` → `iceberg.Timestamp` microseconds (any Arrow timestamp unit, floor division:
     s ×1e6, ms ×1e3, ns ÷1e3);
   - each (Arrow type, table type) pair of a non-`void` source field is checked **once when the writer resolves the
     spec**; an unsupported pair → `InvalidArgument` naming column and both types (not one error per batch).
   The tuple value is the transformer's result typed per `Transform.ResultType` (`int32` for year/month/hour/bucket,
   `iceberg.Date` for day, the source Go type for identity/truncate). Null source → null value. The tuple map has an
   entry for **every** spec field id — `nil` for nulls and for every `void` field — because the commit validator
   requires presence (`table/transaction.go:1071-1078`); do not copy `FileToDataFile`'s drop-nil pattern.
7. **Grouping key.** Rows are grouped by a canonical byte encoding of the tuple (type tag + value bytes; strings and
   `[]byte` by content; floats by IEEE bits with NaN canonicalised; nil distinct), not by Go map equality.
8. **Writer structure (goplane).** One table writer keeps today's single queue (capacity 4) and single `run`
   goroutine. A partitioned writer **splits each batch after dequeue** (so queue accounting, `QueuedBytes` and
   back-pressure stay per incoming batch), into one sub-batch per tuple in first-seen order, using `compute.Take`
   (or a zero-copy slice for a contiguous run). It keeps `map[key]*openFile` plus an LRU. Each open file is today's
   per-file machinery (encoder, sink, IO context, credentials check, rotation). Rotation bytes for a sub-batch are
   counted from the sub-batch's own buffers (`compute.Take` output) or, for a slice, as parent bytes × slice rows /
   parent rows. File sequence numbers come from one writer-wide counter. `fail`, `finishLast`, `Abort`, plane
   `Close`/`Interrupt` and context cancellation act on **every** open file (cancel IO context, close sink, abort
   multipart, remove local partials); any one file's error aborts all open files and fails the writer. Aborts of open files run **concurrently** (each still
   bounded at 30 s by R3a), so cleanup of up to 32 files fits within plane Close's 30 s wait. Unpartitioned
   specs keep today's single-file path unchanged.
9. **Open files and memory (rev 4, after Task 9 measurements; maintainer: "continue as you suggest").** Task 9
   measured ~82 MiB per open partition file (a full 229 376-row row group buffered in the encoder): bucket[8] peaked
   at 1 174 MiB vs 517 MiB unpartitioned. A **shared row-group byte budget** now bounds that:
   - `PartitionBufferBytes` — default 256 MiB, env `DATUPLET_GATEWAY_PARTITION_BUFFER_BYTES` (16 MiB–8 GiB, invalid →
     default + warning), read once in `goplane.Factory`. It applies to **partitioned** writers only; unpartitioned
     writers keep today's row-group rule unchanged.
   - **Trigger, not a hard bound** (Codex plan review 8c). An open file's *buffered page bytes* =
     `pqarrow.FileWriter.RowGroupTotalCompressedBytes()` of its current buffered row group (the encoded pages it
     holds). This is an approximate flush trigger: it excludes each column encoder's not-yet-paged values,
     dictionary state, level and scratch buffers, and encode-time temporaries. `RowGroupTotalBytesWritten()` is not
     added (it re-counts page payload). The accepted memory result is the **measured gateway RSS in Task 9**, not a
     formula.
   - Before writing a sub-batch to file *f*: if *f*'s buffered page bytes ≥ `PartitionBufferBytes / openFiles`
     (its fair share at that moment), *f* first ends its row group (`NewBufferedRowGroupChecked`, which flushes it to
     the upload stream). After the write, while the sum over all open files exceeds `PartitionBufferBytes`, the file
     with the most buffered page bytes ends its row group. Both checks run inside the writer's `run` goroutine, which
     owns all file state. Row groups therefore shrink as more files are open, and every partition still gets one
     file (until rotation). Rotation accounting is unchanged: cumulative Arrow bytes fed per file vs
     4 × `TargetFileBytes`; `WriteBuffered`'s 229 376-row limit still applies independently.
   - **Errors at both flush sites** (before a write, and flushing a *different* file's largest row group) return
     through the existing `consume` → `fail` path, which keeps every affected file in the open set, cancels upload
     contexts before closing sinks, aborts all open files concurrently, and leaves no partial objects, local files or
     open multipart uploads.
   - `MaxOpenPartitions` default lowers from 32 to **16**, same env `DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS` (1–1024).
     Each open file also holds upload buffers (an S3 multipart part is ≥ 5 MiB), which the byte budget cannot
     shrink, so the cap bounds that part. A sub-batch for a tuple not open while at the cap finishes the
     least-recently-written file, as before.
   - **More distinct tuples per batch than the cap** (Task 9: bucket[200] → 48 324 files of ~41 rows, 532 s, 43 s
     commit) cannot be streamed cheaply. The first eviction's warning now says so and names the remedy: cluster the
     input by the partition column(s) or use a coarser transform. In addition, when a writer closes having produced
     more than 4 × distinct-tuples files, it logs one summary warning (table, files, tuples). Distinct tuples are
     counted for the writer's whole lifetime (a set of the canonical tuple keys' 64-bit hashes — 8 bytes per tuple;
     a hash collision can only under-count, which only suppresses the warning). Documented in
     `known-limitations.md` and `troubleshooting.md` with the Task 9 numbers.
10. **File paths.** `<DataPrefix><PartitionToPath(tuple, schema)>/<FilePrefix>-<seq>.parquet` via
    `PartitionSpec.PartitionToPath`, joined by concatenation. A resulting object key over 1 024 bytes → write error
    naming the field (S3 key limit), not a truncated path.
11. **Commit.** `WrittenFile` carries `SpecID` and `Partition map[int]any`. `CommitJob` gains per-file tuples and the
    spec id; `CommitFunc` / `icebergjob.CommitTableFiles` take them. The idempotency key stays computed from paths
    only. Unpartitioned tables: today's `AddFiles` / `ReplaceDataFiles`, unchanged. Partitioned tables:
    - **before** `RetryOnConflict`, build each `iceberg.DataFile` once from the file's footer metrics (as
      `table.FileToDataFile` with the unpartitioned spec computes them) plus the explicit tuple, through
      `iceberg.NewDataFileBuilder` with the table's spec;
    - **inside** each retry attempt, reload the table, check the idempotency key as today, and check that the
      table's default spec id equals the files' spec id — otherwise fail with
      `partition spec changed during the run (spec id X → Y)` (FailedApplication, not retried);
    - APPEND → `Transaction.AddDataFiles`; FULL_LOAD → `ReplaceDataFilesWithDataFiles` with the current snapshot's
      **data** files as `DataFile` objects (delete files excluded; the path listing used today also returns delete
      files — fix for both paths).
    Inferring tuples from statistics is never used (`FileToDataFile` panics for `bucket`, `table/arrow_utils.go:1971`;
    truncated string bounds are unsafe).
12. **Reads unchanged.** `PlanFiles` handles partitioned tables; raw eligibility unchanged.

## Verification

- Unit (local catalog):
  - identity(string) + day(timestamp) table, rows spanning tuples within and across batches → one file per tuple
    until rotation; committed tuples match rows; a row-filter scan plans only matching files;
  - bucket[16] and truncate[4] on strings > 16 chars; bucket tuples checked against the **Iceberg spec test vectors**
    (`bucket` of int `34` → hash `2017239379`; of string `"iceberg"` → `1210000089`; then mod N), not only against
    iceberg-go itself;
  - type conversions: date32, date64, timestamp[ms], timestamp[ns], a timestamptz table column, int16 source;
  - null and `void` values committed **and scanned back**;
  - validation: `days`, `bucket[-1]`, year(ts)+day(ts), partition-name collision, unknown column, transform invalid
    for type, partitioned v3 table, inferred-schema path (error from the first chunk, HTTP 400);
  - existing-table spec wins (warning; table spec used; catalog-reassigned ids honoured);
  - writer: cap 2 with 3 interleaved tuples (eviction warning once; every row committed exactly once); writer-wide
    unique file names; Abort / Interrupt / plane Close with 3 open files leave no partial objects or open uploads;
    one file's upload error aborts the others; rotation of a partitioned writer happens at the right byte count;
  - commit: FULL_LOAD replace on a partitioned table; idempotency hit; 409 retry; spec-id-changed error;
    unpartitioned table still uses `AddFiles` (spy).
- Integration (Lakekeeper + silo): server end-to-end write to a new partitioned table, `Commit`, iceberg-go row-filter
  scan returns only matching rows and plans fewer files than the unfiltered scan; APPEND twice; gateway read returns
  all rows; a null-partition row round-trips.
- Perf (Task 9): unpartitioned baseline vs identity-partitioned low cardinality (≤ 8 tuples) vs high cardinality
  (> cap, e.g. 200 tuples); report wall time, peak RSS, file count and commit time.

## Risks

- Memory: encoder buffers bounded by `PartitionBufferBytes` (rev 4); upload part buffers × up to 16 open files remain
  outside the budget — re-measured in Task 9.
- Scalar per-row transform evaluation; CPU measured in Task 9.
- High cardinality per batch yields many small files and a footer read per file at commit.
