# All-Go Data Gateway data plane — design

**Status:** design, 2026-11-01. Maintainer decision (2026-11-01): build an all-Go gateway on a fresh branch from
`main` (`ecff5c5`) instead of merging the DuckDB data plane (draft PR #46, `feat/duckdb-sidecar`, left open as is).
Evidence: `docs/superpowers/specs/2026-11-01-go-write-plane-spike-report.md` (Go write plane matches the DuckDB plane
on wall time and, with a Go memory limit, on memory) and the DuckDB branch's read measurements (DuckDB decode +
re-encode of every row made a full `FORMAT_PARQUET` read ~7× slower than `main`'s file passthrough).

## Intent

Replace the gateway's hand-written buffering/upload pipeline and its Iceberg-unaware read path with a small Go data
plane built on arrow-go `pqarrow` and iceberg-go v0.7.0:

- **Writes** as fast as the DuckDB plane (spike: 15.4 s vs DuckDB 15.9 s vs `main` 18.9 s on 2 M rows) with no CGO.
- **Reads** as fast as `main` for clean tables (byte passthrough of data files) and **correct by Iceberg semantics**
  when they are not (delete files applied, columns mapped by field id / name mapping, types normalized to the table
  schema) — decided per data file.
- **iceberg-go upgraded** from the pinned pseudo-version `v0.5.1-0.20260429200022-af2ddd4adf06` to `v0.7.0`.

Components, the SDK, the proto, the operator contract, the gateway image and the e2e suite stay unchanged.

## Goals

1. iceberg-go `v0.7.0` in every module that uses it (root, `components/queryengine`, `tests/e2e`, and anything
   `make tidy` pulls along); arrow-go/gocloud follow MVS. **Go 1.27.3** (latest stable, maintainer 2026-11-01:
   "update go to the latest version … or the latest that other libraries support"): the `go` directive of every
   module and the `golang:1.25-*` builder images in `utils/docker/` move to 1.27.3 / `golang:1.27-*`; CI reads
   `go.mod`. If any module or tool fails to build or test on 1.27, fall back to 1.26.10 for all and record why.
2. Writes: Arrow records → arrow-go `pqarrow` Parquet (ZSTD) → streamed multipart upload through iceberg-go IO to
   the table's data prefix with Lakekeeper-vended credentials; commit unchanged (commit pool → `icebergjob`).
3. Reads: `OpenReader` plans the table's current snapshot with iceberg-go (`Scan.PlanFiles`); each data file is
   either served **raw** (its bytes are a valid `FORMAT_PARQUET` chunk) or **decoded** by iceberg-go
   (`Scan.ReadTasks`: deletes applied, schema-mapped) and re-encoded per chunk.
4. Code made redundant is deleted: `pkg/datagateway/{buffer,backend,partition}`, the files.json breadcrumb, the
   external-files/parquet-patcher path, the StorageBootstrap builder, the read-side manifest walker
   (`lakekeeper.Resolver.LoadTableForRead`), `datuplet gateway`'s local/direct-MinIO dev modes.
5. Measured on the spike harness (L dataset, 2 M rows, unmodified http-json-extractor, interleaved vs `main`
   `ecff5c5`, warm-up + 3 runs): write median wall ≤ 0.95 × main and median summed peak RSS ≤ 1.00 × main; full
   `FORMAT_PARQUET` HTTP read of a clean (raw-eligible) table median wall ≤ 1.25 × main. A decoded read (table with
   delete files) is measured and reported without a bar.

## Non-goals

- Changing components, the SDK, the proto, the gateway image or its entrypoint (`datuplet gateway` stays); the only
  image change is the Go builder base version (Goals 1).
- Iceberg partition specs (superseded 2026-11-02: see `2026-11-02-gateway-iceberg-partitioning-design.md`), read-time projection/filters/transforms, working incremental reads, GetSchema/GetSample,
  run-pod NetworkPolicy, loopback auth, sidecar resource requests/limits in the operator.
- Any migration (greenfield; nothing deployed).

## Anti-goals (failure even if it "works")

- A read that serves a data file raw when Iceberg semantics would change its rows, columns or types (a missed
  delete file, a renamed/added/dropped column, a type the table schema widens).
- Raw and decoded chunks of one read disagreeing on column names or Arrow types.
- Vended credentials or the run JWT in a log line or error text.
- Losing today's commit guarantees: one commit (one snapshot) per writer, single-snapshot FULL_LOAD replace per
  writer, the `datuplet.commit-key` idempotency key, the `datuplet.actor`/`run-id`/`run-mode`/`pipeline-api` audit
  properties, 409 retry.
- A CGO dependency in the root module.

## Key decision: port the plane-agnostic server code from the DuckDB branch (controller's call)

The DuckDB branch rebuilt the server around a `DataPlane` interface entirely inside the pure-Go root module and had
it reviewed task by task (Codex) and proven by the full e2e suite. That code is plane-agnostic: the server, the
fake plane for tests, the config loader, the Parquet type golden, the post-drop table-create fix, the record-type
acceptance check, the read chunking and serializers. This branch **ports those files** (checked out from the DuckDB
branch commits named in the plan, then adapted) instead of re-deriving them, and replaces only the plane: no
`gateway/` CGO module, no DuckDB, no new binary or image. Nothing from the DuckDB branch is merged; PR #46 is left
untouched. Cost if wrong: the ported code carries the DuckDB branch's reviewed decisions (listed below as
"inherited"), which the maintainer already ruled on.

## Architecture

```
component ──gRPC :50051 / HTTP :50052──► pkg/datagateway.ServerV2
                                          │  format adapters → arrow.RecordBatch, drop processors,
                                          │  table create/load + vended creds (lakekeeper.Resolver, catalogwriter),
                                          │  commit pool → icebergjob.CommitTableFiles (iceberg-go AddFiles/ReplaceDataFiles)
                                          ▼
                                   DataPlane interface (pkg/datagateway/dataplane.go)
                                          ▲
                         pkg/datagateway/goplane (pure Go)
                           writer: channel(4) → pqarrow FileWriter (ZSTD) → iceberg-go IO Create (multipart upload)
                           reader: iceberg-go LoadTable + Scan.PlanFiles → per file: raw bytes | Scan.ReadTasks
cmd/datuplet gateway  ──  NewServerV2(cfg, goplane.Factory)
```

`goplane` imports `datagateway` (interface types); `datagateway` never imports `goplane`; `cmd/datuplet/gateway.go`
wires them.

## Write path

Inherited from the DuckDB branch (ported, unchanged in behaviour):
1. Bucket/table resolution, `LoadOrCreateForWrite`, write mode, format adapters as today. The drop pipeline is built
   before any table create; every create (deferred/inferred and eager/supplied schema) uses the **post-drop** schema;
   a supplied schema no longer suppresses data. New tables are created with all fields optional.
2. Record-type acceptance moves from the deleted `BufferManager` into the server at the same pipeline position
   (maintainer ruling R7 on the DuckDB branch): post-drop record checked against the writer's output schema on the
   first chunk before the plane writer opens; `large_string`/`large_binary` rejected for every record.
3. `CloseWriter` → `TableWriter.Close` → totals from `WrittenFile`s → commit pool (zero files → no dispatch;
   FULL_LOAD with zero rows still does not truncate). `external_files` → error `external data files are not
   supported`; `partition_fields` → Iceberg partition spec (addendum `2026-11-02-gateway-iceberg-partitioning-design.md`); no files.json, `_schema-*.json`,
   `_manifest-*.json`.

New (the Go writer, `goplane`):
4. Per writer: a channel of capacity 4 batches feeds one goroutine that encodes with
   `pqarrow.NewFileWriter(schema, sink, props, arrowProps)`; `props`: `parquet.WithCompression(Zstd)`,
   `WithMaxRowGroupLength(RowGroupRows)` (default 229376), `WithStats(true)`, dictionary as `main`'s writer
   (`WithDictionaryDefault(true)`); `arrowProps`: `WithStoreSchema()`. Records are written with `WriteBuffered`.
   The Arrow schema written is the writer's schema with `PARQUET:field_id` metadata stripped (iceberg-go `AddFiles`
   rejects Parquet carrying field ids; tables have a name mapping — `main`'s `stripFieldIDMetadata`).
5. Rotation: a file ends when the uncompressed Arrow bytes fed into it reach 4 × `TargetFileBytes` (default
   128 MiB → 512 MiB). File names `<DataPrefix><FilePrefix>-%05d.parquet` (`FilePrefix` = `part-<run8>-<writerID>`).
6. Upload: per file, `iceberg-go/io.LoadFS(ctx, props, path)` then `WriteFileIO.Create(path)` (gocloud blob writer,
   streaming multipart). `props` from the current vended credentials: S3 → `s3.access-key-id`,
   `s3.secret-access-key`, `s3.session-token`, `s3.region` (default `us-east-1` when empty), `s3.endpoint` (the
   vended endpoint verbatim, when set); path-style addressing unless the endpoint host is `amazonaws.com` or ends in
   `.amazonaws.com` (then `s3.force-virtual-addressing=true`). GCS → `gcs.oauth2.token` (+ project) consumed by the
   `pkg/datupleticeio` `gs://` factory (blank-imported by `goplane`). Closing the pqarrow writer closes the sink and
   completes the upload. **Failure / abort cleanup is explicit:** on any encode, upload or close error, on `Abort`
   and on lifetime-context cancellation, the writer (a) cancels the context the file's IO was loaded with **before**
   closing the sink — for gocloud blob writers this is what makes `Close` not complete the upload; because the AWS
   transfer manager then sends `AbortMultipartUpload` on that already-cancelled context (and vended credentials may
   not list uploads), an S3 client middleware re-issues the abort on an uncancelled context bounded to 30 s
   (Task 3 ruling R3a) — then (b) closes the sink itself (never relying on `pqarrow.FileWriter.Close` reaching the sink,
   which it skips on an earlier row-group error), and (c) for local `file://` prefixes removes the partial file.
   No partial object may exist afterwards and no multipart upload may stay open.
7. Credential lifetime (inherited rule): before each file the writer requires a remaining validity of at least
   `max(2 min, 25 % of Expires − Issued)`; otherwise `Invalidate()` + `Get()` once; if still too short the write fails
   with `goplane: vended credentials expire too soon`. One file must finish within that validity.
8. Types: `pqarrow` is the encoder `main` uses, so for every input type `main` accepts the Parquet column types equal
   the golden recorded from `main`'s writer (inherited golden generator, relocated to
   `pkg/datagateway/testdata/parquet-types.golden.json`) with **no deviations** (the DuckDB plane's two timestamp
   deviations do not apply). Repetition follows the Arrow field nullability, as `main`.
9. Memory: at `datuplet gateway` start, when `GOMEMLIMIT` is unset and a cgroup memory limit is readable
   (`/sys/fs/cgroup/memory.max`, v1 `/sys/fs/cgroup/memory/memory.limit_in_bytes`), the process calls
   `debug.SetMemoryLimit(70 % of the limit)` and logs it. No limit found → Go defaults. (Spike: default GC kept
   ~513 MiB RSS; a 200 MiB soft limit gave 317 MiB at +0.9 s CPU, same wall.)

## Read path

1. `OpenReader` resolves bucket/table as today and calls `plane.OpenTableReader`. The plane loads the table through
   the Lakekeeper catalog (`lakekeeper.Resolver`, built by `goplane.Factory` from `BootInfo`), pins the current
   snapshot and runs `Scan.PlanFiles` once (no filter, all columns).
2. **Split coalescing, then raw eligibility, per task.** iceberg-go v0.7.0 splits a Parquet data file larger than
   the table's `read.split.target-size` (default 128 MiB) into consecutive byte-range tasks of the same file
   (`table/scan_splits.go`). The plane first coalesces each run of consecutive tasks with the same data file whose
   ranges are contiguous and cover exactly `[0, FileSizeBytes)` back into one whole-file task. Then, per task — all
   must hold, else the task is decoded with its own range:
   - `ReaderSpec.RawParquet` is true (the request format is `FORMAT_PARQUET`; other formats decode everything);
   - the task covers the whole file: `Start == 0 && Length == File.FileSizeBytes()` (a partial task is never raw —
     serving the whole object for each split would duplicate rows);
   - the task has no `DeleteFiles`, `EqualityDeleteFiles` or `DeletionVectorFiles`;
   - the data file format is Parquet;
   - the file's Parquet footer (read through the table IO, ranged) converted with `pqarrow` yields an Arrow schema
     whose fields equal, in order, the table's current schema converted with iceberg-go `SchemaToArrowSchema`
     (no large types): same names and identical Arrow data types; nullability and field metadata ignored;
   - **field identity:** every file column resolves to the same Iceberg field id as the current-schema column it is
     compared with. A file carrying Parquet field ids uses them; a file without field ids is resolved through the
     table's name mapping (`Table.NameMapping()`, the way iceberg-go's reader applies it). A missing, incomplete or
     ambiguous mapping, or any id mismatch, makes the task non-raw.
3. A **raw part** is one read chunk: `X-Datuplet-Rows` = the manifest entry's record count, body = the file's bytes.
   HTTP streams the body straight from storage (no buffering; `Content-Length` = file size). gRPC `ReadChunk`
   buffers the file into the message (today's behaviour and today's gRPC size cap).
4. **Decoded parts**: consecutive non-raw tasks form one decoded part, read **one task per `Scan.ReadTasks` call**
   in planning order (deletes applied, columns projected to the current schema by field id / name mapping, types
   promoted, the task's byte range respected) — `ReadTasks` obtains one IO per call, so per-task calls are what let
   credentials renew between files (Read path 6). Batches are chunked by `chunk_size`
   (default 32 MiB uncompressed) and serialized as today's DuckDB-branch server does (inherited
   `read_serialize.go`): `FORMAT_PARQUET` → one self-contained `pqarrow` ZSTD file per chunk; `FORMAT_ARROW_IPC` →
   one IPC stream; CSV/JSON/JSONL → the `format` adapters.
5. Every raw and decoded `FORMAT_PARQUET` (and CSV/JSON/JSONL) chunk carries the table's current schema (names and
   Arrow types) — guaranteed by rule 2. `FORMAT_ARROW_IPC` chunks carry the SDK wire schema (Read path 8).
6. Credentials: every raw file open, every footer read and every (single-task) `ReadTasks` call uses IO built from
   the table's current vended credentials (`catalogwriter.VendedCreds.Get`, the renewal the writer uses), so a long
   read — raw or decoded — renews between files. One file must be read within its credentials' validity.
7. Inherited read behaviour: empty table (no snapshot or no files) → one last chunk with zero rows (`FORMAT_PARQUET`:
   a valid zero-row Parquet file with the table schema); headers `X-Datuplet-Is-Last`/`X-Datuplet-Rows` keep their
   meaning; incremental specs log today's warning and read the full snapshot; GetSchema/GetSample →
   `codes.Unimplemented`; the reader's lifetime is independent of the OpenReader RPC context.
8. **`FORMAT_ARROW_IPC` wire contract for the frozen SDK** (deviation from the ported serializer). The frozen
   `sdk/go/arrow` reader reads only the first batch of each chunk and rejects a chunk whose IPC schema differs from
   the schema it derives from `OpenReaderResponse.schema` (`protoSchemaToArrow`, `sdk/go/arrow/reader.go:132`:
   e.g. every `timestamp` → `timestamp[us, tz=UTC]`). So for this format:
   - each chunk is one IPC stream holding exactly **one** record batch (the chunk's batches concatenated);
   - the chunk's Arrow schema is the **SDK wire schema**: `OpenReaderResponse.schema` (the server's
     `arrowSchemaToProto(Schema())`, unknown types reported as `string`) mapped exactly as `protoSchemaToArrow`
     does. The server reproduces that mapping in one helper, pinned by a table test over every proto type string
     that cites the SDK source line, and casts every decoded column to its wire type (arrow-go `compute` cast; a
     naive timestamp is reinterpreted as UTC, `timestamp[ns]` truncates to µs, unknown types become strings);
   - a column the SDK cannot represent and that cannot be cast to its wire type fails `OpenReader` with
     `InvalidArgument` naming the column and type, before any chunk is served;
   - Parquet and CSV/JSON/JSONL chunks keep the table's own Arrow types (Read path 5).
   The DuckDB branch's SDK change (`sdk/go/arrow/reader.go`, commit `270014ad`) is **not** ported.

## Boot (fixes carried over from the DuckDB branch — maintainer, 2026-11-01: "bring fixes … if it makes sense")

The component's SDK makes a single, non-retried `GetConfig` call at start (`sdk/go/client.go` `New()`), so any window
in which the sidecar is not yet listening fails the component. The DuckDB branch fixed this with a listen-first boot
(commit `35c4f621`, reviewed by Codex, e2e-proven); it is plane-agnostic and is ported:

1. `datuplet gateway` binds the gRPC listener before boot and the HTTP listener as soon as the config is loaded; a
   `BootGate` (`pkg/datagateway/boot_gate.go`) holds every unary, streaming and HTTP request until boot ends.
2. A failed boot is served as the error (gRPC `Unavailable`, HTTP 503, token-free text via `SanitizeBootError`) until
   SIGTERM/SIGINT or a 10-minute bound, then the process exits 20 (exiting at once would make the kubelet restart the
   sidecar and race the component again).
3. SIGTERM/SIGINT are registered before boot; a signal caught during boot is handled when boot returns.
4. New relative to the DuckDB branch (open findings on that code): the gRPC health service reports `NOT_SERVING`
   until the gate is ready (it reported `SERVING` before boot), and the gate's waiters provably return on request
   cancellation (direct tests of the wait path, not only client deadlines).

Everything else carried over from the DuckDB branch is listed in "Key decision" (server code, fixes from its review
rounds) and in Read path 8 (the SDK change that is *not* carried over).

## Verification

- Unit (bare machine, `go test ./...`): server against the fake plane (ported tests + raw/decoded part tests);
  `goplane` writer and reader against local `file://` tables built with iceberg-go in tests.
- Integration (`//go:build integration`, Docker: Postgres + Lakekeeper v0.12.1 + silo, container prefix `gw-it-`):
  write → `icebergjob.CommitTableFiles` APPEND/FULL_LOAD → read back; raw eligibility on a clean table; decoded
  reads after an iceberg-go position delete, an added column, a renamed column, and a non-canonical (int8) write;
  credential `Invalidate()` between files (raw and within one decoded run); an aborted S3 write leaves no object and
  no open multipart upload. **Delete fixtures:** iceberg-go v0.7.0 defaults `write.delete.mode` to copy-on-write
  (`table/properties.go:102`), which rewrites files instead of writing delete files — every delete fixture sets
  `write.delete.mode=merge-on-read` on a format-v2 table, deletes a strict subset of one file's rows, reloads the
  table and asserts the planned task carries delete files before asserting read output. Equality deletes and v3
  deletion vectors get their own fixtures where iceberg-go can write them, otherwise synthetic `FileScanTask`s in
  unit tests. Runs in PR CI's integration line.
- Live: `make e2e-k8s` on OrbStack, full suite green.
- Performance: Goals 5, on the spike harness, report committed.

## Risks and what is not verified

- **GCS**: implemented through `pkg/datupleticeio`'s `gs://` factory with vended OAuth tokens; no GCS environment
  locally or in CI. Must be proven on GKE before a release (known limitation).
- **iceberg-go v0.7.0** is a minor-version jump across `icebergjob`, `catalogwriter`, `datupleticeio`, the
  lakekeeper resolver and `components/queryengine`; API breaks are fixed in Task 1 and covered by unit, integration
  and e2e tests.
- **Raw-eligibility false negatives** cost speed, not correctness (the file is decoded). Tables written by `main`'s
  writer with non-canonical types (int8, uint*, ms/ns timestamps) are decoded on read.
- **Gateway CPU** on writes is ~1.5× the DuckDB plane's (spike); invisible while the component is the bottleneck.

## Left to the builder

File layout inside `goplane`, test fixture details, log wording, how consecutive decoded tasks are grouped, and
anything the plan does not pin.
