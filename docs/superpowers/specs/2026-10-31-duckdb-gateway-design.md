# DuckDB data plane for the Data Gateway (spike candidate A) — design

**Status:** design, 2026-10-31. Implements the GO verdict of
`docs/superpowers/specs/2026-10-31-duckdb-sidecar-spike-report.md` (candidate A: an embedded DuckDB 2.0 data plane
behind the unchanged gRPC + HTTP component contract). Spike plan: `docs/superpowers/plans/2026-10-31-duckdb-sidecar-spike.md`.

## Intent

Replace the Data Gateway sidecar's hand-written data plane (Arrow→Parquet buffering, S3/GCS upload and download
backends, partition router, read-side file walking) with an embedded DuckDB 2.0 engine. Components, the SDK, the
proto, the operator contract and the e2e suite stay unchanged. The spike measured the extraction path at 0.83× wall and
0.59× summed RSS of today's gateway and a streamed read at well below today's memory.

## Goals

1. Writes: every Arrow record a component sends is turned into Parquet by DuckDB (`COPY … TO '<object>' (FORMAT parquet)`)
   and uploaded by DuckDB's httpfs directly to the table's data prefix with Lakekeeper-vended credentials.
2. Reads: `OpenReader` streams the table through DuckDB's Iceberg extension (catalog ATTACH, `READ_ONLY`) and the
   streaming C API, so memory is bounded and merge-on-read delete files are applied.
3. The contract is unchanged: same RPCs, same HTTP endpoints and headers, same formats on the wire, same ports and flags.
4. Code that DuckDB makes redundant is deleted in the same branch: `pkg/datagateway/{buffer,backend,partition}`, the
   files.json breadcrumb, the external-files/parquet-patcher path, the StorageBootstrap builder, the read-side manifest
   walker, and the `datuplet gateway` subcommand of the pure-Go CLI.
5. Extraction speed and memory are no worse than today, measured: on the spike's L dataset (2 M rows, unmodified
   http-json-extractor), median wall ≤ 1.10 × baseline and summed peak RSS (sidecar + extractor) ≤ 1.25 × baseline.

## Non-goals

- Changing any component (sql-transform keeps reading `FORMAT_PARQUET` over HTTP; a faster sql-transform-in-sidecar
  is a separate decision, see the spike report).
- Iceberg partition specs, read-time transforms/projection, working incremental reads, GetSchema/GetSample, exposing
  Prometheus metrics, loopback authentication between component and sidecar, run-pod NetworkPolicy.
- Any migration: nothing is deployed (greenfield).

## Anti-goals (failure even if it "works")

- Component-supplied text ever reaching DuckDB unquoted (column names are component-controlled).
- Vended credentials, the run JWT, or a `CREATE SECRET` statement appearing in a log line or error text.
- The root Go module (`go build/vet/test ./...`, pipeline-api, operator, observer, CLI) gaining a CGO/DuckDB dependency.
- Losing today's commit guarantees: one commit (one snapshot) per writer — the commit pool commits each writer
  separately, so two writers to one table in a run make two snapshots and a second FULL_LOAD replaces the first; this
  pre-existing behaviour is kept as is, not fixed here — single-snapshot FULL_LOAD replace per writer, the
  `datuplet.commit-key` idempotency key, the `datuplet.actor`/`run-id`/`run-mode`/`pipeline-api` snapshot audit
  properties (asserted by `tests/e2e/scenarios_audit_test.go` and read by the storage UI), 409 retry.

## Key decision: DuckDB writes Parquet, Go keeps the Iceberg commit (controller's call)

The spike's candidate A committed through DuckDB-Iceberg (`INSERT INTO lk.ns.t`). That path cannot set snapshot summary
properties (audit trail + idempotency key), has no single-snapshot overwrite (FULL_LOAD becomes DELETE+INSERT with
position deletes and compaction debt), and its COMMIT-after-failed-statement silently rolls back. The existing Go commit
path (`commit_pool.go` → `pkg/icebergjob.CommitTableFiles` → iceberg-go `AddFiles` / `ReplaceDataFiles`) has none of
these gaps and is covered by e2e. So:

- **DuckDB** produces and uploads the Parquet data files (the part the spike measured as the memory and speed win) and
  serves reads.
- **Go** keeps table create/load (`pkg/datagateway/lakekeeper.Resolver.LoadOrCreateForWrite`), vended credentials
  (`pkg/catalogwriter.VendedCreds`), and the commit pool + `icebergjob.CommitTableFiles` unchanged.

Consequence: about 7 k LOC are deleted instead of the spike's 10–11 k estimate (the commit code stays by choice). The
alternative (DuckDB-Iceberg commits) remains possible later behind the same `DataPlane` interface.

## Architecture

```
component ──gRPC :50051 / HTTP :50052──► pkg/datagateway.ServerV2 (root module, pure Go)
                                          │  format adapters (Arrow IPC, JSONL, CSV, JSON, Parquet) → arrow.RecordBatch
                                          │  drop processors (Go, zero-copy column projection)
                                          │  table create/load + vended creds (Resolver, catalogwriter)
                                          │  commit pool → icebergjob.CommitTableFiles (iceberg-go)
                                          ▼
                                   DataPlane interface (root: pkg/datagateway/dataplane.go)
                                          ▲
                         gateway/duckplane (new CGO module `gateway/`, DuckDB 2.0 via duckdb-go-bindings)
                           writer: Arrow channel → registered Arrow view → COPY … TO 's3://…/part-….parquet'
                           reader: ATTACH Lakekeeper READ_ONLY → streaming SELECT → arrow.RecordBatch
```

### Module layout

- New Go module `gateway/` (`module github.com/datuplet/datuplet/gateway`, `replace github.com/datuplet/datuplet => ..`),
  mirroring `components/queryengine`. Packages:
  - `gateway/duck` — a thin layer over `github.com/duckdb/duckdb-go-bindings` v0.20000.0 (already in the module graph):
    open a database with config, connections, exec, **streaming** query to `array.RecordReader`
    (`PendingPreparedStreaming` + `StreamFetchChunk`/`FetchChunk` + `DataChunkToArrowArray`), register an
    `array.RecordReader` as a view (`ArrowScan` over a C-allocated `ArrowArrayStream`, as duckdb-go's `RegisterView`
    does), `Interrupt`, and SQL quoting helpers. No `database/sql`, no `unsafe` access into duckdb-go internals. The
    spike's only working streaming read went through duckdb-go internals; owning the bindings layer is the supported
    route (spike report, Read memory optimization).
  - `gateway/duckplane` — the `DataPlane` implementation.
  - `gateway/cmd/datagateway` — the sidecar binary.
- Root `pkg/datagateway` keeps the server, control plane, commit pool and adds the `DataPlane` interface plus an
  in-memory fake for tests, so `go test ./...` on the root stays CGO-free and covers the server logic.

### DataPlane interface (root)

```go
type DataPlane interface {
    OpenTableWriter(ctx context.Context, spec WriterSpec) (TableWriter, error)
    OpenTableReader(ctx context.Context, spec ReaderSpec) (array.RecordReader-like stream, error)
    Interrupt()          // cancel: abort every in-flight statement
    Close() error
}
```

The plane is built inside `NewServerV2` by a `DataPlaneFactory` after the run token has been validated, because the
reader's catalog ATTACH needs the validated `project_id`/`warehouse` claims and the raw token. Writers and readers own
their lifetime context (derived from the plane, cancelled by `Abort`/`Close`/`Interrupt`); the `ctx` passed to `Write`,
`Next` or `Open…` only bounds that call, never the background COPY or the streaming result (today's code detaches
the read stream from the OpenReader RPC context for the same reason, `server_v2_reading.go:96-105`).

Exact signatures are fixed in the plan (Task 1). `WriterSpec` carries: the opaque data prefix (`<table-location>/data/`),
the file-name prefix (`part-<run8>-<writerID>`), the post-drop Arrow schema, a credentials callback returning the
current vended `catalogwriter.Creds` (S3 or GCS), target file size and row-group size. `TableWriter.Write(rec)` blocks
for backpressure; `Close()` returns the written files with path, row count and byte size; `Abort()` discards.
`ReaderSpec` carries namespace and table.

### Write path

1. Unchanged up to the first chunk: bucket/table resolution, `LoadOrCreateForWrite` (deferred create when no schema),
   write mode from config, format adapter parses the body into Arrow records (first chunk fixes the schema; later
   chunks must match, as today).
2. Drop processors run in Go on each record. **Fix:** every table create — deferred (inferred schema) or eager
   (component-supplied `OpenWriterRequest.schema`) — uses the schema *after* the drop processors (today both used the
   pre-drop schema). A supplied schema no longer suppresses the data (today it silently dropped all rows).
3. Every new table is created with **all fields optional** (DuckDB writes OPTIONAL Parquet columns; iceberg-go rejects
   optional columns against a required field). Greenfield, so no existing required-field tables exist.
4. The writer feeds records into a bounded channel (capacity 4) read by a registered Arrow view; one
   `COPY (SELECT <canonical casts> FROM <view>) TO '<prefix>part-<run8>-<wID>-NNNNN.parquet' (FORMAT parquet,
   COMPRESSION zstd, ROW_GROUP_SIZE <rows>, RETURN_STATS true)` per output file. The writer rotates to a new file
   (ends the view, collects the stats row, starts the next COPY lazily) when the uncompressed Arrow bytes fed reach
   the rotation threshold (default 4 × target file size, i.e. 512 MiB for the 128 MiB default) — so each COPY is short,
   names are under our control, and credentials are re-read per file.
5. Before each COPY the writer `CREATE OR REPLACE SECRET`s a per-writer secret scoped to the table's data prefix from
   the current vended credentials (S3: key id, secret, session token, region, endpoint host:port; GCS:
   `TYPE gcs, BEARER_TOKEN`). The endpoint is parsed like today's MinIO backend (`backend/minio.go` `splitEndpoint`):
   a bare `host[:port]` means HTTPS, an explicit `http://`/`https://` scheme decides `USE_SSL`, any other scheme is an
   error. `URL_STYLE` is decided independently of TLS: `vhost` only when the parsed hostname is `amazonaws.com` or ends
   in `.amazonaws.com`, otherwise `path`. **Credential lifetime:** `VendedCreds.Get` may return cached credentials
   close to expiry when a renewal failed, so before each COPY the writer requires a remaining validity of at least
   `max(2 min, 25 % of the credentials' issued TTL)`; if the credentials returned have less, it calls `Invalidate()`
   and `Get()` once more, and if they still have less it fails the write with a clear error instead of starting a
   COPY that could outlive them. A single file must finish within that remaining validity. Secret statements are
   never logged and errors from them are replaced by a fixed message.
6. Types: the set of Arrow input types the new path accepts, and the Parquet column types it writes for them, equal
   today's end-to-end behaviour, recorded before `buffer/` is deleted in a golden file (per input type: the Parquet
   physical/logical/converted type today's writer produces for a first chunk and for a second chunk of the same type,
   or today's rejection). Today's format adapters and `BufferManager` reject records whose Arrow types differ from the
   canonical gateway schema (`buffer/manager.go:332-336`, `buffer/coalesce.go:45-54`), so some input types (for
   example int8) may be rejected today; the new path rejects exactly those with the same error class and casts the
   accepted ones (`SELECT` list of `CAST(<Ident(col)> AS <type>)`) so the Parquet column types match the golden.
   Repetition is the one deliberate difference: every column is OPTIONAL (item 3).
   **Maintainer ruling 2026-11-01** — two accepted timestamp deviations:
   - `timestamp[s|ms, tz=UTC]` is written as UTC **microseconds** (`Timestamp(isAdjustedToUTC=true, MICROS)`,
     `TIMESTAMP_MICROS`) instead of the golden's milliseconds: DuckDB 2.0 has no `TIMESTAMPTZ_MS`/`TIMESTAMPTZ_S`.
     The instants are identical and iceberg-go `AddFiles` accepts the files. Pinned by `knownDeviations` in
     `gateway/duckplane/golden_test.go`.
   - Naive timestamps (`s`/`ms`/`us`, no time zone) and every nanosecond timestamp fail iceberg-go `AddFiles` at
     commit: the table column is `timestamptz` (all timestamps map to it) while the Parquet column is not UTC-adjusted,
     and this iceberg-go rejects nanosecond precision. The Parquet types equal the golden, so today's writer fails the
     same way; not fixed here. Pinned by `TestIntegrationAddFilesPerType` (`addFilesRejects`).
7. `CloseWriter` closes the writer, returns `total_rows`/`total_bytes`/`files_written` from the stats rows, and
   dispatches the paths to the unchanged commit pool. Zero files: no dispatch (today's SKIPPED semantics; FULL_LOAD
   with zero rows still does not truncate).
8. `Commit`, idempotency key, audit properties, 409 retry, per-table results: unchanged code.
9. Removed write features (decided, documented in `docs/known-limitations.md`): `external_files` on CloseWriter returns
   an error ("external data files are not supported"); `partition_fields` are ignored with one warning per writer
   (they never reached Iceberg: no partition spec was ever set, files were only placed in Hive-style directories);
   the files.json breadcrumb and the `_schema-*.json` / `_manifest-*.json` sidecar files (no readers) are no longer
   written.

### Read path

1. `OpenReader` resolves bucket/table as today. The first reader in a sidecar ATTACHes Lakekeeper once:
   `CREATE SECRET (TYPE ICEBERG, TOKEN '<run jwt>')`, `ATTACH '<project-id>/<warehouse>' AS lk (TYPE ICEBERG,
   URI '<lakekeeper_url>', SECRET …, ACCESS_DELEGATION_MODE 'vended_credentials', READ_ONLY)`, then `DROP SECRET`
   (the queryengine pattern, `components/queryengine/attach.go`). Extractor sidecars never attach.
2. The reader runs `SELECT * FROM lk."<ns>"."<table>"` on its own connection through the streaming API with
   `enable_external_file_cache=false` and `read_ahead_depth=1` (spike: 143 MiB for a 2 M-row full scan vs 1 085 MiB
   materialized).
3. Output formats per HTTP/gRPC chunk: `FORMAT_ARROW_IPC` — one IPC stream of the next batches; `FORMAT_PARQUET` — one
   self-contained Parquet file per chunk written with arrow-go `pqarrow` (ZSTD) from the next batches (sql-transform's
   only path; it stages each chunk as a file); `FORMAT_CSV`/`JSON`/`JSONL` — the existing `format` adapters' Serialize.
   Chunk boundary: `chunk_size` from config (default 32 MiB) of uncompressed Arrow bytes. Headers `X-Datuplet-Is-Last`
   and `X-Datuplet-Rows` keep today's meaning. An empty table returns a single last chunk with zero rows (today:
   OpenReader errors); sql-transform already handles zero chunks/rows.
4. Incremental read specs keep today's behaviour (logged, full snapshot, `DeltaInfo` nil). Read transforms stay
   ignored. GetSchema/GetSample return `codes.Unimplemented` (today they always errored in K8s).

### Engine settings and lockdown

One in-memory DuckDB database per sidecar. At open: `threads` (config, default 1 — the spike's best extraction
setting), `memory_limit` (config; default 50 % of the cgroup memory limit read from `/sys/fs/cgroup/memory.max`, else
512 MiB), `temp_directory` (default `/scratch`), `max_temp_directory_size` (default 1 GiB),
`preserve_insertion_order=false`, `enable_external_file_cache=false`, `read_ahead_depth=1`,
`extension_directory`/`secret_directory`/`home_directory` set explicitly (no reliance on `$HOME`), `LOAD httpfs` and
`LOAD iceberg` from the baked extension directory before the lock (the binary always loads both; unit tests can open a
plane with no extensions for local-file writes). Then `autoinstall_known_extensions=false`,
`autoload_known_extensions=false`, `allow_community_extensions=false`, `allow_unredacted_secrets=false`,
`lock_configuration=true` last. `LocalFileSystem` stays enabled (spill). All SQL is generated by the sidecar; every
identifier goes through one quoting helper and every literal through one escaping helper.

### Packaging and deployment

- Image `datuplet/gateway` (same name and tag scheme, so chart/operator/e2e image lists do not change) rebuilt from
  `utils/docker/Dockerfile.gateway`: `golang:1.25-bookworm` builder, `CGO_ENABLED=1 go build -tags duckdb_arrow` of
  `gateway/cmd/datagateway`, extensions baked by `utils/docker/install-duckdb-ext`, runtime `debian:bookworm-slim` +
  `libstdc++6`, user uid/gid **65532** (the uid the operator forces), offline `--network=none` verify stage, `/scratch`.
- The binary accepts today's operator args (`--minio --config … --addr … --run-token-path … --pod-annotations-path …`);
  `--minio` is accepted and ignored. The `datuplet gateway` subcommand and its local/direct-MinIO dev modes are removed
  from the CLI.
- Operator: the `gateway` sidecar gets an `emptyDir` at `/scratch` (sizeLimit 2 Gi) and explicit resources (requests
  cpu 100m / memory 256 Mi, limit memory 1 Gi) instead of silently inheriting the namespace LimitRange's 512 Mi.
- CI: the module joins the `duckdb-modules` job in `.github/workflows/pr.yml`, `DUCK_MODS` in
  `scripts/verify-versions.sh`, and the DuckDB bump rule in `docs/dependency-upgrades.md`. `make tidy` discovers it.
  Its integration test (`//go:build integration`, Docker: Postgres + Lakekeeper v0.12.1 + silo, copied from
  `components/queryengine/integration_test.go`) runs locally, like the queryengine one.

## Verification

- Unit: root server tests against the fake DataPlane (bare machine); `gateway/duck` and `gateway/duckplane` unit tests
  against local files (`-tags duckdb_arrow`, CGO).
- Integration (local, Docker): write → `icebergjob.CommitTableFiles` → read back for APPEND, FULL_LOAD, multi-writer,
  type parity, empty table, credential refresh between files.
- Live: `make e2e-k8s` on OrbStack, full suite green (authn + OpenFGA + run tokens on).
- Performance: the spike harness (`spikes/duckdb-sidecar/` in the main checkout, uncommitted) re-run with the new binary
  vs the `ecff5c5` gateway, interleaved, L dataset, 3 runs each; pass bar in Goals 5. sql-transform's read of the L
  table timed old vs new and reported (no pass bar; a slowdown is reported, not hidden).

## Risks and what is not verified by this branch

- **GCS** (`gs://` warehouses): implemented through DuckDB's `gcs` secret with `BEARER_TOKEN` and the Iceberg
  extension's vended GCS credentials, but there is no GCS environment in CI or locally (Lakekeeper hard-codes
  storage.googleapis.com). Must be proven on GKE before a release. Documented as a known limitation.
- **Credential lifetime**: one Parquet file must finish within the remaining validity checked before its COPY
  (Write path 5; ≥ 3.75 min at the 15 min default TTL). Lakekeeper STS credentials stay valid until expiry, so a
  test cannot prove DuckDB dropped the old key; the tests prove a fresh secret is installed per file. Reads run under
  the DuckDB extension's own vended credentials; a single read longer than the TTL is unverified.
- **Extensions in CI**: PR CI runners may not reach extensions.duckdb.org, so the module's unit tests use no loadable
  extension (local files only); httpfs/iceberg paths are covered by the local integration tests, the image's offline
  verify stage, and e2e.
- **Run JWT** is a static 24 h bearer for the read ATTACH (as today for the Go path).
- **Image size** grows from a small static Go image to roughly 300 MB (like query-worker).

## Left to the builder

File-level structure inside the new module, test fixture details, log wording, the exact rotation bookkeeping, and
anything the plan does not pin.
