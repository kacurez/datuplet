# DuckDB Data Plane for the Data Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per repo CLAUDE.md, every task review and the final whole-branch gate are run by **Codex** (codex-companion), and Critical/Important findings stop for the maintainer.

**Goal:** Replace the Data Gateway sidecar's Go data plane (Parquet buffering, S3/GCS backends, partition router, read-side file walking) with an embedded DuckDB 2.0 engine behind the unchanged gRPC + HTTP component contract, keeping the Go Iceberg commit path.

**Architecture:** Root `pkg/datagateway` (pure Go) keeps the server, format adapters, drop processors, Lakekeeper resolver, vended credentials and the commit pool, and talks to a new `DataPlane` interface. A new CGO Go module `gateway/` implements it on `duckdb-go-bindings` (writer: Arrow channel → registered Arrow view → one `COPY … TO '<s3 object>' (FORMAT parquet)` per output file; reader: Lakekeeper ATTACH `READ_ONLY` + streaming SELECT) and ships the sidecar binary `gateway/cmd/datagateway`.

**Tech Stack:** Go 1.25.7, `github.com/duckdb/duckdb-go-bindings` v0.20000.0 (DuckDB 2.0.0), `github.com/duckdb/duckdb-go/v2` v2.20000.0 (pin parity only), arrow-go v18.6.0, iceberg-go `v0.5.1-0.20260429200022-af2ddd4adf06`, Docker/OrbStack for integration and e2e.

**Spec:** `docs/superpowers/specs/2026-10-31-duckdb-gateway-design.md` (read it; it is binding). Evidence: `docs/superpowers/specs/2026-10-31-duckdb-sidecar-spike-report.md`.

## Global Constraints

- **Branch** `feat/duckdb-sidecar` in worktree `/Users/tomaskacur/mydevel/datuplet/.claude/worktrees/duckdb-sidecar`. Work only there. One logical commit per task (fix rounds may add commits). **Never** `git push`, `git tag`, `gh pr create`, or touch `main`.
- `git add` only the exact paths the task touched — never `git add -A` / `git add .` (the main checkout has unrelated untracked spike files; this worktree must stay clean of them). Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Never** `rm -rf`. Delete tracked files with `git rm`; temp files under the session scratchpad only.
- **Never** bare `go mod tidy`; use `make tidy` (covers every module, discovers `gateway/` automatically).
- **Root module stays CGO/DuckDB-free:** `go build ./... && go vet ./... && go test -count=1 ./...` from the repo root must pass with `CGO_ENABLED=0`. Any import of `github.com/duckdb/*` outside `gateway/`, `components/queryengine`, `components/sql-transform`, `utils/docker/install-duckdb-ext` is a defect.
- **New module** `gateway/`: `module github.com/datuplet/datuplet/gateway`, `go 1.25.7`, `require github.com/duckdb/duckdb-go/v2 v2.20000.0` (kept for `scripts/verify-versions.sh` pin parity; may be blank-imported in a test) and `github.com/duckdb/duckdb-go-bindings v0.20000.0`, `replace github.com/datuplet/datuplet => ..`. Every CGO file carries `//go:build duckdb_arrow`; tests run with `CGO_ENABLED=1 go test -tags duckdb_arrow -count=1 ./...` from `gateway/`.
- **Contract frozen:** `api/proto/gateway/v2/gateway.proto`, generated code, `sdk/go`, `components/*`, `cmd/mock-datagateway`, `tests/e2e` are not modified (except where a task explicitly says so).
- **Secrets hygiene:** never log or wrap into an error any SQL statement containing a secret or token, vended credential values, or the run JWT. Errors from `CREATE SECRET` / ATTACH are replaced by fixed messages (pattern: `components/queryengine/attach.go:101-115`).
- **SQL hygiene:** every identifier through one quoting helper (`"` doubled, wrapped in `"`), every string literal through one escaping helper (`'` doubled, wrapped in `'`); no other string interpolation into SQL. Column names come from components and are hostile input.
- **Storage paths are opaque strings** (CLAUDE.md): never `path.Join`/`filepath.Join` on a URL; concatenate with the existing `joinStoragePath` pattern.
- **Exit codes / status protocol unchanged** (`0` / `1` / `>=20`, `DUPLET_STATUS_MESSAGE:`).
- Engine settings fixed by the spec: `threads` default 1; `memory_limit` default 50 % of `/sys/fs/cgroup/memory.max` else `512MiB`; `temp_directory` default `/scratch`; `max_temp_directory_size` default `1GiB`; `preserve_insertion_order=false`; `enable_external_file_cache=false`; `read_ahead_depth=1`; lockdown `autoinstall_known_extensions=false`, `autoload_known_extensions=false`, `allow_community_extensions=false`, `allow_unredacted_secrets=false`, `lock_configuration=true` last. Env overrides: `DATUPLET_GATEWAY_DUCKDB_THREADS`, `DATUPLET_GATEWAY_DUCKDB_MEMORY_LIMIT`, `DATUPLET_GATEWAY_DUCKDB_TEMP_DIR`, `DATUPLET_GATEWAY_DUCKDB_EXTENSION_DIR`.
- Writer constants: channel capacity 4 batches; rotation when fed uncompressed Arrow bytes ≥ 4 × `TargetFileBytes` (default target 128 MiB); `ROW_GROUP_SIZE` default 229376 rows; `COMPRESSION zstd`; file names `<DataPrefix>part-<run8>-<writerID>-%05d.parquet` (today's naming, `server_v2_http.go:199-204`).
- Integration fixtures copy `components/queryengine/integration_test.go` (Docker CLI, Postgres + `quay.io/lakekeeper/catalog:v0.12.1` + silo pinned digest, Lakekeeper authn off), with container names prefixed `dg-it-`; build tag `integration`; run locally, not in PR CI.

## Review Focus

1. **Hostile column names and table names** (quotes, `"`, `'`, `;`, newlines, unicode) reaching DuckDB via the canonical-cast SELECT list, drop lists or the reader's `lk."ns"."t"` — expected: written and read back verbatim, no SQL error, no injection. Test in Task 3 and Task 4.
2. **Credentials expiring between files** of a long write — expected: each COPY uses the creds current at its start; a renewal between files is picked up. Test in Task 3 (fake creds func rotating keys, integration test with a forced `Invalidate`).
3. **Failure mid-stream** (COPY error, cancel, component disconnect) — expected: `Write` returns the error promptly, no goroutine or connection leak, `Close` returns the error, nothing is dispatched to the commit pool, `Abort` is idempotent. Test in Task 3 and Task 5.
4. **Parquet that iceberg-go `AddFiles` rejects** (field ids present, required vs optional mismatch, type drift from today's writer) — expected: DuckDB output matches the golden from today's writer except nullability, and AddFiles accepts it. Test in Task 1 (golden) and Task 3 (integration commit).
5. **Empty and zero-row cases**: writer opened and closed with no rows (no files, SKIPPED), read of an empty table (one last chunk, zero rows, valid Parquet with the table schema for `FORMAT_PARQUET`). Test in Task 4 and Task 6.

---

### Task 1: DataPlane interface, fake, config loader, Parquet type golden

**Model:** sonnet · root module only.

**Files:**
- Create: `pkg/datagateway/dataplane.go`, `pkg/datagateway/fake_dataplane_test.go` (the fake lives **inside package `datagateway` as a `_test.go` file** — a separate `dataplanetest` package would import `datagateway` while `package datagateway` tests import it: an import cycle), `pkg/datagateway/gatewayconfig.go`, `pkg/datagateway/gatewayconfig_test.go`, `pkg/datagateway/buffer/golden_types_test.go`, `gateway/duckplane/testdata/parquet-types.golden.json` (generated).
- Modify: `cmd/datuplet/gateway.go` (use the moved loader; behaviour unchanged).

**Interfaces — Produces (exact, later tasks rely on them):**
```go
package datagateway
// BootInfo is what the plane needs from boot validation; built by NewServerV2 after validateRunTokenForBoot.
type BootInfo struct {
    RunID         string
    LakekeeperURL string // cfg.LakekeeperURL ("" in tests / no catalog)
    ProjectID     string // validated claim
    Warehouse     string // validated claim (bare name)
    Token         string // raw run JWT; never logged
}
type DataPlaneFactory func(ctx context.Context, boot BootInfo) (DataPlane, error)
// CredsSource is satisfied by *catalogwriter.VendedCreds (Get + Invalidate).
type CredsSource interface {
    Get(ctx context.Context) (catalogwriter.Creds, error)
    Invalidate()
}
type WriterSpec struct {
    DataPrefix      string        // opaque "<table-location>/data/" incl. trailing slash
    FilePrefix      string        // "part-<run8>-<writerID>"
    Schema          *arrow.Schema // post-drop schema; every Write must match it
    Creds           CredsSource   // nil only for local-path prefixes (tests)
    TargetFileBytes int64         // <=0 → 128 MiB
    RowGroupRows    int64         // <=0 → 229376
}
type WrittenFile struct { Path string; Rows int64; Bytes int64 }
type TableWriter interface {
    Write(ctx context.Context, rec arrow.RecordBatch) error // blocks for backpressure; does not take ownership (retains internally if kept)
    QueuedBytes() int64
    Close(ctx context.Context) ([]WrittenFile, error)       // flushes; idempotent second call returns same result
    Abort()                                                 // discard; idempotent; safe after Close
}
type ReaderSpec struct { Namespace, Table string }
type RecordStream interface {
    Schema() *arrow.Schema
    Next(ctx context.Context) (arrow.RecordBatch, error)    // io.EOF at end; caller Releases the batch
    Close() error
}
type DataPlane interface {
    OpenTableWriter(ctx context.Context, spec WriterSpec) (TableWriter, error)
    OpenTableReader(ctx context.Context, spec ReaderSpec) (RecordStream, error)
    Interrupt()   // abort every in-flight statement (cancel annotation)
    Close() error
}
func LoadConfigFile(configPath, runTokenPath, podAnnotationsPath string) (*Config, error) // the YAML→Config logic moved verbatim from cmd/datuplet/gateway.go (GatewayConfig types + conversion), minus the local/minio static-backend construction
```
**Context ownership (binding for every implementation):** `OpenTableWriter`/`OpenTableReader` derive the writer's/reader's lifetime context from the plane's own context, never from the `ctx` argument (an RPC/HTTP request context is cancelled when that request returns). The `ctx` of `Open…`, `Write`, `Next`, `Close` only bounds that call's blocking. The lifetime context is cancelled by `Abort`, reader `Close`, `Interrupt`, plane `Close`, or an internal failure.

`fakeDataPlane` (in `fake_dataplane_test.go`) implements `DataPlane` in memory: writers record every batch (retained) and return synthetic `WrittenFile`s (`<DataPrefix><FilePrefix>-00000.parquet`, rows/bytes summed); readers replay batches registered with `setTable(ns, table string, schema *arrow.Schema, batches ...arrow.RecordBatch)`; failure injection via `failWrites(err)` / `failOpenReader(err)`; `interrupted() bool`; `fakeFactory(p *fakeDataPlane) DataPlaneFactory` records the `BootInfo` it was called with.

- [ ] **Step 1: Write failing tests** — `gatewayconfig_test.go`: `TestLoadConfigFile_OperatorShape` loads a YAML identical in shape to `generateGatewayConfig` output (copy one from `pkg/k8s/controllers` test fixtures or render one) and asserts run_id, lakekeeper_url, pipeline_api_jwks_url, output_tables (name, bucket, write_mode, columns), default_bucket/default_write_mode, processors, secrets_dir, gateway sizes; `TestLoadConfigFile_Defaults` asserts chunk 32 MiB / buffer 64 MiB / target 128 MiB defaults. `fake_dataplane_test.go` self-tests: write → close → files; read replay; injected failures; a writer opened with an already-cancelled ctx still accepts later `Write`s (context ownership rule).
- [ ] **Step 2: Run** `go test ./pkg/datagateway/... -run 'LoadConfig|Fake' -v` → FAIL (undefined).
- [ ] **Step 3: Implement** the interface, fake and loader; switch `cmd/datuplet/gateway.go` to `datagateway.LoadConfigFile` (its local/minio dev modes keep working until Task 5 removes the subcommand).
- [ ] **Step 4: Golden generator** — `buffer/golden_types_test.go`, test `TestGoldenParquetTypes` (runs only with `-update-golden`; otherwise asserts the golden file exists and parses): for each Arrow input type in {int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, string, large_string, bool, timestamp[s|ms|us|ns] with and without tz "UTC", date32, date64, binary, large_binary}, send **two** Arrow IPC chunks of that type (3 rows each, one null, boundary values such as max uint64 and pre-1970 timestamps) through today's full write path — the `format` Arrow IPC adapter, schema inference, `BufferManager` + `StreamingParquetWriter` configured exactly as `server_v2_http.go:156-300` does (drive `processParsedRecord` with a local static backend if that is simplest) — then record per type: accepted or `{"rejected_at_chunk": 1|2, "error": "<text>"}`; for accepted types, per column the parquet physical type, logical type, converted type, repetition, field_id presence, and the read-back values (as strings) so value round-trips (timestamp units/tz, unsigned) are pinned too. Write JSON to `../../../gateway/duckplane/testdata/parquet-types.golden.json` (create dirs). Run once with `-update-golden`, then without.
- [ ] **Step 5: Run** `CGO_ENABLED=0 go build ./... && go vet ./... && go test -count=1 ./pkg/datagateway/... ./cmd/datuplet/...` → PASS.
- [ ] **Step 6: Commit** `feat(datagateway): DataPlane interface, fake, config loader, parquet type golden`.

### Task 2: `gateway/` module and the `duck` bindings layer

**Model:** opus.

**Files:**
- Create: `gateway/go.mod`, `gateway/go.sum` (via `make tidy`), `gateway/duck/{db.go,conn.go,stream.go,arrowview.go,quote.go,doc.go}` + tests `gateway/duck/{duck_test.go,quote_test.go}`, `gateway/duck/notag.go` (`//go:build !duckdb_arrow`, package doc only so `go vet` without the tag does not fail on an empty package).
- Modify: `.github/workflows/pr.yml` (job `duckdb-modules`: add a `gateway` test step and `gateway/go.sum` to `cache-dependency-path`), `scripts/verify-versions.sh` (`DUCK_MODS` += `gateway`), `docs/dependency-upgrades.md` (DuckDB bump rule names the new module).

**Interfaces — Produces:**
```go
package duck
type Settings struct { Threads int; MemoryLimit, TempDir, MaxTempDirSize, ExtensionDir, SecretDir, HomeDir string }
func Open(s Settings) (*DB, error)                 // in-memory DB, applies Settings + preserve_insertion_order=false, enable_external_file_cache=false, read_ahead_depth=1
func (*DB) Connect() (*Conn, error)
func (*DB) Lock(c *Conn) error                     // the five lockdown SETs, lock_configuration last; error leaves DB unusable
func (*DB) Close() error
func (*Conn) Exec(ctx context.Context, sql string) error            // ctx cancel → Interrupt
func (*Conn) ExecRedacted(ctx context.Context, sql, label string) error // error text = "duck: <label> failed" only
func (*Conn) QueryStream(ctx context.Context, sql string) (*Stream, error) // streaming result → Arrow; ctx bounds only the prepare/execute call
type Stream struct{ /* implements array.RecordReader */ }
func (*Stream) IsStreaming() bool                               // bindings.ResultIsStreaming on the underlying result
func (*Stream) NextCtx(ctx context.Context) (arrow.RecordBatch, error) // io.EOF at end; ctx cancel → Interrupt + error
func (*Stream) Release()                                        // early close safe at any point; frees result + chunks
func (*Conn) QueryRows(ctx context.Context, sql string) ([][]any, error)            // small results (stats rows), materialized
func (*Conn) RegisterArrowView(name string, r array.RecordReader) (release func(), err error)
func (*Conn) Interrupt()
func (*Conn) Close() error
func Ident(s string) string   // "..." with " doubled
func Literal(s string) string // '...' with ' doubled; rejects NUL with panic-free error variant LiteralChecked
```
Implementation notes (decisions, not code): use `duckdb-go-bindings` only (`OpenExt` + `Config` for settings that must be set at open, `Connect`, `Query`, `Prepare` + `PendingPreparedStreaming` + `ExecutePending`, `FetchChunk` (works on streaming results; treat a NULL chunk with empty error as end of stream — spike quirk), `DataChunkToArrowArray` with `ResultGetArrowOptions`, `ArrowScan` with a C-allocated `ArrowArrayStream` filled by `cdata.ExportRecordReader` (mirror `duckdb-go/v2@v2.20000.0/arrow.go:118-140`), `Interrupt`). Destroy every result, prepared statement and chunk. One `QueryStream` per connection at a time (document it). The `duck` tests use no loadable extension (parquet is statically linked), so they run on a fresh CI runner.

- [ ] **Step 1: Failing tests** (`-tags duckdb_arrow`): `TestStreamingQueryIsStreaming` — `SELECT range AS i, repeat('x', 100) AS pad FROM range(3_000_000)` through `QueryStream`: `IsStreaming()` is true, total rows exact, Go-side retained batches ≤ 2; `TestStreamingQueryRSSBounded` — the test re-executes its own binary as a child (`os.Args[0] -test.run=TestHelperStreamDrain` with an env flag) that drains the same query, and reads the child's peak RSS from `cmd.ProcessState.SysUsage().(*syscall.Rusage).Maxrss` (bytes on darwin, KiB on linux — normalise); run the helper twice — once draining via `QueryStream`, once via a materialized path (`PendingPrepared` + `ExecutePending`, test-only) — and assert streaming peak < 0.5 × materialized peak (record both values in the report); `TestStreamEarlyRelease` (Release after 2 batches, then the same connection runs another query fine); `TestStreamCancelMidFetch` (ctx cancelled between `NextCtx` calls → next call errors within 2 s, connection reusable after); `TestRegisterArrowViewCopyToParquet` — register a 3-batch reader, `COPY (SELECT * FROM v) TO '<tmp>/x.parquet' (FORMAT parquet, RETURN_STATS true)` via `QueryRows`, stats row count = 3×N, file exists; `TestInterruptCancelsQuery` — long query, ctx cancel returns within 2 s with an error; `TestLockBlocksSet` — after `Lock`, `SET threads=4` errors; `TestExecRedactedHidesSQL` — failing statement containing `SECRET-VALUE` → error text has no `SECRET-VALUE`; `quote_test.go`: `Ident`/`Literal` round-trip hostile strings (`a"b`, `x'y`, `;DROP`, newline, unicode) through `SELECT <Literal> AS <Ident>` and read back verbatim.
- [ ] **Step 2: Run** `cd gateway && CGO_ENABLED=1 go test -tags duckdb_arrow ./duck/ -v` → FAIL.
- [ ] **Step 3: Implement** `duck`. `make tidy`.
- [ ] **Step 4: Run** the same → PASS; `cd gateway && go vet ./...` (no tag) → PASS; repo root `CGO_ENABLED=0 go build ./... && go test -count=1 ./...` → PASS; `bash scripts/verify-versions.sh` → PASS.
- [ ] **Step 5: Commit** `feat(gateway): new CGO module with a thin duckdb-go-bindings layer`.

### Task 3: DuckDB table writer (`duckplane`)

**Model:** opus.

**Files:**
- Create: `gateway/duckplane/{plane.go,writer.go,casts.go,secrets.go,settings.go}`, tests `gateway/duckplane/{writer_test.go,casts_test.go,golden_test.go}`, `gateway/duckplane/integration_test.go` (`//go:build integration && duckdb_arrow`), `gateway/duckplane/fixture_test.go` (shared integration fixture).

**Interfaces:**
- Consumes: Task 1 `datagateway.{DataPlane,TableWriter,WriterSpec,WrittenFile,CredsSource}`; Task 2 `duck.*`; `catalogwriter.{Creds,S3Creds,GCSCreds}`.
- Produces:
```go
package duckplane
type Config struct {
    Settings   duck.Settings  // from SettingsFromEnv()
    Extensions []string       // LOADed before Lock; the binary passes {"httpfs","iceberg"}; unit tests pass none
    Catalog    CatalogAttach  // used by Task 4; zero value = reads unsupported
}
type CatalogAttach struct { URL, ProjectID, Warehouse, Token string }
func SettingsFromEnv() (duck.Settings, error)         // DATUPLET_GATEWAY_DUCKDB_* + cgroup-derived memory default (spec)
func New(cfg Config) (*Plane, error)                  // opens DB, LOADs cfg.Extensions, initialises the secret manager, locks; *Plane implements datagateway.DataPlane
func Factory(base Config) datagateway.DataPlaneFactory // fills Catalog from BootInfo (URL, ProjectID, Warehouse, Token) and calls New
func ParseS3Endpoint(raw string) (hostPort string, useSSL bool, urlStyle string, err error) // spec Write path 5
```
Until Task 4, `(*Plane).OpenTableReader` returns `errors.New("duckplane: reads not implemented")` so `*Plane` satisfies `datagateway.DataPlane`.

Writer behaviour (binding decisions from the spec): own connection per writer; lifetime context from the plane (see Task 1 context ownership); on first `Write` and after each rotation start a COPY: get creds and enforce the minimum remaining validity `max(2m, 25% of Expires-Issued)` (else `Invalidate()` + `Get()` once, else fail with `duckplane: vended credentials expire too soon`), refresh secret (`CREATE OR REPLACE SECRET "dgw_<writerID>" (TYPE s3, KEY_ID, SECRET, SESSION_TOKEN, REGION, ENDPOINT '<hostPort>', USE_SSL <useSSL>, URL_STYLE '<urlStyle>', SCOPE '<DataPrefix>')` from `ParseS3Endpoint(creds.Endpoint)`, or `(TYPE gcs, BEARER_TOKEN, SCOPE)` via `ExecRedacted`; skip for local-path prefixes), register a channel-fed Arrow view `dgv_<writerID>_<n>`, run `COPY (SELECT <casts> FROM view) TO '<DataPrefix><FilePrefix>-%05d.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE <rows>, RETURN_STATS true)` in a goroutine; `Write` sends the retained batch into the channel (blocks when full, returns COPY's error if it ended); rotation at fed bytes ≥ 4 × TargetFileBytes closes the channel, waits for the COPY result, records `WrittenFile{Path, Rows: count, Bytes: file_size_bytes}`; `Close` ends the current COPY (no COPY started if no rows were ever written → zero files); `Abort` closes the channel with an error so the COPY fails, releases everything. Types per spec §Write path 6, decided by the golden file: the server keeps today's format adapters and schema checks, so the writer only sees records today's path accepts; the writer's cast list maps each accepted Arrow type to the DuckDB type whose Parquet output equals the golden (physical/logical/converted type, no field_id, values round-trip as recorded); repetition OPTIONAL for all columns. A record whose Arrow type has no golden entry is rejected by the writer with an "unsupported Arrow type" error.

- [ ] **Step 1: Failing unit tests** (local-path prefix under `t.TempDir()`, no creds): `TestWriterSingleFile` (3 batches → 1 file, rows/bytes from stats match `parquet_metadata`); `TestWriterRotates` (TargetFileBytes tiny → ≥ 3 files, names `-00000`,`-00001`,…, total rows exact); `TestWriterZeroRows` (Close without Write → no files, nothing on disk); `TestWriterHostileColumnNames` (columns `a"b`, `x'y`, `;DROP`, `nl\nx`, `ünï`) → read back with `read_parquet` names verbatim; `TestWriterCopyErrorSurfaces` (unwritable prefix) → `Write` or `Close` returns error within 5 s, no goroutine leak (`goleak`-style count via `runtime.NumGoroutine` before/after); `TestWriterAbortIdempotent`; `TestCredsRefreshPerFile` (fake CredsSource counting `Get` calls with an S3-shaped creds value and a local prefix stub that only validates the generated secret SQL via an injected exec hook) → one creds call per file; `TestCredsNearExpiryRefetched` (first Get returns creds with 30 s left → `Invalidate` + second Get → COPY proceeds) and `TestCredsStillNearExpiryFails` (both Gets near expiry → write fails with `vended credentials expire too soon`, no COPY started); `TestWriteCtxDoesNotOwnWriter` (first `Write` with a ctx that is cancelled right after the call returns; a second `Write` with a live ctx succeeds; `Close` returns the rows of both); `TestParseS3Endpoint` table: `minio:9000` → (minio:9000, true, path), `http://minio:9000` → (…, false, path), `https://s3.eu-west-1.amazonaws.com` → (…, true, vhost), `https://notamazonaws.com` → path, `https://x.amazonaws.com.evil.io` → path, `ftp://x` → error, empty → error; `golden_test.go` `TestParquetTypesMatchGolden` drives every accepted golden type (two chunks) through the writer and compares `parquet_schema()` columns and read-back values to the golden (OPTIONAL excepted).
- [ ] **Step 2: Run** `cd gateway && CGO_ENABLED=1 go test -tags duckdb_arrow ./duckplane/ -v` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Integration test** `TestIntegrationWriteAndCommit` (fixture: Postgres + Lakekeeper + silo; create namespace/table with `pkg/datagateway/lakekeeper.Resolver.LoadOrCreateForWrite` using a schema with every golden type, all optional; real `catalogwriter.VendedCreds`; writer → files → `icebergjob.CommitTableFiles` APPEND then FULL_LOAD; assert via DuckDB `iceberg_scan`/catalog ATTACH: row counts 2 runs APPEND = sum, FULL_LOAD = last, snapshot summary carries `datuplet.commit-key`; second commit with identical key → `IdempotencyHit`); `TestIntegrationCredsInvalidateBetweenFiles` (rotate files with `VendedCreds.Invalidate()` between them → still succeeds). The integration fixture provisions extensions itself: before `New`, a throwaway unlocked DuckDB connection runs `INSTALL httpfs; INSTALL iceberg` into a per-run `t.TempDir()` extension directory (network to extensions.duckdb.org; that is why these tests are local-only), and the plane is opened with `Extensions: {"httpfs","iceberg"}` and that directory. Run: `cd gateway && CGO_ENABLED=1 go test -tags 'duckdb_arrow integration' -run Integration ./duckplane/ -v -timeout 15m` → PASS (Docker required; if Docker or the extension download is unavailable that is BLOCKED, not a pass).
- [ ] **Step 5: Run** unit tests again, root `CGO_ENABLED=0 go test -count=1 ./...` → PASS.
- [ ] **Step 6: Commit** `feat(gateway): DuckDB table writer (COPY to parquet per file, vended-creds secrets)`.

### Task 4: DuckDB table reader (`duckplane`)

**Model:** opus.

**Files:**
- Create: `gateway/duckplane/{reader.go,attach.go}`, tests `gateway/duckplane/reader_test.go`; extend `gateway/duckplane/integration_test.go`.

**Interfaces:**
- Consumes: Task 3 `Plane`, `Config.Catalog`; Task 2 `duck.Conn.QueryStream`.
- Produces: `(*Plane).OpenTableReader` returning a `datagateway.RecordStream`; lazy, once-per-plane (guarded by `sync.Once`-style error caching) `attach()` (iceberg is already loaded by `New`; `CREATE OR REPLACE SECRET dg_lk_tok (TYPE ICEBERG, TOKEN …)` via `ExecRedacted` after JWT shape validation copied from `components/queryengine/attach.go:29-40`; `ATTACH '<ProjectID>/<Warehouse>' AS lk (TYPE ICEBERG, URI '<URL>', SECRET dg_lk_tok, ACCESS_DELEGATION_MODE 'vended_credentials', READ_ONLY)`; `DROP SECRET dg_lk_tok`). Because `lock_configuration` must come after the first secret use (queryengine `engine_open.go:111-115`), `New` initialises the secret manager before `Lock` (Task 3) so this later ATTACH works — prove it in a test. Reader: own connection, lifetime context from the plane (Task 1 rule), `QueryStream("SELECT * FROM lk.<Ident(ns)>.<Ident(table)>")`; `Next(ctx)` returns batches (ctx bounds only that call), `io.EOF` at end; `Close` releases the stream and connection; reader on a plane without `Catalog` → error "reads require a catalog".

- [ ] **Step 1: Failing tests**: `TestReaderRequiresCatalog` (unit, no extensions); integration (extensions provisioned by the fixture, Task 3) `TestIntegrationAttachAfterLock` (plane opened and locked by `New`, then the first `OpenTableReader` ATTACHes successfully and reads a committed table), `TestIntegrationReadBack` (write via Task 3 + commit, read back: rows, schema names/types, values for 2 rows), `TestIntegrationReadEmptyTable` (table with no snapshot → stream with schema, zero rows, `io.EOF`), `TestIntegrationReadHostileNames` (namespace `ns"x` is not allowed by Lakekeeper — use a table named `t"x'y` if Lakekeeper accepts it, else document and test column names only), `TestIntegrationReaderIsStreaming` (the reader's underlying `duck.Stream.IsStreaming()` is true for the Iceberg scan; ≤ 2 batches alive at once); `TestIntegrationReaderCtxOwnership` (open with a ctx cancelled right after `OpenTableReader` returns, `Next` with live ctxs still drains all rows; `Close` mid-stream then a new reader works); the process-RSS proof against today's gateway is Task 9.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** unit + integration → PASS.
- [ ] **Step 5: Commit** `feat(gateway): streaming DuckDB table reader via read-only Lakekeeper attach`.

### Task 5: Rewire the server write path onto DataPlane; new sidecar binary

**Model:** opus.

**Files:**
- Modify: `pkg/datagateway/{server_v2.go,server_v2_writing.go,server_v2_http.go,server_v2_commit.go,config.go}`, tests `pkg/datagateway/{server_v2_test.go,server_v2_commit_test.go,server_v2_bucket_resolve_test.go,external_files_test.go}` (rewrite or delete per the map below).
- Create: `gateway/cmd/datagateway/main.go`, `gateway/cmd/datagateway/main_test.go`, `gateway/cmd/datagateway/notag.go` (`//go:build !duckdb_arrow`: `main` prints "built without duckdb_arrow" and exits 20, like `components/sql-transform/main_notag.go`).
- Delete (`git rm -r`): `pkg/datagateway/buffer/` (golden generator included — the golden JSON stays in `gateway/duckplane/testdata/`), `pkg/datagateway/partition/`, `pkg/datagateway/files_manifest.go` + test, `pkg/datagateway/parquet_patcher.go`, `pkg/datagateway/external_files_test.go`, `cmd/datuplet/gateway.go` and the `gateway` subcommand wiring in `cmd/datuplet/main.go`.
- Modify: `pkg/datagateway/manifest/` — keep `SchemaToIceberg` (all fields `Required: false` now, per spec), delete the `_schema`/`_manifest` JSON writers and readers and their tests.

**Interfaces:**
- Consumes: Task 1 `DataPlane`, `LoadConfigFile`; Task 3/4 `duckplane.New`, `SettingsFromEnv`.
- Produces: `func NewServerV2(cfg *Config, newPlane DataPlaneFactory) (*ServerV2, error)` — runs today's boot sequence (secret resolution, `validateRunTokenForBoot`, resolver, audit props, commit pool, `datupleticeio.SetTokenProvider`), then calls `newPlane(ctx, BootInfo{RunID, LakekeeperURL, ProjectID, Warehouse, Token})` exactly once, publishes the plane, and only then starts the cancel watcher (ordering detailed below) with the validated claims (empty claims when no run token is configured, as in unit tests); returns errors instead of `log.Fatalf` where today it fatals (the binary turns them into exit code 20). **Every** existing caller of `NewServerV2` is migrated in this task, including the read-side tests that Task 6 later rewrites (`server_v2_streaming_test.go:51`, `cancel_watch_test.go:172`, all `createTestServerV2*` helpers) — mechanical migration only, so the root module stays green; binary `datagateway` (wires `duckplane.Factory(duckplane.Config{Settings: SettingsFromEnv(), Extensions: {"httpfs","iceberg"}})`) with flags `--config`, `--addr` (default `:50051`), `--run-token-path` / env `RUN_TOKEN_PATH`, `--pod-annotations-path` / env `POD_ANNOTATIONS_PATH`, and accepted-but-ignored `--minio` and `--local` (so the operator's args keep working); SIGTERM → `server.Close()`.

Behaviour to implement (spec §Write path): writer state holds a `TableWriter` instead of `bufferMgr`/`partitionRouter`/`writerBackend`; the drop pipeline is built **before** any table create, and every create path uses the post-drop schema — the eager create in `OpenWriter` from a supplied `OpenWriterRequest.schema` (today `server_v2_writing.go:127-134`, before the pipeline exists at :166-169) applies `pipeline.TransformSchema` to the supplied schema first, while parsing still uses the original input schema; the deferred create on the first chunk does the same with the inferred schema; a supplied schema no longer suppresses data; opens the `TableWriter` with `Creds` = the table's `*catalogwriter.VendedCreds` (keep `Resolver` building `VendedCreds` per table; delete its backend construction); **today's record-type acceptance check moves from the deleted `BufferManager` into the server, at the same position in the pipeline**: parse (adapter-level input validation unchanged) → drop pipeline → validate the *post-drop* record against the canonical **output** schema (`schema.ToArrowType` of the post-drop gateway schema, compared as `buffer/manager.go:332-336` + `buffer/coalesce.go:45-54` do today; today's error text) → `TableWriter.Write`, so the plane only ever receives canonical Arrow types and a non-canonical type in a dropped column stays accepted as today; `WriteChunkResponse.buffer_size_bytes` = `QueuedBytes()`; `CloseWriter` → `Close` → totals from `WrittenFile`s → `commitPool.Dispatch` (zero files → none); `external_files` → error `external data files are not supported`; `partition_fields` → one `log.Printf` warning per writer, ignored; plane creation ordering: `NewServerV2` calls the factory **before** starting the cancel watcher, publishes the plane, and only then starts the watcher (a cancel annotation already present at boot is seen on the watcher's first poll and interrupts the published plane); if the factory fails, `NewServerV2` closes the commit pool and returns the error with no watcher started; the watcher calls `plane.Interrupt()` and aborts open writers; `Close()` closes the plane after the pool drains. `GetConfig` stops building `StorageBootstrap` (field left empty). Delete the `DATUPLET_GATEWAY_ASYNC_*` knobs, `readControlFile` legacy partition path, and `writeSchemaAndManifest`.

Test map: keep and adapt secrets, GetConfig, run-token boot, OpenWriter bucket resolution + error texts, write-mode, Commit reconciliation (fixtures now use the package-local `fakeDataPlane`); add `TestGoldenAcceptanceReplayedThroughServer` (reads `gateway/duckplane/testdata/parquet-types.golden.json` and replays **every** entry — accepted and rejected — as two Arrow IPC chunks over the HTTP write endpoint against the fake plane: rejected entries fail at the recorded chunk with the recorded error class, accepted entries reach the plane as canonical Arrow types with the recorded values); `TestNonCanonicalTypeInDroppedColumnAccepted` (one IPC chunk with retained `id:int64` and dropped `discard:int8` → accepted, plane receives only `id`), `TestFactoryBeforeCancelWatcher` (blocking factory + cancel annotation present at boot → after the factory returns, the plane is interrupted on the watcher's first poll); `TestFactoryFailureStartsNoWatcher` (factory error → `NewServerV2` error, no goroutine leak); add `TestWritePathUsesPostDropSchemaForCreate`, `TestSuppliedSchemaStillWritesData`, `TestSuppliedSchemaWithDropCreatesPostDropTable` (supplied schema containing a dropped column + drop processor → the schema passed to `LoadOrCreateForWrite` lacks the column, and the batches the plane receives lack it too), `TestFactoryReceivesValidatedClaims` (signed test token + httptest JWKS as in the existing `validateRunTokenForBoot` tests → `BootInfo` carries project/warehouse/token/run id), `TestExternalFilesRejected`, `TestPartitionFieldsIgnoredWithWarning`, `TestCancelInterruptsPlaneAndAbortsWriters`, `TestCloseWriterDispatchesWrittenFiles` (paths/rows from fake), `TestWriteErrorFromPlaneIs500` (HTTP envelope `PROCESS_ERROR`). `gateway/cmd/datagateway/main_test.go`: flags parse incl. `--minio`; missing `--config` exits non-zero with usage.

- [ ] **Step 1: Write the new/adapted failing tests.** **Step 2: Run** `go test ./pkg/datagateway/... -v` → FAIL.
- [ ] **Step 3: Implement + deletions.** Keep `server_v2_reading.go` compiling against the old backend for now (Task 6 rewires it) — if deleting `buffer/` breaks the read side, move only what the read side still needs, do not rewrite it here.
- [ ] **Step 4: Run** root `CGO_ENABLED=0 go build ./... && go vet ./... && go test -count=1 ./...` → PASS; `cd gateway && CGO_ENABLED=1 go build -tags duckdb_arrow ./... && go test -tags duckdb_arrow -count=1 ./...` → PASS; `cd gateway && CGO_ENABLED=1 go test -tags 'duckdb_arrow integration' -run Integration ./... -timeout 20m` → PASS.
- [ ] **Step 5: Commit** `feat(datagateway)!: write path on the DuckDB data plane; datagateway binary replaces 'datuplet gateway'`.

Known intermediate state (accepted): between this commit and Task 7 the `utils/docker/Dockerfile.gateway` image cannot build (its `datuplet gateway` entrypoint is gone). No task between them builds images; Task 7 restores it and Task 9 is the first e2e run.

### Task 6: Rewire the server read path; delete the Go storage backends

**Model:** opus.

**Files:**
- Modify: `pkg/datagateway/{server_v2_reading.go,server_v2_http.go,server_v2.go}`, `pkg/datagateway/lakekeeper/lakekeeper.go` (delete `LoadTableForRead` and backend construction; keep `LoadOrCreateForWrite`, `Catalog`, `VendedCreds` construction), tests `pkg/datagateway/{server_v2_streaming_test.go,server_v2_test.go}`, `pkg/datagateway/lakekeeper/lakekeeper_test.go`.
- Create: `pkg/datagateway/read_serialize.go` + `read_serialize_test.go`.
- Delete (`git rm -r`): `pkg/datagateway/backend/` (and its integration tests), `pkg/lib/controlfile` if it has no remaining importer, any `format` code used only by the deleted backend.
- Modify: `.github/workflows/pr.yml` (drop `./pkg/datagateway/backend/...` from the integration-tag test line; keep `./pkg/datupleticeio/...`), `CLAUDE.md` CI-contract line naming that package.

**Interfaces:**
- Consumes: Task 1 `RecordStream`, Task 4 reader.
- Produces: `OpenReader` → `plane.OpenTableReader(ReaderSpec{Namespace: bucket, Table: table})`; per chunk the server pulls batches until ≥ `ChunkSize` uncompressed bytes or EOF and serializes: `FORMAT_ARROW_IPC` one IPC stream; `FORMAT_PARQUET` one self-contained Parquet file via arrow-go `pqarrow` (ZSTD) from those batches; CSV/JSON/JSONL via existing `format` adapters' Serialize. HTTP `GET /data/read/{id}` and gRPC `ReadChunk` keep today's headers/fields (`X-Datuplet-Is-Last`, `X-Datuplet-Rows`, `DataChunk.is_last`, rows). Empty table: one chunk, `is_last=true`, zero rows; for `FORMAT_PARQUET` that chunk is a valid zero-row Parquet file with the table schema. Incremental spec: log today's warning and read the full table. `GetSchema`/`GetSample` → `status.Error(codes.Unimplemented, …)`.

- [ ] **Step 1: Failing tests** (fake plane): `TestReadParquetChunksAreSelfContained` (3 chunks, each parses with `pqarrow`, total rows), `TestReadArrowIPCChunks`, `TestReadCSVAndJSONLChunks`, `TestReadEmptyTableParquetZeroRowFile`, `TestReadHeadersIsLastAndRows` (HTTP), `TestReadPlaneErrorSurfaces`, `TestGetSchemaUnimplemented`, `TestReadSurvivesOpenReaderCtxCancel` (the OpenReader RPC ctx is cancelled after it returns; subsequent HTTP GETs still drain every chunk), `TestCloseReaderReleasesStream` (fake records `Close`), `TestCancelInterruptsReaders`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement + deletions.**
- [ ] **Step 4: Run** root `CGO_ENABLED=0 go build ./... && go vet ./... && go test -count=1 ./...` → PASS; `go test -count=1 -tags=integration ./pkg/datupleticeio/...` → PASS (needs Docker); gateway module unit + integration → PASS; add `gateway/duckplane` integration `TestIntegrationServerReadParquetEndToEnd`: real `ServerV2` (`NewServerV2(cfg, duckplane.Factory(...))`) + fixture. Because a real server refuses Lakekeeper config without validated claims, the test generates an RSA key, serves its JWKS from an `httptest.Server`, mints an RS256 run token with exactly the claims the validator requires (`iss=datuplet-api`, `aud=datuplet-catalog`, `token_kind=run`, `sub=run_id=$RUN_ID`, `project_id` = the fixture's Lakekeeper project, `warehouse` = the fixture warehouse name, exp +1 h), sets `RUN_ID`, and writes the token to a temp file (Lakekeeper authn is off in the fixture, so it accepts any bearer). Flow: write via HTTP Arrow IPC, `Commit`, read via HTTP `FORMAT_PARQUET`, stage chunks to files, DuckDB `read_parquet` count = written; snapshot summary carries `datuplet.run-id` and `datuplet.commit-key`.
- [ ] **Step 5: Commit** `feat(datagateway)!: read path on the DuckDB data plane; remove Go storage backends`.

### Task 7: Image, build targets, release wiring, operator sidecar resources

**Model:** sonnet.

**Files:**
- Modify: `utils/docker/Dockerfile.gateway` (rewrite on the `utils/docker/query-worker.Dockerfile` pattern: `golang:1.25-bookworm` builder, `COPY . .`, `CGO_ENABLED=1 go build -tags=duckdb_arrow -o /datagateway ./cmd/datagateway` from `gateway/`; baker `utils/docker/install-duckdb-ext` run as `HOME=/root`; runtime `debian:bookworm-slim` + `ca-certificates libstdc++6`; user `datuplet` **uid/gid 65532**, home `/home/datuplet`, baked `/home/datuplet/.duckdb` chowned 65532; `/scratch` owned by 65532; `ENV HOME=/home/datuplet DATUPLET_GATEWAY_DUCKDB_EXTENSION_DIR=/home/datuplet/.duckdb/extensions`; `ENTRYPOINT ["datagateway"]`; `verify` stage `RUN --network=none` that runs `/ext-check` **and** `datagateway --selfcheck` (new flag: opens the plane with default settings, loads httpfs + iceberg, locks, exits 0) as uid 65532), `Makefile` (`build-gateway` unchanged command, comments; `build` no longer mentions gateway), `.github/workflows/_release-services.yml` (gateway comment: CGO + multi-arch; timeouts if needed), `pkg/k8s/controllers/pipelinerun_jobs.go` (gateway container: `emptyDir` volume `gateway-scratch` sizeLimit 2Gi mounted at `/scratch`; resources requests cpu 100m / memory 256Mi, limits memory 1Gi; no change to args/ports), its tests in `pkg/k8s/controllers/`.
- Modify: `gateway/cmd/datagateway/main.go` (`--selfcheck`).

- [ ] **Step 1: Failing tests**: controller test `TestGatewaySidecarHasScratchAndResources` (volume, mount, sizeLimit, requests/limits exact); `main_test.go` `--selfcheck` flag parsed.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.**
- [ ] **Step 4: Verify**: `make build-gateway` → image builds including the verify stage (proves offline extension load as uid 65532); `docker image ls datuplet/gateway:latest` size recorded in the report; `docker run --rm --entrypoint datagateway datuplet/gateway:latest --selfcheck` → exit 0; root tests + `go test ./pkg/k8s/...` → PASS. Changing CRDs is not part of this task (no `make k8s-reload-crds` needed).
- [ ] **Step 5: Commit** `build(gateway): CGO DuckDB image, offline-verified extensions, sidecar scratch + resources`.

### Task 8: Docs and dead-code cleanup

**Model:** sonnet.

**Files:**
- Modify: `CLAUDE.md` (project overview "Data Gateway sidecar"; Commands/one-test; CI contract item 6 adds `gateway`; Non-obvious conventions: `joinStoragePath` list (drop deleted files), the `pkg/lib/datalake` vs `pkg/datagateway/backend` bullet (backend gone → DuckDB data plane in `gateway/duckplane`), the "Buffer package outputs Parquet only" bullet (replace), the "Per-table files.json manifests" bullet (rewrite: CloseWriter → `TableWriter.Close` → commit pool; no files.json), monorepo `go mod tidy` module list (+ `gateway/`), Key directories (+ `gateway/`), "Primary languages" (+ CGO in `gateway/`)), `README.md` architecture-at-a-glance, `docs/architecture.md` §"Data Gateway sidecar" + credential flow + data flow, `docs/components.md` (gateway mentions; sql-transform section unchanged in behaviour), `docs/known-limitations.md` (add: GCS on the DuckDB data plane unverified; partition fields ignored; external files unsupported; files.json removed; FULL_LOAD zero rows still no truncate; fix the stale `loadTableRefresh` paragraph only if it touches gateway text), `docs/troubleshooting.md` (`-c datagateway` → `-c gateway`; commit-failure section: no files.json; DuckDB errors), `docs/dependency-upgrades.md` (already in Task 2 — verify), `REVIEW.md` if it names deleted paths.
- Delete: dead code found by `make lint` (`deadcode -test ./...`) in the root module that the deletions orphaned — e.g. `pkg/icebergjob` `CommitTable` + `manifest_loader.go` if unused, `schema` inference helpers only the backend used. Do not delete anything `deadcode` does not flag unless a grep proves it unused.

- [ ] **Step 1:** `make lint` → list findings. **Step 2:** delete orphaned code, update docs. **Step 3:** `make lint` → clean; root tests → PASS; gateway module tests → PASS.
- [ ] **Step 4: Commit** `docs,chore: DuckDB data plane docs; drop code orphaned by the backend removal`.

### Task 9: Live verification — e2e on OrbStack and performance parity

**Model:** sonnet · controller may run it directly. No code changes except fixes that go back through a review loop.

- [ ] **Step 1: e2e.** Check `duckdb --version` (2.0.0) first. `make e2e-k8s` against OrbStack (uses `datuplet-e2e`, builds all images incl. the new gateway, installs, runs the full suite, tears down). Expected: every scenario PASS, none SKIP (`E2E_REQUIRE=1` as in CI). On failure: collect `-c gateway` logs, report BLOCKED with the evidence — do not patch tests.
- [ ] **Step 2: Performance.** Use the spike harness at `/Users/tomaskacur/mydevel/datuplet/spikes/duckdb-sidecar` (main checkout, uncommitted; fixture `fixture/up.sh`, baseline driver `baseline/run.sh`, `measure/run.sh`, `cand/interleave.sh`). Baseline = `datuplet gateway` built from `ecff5c5` (`git -C /Users/tomaskacur/mydevel/datuplet worktree add <scratch>/base ecff5c5` read-only build); candidate = `gateway/cmd/datagateway` from this branch, same flags/config (the harness's stub JWKS + minted run token). Unmodified `http-json-extractor` from this branch. L dataset, interleaved baseline/candidate, warm-up + 3 runs each. Pass bar: median wall ≤ 1.10 × baseline and median summed peak RSS ≤ 1.25 × baseline. Also time a full `FORMAT_PARQUET` HTTP read of the L table (old vs new, 3 runs; `cmd/readbench gw` mode) and report it without a bar.
- [ ] **Step 3: Report** to `docs/superpowers/specs/2026-10-31-duckdb-gateway-verification.md`: e2e summary (scenario list, PASS/FAIL), perf table with commands and raw file paths, image size, anything not verified (GCS). Commit `docs: DuckDB gateway verification report`.

---

## Final gate

Codex whole-branch review of `public/main...feat/duckdb-sidecar` with the spec, this plan and the Task 9 report. Critical/Important findings stop for the maintainer. Then report to the maintainer; pushing and the draft PR happen only on their go.
