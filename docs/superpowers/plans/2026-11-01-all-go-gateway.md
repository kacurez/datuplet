# All-Go Data Gateway Data Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per repo CLAUDE.md every task review and the final whole-branch gate are run by **Codex** (codex-companion); Critical/Important findings stop for the maintainer.

**Goal:** Replace the gateway's buffering/upload pipeline and its Iceberg-unaware read path with a pure-Go data plane (arrow-go `pqarrow` writer, iceberg-go v0.7.0 scan with per-file raw passthrough) behind the unchanged component contract, and upgrade iceberg-go to v0.7.0.

**Architecture:** `pkg/datagateway.ServerV2` talks to a `DataPlane` interface; the plane-agnostic server code (interface, fake, config loader, type golden, write/read rewiring, read serializers) is **ported from the DuckDB branch** (`feat/duckdb-sidecar`, commits named per task) and adapted. A new package `pkg/datagateway/goplane` implements the plane. `cmd/datuplet gateway` wires `goplane.Factory`; the gateway image is unchanged.

**Tech Stack:** Go 1.25.9, arrow-go v18 (`pqarrow`), iceberg-go v0.7.0 (scan, IO, `AddFiles`), gocloud blob (S3/GCS via iceberg-go IO), Docker/OrbStack for integration and e2e.

**Spec:** `docs/superpowers/specs/2026-11-01-all-go-gateway-design.md` (binding). Evidence: `docs/superpowers/specs/2026-11-01-go-write-plane-spike-report.md`.

## Global Constraints

- **Branch** `feat/all-go-gateway` in worktree `/Users/tomaskacur/mydevel/datuplet/.claude/worktrees/all-go-gateway`, based on `public/main` `ecff5c5`. Work only there. One logical commit per task (fix rounds may add commits). **Never** `git push`, `git tag`, `gh pr create`, touch `main`, or modify the DuckDB branch / PR #46.
- **Porting from the DuckDB branch** (maintainer: also bring its fixes where they make sense — this plan ports its reviewed server code with all review-round fixes, the listen-first boot gate, and closes two open findings on that gate) is done only with `git show <commit>:<path>`, `git diff <a> <b> -- <paths> | git apply -3`, or `git checkout <commit> -- <path>` for the exact commits and paths a task names; never merge or cherry-pick whole commits. Nothing under `gateway/`, `sdk/`, `utils/docker/`, `charts/` comes over. After porting, the task adapts the code to this plan's interfaces.
- `git add` only the exact paths the task touched — never `git add -A` / `git add .`. Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Never** `rm -rf`; delete tracked files with `git rm`. Temp files under the session scratchpad only.
- **Never** bare `go mod tidy`; use `make tidy` (all modules).
- **Pure Go:** `CGO_ENABLED=0 go build ./... && go vet ./... && go test -count=1 ./...` from the repo root must pass on a bare machine. No `github.com/duckdb/*` import outside `components/queryengine`, `components/sql-transform`, `utils/docker/install-duckdb-ext`.
- **Contract frozen:** `api/proto/gateway/v2/gateway.proto` + generated code, `sdk/go`, `components/*`, `cmd/mock-datagateway`, `tests/e2e` scenarios, `utils/docker/Dockerfile.gateway`, the operator's sidecar args. (`tests/e2e/go.mod` and `components/queryengine/go.mod` change only through `make tidy` in Task 1.)
- **Secrets hygiene:** never log or wrap into an error vended credential values, IO props, or the run JWT.
- **Storage paths are opaque strings:** never `path.Join`/`filepath.Join` on a URL; concatenate.
- **Exit codes / status protocol unchanged** (`0` / `1` / `>=20`, `DUPLET_STATUS_MESSAGE:`).
- Writer constants (spec Write path 4–7): channel 4 batches; rotation at fed uncompressed Arrow bytes ≥ 4 × `TargetFileBytes` (default 128 MiB); `RowGroupRows` default 229376; ZSTD; dictionary on; stats on; `WithStoreSchema`; field-id metadata stripped; names `<DataPrefix><FilePrefix>-%05d.parquet`; creds validity `max(2m, 25% of Expires−Issued)`.
- Read constants (spec Read path): raw eligibility rules exactly as spec Read path 2; `chunk_size` default 32 MiB uncompressed; one record batch per `FORMAT_ARROW_IPC` chunk.
- Integration fixtures: Docker CLI, Postgres + `quay.io/lakekeeper/catalog:v0.12.1` + the silo image the charts pin, Lakekeeper authn off, container names prefixed `gw-it-`, build tag `integration`. Model them on the DuckDB branch's `gateway/duckplane/fixture_test.go` (`git show 8ed7edab:gateway/duckplane/fixture_test.go`), rewritten in pure Go without DuckDB.

## Review Focus

1. **A data file served raw when Iceberg semantics change it** — position/equality deletes or deletion vectors on the file, an added, dropped or renamed column since it was written, a type the table schema widens (int8 → int), a file with mismatching field ids. Expected: decoded, deletes applied, columns and types as the current schema. Tests in Task 5 (unit + integration).
2. **Raw and decoded chunks of one read disagreeing** on names or Arrow types (mixed table: some files deleted-from, others clean). Expected: every `FORMAT_PARQUET` chunk's Arrow schema equals `TableReader.Schema()`. Tests in Task 5 and Task 6.
3. **Credentials expiring between files** of a long write or read. Expected: each file (write) and each raw open / `ReadTasks` call (read) uses credentials current at its start; near-expiry creds are re-fetched once, then the operation fails clearly. Tests in Task 3 and Task 5.
4. **Failure mid-stream** (upload error, cancel annotation, component disconnect, Abort). Expected: `Write` returns the error promptly, the in-flight upload is aborted (no object completed), no goroutine leak, `Close` returns the error, nothing dispatched to the commit pool, `Abort` idempotent. Tests in Task 3 and Task 4.
5. **Empty and zero-row cases:** writer closed with no rows (no file, SKIPPED); read of a table with no snapshot, and of a snapshot with zero data files (one last chunk, zero rows; `FORMAT_PARQUET` = valid zero-row file with the table schema). Tests in Task 5 and Task 6.

---

### Task 1: Upgrade iceberg-go to v0.7.0

**Model:** opus · all modules.

**Files:** `go.mod`/`go.sum` of every module `make tidy` touches (root, `tests/e2e`, `components/*`, `sdk/go*`, `utils/docker/install-duckdb-ext` as needed); Go sources broken by the upgrade (expected: `pkg/icebergjob/`, `pkg/catalogwriter/`, `pkg/datupleticeio/`, `pkg/datagateway/lakekeeper/`, `pkg/datagateway/backend/` (still present), `pkg/pipelineapi/storage/`, `components/queryengine/`); `docs/dependency-upgrades.md` only if it pins the iceberg-go version.

**Interfaces:** Consumes nothing. Produces: every module on `github.com/apache/iceberg-go v0.7.0`; `go` directive `1.25.9` wherever MVS requires it (iceberg-go v0.7.0 declares `go 1.25.9`); arrow-go / gocloud / cloud.google.com/go/storage at whatever MVS selects (expected arrow-go v18.8.0, gocloud v0.46.0). No behaviour change.

- [ ] **Step 1:** `go get github.com/apache/iceberg-go@v0.7.0` in the root module, then `make tidy`.
- [ ] **Step 2:** `CGO_ENABLED=0 go build ./... && go vet ./...` → fix every compile break by adapting to the v0.7.0 API (read the v0.7.0 source in the module cache; keep semantics identical: `AddFiles`, `ReplaceDataFiles`, snapshot properties, idempotency lookup, REST catalog options, vended-credential props, the `gs://` factory override in `pkg/datupleticeio`). No unrelated refactors.
- [ ] **Step 3:** Run `go test -count=1 ./...` (root) → PASS; `go test -count=1 -tags=integration ./pkg/datagateway/backend/... ./pkg/datupleticeio/...` → PASS (Docker); with `TEST_DATABASE_URL` set to a local Postgres (`docker run -d --name gw-it-pg -p 55432:5432 -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test postgres:16`), `go test -count=1 -p 1 ./pkg/pipelineapi/...` → PASS; `cd components/queryengine && go test -count=1 -tags duckdb_arrow ./...` and `cd components/sql-transform && go test -count=1 -tags duckdb_arrow ./...` → PASS; `cd tests/e2e && go vet ./...` → PASS; `scripts/verify-versions.sh` → PASS.
- [ ] **Step 4:** Commit `build(deps): iceberg-go v0.7.0 (go 1.25.9)`. The report lists every API adaptation made and every module whose `go`/deps changed.

### Task 2: DataPlane seam ported from the DuckDB branch

**Model:** sonnet · root module.

**Port:** from `feat/duckdb-sidecar` commits `35224fda`, `7017686a`, `73a73e0d` (its Task 1 and two review rounds): `pkg/datagateway/dataplane.go`, `pkg/datagateway/fake_dataplane_test.go`, `pkg/datagateway/gatewayconfig.go`, `pkg/datagateway/gatewayconfig_test.go`, `pkg/datagateway/buffer/golden_types_test.go`, and the `cmd/datuplet/gateway.go` change (use `datagateway.LoadConfigFile`). Take the files as of `73a73e0d` (`git show 73a73e0d:<path>`; `cmd/datuplet/gateway.go` via `git diff ecff5c51 73a73e0d -- cmd/datuplet/gateway.go | git apply -3`). The golden moves: generator writes `pkg/datagateway/testdata/parquet-types.golden.json` (create from `git show 73a73e0d:gateway/duckplane/testdata/parquet-types.golden.json`, then regenerate with `-update-golden` on this branch and confirm no diff).

**Adapt — read interface (replaces the ported `ReaderSpec`/`OpenTableReader` shape), exact:**
```go
type ReaderSpec struct {
    Namespace, Table string
    RawParquet       bool // request format is FORMAT_PARQUET: raw-eligible data files may come back as RawFile parts
}
// ReadPart is one ordered piece of a table scan; exactly one of Raw and Records is set.
type ReadPart struct {
    Raw     *RawFile
    Records RecordStream // decoded batches with TableReader.Schema(); the caller closes it
}
type RawFile struct {
    Path string
    Rows int64                                            // record count from the manifest entry
    Size int64                                            // bytes
    Open func(ctx context.Context) (io.ReadCloser, error) // the file's bytes, credentials current at call time
}
type TableReader interface {
    Schema() *arrow.Schema                          // the table's current schema; every part carries exactly it
    NextPart(ctx context.Context) (ReadPart, error) // io.EOF after the last part (immediately for an empty table)
    Close() error                                   // closes the reader and any part stream still open
}
// DataPlane.OpenTableReader(ctx context.Context, spec ReaderSpec) (TableReader, error)
```
`RecordStream`, `TableWriter`, `WriterSpec`, `WrittenFile`, `CredsSource`, `BootInfo` (with its redacting `String()`), `DataPlaneFactory`, `LoadConfigFile` and the context-ownership rule stay as ported. The fake gains `setTableParts(ns, table string, schema *arrow.Schema, parts ...fakePart)` where a `fakePart` is raw bytes (+rows) or batches, and records `Close` of the reader and of every part stream; existing fake self-tests are adapted, new ones cover parts (order, EOF, empty table, raw `Open` after `Close` fails).

- [ ] **Step 1:** Port the files; adapt the read interface and the fake; relocate the golden.
- [ ] **Step 2:** `go test ./pkg/datagateway/... ./cmd/datuplet/... -run 'LoadConfig|Fake|Golden' -v` → PASS; `go test ./pkg/datagateway/buffer/ -run TestGoldenParquetTypes -update-golden` then `git diff --exit-code pkg/datagateway/testdata/` → no diff.
- [ ] **Step 3:** Root `CGO_ENABLED=0 go build ./... && go vet ./... && go test -count=1 ./...` → PASS.
- [ ] **Step 4:** Commit `feat(datagateway): DataPlane seam (ported from the DuckDB branch) with a raw/decoded read interface`.

### Task 3: Go table writer (`goplane`)

**Model:** opus.

**Files:** Create `pkg/datagateway/goplane/{plane.go,writer.go,storage.go}`, tests `pkg/datagateway/goplane/{writer_test.go,storage_test.go,golden_test.go}`, `pkg/datagateway/goplane/{fixture_test.go,integration_test.go}` (`//go:build integration`).

**Interfaces:**
- Consumes: Task 2 `datagateway.{DataPlane,TableWriter,WriterSpec,WrittenFile,CredsSource,BootInfo}`; `catalogwriter.{Creds,S3Creds,GCSCreds}`.
- Produces:
```go
package goplane
type TableLoader interface { // reads only; Task 5 uses it
    LoadTable(ctx context.Context, ns, tbl string) (*table.Table, datagateway.CredsSource, error)
}
type Config struct{ Tables TableLoader } // nil Tables: OpenTableReader returns an error
func New(cfg Config) *Plane               // *Plane implements datagateway.DataPlane
func Factory(ctx context.Context, boot datagateway.BootInfo) (datagateway.DataPlane, error) // Task 5 fills Tables from boot; until then New(Config{})
func ioProps(c catalogwriter.Creds) (map[string]string, error) // spec Write path 6 (S3 keys, region default, endpoint, virtual addressing for *.amazonaws.com; GCS token props)
```
`pkg/datupleticeio` is blank-imported by `goplane`. Until Task 5, `OpenTableReader` returns `errors.New("goplane: reads not implemented")`.

Writer behaviour — spec Write path 4–7 exactly (the spike `spikes/go-write-plane/goplane/goplane.go` in the DuckDB worktree is a starting point, not a reference: it lacks creds validity, abort-cancels-upload, GCS, field-id stripping). Lifetime context from the plane (context-ownership rule); a failed file or `Abort` cancels the context the upload's IO was loaded with so the multipart upload is aborted; `Close` with no rows → zero files; `QueuedBytes` = bytes waiting in the channel.

- [ ] **Step 1: Failing unit tests** (local `file://` prefix under `t.TempDir()`, nil creds): `TestWriterSingleFile` (3 batches → 1 file; rows/bytes match the Parquet footer); `TestWriterRotates` (tiny target → ≥ 3 files `-00000`…, total rows exact); `TestWriterZeroRows`; `TestWriterParquetProps` (ZSTD, row groups ≤ `RowGroupRows`, no field ids even when the input schema carries `PARQUET:field_id`); `TestWriterHostileColumnNames` (`a"b`, `x'y`, `;DROP`, `nl\nx`, `ünï` round-trip); `TestWriterUploadErrorSurfaces` (unwritable prefix → `Write` or `Close` errors within 5 s, goroutines back to baseline); `TestWriterAbortIdempotent` + `TestAbortLeavesNoObject` (abort mid-file → the file does not exist); `TestWriteCtxDoesNotOwnWriter`; `TestCredsPerFile` (fake `CredsSource` counting `Get` → one per file, IO props built from the creds current at that file); `TestCredsNearExpiryRefetched`; `TestCredsStillNearExpiryFails` (`goplane: vended credentials expire too soon`, no file started); `TestIOProps` table (S3 minio endpoint → path style; `https://s3.eu-west-1.amazonaws.com` → virtual; `https://x.amazonaws.com.evil.io` → path; empty region → `us-east-1`; GCS → token props; no secret value in any returned error); `golden_test.go` `TestParquetTypesMatchGolden` (every accepted golden type, two chunks, through the writer → physical/logical/converted type, repetition and read-back values equal the golden, no deviations).
- [ ] **Step 2: Run** `go test ./pkg/datagateway/goplane/ -v` → FAIL. **Step 3: Implement.**
- [ ] **Step 4: Integration** (fixture: Postgres + Lakekeeper + silo): `TestIntegrationWriteAndCommit` — table created via `lakekeeper.Resolver.LoadOrCreateForWrite` with a schema of every golden type that `AddFiles` accepts; real `catalogwriter.VendedCreds`; writer → files → `icebergjob.CommitTableFiles` APPEND twice then FULL_LOAD; read back with iceberg-go `Scan().ToArrowRecords` → APPEND rows = sum, FULL_LOAD = last; snapshot summary carries `datuplet.commit-key`; same key again → idempotency hit. `TestIntegrationCredsInvalidateBetweenFiles`. Run `go test -count=1 -tags=integration -run Integration ./pkg/datagateway/goplane/ -v -timeout 15m` → PASS (Docker unavailable = BLOCKED, not a pass).
- [ ] **Step 5:** Root build/vet/test → PASS. Commit `feat(goplane): Go table writer (pqarrow ZSTD, streamed upload via iceberg-go IO, per-file vended creds)`.

### Task 4: Rewire the server write path onto the DataPlane

**Model:** opus.

**Port:** the root-module changes of DuckDB-branch commits `4988c601` and `233f42cd` (its Task 5 + review round): `git diff 472a2b33 233f42cd -- pkg/ | git apply -3`, then resolve. Do **not** take `cmd/` (the DuckDB branch deleted `cmd/datuplet/gateway.go`; here it stays), `gateway/`, `go.mod`/`go.sum` (`make tidy` instead). The port brings: writer state on `TableWriter`, post-drop create, record-type acceptance in the server (ruling R7), `NewServerV2(cfg, newPlane DataPlaneFactory)` with factory-before-cancel-watcher ordering, `external_files` error, `partition_fields` warning, `GetConfig` without `StorageBootstrap`, deletion of `pkg/datagateway/{buffer,partition}/`, `files_manifest.go`, `parquet_patcher.go`, `external_files_test.go`, the `_schema`/`_manifest` writers in `manifest/`, and the tests `server_v2_write_test.go`, `golden_replay_test.go` (repoint to `pkg/datagateway/testdata/parquet-types.golden.json`), adapted `server_v2_test.go`, `server_v2_commit_test.go`, `lakekeeper_test.go`. The golden generator `buffer/golden_types_test.go` is deleted with `buffer/` (the JSON stays).

**Adapt:** `cmd/datuplet/gateway.go` keeps the `datuplet gateway` subcommand with today's flags (`--minio`/`--local` accepted; `--minio` path is the operator's) and calls `datagateway.NewServerV2(cfg, goplane.Factory)`; the local/direct-MinIO dev modes that built static backends are removed (flags still parse). `NewServerV2` errors exit 20. Keep `server_v2_reading.go` compiling against the old backend (Task 6 rewires it); if the deletions break the read side, move only what it needs.

- [ ] **Step 1:** Port; adapt; add `cmd/datuplet` test `TestGatewaySubcommandFlags` (`--minio --config x --addr y --run-token-path z --pod-annotations-path w` parse; missing `--config` → non-zero exit with usage).
- [ ] **Step 2: Run** root `CGO_ENABLED=0 go build ./... && go vet ./... && go test -count=1 ./...` → PASS (ported write tests included, against the fake plane).
- [ ] **Step 3:** Add `goplane` integration `TestIntegrationServerWriteEndToEnd`: real `NewServerV2(cfg, goplane.Factory)` with a minted RS256 run token + `httptest` JWKS (claims exactly as the validator requires: `iss=datuplet-api`, `aud=datuplet-catalog`, `token_kind=run`, `sub=run_id=$RUN_ID`, fixture `project_id` and `warehouse`, exp +1 h; pattern: `git show 0d04dbc0:gateway/duckplane/server_integration_test.go`), write via HTTP Arrow IPC, `Commit`, assert rows via iceberg-go scan and snapshot summary `datuplet.run-id` + `datuplet.commit-key`. Run with `-tags=integration` → PASS.
- [ ] **Step 4:** Commit `feat(datagateway)!: write path on the Go data plane; remove the buffer/partition pipeline`.

### Task 5: Go table reader (`goplane`) — iceberg-go scan with per-file raw passthrough

**Model:** opus.

**Files:** Create `pkg/datagateway/goplane/{reader.go,eligibility.go}`, tests `pkg/datagateway/goplane/{reader_test.go,eligibility_test.go}`, extend `integration_test.go`. Modify `pkg/datagateway/lakekeeper/lakekeeper.go`: add `func (r *Resolver) LoadTableForScan(ctx context.Context, ns, tbl string) (*table.Table, *catalogwriter.VendedCreds, error)` (catalog `LoadTable` + the table's `VendedCreds`, built the way the write path builds them) — `*Resolver` satisfies `goplane.TableLoader` through a thin adapter in `goplane`. `goplane.Factory` builds a `Resolver` from `BootInfo` (`NewResolver(boot.LakekeeperURL, boot.Warehouse, boot.Token, boot.ProjectID)`) when `LakekeeperURL != ""`.

**Behaviour:** spec Read path 1, 2, 4, 5, 6 exactly. `OpenTableReader` loads the table, rebuilds it with `table.New(ident, meta, metadataLocation, fsF, cat)` where `fsF` builds IO from `CredsSource.Get` per call (spec Read path 6), plans once (`PlanFiles`, no filter, all columns), converts the current schema with `table.SchemaToArrowSchema(sc, nil, false, false)` (= `Schema()`), then emits parts in task order: an eligible task → `RawFile{Path, Rows: task.File.Count(), Size: task.File.FileSizeBytes(), Open}`; maximal runs of non-eligible tasks → one `Records` part over `Scan.ReadTasks(ctx, run)`. When `ReaderSpec.RawParquet` is false, the whole scan is one `Records` part. No snapshot / zero tasks → `NextPart` returns `io.EOF` immediately. Eligibility reads the footer through the table IO (`pqarrow` / `file.NewParquetReader` on the IO file's `ReaderAt`) once per task.

- [ ] **Step 1: Failing unit tests** (tables built in-test with iceberg-go on a local `file://` warehouse — an in-memory/sql catalog or `table.New` over metadata written by the test; data files written with Task 3's writer and committed with `AddFiles`): `TestEligibilityRules` table — clean file → raw; task with position deletes / equality deletes / deletion vector → decoded; column added after the file → decoded; column renamed → decoded; int8 column written by the golden writer → decoded; file with matching field ids → raw; file with a mismatching field id → decoded; `RawParquet=false` → one Records part. `TestReaderMixedPartsSchemaEqual` (some files deleted-from via `Transaction.Delete`, others clean → parts interleave in task order; every part's Arrow schema == `Schema()`; decoded rows exclude deleted rows; total = expected). `TestReaderEmptyTable` (no snapshot) and `TestReaderZeroFiles` (snapshot without data files) → `io.EOF` first. `TestRawOpenUsesCurrentCreds` (fake creds counting `Get`; `Open` after `Invalidate` uses the new creds). `TestReaderCloseClosesParts`. `TestReaderCtxDoesNotOwnReader`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.**
- [ ] **Step 4: Integration:** `TestIntegrationReadRawAndDecoded` — write two files with the writer + commit; delete rows from one with iceberg-go `Transaction.Delete` through the Lakekeeper catalog; open a reader with `RawParquet=true` → one raw part (clean file, bytes identical to the object) and one decoded part (rows minus deleted); `TestIntegrationReadAfterSchemaEvolution` (add a column via `UpdateSchema`, append a file with it → old file decoded with nulls for the new column, new file raw); `TestIntegrationReadCredsInvalidate`. Run with `-tags=integration` → PASS.
- [ ] **Step 5:** Root build/vet/test → PASS. Commit `feat(goplane): table reader on iceberg-go v0.7 scan with per-file raw passthrough`.

### Task 6: Rewire the server read path; delete the Go storage backends

**Model:** opus.

**Port:** root-module changes of DuckDB-branch commits `0d04dbc0` and `270014ad` (its Task 6 + review): `git diff 233f42cd 270014ad -- pkg/ | git apply -3`, then resolve. Not ported: `sdk/` (the SDK multi-batch change — spec Read path 8), `gateway/`, `go.mod`, `CLAUDE.md`/docs (Task 7). The port brings `read_serialize.go` + tests, the chunking `readerState` in `server_v2_reading.go`, HTTP/gRPC read handlers, GetSchema/GetSample `Unimplemented`, deletion of `pkg/datagateway/backend/`, `pkg/lib/controlfile`, `lakekeeper.LoadTableForRead` + backend construction, and the read tests in `server_v2_streaming_test.go`.

**Adapt to the part interface:** `OpenReader` → `plane.OpenTableReader(ReaderSpec{Namespace, Table, RawParquet: format == FORMAT_PARQUET})`. Chunk production: a `Raw` part is exactly one chunk (`is_last` decided by peeking the next part; rows = `RawFile.Rows`); HTTP `GET /data/read/{id}` streams `RawFile.Open` with `Content-Length` = `Size` and today's headers, no buffering; gRPC `ReadChunk` reads it fully into the message. A `Records` part is chunked by `chunk_size` and serialized by `encodeChunk`; a chunk never spans two parts. `FORMAT_ARROW_IPC` chunks hold exactly one record batch (concatenate the chunk's batches before `ipc.Writer.Write`; spec Read path 8). Empty table → one last zero-row chunk (`FORMAT_PARQUET`: zero-row file with `Schema()`). Reader errors are sticky (ported behaviour). Modify `.github/workflows/pr.yml`: the integration-tag test line runs `./pkg/datagateway/goplane/... ./pkg/datupleticeio/...` instead of `./pkg/datagateway/backend/...`.

- [ ] **Step 1: Tests** (fake plane; ported ones adapted plus): `TestReadRawPartStreamsFileBytes` (HTTP body == raw bytes, `Content-Length`, `X-Datuplet-Rows` = Rows, no full buffering — fake `Open` returns a reader that fails if read past a limit before headers are written), `TestReadMixedPartsChunking` (raw, records, raw → chunk sequence and `is_last` only on the final chunk), `TestReadArrowIPCOneBatchPerChunk` (a chunk built from 3 batches decodes to exactly one batch with all rows, via `sdk/go`'s reader), `TestReadGRPCRawPart`, `TestReadEmptyTableParquetZeroRowFile`, `TestReadPlaneErrorSurfaces`, `TestReadSurvivesOpenReaderCtxCancel`, `TestCloseReaderClosesTableReader`, `TestCancelInterruptsReaders`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement + deletions.**
- [ ] **Step 4:** Root `CGO_ENABLED=0 go build ./... && go vet ./... && go test -count=1 ./...` → PASS; `go test -count=1 -tags=integration ./pkg/datagateway/goplane/... ./pkg/datupleticeio/...` → PASS; add integration `TestIntegrationServerReadParquetEndToEnd` (real server + fixture as Task 4 Step 3: write, commit, delete rows from one file, HTTP `FORMAT_PARQUET` read → chunks staged to files, every chunk's Arrow schema equal, total rows = written − deleted).
- [ ] **Step 5:** Commit `feat(datagateway)!: read path on the Go data plane (raw passthrough + iceberg-go decode); remove Go storage backends`.

### Task 7: Listen-first boot gate (ported from the DuckDB branch) + its open findings

**Model:** sonnet.

**Port:** root-module changes of DuckDB-branch commit `35c4f621`: `git diff 35c4f621~1 35c4f621 -- pkg/datagateway | git apply -3` (brings `boot_gate.go`, `boot_gate_test.go` and the `config.go` / `server_v2.go` / `server_v2_http.go` hooks), then resolve. Its `pkg/k8s/controllers` hunks only revert `af6bad4c` (never ported) — skip them. The process logic of `git show 35c4f621:gateway/cmd/datagateway/main.go` (`run`, `boot`, `bootFailureHold`, signal registration before boot) and its tests (`main_test.go`) are the reference for rewriting `cmd/datuplet/gateway.go` — adapted, not copied as files.

**Behaviour:** spec "Boot" 1–4. `cmd/datuplet/gateway.go`: register SIGINT/SIGTERM first; bind gRPC on `--addr`, serve it through `BootGate.ServeGRPC`; load config; bind HTTP (`cfg.HTTPListenAddr()`), `BootGate.ServeDataHTTP`; `ApplyMemoryLimit`-free at this point (Task 8 adds it first thing); `NewServerV2(cfg, goplane.Factory)`; on error `gate.Fail(SanitizeBootError(err, runTokenPath))`, hold until signal / 10 min / serve error, exit 20; on success `gate.Ready(server)`, then wait for signal or serve error and `server.Close()`. New vs the DuckDB branch: the gate's gRPC health service starts `NOT_SERVING` and switches to `SERVING` in `Ready` (stays `NOT_SERVING` after `Fail`).

- [ ] **Step 1: Tests:** ported `boot_gate_test.go` adapted; add `TestHealthNotServingUntilReady` (health `Check` → `NOT_SERVING` before `Ready`, `SERVING` after; `NOT_SERVING` after `Fail`), `TestGateWaitReturnsOnCancel` (direct `wait(ctx)` with a cancelled ctx while the gate is closed returns `ctx.Err()` within 1 s) and `TestGateHTTPReturnsOnCancel` (in-process HTTP request whose ctx is cancelled while the gate is closed → handler returns, gate still closed). `cmd/datuplet` tests adapted from the reference `main_test.go`: boot failure is served as `Unavailable` / 503 without the token, then exit 20 after the (test-shortened) hold; SIGTERM during a blocked boot is handled once boot returns; a successful boot answers `GetConfig`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.**
- [ ] **Step 4:** Root build/vet/test → PASS; `go test -count=1 -tags=integration ./pkg/datagateway/goplane/...` → PASS (the end-to-end server tests now boot through the gate if they use the subcommand path; otherwise unchanged).
- [ ] **Step 5:** Commit `fix(gateway): listen before boot; gate RPCs until ready, serve boot errors (ported from the DuckDB branch)`.

### Task 8: Memory limit, docs, dead code

**Model:** sonnet.

**Files:** Create `pkg/datagateway/memlimit.go` + `memlimit_test.go`: `func ApplyMemoryLimit(getenv func(string) string, readFile func(string) ([]byte, error)) (limit int64, applied bool)` — spec Write path 9 (`GOMEMLIMIT` set → not applied; cgroup v2 `max` or unreadable → try v1; v1 values ≥ 2^62 mean unlimited; applied → `debug.SetMemoryLimit(limit*70/100)`), called first in `cmd/datuplet/gateway.go` with one log line. Modify docs: `CLAUDE.md` (project overview; joinStoragePath list; the `pkg/lib/datalake` vs `backend` bullet → `goplane`; replace the "Buffer package outputs Parquet only" bullet; rewrite the files.json bullet; CI contract line 1 integration packages; Key directories), `docs/architecture.md` (Data Gateway sidecar, data flow, credential flow, read path raw/decoded), `docs/known-limitations.md` (GCS unverified on the Go plane; partition fields ignored; external files unsupported; files.json removed; FULL_LOAD zero rows does not truncate; raw passthrough falls back to decode for non-canonical types), `docs/troubleshooting.md` (no files.json; read decode vs raw in logs), `docs/dependency-upgrades.md` (iceberg-go v0.7.0), `README.md` architecture-at-a-glance if it names deleted parts. Delete code `make lint` (`deadcode`) flags as orphaned by Tasks 4–6 (e.g. `pkg/icebergjob` manifest loader / `CommitTable` if unreferenced; schema helpers only the backend used) — nothing it does not flag unless a grep proves it unused.

- [ ] **Step 1:** `memlimit_test.go`: table over (`GOMEMLIMIT` set; v2 `1073741824`; v2 `max` + v1 `536870912`; v1 `9223372036854771712`; nothing readable) → expected (limit, applied). FAIL → implement → PASS.
- [ ] **Step 2:** `make lint` → list findings; delete orphaned code; update docs; `make lint` → clean except the pre-existing lakekeeper-chart golden drift if `main` has it (record evidence: same diff on `ecff5c5`).
- [ ] **Step 3:** Root build/vet/test → PASS. Commit `docs,chore: Go data plane docs, cgroup-derived GOMEMLIMIT, drop orphaned code`.

### Task 9: Live verification — e2e on OrbStack and performance

**Model:** controller runs it directly. No code changes except fixes that go back through a review loop.

- [ ] **Step 1: e2e.** `duckdb --version` (2.0.0) first. `make e2e-k8s` against OrbStack (isolated kubeconfig `/private/tmp/claude-501/-Users-tomaskacur-mydevel-datuplet/3d389a6c-eeb7-4104-b41d-80cf4ba3d039/scratchpad/orb.kubeconfig`; touch only `datuplet-e2e`). Expected: every scenario PASS except those that SKIP on `main` for environment reasons (list them with the reason). On failure collect `-c gateway` logs, report BLOCKED with evidence — do not patch tests.
- [ ] **Step 2: Performance** on the spike harness `/private/tmp/claude-501/-Users-tomaskacur-mydevel-datuplet/3d389a6c-eeb7-4104-b41d-80cf4ba3d039/scratchpad/perf` (`fixture/up.sh`, `baseline/run.sh` with `GW_IMPL=base|go`, `interleave3.sh`, `summarize3.py`, `read-interleave.sh`; `bin/datuplet-base` = `main` `ecff5c5`; `bin/gogateway` replaced by this branch's `go build -o bin/datuplet-go ./cmd/datuplet` run as `datuplet-go gateway` — add `GW_IMPL=allgo` for it). Write: L, interleaved base/allgo, warm-up + 3 → bars spec Goals 5; also one 3-run series with `GOMEMLIMIT=200MiB` (reported). Read: full `FORMAT_PARQUET` HTTP read (`readbench gw -format parquet -sink discard`) of a clean L table written by each side, interleaved, 3 runs → bar ≤ 1.25 × base; then delete ~1 % of rows from one file of the allgo table with iceberg-go and time the decoded read (reported, no bar).
- [ ] **Step 3: Report** `docs/superpowers/specs/2026-11-01-all-go-gateway-verification.md`: e2e summary, perf tables with commands and raw result paths, anything not verified (GCS, cgroup-limited run). Commit `docs: all-Go gateway verification report`.

---

## Final gate

Codex whole-branch review of `public/main...feat/all-go-gateway` with the spec, this plan and the Task 9 report. Critical/Important findings stop for the maintainer; approved fixes get a Codex re-review. Then push `feat/all-go-gateway` and open a **draft** PR (`gh pr create --draft`) whose body states: what replaced what, the ported-from-DuckDB-branch list, the measured numbers, and what is not verified.
