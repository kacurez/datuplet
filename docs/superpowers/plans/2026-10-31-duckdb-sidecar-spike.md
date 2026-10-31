# DuckDB-as-sidecar Spike Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decide, with measurements, whether an embedded DuckDB 2.0 (iceberg + httpfs, locked, optionally serving Quack) can replace the Data Gateway's data plane — writes, commits, reads — at the same extraction speed and memory as today, with acceptable feature parity and lock-in.

**Architecture:** Everything runs on the macOS host against a local Lakekeeper v0.12.1 (authn disabled, STS vending on) + Postgres 16 + silo (the chart's pins), started as `spk-*` Docker containers. The **baseline** is the unmodified `datuplet gateway` binary and the unmodified `http-json-extractor` binary built from `main` (`ecff5c5`), booted with a stub JWKS and a minted run token. The **candidate** is a throwaway Go program (`spikes/duckdb-sidecar/sidecar`) embedding duckdb-go v2.20000.0, which ATTACHes Lakekeeper writable with a bearer token and accepts data three ways (A: the gateway's own gRPC + HTTP Arrow IPC surface, so the unmodified extractor works; B: files in a shared directory + a templated INSERT; C: a Quack client). A deterministic JSON HTTP server feeds every run the same records.

**Tech stack:** Go, duckdb-go/v2 v2.20000.0 (`-tags duckdb_arrow`), DuckDB 2.0.0 CLI for verification, Docker (OrbStack), `/usr/bin/time -l`.

**Spec:** none — this is a spike; the Questions section below is the spec. Inputs: the 2026-10-31 architecture discussion (DuckDB in the sidecar, components talk to it over loopback; HTTP Arrow contract vs SQL/Quack contract) and the DuckDB 2.0 upgrade spike report `docs/superpowers/specs/2026-10-28-duckdb-2-spike-report.md` (lockdown posture, ATTACH-by-URI, vended credentials).

**Output:** `docs/superpowers/specs/2026-10-31-duckdb-sidecar-spike-report.md` (one section per task, the controller writes the verdict) plus raw results under `spikes/duckdb-sidecar/results/`.

## Global Constraints

- **No git commits, no push.** The maintainer's global rule; all work stays in the working tree on branch `spike/duckdb-sidecar`. The controller offers a commit at the end.
- **No platform code changes.** Only `spikes/duckdb-sidecar/**` (its own Go module, `module github.com/datuplet/datuplet/spikes/duckdb-sidecar`, never added to `make tidy`) and the report file. Baseline binaries are built from the tree as-is: `go build -o spikes/duckdb-sidecar/bin/datuplet ./cmd/datuplet` and `go build -C components/http-json-extractor -o $PWD/spikes/duckdb-sidecar/bin/http-json-extractor .`.
- **Pins:** duckdb-go/v2 `v2.20000.0`; DuckDB CLI 2.0.0 at `/private/tmp/claude-501/-Users-tomaskacur-mydevel-datuplet/3d389a6c-eeb7-4104-b41d-80cf4ba3d039/scratchpad/review/scratch-b6/dl/duckdb` (run with `-no-agent`); images `postgres:16`, `quay.io/lakekeeper/catalog:v0.12.1`, `docker.io/pgsty/silo:RELEASE.2026-10-26T00-00-00Z@sha256:7bb2aff6b89efe1cb1303c10fc21d579447434ed06649ac9ec05a5e2dbf30ad7`.
- **Fixture:** container names `spk-pg`, `spk-lk`, `spk-silo`; host ports in one file `spikes/duckdb-sidecar/fixture/env.sh`; Lakekeeper bootstrap + warehouse creation copied from `components/queryengine/integration_test.go:395-440` (storage profile `s3-compat`, `sts-enabled: true`, `path-style-access: true`). Cleanup is `docker rm -f spk-pg spk-lk spk-silo` and deleting under `spikes/duckdb-sidecar/results/` only. Never `rm -rf` anything else.
- **Measurement protocol:** host otherwise idle; tasks run strictly one at a time; per configuration 1 warm-up run (discarded) + 3 measured runs, median reported with min/max; every run writes a fresh table name; record wall time (harness), max RSS and user+sys CPU per process (`/usr/bin/time -l`), rows, data files, bytes, snapshots (from the catalog via the DuckDB CLI), parquet codec and row-group count. Raw output per run as one JSON line in `results/<task>/<config>.jsonl`. A number in the report without its command and raw file is not a result.
- **Datasets** (fixture server, seed 42, flat scalar records, 12 fields: `id` int64, `uuid` string, `created_at` ISO-8601 string, `category` one of 20 strings, `amount` float, `quantity` int, `flag` bool, `note` string 30 % null, `text` 80-200 chars, `lat` float, `lon` float, `code` 6-char string; offset pagination, 50 000 rows per page): **S** = 200 000 rows, **L** = 2 000 000 rows, **XL** = 10 000 000 rows (memory-boundedness only: baseline + best candidate, 1 run each).
- **Time boxes:** a task that hits its box reports BLOCKED or DONE_WITH_CONCERNS with what it has; the controller rules.

## Questions (the spike's acceptance criteria)

Each answer in the report is **Yes / No / Partly** + evidence (command, raw file, Lakekeeper request log excerpt).

- **Q1 Auth and credentials.** Can a DuckDB 2.0 engine in a Go process ATTACH Lakekeeper *writable* with a bearer token, receive vended S3 credentials, and CREATE/INSERT? What breaks with the token secret dropped after ATTACH (the query-worker hardening)? (Lakekeeper authn is disabled locally, so token *validation* and OpenFGA are out of scope and listed as not covered.)
- **Q2 Write semantics.** (a) APPEND: N INSERTs in one transaction → how many snapshots, data files, REST commits; file size control. (b) FULL_LOAD: which statement form replaces a table atomically in one snapshot (readers never see an empty table). (c) MERGE INTO upsert on a key, v2 vs v3 delete files. (d) Two writers appending to one table concurrently: retried, failed, or lost/duplicated rows? (e) Can a commit carry an idempotency key (snapshot summary property)? If not, what does a retried pod do? (f) One transaction across two tables: atomic via `/v1/transactions/commit`? (g) Schema: create-from-data, appending a batch with a new column, type mismatch, declared columns. (h) Zero-row output commits as success.
- **Q3 Read semantics.** (a) Full and filtered scans of the L table inside the sidecar vs the gateway's HTTP read path: time, RSS, bytes fetched. (b) Incremental read "rows added after snapshot X" with APPEND_ONLY vs REQUIRES_FULL_REFRESH detection — native function or metadata-driven file list?
- **Q4 Extraction performance (the maintainer's priority).** For S and L: baseline vs candidates A, B, C. **Pass:** best candidate median wall ≤ 1.10 × baseline on L, and summed peak RSS (component + sidecar) ≤ 1.25 × baseline on L, and peak RSS on XL ≤ 1.5 × its L value (bounded, not proportional to data).
- **Q5 Footprint.** Sidecar idle RSS, ATTACH-to-ready latency, binary + extensions size vs today's gateway binary.
- **Q6 Quack and lockdown.** Served on loopback with a token and an authorization macro: can component-supplied SQL (i) reach the network outward, (ii) read the run-token file or anything outside the shared directory, (iii) read vended credentials (`duckdb_secrets()`, error texts), (iv) write a table outside its declared outputs although the Lakekeeper token is project-wide? Does the authorization callback see enough (statement text only, or bound targets) to enforce (iv)?
- **Q7 Feature parity.** Every gateway feature in the matrix below marked **native**, **wrapper code** (Go in the sidecar, estimated LOC), or **gap** (with workaround or none).
- **Q8 Lock-in.** What becomes DuckDB-specific under each contract (HTTP Arrow IPC unchanged vs SQL/Quack), what stays portable (Iceberg tables, Lakekeeper), and what a reversal would cost.

Parity matrix rows (Q7): GetConfig + `$[name]` secrets; run-token JWT validation at boot; cancel annotation watch; Log RPC; OpenWriter formats (Arrow IPC, Parquet, JSON, CSV); schema inference from first chunk; declared columns (`TableOutputConfig.columns`); write-time TransformSpec (filter/select/drop/rename/compute/cast/reorder) and `drop` processors; output bucket/table allowlist; APPEND / FULL_LOAD; target file size / row-group size / buffering; partition router; files.json breadcrumb; ExternalDataFile commits; Commit barrier + per-table results (snapshot id, files, rows, bytes); 409 retry; idempotency key; zero-path success; OpenReader (formats, chunking, read-time transforms); incremental reads + DeltaInfo; GetSchema / GetSample; S3 and GCS (WIF + service-account key) backends; metrics and profiling.

## Review Focus

1. **A rigged comparison.** Baseline and candidates must use the same records, page size, catalog, storage, codec and host; any difference (e.g. the gateway's 128 MiB target file size vs DuckDB's defaults) is recorded next to the number.
2. **Commit counting.** "One snapshot per run" must be proven from `iceberg_snapshots()` and the Lakekeeper request log, not inferred.
3. **Concurrent-writer correctness.** Q2d must count rows after both writers finish; "no error" is not "no loss".
4. **Memory measured at the right place.** Candidate A's RSS includes the Arrow decode and DuckDB buffers in one process; the baseline's is gateway + extractor. Report both per-process and summed.
5. **The authorization claim.** Q6(iv) needs a real attempted write to an undeclared table, not a reading of the docs.

---

### Task 1: Harness — fixture, JSON server, measurement, verifier

**Model:** sonnet · **Time box:** 2 h

**Files (create):** `spikes/duckdb-sidecar/go.mod`; `fixture/env.sh`, `fixture/up.sh`, `fixture/down.sh`, `fixture/bootstrap.sh` (or a Go `cmd/bootstrap`); `cmd/jsonsrv/main.go`; `measure/run.sh`; `measure/verify.sh`; `results/.gitkeep`.

**Produces (later tasks rely on):**
- `fixture/up.sh` → containers healthy, project + warehouse `spk` created, prints `LK_CATALOG_URL`, `LK_WAREHOUSE`, `LK_PROJECT_ID`, `S3_ENDPOINT` into `fixture/.state.env`.
- `cmd/jsonsrv` → `GET /records?$offset=&$limit=` returning a JSON array; flags `-rows`, `-seed`, `-addr`; identical bytes for identical offset/limit/seed.
- `measure/run.sh <task> <config> <run-id> -- <cmd…>` → runs under `/usr/bin/time -l`, appends one JSON line (wall_s, max_rss_bytes, user_s, sys_s, exit) to `results/<task>/<config>.jsonl`.
- `measure/verify.sh <namespace> <table>` → DuckDB CLI 2.0.0 attaches the local catalog and prints JSON: row count, snapshot count + operations (`iceberg_snapshots`), data-file count + total bytes (`iceberg_metadata`), parquet codec and row groups of one data file.

- [ ] Step 1: write the files; `fixture/up.sh` then `fixture/bootstrap.sh`; prove with the DuckDB CLI: `CREATE TABLE lk.probe.t AS SELECT 1 AS x` then `measure/verify.sh probe t` → 1 row, 1 snapshot.
- [ ] Step 2: `go run ./cmd/jsonsrv -rows 200000` and fetch page 3 twice → identical sha256.
- [ ] Step 3: report `## Harness` in the report file: versions, ports, the proof outputs.

### Task 2: Baseline — today's gateway + extractor, locally

**Model:** opus · **Time box:** 3 h

**Files (create):** `spikes/duckdb-sidecar/cmd/jwks-stub/main.go` (generates an RS256 key, serves JWKS, writes a run token with `iss=datuplet-api`, `aud=datuplet-catalog`, `token_kind=run`, `sub=run_id=$RUN_ID`, `project_id`, `warehouse`, 24 h expiry — claims as validated in `pkg/datagateway/runtoken*` and `pkg/datagateway/jwks`); `baseline/gateway.yaml`; `baseline/run.sh`.

**Consumes:** Task 1 outputs. Reads (does not modify) `cmd/datuplet/gateway.go`, `pkg/datagateway/config.go`, `pkg/datagateway/server_v2*.go`, `components/http-json-extractor/main.go`, `cmd/mock-datagateway/example-nyc311.json` (config shape).

- [ ] Step 1: boot `bin/datuplet gateway` on the host in the mode the cluster uses, pointing at the local Lakekeeper, the stub JWKS and the minted token, output bucket `raw`, write mode APPEND; run `bin/http-json-extractor` against `jsonsrv` (S) and confirm with `verify.sh` that 200 000 rows landed. Use only flags, env and config files; if boot needs a code change, stop and report BLOCKED with the exact failing check.
- [ ] Step 2: measure S and L (warm-up + 3 each) and XL (1); record per-process RSS/CPU for gateway and extractor, wall, rows, files, bytes, snapshots, codec, row groups, and the gateway's effective buffer / target-file settings.
- [ ] Step 3: report `## Baseline` (table + commands + raw files + anything surprising).

### Task 3: Candidate core + write semantics (Q1, Q2, Q5)

**Model:** opus · **Time box:** 4 h

**Files (create):** `spikes/duckdb-sidecar/internal/engine/engine.go` (open DuckDB, INSTALL/LOAD iceberg + httpfs into `spikes/duckdb-sidecar/.ext`, secret, ATTACH writable by URI, optional lockdown); `cmd/lkproxy/main.go` (reverse proxy in front of Lakekeeper logging method, path, status and a body summary per request to `results/<task>/lk-requests.jsonl`); `cmd/semantics/main.go` (runs each Q2 scenario, prints a JSON verdict per scenario).

**Consumes:** Task 1. Reference for ATTACH/lockdown statements: `components/queryengine/attach.go`, `components/queryengine/engine_open.go` (copy, do not import).

- [ ] Step 1: Q1 — ATTACH writable through `lkproxy`; CREATE + INSERT; then repeat with the secret dropped after ATTACH and record what still works.
- [ ] Step 2: Q2 a–h as separate scenarios against fresh tables, each with its snapshot list, file list and the Lakekeeper requests it caused. Q2d runs two OS processes, 10 × 50 000-row INSERT commits each, then counts rows. Q2e: try `snapshot summary` / table-property routes and a crash-after-commit retry (kill -9 after COMMIT returns, rerun) and count duplicates.
- [ ] Step 3: Q5 — idle RSS after ATTACH, ATTACH-to-ready latency (median of 5), binary size, extension sizes.
- [ ] Step 4: report `## Write semantics` and `## Footprint`.

### Task 4: Extraction candidates A, B, C (Q4)

**Model:** opus · **Time box:** 5 h

**Files (create):** `spikes/duckdb-sidecar/cmd/sidecar/main.go` — candidate A serves the gateway's gRPC (GetConfig, OpenWriter, CloseWriter, Commit, Log, Shutdown) and HTTP write endpoint exactly as the unmodified extractor and `sdk/go` expect (start from `cmd/mock-datagateway/main.go`; copy it), decoding each Arrow IPC payload and inserting it into the attached table; candidate B adds `POST /ingest {output, file}` that runs a templated `INSERT INTO <declared output> SELECT * FROM read_json(<shared-dir file>)`; `cmd/ndjson-client/main.go` (fetches the same pages, writes each page file to the shared directory, calls `/ingest`, then `/commit`); `cmd/quack-client/main.go` (candidate C: duckdb-go with the quack extension, loads pages locally, `INSERT INTO` the sidecar over Quack — first establish whether the server exposes its attached Iceberg catalog to Quack clients; if not, record the workaround used).

**Consumes:** Task 3 `internal/engine`; Task 1 harness; Task 2 baseline numbers.

- [ ] Step 1: correctness first — for each candidate on S, `verify.sh` shows 200 000 rows, the column types, one snapshot per Commit (or the number observed, explained).
- [ ] Step 2: tuning sweep on L, one run each, for the knobs that matter: transaction scope (one per writer vs per chunk), Arrow-view INSERT vs appender into a local temp table + one `INSERT … SELECT` at Commit, `memory_limit` (256 MiB / 1 GiB), `threads`, `preserve_insertion_order=false`, per-page vs batched files for B. Pick the best configuration per candidate and say why.
- [ ] Step 3: measure the best configuration of A, B, C on S and L (warm-up + 3) and the best overall on XL (1). Same table as the baseline, summed and per-process RSS.
- [ ] Step 4: qualitative check for B: land the same records with one column kept as raw JSON into a VARIANT column of a `format-version = 3` table; report whether Lakekeeper accepts it and whether the DuckDB CLI reads it back.
- [ ] Step 5: report `## Extraction performance` with the Q4 pass/fail line computed from the medians.

### Task 5: Read semantics (Q3)

**Model:** sonnet · **Time box:** 2 h

**Files (create):** `spikes/duckdb-sidecar/cmd/readbench/main.go` (baseline: `sdk/go` client against the baseline gateway, `OpenReader` the L table as Parquet over HTTP and drain it; candidate: the same table scanned inside the engine — `count(*)`, a full `SELECT *` into a discarding sink, and a 1 % selective filter on `category`/`created_at`).

- [ ] Step 1: measure both paths (warm-up + 3): wall, RSS, bytes fetched from silo (silo request log or `lkproxy` + S3 access counts).
- [ ] Step 2: incremental — append two batches to a copy of the L table; produce "rows added after snapshot X" in the engine (native function if one exists, else a file list from `iceberg_metadata`/manifests read with `read_parquet`); then run the baseline gateway's incremental `OpenReader` for the same X and compare row counts and the APPEND_ONLY vs REQUIRES_FULL_REFRESH classification after a FULL_LOAD in the range.
- [ ] Step 3: report `## Read semantics`.

### Task 6: Quack and lockdown (Q6)

**Model:** opus · **Time box:** 3 h

**Files (create):** `spikes/duckdb-sidecar/cmd/quackprobe/main.go` and the sidecar's Quack mode (`quack_serve` on `127.0.0.1`, per-run token, an authorization macro that admits only `INSERT INTO <declared outputs> …` and `SELECT`), plus the lockdown posture from `components/queryengine/engine_open.go` with the shared directory as the only allowed path.

- [ ] Step 1: probe matrix through Quack with the correct token: outbound `ATTACH`/`CONNECT`/`read_csv('http…')`/`httpfs` URL reads; `read_text` of the run-token path and of `/etc/hosts`; `duckdb_secrets()` and error-text leakage of vended keys; `CREATE SECRET`; `SET` anything; INSERT into an undeclared table in the same namespace and in another namespace; `CREATE TABLE`/`DROP TABLE` in the catalog. Each row: statement, outcome, error text, and a loopback-listener connection count (pattern: `components/queryengine/lockdown_quack_test.go`).
- [ ] Step 2: the same matrix with a wrong token, and with the authorization macro removed (to show what the macro is carrying).
- [ ] Step 3: report what the authorization callback receives (statement text, parsed, or bound) and `## Quack and lockdown`.

### Task 7: Verdict (controller, in-session)

- [ ] Step 1: fill `## Parity matrix` (Q7) from Tasks 2-6 and the code: each row native / wrapper (LOC estimate) / gap.
- [ ] Step 2: `## Lock-in` (Q8) and `## Verdict`: one of GO (DuckDB data plane behind the unchanged HTTP Arrow contract), GO with contract change (SQL/Quack), NO-GO — with the Q4 pass line, the open risks, and what a production design would still have to prove (authn + OpenFGA on a cluster, GCS, multi-replica concurrency).
- [ ] Step 3: one Codex adversarial review of the report against `spikes/duckdb-sidecar/results/` (claims vs raw numbers, rigged-comparison check); findings presented to the maintainer, not auto-fixed.
