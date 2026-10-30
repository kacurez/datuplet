# DuckDB 2.0 upgrade — design

**Date:** 2026-10-28
**Status:** rev 5 (after the spike, three Codex rounds and `/code-review max` — see "Revision notes" at the end)
**Risk tier (REVIEW.md):** High — `.github/workflows/e2e.yml` is a CODEOWNERS
path, and `components/queryengine` lockdown is a sandbox-posture path.
Before merge: green PR CI, `/code-review max`, `/security-review` on the
lockdown change, `make lint`, the OpenFGA integration test, `make e2e-k8s`
on OrbStack.

## Intent

Move Datuplet's three embedded-DuckDB binaries from DuckDB 1.5.3
(`duckdb-go/v2 v2.10503.0`) to DuckDB 2.0.0 LTS (`duckdb-go/v2 v2.20000.0`),
keep the query-worker sandbox posture provably closed against the new
attack surface 2.0 adds (the Quack remote protocol), and tell sql-transform
and ad-hoc-query users which SQL dialect changes they will see. Nothing else.

**Why now.** 2.0.0 is an LTS line (supported to at least Oct 2027); 1.4 LTS,
which the e2e CLI still pins, reaches end of life on 2026-11-17; 1.5 is
extended to March 2027 but is not where fixes land. The feature that pays
for the move is asynchronous I/O for Parquet reads from object storage,
which is precisely the query-worker's and `datuplet-query`'s path (Iceberg
scan straight from S3/GCS with lakekeeper-vended credentials). The move is
otherwise a currency bump on an experimental 0.x project.

## Facts the design rests on (verified 2026-10-28)

| Fact | Evidence |
|------|----------|
| DuckDB 2.0.0 LTS released 2026-10-21; first patch (2.0.1) expected 2026-11-18; the team advises mission-critical users to wait for it | duckdb.org announcement + release calendar |
| Go client `github.com/duckdb/duckdb-go/v2 v2.20000.0` is on the proxy; requires `go 1.24`, `arrow-go/v18 v18.5.1` (we have 18.6.0), bindings `v0.20000.0`; upstream still ships `arrow.go` behind the `duckdb_arrow` tag | `go list -m -versions`, `go mod download -json`, duckdb-go tag v2.20000.0 |
| Static libs now resolve from the `go.duckdb.org/libduckdb/<platform>` module path, not GitHub; GOPROXY-restricted builds must allow it | duckdb-go-bindings README + v0.20000.0 release notes |
| Bindings still use C API v1 ("Existing calls keep their signatures"); C API v2 ships but is unstable; DuckDB v2 no longer self-loads statically linked extensions, the bindings register them at init | duckdb-go-bindings README |
| `iceberg` + `httpfs` extensions are published for v2.0.0 on linux_amd64, linux_arm64, osx_arm64 | `curl -I extensions.duckdb.org/v2.0.0/...` → 200 |
| Iceberg ATTACH: `URI` is canonical, `ENDPOINT` a deprecated alias; `ACCESS_DELEGATION_MODE` unchanged | duckdb.org iceberg options page |
| Quack client entry points: `ATTACH 'quack:host[:port]' AS x (TOKEN '…' \| TYPE quack \| DISABLE_SSL true)`, `CONNECT <alias>` / `CONNECT '<connection string>'` (also `postgres:`/`mysql:` strings), `DISCONNECT`, `FROM quack_query('quack:host', 'SQL', token = '…')`, `FROM <alias>.query('SQL')`, `CREATE SECRET (TYPE quack, TOKEN '…', SCOPE 'quack:host')`; default port 9494; the extension is "transparently autoinstalled and autoloaded on first use" | duckdb.org quack overview, CONNECT statement, quack extension page |
| Async I/O knobs: `async_threads` (default 4×threads, cap 256), `read_ahead_depth` (default -1 = bounded by `memory_limit`), `http_client_pool_capacity` | duckdb.org data/io page |
| Breaking SQL changes: `~`/`!~` partial match, `!` precedence, single-arrow lambda disabled, `//` and `%` by zero raise, untyped NULL keeps NULL type, `X''`/`B''` literals typed, MERGE multi-match raises, S3 URL params disabled, `custom_profiling_settings`/`enable_http_logging` removed, `preserve_identifier_case` is VARCHAR | duckdb.org migration guide |
| Storage format v69 unreadable by <2.0 — **irrelevant here**: no `.duckdb` file persists; every engine is in-memory + spill dir | migration guide |
| CLI 2.0.0 assets renamed `duckdb-cli-linux-amd64.tar.gz` (was `duckdb_cli-linux-amd64.zip`); CLI agent mode auto-activates only when an AI-agent env var is set **and** stdout is not a TTY **and** no `-json`/`-csv` flag was passed; `DUCKDB_AGENT_MODE=0` forces it off | GitHub release assets; duckdb.org agent-mode page |
| Shipped SQL (`examples/pipelines`, e2e manifests) contains no single-arrow lambdas, `~` regex operators or factorials | grep |

## Current state

DuckDB appears in exactly these places:

| Where | Pin | Role |
|-------|-----|------|
| `components/sql-transform/` (own module) | duckdb-go v2.10503.0 | User SQL over disk-staged inputs. **Real input path (`io.go`):** `OpenReader(FORMAT_PARQUET)`, then chunked `GET`s on the gateway's HTTP `/data/read/{readerID}` endpoint streamed with `io.Copy` to one local parquet file per chunk, then `CREATE VIEW <name> AS SELECT * FROM read_parquet([...])`. Outputs: `COPY <name> TO '<staged>.parquet'` then SDK writer. **Does not use duckdb-go's Arrow API**; the `duckdb_arrow` build tag is a repo convention. Resource settings are component config: `memory` (default 1GB), `max_temp_size` (6GB), `threads` (2), `temp_directory` (/tmp/duckdb-spill) → `SET memory_limit / max_temp_directory_size / threads / temp_directory`. No unit tests; proven by e2e `multi-table-join` + `TestBigDataJoinProof`. |
| `components/queryengine/` (own module) | duckdb-go v2.10503.0 | query-worker Pod + `datuplet-query` CLI. `attachCatalog`: `INSTALL/LOAD iceberg, httpfs` → `CREATE SECRET lk_tok (TYPE ICEBERG, TOKEN …)` → `ATTACH '<project>/<warehouse>' AS lk (TYPE ICEBERG, ENDPOINT …, SECRET lk_tok, ACCESS_DELEGATION_MODE 'vended_credentials')` → `USE lk."<first namespace>"`; then `lock()` (RFC 022 §6.2). Tests: `-tags duckdb_arrow` (CGO) unit suite incl. `TestLockdownDeny` (probe table: `name, stmts, denyStmt, wantFSLayer, note`; helpers `lockedEngine` cold and `lockedEngineViaAttach` production-warmed); `-tags 'duckdb_arrow integration'` `TestAttachIntegration` (host-network lakekeeper/MinIO/postgres via Docker; namespace fixture `qe`) which also calls `runLockdownDenyIntegration`. |
| `utils/docker/install-duckdb-ext/` (own module) | duckdb-go v2.10503.0 | Build-time baker: `INSTALL/LOAD iceberg, httpfs` into `$HOME/.duckdb/extensions/<version>/<platform>/`, COPY-ed into the query-worker + datuplet-query images so a locked engine's `INSTALL` is a local no-op. **Today nothing executes DuckDB inside the final image at build time**; the baked dir is only listed. |
| `.github/workflows/e2e.yml` | CLI v1.4.1 (`duckdb_cli-…zip`) | `tests/e2e/framework/assertions.go` runs `duckdb -json -c …` to read committed Iceberg tables. |
| Dev laptops | homebrew CLI v1.4.1 | Same assertions locally. |

**Lockdown facts the design must respect** (from `lockdown_deny_test.go`):
- `SELECT * FROM duckdb_extensions()` is a *documented deny* post-lock (it
  reads the extension dir through the disabled LocalFileSystem). Any
  extension inventory therefore runs **before** `lock()`.
- The matrix harness accepts *any* error for a row unless `wantFSLayer`
  is set. A row whose only proof is "it errored" proves nothing about the
  layer.
- HTTP egress post-lock (`read_parquet('http://…')`) and `CREATE SECRET`
  post-lock are **intentionally permitted** and contained by the
  NetworkPolicy (known limitation on non-enforcing CNIs). A remote
  `.duckdb` attached over httpfs belongs to that same permitted class; it
  is not a Quack path and this PR does not change its status.

**CI coverage gap that shapes the plan:** `pr.yml` runs `go vet/test ./...`
on the **root** module only. The component modules are never unit-tested in
PR CI; they are exercised only by `e2e.yml` (image builds + `TestQuery_*` +
`multi-table-join`). So every code task runs the component module's own
tests locally with `-tags duckdb_arrow`, and the lockdown matrix runs
locally before the branch is pushed.

**Stale docs observed:** CLAUDE.md ("materialized into a DuckDB table …
workaround for duckdb-go's GROUP BY / JOIN …"), `main.go`'s own header
("stream-writes each input's Arrow batches to a temp parquet file") and
`components/sql-transform/README.md` (`arrow_scan` / `Arrow.RegisterView` /
"no parquet files are written to the component's local disk", in the
"Inputs → DuckDB views" list, "Empty-input semantics" and "Architecture"
item 1) all describe designs that `io.go` no longer implements. Fixed in
passing (D10) with wording derived from `io.go`.

## Goals

1. All three modules on `duckdb-go/v2 v2.20000.0`; `make tidy` clean;
   images build, and the build itself proves `INSTALL/LOAD iceberg, httpfs`
   succeed offline in the final image layout (D12).
2. query-worker lockdown posture **proven** against 2.0: existing deny
   matrix green; new rows for every documented Quack entry point, each with
   an explicit layer marker and a loopback listener proving no socket was
   opened; a pre-lock pinned list of statically linked extensions.
3. ATTACH uses the canonical `URI`; everything else about the lakekeeper
   handshake (project-qualified warehouse, `USE` workaround) unchanged.
4. e2e CLI pinned to 2.0.0 so CI reads our iceberg-go-written tables with
   the 2.0 reader too; agent mode explicitly off.
5. Users told what changed: CHANGELOG entry + a "DuckDB engine" section in
   the sql-transform README (version, the existing resource knobs, the
   dialect changes) + a pointer in the ad-hoc-query doc.
6. Full `make e2e-k8s` green on OrbStack; the untyped-NULL output probe and
   a query-worker RSS baseline recorded; all five PR CI jobs green.

## Non-goals

- Quack server mode anywhere (replacing the per-request sandbox engine with
  a shared server, or `datuplet-query` as a thin Quack client).
- VARIANT columns end to end (needs Iceberg v3 variant through iceberg-go
  and the Data Gateway).
- DuckLake, Delta, `DEFAULT_TABLE_LOCATION_FROM_NAMESPACE`,
  `SUPPORT_NESTED_NAMESPACES`, `DEFAULT_SCHEMA` (the namespace is only known
  *after* ATTACH; `Request` carries none — the `USE` workaround stays).
- Changing sql-transform's I/O design or its resource knobs.
- New tuning knobs for async I/O (D9).
- Renaming/removing the `duckdb_arrow` build tag.
- Changing the status of the permitted egress classes (HTTP reads, secret
  creation, httpfs remote attach) — containment stays with the NetworkPolicy.
- Bumping the root module or app-worker (they do not link DuckDB).

## Anti-goals (what makes this a failure even if green)

- A deny-matrix row weakened to make 2.0 pass, or a new row that passes on
  "some error" without naming its layer.
- A green e2e with the DuckDB CLI step silently skipping. The CLI install
  must `duckdb --version` and assertions must run.
- Secrets in logs: the catalog JWT stays inside `CREATE SECRET`; the ATTACH
  rewrite must not move it into the statement text or an error wrap.
- A commit on the branch with red tests in a touched module (CLAUDE.md:
  build + test before every commit). A root-only `go mod tidy`.

## Design decisions

### D1. Pins
`github.com/duckdb/duckdb-go/v2 v2.20000.0` in `components/sql-transform`,
`components/queryengine`, `utils/docker/install-duckdb-ext`. `make tidy`
settles the indirect `duckdb-go-bindings v0.20000.0` + `go.duckdb.org/libduckdb/*`
lines. No `replace`. Go directives stay `1.25.7`.

### D2. Ship on 2.0.0; maintainer may hold for 2.0.1
The PR is built and e2e-proven on 2.0.0. The "wait for the first patch"
advice applies to mission-critical production; Datuplet is experimental
0.x. The maintainer decides at merge time: merge now, or hold the draft PR
until 2026-11-18 and bump the three pins to `v2.20001.0` (one `make tidy`).
Renovate's weekly gomod group carries later patches. The CHANGELOG names
the exact engine version.

### D3. sql-transform: I/O design unchanged; "zero code change" is an assumption to prove
The chunked-parquet stage + `read_parquet` views stay (D3 rationale: the
in-memory `CREATE TABLE AS` was dropped for OOM reasons; a one-shot stream
cannot be scanned twice regardless of engine). 2.0's async I/O also applies
to local parquet reads. Settings and their config knobs are unchanged. The
expected code delta is zero beyond `go.mod`/`go.sum`, **but that is proven,
not assumed**: by the spike build, by the e2e join scenarios on the
cluster, and by the untyped-NULL output probe (a `SELECT NULL AS x` output
column now has type `NULL`; the Data Gateway / iceberg-go must either
accept it or fail as a *user* error). If the probe fails as
FailedApplication, the maintainer decides between a known-limitations
entry and a cast in the component; it is not patched silently.

### D4. ATTACH on 2.0
- `ENDPOINT '<url>'` → `URI '<url>'` (canonical; alias deprecated). Same
  escaping, same position, JWT stays in the SECRET.
- The project-qualified warehouse string (`<project-id>/<warehouse>`,
  Spike 0.1 §1), `ACCESS_DELEGATION_MODE 'vended_credentials'` and the
  post-attach `USE lk."<first-namespace>"` workaround (Spike 0.1 §4) stay.
  The `duckdb_schemas()`/`database_name` assumption is re-verified by
  `TestAttachIntegration`, whose fixture gains a second namespace
  `a-hyphen` (one table `probe`, one row) that sorts before `qe`, so the
  quoted-identifier `USE` path is the one production takes. Because the
  current schema then becomes `a-hyphen`, the existing bare `posts`
  assertion is replaced by bare `probe` (= 1) and `"a-hyphen".probe`
  (= 1); the fully-qualified and two-part `qe.posts` (= 7) assertions and
  the post-lock positive control stay.

### D5. Lockdown posture vs the Quack surface
2.0 adds a remote protocol whose extension autoloads on first use. The
posture already disables autoinstall/autoload/community extensions and
locks configuration, so Quack is reachable only if the Go bindings
**statically link** it. Two tests pin this:

**Inventory (pre-lock).** `TestStaticallyLinkedExtensions` opens a fresh
engine with `openEngine` only (no `lock()`, because `duckdb_extensions()`
is a documented post-lock deny) and reads
`SELECT extension_name FROM duckdb_extensions() WHERE install_mode = 'STATICALLY_LINKED' ORDER BY 1`.
It asserts the **exact** static set the spike measured on 2.0.0 —
`autocomplete, core_functions, icu, json, parquet, tpcds, tpch` — so any
growth or shrink on a later bindings bump fails loudly, and separately
asserts `quack`, `httpfs`, `iceberg`, `postgres_scanner`, `mysql_scanner`
absent from that set (the canonical row names; no row is called
`postgres` or `mysql`, so those literals would pass vacuously). It logs
the whole list. `install_mode` distinguishes linked-in extensions from
ones merely installed on the developer's machine.

`tpch` and `tpcds` are new in 2.0.0 (1.5.3 linked
`autocomplete, core_functions, icu, json, parquet`) and their functions
(`dbgen`, `dsdgen`, `tpch_queries()`, `tpcds_queries()`) are reachable
post-lock. They write into the CURRENT catalog, which on the production
engine is the attached lakekeeper catalog `lk` (rev 5: the earlier
"in-memory, no file or network reach" premise was measured on an
unattached engine). The catalog is attached `READ_ONLY`, so against `lk`
they fail before any catalog write request; aimed at the `memory` catalog they
still generate data in RAM, bounded by `memory_limit`,
`max_temp_directory_size` and the request timeout. A statically linked
extension has no runtime off-switch, so removing them would need a custom
`libduckdb` build (out of scope). The test comment and
`docs/known-limitations.md` say so; `TestLockdownDenyAttachedReadOnly`
proves the refusal.

**Deny rows (post-lock), with explicit layer predicates.** A new
`TestLockdownDenyQuack` (own file, own `quackProbe{name, stmt, class, subject}`
table so the existing matrix stays untouched) requires every row to error
**and** the error to satisfy its row's *deny class* — a predicate that
names the layer, the same role `wantFSLayer` plays for the FS layer.
Matching the protocol name alone is not a proof (a parser error quoting
`quack_query`, or a credential error mentioning Quack, would pass). Four
classes; the "must match" phrases are the verbatim 2.0.0 texts the spike
recorded (`## Quack probe (warmed engine)`):

| Class | Must match (case-insensitive) | Must NOT match (reject list, every class) |
|-------|-------------------------------|-------------------------------------------|
| `fsLayer` | `LocalFileSystem has been disabled` | `connection refused`, `could not connect`, `failed to connect`, `connection reset`, `timed out`, `unauthori[sz]ed`, `invalid token`, `parser error`, `syntax error` |
| `extensionUnavailable` | the subject name **and** `but it exists in the <subject> extension` (DuckDB's not-loaded / autoload-off text) | same |
| `secretTypeAbsent` | `secret type 'quack' does not exist` | same |
| `configLocked` | `the configuration has been locked` | same |

The reject list names transport, credential and parse failures by their
specific phrases; a bare `connect` is deliberately absent, because a
diagnostic may echo the offending `CONNECT` statement (2.0 appends a
`LINE 1: …` context line to some errors) without any socket having been
opened — the listener, not a word, is the network proof.

Why `fsLayer` exists: for the four prefix-routed statements below, DuckDB
maps the `quack:` / `postgres:` prefix to its extension and attempts an
**explicit load of the extension file from the local extension directory,
even with `autoload_known_extensions=false`**; on the locked engine that
file read is what the disabled LocalFileSystem refuses, and the error
does not name the extension. The deny is two-layered (not linked, and the
load path is closed); the class names the layer that actually fires. A
companion pre-lock subtest `layer-naming` runs the same four statements
on an extension-posture-only engine (FS enabled, no `lock()`, no
`disabled_filesystems`, `extension_directory` pointed at an empty temp
dir so a developer's locally installed extensions cannot change the
outcome, cwd moved to a temp dir) and pins the underlying text
`Extension "quack" is an existing extension` /
`Extension "postgres_scanner" is an existing extension`, so the layer
behind the FS deny is named rather than inferred; it also asserts the
cwd stayed file-free and the listener saw no connection.

Rows:

| Statement | Class |
|-----------|-------|
| `ATTACH 'quack:127.0.0.1:<port>' AS q (TOKEN 'x')` | fsLayer (prefix-routed extension load refused; `layer-naming` pins `quack`) |
| `ATTACH 'quack:127.0.0.1:<port>' AS q (TYPE quack)` | fsLayer (same) |
| `CONNECT 'quack:127.0.0.1:<port>'` | fsLayer (same) |
| `CONNECT 'postgres:host=127.0.0.1 port=<port>'` | fsLayer (same; `layer-naming` pins `postgres_scanner`) |
| `FROM quack_query('quack:127.0.0.1:<port>', 'SELECT 1', token = 'x')` | extensionUnavailable (`quack`) |
| `CREATE SECRET qs (TYPE quack, TOKEN 'x', SCOPE 'quack:127.0.0.1')` | secretTypeAbsent — secret creation itself stays permitted (lock-exempt manager); the proof is that the *type* is unregistered |
| `SET s3_enable_deprecated_url_parameters = true` (warmed engine only; the setting exists only with httpfs loaded) | configLocked |

Every row runs on **both** the cold `lockedEngine` and the
production-warmed `lockedEngineViaAttach` (httpfs loaded, secret manager
initialised, as in production). Each statement runs under a 10 s context
timeout. `<port>` is a loopback `*net.TCPListener` on `127.0.0.1:0` owned
by the subtest. There is **no accept goroutine**: the proof is a
**drain barrier**. After each statement returns, the test calls
`drain(200 ms)` — `SetDeadline(now+200 ms)`, then `Accept` in a loop
(count and close each connection) until the deadline error — and
requires the count to be 0. A TCP handshake completes in the kernel
without any `Accept` call, so a connection DuckDB opened during the
statement is sitting in the accept queue when the statement returns and
the drain accepts it; closing the listener first (the rev-3 design)
could discard that backlog. A negative-control subtest dials the listener
itself and then runs the same `drain`, asserting one accepted connection,
proving the barrier sees queued connections. The warmed engine is
mandatory at the gate: `lockedEngineViaAttach` itself (not a separate
preflight) turns its iceberg/httpfs-unavailable `t.Skipf` into `t.Fatalf`
when `QE_REQUIRE_WARMED=1`, and Task 8 runs the whole queryengine suite
with it set and `-v`, asserting zero `--- SKIP` lines, so warmed coverage
can never be lost silently. The spike found neither `quack` nor
`postgres_scanner` linked; if a later bump links either, the inventory
test and the rows fail together and the finding is escalated to the
maintainer before any posture change (the only way the `lock()` list may
grow).

### D6. Error-text assertions
The PEG parser and the ICU rewrite may reword errors. Policy: assert on
**layer markers** (`LocalFileSystem` / `disabled`; `wantMarker` words like
`quack`, `lock`), as `fsLayerMarker` already does, and update verbatim
text only where a row genuinely matched on prose. A row that stops
erroring is a hole, not a reword.

### D7. e2e CLI → 2.0.0
`e2e.yml`: `DUCKDB_VERSION=v2.0.0`, asset `duckdb-cli-linux-amd64.tar.gz`
(tar, not unzip), `set -euo pipefail` + `curl -f` + `duckdb --version` kept
as the fail-closed check. `assertions.go`: exec env adds
`DUCKDB_AGENT_MODE=0` (defensive; `-json` already prevents
auto-activation). Dev laptops on homebrew 1.4.1 keep working — the
assertion SQL is plain `SELECT`/`COUNT`/`DESCRIBE` over `iceberg_scan`.

### D8. User-facing dialect changes are documented, not shimmed
No `SET lambda_syntax`, `regex_match_operator_semantics`,
`error_on_division_by_zero` or `legacy_parser` shims in either engine:
shims drift and 2.1 removes the flags. Instead:
- `CHANGELOG.md` `[Unreleased] → Changed`: engine version + the dialect
  changes, one line each, with the migration-guide link.
- `components/sql-transform/README.md`: new "DuckDB engine" section:
  version; the **existing** config knobs `memory`, `max_temp_size`,
  `threads`, `temp_directory` with their defaults (they are overridable
  per pipeline — the rev-1 claim that they were not was wrong); the
  dialect list.
- `docs/ad-hoc-query.md`: one paragraph pointing at the same list.

### D9. Async I/O: defaults, no new knobs
`read_ahead_depth=-1` is bounded by `memory_limit`, which both engines set;
`async_threads` defaults to 4× `threads` (8 for the shipped `threads=2`).
Nothing is tuned in this PR. The cluster task records query-worker Pod
RSS during a representative query as a baseline. A known-limitations line
notes that async read-ahead shares the `memory_limit` budget.

### D10. Docs drift fixed in passing (wording derived from `io.go`)
- CLAUDE.md sql-transform bullet: inputs arrive as parquet chunks over the
  gateway's HTTP read endpoint, are streamed to local temp files and
  exposed as `read_parquet` views (bounded RAM).
- `components/sql-transform/README.md`: the "Inputs → DuckDB views"
  paragraph + its 3-step list, the "Empty-input semantics" paragraph and
  "Architecture" item 1 rewritten to the same design. The empty-input
  paragraph states the real semantics: with no data files the component
  registers a zero-row placeholder view (`SELECT * FROM (SELECT 1) WHERE 1=0`)
  that does **not** carry the input's columns, so `count(*)` works but SQL
  naming a column of an empty input fails at bind time.
- `main.go`'s header items 1 **and** 2 (both still describe Arrow IPC)
  get the same correction.

### D11. Phase 0 spike gates the code tasks and commits nothing but its report
The spike bumps the pins in the working tree, builds all three modules,
runs the queryengine unit suite (recording every failure and classifying
it), runs the pre-lock inventory and a manual post-lock `CONNECT` probe
on the warmed engine (to learn the real error text for D5's markers),
runs `TestAttachIntegration` unmodified and with `URI`, builds the images.
It then **reverts the pins** and commits only
`docs/superpowers/specs/2026-10-28-duckdb-2-spike-report.md`. The real
bump lands in the next task together with the adaptations, as one green
commit. Code tasks start only when the report shows a green build and no
deny row *succeeding*; a succeeding row is escalated first.

### D12. Offline extension gate executed inside the image build
Both `query-worker.Dockerfile` and `datuplet-query.Dockerfile` gain a
`verify` stage: `FROM runtime AS verify` (the runtime stage is named),
`COPY --from=builder /install-duckdb-ext /ext-check` (the baker is now
`go build`-ed in the builder, not only `go run`), `USER datuplet`,
`ENV HOME=/home/datuplet`, `RUN --network=none /ext-check && touch /tmp/ext-verified`
(`/tmp`, because the runtime user owns only its home and `/scratch`);
the final stage does `COPY --from=verify /tmp/ext-verified /tmp/ext-verified`
so the verify stage always runs (BuildKit may serve it from cache when
its inputs are unchanged, which is correct). The baker executes the exact runtime
sequence (`INSTALL/LOAD iceberg, httpfs`) under the final image's user,
HOME, libc and extension-dir layout with no network: a missing or
mis-pathed baked extension fails the build instead of the first query.
The baker binary is still not in the final image (it lives in `verify`).
`RUN --network=none` is BuildKit syntax; the Makefile already exports
`DOCKER_BUILDKIT=1`.

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| 2.0.0 regression before 2.0.1 | D2 hold option; LTS line; full OrbStack e2e before PR; Renovate patch flow after. |
| Static libs from `go.duckdb.org` blocked in some build env | Default GOPROXY handles any module path; Dockerfiles use the default proxy. Spike image builds prove it. Documented in dependency-upgrades.md. |
| PEG parser rewords error text → brittle tests | D6 marker policy. |
| `quack` statically linked → new egress path from the sandbox | D5 inventory + listener-backed rows; escalate, never quietly patch `lock()`. |
| Component modules untested in PR CI | Every task runs `go test -tags duckdb_arrow ./...` in the touched module; lockdown matrix run before push; e2e builds images in CI. |
| Baked extension path changes (`…/v2.0.0/…`) | D12 executes the runtime INSTALL/LOAD offline at build time in both images. |
| ICU now an extension; timezone functions in user SQL | Spike: `icu` is statically linked and loaded on 2.0.0 (as on 1.5.3); the inventory pins it; the README says no `LOAD icu` is needed. |
| Untyped NULL output columns | D3 probe on the cluster; maintainer decision if it fails hard. |
| Homebrew CLI on laptops stays 1.4.x | Assertions need only `iceberg_scan` + `SELECT`; both versions read iceberg-go output. |

## Verification matrix

| Check | Where | Command |
|-------|-------|---------|
| Builds | each module | `CGO_ENABLED=1 go build -tags duckdb_arrow ./...` |
| queryengine unit + lockdown | local | `QE_REQUIRE_WARMED=1 CGO_ENABLED=1 go test -tags duckdb_arrow -count=1 -v ./...` in `components/queryengine`, zero `--- SKIP` lines (the gate form); without the variable on a laptop that cannot install iceberg/httpfs |
| queryengine integration | local, Docker | `go test -tags 'duckdb_arrow integration' -count=1 -run TestAttachIntegration ./...` (also runs `runLockdownDenyIntegration`); the fixture's server and `mc` images are the chart's pinned `pgsty/silo` (MinIO's community images are gone; pre-existing break fixed in Task 2) |
| Images + offline INSTALL | local Docker | `make build-component-sql-transform build-component-datuplet-query` and the query-worker build; the `verify` stage is the gate |
| Tidy / lint | repo | `make tidy && git diff --exit-code`, `make lint` |
| OpenFGA integration | local, Docker | `go test -tags=integration -count=1 ./pkg/pipelineapi/authz/...` (CLAUDE.md: not in CI) |
| Cluster | OrbStack | `make e2e-k8s` (full suite, tears down on success), then `make deploy-local` + `datuplet pipeline put` / `datuplet trigger` for the baseline run, the NULL probe (its own output table `etl.null_probe`, so the gateway creates a schema from the NULL-bearing output instead of reusing `etl.user_summary`'s) and the RSS baseline, then `make undeploy-local`. `make k8s-retry-duckdb` is **not** used: it `kubectl apply`s an envelope-free PipelineDoc and cannot work (pre-existing, out of scope). |
| CI | GitHub | all 5 `pr.yml` jobs + `e2e.yml` (required by the main ruleset) |

## Left to the builder

- The exact `wantMarker` words, taken from the spike's recorded error text
  (2.0.0 texts: `LocalFileSystem has been disabled`, `but it exists in the quack extension`, `secret type 'quack' does not exist`, `the configuration has been locked`).
- The hyphenated namespace fixture's name and table in `integration_test.go`
  (must sort before `qe`).
- Any assertion update in `engine_test.go` (the "status result set for
  CREATE..AS" empirical note) if 2.0 changes the shape.

## Follow-ups (not in this PR)

- `SUPPORT_NESTED_NAMESPACES` once lakekeeper nested namespaces are used.
- VARIANT end to end (Iceberg v3 variant in iceberg-go + Data Gateway).
- Quack-based shared query server, only if the per-request sandbox cost
  becomes the console's bottleneck — a design of its own.

## Revision notes

rev 2 (2026-10-28, after Codex review of rev 1): inventory moved pre-lock
(`duckdb_extensions()` is a post-lock deny); Quack rows now carry explicit
markers, run on the warmed engine and are backed by a loopback listener,
and cover `TYPE quack`, `quack_query`, quack secrets and
`CONNECT 'postgres:…'`; httpfs remote attach classified as pre-existing
permitted egress; offline extension check now executes DuckDB at build
time (D12); spike commits only its report, the bump lands green (D11);
final gate adds the OpenFGA integration test; `DEFAULT_SCHEMA` dropped
(namespace unknown before ATTACH); sql-transform knobs documented as
overridable; I/O wording derived from `io.go` (FORMAT_PARQUET over HTTP,
not Arrow IPC); cluster verification split into the `e2e-k8s` gate and a
kept `deploy-local` stack for probes.

rev 3 (2026-10-28, after Codex review of rev 2: 5 Important / 1 Nit):
Quack rows now assert a deny *class* predicate (extension unavailable /
secret type absent / config locked) with a fixed reject list, the listener
is drained with a WaitGroup, every statement is time-bounded, a
negative-control subtest proves the detector, and `QE_REQUIRE_WARMED=1`
makes the warmed run mandatory at the gate; the integration fixture
replaces the bare `posts` assertion with bare `probe` and seeds one row;
the kept-stack run uses the `datuplet` CLI (the Make retry target is
pre-existing broken); D12's marker lives in `/tmp`; the NULL probe writes
its own `etl.null_probe` table; `main.go` header items 1 and 2 and the
README empty-input paragraph now state the placeholder-view semantics.

rev 4 (2026-10-29, after the spike — commit 324d979 — and Codex review of
rev 3: 4 Important / 1 Nit): the four prefix-routed Quack rows deny at the
FS layer on 2.0 (prefix-routed explicit extension-file load refused by
`disabled_filesystems`), so D5 gains a `fsLayer` class and a pre-lock
`layer-naming` subtest that pins the extension name; the listener is a
deadline **drain barrier** run after every statement (the rev-3
close-then-wait could discard the kernel accept backlog); the reject list
drops bare `connect`; `lockedEngineViaAttach` itself enforces
`QE_REQUIRE_WARMED=1` and the gate asserts zero `--- SKIP` lines; the
inventory pins the exact 2.0.0 static set (`tpch`/`tpcds` newly linked,
accepted as present-by-design) under the canonical `postgres_scanner` /
`mysql_scanner` names; `icu` is linked, so no `LOAD icu` note; the
integration fixture's dead MinIO images are swapped for the chart's
`pgsty/silo` pin; cluster queries use `datuplet query --sql`; the README
`UNION ALL` example is withdrawn (it cannot supply columns the first
branch fails to bind). REVIEW.md's three-round cap applies: no fourth
doc round; the harness is judged as code in Task 3's review, and the
"is the approach wrong?" question is put to the maintainer in the PR.

rev 5 (2026-10-29, after `/code-review max`): two review findings on the
attached, locked engine. `CALL dbgen()` / `dsdgen()` create their tables
in the current catalog — the attached `lk` — and issued stage-create
POSTs to lakekeeper (the in-memory premise in D5 was measured on an
unattached engine); and 2.0's iceberg catalog-mutating table functions
(`iceberg_rollback_to_snapshot`, `set_iceberg_table_properties`) reached
`/v1/transactions/commit`, a metadata-only write the read-only vended
credentials cannot stop. Decision: `attachCatalog` attaches the catalog
`READ_ONLY` — the ad-hoc query surface is read-only by product
definition, so this is defence in depth and lakekeeper's FGA grant stays
the authorization of record. A new `TestLockdownDenyAttachedReadOnly`
runs the deny matrix against an in-process fake REST catalog that accepts
every commit (a leak would succeed), with a negative control that drops
`READ_ONLY` and must see the write reach the catalog; the integration
write probe is now a hard assertion (INSERT / UPDATE / DELETE /
`iceberg_rewrite_data_files` refused by the read-only attach); those
integration assertions are compile-checked only and await a live run.

rev 5 addendum (2026-10-29): `lock_configuration` does not block ATTACH,
so post-lock user SQL could ATTACH the catalog a second time, without
`READ_ONLY`, reusing the stored bearer secret `lk_tok`, and write through
it (`POST /v1/namespaces`, `/v1/transactions/commit`, stage-create).
`attachCatalog` now runs `DROP SECRET lk_tok` once the ATTACH (and `USE`)
succeeded, before `lock()`; the attached catalog keeps its own credential
state, and the fake catalog's `Authorization` headers prove post-drop
reads still carry the bearer. Residual, out of scope: a user holding their
own valid catalog JWT can attach it writable, but they already have that
lakekeeper access and FGA governs it; real lakekeeper rejects
unauthenticated clients. Wording narrowed to what the tests prove: DuckDB
rejects the listed statements before any catalog write request — binding
may still read catalog metadata and storage. The "exact" static set in D5
and in the rev 4 note is exact only over the rows `duckdb_extensions()`
lists: `libduckdb` v0.20000.0 links and registers nine components, and the
two capability-style ones, `duckdb_httplib` and
`duckdb_loadable_extensions`, never appear there and no API exposes them,
so `TestStaticallyLinkedExtensions` cannot pin them.
