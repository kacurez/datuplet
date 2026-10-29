# DuckDB 2.0 Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Revision 4** — after the spike (Task 1, commit `324d979`) and Codex round 3 (4 Important, 1 Nit): the four prefix-routed Quack rows deny at the FS layer on 2.0 and get a `fsLayer` class plus a pre-lock `layer-naming` subtest; the listener is a deadline **drain barrier** (no goroutine) run after every statement; `lockedEngineViaAttach` itself enforces `QE_REQUIRE_WARMED=1`; the reject list drops bare `connect`; the inventory pins the exact 2.0.0 static set (`tpch`/`tpcds` accepted as present-by-design) and the canonical `postgres_scanner`/`mysql_scanner` names; Task 2 swaps the integration fixture's dead MinIO images for the chart's `pgsty/silo` pin; `icu` is linked, so the README `LOAD icu` note is dropped; Task 7 uses `datuplet query --sql`; the README `UNION ALL` example is removed; Task 8 asserts zero SKIPs. REVIEW.md's three-round cap: no fourth doc round — the harness is judged as code in Task 3's review and the approach question goes to the maintainer in the PR.

**Revision 3** — rev 1 was reviewed by Codex (7 Important, 3 Nit): the spike commits only its report and the bump lands green in Task 2; the extension inventory runs pre-lock; a Docker `verify` stage executes the offline `INSTALL/LOAD` inside the image build (Task 4); the cluster task keeps a stack alive for the probes; the final gate adds the OpenFGA integration test; `DEFAULT_SCHEMA` is dropped; docs wording comes from `io.go` and documents the existing resource knobs. Rev 2 was reviewed again (5 Important, 1 Nit): the Quack rows now assert a deny-class predicate with a reject list, the listener is drained and a negative control proves it, `QE_REQUIRE_WARMED=1` makes the warmed run mandatory at the gate; the integration fixture replaces the bare `posts` assertion with bare `probe` and seeds one row; the kept-stack run uses the `datuplet` CLI (not the broken `make k8s-retry-duckdb`); the NULL probe writes its own `etl.null_probe` table; `main.go` header items 1 and 2 are both corrected and the README states the empty-input placeholder semantics.

**Goal:** Move `components/sql-transform`, `components/queryengine` and `utils/docker/install-duckdb-ext` from DuckDB 1.5.3 to DuckDB 2.0.0 LTS, keep the query-worker lockdown provably closed against 2.0's Quack surface, make the image build prove offline extension loading, pin the e2e CLI to 2.0.0, and document the SQL dialect changes users will see.

**Architecture:** A version bump in three Go modules, gated by a spike that records what 2.0 actually changed before any adaptation is written. The query-worker's RFC 022 posture (`lock()` in `engine_open.go`) is extended with a pre-lock statically-linked-extension pin and a post-lock Quack deny matrix with layer markers and a loopback listener. Both DuckDB images gain a `verify` stage that runs the extension baker with `--network=none` under the runtime user. No I/O design changes in sql-transform; dialect changes are documented, never shimmed.

**Tech Stack:** Go 1.25.7 (CGO, `-tags duckdb_arrow`), `github.com/duckdb/duckdb-go/v2 v2.20000.0`, Docker BuildKit (`RUN --network=none`, host-network integration tests), OrbStack k8s for e2e, Codex (via `codex-companion.mjs`) as the reviewer.

**Spec:** `docs/superpowers/specs/2026-10-28-duckdb-2-upgrade-design.md` (rev 4) — the plan argues from it; executors read both.

## Execution model

Subagent-driven: one fresh subagent per task, on the shared feature
branch `feat/duckdb-2.0` (in-place checkout, remote `public`), with the
model below. After every task the orchestrator runs a Codex review of
that task's commit(s) and presents Important findings to the maintainer
before any fix is dispatched (CLAUDE.md "Code reviews + plan workflow").
Minors go to the ledger rolled up at the final gate.

| # | Task | Model | Why |
|---|------|-------|-----|
| 1 | Spike: build, test deltas, inventory, probes, images (report only) | **opus** | Unknowns to judge, nothing to patch. |
| 2 | Pins + queryengine adaptation, one green commit | **sonnet** | Bounded by the spike report; careful with the lakekeeper handshake and the integration fixture. |
| 3 | Lockdown hardening for the Quack surface | **opus** | Security posture; a weak row is the anti-goal. |
| 4 | Docker `verify` stage (offline INSTALL/LOAD at build time) | **sonnet** | Two Dockerfiles + baker build; must reason about stage inheritance. |
| 5 | e2e CLI 2.0.0 + agent mode off | **haiku** | Mechanical, two files. |
| 6 | CHANGELOG + docs (dialect note, knobs, I/O wording) | **haiku** | Exact copy given below. |
| 7 | OrbStack: `e2e-k8s` gate, kept `deploy-local` stack for NULL probe + RSS baseline | **opus** | Real-cluster debugging if anything is red. |
| 8 | Final gate (orchestrator, in-session) | — | `make lint`, OpenFGA integration test, `/code-review max`, `/security-review`, Codex whole-branch, push, draft PR. |

Codex per-task review call (orchestrator, from the repo root; `<base>` is
the commit the task started from):

```bash
node "$HOME/.claude/plugins/cache/openai-codex/codex/1.0.6/scripts/codex-companion.mjs" adversarial-review --wait --base <base> --scope branch "Task N brief: <one paragraph from this plan>. Implementer report: <report path>. Review per REVIEW.md: cite file:line, label Important/Nit/Pre-existing, say plainly if nothing needs fixing."
```

## Global Constraints

- Pin is exactly `github.com/duckdb/duckdb-go/v2 v2.20000.0` in all three modules; indirect `github.com/duckdb/duckdb-go-bindings v0.20000.0` and `go.duckdb.org/libduckdb/{darwin-amd64,darwin-arm64,linux-amd64,linux-arm64,windows-amd64} v0.20000.0` are whatever `make tidy` writes. No `replace`. `go` directives stay `1.25.7`.
- Never a bare `go mod tidy`; always `make tidy` from the repo root, then `git diff --exit-code` on every `go.mod`/`go.sum` outside the three modules (they must not move).
- Every build/test in a component module runs with `CGO_ENABLED=1 go <build|test> -tags duckdb_arrow` from that module's directory; PR CI does not run these, so they run here.
- **Every commit is green**: `go build` + the touched module's test suite pass before `git commit` (CLAUDE.md). No "fix it in the next task".
- Deny rows are never weakened, and a new deny row always names its layer (`wantFSLayer` or a `denyClass`). A row that stops erroring on 2.0, or a Quack row whose listener receives a connection, is escalated to the maintainer, not patched.
- `duckdb_extensions()` is never queried on a locked engine (documented deny at `lockdown_deny_test.go:350`); the inventory runs on an `openEngine`-only engine.
- The catalog JWT lives only inside `CREATE SECRET`; it never appears in the ATTACH statement text or in any error wrap.
- No compatibility shims: no `SET lambda_syntax`, `regex_match_operator_semantics`, `error_on_division_by_zero`, `legacy_parser`, `enable_legacy_parser` anywhere. No new async-I/O settings (`async_threads`, `read_ahead_depth`, `http_client_pool_capacity`).
- No `DEFAULT_SCHEMA`; the `USE lk."<first namespace>"` workaround stays.
- Commit message prefixes per history (`chore(deps):`, `fix(queryengine):`, `test(queryengine):`, `build(docker):`, `ci(e2e):`, `docs:`), each ending with the attribution trailer the session supplies.
- Nothing is pushed and no PR is opened before Task 8.

## Review Focus

Inputs the spec implies but no existing test exercises; each is pinned to
the task that owns the code.

1. **Any Quack entry point on a locked engine opens no socket** — the loopback listener's drain barrier in Task 3 must count zero connections after every row, on both the cold and the warmed engine.
2. **A hyphenated namespace that sorts first** must resolve bare and two-part names after ATTACH (the `USE lk."<schema>"` quoting path). → Task 2 integration fixture.
3. **`INSTALL/LOAD iceberg, httpfs` inside the final image with no network** must succeed under the runtime user and HOME. → Task 4 `verify` stage; a mis-baked image fails the build.
4. **An untyped `NULL` output column in sql-transform** (`SELECT NULL AS x`) now has type `NULL`; the Data Gateway must commit it or fail as FailedUser, never FailedApplication. → Task 7 probe; a hard failure is a maintainer decision recorded in the report, not patched.
5. **The e2e CLI step fails closed**: a 404 on the renamed asset must fail the job at install time. → Task 5 keeps `set -euo pipefail` + `curl -f` + `duckdb --version`.

---

### Task 1: Spike — build, test deltas, inventory, probes, images (report only)

**Model:** opus

**Files:**
- Create: `docs/superpowers/specs/2026-10-28-duckdb-2-spike-report.md`
- Working-tree only, reverted before commit: the six `go.mod`/`go.sum` under `components/sql-transform`, `components/queryengine`, `utils/docker/install-duckdb-ext`; `components/queryengine/attach.go` (URI probe); temporary `components/queryengine/zz_spike_test.go`

**Interfaces:**
- Produces: the spike report with these exact headings, consumed by Tasks 2, 3, 4, 6: `## Build`, `## Unit tests (queryengine)`, `## Deny matrix`, `## Statically linked extensions`, `## Quack probe (warmed engine)`, `## ATTACH URI`, `## Images`, `## Findings for Task 2`, `## Findings for Task 3`, `## Findings for Task 6`, `## Escalations`.

- [ ] **Step 1: Bump the pins in the working tree**

In each of the three `go.mod` files set `github.com/duckdb/duckdb-go/v2 v2.20000.0`. From the repo root:

Run: `make tidy && git status --short`
Expected: only the six `go.mod`/`go.sum` under the three modules changed; `git diff --exit-code go.mod go.sum sdk/go/go.mod sdk/go/arrow/go.mod tests/e2e/go.mod components/data-generator/go.mod components/http-json-extractor/go.mod` exits 0. Record under `## Build`.

- [ ] **Step 2: Build all three modules**

Run, in each module directory: `CGO_ENABLED=1 go build -tags duckdb_arrow ./...`
Expected: exit 0 for all three. Record compiler output (or "clean") under `## Build`.

- [ ] **Step 3: queryengine unit tests — record, classify, fix nothing**

Run: `cd components/queryengine && CGO_ENABLED=1 go test -tags duckdb_arrow -count=1 ./... 2>&1 | tee /tmp/qe-2.0.txt`
Under `## Unit tests (queryengine)` list every failing test with its assertion message and one classification: *error-text reword*, *result-shape change*, *behaviour change*. Under `## Deny matrix` list `TestLockdownDeny` rows that FAILED because the statement **succeeded** (none expected) separately from rows that failed only on the layer marker. Any row whose statement succeeded goes under `## Escalations` — write the report (Step 8) and stop; the orchestrator escalates.

- [ ] **Step 4: Pre-lock inventory of statically linked extensions**

Create `components/queryengine/zz_spike_test.go` (`//go:build duckdb_arrow`) with `TestZZSpikeInventory`: `openEngine(ctx, Request{TempDir: t.TempDir()})`, **no** `lock()`, then
`SELECT extension_name, loaded, installed, install_mode FROM duckdb_extensions() ORDER BY 1`
and `t.Log` every row. Run `-run TestZZSpikeInventory -v`. Put the rows under `## Statically linked extensions` as a table and state explicitly, for each of `quack`, `httpfs`, `iceberg`, `postgres`, `mysql`, `json`, `parquet`, `icu`, `core_functions`, whether `install_mode = 'STATICALLY_LINKED'` and `loaded = true`.

- [ ] **Step 5: Quack probe on the warmed, locked engine**

In the same temp file add `TestZZSpikeQuack`: `e, _ := lockedEngineViaAttach(t)` (it skips if iceberg/httpfs cannot be installed — note that in the report if so), then run each of these and `t.Logf("%s → %v", stmt, err)`:
`ATTACH 'quack:127.0.0.1:1' AS q (TOKEN 'x')`, `ATTACH 'quack:127.0.0.1:1' AS q (TYPE quack)`, `CONNECT 'quack:127.0.0.1:1'`, `CONNECT 'postgres:host=127.0.0.1 port=1'`, `SELECT * FROM quack_query('quack:127.0.0.1:1', 'SELECT 1', token = 'x')`, `CREATE SECRET qs (TYPE quack, TOKEN 'x', SCOPE 'quack:127.0.0.1')`, `SET s3_enable_deprecated_url_parameters = true`.
Record the verbatim error text of each under `## Quack probe (warmed engine)`; a statement that **succeeds** goes under `## Escalations`.

- [ ] **Step 6: ATTACH URI probe (Docker required)**

Run: `cd components/queryengine && CGO_ENABLED=1 go test -tags 'duckdb_arrow integration' -count=1 -run TestAttachIntegration ./... 2>&1 | tee /tmp/qe-int-2.0.txt`
Expected with the unmodified `ENDPOINT` statement: PASS (record any deprecation warning). Then change `attach.go`'s `ENDPOINT '%s'` to `URI '%s'`, re-run, record PASS/FAIL under `## ATTACH URI`, then `git checkout components/queryengine/attach.go`. If Docker is unavailable, write "not run — Docker unavailable" and why; Task 2 must then run it.

- [ ] **Step 7: Images**

Run from the repo root: `make build-component-sql-transform build-component-datuplet-query && docker build -t datuplet/query-worker:latest -f utils/docker/query-worker.Dockerfile .`
Expected: three images; the query-worker build log's `find /root/.duckdb` output lists `extensions/v2.0.0/linux_<arch>/iceberg.duckdb_extension` and `httpfs.duckdb_extension`. Record the paths under `## Images`.

- [ ] **Step 8: Write the report, revert, commit the report only**

Fill `## Findings for Task 2` (every change `attach.go`/tests need: URI result, reworded strings, result shapes, the `duckdb_schemas()` column name on 2.0), `## Findings for Task 3` (the static set; the recorded error text per Quack statement → the marker word each row should assert; whether `quack`/`postgres` are statically linked), `## Findings for Task 6` (whether `icu` is statically linked — the README must say `LOAD icu` is needed if not). Delete `zz_spike_test.go`. Then:

```bash
git checkout components/sql-transform/go.mod components/sql-transform/go.sum components/queryengine/go.mod components/queryengine/go.sum utils/docker/install-duckdb-ext/go.mod utils/docker/install-duckdb-ext/go.sum
git status --short   # expect only the report
git add docs/superpowers/specs/2026-10-28-duckdb-2-spike-report.md
git commit -m "docs: DuckDB 2.0 spike report (build, test deltas, extension inventory, Quack probes)"
```

---

### Task 2: Pins + queryengine adaptation (one green commit)

**Model:** sonnet

**Files:**
- Modify: `components/sql-transform/go.mod`, `go.sum`; `components/queryengine/go.mod`, `go.sum`; `utils/docker/install-duckdb-ext/go.mod`, `go.sum`
- Modify: `components/queryengine/attach.go:117-158` (ATTACH statement; comments naming 1.5.3)
- Modify: `components/queryengine/engine_test.go:60-66` only if the report lists a result-shape change
- Modify: `components/queryengine/lockdown_deny_test.go` only for rows the report classifies *error-text reword* (marker-based matching, spec D6)
- Modify: `components/queryengine/lockdown_deny_integration_test.go:43` comment if `iceberg_schema_properties` changed
- Modify: `components/queryengine/integration_test.go` (namespace fixture near line 65; fixture images at lines 237 and 355)

**Interfaces:**
- Consumes: spike report `## Findings for Task 2`, `## ATTACH URI`.
- Produces: `attachCatalog(ctx, e, r)` unchanged signature; ATTACH text `ATTACH '<warehouse>' AS lk (TYPE ICEBERG, URI '<url>', SECRET lk_tok, ACCESS_DELEGATION_MODE 'vended_credentials')`; the `USE` block unchanged. Task 3 adds new test files only, so keep `lockedEngine`, `lockedEngineViaAttach`, `fsLayerMarker` and the `probe` struct as they are.

- [ ] **Step 1: Apply the pins**

Set `github.com/duckdb/duckdb-go/v2 v2.20000.0` in the three `go.mod`; run `make tidy`; verify as in Task 1 Step 1.

- [ ] **Step 2: URI**

Edit `attach.go`: `ENDPOINT '%s'` → `URI '%s'`; update the comment citing "v0.12.1 + DuckDB 1.5.3 iceberg ext" to "lakekeeper v0.12.1 + DuckDB 2.0.0 iceberg ext (spike report 2026-10-28)"; refresh the `duckdb_schemas()` comment ("in this DuckDB build (1.5.3)") to what the report says 2.0 exposes.

- [ ] **Step 3: Apply reword/shape deltas from the report**

For each item in `## Findings for Task 2`: a reworded error → widen the marker to a layer word, never a pass; a changed status-result shape in `engine_test.go` → update the exact-shape assertion and its version comment. Add no `SET`.

- [ ] **Step 4: Hyphenated-namespace fixture (Review Focus 2)**

In `integration_test.go` add constants `probeNamespace = "a-hyphen"`, `probeTable = "probe"`, `probeRowCount = 1`, and provision that namespace + table the same way `seedNamespace`/`seedTable` are provisioned, seeding **one** row with the same `(id, title)` schema (a one-row record, not `buildSeedRecord`, which emits `seedRowCount` = 7 rows). Because `a-hyphen` sorts before `qe`, `attachCatalog`'s `USE` now selects `lk."a-hyphen"`, so change the assertions after attach to:
- (a) `SELECT count(*) FROM lk."qe"."posts"` = 7 — unchanged;
- (b) `SELECT count(*) FROM "qe".posts` = 7 — unchanged;
- (c) `SELECT count(*) FROM probe` = 1 — **replaces** the bare `posts` assertion at `integration_test.go:112` (bare names now resolve in `a-hyphen`);
- (c2) `SELECT count(*) FROM "a-hyphen".probe` = 1 — new, two-part with a quoted hyphenated schema;
- the post-lock positive control on `lk."qe"."posts"` = 7 — unchanged.
Grep the rest of the test (the `Run`-entrypoint section after the lock) for any other **bare** `posts` reference and qualify it as `"qe".posts`. Keep `runLockdownDenyIntegration` wiring untouched.

- [ ] **Step 4a: Fixture images (pre-existing break, spike `## ATTACH URI`)**

MinIO deleted its community images, so `TestAttachIntegration` cannot pull `minio/minio:RELEASE.2024-12-18T13-15-44Z` (line 237) or `minio/mc` (line 355) on any branch. Replace both with the chart's pinned drop-in, copied verbatim from `charts/datuplet-infra/values.yaml:99-103`: `docker.io/pgsty/silo:RELEASE.2026-10-26T00-00-00Z@sha256:7bb2aff6b89efe1cb1303c10fc21d579447434ed06649ac9ec05a5e2dbf30ad7` (same `server /data --address :<port>` command; `mc` is bundled, so the `makeBucket` `--entrypoint sh` + `mc alias set …` script is unchanged). Update the `makeBucket` doc comment ("one-shot minio/mc container" → "one-shot silo container, which bundles mc"). Do not alias images with `docker tag`; the pin must work on a clean machine.

- [ ] **Step 5: Run everything, then commit**

Run: `cd components/queryengine && CGO_ENABLED=1 go test -tags duckdb_arrow -count=1 ./...`
Expected: PASS, every `TestLockdownDeny` row still erroring.
Run (Docker): `CGO_ENABLED=1 go test -tags 'duckdb_arrow integration' -count=1 -run TestAttachIntegration ./...`
Expected: PASS (if Docker is unavailable say so in the report; Task 7 covers the catalog path on the cluster).
Run: `cd components/sql-transform && CGO_ENABLED=1 go build -tags duckdb_arrow ./... && cd ../../utils/docker/install-duckdb-ext && CGO_ENABLED=1 go build -tags duckdb_arrow ./...`
Expected: clean.

```bash
git add components/sql-transform/go.mod components/sql-transform/go.sum components/queryengine utils/docker/install-duckdb-ext/go.mod utils/docker/install-duckdb-ext/go.sum
git commit -m "chore(deps): DuckDB 2.0.0 LTS (duckdb-go v2.20000.0); queryengine ATTACH via URI, 2.0 test alignment"
```

---

### Task 3: Lockdown hardening for the Quack surface

**Model:** opus

**Files:**
- Create: `components/queryengine/lockdown_quack_test.go`
- Create: `components/queryengine/lockdown_static_ext_test.go`
- Modify: `components/queryengine/lockdown_deny_test.go:16-17` (posture baseline comment → "DuckDB 2.0.0 (duckdb-go/v2 v2.20000.0)") and `:99-101` (`lockedEngineViaAttach`: `t.Skipf` → `t.Fatalf` when `QE_REQUIRE_WARMED=1`)
- Modify (**only** on the maintainer's go after an escalation): `components/queryengine/engine_open.go` `lock()`

**Interfaces:**
- Consumes: spike report `## Findings for Task 3`, `## Statically linked extensions`, `## Quack probe (warmed engine)`; helpers `lockedEngine(t) (*engine, string)` and `lockedEngineViaAttach(t) (*engine, string)` from `lockdown_deny_test.go`.
- Produces: `TestLockdownDenyQuack`, `TestStaticallyLinkedExtensions`; `lockedEngineViaAttach` honours `QE_REQUIRE_WARMED`.

- [ ] **Step 1: Write `lockdown_quack_test.go` (failing first if any row passes)**

```go
//go:build duckdb_arrow

// denyClass names the layer a Quack-surface statement must be refused at
// (spec D5). A row passes only if its error satisfies the class predicate;
// matching the protocol name alone is not a proof.
type denyClass int

const (
	classFSLayer              denyClass = iota // prefix-routed extension-file load refused by disabled_filesystems
	classExtensionUnavailable                  // extension not loaded and cannot be autoloaded/installed
	classSecretTypeAbsent                      // CREATE SECRET with an unregistered TYPE
	classConfigLocked                          // lock_configuration refused a SET
)

// rejectRe: an error matching any of these is a transport/credential/parse
// failure, never a posture deny, whatever else it says. No bare "connect":
// a diagnostic may echo the CONNECT statement without opening a socket; the
// listener is the network proof.
var rejectRe = regexp.MustCompile(`(?i)(connection refused|could not connect|failed to connect|connection reset|timed out|unauthori[sz]ed|invalid token|parser error|syntax error)`)

// classRe: verbatim phrases from the spike report (## Quack probe). The
// subject (quack) is checked separately where set.
var classRe = map[denyClass]*regexp.Regexp{
	classFSLayer:              regexp.MustCompile(`(?i)LocalFileSystem has been disabled`),
	classExtensionUnavailable: regexp.MustCompile(`(?i)but it exists in the \w+ extension`),
	classSecretTypeAbsent:     regexp.MustCompile(`(?i)secret type 'quack' does not exist`),
	classConfigLocked:         regexp.MustCompile(`(?i)the configuration has been locked`),
}

type quackProbe struct {
	name, stmt string
	class      denyClass
	subject    string // extension/type name the error must ALSO contain ("" = none; fsLayer text never names it)
	warmOnly   bool   // only on lockedEngineViaAttach (setting exists only with httpfs loaded)
}

var quackProbes = []quackProbe{
	{"attach-token", "ATTACH 'quack:127.0.0.1:<port>' AS q (TOKEN 'x')", classFSLayer, "", false},
	{"attach-type-quack", "ATTACH 'quack:127.0.0.1:<port>' AS q (TYPE quack)", classFSLayer, "", false},
	{"connect-quack", "CONNECT 'quack:127.0.0.1:<port>'", classFSLayer, "", false},
	{"connect-postgres", "CONNECT 'postgres:host=127.0.0.1 port=<port>'", classFSLayer, "", false},
	{"quack-query", "SELECT * FROM quack_query('quack:127.0.0.1:<port>', 'SELECT 1', token = 'x')", classExtensionUnavailable, "quack", false},
	{"secret-type-quack", "CREATE SECRET qs (TYPE quack, TOKEN 'x', SCOPE 'quack:127.0.0.1')", classSecretTypeAbsent, "quack", false},
	{"set-s3-deprecated-url-params", "SET s3_enable_deprecated_url_parameters = true", classConfigLocked, "", true},
}

// denied reports whether err proves the row's class: err != nil,
// !rejectRe.Match, classRe[class].Match, and (subject == "" || contains subject).
func denied(err error, p quackProbe) (ok bool, why string)

// quackListener owns one loopback *net.TCPListener per subtest. No accept
// goroutine: drain is the barrier. A completed TCP handshake waits in the
// kernel accept queue until Accept is called, so draining AFTER the statement
// returned sees every connection the statement opened; closing first could
// discard the backlog.
type quackListener struct{ ln *net.TCPListener }

// newQuackListener: net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}); t.Cleanup(ln.Close).
func newQuackListener(t *testing.T) *quackListener
func (l *quackListener) Port() string
// drain: SetDeadline(now+d), then Accept in a loop (close each conn, count)
// until the deadline error; returns the count.
func (l *quackListener) drain(d time.Duration) int

// layerNamingEngine: openEngine + the extension posture only
// (autoinstall_known_extensions=false, autoload_known_extensions=false,
// allow_community_extensions=false), NO disabled_filesystems, NO lock();
// SET extension_directory = t.TempDir() (empty, so a developer's locally
// installed quack cannot change the outcome); t.Chdir(t.TempDir()) so a stray
// local DB file would be visible.
func layerNamingEngine(t *testing.T) *engine

// TestLockdownDenyQuack subtests:
//   "listener-detects-connection": net.Dial the listener, then drain(200ms) == 1 (negative control, same barrier).
//   "layer-naming": layerNamingEngine; the four classFSLayer statements must error and the text must
//        contain `Extension "quack" is an existing extension` (rows 1-3) /
//        `Extension "postgres_scanner" is an existing extension` (row 4); cwd stays empty; drain == 0.
//   "cold":   e, _ := lockedEngine(t); every non-warmOnly probe.
//   "warmed": e, _ := lockedEngineViaAttach(t); every probe (the helper Fatalfs under QE_REQUIRE_WARMED=1).
// Each probe: ctx, cancel := context.WithTimeout(ctx, 10*time.Second);
// _, err := e.conn.ExecContext(ctx, strings.ReplaceAll(p.stmt, "<port>", l.Port()));
// cancel(); require denied(err, p); require l.drain(200*time.Millisecond) == 0.
func TestLockdownDenyQuack(t *testing.T)
```

The regexes above are the spike's recorded 2.0.0 texts; keep `rejectRe` as written. If a row's error matches `rejectRe`, or `drain` returns non-zero, that is an escalation (the statement reached the network or failed for the wrong reason), not a regex to loosen. In `lockdown_deny_test.go:99-101` change `lockedEngineViaAttach` so the INSTALL/LOAD failure is `t.Fatalf` when `os.Getenv("QE_REQUIRE_WARMED") == "1"` and `t.Skipf` otherwise (same message); no separate preflight.

- [ ] **Step 2: Run it (warmed coverage mandatory)**

Run: `cd components/queryengine && set -o pipefail && QE_REQUIRE_WARMED=1 CGO_ENABLED=1 go test -tags duckdb_arrow -count=1 -run 'TestLockdownDenyQuack' -v ./... 2>&1 | grep -E '^\s*--- (PASS|FAIL|SKIP)'`
Expected: `listener-detects-connection`, every `layer-naming/*`, `cold/*` and `warmed/*` line is `--- PASS`; no `SKIP`. A row that **succeeds**, a row whose error is rejected as transport/credential, or a listener count above zero: stop, write the report with the exact output; the orchestrator escalates. Do not touch `lock()`.

- [ ] **Step 3: Write `lockdown_static_ext_test.go`**

```go
//go:build duckdb_arrow

// TestStaticallyLinkedExtensions pins what the duckdb-go bindings link in.
// Runs PRE-lock on purpose: duckdb_extensions() is a documented post-lock deny.
func TestStaticallyLinkedExtensions(t *testing.T) {
	// e, _ := openEngine(ctx, Request{TempDir: t.TempDir()}); defer e.Close()
	// rows := SELECT extension_name FROM duckdb_extensions() WHERE install_mode = 'STATICALLY_LINKED' ORDER BY 1
	// Exact set the spike measured on 2.0.0 (1.5.3 lacked tpcds/tpch). dbgen/dsdgen write into
	// the CURRENT catalog — the attached lk — which is READ_ONLY, so they fail there before any
	// request (TestLockdownDenyAttachedReadOnly); aimed at `memory` they generate in RAM, bounded
	// by memory_limit / max_temp_directory_size / the request timeout (spec D5). Any growth or
	// shrink fails here so a bindings bump reconciles it on purpose.
	wantStatic := []string{"autocomplete", "core_functions", "icu", "json", "parquet", "tpcds", "tpch"}
	// Canonical duckdb_extensions() names: there is no row called "postgres" or "mysql".
	mustNotHave := []string{"quack", "httpfs", "iceberg", "postgres_scanner", "mysql_scanner"}
	// assert rows == wantStatic (exact, order-insensitive) and no mustNotHave name among them;
	// t.Logf the full list
}
```

- [ ] **Step 4: Refresh the posture comment, run the module suite, commit**

Edit `lockdown_deny_test.go:16-17` baseline to "DuckDB 2.0.0 (duckdb-go/v2 v2.20000.0), re-proven 2026-10-28".
Run: `CGO_ENABLED=1 go test -tags duckdb_arrow -count=1 ./...`
Expected: PASS.

```bash
git add components/queryengine/lockdown_quack_test.go components/queryengine/lockdown_static_ext_test.go components/queryengine/lockdown_deny_test.go
git commit -m "test(queryengine): deny every Quack entry point post-lock with layer markers + loopback listener; pin statically linked extensions (DuckDB 2.0)"
```

---

### Task 4: Docker `verify` stage — offline INSTALL/LOAD inside the image build

**Model:** sonnet

**Files:**
- Modify: `utils/docker/query-worker.Dockerfile` (builder lines 44-54, runtime stage from line 57)
- Modify: `utils/docker/datuplet-query.Dockerfile` (builder lines 40-46, runtime stage from line 49)
- Modify: `utils/docker/install-duckdb-ext/main.go:3-12` (doc comment: the binary now also runs in the `verify` stage; still never in the final image)

**Interfaces:** none consumed later; Task 7 and CI build these images.

- [ ] **Step 1: Build the baker as a binary in the builder stage**

In both Dockerfiles replace the `go run -tags=duckdb_arrow .` baking line with
`CGO_ENABLED=1 go build -tags=duckdb_arrow -o /install-duckdb-ext . && HOME=/root /install-duckdb-ext && echo "Baked extension directory:" && find /root/.duckdb -type f | sort`
(same cache mounts).

- [ ] **Step 2: Name the runtime stage and add `verify` + final**

Change `FROM debian:bookworm-slim` (runtime) to `FROM debian:bookworm-slim AS runtime`. After the runtime stage's last instruction append:

```dockerfile
# ---- Offline extension gate ------------------------------------------------
# Proves, inside the runtime layout (user, HOME, libc, baked extension dir),
# that INSTALL/LOAD iceberg + httpfs succeed with NO network. A mis-baked or
# mis-pathed extension fails the build here instead of the first query.
FROM runtime AS verify
COPY --from=builder /install-duckdb-ext /ext-check
USER datuplet
ENV HOME=/home/datuplet
RUN --network=none /ext-check && touch /tmp/ext-verified

# ---- Final image: runtime + proof the gate ran ------------------------------
FROM runtime
COPY --from=verify /tmp/ext-verified /tmp/ext-verified
```

The baker binary is in `verify` only; the final image carries just the marker file.

- [ ] **Step 3: Prove the gate both ways**

Run: `make build-component-datuplet-query && docker build -t datuplet/query-worker:latest -f utils/docker/query-worker.Dockerfile .`
Expected: both builds succeed and the `verify` stage log shows `OK: INSTALL iceberg`, `OK: LOAD iceberg`, `OK: INSTALL httpfs`, `OK: LOAD httpfs`.
Negative proof (not committed): temporarily change `COPY --from=builder --chown=datuplet:datuplet /root/.duckdb /home/datuplet/.duckdb` to copy into `/home/datuplet/.duckdb-wrong` in one Dockerfile and rebuild.
Expected: the build FAILS in `verify` (INSTALL tries the network and cannot). Revert.
Run: `docker run --rm --entrypoint /bin/sh datuplet/query-worker:latest -c 'ls -l /tmp/ext-verified && ! test -e /ext-check'`
Expected: marker present, baker absent.

- [ ] **Step 4: Commit**

```bash
git add utils/docker/query-worker.Dockerfile utils/docker/datuplet-query.Dockerfile utils/docker/install-duckdb-ext/main.go
git commit -m "build(docker): verify stage runs INSTALL/LOAD iceberg+httpfs offline inside the runtime layout"
```

---

### Task 5: e2e CLI 2.0.0 and agent mode off

**Model:** haiku

**Files:**
- Modify: `.github/workflows/e2e.yml:105-120` ("Install DuckDB CLI" step)
- Modify: `tests/e2e/framework/assertions.go:108` (exec env)

- [ ] **Step 1: Verify the asset, then edit the workflow step**

Run: `curl -sI -o /dev/null -w '%{http_code}\n' -L https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-linux-amd64.tar.gz` → expected `200`. Download it to the scratchpad and `tar -tzf` it; note whether `duckdb` is top-level.

Replace the version/asset lines so the step body reads (keep `set -euo pipefail` and the existing comment above):

```yaml
          # 2.0.0 renamed the asset (duckdb_cli-…zip → duckdb-cli-…tar.gz).
          # 1.4 LTS is EOL 2026-11-17; the engines moved to 2.0.0, so the CLI
          # reads our tables with the same major. Laptops on homebrew 1.4.x
          # still pass: the assertions are plain SELECT/COUNT/DESCRIBE over
          # iceberg_scan.
          DUCKDB_VERSION=v2.0.0
          curl -fsSL -o /tmp/duckdb.tar.gz \
            "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb-cli-linux-amd64.tar.gz"
          sudo tar -xzf /tmp/duckdb.tar.gz -C /usr/local/bin duckdb
          duckdb --version
```

If the tarball nests the binary, add the matching `--strip-components` and say so in the report.

- [ ] **Step 2: Force agent mode off in the assertion helper**

In `duckdbQuery`, after `cmd := exec.Command(...)`, add
`cmd.Env = append(os.Environ(), "DUCKDB_AGENT_MODE=0") // 2.0 CLI agent mode would swap -json for Markdown; -json already prevents it, this makes it explicit.`
Add the `os` import if missing.

- [ ] **Step 3: Verify and commit**

Run: `cd tests/e2e && go vet ./... && go build ./...` → clean.
Run: `actionlint .github/workflows/e2e.yml` → no output.

```bash
git add .github/workflows/e2e.yml tests/e2e/framework/assertions.go
git commit -m "ci(e2e): pin DuckDB CLI 2.0.0 (tar.gz asset); force agent mode off in assertions"
```

---

### Task 6: CHANGELOG and docs

**Model:** haiku

**Files:**
- Modify: `CHANGELOG.md` (`## [Unreleased]` → `### Changed`, around line 31)
- Modify: `components/sql-transform/README.md` (lines 41-45 paragraph + list; line 161; line 167; new section before "## How inputs and outputs map to SQL")
- Modify: `components/sql-transform/main.go:7-13` (header comment item 2)
- Modify: `docs/ad-hoc-query.md` ("## Server query service", one paragraph)
- Modify: `docs/known-limitations.md` ("## Ad-hoc SQL query", one bullet)
- Modify: `docs/dependency-upgrades.md` ("## Go modules …", one bullet)
- Modify: `CLAUDE.md:308-312` (sql-transform bullet)

**Interfaces:**
- Consumes: spike report `## Findings for Task 6` (whether `icu` is statically linked).

- [ ] **Step 1: CHANGELOG**

Under `### Changed` add:

```markdown
- **DuckDB 2.0.0 LTS** in `sql-transform`, `query-worker` and `datuplet-query`
  (`duckdb-go/v2 v2.20000.0`, was 1.5.3). The e2e DuckDB CLI is pinned to
  2.0.0, and both DuckDB images now prove at build time that the baked
  `iceberg`/`httpfs` extensions load offline. User-visible SQL dialect
  changes, per the
  [DuckDB 2.0 migration guide](https://duckdb.org/docs/2.0/operations_manual/migration.html):
  `~`/`!~` now match partially; `!` (factorial) binds tighter; the
  single-arrow lambda `x -> …` is disabled (use `lambda x : …`); integer
  `//` and `%` by zero raise instead of returning NULL; an untyped `NULL`
  column keeps type `NULL` instead of `INTEGER`; `X'…'`/`B'…'` literals are
  BLOB/BITSTRING; `MERGE INTO` raises when one target row matches twice;
  S3 settings in URL query parameters are ignored (use secrets). No
  compatibility flags are set in either engine.
```

- [ ] **Step 2: sql-transform README — engine section and I/O wording**

Insert before `## How inputs and outputs map to SQL`:

```markdown
## DuckDB engine

The component embeds DuckDB **2.0.0** (`duckdb-go/v2 v2.20000.0`). Engine
resources are set from `config` (all optional): `memory` (DuckDB
`memory_limit`, default `1GB`), `max_temp_size` (`max_temp_directory_size`,
default `6GB`, a hard cap on spill), `threads` (default `2`), `temp_directory`
(default `/tmp/duckdb-spill`). No other DuckDB settings are exposed and no
compatibility flags are set, so the 2.0 dialect applies as documented in the
[migration guide](https://duckdb.org/docs/2.0/operations_manual/migration.html):
`~`/`!~` match partially; `!` binds tighter; `x -> …` lambdas are disabled
(`lambda x : …`); integer `//` and `%` by zero raise; an untyped `NULL`
column keeps type `NULL`; `X'…'`/`B'…'` literals are BLOB/BITSTRING;
`MERGE INTO` raises on a doubly matched target row.
```

The spike report shows `icu` statically linked and loaded on 2.0.0 (as on 1.5.3): append `ICU collations and time-zone functions work without \`LOAD icu\`; the extension is statically linked into the engine.` Do not add a `LOAD icu` note.

Replace the "Inputs → DuckDB views" paragraph and its 3-item list (lines 41-45) with:

```markdown
**Inputs → DuckDB views.** Each declared input is registered as a DuckDB view. `<view_name>` is `logicalName` if set, otherwise the physical `table`. Inputs are fetched from DataGateway as parquet and staged on the component's ephemeral disk; only one HTTP read buffer is in memory at a time. Internally:

1. Component opens an `OpenReader(FORMAT_PARQUET)` on the input and takes the gateway's HTTP read endpoint from the reply (gRPC's 64 MiB message cap rules it out for large source files).
2. It `GET`s the stream chunk by chunk, streaming each response body to its own parquet file under the work dir (`io.Copy`, ~32 KiB resident), until the gateway sets `X-Datuplet-Is-Last`.
3. It runs `CREATE VIEW <view_name> AS SELECT * FROM read_parquet([<chunk files>])` and then the user SQL; DuckDB scans the staged files lazily, row group by row group.
```

Replace the "Empty-input semantics" paragraph (line 161) with:

```markdown
When a declared input has no data files for the current run (first incremental window, upstream extractor produced nothing), the gateway closes the stream with no chunks and the component registers a zero-row placeholder view (`CREATE VIEW <view_name> AS SELECT * FROM (SELECT 1) WHERE 1=0`). The placeholder does **not** carry the input's columns: `SELECT count(*) FROM <view_name>` works and yields 0, but SQL that names a column of the empty input fails at bind time with a column-not-found error — the same outcome as referencing a missing column on a real table. If a pipeline can legitimately see an empty input, make the producing stage always emit its schema; a `UNION ALL` with a typed empty `SELECT` does not help, because the first branch still fails to bind the missing column.
```

Replace "Architecture" item 1 (line 167) with:

```markdown
1. **Inputs.** For each declared input the component opens `OpenReader(FORMAT_PARQUET)`, drains the gateway's HTTP read endpoint chunk by chunk into local parquet files, and registers `CREATE VIEW … AS SELECT * FROM read_parquet([...])`. **Parquet bytes are staged on the component's ephemeral disk; nothing but one HTTP read buffer is resident.**
```

- [ ] **Step 3: main.go header, ad-hoc-query, known-limitations, dependency-upgrades, CLAUDE.md**

- `components/sql-transform/main.go` header items 1 **and** 2 (lines 6-13; item 1 still says Arrow IPC / `FORMAT_ARROW_IPC`) → `1. Opens each declared input via the DataGateway SDK with FORMAT_PARQUET and takes the gateway's HTTP read endpoint from the reply (gRPC's 64 MiB message cap rules it out for large source files).` and `2. GETs the stream chunk by chunk, streaming each response body to its own parquet file on local ephemeral disk (one HTTP read buffer resident), and exposes the chunk set to user SQL as a DuckDB view over read_parquet(...); an input with no data files gets a zero-row placeholder view. The disk-stage replaces an earlier in-memory CREATE TABLE AS SELECT * FROM <arrow_view> that OOM'd on multi-GB inputs and needed a multi-scan workaround for duckdb/duckdb#19040.` (keep the comment's line wrapping style).
- `docs/ad-hoc-query.md`, end of "## Server query service": `The engine is DuckDB 2.0.0; see the CHANGELOG entry for the dialect changes versus 1.5.`
- `docs/known-limitations.md`, "## Ad-hoc SQL query": `**Async read-ahead shares `memory_limit`.** DuckDB 2.0 keeps multiple object-storage reads in flight; the read-ahead budget is bounded by the same `memory_limit` the worker sets, so a wide scan can spill earlier than on 1.5. No separate knob is exposed.`
- `docs/known-limitations.md`, same section, next line: `**Statically linked `tpch`/`tpcds` are reachable post-lock.** DuckDB 2.0.0 links the benchmark generators into the engine (`dbgen`, `dsdgen`, `tpch_queries()`, `tpcds_queries()`). The generators write into the CURRENT catalog, which is the attached lakekeeper catalog; because it is attached read-only, `CALL dbgen()` / `dsdgen()` against it fail before any request. Aimed at the in-memory `memory` catalog they still generate data in RAM, bounded by `memory_limit`, `max_temp_directory_size` and the request timeout; a statically linked extension has no runtime off-switch. `TestStaticallyLinkedExtensions` pins the exact linked set and `TestLockdownDenyAttachedReadOnly` proves the refusal.`
- `docs/dependency-upgrades.md`, "## Go modules …": `- DuckDB (duckdb-go/v2): since 2.0 the static libraries resolve from the `go.duckdb.org/libduckdb/*` module path; a GOPROXY allowlist must include it. Bump `components/sql-transform`, `components/queryengine` and `utils/docker/install-duckdb-ext` together, run the queryengine suite locally (`-tags duckdb_arrow`; PR CI does not), and let the images' `verify` stage prove the baked extensions still load offline.`
- `CLAUDE.md` sql-transform bullet: replace "Inputs stream from Data Gateway via Arrow IPC and are materialized into a DuckDB table before user SQL runs (workaround for duckdb-go's GROUP BY / JOIN / UNION ALL bug against registered Arrow streams)." with "Inputs arrive from Data Gateway as parquet chunks over its HTTP read endpoint, are streamed to local temp files and exposed to user SQL as `read_parquet` views (bounded RAM; a one-shot stream cannot be scanned twice)."

- [ ] **Step 4: Verify and commit**

Run: `grep -rnE "1\.5\.3|arrow_scan|RegisterView|FORMAT_ARROW_IPC" CLAUDE.md docs/*.md components/sql-transform/README.md components/sql-transform/main.go`
Expected: no matches (the `docs/superpowers/` history is excluded by the glob).
Run: `cd components/sql-transform && CGO_ENABLED=1 go build -tags duckdb_arrow ./...` → clean (comment-only change).

```bash
git add CHANGELOG.md components/sql-transform/README.md components/sql-transform/main.go docs/ad-hoc-query.md docs/known-limitations.md docs/dependency-upgrades.md CLAUDE.md
git commit -m "docs: DuckDB 2.0 engine note, dialect changes, sql-transform I/O wording from io.go"
```

---

### Task 7: OrbStack — `e2e-k8s` gate, kept stack for the NULL probe and RSS baseline

**Model:** opus

**Files:**
- Modify: `docs/superpowers/specs/2026-10-28-duckdb-2-spike-report.md` (append `## Cluster verification`)
- Temporary (not committed): `/tmp/etl-duckdb-null.yaml`

**Interfaces:**
- Consumes: every prior commit; images built from this branch. Use the `datuplet-operator` skill for the CLI loop (put, trigger, runs, query).

- [ ] **Step 1: Full e2e gate (tears itself down on success)**

```bash
kubectl config view --minify --flatten --context orbstack > /tmp/orb.kubeconfig
export KUBECONFIG=/tmp/orb.kubeconfig
make e2e-k8s 2>&1 | tee /tmp/e2e-duckdb2.log
```
Expected: PASS with zero SKIPs among `TestQuery_HappyPath`, `TestQuery_WriteProbe`, `TestQuery_StoragePreview`, `TestScenarios/multi-table-join`, `TestBigDataJoinProof` (`TestQuery_NetworkPolicyEgress` self-skips on OrbStack — expected). On failure: diagnose from the query-worker / sql-transform pod logs, fix on the branch in its own green commit, re-run `make e2e-k8s`.

- [ ] **Step 2: Kept stack and baseline run (CLI, not the Make retry target)**

```bash
make deploy-local
datuplet pipeline put -f examples/pipelines/etl-duckdb.yaml
datuplet trigger duckdb-transform
```
then `datuplet runs` (or the run-wait form the `datuplet-operator` skill shows) until the run is terminal. Expected: Succeeded, and `datuplet query --sql 'SELECT count(*) FROM etl.user_summary'` returns 10. This is sql-transform on 2.0 outside the e2e harness. `make k8s-retry-duckdb` is **not** used: it `kubectl apply`s the envelope-free PipelineDoc and fails (pre-existing; note it as Pre-existing in the report, do not fix).

- [ ] **Step 3: Untyped NULL probe (Review Focus 4) — its own output table**

Write `/tmp/etl-duckdb-null.yaml` from `examples/pipelines/etl-duckdb.yaml` with: `name: duckdb-null-probe`; stage 2 output `- name: null_probe`, `bucket: "etl"`, `writeMode: FULL_LOAD` (replacing `user_summary`); SQL `CREATE TABLE null_probe AS SELECT "userId", COUNT(*) AS post_count, NULL AS probe_null FROM data GROUP BY "userId" ORDER BY "userId";`. A distinct table is required: the gateway reuses an existing table's schema (`pkg/datagateway/lakekeeper/lakekeeper.go:150`), so writing into `user_summary` would never exercise schema creation from a NULL-typed column. Then `datuplet pipeline put -f /tmp/etl-duckdb-null.yaml` + `datuplet trigger duckdb-null-probe` + wait. Record under `## Cluster verification`: Succeeded with `datuplet query --sql 'DESCRIBE etl.null_probe'` (the `probe_null` type), or the failure class and message. Do **not** patch the component; a FailedApplication is a maintainer decision.

- [ ] **Step 4: RSS baseline (spec D9)**

Run a representative query with `datuplet query` against `etl.user_summary` while sampling `kubectl -n datuplet top pod | grep query-worker` every 2 s; if `kubectl top` is unavailable, `kubectl -n datuplet exec <query-worker pod> -- cat /sys/fs/cgroup/memory.peak` after the query. Record the peak and method, or "not measured: <reason>".

- [ ] **Step 5: Tear down and commit the addendum**

```bash
make undeploy-local
git add docs/superpowers/specs/2026-10-28-duckdb-2-spike-report.md
git commit -m "docs: DuckDB 2.0 cluster verification results"
```

---

### Task 8: Final gate (orchestrator, in-session)

- [ ] **Step 1: Lint, tidy, OpenFGA integration**

Run: `make lint && make tidy && git diff --exit-code` → clean.
Run: `go test -tags=integration -count=1 ./pkg/pipelineapi/authz/...` → PASS (Docker).
Run: `cd components/queryengine && set -o pipefail && QE_REQUIRE_WARMED=1 CGO_ENABLED=1 go test -tags duckdb_arrow -count=1 -v ./... 2>&1 | tee /tmp/qe-gate.txt | tail -5` → `ok` for all three packages; then `grep -c -- '--- SKIP' /tmp/qe-gate.txt` → `0` (the spike measured 0 SKIP on a warm laptop; any SKIP is a lost row).

- [ ] **Step 2: Local reviews per REVIEW.md High tier**

`/code-review max` on the branch; `/security-review` scoped to `components/queryengine` and `utils/docker/`. Present Important findings to the maintainer; fix only on approval, then re-run the queryengine suite.

- [ ] **Step 3: Codex whole-branch gate**

```bash
node "$HOME/.claude/plugins/cache/openai-codex/codex/1.0.6/scripts/codex-companion.mjs" adversarial-review --wait --base main --scope branch "Whole-branch gate for the DuckDB 2.0 upgrade (spec docs/superpowers/specs/2026-10-28-duckdb-2-upgrade-design.md rev 2). Minor ledger: <ledger>. Review per REVIEW.md."
```

- [ ] **Step 4: Push and draft PR**

```bash
git push -u public feat/duckdb-2.0
gh pr create --draft --title "chore: DuckDB 2.0.0 LTS (duckdb-go v2.20000.0)" --body-file /tmp/pr-body.md
```
PR body: spec + plan links, the spike report's headline findings, the D2 hold-for-2.0.1 option as the maintainer's choice, the verification matrix results, the Minor ledger, the attribution footer. Then bind the PR with the ccd_pr tools and read CI.
