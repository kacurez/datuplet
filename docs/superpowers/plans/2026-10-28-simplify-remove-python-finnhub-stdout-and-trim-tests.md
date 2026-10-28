# Simplify: remove Python SDK, finnhub-extractor, stdout-writer; trim tests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the Python SDK and everything that depends on it, delete the `finnhub-extractor` and `stdout-writer` components with all their wiring, and cut the unit + e2e test suites down to the tests that pin distinct behaviour.

**Architecture:** Three built-in components remain (`data-generator`, `http-json-extractor`, `sql-transform`) plus `query-worker` and `app-worker`. The only Go SDK is `sdk/go` (+ `sdk/go/arrow`). Nothing in the runtime data path changes; this is deletion, de-duplication, and table-driven consolidation of tests. Every merged test keeps every case as a row. The e2e suite keeps one scenario per deployment behaviour and drops happy-path repeats.

**Tech Stack:** Go 1.x multi-module monorepo, Helm charts, GitHub Actions, OrbStack K8s for e2e, Codex for reviews.

**Spec:** Tomas's request of 2026-10-28, verbatim:

> remove python based sdk + all components that are using it (I guess only pandas transformation); remove finnhub-extractor and all tests/docs/helm chart.. around it; remove stdout-writer and all tests/docs/helm chart.. around it + make sure data-generator cover tests previously done via stdout-writer; also look into the current tests and try to simplify them, lets have less tests, no need for crazy overtesting, include e2e tests in this analysis. Do it in the current PR. First prepare a plan, have it reviewed by codex and then via subagent driven mode execute it. Make sure each task has model assigned, I believe most of the task would only require sonnet/opus.

Two removals in this plan go beyond that text and are called out so they can be vetoed: `tests/integration/` (it holds the only other Python file, `test_python_gateway_v2.py`, plus a parked test of deleted TableCommit code; `make tidy` and CI already skip it) and `examples/pipelines/incremental-reads.yaml` (a stdout-writer-only example with no replacement sink).

Codex reviewed this plan on 2026-10-28 (session `01a182df-0af0-7c12-abf6-994b2de31350`): 6 Important, 3 Nit, 2 questions, all folded in below.

Plus the audit appendix
`docs/superpowers/plans/2026-10-28-simplify-test-audit-appendix.md` (Parts E, A, B). The appendix holds the per-test KEEP / MERGE / DELETE lists with `file:line`; this plan says which proposals are adopted and in what order. Executors read both.

**Branch / PR:** everything lands on `docs/review-guardrails` (draft PR #44, stacked on #43). One commit per task.

## Global Constraints

- `make tidy` after any `go.mod` change — never a bare `go mod tidy` (CLAUDE.md).
- `make sync-component-schemas` after any `components/*/schema.json` change; CI diffs the chart copies.
- `go build ./... && go test ./...` from the root, plus `go test ./...` in every touched module (`components/*`, `sdk/go`, `sdk/go/arrow`, `tests/e2e`), before each commit.
- Never `git push origin main`, never `git tag`. Push `docs/review-guardrails` to remote `public`.
- Merging tests = table-driven with **every original case kept as a row**. Do not drop a case because it "looks the same"; the appendix names the survivors.
- Do not delete: security-boundary tests (authz denials, token validation, redaction, path traversal), incident tests, Form-Subset lint tests, the only test of a public function. The appendix already honours this; if an executor disagrees with an appendix line, they skip it and say so in the task report — they do not invent extra deletions.
- Build-tagged `integration` tests are out of scope and untouched.
- `docs/superpowers/` history files are never edited (they are records, not docs).
- **Reference sweep command** (used by Tasks 1, 2, 3, 12; expected output: nothing):

  ```bash
  rg -n --hidden -i -e 'stdout-writer' -e 'stdout_writer' -e 'stdoutwriter' -e 'finnhub' -e 'pandas' -e 'sdk/python' -e 'python-sandbox' -e 'python_sandbox' -e 'tests/integration' \
    -g '!.git/**' -g '!.claude/worktrees/**' -g '!docs/superpowers/**' -g '!CHANGELOG.md' .
  ```
  The committed skills under `.claude/skills/` are deliberately **inside** the sweep. (`ui/product/lib/schema-form.js:780` mentions the word "python" only as an example `x-datuplet-multiline` value; it does not match this pattern and stays.)
- **Verification commands never mask failures.** No `|| true`, no `2>/dev/null` on a test run; any pipeline through `tee` runs under `set -o pipefail`. This is REVIEW.md's fake-green failure mode.
- The `main` ruleset's required checks are the five `pr.yml` job names plus `kind + helm + e2e`; the `_release-components.yml` jobs deleted in Tasks 2–3 are not required contexts, so deleting them cannot block merges.

## Review Focus

Reviewers (Codex, per task) check these first; none has a dedicated test:

1. **A deleted component name still referenced somewhere that CI does not exercise** (docs, skills, `upgrade-e2e.yml`, `release-verify.yml`, `examples/README.md`). Expected: `grep -rn` for `stdout-writer|finnhub|pandas|sdk/python|python-sandbox` over the repo (excluding `.git`, `.claude/worktrees`, `docs/superpowers/`, `CHANGELOG.md`) is empty after Task 3.
2. **A merged table that silently lost a row** — compare the number of `t.Run` rows to the number of original `Test*` funcs the appendix lists for that merge.
3. **`helm template` of `datuplet-app` with and without `components.builtins=true`** still renders after the ComponentDefinition templates are gone and `enablePandasTransform` is removed.
4. **e2e `write-modes` merge**: the APPEND half may hit the same cancel-watch race that skipped `append-mode`. Expected: either both halves pass on OrbStack, or the task falls back to keeping `append-mode` skipped and says so.
5. **`tests/release-verify/pipeline.yaml`** after replacing the stdout-writer stage. The contract is in `tests/release-verify/run-pipeline.sh` (called from `release-verify.yml:90-101`), not the workflow: a top-level `name` must exist (`:13-14`), login + project lookup + upload + trigger must succeed (`:18-38`), and the run must reach `Succeeded` (any `Failed*`, `Cancelled`, `Expired` or timeout fails, `:42-56`). No rows, tables or stdout are inspected. So the rewrite must keep `${PIPELINE_NAME}`, unpinned built-in component references (version omitted → `defaultVersion`), and a two-stage pipeline that actually succeeds on a fresh install; `verify.data` is kept only because stage 2's input must exist.

---

## Task order and models

| # | Task | Model | Why this model |
|---|------|-------|----------------|
| 1 | Delete Python SDK, python-sandbox, pandas-transform, tests/integration | sonnet | mechanical deletion + reference sweep |
| 2 | Delete finnhub-extractor | sonnet | mechanical |
| 3 | Delete stdout-writer; repoint examples, release-verify, validate test; drop its e2e scenarios | sonnet | mechanical, one YAML rewrite |
| 4 | e2e suite cut (Part E) + live OrbStack run | opus | judgment on the write-modes merge and cluster debugging |
| 5 | Trim `pkg/appworker` tests (Part A) | sonnet | table merges, listed deletes |
| 6 | Trim `pkg/pipelineapi/http` + `apps` tests (Part A) | sonnet | same |
| 7 | Trim `pkg/pipelineapi/{storage,queryproxy,k8s,tokens,auth,authz,store}` (Part A) | sonnet | same |
| 8 | Trim `cmd/datuplet` + `cmd/pipeline-api` (Part B) | sonnet | same |
| 9 | Trim `pkg/datagateway/**`, `pkg/catalogwriter`, `pkg/icebergjob` (Part B) | sonnet | same |
| 10 | Trim `pkg/k8s/controllers`, `pkg/pipeline/*`, `sdk/go`, `sdk/go/arrow` (Part B) | sonnet | same |
| 11 | Trim `components/{queryengine,data-generator,http-json-extractor}` (Part B) | sonnet | separate modules |
| 12 | Final sweep: docs, CLAUDE.md, CHANGELOG, lint, full test, push | opus | cross-cutting consistency |

Each task ends with a Codex per-task review (CLAUDE.md "Code reviews + plan workflow"); Task 12 additionally runs the whole-branch gate **before** the push (its Step 5). Tasks 5–11 are independent of each other but touch distinct packages; they run sequentially anyway (one branch). The equivalence decisions the audits left open are resolved in the task text below, so Sonnet executors apply them mechanically and do not re-litigate them.

---

### Task 1: Delete the Python SDK and its dependents

**Files:**
- Delete: `sdk/python/` (whole tree), `sdk/python-sandbox/` (whole tree), `components/pandas-transform/`, `tests/integration/` (parked since v0.1.0, contains `test_python_gateway_v2.py` and a TableCommit test for deleted code), `charts/datuplet-app/templates/components/pandas-transform.yaml`, `charts/datuplet-app/files/component-schemas/pandas-transform.json`
- Modify: `charts/datuplet-app/values.yaml:412-418` (drop `enablePandasTransform` and the "six built-in components" wording → "three"), `.dockerignore:12`, `Makefile:60` (commented pandas build line), `Makefile:100` (comment), `Makefile:360-379` (`tidy` carve-out for `tests/integration` — remove the `-not -path './tests/integration/go.mod'` filter and the "intentionally skipped" echo), `CLAUDE.md:21` ("Python (SDK + a few scripts)" → Go only; keep "Shell (automation)"), `CLAUDE.md:373-374` (drop the `sdk/python` half of the SDK row and the `sdk/python-sandbox/` row), `CLAUDE.md` module list line 342 (drop `tests/integration/` clause), `docs/components.md:13,28,404-430,484,518`, `docs/quickstart-kind.md:97`, `docs/agent-quickstart.md:72`, `.claude/skills/datuplet-operator/references/components.md:29`, `.claude/skills/datuplet-component-author/SKILL.md:55,96-97`, `.claude/skills/datuplet-component-author/references/build-a-component.md:16-17,47`, `ui/product/lib/schema-form.js:524` (comment: pick a surviving example), `README.md:88` ("thin Go and Python SDKs" → "a thin Go SDK")

**Interfaces:**
- Produces: a repo where the Global Constraints sweep command, restricted to `-e 'sdk/python' -e 'python-sandbox' -e 'python_sandbox' -e 'pandas' -e 'tests/integration'`, prints nothing.

- [ ] **Step 1: Delete the trees**

```bash
git rm -r sdk/python sdk/python-sandbox components/pandas-transform tests/integration \
  charts/datuplet-app/templates/components/pandas-transform.yaml \
  charts/datuplet-app/files/component-schemas/pandas-transform.json
```

- [ ] **Step 2: Edit every reference listed under Modify.** For prose, delete the sentence or row; do not leave "(removed)" markers. `values.yaml`: the `enablePandasTransform` key goes; grep `charts/datuplet-app/templates` for `enablePandasTransform` to confirm nothing else reads it.

- [ ] **Step 3: Verify**

Run the Global Constraints sweep restricted to the Python/pandas/tests-integration patterns. Expected: no output.

```bash
make tidy && go build ./... && go vet ./... && make sync-component-schemas && git diff --exit-code charts/datuplet-app/files/component-schemas
helm template x charts/datuplet-app --namespace datuplet > /dev/null
```
Expected: all exit 0; `tidy` no longer prints "tests/integration intentionally skipped".

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "chore: remove Python SDK, python-sandbox, pandas-transform and parked tests/integration"
```

---

### Task 2: Delete finnhub-extractor

**Files:**
- Delete: `components/finnhub-extractor/`, `charts/datuplet-app/templates/components/finnhub-extractor.yaml`, `charts/datuplet-app/files/component-schemas/finnhub-extractor.json`
- Modify: `Makefile:58,62,111` (build line, comment, `build-components-local` loop), `.github/workflows/_release-components.yml:3-5,45-50` (header comment + job), `.github/workflows/release.yml:62-63` (comment), `.github/workflows/upgrade-e2e.yml:139`, `README.md:49,99`, `docs/quickstart-kind.md:87`, `docs/components.md:129,340-403` (section + IO table row), `docs/agent-quickstart.md:70`, `.claude/skills/datuplet-operator/references/components.md:27,34`, `.claude/skills/datuplet-operator/references/scenarios.md:191-192` (drop the market-data hint), `ui/product/pages/pipeline-detail.js:547` (comment)

- [ ] **Step 1:** `git rm -r` the three paths.
- [ ] **Step 2:** Edit every Modify reference. In `_release-components.yml` also fix the "five built-in component images" count (will be three after Task 3; write "three" now and Task 3 confirms).
- [ ] **Step 3: Verify**

Run the Global Constraints sweep restricted to `-e finnhub`. Expected: no output.

```bash
make tidy && go build ./... && make sync-component-schemas && git diff --exit-code charts/datuplet-app/files/component-schemas && helm template x charts/datuplet-app --namespace datuplet > /dev/null
```
- [ ] **Step 4: Commit** `chore: remove finnhub-extractor component`

---

### Task 3: Delete stdout-writer and repoint its users

The e2e audit (appendix Part E §3) established that neither e2e scenario that used stdout-writer depended on it: `read-back` asserts a duckdb `row_count` on the *extractor's* table, and `error-missing-table` gets its 400 from the hyphenated table name before any pod runs. The "read a table back through the gateway" behaviour is proved by `multi-table-join` / `duckdb-etl` (sql-transform reads stage-1 tables); the generator side is `data-generator`. So nothing moves onto data-generator — the coverage was never in stdout-writer.

**Files:**
- Delete: `components/stdout-writer/`, `charts/datuplet-app/templates/components/stdout-writer.yaml`, `charts/datuplet-app/files/component-schemas/stdout-writer.json`, `tests/e2e/pipelines/k8s/read-back.yaml`, `tests/e2e/pipelines/k8s/error-missing-table.yaml`, `examples/pipelines/incremental-reads.yaml`
- Modify:
  - `Makefile:56,71,75,111` (build lines, `build-components-e2e` help text + body, local loop)
  - `.github/workflows/e2e.yml:86`, `.github/workflows/upgrade-e2e.yml:138`, `.github/workflows/_release-components.yml:31-36`, `.github/workflows/release.yml:62`
  - `.gitignore:13`, `.dockerignore:9`
  - `tests/e2e/framework/components_bootstrap.go:155` (drop from `builtinE2EComponents`)
  - `tests/e2e/scenarios_test.go:120-127` (delete the `read-back` row)
  - `tests/e2e/scenarios_registry_test.go:240` (delete `TestErrorMissingTableRejected`)
  - `tests/release-verify/pipeline.yaml:14-22`: replace the `read` stage with a `sql-transform` stage reading `verify.data` and writing `verify.data_copy` with `sql: SELECT * FROM data` (check the `inputs`/`outputs.tables` shape against `tests/e2e/pipelines/k8s/duckdb-etl.yaml:19-35`). The contract is in Review Focus #5: keep `${PIPELINE_NAME}`, leave `version` omitted on both components, and make sure the two-stage run reaches `Succeeded` (the `sql-transform` ComponentDefinition is a chart built-in, so it resolves on a fresh install).
  - `examples/pipelines/processors-drop.yaml:1-5,45-55`: delete stage 2 (`load`), fix the header comment ("then stdout-writer prints what's left" → "inspect `raw.products` in the storage browser or `datuplet storage`"), `Components:` line → `data-generator`.
  - `examples/README.md:12-13`: drop the incremental-reads row, fix processors-drop components.
  - `pkg/pipeline/validate/registry_test.go:296,301,307,314,319,325`: rename the registry key to `sink` and the two expected messages accordingly (`"component sink takes no outputs"`, `"component sink requires at least one output"`). Task 10 later merges these tests; just rename here.
  - `docs/components.md:121,130,432-470`, `docs/pipeline-api.md:477` ("e.g. stdout-writer" → drop the example), `docs/agent-quickstart.md:73`, `docs/quickstart-kind.md:85`, `README.md:48,99`
  - `.claude/skills/datuplet-operator/references/components.md:28,42` (drop the row and the "see the rows while debugging" hint — replace the hint with `datuplet storage preview` / the UI storage browser)
  - `.claude/skills/datuplet-component-author/SKILL.md:51` and `references/build-a-component.md:95`: there is no built-in sink any more; say so and point at `sdk/go` `OpenReader` for the read side.
  - `ui/product/pages/pipeline-detail.js:761` (comment)

- [ ] **Step 1:** `git rm` the six Delete paths.
- [ ] **Step 2:** Edit every Modify reference.
- [ ] **Step 3: Verify**

Run the Global Constraints sweep restricted to the three stdout-writer patterns. Expected: no output.

```bash
make tidy && go build ./... && go test ./pkg/pipeline/validate/ && (cd tests/e2e && go vet ./... && go test -run TestK8sFixturesAreValidDocs ./...) && go test ./examples/ && make sync-component-schemas && git diff --exit-code charts/datuplet-app/files/component-schemas && helm template x charts/datuplet-app --namespace datuplet > /dev/null
```
Expected: exit 0 (the fixture-guard test parses every remaining `pipelines/k8s/*.yaml`; the examples guard parses `examples/pipelines/*.yaml`).

- [ ] **Step 4: Commit** `chore: remove stdout-writer component; repoint release-verify and examples to sql-transform`

---

### Task 4: Cut the e2e suite (appendix Part E §5) and run it live

Adopted from Part E. Decisions where the audit left a choice: **keep** `TestRegistry_ErrorSchemaInvalid` (cheap, no pods), **keep** `TestAgentLoop_UserApps` (only CLI-apps e2e), **do not add** cancel/reaper/error-exit scenarios (new coverage is out of scope; note them in the task report as a follow-up), **attempt** the `write-modes` merge with the fallback below.

**Files:** `tests/e2e/scenarios_test.go`, `scenarios_registry_test.go`, `scenarios_resources_test.go`, `scenarios_query_test.go`, `scenarios_audit_test.go`, `framework/*.go`, `pipelines/k8s/*.yaml`

- [ ] **Step 1: Delete scenarios + fixtures.** `TestScenarios` rows `explicit-tables`, `http-json-extract`, `duckdb-etl`, `multi-component-stage`, `fga-matrix-alice-trigger-and-browse`, and the commented `drop-processor` block; top-level `TestRegistry_UnknownComponent`, `TestResources_ClampedAtAdmission`, `TestResources_OverMaxModificationRejected`, `TestQuery_Truncation`, `TestQuery_Timeout`, `TestQuery_Concurrency`, `TestQuery_AuditSmoke`, `TestQueryBootstrap` (fold `ensureAdminFGAGrant` into `getQueryProjectID` with a `sync.Once`). Fixtures: `unknown-component.yaml`, `resource-clamp.yaml`, `resource-default.yaml`, `drop-processor.yaml`, `explicit-tables.yaml`, `duckdb-etl.yaml`, `multi-component-stage.yaml`, `error-crash.yaml` (orphan). Keep `http-json-extract.yaml` (the query seed uses it) and `multi-table-join.yaml`.
- [ ] **Step 2: Merges.**
  - `fga-matrix-bob-trigger-and-browse` gains the `schema` assertion that `http-json-extract` carried; switch its `min_row_count` to `row_count: 100`.
  - `full-load-mode` + `append-mode` → one scenario `write-modes`: a new `pipelines/k8s/write-modes.yaml` with two `http-json-extractor` components in one stage, one `defaultWriteMode: FULL_LOAD` → bucket `{{.RunPrefix}}-full`, one `APPEND` → `{{.RunPrefix}}-append`; `RunTwice: true`; assertions `row_count 100` on full, `row_count 200` on append. Under `RunTwice` this is 2 pipeline runs regardless of component count (`framework/scenario.go:208-216`). **Fallback, only on the known race:** if the second run fails with the cancel-watch signature documented at `scenarios_test.go:67-77` (capture the operator/observer diagnostics and quote them in the report) twice in a row, keep `append-mode.yaml` and its skipped row with the existing `SkipReason`, and make `write-modes` FULL_LOAD-only. Any other failure is a real failure: fix or stop, do not fall back.
  - `TestRegistry_UnpinnedResolvesStable` absorbs `TestResources_RegistryDefaultsApplied`'s Job + `resolvedSpec` assertions (same fixture `unpinned-resolution.yaml`, already no `resources` block).
  - `TestResources_SuperadminPutSucceeds` becomes `TestResources_PutGate` with three subtests: nonadmin 403, superadmin over-max 400, superadmin within-max 204, using the existing inline YAML constants.
  - `TestAuditTrail_ClusterRun` reuses the query seed run instead of running `explicit-tables.yaml`, but the seed must be isolated: today `ensureQueryTable` (`scenarios_query_test.go:164-193`) writes `<runPrefix>-api.data`, and the surviving Bob scenario writes the same table via `http-json-extract.yaml`, so under `-shuffle` the "current snapshot" the audit reads (`scenarios_audit_test.go:115-147`) could belong to Bob's run. Give the seed its own prefix (e.g. `<runPrefix>-seed`), have `ensureQueryTable` return `(namespace, table, runID)` via a `sync.Once`, and have the audit test consume that triple. Keep the zero-RunID check and all four audit assertions. Verify with `go test -run TestAuditTrail -count=1` alone and then the whole suite with `-shuffle on`.
- [ ] **Step 3: Delete dead framework code and every caller.** Declarations (Part E §6 "already dead"): `framework/backdate.go`, `framework/secrets.go`, `framework/k8s_token.go` + `k8s_token_test.go` + `MintTestUserRunToken` in `signer.go`, `Scenario.{SetupPipeline,SetupDelay,BackdateSetup,BackdateBucket,BackdateTable}`, `RunOpts.{StorageRoot,Env,SecretsDir}`, `K8sBackend.Resources` + the `Backend` interface, assertion types `column_absent`/`query`/`min_row_count` and their `Assert*` funcs, `TemplateVars.{TestDataDir,WarehouseDir}`, `CharlieID`/`DoraID`; then `intPtr`, `extractAuditLines` in `scenarios_query_test.go`. Callers that must go in the same step or the module stops compiling: the `TestDataDir` initializers at `scenarios_secrets_test.go:210` and `framework/scenario.go:163`; the setup-field reads at `framework/scenario.go:167-194`; the assertion `switch` arms at `framework/scenario.go:291`, `:305`, `:312`; the `Resources` cleanup loop at `framework/k8s_runner.go:411`; the Charlie/Dora rows in `TestUsers` at `framework/users.go:53-54`. Do **not** delete `scenarios_resources_test.go` wholesale — it holds `grantAdminSuperadmin` etc. used by `TestMain`.
- [ ] **Step 4: Offline verify**

```bash
cd tests/e2e && go vet ./... && go test -count=1 -run 'TestK8sFixturesAreValidDocs' ./... && go run golang.org/x/tools/cmd/deadcode@latest -test ./...
```
Expected: vet clean, fixture guard passes, deadcode reports nothing in `framework/`.

- [ ] **Step 5: Live verify on OrbStack**

```bash
set -o pipefail; make e2e-k8s 2>&1 | tee /tmp/e2e-task4.log; echo "exit=$?"
grep -E '^(--- (PASS|FAIL|SKIP)|ok|FAIL)' /tmp/e2e-task4.log | sort | uniq -c
```
Expected: `exit=0`; every remaining test PASS; the only SKIPs are `error-bad-config` (and `append-mode` if the fallback was taken), `TestBigDataJoinProof`, and `TestQuery_NetworkPolicyEgress` on a non-enforcing CNI. Paste the tally into the task report. Count pipeline runs from the surviving invocations (`RunPipeline` log lines): expected 9 — write-modes 2, bob 1, seed 1 (audit reuses it), secrets 1, unpinned+defaults 1, freeze 1, agent-loop 1, error-schema-invalid 0, PutGate 0 — plus 1 if `TestAgentLoop_UserApps` triggers one.

- [ ] **Step 6: Commit** `test(e2e): keep one scenario per deployment behaviour; drop happy-path repeats and dead framework code`

---

### Tasks 5–11: unit-test trims

Shared procedure for each task (steps are identical; only the package list and appendix part differ):

- [ ] **Step 1:** Read the appendix section(s) for the task's packages. Apply every **DELETE** line and every **MERGE** line. Merges become table-driven tests (`[]struct{name string; ...}` + `t.Run`) with one row per original test; keep the original assertions' *meaning* (status codes, sentinels, types), drop exact-wording `Contains` checks only where the appendix says the wording is not the contract.
- [ ] **Step 2:** For "optional" / "borderline" lines, follow the per-task decision list below; otherwise leave them.
- [ ] **Step 3:** `gofmt -l` the touched files (expect empty), then run the package tests with `-count=1`. Then `go vet` and the `deadcode` lint on the module.
- [ ] **Step 4:** In the task report, give the table: package → test funcs before/after, test LOC before/after (`grep -c '^func Test'`, `wc -l`), and any appendix line you skipped with the reason.
- [ ] **Step 5:** Commit: `test(<area>): consolidate into table-driven tests; drop duplicates (audit 2026-10-28)`.

#### Task 5: `pkg/appworker` (Part A)
Decisions: delete both CSV golden Go-port tests and helpers; delete all 11 cosmetic/source-grep tests listed; delete `TestQuery_PropagatesContextCancellation`; the optional TTL fake-clock consolidation → do it; trim `TestMetrics_CounterMovesPerOutcome` to ok + one error row. **Overrides of the appendix (Codex finding 2):** keep `TestQueryRunnerEnforcesBudgetIndependently` (`render_test.go:718`) — it is the only direct pin of the runner's own query limit; the survivor at `:683` exercises engine + runner together and cannot see a broken runner limit. Keep `TestPerAppGateIsKeyedPerApp` (`render_test.go:1216`) — the claimed survivor `TestPoolGateReturnsCapacityAfterBoundedWait` sets per-app limit 2 with one active request and tests the global pool, so it cannot detect a per-app gate that is accidentally global.
Verify: `go test -count=1 ./pkg/appworker/`.

#### Task 6: `pkg/pipelineapi/http`, `pkg/pipelineapi/apps` (Part A)
Decisions: add the `seedProject` helper and use it in the ~30 DB-backed tests; **keep** `TestAppQueryRoute_CancelledContextAbortsBlockingQuery` (end-to-end cancellation proof; its appworker twin was deleted in Task 5); keep the exact 403 envelope assertion in `TestQueryTokenHandler_PolicyOff_Forbidden`.
Verify: `go test -count=1 ./pkg/pipelineapi/http/ ./pkg/pipelineapi/apps/` (DB-backed tests skip without `TEST_DATABASE_URL`; if a local Postgres is available, run once with it set).

#### Task 7: `pkg/pipelineapi/{storage,queryproxy,k8s,tokens,auth,authz,store}` (Part A)
Decisions: **keep** `TestHandler_NeverLogsTokens` (redaction, belt-and-braces); **keep** `TestListAllTables_BoundedParallelLoad` (pins the documented 8-wide bound); `auth/password_test.go` was not readable by the auditor — read it and apply the proposed 4→1 merge only if the tests are what their names say. In `tokens/cli_api_test.go`, `TestMintCLIAPIToken_AudienceDistinctFromCatalog` (`:106`) may be folded into the merged ClaimsShape table only if that table gains a **mandatory** assertion `APITokenAudience != TableTokenAudience` (Codex finding 2: `ClaimsShape` compares against the constant, so changing the constant to the catalog audience would otherwise pass).
Verify: `go test -count=1 ./pkg/pipelineapi/...`.

#### Task 8: `cmd/datuplet`, `cmd/pipeline-api` (Part B)
Decisions: apply all, except: **keep both** `resolveQuerySQL` (`cmd/datuplet/query_test.go:16`) and `resolveSQL` (`components/queryengine/cmd/datuplet-query/flags_test.go:33`) precedence tests — they pin two independent implementations (`cmd/datuplet/query.go:45` and `components/queryengine/cmd/datuplet-query/flags.go:62`, neither calls the other). Sharing the code would be a refactor outside this plan.
Verify: `go test -count=1 ./cmd/...`.

#### Task 9: `pkg/datagateway`, `pkg/datagateway/{backend,format,buffer,schema}`, `pkg/catalogwriter`, `pkg/icebergjob` (Part B)
Decisions: **delete** both `membench_test.go` files outright (not convert); share one `testRangeReaderAt` helper between the MinIO and GCS range-reader tables; keep every `TestScrubBody*` row and every `parseCreds` reject row.
Verify: `go test -count=1 ./pkg/datagateway/... ./pkg/catalogwriter/ ./pkg/icebergjob/`.

#### Task 10: `pkg/k8s/controllers`, `pkg/pipeline/{validate,config,schemalint}`, `sdk/go`, `sdk/go/arrow` (Part B)
Decisions: do the optional `schemalint` table merge (no rule lost); do the optional `TestValidatePipeline_TableCases` positive-twin trims; the `registry_test.go` IO-capability merge uses the `sink` name from Task 3; delete the 13 façade lifecycle duplicates in `sdk/go/arrow` but keep the two sticky-failure façade tests.
Verify: `go test -count=1 ./pkg/k8s/... ./pkg/pipeline/...` and `(cd sdk/go && go test -count=1 ./...)` and `(cd sdk/go/arrow && go test -count=1 ./...)`. `TestLint_BuiltinsCleanZeroIssues` must still pass against the three remaining schemas.

#### Task 11: `components/queryengine` (all packages), `components/data-generator`, `components/http-json-extractor` (Part B)
Decisions: apply all; keep every `verify_test.go` claim-mutation case as a row; for `leak_test.go` keep `TestFinishRandomQuietly_SafeAfterFailedFlush` in data-generator (it is that module's only pin) — do not rely on the sdk twin.
Verify, two separate commands so neither can mask the other:

```bash
for m in components/queryengine components/data-generator components/http-json-extractor; do (cd $m && go test -count=1 ./...) || exit 1; done
```
Expected: exit 0. Then the `duckdb_arrow`-tagged tests in `components/queryengine`: first check `go vet -tags duckdb_arrow ./...` builds; if it fails only because the DuckDB library is missing on this machine, say so in the report and stop there; if it builds, `go test -count=1 -tags duckdb_arrow ./...` must exit 0 with its output in the report.

---

### Task 12: Final sweep, lint, docs, push

**Files:** `CLAUDE.md`, `README.md`, `docs/components.md`, `CHANGELOG.md`, `Makefile` comments, `charts/datuplet-app/values.yaml` comment, `.github/workflows/*.yml` comments

- [ ] **Step 1: Reference sweep.** Run the full Global Constraints sweep command. Expected: no output. Fix anything it prints.

- [ ] **Step 2: Counts and prose.** `docs/components.md` intro ("six built-in" → three; the IO table), `values.yaml` tag comment, `README.md:99` list, `Makefile` help strings that enumerate components, `_release-components.yml` header. `CLAUDE.md` Key directories: `components/` row → name the three; `sdk/go/` row only. `CHANGELOG.md` `[Unreleased]` → add under **Removed**: Python SDK + python-sandbox, `pandas-transform`, `finnhub-extractor`, `stdout-writer`, `tests/integration/`; under **Changed**: "test suites consolidated; e2e keeps one scenario per deployment behaviour".
- [ ] **Step 3: Whole-repo verification**

```bash
make tidy && git diff --exit-code '**/go.mod' '**/go.sum'
make lint
go test -count=1 ./...
for m in components/queryengine components/data-generator components/http-json-extractor components/sql-transform sdk/go sdk/go/arrow tests/e2e; do (cd $m && go vet ./... && go test -count=1 ./...); done
make sync-component-schemas && make sync-appshell-schema && git diff --exit-code charts/datuplet-app/files/component-schemas ui/appshell/vegaspec.schema.json
for c in datuplet-operators datuplet-infra datuplet-app datuplet-lakekeeper; do helm lint charts/$c && helm template $c charts/$c --namespace datuplet > /dev/null; done
scripts/verify-versions.sh
```
Expected: every command exits 0. Record test-LOC before/after for the whole repo: `git ls-files '*_test.go' | xargs cat | wc -l` on `main` vs now.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "docs: update component lists, CLAUDE.md and CHANGELOG for the removals"
```

- [ ] **Step 5: Whole-branch review gate (before any push).** This branch touches CODEOWNERS paths (`pkg/pipelineapi/{auth,authz,tokens}`, `Makefile`, `.github/workflows/`), so REVIEW.md's High tier applies:
  1. `/code-review max adc765f...HEAD` (local), then `/security-review` on the branch.
  2. Codex whole-branch review via the companion runner: `review --scope branch --base adc765f`.
  3. Any Important finding → stop and present to Tomas; no fix without approval; Codex re-reviews after approved fixes. Nits go in the PR body.
  Record all three summaries (counts first) in the task report.

- [ ] **Step 6: Push**

```bash
git push public docs/review-guardrails
```
Then report: PR #44 URL, the before/after table (components, test files, test LOC, e2e pipeline runs), and the follow-ups surfaced by the audit that were deliberately not done here (no live cancel/reaper/error-exit e2e; `components/sql-transform` has zero unit tests; `TestQuery_NetworkPolicyEgress` self-skips on OrbStack and kind).

---

## Self-review notes

- Spec coverage: Python SDK + dependents → T1; finnhub → T2; stdout-writer + "data-generator covers it" → T3 (with the finding that the coverage never lived in stdout-writer); fewer tests incl. e2e → T4–T11; "do it in the current PR" → branch constraint; "each task has a model" → table above.
- Tasks 5–11 rely on the appendix for the per-test lists; the plan carries only the decisions the appendix left open. An executor who finds an appendix `file:line` that no longer matches (Task 3 and Task 4 shift lines in `tests/e2e` and `registry_test.go`) locates the test by name, not line.
- Proportion: this plan is ~330 lines for ~15k LOC of deletions; the appendix is the long part and is data, not instructions.
