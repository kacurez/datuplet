# K8s Object Names From IDs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-run K8s objects get names derived only from generated IDs, so user-chosen pipeline, stage and component names never end up in an object name. Component names that still appear in a label value are validated when the pipeline is saved. A run never adopts per-run objects owned by another PipelineRun.

**Architecture:** PipelineRun becomes `run-<runUUID>`, and the pipeline name moves to an annotation. Component Jobs and their gateway ConfigMaps become `run-<runUUID>-s<stageIdx>-c<compIdx>`, with the indexes looked up in the frozen `status.resolvedSpec`. The per-run secrets snapshot becomes `datuplet-runsecrets-<runUUID>`. The run UUID is normalized at admission, so names are always lowercase. When an existing per-run object is found, the controller checks it is controlled by this PipelineRun (by UID). If it isn't, the run fails. The `datuplet.io/component=<name>` label stays, and pipeline validation now requires every component instance name to be a valid DNS-1123 label.

**Tech Stack:** Go, controller-runtime, `k8s.io/apimachinery/pkg/util/validation`, `metav1.IsControlledBy`.

**Spec:** No separate spec. This plan comes from the 2026-11-07 naming audit and its Codex review, summarized under "Audit findings" below.

## Audit findings (the spec for this plan)

User text currently reaches K8s object names or labels in these places:

| # | Object | Current name source | File |
|---|---|---|---|
| 1 | PipelineRun | `<pipeline>-<uuid>`, plus a truncate-and-hash fallback | `pkg/pipelineapi/k8s/run_create.go:54` |
| 2 | component Job | `datuplet-<pr[:15]>-<stage[:10]>-<comp[:10]>-<runID[:8]>` | `pkg/k8s/controllers/pipelinerun_jobs.go:28` |
| 3 | gateway ConfigMap | `gateway-config-<jobName>` | `pkg/k8s/controllers/pipelinerun_jobs.go:84` |
| 4 | `datuplet.io/component` label | raw `comp.Name` | `pkg/k8s/controllers/pipelinerun_jobs.go:186,197` |
| 5 | run UUID in names | `spec.runId` is checked with `uuid.Parse` but stored verbatim (uppercase, compact or `urn:uuid:` forms pass) | `pkg/k8s/controllers/pipelinerun_controller.go:415` |

Ownership and collision bugs:
- The Job/ConfigMap create calls swallow `IsAlreadyExists` without checking who owns the existing object (`pipelinerun_controller.go:948,954`). The poller fetches the Job by name the same way (`:1021`). Today this bites on 8-char run-ID prefix collisions. After the rename it would bite on two PipelineRuns created directly with the same `spec.runId`. Either way, the second run adopts the first run's Job and reports its result.
- The secrets snapshot is reused when one exists by name (`pipelinerun_controller.go:688`), with no ownership check (pre-existing). Its Create (`:737`) also swallows `IsAlreadyExists`, so a snapshot created between the lookup and the Create is adopted too.
- The preview-timeout branch (`pipelinerun_controller.go:792`) runs before stage creation or polling. It reads logs from and deletes the Job by name (`:854`, `:866`) without any ownership check.

Known and NOT in scope (pre-existing, not name-dependent): cancel propagation patches pods selected only by the `datuplet.io/run-id` label (`propagateCancelToPods`, `:851` → `:1167`), so two PipelineRuns with the same `spec.runId` would cancel each other's pods. pipeline-api always generates a fresh UUID; only hand-created PipelineRuns can collide.

Stage and component names are only checked for being non-empty (`pkg/pipeline/validate/validate.go:205,305`). Stage names are unbounded, so they must not be copied into annotations (Codex finding 3).

Already safe and NOT in scope: Namespace (`datuplet-<projectUUID>`, enforced by a DB CHECK), `runtoken-<uuid>`, `preview-<uuid>`, RBAC Roles/Bindings, the project secrets Secret, LimitRange (fixed names), container names.

Explicitly NOT in scope: renaming the Pipeline CR, renaming ComponentDefinition, validating stage names, any CRD schema change, any migration path (fresh installs only).

## Global Constraints

- New PipelineRun name: exactly `"run-" + runID.String()`.
- New Job name: exactly `fmt.Sprintf("run-%s-s%d-c%d", pr.Status.RunID, stageIdx, compIdx)`. Indexes are 0-based positions in `pr.Status.ResolvedSpec.Stages[stageIdx].Components[compIdx]`.
- ConfigMap name scheme stays `"gateway-config-" + jobName`.
- Run-secrets snapshot name: exactly `"datuplet-runsecrets-" + pr.Status.RunID`.
- `pr.Status.RunID` is always the canonical form `uuid.UUID.String()` (lowercase, hyphenated, 36 chars).
- The only new annotation is `datuplet.io/pipeline-name` on the PipelineRun (pipeline names are already capped at 253 chars). No stage-name or component-name annotations.
- An existing Job, ConfigMap or snapshot that is not `metav1.IsControlledBy(obj, pr)` fails the run with `FailedApplication`, message `fmt.Sprintf("%s %s already exists and belongs to another PipelineRun", kind, name)`.
- Component instance names must pass `validation.IsDNS1123Label`.
- No CRD changes. Don't touch `charts/datuplet-app/crds/`.
- Run `go build ./... && go test ./...` before each commit (DB-backed packages need `TEST_DATABASE_URL` and `-p 1`). Run `make lint` before the PR. Run `make e2e-k8s` at the end, because this is a controller change.

## Review Focus

1. **Poller/builder/timeout drift:** the Job name used when polling (`pipelinerun_controller.go:1018`) and on preview timeout (`:854`) must equal the name used when building (`pipelinerun_jobs.go:83`) for components in stage > 0 and for the second component of a stage. Pinned by `TestComponentJobName_PositionalAndStable` and the extended preview-timeout test (Task 2).
2. **Same `spec.runId` on two PipelineRuns:** the second must fail `FailedApplication`, not adopt the first run's Job, ConfigMap or snapshot (including a snapshot that appears between lookup and Create), and its preview timeout must not delete the first run's Job. Pinned by `TestReconcile_ForeignOwnedObjectsFailRun`, `TestSnapshot_CreateRaceForeignOwned` and `TestHandleRunning_PreviewDeadline_ForeignJobUntouched` (Task 2).
3. **Non-canonical `spec.runId`** (uppercase, compact, `urn:uuid:` form): admission stores the canonical form, and every derived name is a valid DNS-1123 label. Pinned by `TestAdmission_CanonicalizesRunID` (Task 2).
4. **Cancel and observer still find the run after the rename:** `runbackend` Cancel (`pkg/pipelineapi/runbackend/k8s.go:378`) and the observer's decoy guard (`pkg/pipelineapi/k8s/db_updater.go:55`) must compute the creator's name. Pinned by `TestDBRunUpdater_RejectsDecoyName` plus the run_create test (Task 3).
5. **Long pipeline names (up to 253 chars, with dots):** must no longer affect PipelineRun creation. Pinned by `TestPipelineRunName_IgnoresPipelineName` and the extended CreateRunResources test (Task 3).

---

### Task 1: Component instance names must be DNS-1123 labels

**Files:**
- Modify: `pkg/pipeline/validate/validate.go` (`validateComponent` near line 305; stale comment at line 233)
- Test: `pkg/pipeline/validate/validate_test.go`

**Interfaces:**
- Produces: the guarantee that every component `name` in a saved pipeline satisfies `validation.IsDNS1123Label`. Task 2 relies on it for the label value.

- [ ] **Step 1: Write the failing test** `TestValidateComponentName_DNSLabel` (table-driven):
  - These names must each produce a finding at path `stages[i].components[j].name` with severity error: `"Load_Customers"`, `"load customers"`, `"a.b"`, `"-x"`, `"x-"`, `strings.Repeat("a", 64)`.
  - These must produce no name finding: `"c1"`, `"http-json-extractor-2"`, `strings.Repeat("a", 63)`.
  - An empty name still yields only the existing "name is required" finding.
- [ ] **Step 2: Run** `go test ./pkg/pipeline/validate/ -run TestValidateComponentName_DNSLabel -v`. Expected: FAIL.
- [ ] **Step 3: Implement.** In `validateComponent`, when `c.Name != ""`, append one error finding per message from `validation.IsDNS1123Label(c.Name)`. Message: `fmt.Sprintf("stage %d, component %d: invalid component name %q: %s", stageIdx, compIdx, c.Name, msg)`. Path: the same `base + ".name"` as the "required" finding. Rewrite the comment at line 233: names must be unique because the `datuplet.io/component=<name>` label identifies a component's pods within a run.
- [ ] **Step 4: Run** `go test ./pkg/pipeline/... ./pkg/pipelineapi/http/...` and the examples guard test (find it with `grep -rln "examples/pipelines" --include='*_test.go' .`). Expected: PASS. Rename any fixture that uses an invalid component name; never loosen the check.
- [ ] **Step 5: Commit** `fix(validate): require component names to be DNS-1123 labels`.

### Task 2: Controller-built names come from the run ID and positions; ownership is checked

**Files:**
- Modify: `pkg/k8s/controllers/pipelinerun_jobs.go` (`componentJobName` lines 20-35, `runSecretsName` line 40, `buildComponentJob` line 83)
- Modify: `pkg/k8s/controllers/pipelinerun_controller.go` (run ID admission ~line 414; snapshot reuse ~688; preview timeout ~854; Job/ConfigMap create ~948-957; poller ~1018-1027; `shortID` at line 75: delete it if no callers remain)
- Modify tests that call `componentJobName(x, "<stage>", "<comp>")`: `pipelinerun_jobs_test.go`, `pipelinerun_secrets_test.go`, `pipelinerun_controller_test.go`, `pipelinerun_freeze_test.go`, `resource_clamp_admission_test.go`, `pipelinerun_imageid_test.go` (all in `pkg/k8s/controllers/`)
- Modify: `tests/e2e/scenarios_secrets_test.go:238-241` (snapshot name)

**Interfaces:**
- Consumes: Task 1's guarantee (component names are unique across the pipeline and are DNS labels).
- Produces:
  - `func componentJobName(pr *datupletv1.PipelineRun, compName string) (string, error)` looks up `compName` in `pr.Status.ResolvedSpec` and returns `run-<RunID>-s<i>-c<j>`. Returns an error if `ResolvedSpec` is nil or the name is absent.
  - `func runSecretsName(pr *datupletv1.PipelineRun) string` returns `"datuplet-runsecrets-" + pr.Status.RunID`.
  - `var errForeignOwned = errors.New("object belongs to another PipelineRun")` and `func checkOwned(obj client.Object, pr *datupletv1.PipelineRun, kind string) error`, which returns `fmt.Errorf("%s %s already exists and belongs to another PipelineRun: %w", kind, obj.GetName(), errForeignOwned)` when `!metav1.IsControlledBy(obj, pr)`, and nil otherwise.
  - Test helper `func mustJobName(t *testing.T, pr *datupletv1.PipelineRun, comp string) string` in `pipelinerun_jobs_test.go`, which calls `t.Fatal` on error.

- [ ] **Step 1: Write the failing name tests** in `pipelinerun_jobs_test.go`. Replace `TestComponentJobName_IncludesRunIDSuffix` with:
  - `TestComponentJobName_PositionalAndStable`: stage 0 `{a, b}`, stage 1 `{c}`, RunID `11111111-2222-3333-4444-555555555555`. Expect `a` → `run-11111111-2222-3333-4444-555555555555-s0-c0`, `b` → `…-s0-c1`, `c` → `…-s1-c0`. Each result passes `validation.IsDNS1123Label`. Repeated calls return the same value.
  - `TestComponentJobName_FullRunIDNoPrefixCollision`: RunIDs `aaaaaaaa-0000-0000-0000-000000000001` and `aaaaaaaa-0000-0000-0000-000000000002` with the same spec give different names for the same component.
  - `TestComponentJobName_UnknownComponent`: an unknown name returns an error, and so does a nil `ResolvedSpec`.
  - `TestComponentJobName_NoUserText`: with PipelineRun name `my-secret-pipeline`, a stage named `ingest` and a component named `extract-orders`, the result contains none of those strings.
  - `TestRunSecretsName_FullRunID`: equals `"datuplet-runsecrets-" + RunID`.
- [ ] **Step 2: Write the failing reconcile tests** in `pipelinerun_controller_test.go` (fake-client pattern already used there):
  - `TestAdmission_CanonicalizesRunID`: `spec.runId` values `"11111111-AAAA-4444-8888-111111111111"`, `"11111111aaaa44448888111111111111"` and `"urn:uuid:11111111-aaaa-4444-8888-111111111111"` each give `status.runId == "11111111-aaaa-4444-8888-111111111111"` after admission. The created Job name passes `validation.IsDNS1123Label`.
  - `TestReconcile_ForeignOwnedObjectsFailRun`, table-driven over Job, ConfigMap and snapshot Secret: pre-create the object under the name this run will derive, controlled by a different PipelineRun (different UID), with the same `spec.runId`. Reconciling the run leaves it in `FailedApplication`, with a message containing `"belongs to another PipelineRun"`, and the foreign object is unchanged.
  - `TestSnapshot_CreateRaceForeignOwned`: the initial snapshot lookup returns NotFound, and the Create returns AlreadyExists because a foreign-owned Secret appeared in between. Use a fake client with interceptor funcs (`sigs.k8s.io/controller-runtime/pkg/client/interceptor`) whose `Create` hook inserts the foreign Secret and then returns AlreadyExists. The run ends in `FailedApplication` with `"belongs to another PipelineRun"`; the foreign Secret is unchanged. (Pre-creating the Secret only exercises the initial lookup, so it does not cover this.)
  - Fix `TestHandleRunning_PreviewDeadline` (`pipelinerun_controller_test.go:304`, ~line 320): compute the Job name only after `Status.ResolvedSpec` is set, and add a second stage so the timed-out component sits at `s1-c0`. Assert that exact Job is deleted.
  - `TestHandleRunning_PreviewDeadline_ForeignJobUntouched`: same setup, but the Job under the derived name is controlled by a different PipelineRun. After the deadline the run is in `FailedApplication` with `"belongs to another PipelineRun"`, and the foreign Job still exists, unchanged.
- [ ] **Step 3: Run** `go test ./pkg/k8s/controllers/ -run 'TestComponentJobName|TestRunSecretsName|TestAdmission_CanonicalizesRunID|TestReconcile_ForeignOwnedObjectsFailRun|TestSnapshot_CreateRaceForeignOwned|PreviewDeadline' -v`. Expected: FAIL (compile errors count as failures).
- [ ] **Step 4: Implement:**
  - **Admission (~414):** keep the parsed `uuid.UUID` and set `runID = parsed.String()` before it is written to status.
  - **Names:** implement `componentJobName` and `runSecretsName` as specified. In `buildComponentJob`, call `componentJobName(pr, comp.Name)` and return its error. Update the comment above `componentJobName`: built only from the run ID and positions; the single source of truth for builder, poller and timeout.
  - **Call sites:** `:854` returns the error; `:1018` logs and `continue`s, same as the current NotFound branch.
  - **Ownership:** on `IsAlreadyExists` from the ConfigMap or Job create (`:948`, `:954`), `Get` the existing object and call `checkOwned`. After the poller's `Get` (`:1021`), call `checkOwned` on the Job. In the snapshot path (`:688`), call `checkOwned` on the existing Secret before returning early. On `IsAlreadyExists` from the snapshot Create (`:737`), fetch the existing Secret through `r.APIReader` (the cache may not have it yet), call `checkOwned`, and return its error.
  - **Preview timeout (`failPreviewTimeout`, ~846-866):** before reading logs or deleting, `Get` the Job and call `checkOwned`. NotFound keeps today's behaviour. A foreign-owned Job is neither read nor deleted: `failPreviewTimeout` itself applies the inline FailedApplication pattern below (it already returns `(ctrl.Result, error)`) and stops processing the remaining components. Delete with `client.Preconditions{UID: &job.UID}` so only the Job just verified can be removed. Leave `propagateCancelToPods` as is (out of scope, see Audit findings).
  - **Failure handling:** there is no general fail helper. For any `errors.Is(err, errForeignOwned)`, use the same inline pattern as the invalid-`spec.runId` block (~line 419): set `pr.Status.Phase = datupletv1.PipelineRunPhaseFailedApplication`, set `pr.Status.Message = err.Error()`, call `r.Status().Update(ctx, pr)` and return `ctrl.Result{}, nil`. Never requeue, and never mutate or delete the foreign object. The snapshot path returns `(…, error)`, so the reconcile caller must check `errForeignOwned` before its generic error handling.
- [ ] **Step 5: Update the other controller tests** to use `mustJobName(t, got, "<comp>")`, computed after the resolved spec exists. Update `tests/e2e/scenarios_secrets_test.go` to expect `"datuplet-runsecrets-" + result.RunID.String()` and fix its comment.
- [ ] **Step 6: Run** `go build ./... && go test ./pkg/k8s/...`, then `(cd tests/e2e && go vet ./...)`. Expected: PASS.
- [ ] **Step 7: Commit** `fix(operator): name run objects by canonical run ID and refuse foreign-owned objects`.

### Task 3: PipelineRun named `run-<uuid>`; pipeline name in an annotation

**Files:**
- Modify: `pkg/pipelineapi/k8s/run_create.go` (doc comments and `PipelineRunName` lines 41-84; PipelineRun ObjectMeta line 133)
- Modify: `pkg/pipelineapi/runbackend/k8s.go:374-378`
- Modify: `pkg/pipelineapi/k8s/db_updater.go:36-59`
- Test: `pkg/pipelineapi/k8s/run_create_test.go`, the db_updater test file (`grep -ln DBRunUpdater pkg/pipelineapi/k8s/*_test.go`)

**Interfaces:**
- Produces: `func PipelineRunName(runID uuid.UUID) string` (package-level in `pkg/pipelineapi/k8s`, next to `PreviewPipelineRunName`) returns `"run-" + runID.String()`. The method `CreateRunOpts.PipelineRunName()` is removed. `CreateRunOpts.PipelineName` stays, because it fills `spec.pipelineRef.name` and the annotation.

- [ ] **Step 1: Write the failing tests:**
  - `TestPipelineRunName_IgnoresPipelineName`: `PipelineRunName(id) == "run-"+id.String()`, and it passes `validation.IsDNS1123Label`.
  - Extend the CreateRunResources test with `PipelineName = strings.Repeat("a", 60) + ".b." + strings.Repeat("c", 100)`. Creation succeeds. The PipelineRun is named `run-<id>`, has annotation `datuplet.io/pipeline-name` equal to the full pipeline name, and `spec.pipelineRef.name` equals the pipeline name. Delete any test of the old truncate-and-hash fallback.
  - `TestDBRunUpdater_RejectsDecoyName` (DB-gated, same pattern as its neighbours): a status with the right RunID and namespace but name `"<pipeline>-<uuid>"` is dropped (`applied == false`). With name `PipelineRunName(runID)` it is applied. A wrong namespace is still dropped.
- [ ] **Step 2: Run** `go test ./pkg/pipelineapi/k8s/ -run 'TestPipelineRunName|TestCreateRunResources|TestDBRunUpdater' -v` (with `TEST_DATABASE_URL` set for the DB test). Expected: FAIL.
- [ ] **Step 3: Implement:**
  - Replace the method and its fallback with the package-level function. Drop imports that become unused.
  - Add `Annotations: map[string]string{"datuplet.io/pipeline-name": opts.PipelineName}` to the PipelineRun.
  - In `runbackend/k8s.go`, use `pkg8s.PipelineRunName(req.RunID)` and drop the `GetPipelineNameByID` call there.
  - In `db_updater.go`, compare against `PipelineRunName(s.RunID)`. Keep the namespace check. Replace the query with `SELECT p.k8s_namespace FROM runs r JOIN projects p ON p.id = r.project_id WHERE r.id = $1`.
  - Delete `store.GetPipelineNameByID` if it has no callers left (`make lint` runs `deadcode`).
- [ ] **Step 4: Run** `go build ./... && TEST_DATABASE_URL=… go test -p 1 ./pkg/pipelineapi/...`. Expected: PASS.
- [ ] **Step 5: Commit** `fix(api): name PipelineRuns run-<uuid>, pipeline name as annotation`.

### Task 4: Repo-wide verification

**Files:** none new. Fix only what these checks surface.

- [ ] **Step 1:** `grep -rnE 'PipelineRunName\(\)|componentJobName\([^,]+, "[^"]+", "|shortID\(|runsecrets-' --include='*.go' --include='*.md' --include='*.sh' . | grep -v spikes/`. Expected: no stale callers. Update docs that show old example names (`docs/troubleshooting.md`, `docs/agent-quickstart.md`, `.claude/skills/datuplet-operator/`).
- [ ] **Step 2:** `make lint`. Expected: PASS (vet, deadcode, chart golden render).
- [ ] **Step 3:** `make e2e-k8s` against OrbStack, after checking `duckdb --version`. Expected: all scenarios PASS. During a run, spot-check `kubectl -n datuplet-e2e get pipelineruns,jobs,configmaps,secrets`: only `run-<uuid>…`, `gateway-config-run-<uuid>…`, `datuplet-runsecrets-<uuid>` and `runtoken-<uuid>` should appear.
- [ ] **Step 4: Commit** any doc fixes as `docs: update K8s object name examples`.
