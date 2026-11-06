# Pipeline Preview Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an agent run an unsaved PipelineDoc on the cluster with outputs redirected to per-preview scratch Iceberg namespaces and a per-table row cap, get one cached result document (schema, rows, sample, `columns:` hint, log tails), and have everything purged on a TTL — isolated per workspace.

**Architecture:** pipeline-api rewrites the doc's output buckets to `pv-<id8>-<bucket>`, stores a `previews` row, grants the preview's synthetic identity a project `editor` tuple, and creates a PipelineRun carrying the inline spec plus a `preview` block. The operator runs it like any run (no retry pod, controller-side deadline); the gateway fences writes to declared buckets and caps rows per table. The observer mirrors status into `previews`; a ticker inside `pipeline-api serve` drops the tuple at terminal phase and purges scratch namespaces at TTL.

**Tech Stack:** Go 1.x, controller-runtime, pgx/Postgres, OpenFGA, Lakekeeper REST (iceberg-go `catalog/rest` via `pkg/catalogwriter`), arrow-go, gRPC + protoc (libprotoc 26.1, protoc-gen-go v1.36.11, protoc-gen-go-grpc 1.6.0 — matches checked-in output), Helm.

**Spec:** `docs/superpowers/specs/2026-11-05-pipeline-preview-mode-design.md` (revision 2). Read it before your task.

## Global Constraints

- Work on branch `feat/pipeline-preview`. Stage files **by name** (`git add <paths>`); never `git add -A`/`.` — the checkout has unrelated untracked files (`spikes/`, `docs/superpowers/plans/2026-07-31-*`, `2026-10-31-*`) that must not be committed.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Before each commit: `go build ./... && go vet ./<touched pkgs>/... && go test ./<touched pkgs>/...`. For a sub-module (`sdk/go`, `components/*`, `tests/e2e`) run the same inside that module.
- Never `filepath.Join`/`path.Join` a storage path. Never edit a `go.mod` by hand; if one must change run `make tidy`.
- Fresh install only: a new table is a new migration file `pkg/pipelineapi/db/migrations/014_previews.sql`; no upgrade/backfill logic anywhere.
- CRDs are hand-maintained: any CRD type change updates `charts/datuplet-app/crds/*.yaml` and the hand-written `DeepCopyInto` for new pointer fields.
- Scratch bucket = `"pv-" + id8 + "-" + bucket`, cut to 63 chars, where `id8 = previewID.String()[:8]`. Bucket regex `^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`.
- Workspace regex `^[a-z0-9][a-z0-9-]{0,62}$`, default `"default"`.
- Request bounds: `row_limit` 1..100000 default 1000; `timeout_seconds` 30..3600 default 300; `ttl_seconds` 60..86400 default 3600.
- Caps (env → defaults): `DATUPLET_PREVIEW_MAX_RUNNING_PER_WORKSPACE`=2, `DATUPLET_PREVIEW_MAX_RUNNING_PER_PROJECT`=8, `DATUPLET_PREVIEW_MAX_PER_PROJECT`=50; chart values `previews.maxRunningPerWorkspace|maxRunningPerProject|maxPerProject`.
- Labels: PipelineRun `datuplet.io/preview-id=<uuid>` (and **no** `datuplet.io/run-id` on the PipelineRun); Jobs/Pods/ConfigMaps keep `datuplet.io/run-id=<preview uuid>` (set from `pr.Status.RunID`, unchanged code).
- PipelineRun name `"preview-" + previewID.String()`; run-token Secret `"runtoken-" + previewID.String()`; synthesized doc name `"preview-" + id8`.
- Terminal phases: `Succeeded, FailedUser, FailedApplication, Cancelled, Expired`. Running phases: `Pending, Running`.
- CLI exit codes: `0` Succeeded; `1` FailedUser / Cancelled / Expired / timed out / validation 400; `20` FailedApplication or transport/5xx.
- **Kubernetes safety:** the default kube context on this machine is a real GKE cluster. Never run `kubectl`, `helm` or any `make k8s-*`/`e2e-*` target without first exporting an isolated OrbStack kubeconfig: `kubectl config view --minify --flatten --context orbstack > "$TMPDIR/orbstack.kubeconfig"; export KUBECONFIG="$TMPDIR/orbstack.kubeconfig"`, then confirm `kubectl config current-context` prints `orbstack`. Unit tasks (1-11) never touch a cluster.
- Never `git stash`. Stop any Docker container you start (`docker stop`) before reporting.
- Purge identity: subject `"preview-sweeper-" + previewID.String()` (never the preview's own run identity). Its `editor` project tuple is written with `authz.WriteProjectTuple` immediately before a purge and removed with `authz.DeleteProjectTuple` immediately after.

## Review Focus

1. **A component that writes to a bucket it did not declare** (e.g. `OpenWriterToBucket("raw", …)` from a preview) must fail with `PermissionDenied` *before* any namespace/table is created in Lakekeeper — not silently land in a real table. (Task 1 test `TestOpenWriter_UndeclaredBucket_NoCatalogCall`.)
2. **Several writers to one table** (a component opening a writer per page, or the http-json-extractor's per-batch writes) must share one cap: total committed rows ≤ `row_limit`, not N × cap. (Task 2 test `TestRowCap_SharedAcrossWriters`.)
3. **A preview whose stage 2 reads a bucket that stage 1 produces** must read the scratch copy, while a bucket *not* produced earlier (a real project table) is read as-is. (Task 5 tests.)
4. **Two agents in one project** must not see, reuse, cap-starve or delete each other's previews; workspace teardown deletes only its own. (Task 7 test `TestCreate_RunningCapPerWorkspace`, Task 9 test `TestListPreviews_ScopedToWorkspace`.)
5. **A purge that fails half-way** (Lakekeeper 5xx on the second table) must leave `cleaned_at` NULL and be retried next tick with already-purged tables treated as success. (Task 7 test `TestPurge_RetriesAfterPartialFailure`.)

---

## File structure

| File | Responsibility |
|---|---|
| `pkg/datagateway/server_v2_writing.go` | write fence in `openWriter`; per-table cap bookkeeping |
| `pkg/datagateway/server_v2_http.go` | slice-to-cap in `processParsedRecord`; `limit_reached` in HTTP response |
| `pkg/datagateway/row_cap.go` (new) | `rowCap` type: per-(bucket,table) counters |
| `pkg/datagateway/gatewayconfig.go`, `config.go` | `preview:` block → `Config.Preview` |
| `api/proto/gateway/v2/gateway.proto` + generated | `WriteChunkResponse.limit_reached = 4` |
| `sdk/go/client.go`, `sdk/go/arrow/*.go` | `Writer.LimitReached()`, `Sink.LimitReached()`, `InferringSink.LimitReached()` |
| `components/http-json-extractor/main.go`, `components/data-generator/*.go` | stop producing on cap |
| `pkg/k8s/api/v1/pipelinerun_types.go`, `charts/datuplet-app/crds/datuplet.io_pipelineruns.yaml` | `spec.pipelineSpec`, `spec.preview` |
| `pkg/k8s/controllers/pipelinerun_*.go` | inline spec, BackoffLimit 0, deadline, gateway `preview` block |
| `pkg/pipelineapi/preview/rewrite.go` (new) | pure doc rewrite |
| `pkg/pipelineapi/db/migrations/014_previews.sql`, `pkg/pipelineapi/store/preview.go` (new) | table + SQL |
| `pkg/pipelineapi/k8s/observer.go`, `preview_updater.go` (new), `cmd/pipeline-observer/main.go` | mirror preview status |
| `pkg/pipelineapi/k8s/preview_create.go` (new), `secret_rbac.go` | PipelineRun + Secret creation; pod-log Role |
| `pkg/pipelineapi/preview/service.go`, `catalog.go`, `sweep.go` (new) | create / cancel / delete / purge / ticker |
| `pkg/pipelineapi/preview/result.go`, `logs.go` (new) | result document assembly |
| `pkg/pipelineapi/http/preview_handlers.go` (new), `server.go`, `cmd/pipeline-api/main.go` | routes + wiring |
| `charts/datuplet-app/values.yaml`, `templates/pipeline-api/deployment.yaml` | env |
| `cmd/datuplet/preview.go` (new), `main.go` | CLI |
| `docs/*`, `.claude/skills/datuplet-operator/SKILL.md`, `CHANGELOG.md` | docs |
| `tests/e2e/scenarios_preview_test.go` (new) | live scenario |

---

### Task 1: Gateway write fence (declared buckets only)

**Files:**
- Modify: `pkg/datagateway/server_v2_writing.go` (`openWriter`, right after bucket resolution, before `variantCols :=`)
- Test: `pkg/datagateway/server_v2_write_test.go`
- Modify: `CHANGELOG.md` (`### Changed` under Unreleased), `docs/known-limitations.md` (Data Gateway section)

**Interfaces:**
- Produces: `func (s *ServerV2) declaredOutputBucket(bucket string) bool` — true iff `bucket == s.config.DefaultBucket` (non-empty), or in `s.config.OutputBuckets`, or equals some `s.config.OutputTables[i].Bucket`. When **all three are empty** (no outputs declared — hand-built test servers, `datuplet gateway` dev mode) return true (fence inactive) so existing tests keep working.
- Error: `status.Errorf(codes.PermissionDenied, "bucket %q is not a declared output of this component (declared: %s)", bucket, strings.Join(sortedDeclared, ", "))`. Check how `OpenWriter` maps errors (`partitionStatus(err)`) and make sure a `status` error passes through unchanged; the HTTP layer is not involved (OpenWriter is gRPC only).

- [ ] **Step 1: Write failing tests** in `server_v2_write_test.go`, using the existing fake-resolver/test-server helpers in that file:
  - `TestOpenWriter_UndeclaredBucket_Denied`: config `DefaultBucket:"pv-ab12cd34-raw"`; `OpenWriter{Bucket:"raw", Table:"t"}` → `status.Code(err) == codes.PermissionDenied`, message contains `"raw"` and `"pv-ab12cd34-raw"`.
  - `TestOpenWriter_UndeclaredBucket_NoCatalogCall`: same with a counting fake lakekeeper resolver → `LoadOrCreateForWrite` call count `== 0`.
  - `TestOpenWriter_DeclaredBuckets_Allowed`: table-driven over (a) `DefaultBucket`, (b) `OutputBuckets:["b1"]` with request bucket `b1`, (c) `OutputTables:[{Name:"t",Bucket:"b2"}]` with request bucket `""` and with `"b2"` → no error.
  - `TestOpenWriter_NoDeclaredOutputs_FenceInactive`: empty outputs config + request bucket `"x"` → no PermissionDenied.
- [ ] **Step 2:** `go test ./pkg/datagateway/ -run 'TestOpenWriter_(Undeclared|Declared|NoDeclared)' -v` → FAIL.
- [ ] **Step 3:** Implement the check in `openWriter` and `declaredOutputBucket`.
- [ ] **Step 4:** Same command → PASS; then `go test ./pkg/datagateway/...` → PASS (fix any existing test that wrote to an undeclared bucket by declaring it in its config, never by weakening the fence).
- [ ] **Step 4b:** `go test -tags=integration ./pkg/datagateway/goplane/...` (needs Docker; CI runs it) → PASS.
- [ ] **Step 5:** CHANGELOG `### Changed`: "**Gateway rejects writes to undeclared buckets.** A component that opens a writer for a bucket not in its `outputs` now gets PermissionDenied and the run fails, instead of the gateway creating that table." Add the same sentence to known-limitations under the Data Gateway section.
- [ ] **Step 6: Commit** `feat(gateway): fence writes to the component's declared output buckets`.

---

### Task 2: Gateway preview row cap + `limit_reached`

**Files:**
- Modify: `api/proto/gateway/v2/gateway.proto` (`WriteChunkResponse`: `bool limit_reached = 4; // preview row cap reached for this table; later rows are discarded`; `ComponentConfig`: `int64 preview_row_limit = 16; // >0 in a preview run: rows per output table the gateway keeps`). `GetConfig` (`server_v2.go`) fills `PreviewRowLimit` from `Config.Preview.RowLimit`; add a `TestGetConfig_PreviewRowLimit` test.
- Regenerate: `pkg/datagateway/proto/v2/gateway.pb.go`, `gateway_grpc.pb.go`, and the duplicate `api/proto/gateway/v2/gateway.pb.go`, `gateway_grpc.pb.go`
- Modify: `pkg/datagateway/gatewayconfig.go`, `pkg/datagateway/config.go` (and `ToConfig`)
- Create: `pkg/datagateway/row_cap.go`
- Modify: `pkg/datagateway/server_v2.go` (`ServerV2` gets `rowCap *rowCap`, built in `NewServerV2` when `cfg.Preview != nil && cfg.Preview.RowLimit > 0`), `server_v2_http.go`
- Test: `pkg/datagateway/row_cap_test.go`, `pkg/datagateway/server_v2_write_test.go`

**Interfaces:**
- Config YAML: `preview: {id: <uuid>, row_limit: <int>}` → `GatewayConfig.Preview *PreviewConfig \`yaml:"preview,omitempty"\`` → `Config.Preview *PreviewConfig \`yaml:"preview,omitempty"\``, `type PreviewConfig struct { ID string \`yaml:"id"\`; RowLimit int64 \`yaml:"row_limit"\` }`.
- `type rowCap struct` (mutex-guarded `map[string]int64` keyed `bucket+"\x00"+table`, plus `limit int64`); `func newRowCap(limit int64) *rowCap`; `func (c *rowCap) take(bucket, table string, n int64) (allowed int64, reached bool)` — atomically grants `min(n, limit-used)`, `reached` true when `used == limit` after the grant (including when `n == 0` and already full). A nil `*rowCap` grants `n`, `false`.
- `processParsedRecord`: after transforms and **before** `ensureTableWriter`, call `s.rowCap.take(ws.bucket, ws.table, outputRecord.NumRows())`. `allowed == 0` → return `&pb.WriteChunkResponse{RowsAccepted:0, LimitReached:true}` without opening the writer or writing (if no writer was ever opened for the table the table is simply not created). `allowed < rows` → `outputRecord.NewSlice(0, allowed)` (release it) and write that. Response sets `LimitReached: reached`.
- HTTP: `httpWriteResponse` gains `LimitReached bool \`json:"limit_reached,omitempty"\``; `handleWrite` also sets header `X-Datuplet-Limit-Reached: true` when set.

- [ ] **Step 1: Write failing tests.**
  - `row_cap_test.go` `TestRowCap_Take`: limit 10 → `take(b,t,4)=(4,false)`, `take(b,t,8)=(6,true)`, `take(b,t,1)=(0,true)`, other table `take(b,t2,3)=(3,false)`; `(*rowCap)(nil).take(b,t,5)=(5,false)`.
  - `server_v2_write_test.go` `TestRowCap_SlicesCrossingChunk`: preview row_limit 5, Arrow IPC chunk of 3 rows then 4 rows → responses `RowsAccepted 3 / false`, `2 / true`; close + commit → committed rows 5.
  - `TestRowCap_SharedAcrossWriters`: two writers on the same (bucket,table), 4 rows each, limit 5 → total accepted 5; second writer's later chunk `RowsAccepted 0, LimitReached true`.
  - `TestRowCap_HTTPLimitReachedField`: via the HTTP write handler, crossing chunk → JSON `limit_reached:true` and header set.
  - `TestRowCap_NoPreviewNoCap`: no preview block → 1000-row chunk fully accepted, `LimitReached false`.
  - `TestGatewayConfig_PreviewBlock`: YAML with `preview: {id: x, row_limit: 7}` → `Config.Preview.RowLimit == 7`.
- [ ] **Step 2:** Run them → FAIL (compile error on `LimitReached` is the expected first failure).
- [ ] **Step 3:** Edit the proto, regenerate both copies:
  ```bash
  (cd api/proto/gateway/v2 && protoc --go_out=../../../../pkg/datagateway/proto/v2 --go_opt=paths=source_relative --go-grpc_out=../../../../pkg/datagateway/proto/v2 --go-grpc_opt=paths=source_relative gateway.proto)
  protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/proto/gateway/v2/gateway.proto
  git diff --stat -- pkg/datagateway/proto api/proto
  ```
  Expected: only the new field (+getter, descriptor bytes) changes; header versions unchanged. If anything else changes, stop and report.
- [ ] **Step 4:** Implement config plumbing, `rowCap`, and the `processParsedRecord`/HTTP changes.
- [ ] **Step 5:** `go test ./pkg/datagateway/...` → PASS; `go build ./... ` and `(cd sdk/go && go build ./...)` → OK.
- [ ] **Step 6: Commit** `feat(gateway): per-table preview row cap with limit_reached signal`.

---

### Task 3: SDK `LimitReached()` + built-ins stop producing

**Files:**
- Modify: `sdk/go/client.go` (`Writer` field `limitReached bool`, set in `writeChunkHTTP` from JSON `limit_reached` and in `writeChunkGRPC` from `resp.LimitReached`; `WriteResult.LimitReached bool`)
- Modify: `sdk/go/client.go` `Config` (`PreviewRowLimit`, filled in `Config()` from the GetConfig response)
- Modify: `components/http-json-extractor/main.go`, `columns.go` (paginated loop, single-request path, sink batch rows), `components/data-generator/*.go` (random-mode emit loop, sink batch rows)
- Test: `sdk/go/limit_test.go` (new), `sdk/go/config_test.go`, `components/http-json-extractor/*_test.go`, `components/data-generator/config_test.go`

**Interfaces:**
- `sdk.Config.PreviewRowLimit int64` (from `ComponentConfig.preview_row_limit`, Task 2); 0 outside previews.
- `func (w *Writer) LimitReached() bool` — sticky once any response carried it. **Not an error:** the Arrow sinks treat write errors as sticky failures, so the signal is an accessor (deliberate deviation from the spec's sentinel-error wording; behaviour is the same — the component stops and exits 0). `WriteResult.LimitReached bool` likewise.
- The speed win comes from the components knowing the cap up front (the sinks flush every 8192 rows and the SDK writer batches 1 MiB, so a gateway-side signal arrives far too late for a 1 000-row cap):
  - http-json-extractor: effective record cap `= min(nonzero(pagination.max_records), PreviewRowLimit)` when `PreviewRowLimit > 0`, applied to the paginated loop (stop paginating once reached, same path as `max_records`) **and** to the single-request path (stop decoding after the cap). Sink batch rows `= min(DefaultBatchRows, PreviewRowLimit)` via `dgarrow.WithBatchRows`.
  - data-generator: effective `rowsCount = min(nonzero(limit.rowsCount), PreviewRowLimit)` when `PreviewRowLimit > 0` (a `sizeInBytes`/`timeoutInSeconds`-only limit also stops at the cap); batch rows capped the same way.
  - The gateway cap (Task 2) stays the backstop for components that ignore the field.

- [ ] **Step 1: Write failing tests.**
  - SDK `TestWriter_LimitReachedFromHTTP`: httptest server answering `{"rows_accepted":2,"limit_reached":true}` → after `WriteChunk`, `w.LimitReached() == true` and `res.LimitReached == true`; a previous response without the field leaves it false.
  - SDK `TestConfig_PreviewRowLimit`: GetConfig fake returning `PreviewRowLimit: 7` (use the existing `config_test.go` seam) → `c.Config().PreviewRowLimit == 7`.
  - extractor `TestEffectiveRecordCap`: table over (max_records, preview) = (0,0)→0, (0,1000)→1000, (500,1000)→500, (5000,1000)→1000. Wire the result into both paths; if the pagination loop has an existing unit seam (check `stream_test.go`), add `TestPagination_StopsAtPreviewCap` (5 pages × 10, cap 15 → 2 pages requested); otherwise test the cap helper only.
  - data-generator `TestEffectiveRowsCount`: same table shape for `rowsCount`, plus sizeInBytes-only limit with preview 1000 → 1000.
- [ ] **Step 2:** Run in each module (`cd sdk/go && go test ./... -run 'LimitReached|PreviewRowLimit'`; `components/http-json-extractor` and `components/data-generator` `-run 'Effective|PreviewCap'`) → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `go test ./...` in `sdk/go`, `sdk/go/arrow`, `components/http-json-extractor`, `components/data-generator` → PASS.
- [ ] **Step 5: Commit** `feat(sdk): expose the preview row limit; built-ins stop producing at the cap`.

---

### Task 4: PipelineRun inline spec + preview block in the operator

**Files:**
- Modify: `pkg/k8s/api/v1/pipelinerun_types.go` (+ `DeepCopyInto`)
- Modify: `charts/datuplet-app/crds/datuplet.io_pipelineruns.yaml` (drop `pipelineRef` from `required`; add `pipelineSpec` as `type: object, x-kubernetes-preserve-unknown-fields: true`; add `preview` object with `id` string, `rowLimit` integer, `timeoutSeconds` integer)
- Modify: `pkg/k8s/controllers/pipelinerun_controller.go` (`handlePending`, `handleRunning`), `pipelinerun_jobs.go` (`buildComponentJob` BackoffLimit, `generateGatewayConfig` preview block)
- Test: `pkg/k8s/controllers/pipelinerun_controller_test.go`, `pipelinerun_jobs_test.go`, `pkg/k8s/api/v1/pipelinerun_types_test.go`

**Interfaces:**
- Types:
  ```go
  // PipelineRunSpec additions
  PipelineSpec *PipelineSpec `json:"pipelineSpec,omitempty"` // inline spec; when set, PipelineRef is ignored
  Preview      *PreviewSpec  `json:"preview,omitempty"`
  type PreviewSpec struct {
      ID             string `json:"id"`
      RowLimit       int64  `json:"rowLimit"`
      TimeoutSeconds int64  `json:"timeoutSeconds"`
  }
  ```
  `PipelineRef` stays a non-pointer struct (an empty `{name:""}` passes the CRD since `pipelineRef` is no longer required); do not change its tag.
- Operator RBAC: add `patch` to the `pods` verbs in `charts/datuplet-app/templates/pipeline-operator/rbac.yaml` so `propagateCancelToPods` actually works (today it fails silently).
- `handlePending`: when `pr.Spec.PipelineSpec != nil`, build `pipeline := &datupletv1.Pipeline{ObjectMeta: metav1.ObjectMeta{Name: pr.Name, Namespace: pr.Namespace, Generation: 0}, Spec: *pr.Spec.PipelineSpec.DeepCopy()}` and skip the Get + not-found retry; all four downstream uses take this value unchanged. Neither set → FailedUser `"spec.pipelineRef or spec.pipelineSpec is required"`.
- Jobs: `BackoffLimit: int32Ptr(0)` when `pr.Spec.Preview != nil`, else 1.
- Gateway config: when `pr.Spec.Preview != nil`, `configMap["preview"] = map[string]any{"id": pr.Spec.Preview.ID, "row_limit": pr.Spec.Preview.RowLimit}`.
- Deadline in `handleRunning` (before stage dispatch): `pr.Spec.Preview != nil && pr.Status.StartTime != nil && now.Sub(StartTime) > TimeoutSeconds` → `r.propagateCancelToPods(ctx, pr)`; delete the current stage's Jobs (`client.PropagationPolicy(metav1.DeletePropagationBackground)`); set phase `FailedUser`, message `fmt.Sprintf("preview timed out after %ds during stage %s", TimeoutSeconds, stageName)`, `CompletionTime`. Requeue interval stays 5 s, so the deadline is enforced within ~5 s.

- [ ] **Step 1: Write failing tests** (use the fake-client harness already in `pipelinerun_controller_test.go`):
  - `TestPipelineRunSpec_DeepCopyPreview`: deep copy then mutate source `PipelineSpec.Stages[0].Name` and `Preview.RowLimit` → copy unchanged.
  - `TestHandlePending_InlineSpec`: PipelineRun with `PipelineSpec` (one data-generator stage) and no Pipeline CR → phase Running, `Status.ResolvedSpec.Stages[0].Name` matches.
  - `TestHandlePending_NoRefNoSpec_FailedUser`.
  - `TestBuildComponentJob_PreviewBackoffZero`: preview → `*job.Spec.BackoffLimit == 0`; non-preview → 1.
  - `TestGenerateGatewayConfig_PreviewBlock`: YAML contains `preview:` with `row_limit: 1000` and the id; absent for non-preview.
  - `TestHandleRunning_PreviewDeadline`: StartTime 10 min ago, TimeoutSeconds 60 → phase FailedUser, message has prefix `"preview timed out after 60s during stage "`, stage Jobs deleted.
- [ ] **Step 2:** `go test ./pkg/k8s/... -run 'Preview|InlineSpec|NoRefNoSpec' -v` → FAIL.
- [ ] **Step 3:** Implement types, DeepCopy, CRD YAML, controller changes.
- [ ] **Step 4:** `go test ./pkg/k8s/...` → PASS; `helm template charts/datuplet-app >/dev/null` → OK.
- [ ] **Step 5: Commit** `feat(operator): run PipelineRuns from an inline spec with a preview block`.

---

### Task 5: Doc rewrite (pure)

**Files:**
- Create: `pkg/pipelineapi/preview/rewrite.go`, `pkg/pipelineapi/preview/rewrite_test.go`

**Interfaces:**
- `func ScratchBucket(previewID uuid.UUID, bucket string) string` — `"pv-"+id8+"-"+bucket`, cut to 63 bytes, then `strings.TrimRight(_, "-")`.
- `type RewriteResult struct { Doc *config.Pipeline; Namespaces []string /* sorted, unique */; Logical map[string]string /* scratch → logical */ }`
- `func Rewrite(doc *config.Pipeline, previewID uuid.UUID, untilStage string) (*RewriteResult, error)` — deep-copies `doc` (JSON round-trip), drops stages after `untilStage` (error `until_stage %q is not a stage of this pipeline` when not found), sets `Name = "preview-"+id8`, maps `outputs.defaultBucket`, `outputs.buckets[].name`, `outputs.tables[].bucket`; maps `inputs.buckets[]` / `inputs.tables[].bucket` only when that logical bucket is produced by an earlier stage of the (truncated) doc; never touches table names; errors `buckets %q and %q both map to %q` when two logical buckets collide after truncation.

- [ ] **Step 1: Write failing tests** `rewrite_test.go`, fixed id `uuid.MustParse("ab12cd34-0000-0000-0000-000000000000")`:
  - `TestScratchBucket`: `raw` → `pv-ab12cd34-raw`; a 60-char bucket → length 63, prefix `pv-ab12cd34-`, no trailing `-`.
  - `TestRewrite_ExplicitAndDynamic`: stage0 `defaultBucket: raw`, stage1 `tables:[{name: s, bucket: curated}]` → buckets rewritten, table name `s` unchanged, `Namespaces == ["pv-ab12cd34-curated","pv-ab12cd34-raw"]`, `Doc.Name == "preview-ab12cd34"`.
  - `TestRewrite_CrossStageInputs`: stage1 `inputs.tables:[{bucket: raw, table: data}, {bucket: ref, table: countries}]`, only `raw` produced by stage0 → `raw`→scratch, `ref` unchanged; `inputs.buckets:[raw]` → scratch.
  - `TestRewrite_UntilStage`: 3 stages, `untilStage="extract"` → 1 stage; unknown name → error.
  - `TestRewrite_CollisionAfterTruncation` → error mentions both buckets.
  - `TestRewrite_DoesNotMutateInput`.
- [ ] **Step 2:** `go test ./pkg/pipelineapi/preview/ -run 'Rewrite|ScratchBucket' -v` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Re-run → PASS; also run the result of `TestRewrite_ExplicitAndDynamic` through `validate.ValidatePipelineDoc(json, "preview-ab12cd34", nil, nil)` inside a test `TestRewrite_OutputValidates` → no error findings.
- [ ] **Step 5: Commit** `feat(preview): rewrite a PipelineDoc onto scratch namespaces`.

---

### Task 6: `previews` table, store, observer mirroring

**Files:**
- Create: `pkg/pipelineapi/db/migrations/014_previews.sql`, `pkg/pipelineapi/store/preview.go`, `pkg/pipelineapi/store/preview_test.go`
- Create: `pkg/pipelineapi/k8s/preview_updater.go`, `pkg/pipelineapi/k8s/preview_updater_test.go`
- Modify: `pkg/pipelineapi/k8s/observer.go` (predicate, `reconcileOne`, `RunStatus.PreviewID uuid.UUID`), `cmd/pipeline-observer/main.go` (wrap with `RoutingUpdater`)

**Interfaces:**
- Migration columns exactly as spec §4.3 plus `lakekeeper_project_id TEXT NOT NULL DEFAULT ''`, `warehouse TEXT NOT NULL DEFAULT ''`, `k8s_namespace TEXT NOT NULL`; `project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE`; `triggered_by UUID REFERENCES users(id) ON DELETE SET NULL`; `phase TEXT NOT NULL DEFAULT 'Pending'`; indexes `(project_id, workspace, created_at DESC)` and `(expires_at) WHERE cleaned_at IS NULL`. Check `users` table name in earlier migrations.
- `store` (pgx, same style as `run.go`):
  ```go
  type Preview struct { ID, ProjectID uuid.UUID; TriggeredBy uuid.UUID; Workspace string; Doc []byte
      RowLimit, TimeoutSeconds, TTLSeconds int; UntilStage string; ScratchNamespaces []string
      LakekeeperProjectID, Warehouse, K8sNamespace string
      Phase, CurrentStage, Message string; StageStatuses []byte; Result []byte
      FGATupleDeletedAt, StartedAt, CompletedAt, CleanedAt *time.Time; CreatedAt, ExpiresAt time.Time }
  var ErrPreviewNotFound = errors.New("preview not found")
  type PreviewCaps struct { RunningPerWorkspace, RunningPerProject, MaxPerProject int }
  var ErrPreviewCap = errors.New("preview cap reached")
  type PreviewCapError struct{ RunningInWorkspace, RunningInProject, LiveInProject int } // Error() text names the counts; Is(target) reports target == ErrPreviewCap
  func InsertPreview(ctx, pool, p *Preview, caps PreviewCaps) error // tx: pg_advisory_xact_lock(hashtext('previews:'||project_id)); delete oldest cleaned rows while total rows >= MaxPerProject; count running (ws + project) and live (cleaned_at IS NULL) rows; any cap hit → *PreviewCapError; insert
  func GetPreview(ctx, pool, projectID, id uuid.UUID) (*Preview, error)
  func GetPreviewByID(ctx, pool, id uuid.UUID) (*Preview, error)
  func ListPreviews(ctx, pool, projectID uuid.UUID, workspace string /* "*" = all */, limit int, cursor string) ([]Preview, string, error)
  type UpdatePreviewPhaseOpts struct { Phase, CurrentStage, Message string; StartedAt, CompletedAt *time.Time; ObservedRV int64; StageStatuses []byte; GuardTerminal bool }
  func UpdatePreviewPhase(ctx, pool, id uuid.UUID, o UpdatePreviewPhaseOpts) (bool, error) // observed_rv guard like UpdateRunPhase; terminal phase → expires_at = completed_at + ttl_seconds
  func SetPreviewResult(ctx, pool, id uuid.UUID, result []byte) error            // only WHERE result IS NULL
  func MarkPreviewTupleDeleted(ctx, pool, id uuid.UUID, at time.Time) error
  func MarkPreviewCleaned(ctx, pool, id uuid.UUID, at time.Time, expired bool) error // expired → phase='Expired' if non-terminal
  func ListPreviewsNeedingTupleDelete(ctx, pool, limit int) ([]Preview, error)       // terminal, fga_tuple_deleted_at IS NULL
  func ListExpiredPreviews(ctx, pool, now time.Time, limit int) ([]Preview, error)   // expires_at < now AND cleaned_at IS NULL
  func ListPreviewIDsInWorkspace(ctx, pool, projectID uuid.UUID, workspace string) ([]uuid.UUID, error) // cleaned_at IS NULL
  ```
- Observer: predicate accepts objects with `datuplet.io/run-id` **or** `datuplet.io/preview-id`; `reconcileOne` reads `preview-id` first and fills `RunStatus.PreviewID` (and `RunID` = same uuid); `type RoutingUpdater struct{ Runs, Previews RunStatusUpdater }` → `Update` routes on `s.PreviewID != uuid.Nil`; `type DBPreviewUpdater struct{ pool *pgxpool.Pool }` → verifies `s.Namespace == previews.k8s_namespace` and `s.PipelineRunName == "preview-"+id` (else benign drop) then `store.UpdatePreviewPhase(..., GuardTerminal:true)`. `NewDBPreviewUpdater(pool) *DBPreviewUpdater`. In `cmd/pipeline-observer/main.go` the coalesced updater wraps `&RoutingUpdater{Runs: dbRuns, Previews: NewDBPreviewUpdater(pool)}`.

- [ ] **Step 1: Write failing tests.** DB tests skip unless `TEST_DATABASE_URL` (follow `run_test.go`'s helper):
  - `TestInsertPreview_RunningCapPerWorkspace`: caps {2,8,50}; two Pending in ws `a` → third in `a` returns `ErrPreviewCap` (errors.Is) with `RunningInWorkspace==2`; insert in ws `b` succeeds.
  - `TestInsertPreview_EvictsOldestCleaned`: MaxPerProject 2, two cleaned rows + insert → oldest cleaned row gone.
  - `TestUpdatePreviewPhase_TerminalResetsExpiry`: ttl 60, completed_at T → `expires_at == T+60s`; a stale observed_rv is ignored.
  - `TestListPreviews_Workspace`: `"a"` returns only a's rows; `"*"` returns all.
  - `TestReconcileOne_PreviewRouting` (fake client, no DB): PipelineRun with preview-id label → Previews updater called, Runs not; run-id label → Runs only.
  - `TestDBPreviewUpdater_RejectsDecoyName` (DB).
- [ ] **Step 2:** `TEST_DATABASE_URL=postgres://postgres:test@localhost:55432/test?sslmode=disable go test -p 1 ./pkg/pipelineapi/store/ ./pkg/pipelineapi/k8s/ -run 'Preview' -v` (start Postgres first: `docker run -d --rm --name dtp-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test -p 55432:5432 postgres:16`; stop it with `docker stop dtp-pg` when the task is done) → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Same command → PASS; then the full `./pkg/pipelineapi/store/ ./pkg/pipelineapi/k8s/ ./pkg/pipelineapi/db/` with the DB URL and `-p 1` → PASS; `go test ./cmd/pipeline-observer/...` → PASS.
- [ ] **Step 5: Commit** `feat(preview): previews table, store, and observer mirroring`.

---

### Task 7: Preview service — create, cancel, delete, purge, ticker

**Files:**
- Create: `pkg/pipelineapi/k8s/preview_create.go` (+ test), modify `pkg/pipelineapi/k8s/secret_rbac.go` (+ test)
- Create: `pkg/pipelineapi/preview/service.go`, `catalog.go`, `sweep.go`, `service_test.go`, `sweep_test.go`

**Interfaces:**
- `pkg8s.CreatePreviewOpts{ Namespace string; PreviewID uuid.UUID; Token string; Spec datupletv1.PipelineSpec; Preview datupletv1.PreviewSpec }`, `func CreatePreviewResources(ctx, c client.Client, o CreatePreviewOpts) error` — same 4 steps as `CreateRunResources` with the names/labels from Global Constraints (Secret label `datuplet.io/preview-id`). The PipelineRun **must** set `Spec.RunID = PreviewID.String()` (else the controller generates a random id, `$RUN_ID` mismatches the token's `run_id`, and the sidecar fails at boot) and `Spec.RunTokenRef = &RunTokenRef{Name: "runtoken-"+id}`.
- Move-in from Task 8 (Task 7's `Deps` needs them; Task 8 implements them) — declare in `catalog.go`:
  ```go
  type Sampler interface { Sample(ctx context.Context, qualifiedWarehouse, ns, table string, variantCols []string, rows int) (*Sample, error) } // qualifiedWarehouse = lakekeeper_project_id + "/" + warehouse from the row
  type Sample struct { Columns []SampleColumn `json:"columns"`; Rows [][]any `json:"rows"`; Truncated bool `json:"truncated"` }
  type SampleColumn struct { Name string `json:"name"`; Type string `json:"type"` }
  type LogTailer interface { Tail(ctx context.Context, namespace, jobName, container string, lines int64) (string, error) }
  ```
- `secret_rbac.go`: `ensureSecretsRBAC` also ensures Role `datuplet-pod-logs` (`pods: get,list`; `pods/log: get`) + RoleBinding to the pipeline-api SA. Constant `PodLogsRoleName = "datuplet-pod-logs"`.
- `catalog.go`:
  ```go
  type Catalog interface {
      ListTables(ctx context.Context, ns string) ([]string, error)    // missing namespace → nil, nil
      LoadTable(ctx context.Context, ns, table string) (*TableInfo, error)
      PurgeTable(ctx context.Context, ns, table string) error          // not found → nil
      DropNamespace(ctx context.Context, ns string) error              // not found → nil
  }
  type TableInfo struct { Columns []Column; TotalRecords int64; VariantColumns []string }
  type Column struct { Name, Type string; Nullable bool } // Type = Iceberg type string (field.Type.String())
  type CatalogFactory func(ctx context.Context, tp catalogwriter.TokenProvider, lakekeeperProjectID, warehouse string) (Catalog, error)
  func NewRESTCatalogFactory(lakekeeperURL string) CatalogFactory // over catalogwriter.NewClient; TotalRecords from current snapshot summary "total-records"
  ```
- `service.go`:
  ```go
  type Deps struct {
      DB *pgxpool.Pool; K8s client.Client; Authz authz.Authorizer; Signer *tokens.Signer
      ProjectNS runbackend.ProjectNS; WarehouseResolver func(ctx context.Context, lkProjectID string) (string, error)
      Catalogs CatalogFactory; UserCatalogToken catalogwriter.TokenProvider // impersonation for ctx user (storage Service.Minter)
      Sampler Sampler; Logs LogTailer; Caps store.PreviewCaps; Now func() time.Time; Audience string
      PurgeWait time.Duration // max wait for the preview's pods to disappear before purging; default 90s; tests use ~100ms
      PollInterval time.Duration // default 2s
  }
  func (s *Service) List(ctx context.Context, projectID uuid.UUID, workspace string, limit int, cursor string) ([]store.Preview, string, error)
  type Service struct{ /* deps */ }
  func New(d Deps) *Service
  type CreateRequest struct { ProjectID, UserID uuid.UUID; Doc *config.Pipeline; Workspace string; RowLimit, TimeoutSeconds, TTLSeconds int; UntilStage string }
  type CreateResponse struct { ID uuid.UUID; Workspace, Phase string; ScratchNamespaces []string; ExpiresAt time.Time }
  func (s *Service) Create(ctx context.Context, r CreateRequest) (*CreateResponse, error) // ctx carries the user (MintRunToken needs it)
  func (s *Service) Cancel(ctx context.Context, projectID, id uuid.UUID) error
  func (s *Service) Delete(ctx context.Context, projectID, id uuid.UUID) error          // cancel + purge now
  func (s *Service) DeleteWorkspace(ctx context.Context, projectID uuid.UUID, ws string) (deleted []uuid.UUID, failed map[uuid.UUID]string)
  var ErrBadRequest = errors.New("bad preview request") // wrap with fmt.Errorf("%w: …")
  ```
  Create order: apply defaults/bounds (out of range → `ErrBadRequest`) → `Rewrite` → re-validate rewritten JSON with `validate.ValidatePipelineDoc(json, rewritten.Name, nil, nil)` (error finding → internal error) → `ProjectNS.Ensure` → project row for `LakekeeperProjectID` → `WarehouseResolver` → `store.InsertPreview` (caps) → FGA `WriteTuples([{User: authz.UserObject(id).String(), Relation: authz.RelationEditor, Object: authz.ProjectObject(lk)}])` (skip when lk == "") → `tokens.MintRunToken(ctx, signer, RunSpec{RunID:id, ProjectID:lk, PipelineName:rewritten.Name, Warehouse, Audience, Lifetime: timeout+10m})` → `config.DocToCR` → `CreatePreviewResources`. Any failure after insert → `UpdatePreviewPhase(FailedApplication, message)` + delete tuple (detached ctx), return error.
  Cancel (runs/k8s.go pattern): delete tuple first (`authz.DeleteProjectTuple`, tolerates missing; then `MarkPreviewTupleDeleted`), annotate pods `datuplet.io/run-id=<id>` with cancel, delete PipelineRun `preview-<id>` with `client.PropagationPolicy(metav1.DeletePropagationForeground)`, then when non-terminal `UpdatePreviewPhase(Phase:"Cancelled", Message:"cancelled by user", CompletedAt:&now)` (so `expires_at` resets to completed_at + ttl).
- `sweep.go`:
  ```go
  func (s *Service) SweepOnce(ctx context.Context) error // ONLY steps 1 (tuple delete at terminal) and 3 (expire → Purge) of spec §4.7; result assembly is NOT here (it needs a user identity — Task 8 does it on GET). Each per-row error logged + counted, never aborting other rows
  func (s *Service) Purge(ctx context.Context, p *store.Preview) error
  func (s *Service) RunSweeper(ctx context.Context, interval time.Duration) // ticker; each tick: conn := pool.Acquire; SELECT pg_try_advisory_lock(0x70726576); if got → SweepOnce; pg_advisory_unlock; Release
  var sweepFailures = prometheus counter vec "pipelineapi_preview_sweep_failures_total" label "step" (tuple|purge|cancel)
  ```
  Purge order: if non-terminal → Cancel; else if the preview's own tuple is still present delete it; delete PipelineRun `preview-<id>` (Foreground, NotFound ok); wait (poll `PollInterval`, max `PurgeWait`) until the PipelineRun is NotFound **and** no Pod labelled `datuplet.io/run-id=<id>` remains in the namespace, else return error (retry next tick); sweeper subject `sw := "preview-sweeper-"+id`; `authz.WriteProjectTuple(ctx, authz, sw, authz.RelationEditor, lk)` (skip when lk == ""); catalog via `s.Catalogs(ctx, tp, lk, warehouse)` where `tp` returns `tokens.MintServiceToken(signer, ServiceTokenSpec{Subject: sw, Audience: s.audience, Lifetime: 5*time.Minute})`; for each scratch namespace: `ListTables` → `PurgeTable` each → `DropNamespace`; verify `ListTables` empty for all; `defer authz.DeleteProjectTuple(…, sw, …)` (always, also on error); `MarkPreviewCleaned(expired: phase non-terminal)`.

- [ ] **Step 1: Write failing tests** with fakes (`fakeCatalog` map-based, fake authorizer recording writes/deletes, controller-runtime fake client; DB tests gated on `TEST_DATABASE_URL`):
  - `TestCreatePreviewResources_Labels` (no DB): PipelineRun name `preview-<id>`, label preview-id, no run-id label, `Spec.PipelineSpec` and `Spec.Preview` set, Secret owner ref.
  - `TestEnsureSecretsRBAC_PodLogsRole`.
  - `TestCreate_HappyPath` (DB): row inserted with workspace/namespaces/warehouse, one editor tuple written, PipelineRun exists.
  - `TestCreate_RejectsOutOfRange` (no DB): `RowLimit: 0`→ default, `RowLimit: 200000` → `ErrBadRequest`; workspace `"Bad_WS"` → `ErrBadRequest`.
  - `TestCreate_RunningCapPerWorkspace` (DB): third in ws `a` → `store.ErrPreviewCap`, no tuple written, no PipelineRun.
  - `TestSweep_DeletesTupleAtTerminal` (DB): Succeeded row → tuple deleted, `fga_tuple_deleted_at` set, scratch tables untouched.
  - `TestPurge_PurgesAndDropsNamespaces` (DB): fake catalog with 2 tables in 2 namespaces → all purged, namespaces dropped, `cleaned_at` set; the fake authorizer saw write(`preview-sweeper-<id>`) before the first purge and delete(`preview-sweeper-<id>`) after the last, and never re-wrote the preview's own identity.
  - `TestPurge_SweeperTupleRemovedOnError` (DB): catalog fails every purge → sweeper tuple still deleted.
  - `TestPurge_RetriesAfterPartialFailure` (DB): fake catalog fails `PurgeTable` once on the 2nd table → first sweep: `cleaned_at` NULL; second sweep: success, purge of already-gone table returns nil.
  - `TestPurge_WaitsForPodsGone` (DB, `PurgeWait: 100*time.Millisecond`): PipelineRun gone but a Pod labelled `datuplet.io/run-id=<id>` remains → Purge returns error, no catalog calls.
  - `TestDeleteWorkspace_OnlyThatWorkspace` (DB).
- [ ] **Step 2:** Run with the Task 6 Postgres container → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `TEST_DATABASE_URL=… go test -p 1 ./pkg/pipelineapi/preview/... ./pkg/pipelineapi/k8s/...` → PASS.
- [ ] **Step 5: Commit** `feat(preview): create, cancel, purge and sweep previews`.

---

### Task 8: Result document assembly

**Files:**
- Create: `pkg/pipelineapi/preview/result.go`, `logs.go`, `result_test.go`

**Interfaces:**
- `Sampler`, `Sample`, `SampleColumn`, `LogTailer` are declared in Task 7's `catalog.go`; this task implements them.
- ```go
  func NewClientsetLogTailer(cs kubernetes.Interface) LogTailer // pod chosen by label batch.kubernetes.io/job-name=<jobName>; NotFound → "", nil
  func NewQuerySampler(core *queryproxy.Core) Sampler          // sub = authenticated user from ctx (auth.UserFromContext), as storage HTTPHandlers.Preview does
  type Result struct { /* JSON exactly as spec §4.5, field names verbatim */ }
  func (s *Service) Get(ctx context.Context, projectID, id uuid.UUID, resample bool) (*Result, error)
  func ColumnsHint(cols []Column) (hint []ColumnHint, warnings []string)
  ```
- `Result` carries `ResultReady bool \`json:"result_ready"\`` — true once the cached document exists (false while non-terminal). Clients stop polling on terminal phase + `result_ready`, never on "outputs non-empty" (a preview can legitimately write nothing).
- `Get` serializes assembly per preview id with `golang.org/x/sync/singleflight` (check it is already in root `go.mod`; if not, use a `sync.Mutex`-guarded map of in-flight ids) so concurrent GETs neither assemble twice nor collide on the query worker's 1-in-flight preview gate.
- `Get`: load row; non-terminal → result with stages from `stage_statuses`, `outputs: []`; terminal and `result IS NULL` (or `resample`) → assemble (catalog via `s.Catalogs(ctx, s.userCatalogToken, lk, warehouse)` — ctx carries the user), `SetPreviewResult`, return. Cached JSON stores only `stages` (with log tails), `outputs`, `warnings`; the row's own fields fill the rest on every read. `resample=true` re-samples outputs whose `sample` is null and overwrites the cache (use a plain UPDATE for that path).
- Iceberg → `columns:` map: `int→int, long→long, float→float, double→double, boolean→boolean, string→string, variant→variant`; anything else → `string` + warning `column %q has type %s; hinted as string — declare a supported type or keep string`.
- `outputs[].rows = TotalRecords`, `at_cap = TotalRecords >= row_limit`; producer = the doc component whose (rewritten) outputs declare that bucket+table, else the component whose `defaultBucket`/`buckets[]` match the namespace when unique, else null.
- Log tails: for each component with a failed phase, `Tail(ns, comp.PodName, "component", 200)` and `Tail(…, "gateway", 200)`. Note `ComponentStatus.PodName` holds the **Job** name (set in `startStage`); that is correct for the `jobName` parameter — do not "fix" it.
- Production `Sampler` (file `sampler.go`): wraps `(*queryproxy.Core).Preview(ctx, user.ID.String(), qualifiedWarehouse, ns, table, variantCols, PreviewLimits{TimeoutS: 30, MaxRows: rows, MaxBytes: 1<<20})`. A `QueryError` (429, `query_disabled`, timeout) becomes a `warnings` entry, never an error. A nil sampler (query worker off) → every sample null with warning `samples unavailable: query service disabled`.

- [ ] **Step 1: Write failing tests** (fakes; DB-gated where a row is needed):
  - `TestColumnsHint`: `[{id long} {ts timestamptz} {v variant}]` → hint `[{id long} {ts string} {v variant}]`, one warning naming `ts`.
  - `TestGet_AssemblesOnceAndCaches` (DB): Succeeded preview, fake catalog with `pv-ab12cd34-raw.data` (TotalRecords 1000, limit 1000) → `outputs[0]` has `bucket raw`, `scratch_table "pv-ab12cd34-raw.data"`, `at_cap true`; second `Get` does not call the catalog or sampler (counters).
  - `TestGet_SamplerUnavailable_Warns` (DB): sampler returns error → `sample null`, one warning, no error.
  - `TestGet_FailedComponentLogTails` (DB): FailedUser stage → `log_tail` and `gateway_log_tail` from the fake tailer.
  - `TestGet_RunningHasNoOutputs` (DB): `result_ready false`.
  - `TestGet_SucceededNoTables_ResultReady` (DB): fake catalog with empty scratch namespaces → `outputs []`, `result_ready true`.
  - `TestGet_ConcurrentAssembleOnce` (DB): 5 goroutines call `Get` → catalog `ListTables` per namespace called once.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** `TEST_DATABASE_URL=… go test -p 1 ./pkg/pipelineapi/preview/...` → PASS.
- [ ] **Step 5: Commit** `feat(preview): assemble and cache the preview result document`.

---

### Task 9: HTTP routes, wiring, chart

**Files:**
- Create: `pkg/pipelineapi/http/preview_handlers.go`, `preview_handlers_test.go`
- Modify: `pkg/pipelineapi/http/server.go` (`previews *preview.Service` field, `WithPreviews(svc *preview.Service) *Server`, routes), `cmd/pipeline-api/main.go` (build `preview.Service` when backend + DB + signer + lakekeeper URL are wired; start `RunSweeper(ctx, 60*time.Second)`; caps from env), `charts/datuplet-app/values.yaml` (`previews:` block with the three caps), `charts/datuplet-app/templates/pipeline-api/deployment.yaml` (env)

**Interfaces:**
- Routes (all `auth.WithUser`, registered only when `s.previews != nil && s.resolver != nil && s.authzr != nil`):
  `POST /api/v1/projects/{pid}/previews`, `GET /api/v1/projects/{pid}/previews`, `GET /api/v1/projects/{pid}/previews/{id}`, `DELETE /api/v1/projects/{pid}/previews/{id}`, `DELETE /api/v1/projects/{pid}/previews` (requires `?workspace=`), `POST /api/v1/projects/{pid}/previews/{id}/cancel`.
- Auth: `s.mustHaveRelation(w, r, "data_admin")` on all routes (reads too — a preview carries a doc with config).
- POST body: JSON `{doc: object, workspace, row_limit, timeout_seconds, ttl_seconds, until_stage}`, 1 MiB `http.MaxBytesReader`. Handler: marshal `doc` → bytes; `name := doc.name || "preview"`; then **mirror `handlePutPipeline`'s gate order exactly** (`pipeline_handlers.go` ~226-264, create semantics: the "old" spec is an empty `&datupletv1.Pipeline{}`): superadmin check, policy `nil` for superadmins and `s.policy` otherwise, the 403 gates (resources diff, `gateway.*` bound errors for non-superadmins) before the 400 for other error findings (`{error:"validation failed", findings}`); then `missingSecretRefs` → 400; then `Create` with `config.CRToDoc(crd)`. Factor a shared helper out of `handlePutPipeline` only if it is a pure extraction; otherwise duplicate the ~20 lines with a comment pointing at PUT. Map `ErrBadRequest`→400, `store.ErrPreviewCap`→409 `{error:"too_many_previews", running_in_workspace, running_in_project, live_in_project}`, `store.ErrPreviewNotFound`→404, else 500. 202 body = `CreateResponse` (snake_case JSON). `DELETE` single → 204, `502 {error:"purge_failed"}` on purge error. `DELETE ?workspace=` → 202 `{deleted:[…], failed:{id:msg}}`. `GET /previews` → `{previews:[{id,workspace,phase,doc_name,until_stage,created_at,expires_at}], next_cursor}`; `?workspace=` default `default`, `*` = all. `GET /previews/{id}?resample=true`.
- `main.go` env: `DATUPLET_PREVIEW_MAX_RUNNING_PER_WORKSPACE`, `DATUPLET_PREVIEW_MAX_RUNNING_PER_PROJECT`, `DATUPLET_PREVIEW_MAX_PER_PROJECT` (invalid → log + default). Log tailer from `kubernetes.NewForConfig` on the same rest config the K8s client uses. Sampler from the existing `queryCore` (nil → sampler nil → samples skipped with warning).

- [ ] **Step 1: Write failing tests** (`httptest`, fake service seam: define `type previewService interface{ Create…; Get…; List…; Cancel…; Delete…; DeleteWorkspace… }` in the handler file so tests inject a fake; `*preview.Service` satisfies it):
  - `TestPostPreview_ValidationFindings400`: data-generator config missing `limit` → 400 with a finding path ending `.random`.
  - `TestPostPreview_Accepted202`: body echoes `id`, `workspace "default"`, `scratch_namespaces`.
  - `TestPostPreview_Cap409`: fake returns `*store.PreviewCapError{2,2}` → 409 JSON with both counts.
  - `TestPostPreview_ResourcesNonSuperadmin403`.
  - `TestListPreviews_ScopedToWorkspace`: `?workspace=b` passes `"b"` to the service; no param passes `"default"`.
  - `TestDeleteWorkspace_RequiresParam400`.
  - `TestPreviewRoutes_RequireDataAdmin403` for a viewer.
- [ ] **Step 2:** `go test ./pkg/pipelineapi/http/ -run Preview -v` → FAIL. **Step 3:** Implement. **Step 4:** `go test ./pkg/pipelineapi/http/... ./cmd/pipeline-api/...` → PASS; `helm template charts/datuplet-app | grep -A1 DATUPLET_PREVIEW_MAX_RUNNING_PER_WORKSPACE` shows `"2"`.
- [ ] **Step 5: Commit** `feat(preview): REST routes, sweeper wiring and chart values`.

---

### Task 10: CLI `datuplet preview`

**Files:**
- Create: `cmd/datuplet/preview.go`, `cmd/datuplet/preview_test.go`; Modify: `cmd/datuplet/main.go` (dispatch + usage line)

**Interfaces:**
- Subcommands exactly as spec §4.8. Flags parsed with the same `flag.FlagSet` + `loadRemoteArgs(remote, tokenFile, project)` pattern as `pipeline.go`; flags before positionals. `--workspace` default from `$DATUPLET_WORKSPACE`, else `default`.
- `datuplet preview -f doc.yaml` (no subcommand, or `run`): read YAML/JSON (`-` = stdin) → map via `sigs.k8s.io/yaml` or `gopkg.in/yaml.v3` (whichever the CLI already uses) → POST; unless `--no-wait`, poll `GET /previews/{id}` every 2 s until `phase` is terminal **and** `result_ready` is true. Preview GETs use an HTTP client timeout of at least 2 minutes (the first terminal GET assembles the document and samples each table); a GET that times out is retried on the next poll. On timeout (`--timeout` + 2 min slack) or SIGINT POST cancel. `--json` prints the final GET body verbatim. Human output: one line per stage `stage <name> <phase> <dur>`, per output `<bucket>.<table> rows=<n> at_cap=<b> → <scratch_table>`, the schema as an aligned table, then up to 10 sample rows, then warnings. `--rows`, `--until-stage`, `--timeout` (duration → seconds), `--ttl`.
- Exit codes per Global Constraints; validation 400 prints findings (same renderer as `pipeline validate`) and exits 1; 409 prints the counts and exits 1.

- [ ] **Step 1: Write failing tests** with an `httptest` fake API (pattern from `trigger_test.go`):
  - `TestPreviewRun_WaitsAndPrintsJSON`: POST 202 then GET Running then GET Succeeded `result_ready:false` then GET Succeeded `result_ready:true` → stdout is the last body, exit 0, exactly 3 GETs.
  - `TestPreviewRun_FailedUserExit1`, `TestPreviewRun_FailedApplicationExit20`, `TestPreviewRun_ValidationExit1` (findings printed).
  - `TestPreviewRun_WorkspaceFromEnv`: `DATUPLET_WORKSPACE=w1` → POST body `workspace "w1"`.
  - `TestPreviewDeleteWorkspace_CallsBulkRoute`.
- [ ] **Step 2:** `go test ./cmd/datuplet/ -run Preview -v` → FAIL. **Step 3:** Implement. **Step 4:** `go test ./cmd/datuplet/...` → PASS; `make build && bin/datuplet preview --help` lists the flags.
- [ ] **Step 5: Commit** `feat(cli): datuplet preview`.

---

### Task 11: Docs and agent skill

**Files:** `docs/agent-quickstart.md` (new "Step 3b — Preview before you persist" between validate and put, plus workspaces), `docs/pipeline-api.md` (Previews section: routes, request/response shapes, caps, lifecycle, the rewrite divergence and "APPEND vs FULL_LOAD unobservable" notes), `docs/known-limitations.md` (Previews section: input reads are not capped, log tails only ≤1 h after a stage finishes, cancel discards the running stage's output, samples need the query worker, scratch namespaces visible to project members), `.claude/skills/datuplet-operator/SKILL.md` (operator loop step "3b. Preview": `export DATUPLET_WORKSPACE=<task-slug>`, `datuplet preview -f … --json`, read `outputs[].columns_hint`, `--until-stage`, `datuplet query` on `scratch_table`, teardown with `preview delete --workspace … --all`), `CHANGELOG.md` (`### Added` entry), `docs/architecture.md` (one paragraph under pipeline-api responsibilities).

- [ ] **Step 1:** Write the docs; every command shown must match Task 10's flags exactly (grep `cmd/datuplet/preview.go`).
- [ ] **Step 2:** `make lint` → PASS (includes deadcode).
- [ ] **Step 3: Commit** `docs: pipeline preview mode`.

---

### Task 12: e2e scenario + live verification on OrbStack

**Files:** Create `tests/e2e/scenarios_preview_test.go` (follow `scenarios_agent_loop_test.go` + `framework/agent_loop.go` for invoking the CLI and querying) and `tests/e2e/pipelines/k8s/preview-etl.yaml` — an envelope-free two-stage doc modelled on the existing k8s fixtures (they use the in-cluster fixture `http://e2e-http-fixture.datuplet-e2e.svc.cluster.local/posts` and `{{.RunPrefix}}-…` buckets; `pipelines_doc_guard_test.go` parses every fixture, so keep it valid): stage `extract` http-json-extractor → `{{.RunPrefix}}-raw` table `data`; stage `transform` sql-transform → `{{.RunPrefix}}-etl` table `user_summary` (`SELECT "userId", count(*) AS post_count FROM data GROUP BY 1`). Note that the scratch bucket for `{{.RunPrefix}}-raw` is `pv-<id8>-{{.RunPrefix}}-raw` — keep RunPrefix short enough that this stays ≤63 chars or the test must expect the truncated form via the result document's `scratch_table`.

**Scenario `TestPreview_AgentLoop`** (skips unless `E2E_K8S=1`), workspace `e2e-a`, using `preview-etl.yaml` with `--rows 50` (the fixture must return ≥ 51 posts for `at_cap`; check `tests/e2e/manifests` and use a smaller `--rows` if it returns fewer):
- result `phase Succeeded`, `result_ready true`; output with logical `bucket <RunPrefix>-raw` / `table data` has `rows 50`, `at_cap true`, non-empty `schema`, `columns_hint`, and `sample.rows` non-empty; output `<RunPrefix>-etl.user_summary` present.
- `datuplet storage tables` does **not** list `<RunPrefix>-raw.data` or `<RunPrefix>-etl.user_summary`; it lists the `scratch_table`s from the result.
- `datuplet query --sql 'select count(*) from "<scratch ns>"."data"'` returns 50.
- `--until-stage extract` → exactly one stage in `stages`.
- `datuplet preview list --workspace e2e-b --json` does not contain the `e2e-a` ids.
- a preview whose SQL references a missing column → exit 1, `phase FailedUser`, a component `log_tail` non-empty.
- `datuplet preview delete --workspace e2e-a --all` → afterwards no `pv-<id8>-*` table is listed.
- a preview with `--ttl 60s` is purged by the sweeper within 3 min (poll `storage tables`).

- [ ] **Step 1:** Write the scenario; `cd tests/e2e && go vet ./...`.
- [ ] **Step 2:** `duckdb --version` (must print a version; the e2e assertions shell out to it).
- [ ] **Step 3:** Full live run against OrbStack **after the Kubernetes-safety export in Global Constraints** (`kubectl config current-context` must print `orbstack`): `make e2e-k8s` (builds images, installs into `datuplet-e2e`, runs the suite, tears down). Expected: every scenario PASS including `TestPreview_AgentLoop`; record the wall-clock of one capped single-stage preview from the test log for the PR body.
- [ ] **Step 4:** On failure, debug with `make e2e-k8s-deploy` + `scripts/e2e-port-forward.sh datuplet-e2e` and `-run TestPreview_AgentLoop` (see CLAUDE.md), fix in the owning task's files, re-run.
- [ ] **Step 5: Commit** `test(e2e): preview mode agent loop`.
