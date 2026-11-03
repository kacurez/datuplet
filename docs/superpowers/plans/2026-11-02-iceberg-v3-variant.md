# Iceberg v3 + VARIANT + Lakekeeper/OpenFGA upgrade: implementation plan (rev 2)

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Upgrade Lakekeeper to v0.14.2 and OpenFGA to v1.22.1 first, with the FGA model re-forked to 4.13. Then make gateway-created tables Iceberg v3, partitioned tables included, and add VARIANT columns end to end. The plan has 10 tasks. Rev 2 applies the Codex plan review (`codex-plan-review.txt`, Codex, 8 Important, 5 Minor).

**Architecture:**
- **Phase A (Tasks 1–3), infra:** charts, the FGA DSL, pipeline-api relation names and fixture pins. It is verified by e2e before any gateway change lands.
- **Phase B (Task 4):** table creation changes and the partitioned commit. The gateway assigns `first_row_id` itself.
- **Phase C (Tasks 5–8):** a new `variant` type, carried through:
  - the gateway's separate input and destination schemas;
  - a canonical-layout normalizer;
  - the read encoders;
  - the pipeline config;
  - queryengine and preview.
- **Phase D (Tasks 9–10):** hygiene, docs, and a live e2e.

**Tech stack:** Go 1.27.3, iceberg-go v0.7.0, arrow-go v18.8.0 (`arrow/extensions` VariantType, `parquet/variant`), Lakekeeper v0.14.2 (chart `lakekeeper-0.13.0`), OpenFGA v1.22.1 (chart `openfga-0.3.19`, CLI `v0.8.2`), DuckDB 2.0.0 (duckdb-go v2.20000.0).

**Spec:** `docs/superpowers/specs/2026-11-02-iceberg-v3-variant-design.md`. Decisions are referenced as A1–A6, B1–B4, C1–C7 and D1–D2. Evidence: `docs/superpowers/specs/2026-11-02-iceberg-v3-spike-report.md`.

## Global Constraints

- **Branch:** `feat/iceberg-v3`, from `public/main` `b3fca0d5`, in the worktree `/Users/tomaskacur/mydevel/datuplet/.claude/worktrees/all-go-gateway`.
  - One logical commit per task. Fix rounds may add commits.
  - **Never** `git push`, `git tag` or `gh pr create`, and never touch `main`. The maintainer pushes and opens the PR.
  - `git add` exact paths only.
  - Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Never** `rm -rf`; delete tracked files with `git rm`. **Never** a bare `go mod tidy`; use `make tidy`.
- **Frozen:**
  - `api/proto/gateway/v2/*.proto` messages and the generated code. `"variant"` is a new *value* of the free-form `ColumnDef.type` string, not a proto change.
  - `sdk/go`.
  - `components/*` non-test code. The exceptions are `components/queryengine` (C6) and its fixture pins (A4). **Test-only files** under `components/` may be added or changed where a task says so.
  - `cmd/mock-datagateway`.
  - The exit codes and the `DUPLET_STATUS_MESSAGE:` protocol.
- **Secrets hygiene:** never log credentials, IO props or JWTs, and never wrap them into errors.
- **Storage paths are opaque strings:** concatenate them; never `path.Join`.
- **CRDs are hand-maintained** (`charts/datuplet-app/crds/`). Change them together with the kubebuilder markers in `pkg/k8s/api/v1/`. Clusters need `make k8s-reload-crds`.
- **FGA versions:** `fgaModel.version` (`charts/datuplet-app/values.yaml`) must equal `platform.fgaModelVersion` (`charts/datuplet-lakekeeper/values.yaml`); `scripts/verify-versions.sh` checks this. The value is exactly `4.13`.
- **Nested Go modules:** `components/queryengine` and `components/sql-transform` are separate modules with no `go.work`. Test them with `cd <module> && go test -tags duckdb_arrow -count=1 ./...`.
- **Docker cleanup is mandatory** after every test run that starts containers:
  - remove the containers and their volumes;
  - report the before and after `docker volume ls -q` counts;
  - never touch the `k8s_*` containers.
- **Gates for every task:**
  - `CGO_ENABLED=0 go build ./... && go vet ./... && go test -count=1 ./...`, plus `-race` on the touched packages;
  - the `-tags=integration` goplane tests, when goplane, icebergjob or lakekeeper code changes;
  - `helm lint` and `make chart-render-check`, for chart changes.
- **Reviewer:** Codex via codex-companion, per task and for the final whole-branch gate. If Codex hits its usage limit, a Claude opus stand-in gets the same prompt (ruling R0).
  - Critical or Important findings are shown to the maintainer before any fix.
  - Minor findings go to the ledger.

## Review Focus

1. **FGA access drift after the re-fork.**
   - viewer, editor, explicit_reader and project_admin keep exactly today's pipeline-api and Lakekeeper permissions, including ancestor discovery for a table shared on its own.
   - A non-member is denied.
   - Covered by the Task 2 tests and the Task 3 e2e FGA matrix.
2. **Fresh-install order.** On a fresh install, Lakekeeper v0.14.2 must start only after our 4.13 pin exists. Otherwise it installs the upstream model without the Datuplet extensions. The existing `wait-for-fga-pin` hook enforces this. It is verified by the Task 3 e2e, which installs from scratch and passes the FGA matrix and apps.
3. **Overlapping row ids on partitioned v3.** They can come from a retry, two commits in a row, FULL_LOAD, or `commit.retry.num-retries` above 0, including a value set mid-commit. Covered by the Task 4 tests.
4. **VARIANT losing data silently.**
   - Output that comes back null or as mangled CSV.
   - A shredded or non-canonical layout that is rejected or given the wrong type.
   - Invalid JSON that is accepted.
   - JSON numbers that lose precision.
   - String vs JSON-text ambiguity.
   - JSON values printed as Go maps in query results.

   Covered by Tasks 5, 6 and 8.
5. **v2 tables regressing.** An existing v2 table, unpartitioned or partitioned, keeps writing and reading unchanged. Covered by the Task 4 tests.

---

### Task 1: OpenFGA v1.22.1

**Model:** sonnet. **Spec:** A1.

**Files:**
- `charts/datuplet-infra/Chart.yaml`: openfga `0.3.3` → `0.3.19`. `helm dependency update` replaces the vendored `.tgz` and updates `Chart.lock`.
- `charts/datuplet-app/values.yaml:425` and `charts/datuplet-infra/values.yaml:30`: `openfga/cli` `v0.6.2` → `v0.8.2`. Check that every command `templates/authz/bootstrap-job.yaml` uses still exists in v0.8.2. The Codex review confirmed `model write --file` and `--config`.
- `pkg/pipelineapi/authz/openfga_integration_test.go:32`: image `openfga/openfga:v1.22.1`.
- `docs/dependency-upgrades.md`.

Steps:
- [ ] **Step 1:** Make the changes.
- [ ] **Step 2:** Run `helm lint` and `helm template` for `datuplet-infra` and `datuplet-app`, then `go test -tags=integration ./pkg/pipelineapi/authz/... -run TestOpenFGA`. Clean up Docker afterwards.
- [ ] **Step 3:** Commit `chore(deps): OpenFGA v1.22.1 (chart 0.3.19), openfga CLI v0.8.2`.

### Task 2: FGA model 4.13 re-fork + `describe_effective`

**Model:** opus. **Spec:** A2, A3.

**Files:**
- `charts/datuplet-app/files/fga/fga.mod` and `components/*.fga`:
  - Use upstream v0.14.2 `authz/openfga/v4.13/`, 11 components (`gh api repos/lakekeeper/lakekeeper/contents/...?ref=v0.14.2`).
  - Re-apply the extensions per A2. That includes `explicit_reader` in `lakekeeper_table.select_effective` **and** `visible_below`.
- `charts/datuplet-app/values.yaml:48-49` and `charts/datuplet-lakekeeper/values.yaml:19`: `"4.13"`.
- `charts/datuplet-app/templates/authz/bootstrap-job.yaml`: the `:304` hash list and the `:322-336` volume items come from one named template.
- pipeline-api: `"describe"` → `"describe_effective"` at the A3 call sites. Also update:
  - the seeds in `pkg/pipelineapi/http/{pipeline,project,run}_handlers_test.go`;
  - the `|describe|` expectation in `pkg/pipelineapi/apps/internal_test.go:583`.
- New `pkg/pipelineapi/authz/model_test.go`: `TestFGAModelCompiles`. It compiles the modular model with the OpenFGA language Go package and asserts the extension relations exist on the right types.
- `pkg/pipelineapi/authz/openfga_integration_test.go`: `TestOpenFGARealModelAccessMatrix` uploads **our** model.
- `Makefile`: a `test-authz-integration` target (D2).
- `docs/fga-model-upgrades.md`: the 4.13 re-fork recipe.

Steps:
- [ ] **Step 1: Failing tests.**
  - `TestFGAModelCompiles`.
  - `TestOpenFGARealModelAccessMatrix` covers project `describe_effective`, `data_admin` and `datuplet_member`, and table `can_read_data` and `can_write_data`:

    | Subject | Expected |
    |---|---|
    | viewer | read allowed; write and data_admin denied |
    | editor | read, write and data_admin allowed |
    | explicit_reader on table T | `can_read_data` on T; namespace `can_get_metadata` and warehouse `can_use` / `can_get_config` allowed; `can_read_data` on a sibling table denied; `describe_effective` denied |
    | project_admin | everything allowed |
    | non-member | everything denied |

  - Handler and apps tests on `describe_effective`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement.
- [ ] **Step 4:** Gates, plus `make test-authz-integration`, `scripts/verify-versions.sh` and `helm template datuplet-app`.
- [ ] **Step 5:** Commit `feat(authz)!: FGA model re-forked from Lakekeeper 4.13; pipeline-api checks describe_effective`.

### Task 3: Lakekeeper v0.14.2 (then e2e)

**Model:** opus. **Spec:** A4, A5 (no upgrade path), A6.

**Files:**
- `charts/datuplet-lakekeeper/Chart.yaml`: `lakekeeper` `0.10.0` → `0.13.0`. `helm dependency update`.
- `values.yaml:52-58`: image tag `v0.14.2`, and update the comment.
- `templates/config-secret.yaml`: remove `LAKEKEEPER__OPENFGA__CONFIGURED_MODEL_VERSION` and its comments at `:13-16,41-48`. Do not add `AUTHORIZATION_MODEL_VERSION`.
- Regenerate the goldens in `charts/datuplet-lakekeeper/_render/*.golden.yaml`.
- Fixture pins to `v0.14.2`:
  - `pkg/datagateway/goplane/fixture_test.go:33`;
  - `components/queryengine/integration_test.go:283`;
  - `components/queryengine/cmd/datuplet-query/integration_test.go:245`;
  - the comment at `components/queryengine/attach.go:54`.
- `pkg/pipelineapi/lakekeeper/manager.go:110-130`: a comment that the endpoint is deprecated.
- Docs:
  - `docs/dependency-upgrades.md`: the FGA version must equal Lakekeeper's active model version (4.13 for v0.14.2). Bumping Lakekeeper means a re-fork and a fresh install, never an in-place upgrade.
  - `docs/fga-model-upgrades.md`.
  - CLAUDE.md: FGA versioning, and the cross-chart CI drift check that exists.
  - `docs/known-limitations.md`.

Steps:
- [ ] **Step 1:** Make the changes.
- [ ] **Step 2:** Gates, plus `helm lint` for every chart and `make chart-render-check`. Run the integration goplane tests on v0.14.2. If the pre-existing golden drift changes, record that.
- [ ] **Step 3:** Commit `chore(deps)!: Lakekeeper v0.14.2 (chart 0.13.0)`.
- [ ] **Step 4 (controller): Phase A e2e.**
  - `make e2e-k8s` on OrbStack, using the isolated kubeconfig and `datuplet-e2e`. All scenarios must PASS, including the FGA matrix and apps.
  - Docker cleanup.
  - On failure, collect the Lakekeeper, OpenFGA and pipeline-api logs and stop for the maintainer.

### Task 4: Iceberg v3 tables, partitioned included

**Model:** opus. **Spec:** B1, B2, B3.

The draft's Tasks 4 and 5 are merged. v3 creation cannot pass its partitioned tests before partitioned v3 commits work.

**Files:**
- `pkg/datagateway/lakekeeper/partition.go`:
  - `:69-75`: every create sets `format-version: "3"` and `commit.retry.num-retries: "0"`.
  - Remove `CheckWriteSpec`'s partitioned-v3 rejection (`:147-154`). In its place, an existing partitioned v3 table with `commit.retry.num-retries` above 0 returns `ErrInvalidPartition`, naming the property.
  - Carry the table format version in the resolved target (`WriterTarget.FormatVersion`, `lakekeeper.go:112-116`), because Task 5 uses it.
- `pkg/icebergjob/commit_shared.go`:
  - `buildDataFiles` returns the DataFiles plus their builders.
  - Inside each `RetryOnConflict` attempt, after the idempotency and spec-id checks and before `NewTransaction`, when the fresh table is v3:
    - re-check the fresh table's effective `commit.retry.num-retries`. A value above 0 is an error that is not retried.
    - assign `first_row_id = NextRowID() + cumulative Count()` in `CommitJob` path order, for `AddDataFiles` and `ReplaceDataFilesWithDataFiles`.
- Warehouse create bodies: add `allowed-format-versions: [1,2,3]` and `default-format-version: 3`, in `cmd/pipeline-api/admin_lakekeeper.go:117-189` and `pkg/pipelineapi/lakekeeper/manager.go:298,381`.
- Update the v2-pinning tests in `pkg/datagateway/lakekeeper/partition_test.go:86,96,139,145`.

Steps:
- [ ] **Step 1: Failing tests.**
  - `TestCreateTableIsV3`, unpartitioned and partitioned.
  - `TestCreateTableDisablesInternalRetry`.
  - `TestExistingV2TableStaysV2`, partitioned and unpartitioned, both write and read.
  - `TestPartitionedV3RejectsInternalRetriesAtResolve`.
  - `TestPartitionedV3RetryPropertyChangedBeforeCommit`: the property is set after resolve; the commit fails, with no ids assigned.
  - `TestPartitionedV3RetryPropertyChangedBetweenAttempts`.
  - `TestPartitionedV3AppendAssignsRowIDs`: two APPENDs give contiguous ranges that don't overlap, and `next-row-id` matches.
  - `TestPartitionedV3RetryReassignsRowIDs`: a 409 between attempts; the retry uses a fresh `NextRowID`.
  - `TestPartitionedV3FullLoad`.
  - `TestPartitionedV3IdempotencyHitAssignsNothing`.
  - `TestPartitionedV2Unchanged`.
  - `TestWarehouseBodyAllowsV3`, at both call sites.
  - Integration:
    - `TestIntegrationServerWriteV3Unpartitioned`: `first-row-id` and `added-rows` are set, and `next-row-id` advances.
    - `TestIntegrationServerPartitionedWriteV3`.
    - `TestIntegrationV3DeletionVectorKeepsOtherFilesRaw`: 5 files, one deletion vector. The delete attaches to one task only, and the read gives `raw_files=4 decoded_files=1` with the correct count.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement.
- [ ] **Step 4:** Gates, plus `-race` and the integration tests.
- [ ] **Step 5:** Commit `feat(datagateway): Iceberg v3 tables; partitioned v3 commits assign first_row_id`.

### Task 5: VARIANT: type model, schemas, write path

**Model:** opus. **Spec:** C1–C4.

**Files:**
- `pkg/datagateway/schema/types.go`: `TypeVariant` in `ToArrowType`, `FromArrowType` (any `extensions.VariantType`) and `ParseDataType`.
- `pkg/datagateway/manifest/types.go:33-58`: variant → `iceberg.VariantType{}`. An unknown type is an error.
- **Input vs destination schema** (C4), in `server_v2_writing.go` (`openWriter` `:190`, `writableArrowType`), `server_v2_http.go` (`ensureTableWriter`, `processParsedRecord` `:138-176`) and the gRPC write path:
  - apply declared variant columns (C3a, C3c) before both create paths;
  - the existing table schema wins (C3b), and a conflicting declaration is an error;
  - normalize each post-transform record to the destination schema before type validation and the plane `Write`;
  - on a concurrent-create race, re-load and re-resolve;
  - C2 comes from the resolved target's format version.
- `pkg/datagateway/format/variant.go`: a normalizer that builds a canonical `NewDefaultVariantType()` array from:
  - JSON-native values;
  - text (Arrow `utf8`, CSV, Parquet string);
  - any `extensions.VariantArray` layout, rebuilt through `NewVariantBuilder`. The retain-as-is path applies only to the exact canonical type.
- **JSON numbers:** the JSON and both JSONL paths (`format/json.go:56,236,316`) use `json.Decoder.UseNumber` or raw tokens. Existing scalar inference must not change; the existing inference tests pin that.
- A variant partition source becomes `InvalidArgument`, carrying the iceberg-go error.
- **Fixtures** in `pkg/datagateway/testdata/variant/`:
  - a shredded Parquet file from DuckDB 2.0, committed together with the command that generated it;
  - an unshredded file with large-binary storage;
  - an unshredded file with dictionary metadata.

Steps:
- [ ] **Step 1: Failing tests.**
  - `TestVariantJSONNativeValues`: object, array, nested, `"hello"`, `"42"` (stays a string), number, bool and null.
  - `TestVariantJSONExactIntegers`: int64 min and max, plus a nested 9007199254740993, through buffered JSON and streaming JSONL.
  - `TestVariantTextInput`: Arrow utf8 and CSV, valid and invalid, plus the `"\"hello\""` case.
  - `TestVariantNormalizesEveryLayout`: shredded, large-binary, dictionary metadata and canonical all end up type-equal to the default.
  - `TestScalarInferenceUnchangedByUseNumber`.
  - `TestVariantDeclaredBeforeEagerCreate` and `TestVariantDeclaredBeforeDeferredCreate`.
  - `TestVariantExistingTableColumnWins`, including a conflicting declaration that errors.
  - `TestVariantConcurrentCreateRace`.
  - `TestVariantSubsequentChunksNormalized`.
  - `TestVariantToV2TableFailsBeforeUpload`.
  - `TestVariantPartitionSourceRejected`.
  - `TestUnknownTypeNoLongerMapsToString`.
  - Integration `TestIntegrationWriteVariantCommitAndRead`:
    - the file has a Variant-annotated group with `metadata`/`value` binary leaves and no child field ids;
    - the commit succeeds and carries metrics;
    - reading back returns equal values;
    - the DuckDB 2.0 CLI reads it, when the CLI is available.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement.
- [ ] **Step 4:** Gates, plus `-race` and the integration tests.
- [ ] **Step 5:** Commit `feat(datagateway): VARIANT columns on write (input/destination schemas, canonical layout, exact JSON numbers)`.

### Task 6: VARIANT read path

**Model:** sonnet. **Spec:** C5.

**Files:**
- `pkg/datagateway/read_serialize.go`:
  - `checkWireCasts` / `castToWire` (`:92-134`): variant → utf8 JSON text via `VariantArray`. Do not use `compute.CanCast`.
  - `encodeChunk`: JSON and JSONL embed the value; CSV writes JSON text; PARQUET keeps the variant.
- `pkg/datagateway/format/json.go:137-170` and `csv.go:216-252`: add variant cases.
- `pkg/datagateway/server_v2_reading.go:184-193`: advertise variant as `"string"`.

Steps:
- [ ] **Step 1: Failing tests.**
  - `TestReadVariantArrowIPCAsJSONText`: decoded with `sdk/go/arrow`; one batch per chunk.
  - `TestReadVariantJSONEmbedsValues`.
  - `TestReadVariantCSVJSONText`.
  - `TestReadVariantParquetRawAndDecoded`: a deletion vector forces one file to be decoded, and the canonical variant survives on both the raw and the decoded path.
  - `TestReadVariantNullsStayNull`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement.
- [ ] **Step 4:** Gates.
- [ ] **Step 5:** Commit `feat(datagateway): serve VARIANT columns in every read format`.

### Task 7: Pipeline config `columns: variant`

**Model:** sonnet. **Spec:** C7.

**Files:**
- `pkg/pipeline/validate/validate.go:82-91,693`.
- CRD enum in `charts/datuplet-app/crds/datuplet.io_pipelines.yaml:264-270`, and the marker in `pkg/k8s/api/v1/pipeline_types.go:282`.
- `pkg/pipeline/config/types.go:122,133-137`.
- The operator pass-through test.
- **Gateway:**
  - `OutputTableConfig.Columns` variant entries feed C3c.
  - The column declarations sent to components (`pkg/datagateway/server_v2.go:519`, the `GetConfig` path) advertise `variant` as `string`.
- `docs/pipeline-api.md`: the column vocabulary, and a "raw JSON into VARIANT" example that declares every column it needs.

Steps:
- [ ] **Step 1: Failing tests.**
  - `TestValidateColumnsVariant`.
  - A test that the CRD enum and the validator agree.
  - `TestOperatorPassesVariantColumn`.
  - `TestGetConfigAdvertisesVariantAsString`.
  - Integration `TestIntegrationHTTPJSONExtractorVariant`:
    - It runs the **real** `components/http-json-extractor` binary, built from the frozen component, against a real `ServerV2` with the goplane fixture (the `make extractor-local` wiring, adapted).
    - The fixture has nested objects. The declared columns are `category`, `id` and `payload: variant`.
    - `payload` must land as variant, with the objects intact.
    - Test-only files may be added under `components/http-json-extractor/`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement.
- [ ] **Step 4:** Gates, plus `helm lint`.
- [ ] **Step 5:** Commit `feat(pipeline): declare VARIANT output columns`.

### Task 8: queryengine and storage preview with VARIANT

**Model:** opus. **Spec:** C6.

**Files:**
- `pkg/pipelineapi/queryproxy/preview.go:32`: the generated preview SQL projects variant columns as `col::JSON::VARCHAR AS col`, using the column types already known from the table schema. A direct `::VARCHAR` gives DuckDB display text, not JSON (Codex probe, DuckDB 2.0.0).
- `components/queryengine/result.go`: a VARIANT or JSON result column fails with a clear error naming the column. For VARIANT the error suggests `::JSON::VARCHAR`; for JSON it suggests `::VARCHAR`. Detect the type from the result column types, before scanning.

Steps:
- [ ] **Step 1: Failing tests.**
  - `TestPreviewProjectsVariantAsJSONText`: values come back as valid JSON text. Assert parsed-JSON equality on nested strings, booleans, and integers above 2^53, plus a negative control showing a direct `::VARCHAR` would fail that check.
  - `TestQueryVariantResultClearError` and `TestQueryJSONResultClearError`, under `duckdb_arrow`.
  - An apps query-route test on the same error.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement.
- [ ] **Step 4:** `cd components/queryengine && go test -tags duckdb_arrow -count=1 ./...`, plus the root gates.
- [ ] **Step 5:** Commit `feat(queryengine): VARIANT in preview; clear error for VARIANT/JSON results`.

### Task 9: Hygiene and docs

**Model:** sonnet. **Spec:** D1, D2.

**Files:**
- `docker rm -f -v` in `pkg/datagateway/goplane/fixture_test.go:105`, `components/queryengine/integration_test.go:297,303` and `components/queryengine/cmd/datuplet-query/integration_test.go:259`. These are test files, so editing them is allowed.
- `goplane/golden_test.go:3-9`: fix the stale comment.
- CLAUDE.md: the `joinStoragePath` and `backend/` mentions.
- `docs/known-limitations.md` and `docs/architecture.md`:
  - v3 is the default for new tables; v2 tables keep working;
  - deletion vectors;
  - VARIANT rules: unshredded, v3 only, no partitioning, text vs JSON-native input, number handling, `::JSON::VARCHAR` for VARIANT in queries;
  - the internal-retry guard.

Steps:
- [ ] **Step 1:** Make the changes. Run the goplane integration tests, then assert the Docker volume count is the same before and after.
- [ ] **Step 2:** Gates.
- [ ] **Step 3:** Commit `chore: integration fixtures remove volumes; v3/VARIANT docs`.

### Task 10: e2e scenario and live verification

**Model:** opus for the scenario code. The controller runs the cluster part.

**Files:**
- `tests/e2e/pipelines/k8s/v3-variant.yaml`, with two stages:
  1. http-json-extractor → a table partitioned by `identity(category)` and `bucket[4](id)`, with `columns: [{name: category, type: string}, {name: id, type: long}, {name: payload, type: variant}]`.
  2. sql-transform reads that table and writes a second table that carries `payload` through, plus one derived variant column.
- A nested-JSON fixture in `tests/e2e/manifests/`.
- The scenario in `tests/e2e/scenarios_test.go`, and assertions in `tests/e2e/framework/assertions.go`.

Steps:
- [ ] **Step 1:** Write the scenario. It asserts:
  - both tables are format v3, and the first one is partitioned;
  - DuckDB CLI v2.0.0 `SELECT payload::JSON::VARCHAR`, compared as parsed JSON (nested strings and large integers included), matches the fixture in both tables;
  - a gateway `FORMAT_JSON` read embeds the payloads;
  - a query-worker preview returns the payload as JSON text;
  - after an iceberg-go merge-on-read delete of one row in the first table, a gateway read shows `decoded_files=1` with the correct count, and re-running stage 2 reads the decoded file correctly.
- [ ] **Step 2 (controller):**
  - `make k8s-reload-crds`, then `make e2e-k8s`. All scenarios must PASS.
  - The 2M-row deletion-vector measurement on the perf harness: raw vs decoded counts and time, v3 vs v2.
  - Write `docs/superpowers/specs/2026-11-02-iceberg-v3-variant-verification.md`.
  - Tear down and prune the volumes.
- [ ] **Step 3:** Commit `test(e2e): partitioned v3 + VARIANT scenario; docs: v3/VARIANT verification report`.

## Final gate

- A whole-branch Codex review of `public/main..HEAD`, with a roll-up of the ledger Minors.
- `make lint`, `make tidy` (no diff), `scripts/verify-versions.sh` and `make test-authz-integration`.
- `cd components/queryengine && go test -tags duckdb_arrow -count=1 ./...` and `cd components/sql-transform && go test -tags duckdb_arrow -count=1 ./...`.
- Stop and report to the maintainer. **No push and no PR** unless the maintainer asks.
