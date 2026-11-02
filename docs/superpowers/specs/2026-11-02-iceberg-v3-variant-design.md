# Iceberg v3 tables, VARIANT, and the Lakekeeper/OpenFGA upgrade: design

The maintainer asked on 2026-11-02: "plan the v3 upgrade with partitioned tables + variant support in dg. Also include in plan to upgrade lakekeeper and openfga to the latest versions (do this first)". The plan is verified with Codex and executed subagent-driven on a clean branch, `feat/iceberg-v3` from `main` `b3fca0d5`.

Evidence:
- `2026-11-02-iceberg-v3-spike-report.md`: live spike on Lakekeeper v0.12.1 and v0.14.2.
- Research notes, cited as file:line below. They are not committed; the raw notes live in the session scratchpad. Every claim this spec relies on is restated here with its citation.

## Intent

- Tables written by the gateway become Iceberg **format v3**, partitioned ones included.
- The main gain is deletion vectors. A delete then marks one data file, and every other file keeps the raw-Parquet read path. The spike measured `raw_files=6 decoded_files=1` on v3 against `0/7` on v2, and 137 ms against 411 ms at 2M rows.
- Row lineage and VARIANT come with v3.
- The gateway gets **VARIANT** columns, so components can land raw JSON and transformations parse it later.
- Before any of that, Lakekeeper and OpenFGA move to their latest releases.

## Goals

1. **Upgrade first.**
   - Lakekeeper `v0.12.1` → `v0.14.2`: vendored chart `lakekeeper-0.10.0` → `lakekeeper-0.13.0`.
   - OpenFGA server `v1.15.0` → `v1.22.1`: chart `openfga-0.3.3` → `0.3.19`. `openfga/cli` `v0.6.2` → latest `v0.8.2`.
   - Datuplet's FGA model is re-forked from upstream 4.13. pipeline-api authorization keeps its current meaning.
2. New tables the gateway creates are format v3. Existing v2 tables keep being written as v2, with no upgrade.
3. Partitioned v3 writes work: the gateway assigns `first_row_id` itself.
4. VARIANT columns:
   - can be declared and written: JSON values, JSON text, Arrow variant (unshredded or shredded), and Parquet variant, including DuckDB output;
   - are stored unshredded;
   - can be read in every output format without errors or silent nulls;
   - are readable by query-worker, storage preview and apps.
5. An e2e scenario proves partitioned v3 plus VARIANT end to end on the cluster.
6. Integration-test fixtures no longer leak Docker volumes.

## Non-goals

- Converting existing tables from v2 to v3.
- Shredded VARIANT storage.
- Inferring VARIANT from data.
- Row-lineage features such as incremental reads.
- `timestamp_ns`.
- Writing deletes from the gateway.
- Changing the frozen contract: `api/proto/gateway/v2/gateway.proto` messages, `sdk/go`, `components/*`. The exceptions are the queryengine change in D9 and the e2e fixture data.
- **Any migration, upgrade or downgrade path**, for charts, the Lakekeeper DB, the FGA store or tables. Datuplet is always installed from scratch (maintainer: "no migration ever!"). That excludes `upgrade.sh` changes, upgrade tests, rollback procedures and upgrade notes.

## Decisions

### Upgrade (Phase A, first)

**A1. OpenFGA `v1.22.1`, chart `0.3.19`.** These are low-risk:
- Postgres migrations are the same 001–006 in both versions.
- No config renames. We use `preshared` authn, so the v1.18 OIDC rule does not apply.
- The chart delta is +4/−1 in values.

Also bump the integration-test image (`pkg/pipelineapi/authz/openfga_integration_test.go:32`) and the CLI image (`charts/datuplet-app/values.yaml:425`, `charts/datuplet-infra/values.yaml:30`). v1.19 adds a `ErrInvalidModel` check on malformed relation references, so the live e2e must cover our model.

**A2. The FGA model is re-forked from Lakekeeper 4.13.**
- Copy upstream v0.14.2's 11 components; two of them are new, `lakekeeper_catalog_tag` and `lakekeeper_generic_table`.
- Re-apply the four Datuplet extensions where 4.13's `_effective` chains need them:
  - `explicit_reader` feeds `lakekeeper_table.select_effective` **and** `lakekeeper_table.visible_below`. Upstream `visible_below` lists only describe, select, modify and ownership (`fga/v4.13/lakekeeper_table.fga:23`), so without this a user granted access to a single table would lose namespace and warehouse discovery (`can_get_metadata`, `can_use`, `can_get_config`);
  - `viewer` feeds `project.select_effective`;
  - `editor` keeps feeding `project.data_admin`;
  - `editor`, `viewer` and `datuplet_member` stay defined on `project`.
- Version **exactly `4.13`** in both charts (`charts/datuplet-app/values.yaml:48-49`, `charts/datuplet-lakekeeper/values.yaml:19`). Lakekeeper v0.14.2 looks up its hard-coded active version 4.13 (`migration-v0.14.2.rs:115-116`). With our 4.13 pin present first, Lakekeeper's own OpenFGA migration does nothing.
- `bootstrap-job.yaml` hard-codes the component file list in two places (`:304` hash, `:322-336` volume items). Both must list the 11 components. Fold the list into one helper, so the next re-fork cannot miss a file.
- No tuple rewrite or store migration: stores are always created fresh (A5).
- New: a Go test compiles `fga.mod` and its components with the OpenFGA language package. It asserts:
  - the model compiles;
  - the extension relations exist;
  - `viewer` reaches `can_read_data` on a table, and `editor` reaches `can_write_data`, through a small in-process check model.

  If in-process evaluation is not practical, the test compiles the model and checks the relation definitions textually. The OpenFGA integration test then runs our real model.

**A3. pipeline-api uses `describe_effective`.** In 4.13 `project.describe` matches direct grants only, so editor, viewer and project_admin members would be denied. Switch these call sites:
- `pkg/pipelineapi/http/pipeline_handlers.go:481,498`
- `run_handlers.go:148,201`
- `project_handlers.go:71-74`
- `pkg/pipelineapi/apps/handlers.go:35`

Also update the comments in `authz/middleware.go:14,41-43`, the 8 test seeds in `pkg/pipelineapi/http/{pipeline,project,run}_handlers_test.go`, and the explicit `|describe|` expectation in `pkg/pipelineapi/apps/internal_test.go:583`. The production path in `apps/internal.go:541` uses the shared `relationRead`. These relations keep their meaning and are not touched: `data_admin`, `datuplet_member`, `project_admin`, server `admin`.

**A4. Lakekeeper `v0.14.2`, chart `lakekeeper-0.13.0`.**
- Update the image tag (`charts/datuplet-lakekeeper/values.yaml:52-58`), the subchart archive and lock, and the golden renders (`make chart-render-check`).
- Update the Go fixture pins: `pkg/datagateway/goplane/fixture_test.go:33`, `components/queryengine/integration_test.go:283`, `components/queryengine/cmd/datuplet-query/integration_test.go:245`.
- Update the comment at `components/queryengine/attach.go:54`.
- **Remove** the misnamed `LAKEKEEPER__OPENFGA__CONFIGURED_MODEL_VERSION` (`config-secret.yaml:48`, plus comments `:13-16,41-48`). Lakekeeper ignores it. Do **not** set the real `…AUTHORIZATION_MODEL_VERSION`: chart 0.13.0 says that would skip the model update and stop Lakekeeper from starting.
- Correct the docs that describe the old key:
  - `docs/fga-model-upgrades.md` (and the stale "CI does not check cross-chart drift" line, since `verify-versions.sh:22-29` does check it);
  - `docs/dependency-upgrades.md:15-17` (FGA version now moves with Lakekeeper's active version);
  - CLAUDE.md.

**A5. Install from scratch only — no upgrade or migration path (maintainer, 2026-11-02: "no migration ever!").**
- Datuplet is installed fresh with `scripts/install.sh`. This plan adds no upgrade guard, no migration step, no rollback procedure and no upgrade tests.
- Ordering on a fresh install is already enforced: the lakekeeper chart's `wait-for-fga-pin` hook (`templates/bootstrap/wait-for-fga-pin-job.yaml:27`) blocks until the app chart has written our 4.13 pin. Lakekeeper's own OpenFGA migration then finds 4.13 present and does nothing (A2).
- `scripts/upgrade.sh` is not changed by this plan.

**A6. Other Lakekeeper changes.** We are unaffected by:
- the authn rule changes (we set neither `OPENID_SCOPE` nor `OPENID_PROVIDERS__*`);
- the config renames we don't use;
- the subchart `tpl` change (our `extraEnv` has no `{{`);
- the audit format (we don't parse Lakekeeper logs);
- the permissions-API changes (we don't call them);
- the deprecated `DELETE /management/v1/project/{id}`, which still works. Leave it, with a comment.

We benefit from concurrent commits now returning 409 instead of 503, which `RetryOnConflict` handles.

### Iceberg v3 (Phase B)

**B1. The gateway creates every new table with `format-version: 3`.**
- This replaces the v2 pin at `pkg/datagateway/lakekeeper/partition.go:69-75`. An explicit table property works on v0.12.1 and v0.14.2 (spike Q1).
- New warehouses created by `admin_lakekeeper.go:117-189` and `pkg/pipelineapi/lakekeeper/manager.go:298,381` also get `allowed-format-versions: [1,2,3]` and `default-format-version: 3`. Tables other engines create through Lakekeeper then default to v3 as well. Spike: with this default, gateway-created tables became v3 with no code change.
- Existing tables are loaded as they are, and the gateway writes their current version.

**B2. Partitioned v3 commits assign `first_row_id`.**
- In `pkg/icebergjob/commit_shared.go`, inside each `RetryOnConflict` attempt, after the idempotency check and the spec-id check, before `NewTransaction`: set `first_row_id` on each file to `fresh.Metadata().NextRowID()` plus the cumulative record count of the earlier files in this commit, in a stable order (the path order of `CommitJob`).
- DataFiles are still built once before the retry loop, and their builders are kept. `DataFileBuilder.Build()` returns the builder's own pointer, so `FirstRowID(id)` can be updated each attempt (`iceberg-go manifest.go:3261-3287`).
- This applies to `AddDataFiles` and `ReplaceDataFilesWithDataFiles`. Both require it on v3 (`table/transaction.go:1565,1665`).
- Unpartitioned commits keep `AddFiles` / `ReplaceDataFiles`. Those inherit `first_row_id` from the manifest list at the same `NextRowID()` (`snapshot_producers.go:1555-1584`), so both paths stay consistent.
- `CheckWriteSpec`'s partitioned-v3 rejection (`partition.go:147-154`) is removed.

**B3. Guard against iceberg-go's internal retry.**
- When table property `commit.retry.num-retries` is greater than 0, iceberg-go's internal retry re-bases the snapshot's `first-row-id` but not the per-file values, so row-id ranges could overlap (`snapshot_producers.go:1703-1741`). iceberg-go reads the setting from the table being committed (`table/table.go:581`).
- The gateway creates tables with `commit.retry.num-retries: 0`.
- There are two checks.
  - **Early:** an existing partitioned v3 table with the property above 0 fails at writer resolve with `InvalidArgument`, before anything is uploaded.
  - **Every commit attempt:** inside every `RetryOnConflict` attempt, after the idempotency check and before the transaction is built, explicit-row-id commits re-check the *freshly loaded* table's effective setting. Another engine may set the property between resolve and commit. A value above 0 fails the commit with a clear error (FailedApplication) and is not retried.

**B4. Readers need no code change for v3.**
- goplane: raw eligibility already treats deletion vectors as deletes (`eligibility.go:20-37`) and is format-agnostic (spike Q4). Files that physically carry `_row_id` columns written by another engine are decoded, not served raw; that is correct.
- DuckDB 2.0, which query-worker, storage preview, apps and the e2e CLI use, reads v3, deletion vectors and partition pruning (spike Q5).
- The tests in this phase pin those behaviours.

### VARIANT (Phase C)

**C1. Type model.**
- Add `schema.TypeVariant` (`"variant"`) in `pkg/datagateway/schema/types.go`.
- `ToArrowType` maps it to `extensions.NewDefaultVariantType()`.
- `FromArrowType` accepts any `extensions.VariantType`, whatever its storage layout.
- `manifest` maps it to `iceberg.VariantType{}`. The silent default-to-string for unknown types (`manifest/types.go:54-56`) becomes an error.
- `"variant"` is a new *value* of the free-form proto `ColumnDef.type` string. It is not a proto change.

**C2. Variant forces v3.**
- A table created with a variant column is v3 (B1).
- The resolved write target carries the destination table's format version, so a variant column bound for an existing **v2** table fails with `InvalidArgument` at writer resolution, before any file is uploaded.

**C3. Where a column becomes variant.** Inference never produces variant: nested JSON stays inferred as string, as today. A destination column is variant when any of these holds:
- (a) the component's declared schema (`OpenWriter`, `pb.Schema` with `"variant"`) says so;
- (b) the column already exists in the table as variant. The catalog wins, as it does for partition specs;
- (c) the pipeline output `columns` declares `type: variant` (C7).

**C4. Input and destination schemas.** The gateway keeps two schemas per writer: the input schema, as parsed from the component's data, and the destination schema written to the table. This needs explicit ordering (Codex plan review):
1. Declared variant columns (C3a, C3c) are applied to the destination schema **before either creation path**:
   - eager create in `openWriter` with a supplied schema (`server_v2_writing.go:190`);
   - deferred create on the first chunk in `ensureTableWriter`.
2. When the table already exists, its schema is the destination for every column it has (C3b), and declared types that conflict with it are an error.
3. After parsing and the transform pipeline, `processParsedRecord` normalizes **each record** to the destination schema before type validation and the plane `Write`:
   - variant destination columns are converted (see the next item);
   - every other column must match as today.

   This covers the supplied-schema path, the deferred-create path, a concurrent create that races (re-load, then re-resolve), and all later chunks.
4. The conversion depends on how the input column arrived:
   - **JSON-native input** (JSON or JSONL adapter, with numbers kept exact via `json.Decoder.UseNumber` / raw tokens, never through float64): the JSON value is stored as-is. Strings stay variant strings, objects and arrays become variant objects and arrays, and `null` becomes SQL NULL. `{"v":"hello"}` stores the variant string `"hello"`. `{"v":"42"}` stores the string `"42"`.
   - **Text-typed input** (Arrow `utf8`, CSV cells, Parquet string): the text is parsed as JSON text. Invalid JSON is a write error naming the row and column. `{"a":1}` stores an object. To store a variant string from text input, send JSON-quoted text such as `"\"hello\""`.

     This is how frozen-SDK components land variant: the http-json-extractor infers nested objects as JSON-text strings.
   - **Arrow or Parquet variant, any layout:** shredded, or unshredded with large-binary or dictionary metadata, including DuckDB 2.0's shredded Parquet output. The gateway rebuilds it into the canonical `NewDefaultVariantType()` layout through `extensions.NewVariantBuilder`. `UnshredVariant` alone returns unshredded non-canonical layouts unchanged (`variant.go:533`). The only shortcut is a retain-as-is path when the type is exactly canonical.
5. Numbers: integers that fit int64 become variant ints, decimals become variant decimals, and anything else follows arrow-go `variant.ParseJSON` rules. Adding `UseNumber` must not change today's scalar inference for non-variant columns.
6. Partitioning by a variant source is already rejected by iceberg-go (`partitions.go:414-416`). The gateway surfaces that rejection as `InvalidArgument`, with a test.

**C5. Read output.** Every format serves variant without error:

| Format | Variant column as |
|---|---|
| `FORMAT_PARQUET` | Variant, raw passthrough or `encodeParquet` |
| `FORMAT_ARROW_IPC` | `utf8` JSON text. The advertised proto type is `"string"` (frozen SDK, `sdk/go/arrow/reader.go:158`) |
| `FORMAT_JSON` / `FORMAT_JSONL` | The JSON value embedded as JSON, not as a quoted string |
| `FORMAT_CSV` | JSON text |

`checkWireCasts` does not rely on `compute.CanCast` for variant, which reports true but fails at run time. It renders through `VariantArray.Value(i).MarshalJSON()`.

sql-transform reads its inputs as `FORMAT_PARQUET`, so it gets real VARIANT columns, raw or decoded.

**C6. queryengine and VARIANT results.** duckdb-go v2.20000.0 cannot scan VARIANT result columns (`type.go:61-65`). A `::JSON` cast does not help either: duckdb-go decodes JSON into Go maps, slices and float64 (`vector_getters.go:221`), and `components/queryengine/result.go:229` then prints maps with `fmt.Sprintf` (`"map[a:1]"`). So:
- **Storage preview** projects variant columns as `col::JSON::VARCHAR AS col` in the SQL it generates. That SQL lives in `pkg/pipelineapi/queryproxy/preview.go:32`.
  - A direct `VARIANT::VARCHAR` cast produces DuckDB *display* text, not JSON. Codex probed DuckDB v2.0.0: `{'a': 1, 'b': [hello, true]}`, with string quoting lost. Going through `::JSON` first returns real JSON.
  - A test checks parsed-JSON equality on nested strings and large integers, not just that the query succeeds.
- **Arbitrary user SQL** (`datuplet query`, apps) that returns a VARIANT column, or a JSON column, fails with a clear error naming the column. For VARIANT the error suggests `col::JSON::VARCHAR`; for JSON it suggests `col::VARCHAR`. Transparent rewriting is out of scope: it would interfere with single execution, multi-statement and parameter handling.

**C7. Pipeline config.**
- Add `variant` to the output `columns` type vocabulary:
  - `pkg/pipeline/validate/validate.go:82-91`;
  - the CRD enum in `charts/datuplet-app/crds/datuplet.io_pipelines.yaml:264-270`;
  - the kubebuilder marker in `pkg/k8s/api/v1/pipeline_types.go:282`;
  - the comment in `pkg/pipeline/config/types.go:122,133-137`.
- The operator passes columns through to the gateway (`pipelinerun_jobs.go:652-656`).
- **The gateway keeps `variant` internally** for the destination schema (C3c). It advertises it as `string` in the column declarations it hands to the component (`server_v2.go:519`, the `GetConfig` path to components). The frozen extractor and SDK path (`components/http-json-extractor/columns.go:127` → `sdk/go/arrow/inferring_sink.go:198`) accepts only int, long, float, double, boolean and string. The component therefore sends JSON text, and C4 text-typed conversion stores it as variant.
- Declared `columns` select columns for components that honour them, such as http-json-extractor. Any example or e2e that declares a variant column must declare **all** the columns it needs, including partition sources.
- CRDs are hand-maintained, so `make k8s-reload-crds` is needed on clusters.

### Hygiene (Phase D)

**D1. Docker volume leak.** Integration fixtures remove containers with `docker rm -f -v`: `pkg/datagateway/goplane/fixture_test.go:105`, `components/queryengine/integration_test.go:297,303`, `components/queryengine/cmd/datuplet-query/integration_test.go:259`.

**D2. Stale docs.** Fix:
- `goplane/golden_test.go:3-9`, which refers to the deleted `buffer` generator;
- CLAUDE.md `joinStoragePath` and `backend/` mentions;
- `make test-authz-integration`, referenced at `openfga_integration_test.go:10-11` but nonexistent. Add the target.

## Verification

- **Unit and integration:** each phase's tests pass under `-race` and the `integration` tag.
- **OpenFGA integration test:** runs against our real model and passes (A2).
- **Phase A e2e:** `make e2e-k8s` on OrbStack after Phase A. All scenarios pass, so Lakekeeper v0.14.2, OpenFGA v1.22.1 and the 4.13 fork work together, and pipeline-api authorization behaves as before. This includes the FGA-matrix scenario `fga-matrix-bob-trigger-and-browse` and apps.
- **New e2e scenario after Phase C** (columns `category`, `id`, `payload: variant`, all declared): an `http-json-extractor` pipeline over a new fixture with nested JSON objects writes a **partitioned** table:
  - `identity` on a category, `bucket[4]` on id;
  - output `columns` declaring a `variant` column.

  Assertions:
  - the table is format v3 and partitioned (via Lakekeeper or iceberg metadata);
  - the variant column round-trips: DuckDB CLI `v::JSON` values equal the fixture objects;
  - a gateway `FORMAT_JSON` read returns embedded JSON;
  - preview/query through query-worker works;
  - after an iceberg-go merge-on-read delete of one row, the read summary shows exactly one decoded file;
  - a sql-transform stage reading the variant table (`FORMAT_PARQUET`, raw and decoded) and writing a variant output (DuckDB 2.0 shredded Parquet → gateway normalization) round-trips the payload.
- **VARIANT physical layout:** a golden or structural assertion that the file has a Variant-annotated group with `metadata` and `value` binary leaves and no field ids on the children.
- **Final measurement** (verification report): with v3 and deletion vectors, read 2M rows with a 1 % delete in one file, comparing raw vs decoded file counts and time, as in spike Q4.
- **Docker:** volumes are cleaned up after every run.

## Risks

- **FGA re-fork mistakes** would silently widen or narrow access. Mitigation: the compile test, the integration test against the real model, and the FGA-matrix e2e.
- **VARIANT JSON fidelity.** Numbers are kept exact through `UseNumber` and raw tokens, and integers above 2^53 stay exact. Representations arrow-go cannot keep exact go in known-limitations.
- **duckdb-go cannot scan VARIANT or JSON result values faithfully** (C6). Preview projects `::JSON::VARCHAR`; user SQL gets a clear error.
- **e2e infrastructure:** Docker Hub pull limits.
