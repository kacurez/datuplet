# Iceberg v3 + VARIANT + Lakekeeper/OpenFGA upgrade: verification report (2026-11-03)

Branch `feat/iceberg-v3`. Spec: `2026-11-02-iceberg-v3-variant-design.md`. Plan: `docs/superpowers/plans/2026-11-02-iceberg-v3-variant.md`.

The checks ran on OrbStack, using an isolated kubeconfig and the `datuplet-e2e` namespace, with the DuckDB CLI v2.0.0 release that CI installs.

## Cluster e2e

**Upgrade checkpoint, Task 3, on tree `a12d3684`.** `make e2e-k8s` passed with 0 FAIL.
- Lakekeeper v0.14.2, OpenFGA v1.22.1, FGA model 4.13.
- This run included `fga-matrix-bob-trigger-and-browse`, `TestApps_ViewerAndSecurity` and `TestQuery_*`.

**Full suite, Task 10, on tree `588ba29e`.** `make k8s-reload-crds && make e2e-k8s` passed every existing scenario:
- `TestScenarios`: `write-modes`, `multi-table-join` and `fga-matrix-bob-trigger-and-browse`;
- `TestApps_ViewerAndSecurity` (13 subtests);
- `TestQuery_HappyPath`, `TestQuery_AuthzDenied`, `TestQuery_WriteProbe` and `TestQuery_StoragePreview`;
- `TestRegistry_*`, `TestSecretsLadder`, `TestResources_PutGate`, `TestAgentLoop_*` and `TestAuditTrail_ClusterRun`.

The same SKIPs as on `main`: `TestQuery_NetworkPolicyEgress` (kindnet), `TestBigDataJoinProof` (opt-in) and `error-bad-config`.

In that run `TestV3VariantPartitioned` failed at its first check, for a local environment reason. The DuckDB 2.0 CLI switches to "agent mode" output whenever `CLAUDECODE` or `AI_AGENT` is set, and the version check rejected that output. This cannot happen on CI.

**The new scenario, on tree `1ff017c9`, against the same deployed stack.** Run with `env -u AI_AGENT -u CLAUDECODE`, `E2E_REQUIRE=1` and `-run TestV3VariantPartitioned`: **PASS** (102.8 s). It asserts:
- **Tables:** `events` and `events_out` are format v3, and `events` is partitioned by `identity(category)` and `bucket[4](id)`.
- **Column types:**
  - `events`: `payload` is a variant (`category` string, `id` long);
  - `events_out`: `payload` and the derived `json_object(...)::VARIANT` column are variants.
- **Payload values:** DuckDB 2.0 `payload::JSON::VARCHAR` equals the fixture as parsed JSON in both tables. The fixture includes nested strings and integers above 2^53, up to int64 max.
- **Gateway `FORMAT_JSON` read:** payloads come back embedded as JSON values. Read summary: `rows=8 outcome=ok`.
- **Query-worker storage preview:** returns `payload` as JSON text.
- **Delete and re-run:**
  - An iceberg-go merge-on-read delete of `id = 2` produced one deletion vector.
  - Re-running stage 2 read `raw_files=5 decoded_files=1 rows=7 outcome=ok`.
  - The payloads were re-checked in both tables.

**Teardown:** helm uninstall, ComponentDefinition sweep and namespace delete, as `e2e-k8s-deploy` does.

## 2M-row deletion-vector measurement

**Setup.** Host perf fixture: Postgres, Silo and Lakekeeper v0.14.2 with a warehouse allowing format versions 1–3. The gateway is `datuplet gateway` built from this branch.
- Each table holds 2M rows written by http-json-extractor in two data files: 1,826,816 and 173,184 rows.
- The v2 twin was pre-created at format version 2 with the same schema, then filled by the same writer.
- The delete was iceberg-go merge-on-read, 20,000 rows.
  - On v3 it produced a **Puffin deletion vector**.
  - On v2 it produced a **Parquet position-delete file**.
- Reads are full `FORMAT_PARQUET` gateway reads, one fresh gateway process per read. Runs were v3 and v2 interleaved, a w0 warm-up was discarded, and r1–r3 are measured. Time is open-to-drained. S3 GETs are counted at Silo.

| case | format | raw / decoded files | rows | mean time, r1–r3 | S3 GETs |
|---|---|---|---|---|---|
| before delete | v3 | 2 / 0 | 2,000,000 | 0.39 s | 10 |
| before delete | v2 | 2 / 0 | 2,000,000 | 0.33 s | 10 |
| delete in the 1.83M-row file | v3 | 1 / 1 | 1,980,000 | 3.90 s | 110 |
| delete in the 1.83M-row file | v2 | 0 / 2 | 1,980,000 | 4.22 s | 125 |
| delete in the 173k-row file | v3 | 1 / 1 | 1,980,000 | **0.92 s** | 26 |
| delete in the 173k-row file | v2 | 0 / 2 | 1,980,000 | **4.32 s** | 125 |

**Reading.** On v3 only the file the deletion vector applies to leaves the raw path. On v2, a position-delete file's bounds send every data file through the decoded path (known limitation M5c). The gain therefore scales with the share of rows outside the deleted-from file:
- **4.7× faster** when the deletion vector sits in the small file;
- about 8% when it sits in the file holding 91% of the rows.

## Docker hygiene

Docker volume counts were 0 before and 0 after both the e2e work and the perf fixture. No containers other than `k8s_*` remain.
