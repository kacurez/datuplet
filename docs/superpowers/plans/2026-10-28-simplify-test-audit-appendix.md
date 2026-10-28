# Test audit appendix (2026-10-28)

Read-only audits produced for the simplification plan
`2026-10-28-simplify-remove-python-finnhub-stdout-and-trim-tests.md`.
Three reports, verbatim. Line numbers are as of `main` at `29e444b`
(branch `docs/review-guardrails` at `adc765f`). "KEEP / MERGE / DELETE" are
the auditor's proposals; the plan's task text says which are adopted.

---

# Part E — K8s e2e suite (`tests/e2e/`)

# K8s e2e audit — /Users/tomaskacur/mydevel/datuplet/tests/e2e (read-only; nothing edited)

Wall times are my estimates from poll intervals and timeouts in the code. `RunPipeline` polls every 2s with a 3 min default timeout. A one-stage run is roughly 30-60s. I have not measured anything.

Findings that change how the cut should be read:

- **No cancel or reaper e2e scenario exists.** The only "cancel" mention is the comment on the skipped `append-mode` (scenarios_test.go:67-77). The route `POST /api/v1/projects/{pid}/runs/{id}/cancel` exists (pkg/pipelineapi/http/server.go:477). The chart ships `charts/datuplet-app/templates/reaper/cronjob.yaml`, which runs `pipeline-api reap-once`. Neither has e2e coverage.
- **No live error-exit-code scenario exists.** `error-bad-config` (scenarios_test.go:183) has a `SkipReason` for an operator terminal-phase race. `error-crash.yaml` is referenced by nothing.
- **No live APPEND coverage exists.** `append-mode` (scenarios_test.go:62) is skipped for a cancel-watch race under `RunTwice`.
- **`TestErrorMissingTableRejected` is a name-validator test, not a missing-table test.** The fixture's table is `does-not-exist`, and `tableNameError` rejects the hyphens (pkg/pipeline/validate/validate.go ~L353-360). Nothing in e2e tests a runtime missing-table read. In stage 0, a nonexistent input table produces no finding at all (validate.go ~L407-418).
- **`TestQuery_AuditSmoke` can never fail.** Every failure path calls `t.Skip`.
- **`TestQuery_NetworkPolicyEgress` self-skips on any CNI that does not enforce NetworkPolicy.** OrbStack/kindnet is documented as non-enforcing in the test. CI uses kind (e2e.yml), so I expect it to skip there too, but I did not verify kind's CNI. If so, there is effectively zero NetworkPolicy e2e coverage. Three NetworkPolicy templates exist: app-worker, query-worker and pipeline-observer.

## 1. Scenario map

### TestScenarios (scenarios_test.go:265), 11 table rows

| Scenario (line) | Pipeline YAML / components | What it proves | Est. time |
|---|---|---|---|
| explicit-tables (:44) | explicit-tables.yaml; http-json-extractor | Explicit `outputs.tables` mode; table exists, 100 rows | 1 run, ~40s |
| full-load-mode (:53) | full-load-mode.yaml; http-json-extractor; `RunTwice` | FULL_LOAD replaces: still 100 rows after 2 runs | 2 runs, ~1.5 min |
| append-mode (:62) | append-mode.yaml; http-json-extractor; `RunTwice` | APPEND accumulates to 200. **SKIPPED** (cancel-watch race) | 0 |
| (drop-processor, :82-101) | drop-processor.yaml; commented out | Parquet field_id bug; dead code | 0 |
| multi-component-stage (:102) | multi-component-stage.yaml; 2x http-json-extractor in one stage | Two components in one stage; posts=100, users=10, schema | 1 run, ~40s |
| read-back (:120) | read-back.yaml; http-json-extractor then **stdout-writer** | Two-stage dependency. Assertion is only a duckdb row_count of 100 on the extractor's bucket | 1 run, 2 stages, ~60-90s |
| http-json-extract (:128) | http-json-extract.yaml; http-json-extractor | Defaultbucket mode; at least 10 rows, schema columns | 1 run, ~40s |
| duckdb-etl (:147) | duckdb-etl.yaml; http-json-extractor then sql-transform | Two-stage ETL: GROUP BY userId, 10 rows | 1 run, ~60-90s |
| multi-table-join (:163) | multi-table-join.yaml; 2x http-json-extractor then sql-transform | Same as duckdb-etl plus a 2-input join; 100 rows | 1 run, ~60-90s |
| error-bad-config (:183) | error-bad-config.yaml; data-generator@dev with `userErrorMessage` | exit_code=1 and FailedUser. **SKIPPED** (operator/harness phase race) | 0 |
| fga-matrix-alice-trigger-and-browse (:241) | http-json-extract.yaml; user Alice | Default identity (project_admin) triggers and browses. Alice is the default user for every other scenario too | 1 run, ~40s |
| fga-matrix-bob-trigger-and-browse (:253) | http-json-extract.yaml; user Bob | Editor (non-admin) identity triggers and browses. `K8sBackend.apiSessionFor` provisions a real DB user and a real-UUID FGA grant | 1 run, ~40s |

That is 9 live rows making 10 pipeline runs.

### Other top-level tests

| Test (file:line) | Components / YAML | What it proves | Est. time |
|---|---|---|---|
| TestBigDataJoinProof (scenarios_test.go:294) | manual-large-input-join.yaml; data-generator then sql-transform | Streaming arrow join of ~400MB facts. Opt-in via `RFC011_BIG_DATA_PROOF=1`, skipped by default | 5-10 min if enabled |
| TestK8sFixturesAreValidDocs (pipelines_doc_guard_test.go:21) | globs every `pipelines/k8s/*.yaml` | Offline: each fixture parses via `config.Parse`. No cluster needed | <1s |
| TestSecretsLadder (scenarios_secrets_test.go:96) | secrets-happy.yaml plus an inline copy; http-json-extractor | `$[api_token]`: PUT warns, POST /runs 400, then PUT secret, run succeeds, per-run snapshot Secret `datuplet-runsecrets-<8>` exists and is immutable on rotation, ownerRef GC on PipelineRun delete | 1 run, ~1 min |
| TestRegistry_UnknownComponent (scenarios_registry_test.go:147) | unknown-component.yaml | PUT returns 400 naming `does-not-exist` | <5s |
| TestRegistry_ErrorSchemaInvalid (:196) | error-schema-invalid.yaml; data-generator@v0.0.1 | PUT returns 400 on `additionalProperties` against the stable CRD schema | <5s |
| TestErrorMissingTableRejected (:240) | error-missing-table.yaml; **stdout-writer** | PUT returns 400, but only because of the hyphenated table name | <5s |
| TestRegistry_UnpinnedResolvesStable (:272) | unpinned-resolution.yaml; data-generator, version omitted | Omitted version resolves to stable v0.0.1, with version and image recorded in `status.components[0]` | 1 run, ~40s |
| TestRegistry_Freeze_MidRunMutation (:330) | freeze-longrunning.yaml; data-generator@dev | Mid-run kubectl patch of the Pipeline and the ComponentDefinition image does not affect the admitted run (frozen `resolvedSpec`, Job image, generation). Mutates the shared "dev" CRD and restores it in cleanup | ≥60s (component `timeoutInSeconds: 60`) |
| TestResources_RegistryDefaultsApplied (scenarios_resources_test.go:110) | resource-default.yaml; data-generator@v0.0.1 | Registry `resources.default` lands on the Job container and in `resolvedSpec` | 1 run, ~40s |
| TestResources_ClampedAtAdmission (:161) | resource-clamp.yaml; data-generator@v0.0.1 | Misnamed: PUT with over-max resources returns 400 "exceeds registry max". Never runs a pod | <5s |
| TestResources_OverMaxModificationRejected (:370) | inline doc; Go constant | Non-superadmin PUT with over-max resources returns 403 (modification gate beats 400) | <10s |
| TestResources_SuperadminPutSucceeds (:395) | inline doc | Non-superadmin within-max returns 403; superadmin returns 204/200 | <10s |
| TestAuditTrail_ClusterRun (scenarios_audit_test.go:35) | explicit-tables.yaml | Iceberg snapshot summary has `datuplet.run-mode`, `pipeline-api`, `actor`, `run-id` | 1 run, ~40s |
| TestAgentLoop_SchemaToVerify (scenarios_agent_loop_test.go:111) | inline docs; data-generator@v0.0.1 | Real `datuplet` CLI headless (cli-api JWT): components get --schema, validate bad (exit 1), validate good, put, trigger --wait, duckdb verify | `go build` plus 1 run, ~2-3 min |
| TestAgentLoop_UserApps (scenarios_agent_loop_apps_test.go:116) | no pipeline; apps fixtures | CLI apps init/put/render/promote/token, throwing bundle gives exit 1 with `author_log`, viewer token exchange. Uses an in-process reverse proxy as a stand-in ingress | second `go build` plus ~1-2 min |
| TestApps_ViewerAndSecurity (scenarios_apps_test.go:72) | no pipeline; app-worker, 2 replicas | 12 stages (:173-602): draft, token exchange 302 and cookie, render HTML/JSON, query_audit principal, SQL injection as bound literal, cross-app cookie 401, revoke propagates within 15s, promote v2 reaches all replicas within 15s, render-log | ~2-3 min |
| TestQueryBootstrap (scenarios_query_test.go:568) | query-worker | query-worker has ready replicas; grants admin FGA; resolves pid. Effectively fixture setup | ~5s |
| TestQuery_HappyPath (:624) | seeds via http-json-extract.yaml (`ensureQueryTable`, one run per process) | POST /projects/{pid}/query returns count=100 | seed ~40s, then <2s |
| TestQuery_AuthzDenied (:711) | seeded table | Zero-grant user gets 403 kind=forbidden from the project gate | <10s |
| TestQuery_Truncation (:800) | none (`range()`) | `max_rows` cap of 10000 and truncated=true | <5s |
| TestQuery_Timeout (:850) | none | `timeout_s=1` gives 408 kind=timeout | 1-10s |
| TestQuery_Concurrency (:924) | none | 3 concurrent same-user queries yield a 429/503 | up to ~15-20s |
| TestQuery_WriteProbe (:1057) | seeded table | Viewer can SELECT; viewer INSERT is denied as sql_error | ~15s |
| TestQuery_NetworkPolicyEgress (:1164) | canary pod plus read_parquet to openfga | Query-worker egress is blocked. Self-skips on a non-enforcing CNI | 30s if it runs |
| TestQuery_AuditSmoke (:1321) | seeded table | Greps `query_audit outcome=ok` in pipeline-api logs. Best-effort, always skips instead of failing | ~5s |
| TestQuery_StoragePreview (:1393) | seeded table | Preview route served by query-worker in under 5s | ~5s |
| TestQuery_LocalMint_Enabled (scenarios_local_query_test.go:98) | none | `allowClientSideQuery=true` in the chart gives POST /query/token 200 and a JWT with the expected claims | <2s |

`scenarios/gcs-pipeline-k8s/` is a shell scenario. `make e2e-k8s-gcs` runs it, CI does not, and its README says bootstrap wiring is incomplete. It is not a Go test, uses http-json-extractor only, and is out of scope.

## 2. Overlap groups and what to keep

1. **Single-extractor http-json happy paths.** Group: explicit-tables, http-json-extract, fga-alice, fga-bob, multi-component-stage, the `ensureQueryTable` seed, and the audit test. The seed runs http-json-extract.yaml again and the audit test runs explicit-tables.yaml again. All end in "100 rows via duckdb".
   - Keep **multi-table-join** (multi-component stage, tables-mode outputs, 2-input sql-transform) as the multi-stage happy path.
   - Keep **fga-bob** as the only identity scenario. Alice is the default identity of every other scenario. Move the `schema` assertion from http-json-extract onto it.
   - Delete the rest.
2. **Two-stage "read what stage 1 wrote".** Group: read-back, duckdb-etl, multi-table-join. Keep multi-table-join. It is a strict superset of duckdb-etl, and of read-back, whose only assertion never touches stdout-writer.
3. **FULL_LOAD vs APPEND.** Merge full-load-mode and append-mode into one scenario `write-modes`: one pipeline with two components, one FULL_LOAD and one APPEND, `RunTwice`. Expect FULL_LOAD=100 and APPEND=2x. Risk: the append skip race may hit it. Fallback is to keep append-mode skipped with an issue link.
4. **PUT-time 400 rejections.** Group: unknown-component, error-schema-invalid, missing-table, resource-clamp. These test `pkg/pipeline/validate` and policy findings, which are unit-testable.
   - Keep none as standalone. TestAgentLoop_SchemaToVerify steps 1-3 already prove live registry-schema enforcement through the CLI.
   - If you want one PUT-400 test, keep error-schema-invalid; it is cheap, with no pods.
5. **Resources.** Group: RegistryDefaultsApplied, ClampedAtAdmission, OverMaxModificationRejected, SuperadminPutSucceeds. The non-superadmin 403 is asserted twice, in OverMaxModificationRejected and SuperadminPutSucceeds.
   - Collapse the three PUT tests into one `TestResources_PutGate` with three assertions: nonadmin 403, superadmin over-max 400, superadmin within-max 204.
   - The inline `resourcesOverMaxPipelineYAML` constant already exists, so resource-clamp.yaml is redundant.
   - Fold RegistryDefaultsApplied into UnpinnedResolvesStable. unpinned-resolution.yaml already has no resources block and resolves to v0.0.1, so it can assert the Job and `resolvedSpec` defaults too. That saves one pod run and removes resource-default.yaml.
6. **Query-worker limits.** Group: Truncation, Timeout, Concurrency. These are query-worker or proxy logic with no warehouse. I found unit tests touching max_rows, timeout and rate_limited in components/queryengine/engine_test.go, components/queryengine/cmd/query-worker/server_test.go, and pkg/pipelineapi/queryproxy/handler_test.go and audit_test.go. I did not check each scenario's exact assertion. Delete all three, or merge into one `TestQuery_Limits` if you want a deployed smoke of the env-var wiring.
7. **Query audit.** TestQuery_AuditSmoke never fails, and the strict version is the `query_audit_attributes_app_principal_jti_and_ok` stage in TestApps_ViewerAndSecurity (:378). Delete AuditSmoke. TestAuditTrail_ClusterRun is a different audit (iceberg snapshot summary), so keep it.
8. **Authz denial.** TestQuery_AuthzDenied (zero-grant 403 at the project gate) and TestQuery_WriteProbe (viewer can read, write denied at the catalog) check different layers. Keep both. The resources 403 is a third, different gate. No route is duplicated.
9. **Apps.** TestAgentLoop_UserApps overlaps TestApps_ViewerAndSecurity on promote, token mint, exchange and render-log. It also builds the CLI a second time. It mostly tests the CLI plus a fake ingress, not chart behaviour. Medium-confidence recommendation: drop it, or move only the throwing-bundle/`author_log` check into D2. If you keep it, share one `go build` between the two agent-loop tests.
10. **TestQueryBootstrap** is setup, not a test. Move `ensureAdminFGAGrant` into the `getQueryProjectID` once and delete it. Every query test currently depends on it running first.

## 3. stdout-writer dependents (under tests/e2e, excluding .claude)

| Reference | Role of stdout-writer | Stand-in? |
|---|---|---|
| framework/components_bootstrap.go:155, in `builtinE2EComponents` | Registers its ComponentDefinition | Delete the line |
| pipelines/k8s/read-back.yaml:16-17 | Stage-2 sink reading `readback.data`. Nothing reads its output or logs | Not needed. The assertion is a duckdb row_count on the stage-1 table. duckdb-etl and multi-table-join already prove a stage reads a prior stage's table. Delete scenario and YAML |
| scenarios_test.go:120-127, the read-back row | Same | Same |
| pipelines/k8s/error-missing-table.yaml:5-6 | Carries an input table named `does-not-exist` | The 400 comes from the hyphenated table-name check, not from the component. Delete YAML and test |

- No assertion helper in `framework/assertions.go` depends on stdout-writer. All assertions read Iceberg via duckdb.
- Outside tests/e2e, stdout-writer is still referenced by:
  - Makefile:56, :75 and :111
  - .github/workflows/e2e.yml:86
  - .github/workflows/upgrade-e2e.yml:138
  - .github/workflows/_release-components.yml:31-35
  - .github/workflows/release.yml:62
  - charts/datuplet-app/templates/components/stdout-writer.yaml
  - tests/release-verify/pipeline.yaml
  - docs and the `.claude` skills

## 4. finnhub dependents

None under tests/e2e. Makefile:62 already says so ("skips finnhub-extractor (no scenario references it)"). References outside e2e:
- Makefile:58
- upgrade-e2e.yml:139
- _release-components.yml:45-49
- release.yml
- charts/datuplet-app/templates/components/finnhub-extractor.yaml
- the `components/finnhub-extractor/` directory
- docs and the `.claude` skills
- ui/product/pages/pipeline-detail.js
- examples/pipelines/processors-drop.yaml

## 5. Recommended cut list

### Delete
- `explicit-tables` (scenarios_test.go:44). Covered by multi-table-join.
- `http-json-extract` (:128). Covered by the seed and fga-bob.
- `duckdb-etl` (:147). Covered by multi-table-join.
- `multi-component-stage` (:102). Covered by multi-table-join.
- `read-back` (:120). Covered by multi-table-join (see section 3).
- `fga-matrix-alice-trigger-and-browse` (:241). Covered by every default-user run plus fga-bob.
- Commented `drop-processor` block (:82-101) and drop-processor.yaml.
- `TestRegistry_UnknownComponent` (scenarios_registry_test.go:147). Validator unit logic.
- `TestRegistry_ErrorSchemaInvalid` (:196). Covered by the agent-loop validate steps (optional keep).
- `TestErrorMissingTableRejected` (:240).
- `TestResources_ClampedAtAdmission` (scenarios_resources_test.go:161), `TestResources_OverMaxModificationRejected` (:370). Merged into the resources PutGate test.
- `TestQuery_Truncation` (scenarios_query_test.go:800), `TestQuery_Timeout` (:850), `TestQuery_Concurrency` (:924). Unit-covered.
- `TestQuery_AuditSmoke` (:1321).
- `TestQueryBootstrap` (:568). Fold into `getQueryProjectID`.
- Optional: `TestAgentLoop_UserApps` (scenarios_agent_loop_apps_test.go:116).
- Fixtures to delete: read-back.yaml, error-missing-table.yaml, unknown-component.yaml, resource-clamp.yaml, resource-default.yaml, drop-processor.yaml, explicit-tables.yaml, duckdb-etl.yaml, multi-component-stage.yaml. error-schema-invalid.yaml too if you drop that test. error-crash.yaml is already orphaned.

### Merge
- `full-load-mode` + `append-mode` become `write-modes` (see group 3).
- `TestRegistry_UnpinnedResolvesStable` + `TestResources_RegistryDefaultsApplied` become one test over unpinned-resolution.yaml.
- `TestResources_SuperadminPutSucceeds` becomes `TestResources_PutGate` (nonadmin 403, superadmin over-max 400, superadmin within-max 204).
- `TestAuditTrail_ClusterRun` should reuse the `ensureQueryTable` seed run. Cache `result.RunID` and read `<prefix>-api.data`, which saves a pipeline run.
- Repoint any remaining `explicit-tables.yaml` use.

### Add or repair (the keep list requires these and they are currently missing)
- **Cancel:** reuse the freeze long-running run. After the freeze assertions, POST `/runs/{id}/cancel`, assert phase Cancelled, and assert pods and Secrets are cleaned up. This also lets you cut the 60s `timeoutInSeconds`.
- **Reaper:** a new test that triggers `reap-once`.
- **Error exit-code:** un-skip `error-bad-config` once the operator race is fixed. Alternatively revive the orphan error-crash.yaml (sql-transform reading a nonexistent table, which validates in stage 0 and fails at run time). Assert only exit_code != 0 or non-Succeeded to dodge the FailedUser/FailedApplication race. I haven't verified which phase it yields.
- **APPEND:** covered only by the merged `write-modes`, or by keeping append-mode skipped.

### Resulting suite
- TestScenarios: `multi-table-join`, `write-modes`, `fga-matrix-bob`, `error-bad-config` (skipped for now)
- TestBigDataJoinProof (opt-in)
- TestSecretsLadder
- TestRegistry_UnpinnedResolvesStable + defaults
- TestRegistry_Freeze_MidRunMutation (+cancel)
- TestResources_PutGate
- TestAuditTrail_ClusterRun
- TestQuery_HappyPath, AuthzDenied, WriteProbe, StoragePreview, NetworkPolicyEgress, LocalMint_Enabled
- TestApps_ViewerAndSecurity
- TestAgentLoop_SchemaToVerify
- TestK8sFixturesAreValidDocs

Numbers: top-level Test funcs go from 27 to about 16 (17 with `UserApps` kept), and Test-function scenarios 31 to ~21. Pod-running pipeline runs go from 17 (10 in TestScenarios, plus seed, audit, secrets, unpinned, resource-default, freeze, agent-loop) to 9. That count is 1 multi-table-join, 2 write-modes, 1 bob, 1 seed (audit now reuses it), 1 secrets, 1 unpinned+defaults, 1 freeze, 1 agent-loop. It becomes 10 when error-bad-config is enabled. Estimated wall time drops from roughly 12-15 min to 7-9 min. That is a gut estimate, not a measurement.

Coverage mapping for the required keep list:
- Happy-path multi-stage: multi-table-join
- FULL_LOAD vs APPEND: write-modes
- Secrets: TestSecretsLadder
- Registry/unpinned: UnpinnedResolvesStable
- Resources: unpinned+defaults and PutGate. The real controller clamp is not e2e-reachable and is unit-covered.
- Cancel/reaper: missing today (see Add or repair)
- Query + authz denial: HappyPath and AuthzDenied (+WriteProbe)
- Apps viewer/security: TestApps_ViewerAndSecurity
- Agent loop: SchemaToVerify
- Audit: TestAuditTrail_ClusterRun
- Error exit-code: currently skipped (see Add or repair)

### Hazards when editing
- scenarios_resources_test.go holds `grantAdminSuperadmin`, `execPipelineAPIAdmin` and `isFGAAlreadyExists`, which TestMain and the PutGate test use. Deleting the file wholesale breaks main_test.go.
- scenarios_resources_test.go and scenarios_registry_test.go share helpers (`registrySkipUnlessReady`, `kubectlJSONPath`, `waitForComponentJob`, `pipelineRunNameForRun`, `assertJobResource`). These stay used after the cut.
- Freeze mutates the shared "dev" ComponentDefinition and restores it in cleanup. Keep it isolated.

## 6. Framework helpers unused after the cut

Dropped by the cut:
- Assertion type `min_row_count` and `AssertMinRowCount`, if you switch bob's assertion to `row_count`.
- In scenarios_query_test.go: `intPtr` and `extractAuditLines`.
- `components_bootstrap.go:155` ("stdout-writer"). The `builtinE2EComponents` comment mentions stdout-writer implicitly.
- Mostly-orphaned `Scenario` fields: `ExpectError` and `SkipReason` are used only by error-bad-config and append-mode.

Already dead before the cut, so safe to delete:
- `framework/backdate.go` (`BackdateSnapshots`)
- `framework/secrets.go` (`writeSecretsDir`)
- `Scenario.SetupPipeline`, `SetupDelay`, `BackdateSetup`, `BackdateBucket`, `BackdateTable`
- `RunOpts.StorageRoot`, `Env`, `SecretsDir`
- `framework/k8s_token.go` in full (`extractPipelineDoc`, `rewriteYAMLWithRunToken`, `createRunTokenSecret`, `RunTokenLifetime`), used only by k8s_token_test.go, plus `MintTestUserRunToken` in `signer.go`
- `K8sBackend.Resources` and the `Backend` interface
- Assertion type `column_absent` and `AssertColumnAbsent` (only the commented drop-processor used it)
- `query` assertion type and `AssertQuery`
- `TemplateVars.TestDataDir` and `WarehouseDir`. `TestScenarios` points `testDataDir` at a nonexistent `tests/e2e/testdata`.
- `CharlieID` and `DoraID` in `framework/users.go`. They are seeded in `TestUsers` but no scenario uses them; the header comment says the negative paths are unit-covered.

Still used and should stay:
- `AssertTableExists` (BigData uses it)
- `AssertSchema` and `AssertRowCount`
- `AssertBrowseTableSucceeds` (bob uses it)
- `K8sQueryTarget`, `MintAdminCLIToken`, `ResolveDatupletProjectID`
- `Seed*FGAGrant`

---

# Part A — pkg/appworker and pkg/pipelineapi/*

# Over-testing audit: pkg/appworker and pkg/pipelineapi/*

Read-only. Nothing was edited, and `.claude/worktrees/` and build-tagged integration files were ignored.

Method and limits:
- I read nearly every test body in the listed packages, but not all of `http/ui_contrast_test.go`, `storage/handlers_test.go` TableInfo tests, `storage/testdata` helpers, or the first ~346 lines of `appworker/server_test.go` (fixtures).
- `auth/password_test.go` could not be read (permission denied). Its entries below are inferred from test names and LOC only. Verify before acting on them.
- LOC savings are estimates from reading the code, not measured.
- "KEEP" counts are Test funcs surviving after all proposed merges and deletes.
- Line numbers are the `func Test...` start line.

Cross-cutting findings:
- **JS source-grep tests.** `appworker/shell_test.go` asserts that substrings exist in shipped JS/CSS. About 40% of it pins cosmetics or implementation tokens rather than behavior. One block tests a Go "port" of the JS that exists only inside the test file.
- **Claim shapes re-asserted in four layers.** The same token claim shapes are asserted in `tokens/*_test.go`, `http/cli_token_handler_test.go`, `http/query_token_handler_test.go` and `apps/identity_manager_test.go`.
- **Copy-paste setup in DB-backed http tests.** `http/pipeline_handlers_test.go` repeats the same 8-line project/lakekeeper/authz seed in ~30 tests, which is ~200 LOC for a `seedProject` helper to absorb.
- **Per-user tests that should be tables.** The following are written as one test per case: bearer-JWT rejections, `mustBeSuperadmin` outcomes, queryproxy gate tests, `S3Props` tests, k8s coalesce and observer skip tests.

---

## pkg/appworker  (6 files, 7,460 LOC, 199 Test funcs)
KEEP: ~130. They pin: auth/cookie/CSRF/rate-limit/XFF security, render-budget and gate semantics, response matrix and param normalization, XSS escaping, the markdown sanitizer allowlist, same-origin and no-innerHTML guards, and the 15s/60s token-cache contracts.

MERGE:
- `TestResolve_HappyPathAndHeader` + `TestResolve_EmptyChannelDefaultsToProduction` -> one table (apiclient_test.go:110, :147). Same route and headers; only the channel input differs. Saves ~20.
- `TestVerifySession_ForwardsBearerInSeparateHeader` + `..._ForwardsCookieAndOmitsForwardingHeaderWhenNoAuthz` + `..._NeverPutsServiceTokenInForwardingHeader` + `..._NoSessionIsNotAnError` -> one table of (cookie, authz) -> expected headers (apiclient_test.go:1010, :1043, :1073, :1145). Saves ~40.
- `TestBundle_RejectsBodyExceedingStructuralCap` + `TestBundle_BodyAtExactCapIsAccepted` -> one boundary table (apiclient_test.go:536, :569). Saves ~25.
- `TestBundle_CallerCannotCorruptCachedBytesByMutatingReturnedSlice` + `TestBundle_PutDoesNotAliasCallerBuffer` -> a single LRU unit test with get-copy and put-copy halves (apiclient_test.go:464, :512). Saves ~40.
- `TestVerifyToken_InactiveTokenNegativeCache60s` + `..._InactiveTokenNegativeCacheSharedAcrossDifferentSecrets` -> one test (apiclient_test.go:664, :706). Same harness, same cache. Saves ~25.
- The four 14s/16s TTL tests could share one fake-clock helper or table: `TestResolve_CachesWithin15sFakeClock` :169, `TestCheckTokenActive_CachesWithin15sFakeClock` :942, `TestVerifyToken_PositiveCache15s` :621, `TestVerifySession_CachesPerSessionWithin15s` :1101. Saves ~50 (optional).
- `TestResolveClientIP_NoXFFUsesRemoteAddr` :1383 + `..._TrustedPeerSelectsFirstUntrustedFromRight` :1419 + `..._AttackerPrependedEntriesDoNotShiftSelection` :1437 + `..._TwoHopConfiguration` :1461 + `..._NormalizesAndRejectsGarbage` :1485 -> one table in auth_test.go. Keep `..._IgnoresXFFFromUntrustedPeer` :1400 as a row. Saves ~55. All are security cases, so merge only, no coverage loss.
- `TestParseCookie_VerifiesMACBeforeParsingThePayload` :1310 + `..._RejectsEmptyFieldsEvenWhenSigned` :1336 -> one table. Saves ~10.
- `TestExchange_XFFCannotBypassTheFailureLimiterFromAnUntrustedPeer` :1507 -> a case inside `TestExchange_EleventhFailureFromOneIPAndAppIsRateLimited` :488. Saves ~30.
- `TestExchange_SuccessConsumesNoFailureBudget` :1623 + `..._UpstreamErrorConsumesNoFailureBudget` :1657 -> one test with a shared "10 failures then 429" tail. Saves ~25.
- `TestLoadConfigOutputDocMaxBytesClampsToStructuralCap` :179 + `..._BundleMaxBytesClampsToStructuralCap` :196 -> rows of `TestLoadConfigCapsClampNotError` :143. Saves ~23.
- render_test.go: `TestRenderUsesRequestIDFromContext` :409 -> one assertion in `TestRenderHappyPath` :319 (~13). `TestRenderMintsAFreshTokenPerRender` :445 + `..._MintsOncePerRenderAcrossQueries` :466 -> one test (~20). `TestRenderQueryTransportFailureIsUnavailable` :514 + `..._ImpersonateFailureIsUnavailable` :535 -> one table (~20). `TestRenderBundleFetchFailureIsUnavailable` :1259 + `..._WithoutAPIIsUnavailable` :1274 -> one table (~15).
- render_test.go chart/refresh: `TestRenderRejectsChartDataURLByPointer` :913 -> top-level row in `TestRenderValidatesNestedChartSpecs` :938 (~25). `TestRenderLeavesOtherFieldsIntactWhenClamping` :1046 + `..._NonIntegerRefreshIntervalIsRejected` :1071 -> rows in `TestRenderClampsRefreshInterval` :1009 (~25).
- server_test.go: `TestRoute_SubPathBecomesCtxPath` :374 + `..._BareAppHasRootCtxPath` :388 -> one table (~12). `TestRoute_CtxPathTooLongIsBadRequest` :426 -> a row in `TestRoute_TraversalIsBadRequest` :396 (~12). `TestReadParams_POSTRequiresJSON` :743 + `..._POSTRejectsNonStringValues` :784 -> one table; the "nested rejected" case is in both (~30). `TestFindBlock_PlainRowTableIsFoundByID` :1308 + `..._ObjectRowModalStillResolves` :1327 -> one table (~20). `TestReadParams_DuplicateKeyLastWins` :688 -> a row in `..._StripsReservedTokenAndBlock` :649 (~10).
- shell_test.go "V2 fix-round" source-grep guards -> one table keeping only the load-bearing tokens (saves ~60). Keep the Go-literal check `modalStateParam == "__dtp_modal"` since it is a cross-language contract. Tests: `TestInteract_RendersAreSequenced` :676, `..._ModalStateReservedNamespace` :705, `..._VegaViewsFinalizedBeforeSwap` :737, `..._RetryAfterIsCapped` :776, `TestTabsRenderer_FinalizesViewsBeforeClearingPanel` :1269, `TestChartRenderer_GuardsDetachedMountRace` :1290. Also fold `TestInteract_ParamsAreStringOnly` :555 into `TestInteract_RerenderPostContract` :511.

DELETE:
- `TestVerifyToken_DifferentSecretsAreNotConflated` (apiclient_test.go:830, ~27). Covered by `TestVerifyToken_PositiveEntryNeverKeyedWithoutSecret` :807, which is the identical scenario.
- `TestQuery_PropagatesContextCancellation` (apiclient_test.go:1379, ~45). Sleep-based. It asserts that `net/http` honours a cancelled context, and the render-level `TestRenderDeadlineCancelsInFlightQuery` (render_test.go:600) already covers deadline-driven cancellation of the in-flight query.
- `TestCookie_NoCredentialAtAllIs401` (auth_test.go:792, ~11). Identical to the "empty" row of `TestCookie_TamperedOrExpiredOrForeignKeyIs401` :611 (JSON GET, no cookie, 401, kind unauthorized, 0 active-checks).
- `TestCookie_ReplayedOnAnotherAppEnvelopeKind` (auth_test.go:594, ~17). Add a `kind` assertion to `TestCookie_ReplayedOnAnotherAppIs401` :577 instead.
- `TestCookieAuthGET_IsNotSubjectToCSRFChecks` (auth_test.go:997, ~12). It is the same request as `TestCookie_HappyPathGivesViewerTokenPrincipal` :559 (cookie GET with no render header or Origin).
- `TestAllowRender_SixtyFirstRenderInAMinuteIsThrottled` (auth_test.go:1053, ~25). Same scenario as `TestAllowRender_PerPrincipalBurst10ThenThrottled` :1032.
- `TestAllowRender_RetryAfterIsMaxOverViolatedBucketsMinimumOne` (auth_test.go:1157, ~30). It computes a max over two buckets but only asserts `retry == 1`, and the other throttle tests already assert `retry >= 1`.
- `TestParseCookie_KeyBound` (auth_test.go:1326, ~10). Covered by the "MAC from another key" row of auth_test.go:611 and by `TestSignCookie_ShapeIsTwoBase64URLSegments` :1270.
- `TestLoadConfigStringOverrides` (config_test.go:51, ~26). Env-to-string pass-through of four fields, trivial.
- `TestLoadConfigOutputDocAndBundleMaxBytesBelowCapHonored` (config_test.go:210, ~17). Strict subset of `TestLoadConfigWithinCapHonored` :227, which uses the same values.
- `TestPerAppGateIsKeyedPerApp` (render_test.go:1216, ~43). `TestPoolGateReturnsCapacityAfterBoundedWait` :1152 already uses app-A then app-B and shows the per-app gate is not what trips.
- `TestResolveIsCalledBeforeVerify` (server_test.go:442, ~23). Subsumed by `TestResolveFailureShortCircuitsBeforeTokenVerify` :465, which asserts the call sequence is exactly `[resolve]`.
- `TestParams_DuplicateKeyLastWins_HTTP` (server_test.go:622, ~8) and `TestParams_ReservedBlockStrippedFromCtxParams_HTTP` (server_test.go:630, ~15). The file's own comment calls the `TestReadParams_*` unit tests "the authoritative proof".
- `TestAccessLog_CarriesPrincipalFields` (server_test.go:912, ~19), `TestAccessLog_ParamsHashIsPostStripping` (server_test.go:892, ~20), `TestAccessLog_ClientIPUsesResolver` (server_test.go:931, ~21). All assert log text. Keep `TestAccessLog_NoURLOrReferer` :869, which is redaction. Client-IP resolution is covered by the `resolveClientIP` table and the HTTP limiter tests.
- `TestMetrics_EndpointExposesRenderCounter` (server_test.go:1022, ~19). Subsumed by `TestMetrics_CounterMovesPerOutcome` :1041, whose "ok" row asserts the same series. Optional: cut that test's 8 rows to ok plus one error, which with `driveCapacity` saves ~80 more.
- shell_test.go, Go-port golden tests: `TestCSVGolden_OWASPEscapingAndRFC4180Quoting` (:857, ~77) and `TestCSVGolden_RowProjectionMatchesRenderedColumns` (:960, ~53), plus helpers `csvFormulaTriggers`, `csvCellGolden`, `buildCSVGolden`, `projectRowGolden`, `projectRowBuggyGolden` (~60). This is ~190 LOC. They test Go functions defined in the test file, not production code, so they are not security tests despite the topic. The real guards are `TestCSVModule_TriggerSetAndQuotingMatchGolden` :1013 and `TestTableRenderer_CSVExportProjectsThroughRenderedColumns` :1095, which stay.
- shell_test.go, cosmetic or implementation-token greps (~330 LOC total with the next entry):
  - `TestShellJS_EmptyDocAndDataUseSharedEmptyState` :1115 (35)
  - `TestErrorCard_ChartMarkdownModalReuseSharedClass` :1150 (23)
  - `TestShellSkeleton_FirstPaintBeforeMount` :1173 (20)
  - `TestShellJS_ChartRendererUsesSVGForPrintTheming` :1193 (16)
  - `TestPrintCSS_HidesChromeForcesLightPageBreak` :1220 (49) plus helper `dtpBlockClassSelector` (6)
  - `TestShellJS_HasRenderersRegistryAndUnknownPlaceholder` :181 (12)
  - `TestShellJS_RegistersAllBlockRenderers` :234 (27)
  - `TestMetricRenderer_FormatsWithIntl` :375 (17)
  - `TestTableRenderer_SortSearchNumeric` :392 (24)
  - `TestInteract_ChartOnClickBindsVegaView` :609 (21)
  - `TestShellJS_RerenderPrimitivesWiredToInteract` :649 (27)
  
  None of these assert behavior; they assert that specific strings exist. If those regressions matter, test them in a real JS runner.
- `TestShellJS_NeverUsesInnerHTML` (shell_test.go:154, ~15). `TestShellRenderers_NeverUseInnerHTML` :261 iterates `shell.js` too.

Estimated LOC after: ~6,000 (savings ~1,460). Keep ~130 of 199 funcs.

Borderline, not counted: `TestQueryRunnerEnforcesBudgetIndependently` (render_test.go:718, 24 LOC) is deliberate belt-and-braces over `TestRenderQueryBudgetExhaustedIsBadRequest` :683. Drop it if you want fewer tests.

---

## pkg/pipelineapi/http  (17 files, 5,331 LOC, 127 Test funcs)
KEEP: ~80. They pin: authz/superadmin/diff-gate 403/503 fail-closed paths, PUT status and finding contracts, secret redaction (values never listed), JWKS, static-file traversal, soft-degrade route registration, the exact 403 envelope the CLI parses, and CLI-token rate-limit and body caps.

MERGE:
- PUT-pipeline validation outcomes -> one table of (body, content-type) -> (status, finding substring) (pipeline_handlers_test.go): `TestPutPipeline_InvalidYAML` :450, `..._LegacyEnvelope_BadRequest` :332, `..._JSONContentType_RejectsNonJSONBody` :379, `..._UnknownComponent_ReturnsRegistryFinding` :476, `..._NameMismatch` :638, `..._UnknownFieldFinding` :677, `..._SemanticFindingHasPath` :730, `..._ReservedNameValidate_BadRequest` :1551. Eight tests repeat the same 12-line setup; saves ~160.
- Diff-gate: `TestPutPipeline_NonSuperadmin_AddResources_Forbidden` :1097 + `..._ResubmitSameResources_OK` :1118 + `..._ChangeCPU_Forbidden` :1136 + `..._SemanticEqualResources_OK` :1154 -> one table. Saves ~35. `..._SuperadminCheckerUnavailable_ServiceUnavailable` :1237 + `..._StoreReadError_ServiceUnavailable` :1284 -> one 503 table (~12).
- `TestValidatePipeline_NameGiven_ResourceGate_SameResources_NoFinding` :1411 + `..._Added_Finding` :1441 + `..._NoName_CreateSemantics_ResourcesFinding` :1475 -> one table. Saves ~48.
- Secret ladder on PUT: `TestPutPipeline_UnknownSecretKey_ReturnsWarning` :780 + `..._KnownSecretKey_Returns204` :826 + `..._FreshProjectForbiddenSecretRead_ReturnsWarning` :882 -> one table over (client, pre-seeded key). Saves ~45.
- Add a shared `seedProject(...)` helper used by ~30 tests (saves ~120 conservatively). This is not a test merge.
- `TestResourcesModified_*` (resource_diff_test.go:67, :75, :83, :91, :99, :107, :120) -> one table. Saves ~30. Keep the cross-stage no-bypass row.
- `TestTriggerRun_UnknownSecretKey_Returns400` + `..._FreshProjectForbiddenSecretRead_Returns400` (run_handlers_test.go:441, :500) -> one table (~21). Trim the exported-label portion of `TestGetRun_AssemblesTimeline` :370 (~30). `buildTimeline` is already unit-tested in `run_timeline_test.go`.
- `TestPutSecret_AsViewer_Forbidden` + `TestDeleteSecret_AsViewer_Forbidden` + `TestListSecrets_AsViewer_Works` -> one authz-matrix table (secret_handlers_test.go:271, :367, :245). Saves ~32.
- Soft-degrade "route not wired": `TestServer_StorageRoute_SoftDegrade` :84, `TestServer_QueryRoute_NotConfigured` :155, `TestServer_AppRoutes_NotConfigured` :291 (server_test.go) + `TestAppQueryRoute_NotConfigured` (app_query_route_test.go:80) -> one table. Saves ~45 combined.
- `TestMustBeSuperadmin_*` (6 tests) -> one table (superadmin_test.go:50, :63, :75, :89, :101, :113, saves ~31). `TestHandleMe_IsSuperadmin_*` (4 tests) -> one table (superadmin_test.go:141, :149, :157, :170, saves ~14).
- `TestUIStatic_ServesIndexAtRoot` :31 + `..._ServesRealFile` :51 + `..._SPAFallbackForDeepLinks` :77 -> one table of GETs (static_handler_test.go, ~28). Keep the traversal and sibling-prefix tests untouched.
- `TestHandleListComponents_IODefaultsOptional` :193 + `..._IOExplicit` :221 + `TestHandleGetComponent_IO` :250 -> one table (component_handlers_test.go, ~38).
- `TestLogin_WrongPassword` :150 + `TestLogin_UnknownEmail_UniformError` :169 (auth_handlers_test.go) -> one table (~13).
- `TestHandleCLIToken_WrongPassword` :209 + `..._UnknownEmail` :224 + `..._DisabledUser` :238 -> one uniform-401 table (cli_token_handler_test.go, ~24).

DELETE:
- `TestListPipelines` (pipeline_handlers_test.go:512, ~33). Only asserts status 200. Covered by `TestPutPipeline_JSON_GetReturnsDocObject_ListHasDescription` :206 and `TestListPipelines_EmptyDescriptionKeyPresent` :545.
- `TestPutPipeline_HardValidationError_StaysBadRequestWithSecretsWired` (pipeline_handlers_test.go:853, ~29). Same input as `TestPutPipeline_InvalidYAML` :450 on a different server wiring. Make it a row (or drop it); the "secrets wired" distinction is not a separate behavior.
- `TestListAndGetRun` (run_handlers_test.go:214, ~44). Covered by `TestTriggerRun_HappyPath` :147, `TestListRuns_EnvelopeAndFilters` :285, and `TestGetRun_AssemblesTimeline` :370.
- `TestPutSecret_CreatesManagedSecretAndAnnotation` (secret_handlers_test.go:118, ~34). A strict subset of `TestPutSecret_SecondKeyPreservesFirst` :152. Move its exact-timestamp assertion there.
- `TestListProjects_EmptyMemberships` (project_handlers_test.go:34, ~23). Trivial empty-list; `TestListProjects_OnlyUserMemberships` :57 covers the membership filter.
- `TestAppQueryRoute_CancelledContextAbortsBlockingQuery` (app_query_route_test.go:169, ~87). Judgement call: timing-based; checks that Go HTTP propagates a cancelled context through the proxy. No other test in `queryproxy` covers cancellation, so veto this one if you want the end-to-end proof. Keep `TestAppQueryRoute_PrincipalReachesAudit` :96 as the wiring test.
- Trim only, not whole tests: the JWT re-parse and claim block in `TestHandleCLIToken_Success` (:147, ~25 LOC) and in `TestQueryTokenHandler_PolicyOn_MintsQueryToken` (:62, ~35 LOC) duplicates the `MintLocalCLIToken_ClaimsShape` / `MintQueryToken_ClaimsShape` tests. Keep the exact 403 envelope assertion in `TestQueryTokenHandler_PolicyOff_Forbidden` :32 because the client depends on the wording.

Estimated LOC after: ~4,300 (savings ~1,030). Keep ~80 of 127 funcs.

---

## pkg/pipelineapi/apps  (6 files, 3,863 LOC, 96 Test funcs)
KEEP: ~74. They pin: the service-credential 401 gate and header-forwarding rules, FGA viewer-tuple/JWT-sub agreement (the load-bearing both-halves test), tuple-before-rows delete ordering, viewer-token verify/active semantics and secret non-leakage, CAS and quota concurrency, and the author-route 401/403 matrix.

MERGE:
- `TestIdentityManager_RegisterEmitsAudit` :256 + `..._UnregisterEmitsAudit` :336 -> fold into the register/unregister happy-path tests (identity_manager_test.go). Saves ~30. `TestAudit_AppLifecycleEventsFireFromHandlers` (audit_test.go:137, ~25) is a rename smoke test; fold into `TestPutApp_...` or delete.
- Register/Unregister idempotent + backend-error pairs: `..._RegisterIsIdempotent` :282, `..._RegisterPropagatesBackendError` :290, `..._UnregisterToleratesMissingTuple` :360, `..._UnregisterPropagatesBackendError` :368 -> one table (op, backend error -> want nil/err). Saves ~16.
- `TestIdentityManager_RejectsEmptyArguments` :646 + `..._MintRequiresSigner` :668 + `..._MintFailsClosedOnUnknownProject` :613 -> one fail-closed table. Saves ~18.
- Trim `TestIdentityManager_MintSubjectIsAppJWTSubject` :382 to `sub` and `project_id` only. The aud/token_kind/60s TTL assertions duplicate `tokens.TestMintAppToken_ClaimsShape`. Saves ~15.
- `TestInternalResolve_HappyPath` :294 + `..._DefaultsToProduction` :332 -> one table (internal_test.go, ~15).
- The four `..._BadRequest` tests -> one table keyed by route: `TestInternalResolve_BadRequest` :361, `TestInternalViewerTokenVerify_BadRequest` :470, `TestInternalTokenActive_BadRequest` :545, `TestInternalSessionsVerify_BadRequest` :817. Saves ~27. Fold `TestInternalLogs_AcceptsMatchingAppIDInBody` :1012 into `..._AppendsRecord` (~6).
- `TestInternalSessionsVerify_NonMember` :595 + `..._NoSessionIsNotA401` :615 + `..._UnknownProject` :827 -> one membership-answer table. Saves ~24.
- `TestInternalSessionsVerify_StripsForwardingHeader` :747 -> a second assertion inside `..._ForwardsBearerCredential` :695. Saves ~28. Keep `..._ServiceTokenNeverReachesResolver` :727 and `..._DoesNotMutateRequestHeaders` :793 (security).
- `TestPromote_ConcurrentCAS_ExactlyOneWinner` :774 + `TestPromote_ConcurrentFirstPromote_ExactlyOneWinner` :855 -> one function parameterised by expected-production (`baseline.Hash` vs `""`), ~90% identical (store_test.go). Saves ~60.

DELETE:
- `TestPutApp_IdempotentSameBundle` (handlers_test.go:239, ~10). Covered by `TestPutVersion_IdempotentSameBytes` (store_test.go:195) and `..._ConcurrentIdempotent_SameBytes` :996.
- `TestIdentityManager_MintIsFreshPerCall` (identity_manager_test.go:455, ~28). Subsumed by `TestIdentityManager_ConcurrentMintsAreDistinct` :483 (distinct tokens and jtis).
- `TestAppFGASubject_NoDoublePrefix` (identity_test.go:36, ~9). Same assertion as `TestAppFGASubject` :22, plus the double-prefix guard in `TestIdentityManager_RegisterWritesViewerTuple` :225.
- `TestTokenSummary_HasNoSecretOrSaltField` (store_test.go:539, ~10). Struct-shape check; `TestListTokens_ReturnsSummariesNotSecrets` (handlers_test.go:735) already asserts the response body carries no secret or salt.

Estimated LOC after: ~3,530 (savings ~335). Keep ~74 of 96 funcs.

---

## pkg/pipelineapi/storage  (9 files, 2,337 LOC, 61 Test funcs)
KEEP: ~39. They pin: delete-table requires `data_admin`, identifier validation, symlink/containment checks, the walker's metadata resolution, the total-failure vs empty-catalog distinction, the 8-wide parallel-load limit (optional), and the `kind` parity in gate-error bodies.

MERGE:
- `TestJoinNS` :23 + `TestShortName` :44 (catalog_proxy_test.go) -> one tiny table (~15). These are pure helpers with edge cases; do not delete both.
- `TestNewCatalogProxy_NilService` :65 + `..._EmptyURL` :75 + `..._NilMinter` :90 -> one error table (~25).
- `TestNewCatalogProxy_OK` :105 + `..._ProjectIDHeader` :141 -> one test asserting both the bearer and `x-project-id` headers (~40).
- `TestDeleteTable_MembershipAloneIsForbidden` :63, `..._NonMemberIsForbidden` :74, `..._Unauthenticated` :84, `..._WithoutLakekeeperIsNotImplemented` :98, `..._InvalidProjectID` :170 -> one status table (delete_table_test.go, ~25). Keep the "membership alone is forbidden" row.
- `TestS3Props_Keys` :77 + `..._EndpointScheme` :96 + `..._RegionDefault` :108 + `..._RegionExplicit` :115 + `..._EmptyEndpointOmitted` :122 + `..._NonEmptyEndpointKept` :129 (fsys_test.go) -> one table. Saves ~35. `NonEmptyEndpointKept` is a subset of `Keys`.
- `TestLoadFS_Local_ReadFile` :14 + `..._Local_PlainPath` :42 -> one table (~25). Fold `TestLoadFSGS` :155 into the scheme table with `TestLoadFS_Scheme_Unsupported` :70 (~8). `TestLoadFSGS` only asserts an error message does not contain "unsupported URI scheme", which is weak.
- `TestListTables_NoMembership` :196 + `..._NoSession` :220 + `..._InvalidProjectID` :234 + `..._WarehouseResolutionFails` :259 -> one gate-error table (handlers_test.go, ~40). Keep the `kind` assertions.
- `TestTableSchema_InvalidIdentifier` :329 + `TestPreview_InvalidIdentifier` :489 -> one table over the {schema, preview, info} routes (~20).
- `TestRejectSymlinks` :45 + `..._MissingLeafViaSymlinkParent` :73 (paths_test.go) -> one test (~15).
- `TestPreview_ResultTooLarge` :459: assert `kind`, not the "too wide" wording. No LOC change.

DELETE:
- `TestAddedRecordsZeroIsDistinctFromMissing` (snapshots_test.go:336, ~45). Hits the same endpoint and fixture as `TestSnapshots_WithAuditKeys` :273, whose `older.AddedRecords != nil && == 0` already checks a real zero is distinct from missing.
- `TestWithLakekeeper` (service_test.go:46, ~41). Asserts that a setter assigned two function fields, then calls the closures it just assigned.

Optional (not counted): `TestListAllTables_BoundedParallelLoad` (catalog_proxy_test.go:388, 67 LOC) pins an `errgroup.SetLimit(8)` implementation detail.

Estimated LOC after: ~2,000 (savings ~340). Keep ~39 of 61 funcs.

---

## pkg/pipelineapi/queryproxy  (7 files, 1,898 LOC, 54 Test funcs)
KEEP: ~31. They pin: app-token-only auth on the app route, no re-mint, per-principal gate, status mapping of worker outcomes, params verbatim with `UseNumber`, clamping table, redirect refusal and response cap, audit-per-outcome and jti-matches-minted-token, `QuoteIdent`.

MERGE:
- `TestExtractJTIFromToken_Valid` :153 + `..._Missing` :167 + `..._MalformedToken` :175 -> one table (audit_test.go, ~17).
- `TestStatementHash_NonEmpty` :185 + `..._Empty` :198 + `..._Deterministic` :204 -> one table (~19).
- `TestAudit_RateLimited_EmptyJTI` (audit_test.go:404) <-> `TestHandler_PerPrincipal429WhenCapExhausted` (handler_test.go:318): same concurrency setup. Assert the audit line inside the handler test and drop the duplicate harness (~55).
- `TestNewWorkerClient_RequiresURL` (client_test.go:24) -> row of `TestHandler_RequiresWorkerURL` :75, `..._RequiresSigner` :84, `..._RequiresGate` :498 -> one table (~8 + 6).
- `TestWorkerClient_RetriesOnceOn429` :186 + `..._NoSecondRetry` :219 -> one table over hits (~20).
- `TestGate_AcquireUpToCap` :5, `..._ReleaseFreesSlot` :18, `..._IndependentPrincipals` :32, `..._ReleaseDeletesEntryAtZero` :46, `..._NonPositiveCapCoercedToOne` :61, `..._ReleaseUnknownPrincipalIsNoop` :71 (gate_test.go) -> one test. Saves ~30.
- `TestHandler_NoParams_Regression` :211 -> a second case in `TestHandler_ParamsForwardedVerbatim` :150 (~15).
- `TestHandler_Unauthenticated` :302 + `..._GateForbidden` :469 + `..._GateInvalidPID` :484 -> one security table (~9; the unauthenticated case also removes the overlap with `TestAudit_Unauthenticated_NoLine` :505).

DELETE:
- `TestAppQuery_ValidAppTokenPassesGate` (app_query_test.go:59, ~21). Strict subset of `TestAppQuery_ForwardsPresentedTokenAsCatalogCredential` :80 (200 plus the JWT equality).
- `TestAppQuery_AuditRecordsAppSubjectAndJTI` (app_query_test.go:108, ~39) + `findAuditLine` helper (~17). Same scenario end-to-end in `http.TestAppQueryRoute_PrincipalReachesAudit` (app_query_route_test.go:96).
- `TestAudit_StatementHashMatchesSHA256` (audit_test.go:573, ~25). Tautological: it compares the audit field to `statementHash(sql)`. Length and hex shape are already asserted for every outcome in `TestAudit_ExactlyOneLinePerRequest` :219.
- `TestNewWorkerClient_TimeoutAboveWorkerMax` (client_test.go:30, ~12). Asserts one constructor arithmetic constant (330s).
- `TestHandler_TransportErrorTo502` (handler_test.go:427, ~13). Same scenario with a stricter assertion in `TestAudit_TransportError_InternalOutcome` :471, which also checks 502.
- `TestHandler_NeverLogsTokens` (handler_test.go:440, ~29). Borderline: the rule keeps redaction tests, but this one is weaker and duplicated. `TestAudit_ExactlyOneLinePerRequest` :219 asserts no `eyJ` and no raw SQL for every outcome including the worker-401 path. Veto if you prefer a belt-and-braces redaction test.

Estimated LOC after: ~1,550 (savings ~350). Keep ~31 of 54 funcs.

---

## pkg/pipelineapi/k8s  (10 files, 1,373 LOC, 43 Test funcs)
KEEP: ~26. They pin: the reaper's owner-reference and non-project-namespace guards, the secrets-RBAC least-privilege verbs, run-token Secret ownership, FGA tuple reaping for terminal and orphan runs, the observer's rv/label/deletion guards, and the "do not cache when inner not applied" rule.

MERGE:
- Coalesce (coalesce_test.go): `DropsIdenticalSecondWrite` :48 + `IgnoresResourceVersionForEquality` :88 -> one. `WritesOnPhaseChange` :60 + `WritesOnStageChange` :71 + `SnapshotOnlyChangeIsNotCoalesced` :155 -> one table. `JanitorDropsOldEntries` :130 + `JanitorKeepsFreshEntries` :178 -> one table. Saves ~75.
- Observer skip cases: `SkipsEmptyPhase` :64, `SkipsDeletionTimestamp` :82, `SkipsMissingLabel` :105, `NoObjectNoCall` :122, `SkipsNonNumericResourceVersion` :159 -> one table (observer_test.go, ~50).
- `ApplyPipelineCRD_RejectsNilDoc` :78 + `..._RejectsUnnamedDoc` :85 -> one (~8).
- `ReapOnce_IgnoresPipelineRunsWithoutStartTime` :71 + `..._DeletesTerminalRunsEvenWithoutStartTime` :94 + `..._PrefersCompletionTimeForTerminalRuns` :188 -> one table over (phase, start, completion, creation) -> reaped (reaper_test.go, ~30).
- `ReapRunTuples_OrphanRowGetsCleanedUp` :159 + `..._FreshOrphanRowIsLeftAlone` :205 -> one table over age (reap_run_tuples_test.go, ~30).

DELETE:
- `TestScheme_IncludesDatupletCRDs` (client_test.go:17, ~10). Every fake-client test already uses `pkg8s.Scheme()` and would fail if the CRD were not registered.
- `TestEnsureProjectLimitRange_Idempotent` (limitrange_test.go:55, ~13). Strictly weaker than `TestEnsureProjectLimitRange_LeavesExistingUntouched` :68 (a second call on an existing object, plus an overwrite check).
- `TestEnsureProjectNamespace_Idempotent` (namespace_test.go:36, ~13). `TestEnsureProjectNamespace_SecretsRBACIdempotent` :123 already calls Ensure twice and asserts the object counts.
- `TestReapRunTuples_IsIdempotent` (reap_run_tuples_test.go:244, ~44). Runs three sweeps but asserts only what `TestReapRunTuples_TerminalRunDeletesTuplesAndRow` :61 already asserts (the code comment concedes this).

Estimated LOC after: ~1,100 (savings ~270). Keep ~26 of 43 funcs.

---

## pkg/pipelineapi/tokens  (6 files, 1,276 LOC, 45 Test funcs)
KEEP: ~21. They pin: claim shapes per token kind (sub is the raw UUID, aud/token_kind pairing), TTL clamps, anonymous/zero-lifetime/nil-signer rejection, redaction in fmt/JSON for all token types, signing-key bootstrap rules, and JWKS fidelity.

MERGE:
- One table over all minters for anonymous / lifetime<=0 / nil signer: `RejectsAnonymous` at mint_test.go:107, :169, :315, :406; `TestMintLocalCLIToken_RejectsAnonymous` :81, `..._RequiresPositiveLifetime` :91, `..._RequiresSigner` :105 (cli_test.go); `TestMintCLIAPIToken_RejectsAnonymous` :76, `..._RequiresPositiveLifetime` :85, `..._RequiresSigner` :97 (cli_api_test.go); `TestMintRunToken_RequiresFields` :117. Saves ~60.
- `TestMintQueryToken_ClaimsShape` :195 + `TestMintInternalQueryToken_ClaimsShape` :322 -> one table (differ only in aud and token_kind), and fold the TTL clamps `TestMintQueryToken_TTLClamp` :281 + `TestMintInternalQueryToken_TTLClamp` :375 into it. Saves ~65.
- `TestMintLocalCLIToken_ClaimsShape` (cli_test.go:18) + `TestMintCLIAPIToken_ClaimsShape` (cli_api_test.go:17) -> one table (jti prefix, exp window, expTime==exp, kind, aud). Saves ~50.
- `TestImpersonationToken_RedactsInFmt` :179 + `TestQueryToken_RedactsInFmt` :415 + `TestImpersonationToken_RedactsInJSON` :438 -> one table over token types (mint_test.go, ~22).
- `TestJWKSFromPublicKey_Shape` :14 + `..._Roundtrip` :41 -> one (jwks_test.go, ~28); `..._NilKey` :82 + `..._NilN` :89 -> one (~5).
- `TestLoadPrivateKeyFromPEMFile_MissingFile` :49 + `..._EmptyKeyID` :55 + `..._BadPEM` :62 + `..._NonRSA` :139 -> one error table (keys_test.go, ~16).

DELETE:
- `TestMintAppToken_FreshPerCall` (mint_test.go:569, ~28). Subsumed by `TestMintAppToken_ConcurrentJTIsAreUnique` :597 (distinct jtis and bytes).
- `TestMintAppToken_RedactsInFmt` (mint_test.go:655, ~10). `MintAppToken` returns `ImpersonationToken`, already covered by `TestImpersonationToken_RedactsInFmt` :179. Same type and method; it is redundant, not a redaction coverage loss.
- `TestLocalCLITokenAcceptedForCatalogAudience` (cli_test.go:122, ~36). Re-asserts `aud` and `token_kind` already pinned in `TestMintLocalCLIToken_ClaimsShape` :18.
- `TestMintCLIAPIToken_AudienceDistinctFromCatalog` (cli_api_test.go:106, ~24). `ClaimsShape` already asserts `aud == APITokenAudience`. Add one `APITokenAudience != TableTokenAudience` line there if you want the replay guard stated.

Estimated LOC after: ~940 (savings ~335). Keep ~21 of 45 funcs.

---

## pkg/pipelineapi/auth  (5 files, 854 LOC, 35 Test funcs)
KEEP: ~18. All token-validation security is retained: wrong alg/kid/iss/aud/kind/expiry/no-exp/sub, user not found/disabled, infra error propagation, ChainResolver priority, and Argon2 hash/verify.

MERGE:
- The eight bearer-JWT rejection tests -> one table driven by a `signCustom(claims-mut)` helper (bearer_resolver_test.go): `WrongAlg` :153, `WrongKid` :178, `WrongIss` :209, `WrongAud` :237, `WrongTokenKind` :265, `Expired` :293, `SubNotUUID` :322, `NoExp` :518. Each is ~28 lines of copy-paste claim construction. Saves ~125, no coverage lost.
- `TestBearerJWTResolver_UserNotFound` :349 + `..._UserDisabled` :365 -> one (~12). `..._ValidToken` :91 + `..._LowerCaseBearer` :128 -> one (~18). `TestChainResolver_PrimaryDelegation` :478 + `..._NoPrimaryFallsBackToFirst` :502 -> one (~12).
- password_test.go (not read; inferred from names): `TestVerify_RejectsMalformedHash` :48, `..._RejectsZeroTime` :62, `..._RejectsZeroThreads` :73, `..._RejectsEmptyHashSegment` :81 -> one reject table. Estimated ~20.
- ratelimit_test.go: `TestLimiter_AllowsUpToRate` :12, `..._DistinctKeysIndependent` :47, `..._EmptyKeyIsTreatedNormally` :62, `..._ZeroRateAlwaysDenies` :76 -> one table (~20). Keep `..._ResetsAfterWindow` :28.
- `TestLookupSession_Expired` :68 + `..._Missing` :86 -> one (~10).

DELETE: none. Everything here is validation or security.

Estimated LOC after: ~640 (savings ~215). Keep ~18 of 35 funcs.

---

## pkg/pipelineapi/authz  (3 non-integration files read, 445 LOC, 16 Test funcs)
KEEP: ~13. They pin: the unavailable-vs-denied split (`ErrAuthzUnavailable` on transport and 5xx), superadmin discovery memoization, Bearer header attachment, idempotent-tuple error detection, and `UserObject` oidc~ idempotence.

MERGE:
- `TestIsServerAdmin_Allowed` :64 + `..._Denied` :80 -> one table (server_admin_test.go, ~11).
- `TestObjectType_Constants` (types_test.go:108) -> a row set in the typed-constructor table (~10).

DELETE:
- `TestTypedConstructors_StringForm` (types_test.go:8, ~22). Duplicates the `NamespaceObject` cases (simple, `__workspace.abc123`) in `TestNamespaceObject` :30; the `ProjectObject` row is covered by `TestObject_TypeNotUserControllable` :94.

Estimated LOC after: ~400 (savings ~43). Keep ~13 of 16 funcs.

---

## pkg/pipelineapi/store  (6 files, 782 LOC, 31 Test funcs)
KEEP: ~29. They pin: stale-rv drop, cancel bypasses rv without lowering it, bigint-safe observed_rv boundary, terminal-phase guard against resurrection, keyset pagination, and typed not-found/already-exists errors. The package is already lean.

MERGE:
- `TestToRunView_PopulatesPointerFieldsFromNonZeroValues` :12 + `..._NilTriggeredByBecomesNilPointer` :48 (runview_test.go) -> one table. The populated case is a trivial field-copy; keep the uuid.Nil -> nil behavior row (~15).

DELETE:
- `TestUpdateRunPhase_FirstWriteAppliesAndReturnsApplied` (run_test.go:104, ~26). `TestUpdateRunPhase` :77 asserts `applied == true` plus the persisted fields on the first write, and `..._LargeObservedRV` :211 covers the rv-gated first write.

Weak but kept (the only test of a public function): `TestListRunsForProject_OrderedDesc` (run_test.go:51) only counts rows despite its name.

Estimated LOC after: ~740 (savings ~41). Keep ~29 of 31 funcs.

---

## Totals

| | Current | After (est.) |
|---|---|---|
| Test LOC (listed packages) | ~25,620 | ~21,190 |
| Test funcs | 707 | ~461 |

That is ~4,430 LOC and ~35% of test funcs. About 60% of the LOC saving comes from `appworker/shell_test.go` and the table-merges in `http` and `auth`.

## Top 5 highest-value deletions / consolidations

1. **`appworker/shell_test.go`: the two CSV "golden" tests and their Go-port helpers** (`TestCSVGolden_OWASPEscapingAndRFC4180Quoting` :857, `TestCSVGolden_RowProjectionMatchesRenderedColumns` :960, plus `csvCellGolden`/`buildCSVGolden`/`projectRowGolden`/`projectRowBuggyGolden`, ~190 LOC). They test Go copies of JS logic that live in the test file, so they cannot catch a production regression. The source-level pins `TestCSVModule_TriggerSetAndQuotingMatchGolden` :1013 and `TestTableRenderer_CSVExportProjectsThroughRenderedColumns` :1095 stay.
2. **`appworker/shell_test.go`: ~11 cosmetic and implementation-token greps** (EmptyState :1115, ErrorCard :1150, Skeleton :1173, SVG print :1193, PrintCSS :1220, HasRenderers :181, RegistersAllBlockRenderers :234, MetricRenderer :375, TableRenderer :392, ChartOnClick :609, RerenderPrimitivesWired :649, ~330 LOC). They assert that strings exist in JS/CSS, not behavior.
3. **`http/pipeline_handlers_test.go`: collapse the 8 PUT-validation tests into one table, and delete `TestListPipelines` :512 and `TestPutPipeline_HardValidationError_StaysBadRequestWithSecretsWired` :853** (~190 LOC). Identical setup, inputs and status assertions repeated eight times.
4. **`auth/bearer_resolver_test.go`: the eight near-identical JWT rejection tests -> one table** (~125 LOC). No coverage lost; every token-validation case stays as a row.
5. **`http/app_query_route_test.go` `TestAppQueryRoute_CancelledContextAbortsBlockingQuery` :169 (~87) plus `appworker/apiclient_test.go` `TestQuery_PropagatesContextCancellation` :1379 (~45).** Both are timing-based tests of Go's `net/http` context propagation. Veto the first if you want the end-to-end cancellation proof.


---

# Part B — cmd/*, pkg/datagateway/*, controllers, pipeline/*, catalogwriter, icebergjob, sdk/go*, components/*

Read-only audit done. Nothing was edited. Line numbers are start-of-func and "~LOC" is approximate. Estimated "after" LOC are rough (+/- 10%). They assume merged tests become table-driven and keep every case that pins distinct behavior.

Scope notes:
- `components/sql-transform` has NO `*_test.go` files at all (0 LOC). It is a coverage gap, not over-testing.
- `components/queryengine` tests tagged `duckdb_arrow` (not `integration`) are included. Only the three `integration`-tagged files are excluded.
- `pkg/datagateway/{runtoken,processor,manifest,lakekeeper,jwks}` were not in your list and were not audited.

## cmd/datuplet  (9 files, 4806 LOC, 143 Test funcs)
KEEP: ~95. The keepers pin: the exit-code contract (render 1 vs 20, promote CAS conflict code, validate exit 0/1/>=2), `--param` secret non-leak (apps_test.go:1308, 1411), the token-printed-once and token-list-projects-safe-type contracts, `confirmTableDeletion` and the no-network-before-confirm guard, the `loadRemoteArgs` precedence tables, the token-never-in-error check, the `login` file modes and raw JWT, and the HTTP request shapes.
MERGE:
- apps_test.go: `TestRunAppsGet_InvalidNameRejectedLocally` (792) + `TestRunAppsDelete_InvalidNameRejectedLocally` (801) + `TestRunAppsPut_InvalidNameRejectedLocally` (810) + `TestRunAppsRender_InvalidNameRejectedLocally` (1156) + `TestRunAppsPromote_InvalidNameRejectedLocally` (1664) + `TestRunAppsTokenCreate_InvalidNameRejectedLocally` (1832) -> one table over {command} x {".."}. `TestValidateAppName` (74) already pins the 6-name set, so this is 36 subtests down to 6.
- apps_test.go usage/arity tests that only assert `err != nil`: `TestRunAppsInit_RequiresExactlyOneArg` (297) + `TestRunAppsPut_MissingBundleFlagErrors` (571) + `TestRunAppsList_RejectsPositionalArgs` (689) + `TestRunAppsDelete_RequiresExactlyOneName` (736) + `TestRunAppsPromote_MissingVersionErrorsLocally` (1617) + `TestRunAppsTokenDelete_RequiresTwoPositionalArgs` (2012) + `TestRunAppsToken_UnknownSubcommandErrors` (2039) -> one table.
- apps_test.go HTTP-status-to-error tests (all `Contains("not found")` wording checks on 404): `TestRunAppsGet_NotFound` (628) + `TestRunAppsDelete_NotFound` (718) + `TestRunAppsLogs_ByRequestIDNotFoundExits1` (1278) + `TestRunAppsPromote_NotFoundExits1` (1673) + `TestRunAppsPromote_UnknownVersionExits1` (1698) + `TestRunAppsTokenCreate_NotFoundExits1` (1814) + `TestRunAppsTokenDelete_NotFoundExits1` (2021) + `TestRunAppsPromoteAndToken_AuthDenialsSurfaceAsErrors` (2054) -> one {command, status} -> exit-code table. Assert the exit code, not the wording.
- apps_test.go: `TestRunAppsInit_CreatesMissingParentDirs` (306) into `TestRunAppsInit_WritesScaffold...` (191), by using a nested dir there.
- apps_test.go: `TestRunAppsPut_TextModePrintsAppIDAndVersionHash` (517) into `TestRunAppsPut_UploadsBase64AndPrintsResultJSON` (451), as text/json modes.
- apps_test.go: `TestRunAppsRender_ProductionChannelHasNoDraftSuffix` (967) into `TestRunAppsRender_DraftJSONSendsBearerAcceptAndPrintsDoc` (920), as a channel table.
- apps_test.go: `TestRunAppsRender_ErrorEnvelopeAuthorLogNullWhenLogs404` (1050) + `TestRunAppsRender_AuthorLog404StaysSilent` (1371) + `TestRunAppsRender_AuthorLogFetch500EmitsStderrNoteButNullObject` (1332) -> one {logs status 404/500} table. Both 404 tests assert the same null author_log and exit 1.
- apps_test.go: `TestRunAppsRender_InvalidChannelRejectedLocally` (1168) + `TestRunAppsRender_InvalidParamRejectedLocally` (1183).
- apps_test.go: `TestRunAppsPromote_BadHashRejectedLocally` (1633) + `TestRunAppsPromote_BadExpectedProductionRejectedLocally` (1650).
- apps_test.go: `TestRunAppsPromote_HappyPathOmitsExpectedProductionOnFirstPromote` (1521) + `TestRunAppsPromote_SendsExpectedProductionWhenProvided` (1558) + `TestRunAppsPromote_JSONPassesThroughServerBody` (1716) -> one table (with/without expected, text/json).
- apps_test.go: `TestRunAppsTokenList_ShowsIDsNotSecrets` (1844) + `TestRunAppsTokenList_JSONProjectsThroughSafeSummaryType` (1927). Use the leaky fixture and keep the method/path asserts.
- apps_test.go: inside `TestParseAppsFlags` (96), collapse the eight copy-pasted "missing value" asserts into a loop.
- remote_test.go: `TestLoadRemoteArgs_MissingTokenFile` (139) + `TestLoadRemoteArgs_MissingClusterJSON` (161). Both only check the "datuplet login --remote" wording.
- remote_test.go: `TestLoadRemoteArgs_ExpiredToken` (69) + `TestLoadRemoteArgs_TokenNotInError` (181) + `TestLoadRemoteArgs_MalformedExpiresAt` (595). All use the same bad-cluster fixture; keep the "token not in error" assert as a column.
- remote_test.go: `TestLoadRemoteArgs_HeadlessNoDatupletFileRead` (416) + `TestLoadRemoteArgs_ProjectEnvFastPath` (494) + `TestLoadRemoteArgs_ProjectFlagWinsOverEnv` (517) + `TestLoadRemoteArgs_ProjectEmptyFastPath` (538). These are the same headless setup with one input varied.
- remote_test.go: `TestLoadRemoteArgs_RemoteUrlNormalization` (235) into `TestLoadRemoteArgs_RemoteUrlMismatchErrors` (207).
- components_test.go: `TestRunComponentsListJSONPassthrough` (279) + `TestRunComponentsGetJSONPassthrough` (350).
- components_test.go: `TestRunComponentsGetSchemaOutputVerbatim` (303) + `TestRunComponentsGetSchemaWithExplicitVersion` (321) + `TestRunComponentsGetSchemaUnknownVersionErrors` (339). The last one duplicates `TestResolveVersion` "explicit miss" (17).
- components_test.go: `TestRunComponentsGetJSONAndSchemaMutuallyExclusive` (385) + `TestRunComponentsListRejectsSchema` (392) into `TestParseComponentsFlags` (399).
- pipeline_test.go: `TestRunPipelineGetDefaultHitsFormatYAML` (116) + `TestRunPipelineGetJSONHitsPlainEndpoint` (148).
- pipeline_test.go: `TestRunPipelinePutJSONBodySendsJSONContentType` (197) + `TestRunPipelinePutYAMLBodySendsYAMLContentType` (234). `TestSniffContentType` (40) already pins the sniffing.
- pipeline_test.go: `TestRunPipelinePutNameMismatchErrorsLocally` (273) + `TestRunPipelinePutNoNameErrors` (291).
- pipeline_test.go: `TestRunPipelineListRendersDescriptionColumn` (310) + `TestRunPipelineListOmitsDescriptionColumnWhenAbsent` (343).
- pipeline_test.go: `TestRunPipelineValidateCleanExitsZero` (418) + `TestRunPipelineValidateWarningsOnlyExitsZero` (463) + `TestRunPipelineValidateErrorsExitsOne` (496). Make it a table with the exit code as a column, so the contract is kept.
- login_test.go: `TestLogin_BadCredentialsExitsNonZero` (178) + `TestLogin_NetworkErrorExitsNonZero` (197).
- trigger_test.go: `TestIsTerminalPhase` (16) + `TestExitCodeForPhase` (35). Keep every row, since this is the exit-code contract.
DELETE:
- `TestRunAppsGet_ValidNameNotRejectedByNameCheck` (apps_test.go:823, ~9 LOC): every happy-path test in the file already proves a valid name passes.
- `TestRunAppsRender_TransportFailureExits20` (apps_test.go:1088, ~7): `TestRunAppsRender_TransportErrorDoesNotLeakParamValues` (1308) runs the same refused-connection setup and also asserts exit 20.
- `TestRunAppsRender_NonEnvelopeErrorExits20` (apps_test.go:1099, ~16): `TestRunAppsRender_NonEnvelopeBodyIsNotEchoed` (1411) runs the same 502 HTML scenario and also asserts exit 20.
- `TestRunAppsTokenList_Empty` (apps_test.go:1896, ~21): asserts only the "no viewer tokens" message wording.
- `TestRunAppsTokenDelete_MalformedTokenIDRejectedLocally` (apps_test.go:1989, ~22): `TestValidateTokenID` (1494) pins the validator. Keep one row in the wiring table.
- `TestFetchComponentsListDecodesSummaryShape` (components_test.go:172, ~22) and `TestFetchComponentDetailDecodesConfigSchema` (components_test.go:195, ~16): the `runComponents*` tests (260, 279, 303, 350) already decode through these functions.
- `TestFetchComponentDetailNotFound` (components_test.go:211, ~14): duplicate of `TestRunComponentsGetNotFound` (371).
- `TestLogin_TokenFileIsRawJWTNotJSON` (login_test.go:134, ~43): `TestLogin_PromptsAndWritesFiles` (66) already asserts the token file equals `fakeJWT` byte-for-byte, plus mode 0600.
- `TestRunsGet_Headless` (runs_test.go:172, ~15): the path assert duplicates `TestFetchRunDetail_DecodesTimeline` (112).
- `TestFetchRunDetail_NotFoundReturnsStatus` (runs_test.go:135, ~15): covered by `TestRunsGet_NotFoundFriendlyError` (187).
- `TestComputeWallclockSeconds` (trigger_test.go:46, ~12): trivial subtraction.
- `TestLocalQueryMessage` (query_test.go:114, ~10): asserts only that the error text contains "datuplet-query".
Note: `resolveQuerySQL` (cmd/datuplet/query_test.go:16) and `resolveSQL` (components/queryengine/cmd/datuplet-query/flags_test.go:33) have near-identical precedence tests in two modules. Keep the one in whichever module owns the logic.
Estimated LOC after: ~3260

## cmd/pipeline-api  (6 files, 1284 LOC, 45 Test funcs)
KEEP: ~25. The keepers pin: the Lakekeeper warehouse wire shape (`TestBuildWarehouseBody_S3` 13, `_GCS` 73), the redaction tests (151, 604), the service-identity project_admin grant idempotency (392), the project-scoped warehouse lookup (524), the `apps_wiring` gates (404 vs 403 vs 401), and the superadmin tuple writes.
MERGE:
- admin_lakekeeper_test.go: `TestBuildWarehouseBody_S3_MissingSpec` (183) + `TestBuildWarehouseBody_GCS_MissingSpec` (194) + `TestBuildWarehouseBody_UnknownType` (173).
- admin_lakekeeper_test.go: `TestBuildWarehouseBodySystemIdentity` (232) into `TestBuildWarehouseBody_GCS` (73), as a table over credential type.
- admin_lakekeeper_test.go: `TestGCSSpecValidateSystemIdentityDefault` (282) + `...SystemIdentityExplicit` (290) + `...ServiceAccountKey` (303) + `...UnknownType` (315) + `...SystemIdentityRequiresSTS` (328) + `...SystemIdentityWithSTSPasses` (343) + `...EmptyCredentialTypeRequiresSTS` (357) + `...ServiceAccountKeyAllowsStsDisabled` (368) -> one {spec} -> wantErr table. Keep all 8 cases; drop the `Contains(...)` wording checks.
- admin_lakekeeper_test.go: `TestLakekeeperWarehouseExistsErrorIncludesProjectID` (584) into `TestLakekeeperWarehouseExistsScopedByProject` (524).
- admin_attach_warehouse_test.go: `TestAttachWarehouse_BadArgs` (15) + `_MissingProject` (24) + `_MissingWarehouse` (33).
- admin_attach_warehouse_test.go: `TestAttachWarehouse_GCSBucketRequired` (154) + `_GCSUnknownCredType` (175) + `_UnknownType` (219).
- admin_component_test.go: `TestApplyComponentDefinition_CreatesWhenMissing` (28) + `_UpdatesWhenPresent` (48) as a table (existing: yes/no).
DELETE:
- `TestBuildWarehouseBody_GCS_InvalidJSON` (admin_lakekeeper_test.go:124, ~27): `TestBuildWarehouseBodyRedactsInvalidGCSKeyJSON` (151) covers the same input and is stricter.
- `TestBuildWarehouseBodyServiceAccountKey` (admin_lakekeeper_test.go:253, ~28): covered by `TestBuildWarehouseBody_GCS` (73), which already uses the service-account-key credential type.
- `TestAttachWarehouse_GCSSystemIdentityWithoutSTS` (admin_attach_warehouse_test.go:104, ~17): it only calls `lakekeeper.GCSWarehouseProfile.Validate()`, which is another package's function. `TestGCSSpecValidateSystemIdentityRequiresSTS` (328) covers the equivalent rule here.
- `TestAdminMigrate_DryRun` (admin_migrate_test.go:12, 21 LOC): the name says dry-run, but it only asserts that a connect error text contains "connect" or "refused".
Estimated LOC after: ~940

## pkg/datagateway (top level)  (11 files, 3447 LOC, 79 Test funcs)
KEEP: ~55. The keepers pin: `ComputeIdempotencyKey` stability and collisions, the commit pool (overflow, cancel, deadline), `ClassifyCommitError`, run-token boot validation (HappyPath, TamperedSignature, RunIDMismatch, MissingRUNID), secret-ref resolution, the `writeModeForTable` bucket-scoping incident, the manifest path resolution table, the cancel-annotation wiring (`TestCancelAnnotation_CancelsCommitPool` 133), and the Arrow IPC streaming read path.
MERGE:
- commit_pool_test.go: `TestCommitPool_WaitTwiceNoWork` (171) into `TestCommitPool_ReuseAcrossSessions` (152).
- commit_pool_test.go: `TestCommitPool_DispatchAfterCancel` (123) into `TestCommitPool_CancelUnblocksInFlight` (99).
- commit_logging_test.go: `TestLooksLikeSchemaMismatch` (35) + `TestIsSchemaDeferred` (70). Both are substring heuristics over error wording; one table.
- commit_logging_test.go: `TestLogCatalogSchemaOnMismatch_NilCatalogIsSafe` (63) + the nil subtest of `TestLogWriterSchema` (86), as one "nil-safe on shutdown" test.
- server_v2_bucket_resolve_test.go: `TestOpenWriter_UndeclaredTable_NamesDeclaredTables` (17) + `TestOpenWriter_NoOutputTablesAndNoDefaultBucket` (58) -> one table. They assert message substrings; keep the gbif typo case as a row, since it encodes the incident.
- external_files_test.go: `TestServerV2_ExternalFiles_WriteStateSetBeforePatch` (87) into `TestServerV2_ExternalFiles` (14). The "state set" assertions are guarded by `if ok2`, so they can silently assert nothing.
- files_manifest_test.go: `TestFilesManifest_DeterministicTableOrder` (87) + `TestFilesManifest_EmptyPathSkipped` (118). `TestFilesManifest_AppendConcurrent` (21) already asserts sort order.
- files_manifest_test.go: `TestFilesManifest_WriteJSONForTable_NoEntry` (195) + `TestFilesManifest_WriteJSONForTable_Errors` (209).
- server_v2_test.go: `TestResolveSecretRefsInConfigReplacesRefs` (28) + `...NoopOnEmpty` (56) + `...MissingDirErrors` (71) + `...MissingFileErrors` (85) -> one table. `NoopOnEmpty` has a dead `cfg` variable.
- server_v2_test.go: `TestServerV2_OpenWriter_MissingTable` (377) + `TestServerV2_OpenReader_MissingBucket` (538) + `TestServerV2_OpenReader_MissingTable` (556) -> one table. Today these assert exact `err.Error()` equality ("table is required").
- server_v2_test.go: `TestProtoToDataFormat` (684) + `TestDataFormatToProto` (707) -> one two-way table.
- server_v2_test.go: `TestBackendSchemaToGatewaySchema` (764) + `_Nil` (792).
- server_v2_test.go: `TestPatchParquetFieldIDs_NoExternalFiles` (904) + `TestPatchParquetFieldIDs_NoSchema` (936). The latter asserts exact error text.
- server_v2_test.go: `TestServerV2_OpenWriter` (342) with `TestOpenWriter_DeclaredTableResolvesBucket` (server_v2_bucket_resolve_test.go:84), as one "open writer resolves bucket" table.
- server_v2_streaming_test.go: `TestOpenReader_FormatArrowIPC_RoutesToStreaming` (86) + `TestOpenReader_FormatArrowIPC_ChunksAreEOFTerminated` (125) + `TestOpenReader_FormatArrowIPC_IOEOFFromReader` (216) -> one table over {3 rows/2 per group, 6 rows, 1 row}. All three run the same fixture, ReadChunk and totalRows check.
- server_v2_commit_test.go: `TestWriteModeForTable_BucketScoped` (157) + `TestWriteModeForTable_NoMatch` (185) + `TestWriteModeForTable_DefaultBucketFullLoad` (204) -> one table.
- cancel_watch_test.go: `TestWatchCancelAnnotation_HonoursContext` (55) + `TestWatchCancelAnnotation_EmptyPathBlocks` (82).
DELETE:
- `TestMemoryFootprint_EndToEnd_JSONL` and `..._CSV` (membench_test.go:31 and :44; the whole 425-LOC file): both `t.Skip` unless `DUPLET_MEMBENCH_ROWS` is set and assert nothing about memory. They are manual benchmarks posing as tests. Delete the file, or move it to `Benchmark*` behind a build tag.
- `TestServerV2_Log` (server_v2_test.go:666, ~18): only asserts "no error".
- `TestInjectFieldIDsIntoArrowSchema` (server_v2_test.go:896, ~8): a bare `t.Skip`.
- `TestValidateSchemaMatchesFieldIDs` (server_v2_test.go:873, ~24): despite the name, it only exercises `buildFieldIDMap`. `TestBuildFieldIDMap` (810) covers that.
- `TestSchemaFieldIDAutoAssignment` (server_v2_test.go:971, ~23): its rows duplicate the "auto-assign" and "mixed" rows of `TestBuildFieldIDMap` (810).
- `TestCommit_WithExternalFiles` (server_v2_test.go:994, ~58): it tolerates the expected error and asserts only `resp != nil`. `TestServerV2_ExternalFiles` (external_files_test.go:14) covers the flow with real assertions.
- `TestParseDataFormat_ArrowIPCAlias` (server_v2_streaming_test.go:188, ~27): it returns early on error and has an unused stream variable. It asserts nothing about the alias. `TestParseDataFormat` in format/adapter_test.go:29 has the alias rows.
- `TestLogWriterSchema` subtests "renders name:type with nullability marker" and "wide schema is truncated..." (commit_logging_test.go:86, ~60): they assert log text only.
Estimated LOC after: ~2440

## pkg/datagateway/backend  (10 non-tagged files, 1600 LOC, 45 Test funcs)
KEEP: ~26. The keepers pin: `local RemoveAll_RejectsEscape` (path traversal), `TestVendedTokenSourceStringRedacts` (bearer redaction), `TestGCSToObjectKey`, the minio streaming failure/cancel semantics, the parquet arrow reader byte-bound and batch-bound chunks, and `GCSBackend_Commit` stats and skip-alien-writers.
MERGE:
- `TestMinIORangeReaderAt_ReadsContiguousBytes` (minio_rangeread_test.go:64) + `_TailReadReturnsEOF` (82) + `_ReadPastEndReturnsEOF` (98) + `_OverflowingReadClampsAndReturnsEOF` (116) + `_ZeroLengthReadIsNoOp` (139) -> one table.
- `TestGCSRangeReaderAt_ReadsContiguousBytes` (gcs_rangeread_test.go:45) + `_TailReadReturnsEOF` (63) + `_ReadPastEndReturnsEOF` (79) + `_OverflowingReadClampsAndReturnsEOF` (97) + `_ZeroLengthReadIsNoOp` (120) -> one table.
- The two range-reader tables above are copy-paste twins. Share one `testRangeReaderAt(t, newReader)` helper to drop another ~80 LOC.
- gcs_test.go: `TestVendedTokenSourceReturnsBearer` (33) + `TestVendedTokenSourceRejectsS3` (56) + `TestVendedTokenSourceSurfacesError` (90).
- minio_remove_all_test.go: `TestMinIOBackend_RemoveAll_PaginatesAndDeletes` (67) + `_TwoBatches` (106) + `_NoObjects_NoError` (136) -> one table over object count (0, 3, 1001). The "Paginates" test only uses 3 objects, so its name is misleading.
- gcs_remove_all_test.go: `TestGCSBackend_RemoveAll_NoObjects_NoError` (90) + `_StripsLeadingSlash` (125).
- local_streaming_test.go: `TestLocalBackend_OpenObjectWriter` (14) + `_CreatesParentDirs` (46) + `_S3URLPath` (69) -> one table over path form.
- minio_streaming_test.go: `TestMinIOStreaming_CloseIdempotent` (182) + `_WriteAfterCloseRejected` (195).
- parquet_arrow_reader_test.go: `TestParquetArrowReader_SingleFile_TwoRowGroups` (20) + `_TwoFiles_TotalRowsCorrect` (57).
DELETE:
- `TestMinIORangeReaderAt_Size` (minio_rangeread_test.go:154, ~7) and `TestGCSRangeReaderAt_Size` (gcs_rangeread_test.go:135, ~7): trivial getters.
- `TestGCSBackendOpenReaderForFilesSignature` (gcs_test.go:129, ~20): a compile-time signature check plus an exact-wording check on "no files provided". The compiler already enforces the interface.
Estimated LOC after: ~1300

## pkg/datagateway/format  (6 files, 1840 LOC, 68 Test funcs)
KEEP: ~40. The keepers pin: the `ParseDataFormat` aliases table, the registry, the Arrow IPC multi-stream, trailing-garbage and non-seekable guards (arrow_ipc_test.go:382-495, incident), the nested-JSON-as-JSON-text incident (json_test.go:435), schema mismatch and type mismatch, and the streaming `ParseReader` paths.
MERGE:
- adapter_test.go: `TestDataFormatString` (7) + `TestDataFormatMimeType` (63) + `TestDataFormatExtension` (85) -> one enum-property table. All three iterate the same 6 enum rows.
- csv_test.go: `TestCSVAdapterParseSimple` (17) + `TestCSVAdapterParseTimestamps` (252) + `TestCSVAdapterParseDates` (268) + `TestCSVAdapterParseBooleans` (284) -> one "inferred type" table.
- csv_test.go: `TestCSVAdapterParseNoHeader` (49) + `TestCSVAdapterParseCustomDelimiter` (79) + `TestCSVAdapterParseHeaderOnly` (173) + `TestCSVAdapterParseWithNulls` (101) -> one options table.
- csv_test.go: `TestCSVAdapterSerialize` (191) into `TestCSVAdapterRoundTrip` (218).
- json_test.go: each JSON/JSONL twin pair becomes one adapter-parameterised table:
  - `TestJSONAdapterParseWithNulls` (65) + `TestJSONLAdapterParseWithNulls` (260)
  - `TestJSONAdapterParseEmpty` (130) + `TestJSONLAdapterParseEmpty` (280)
  - `TestJSONAdapterParseInvalid` (139) + `TestJSONLAdapterParseInvalidLine` (289)
  - `TestJSONAdapterSerialize` (148) + `TestJSONLAdapterSerialize` (302)
  - `TestJSONAdapterRoundTrip` (176) + `TestJSONLAdapterRoundTrip` (336)
  - `TestJSONAdapterParseSimple` (20) + `TestJSONLAdapterParseSimple` (218)
- streaming_test.go: `TestJSONLParseReader_KnownSchema` (97) + `TestJSONLParseReader_NilSchemaInfers` (143).
DELETE:
- `TestCSVAdapterFormat` (csv_test.go:10, ~7), `TestJSONAdapterFormat` (json_test.go:13, ~7), `TestJSONLAdapterFormat` (json_test.go:211, ~7), `TestParquetAdapterFormat` (parquet_test.go:17, ~7), `TestArrowIPCAdapterFormat` (arrow_ipc_test.go:41, ~6): trivial getters. `TestDefaultRegistry` (adapter_test.go:174) already asserts `adapter.Format()` for every registered adapter. Add Arrow IPC and Parquet to that loop.
- `TestArrowIPCAdapterInRegistry` (arrow_ipc_test.go:256, ~12): fold into the `TestDefaultRegistry` loop.
- `TestDataFormatArrowIPC` (arrow_ipc_test.go:269, ~15) and `TestParseDataFormatArrowIPC` (arrow_ipc_test.go:285, ~20): every row is already in the `adapter_test.go` tables (7, 29, 63, 85).
- `TestDataFormatRoundTrip` (adapter_test.go:107, ~20): derivable from the String and Parse tables.
- `TestDefaultParseOptions` (adapter_test.go:195, ~10) and `TestDefaultSerializeOptions` (adapter_test.go:206, ~15): trivial constructor-default pins.
- `TestJSONAdapterNestedIgnored` (json_test.go:409, ~25): subsumed by `TestJSONAdapterNestedObjectSerializedAsJSON` (435), which asserts the string type and JSON content.
- `TestJSONAdapterBooleans` (json_test.go:390, ~18): bool inference is pinned by schema inference tests.
- `TestCSVAdapterParseWithSchema` (csv_test.go:130, ~34): it asserts that the schema it passed in is returned and that its own string columns are strings. `TestArrowIPCAdapterParseWithProvidedSchema` (145) is the real test of provided-schema behavior.
Estimated LOC after: ~1335

## pkg/datagateway/buffer  (5 files, 1336 LOC, 27 Test funcs)
KEEP: ~18. The keepers pin: the async writer pipelining and error propagation, buffer-manager flush, rotation and closed behavior, the streaming parquet writer, and the coalesce behaviors (small records accumulate, fast-path, flush ordering, row trigger, drain on flush/close, `TestSchemasCompatible`).
MERGE:
- async_writer_test.go: `TestAsyncWriter_CloseIdempotent` (122) + `TestAsyncWriter_RejectsWriteAfterClose` (134).
DELETE:
- `TestMemoryFootprint_BufferManagerDirect` (membench_test.go:35), `TestMemoryFootprint_Coalesced` (284), `TestEstimatorAccuracy` (387) (the whole 456-LOC file): all `t.Skip` unless `DUPLET_MEMBENCH_ROWS` is set, so CI never runs them. Delete, or turn into `Benchmark*`.
- `TestDefaultBufferConfig` (manager_test.go:14, ~25): a change-detector on magic numbers. Its own comment records them changing across commits.
- `TestEstimateRecordSize` (manager_test.go:210, ~20): asserts only `size > 0`.
- `TestCompressionString` (parquet_test.go:16, ~20): trivial Stringer.
- `TestDefaultParquetWriterConfig` (parquet_test.go:37, ~16): trivial constructor defaults.
Estimated LOC after: ~760

## pkg/datagateway/schema  (2 files, 982 LOC, 40 Test funcs)
KEEP: ~15. The keepers pin: the `ToArrowType`/`FromArrowType` tables, `TestSchemaEqual`, the `Columns()` copy semantics, the inference null-token set, mixed-type to string fallback, and `TestCustomInferenceConfig`.
MERGE:
- inference_test.go: `TestTypeInferrer_InferValueType_Bool` (40) + `_Int64` (75) + `_Float64` (100) + `_Timestamp` (126) + `_Date` (150) + `_String` (173) + `_Null` (198) -> one table. All are {input} -> DataType.
- inference_test.go: `TestInferSchema_AllSameType` (220) + `_MixedNumericTypes` (251) + `_NullHandling` (270) + `_AllNulls` (300) + `_BoolColumn` (322) + `_DateColumn` (342) + `_TimestampColumn` (361) + `_MixedTypesToString` (380) + `_BoolMixedWithNumeric` (399) + `_EmptyRows` (419) -> one {rows} -> {type, nullable} table. Keep `_EmptyHeaders` (440) and `_RealWorldCSV` (450).
- inference_test.go: `TestInferSchemaFromJSON` (489) + `_MissingFields` (525) + `_NilValues` (557) + `_Empty` (548).
- types_test.go: `TestNewSchema` (187) + `TestNewSchemaEmpty` (224) + `TestNewSchemaInvalidType` (231).
- types_test.go: `TestNewSchemaFromArrow` (242) + `TestNewSchemaFromArrowNil` (263).
- types_test.go: `TestSchemaColumnByName` (270) + `TestSchemaColumnIndex` (296).
DELETE:
- `TestDataTypeRoundTrip` (types_test.go:64, ~17): derivable from the `TestDataTypeString` (9) and `TestParseDataType` (35) tables.
- `TestArrowTypeRoundTrip` (types_test.go:164, ~23): derivable from `TestToArrowType` (81) and `TestFromArrowType` (119).
Estimated LOC after: ~600

## pkg/k8s/controllers  (11 files, 2524 LOC, 61 Test funcs)
KEEP: ~40. The keepers pin: the admission and freeze lifecycle (snapshot immutability, rotation isolation, mid-run mutation, live pipeline deleted), the secret snapshot tests, the error taxonomy (transient vs validation), `TestBuildComponentJob_*` hardening and token mount, the clamp admission freeze, the image-id capture-once, and `TestIterTagFromImage`.
MERGE:
- componentdefinition_controller_test.go: `...ConfigSchemaDoesNotCompile` (88) + `...StableVersionBadFormat` (96) + `...DuplicateVersions` (104) + `...DefaultVersionNotRegistered` (115) + `...ImageMissingTag` (123) + `...ImageLatestTag` (131) + `...ImageRegistryPortNoTag` (139) + `...ResourcesDefaultExceedsMax` (169) -> one `{mutate fn, wantSubstr}` table. All 8 reuse `assertInvalid`. Fold `...Valid_SetsValidPhase` (76), `...ImageRegistryPortWithTag` (147) and `...PrereleaseSkipsVersionAndImageChecks` (157) in as the valid rows.
- pipelinerun_jobs_test.go: `TestGenerateGatewayConfig_LakekeeperURL` (247) + `..._LakekeeperURL_OmittedWhenUnset` (265) + `..._PipelineAPIJWKSURL` (444) + `..._PipelineAPIJWKSURL_TrimsTrailingSlash` (466) + `..._PipelineAPIJWKSURL_OmittedWhenUnset` (528) -> one {reconciler fields} -> {want substring, forbid substring} table.
- pipelinerun_jobs_test.go: `TestBuildComponentJobHasRuntimeTolerations` (490) + `TestBuildComponentJobNilTolerationsOmitsField` (511).
- pipelinerun_jobs_test.go: `TestGatewaySidecarUsesRuntimePullPolicy` (570) + `TestGatewayHonoursRuntimePullPolicyOverride` (614) + `TestComponentContainerHonoursRuntimePullPolicyOverride` (644) -> one {runtime policy, version stability} -> {gateway policy, component policy} table. The component case is the ImagePullBackOff incident; keep it as a row. The prerelease/stable component policy is also in `TestFreeze_ComponentPullPolicy_RegistryDriven` (pipelinerun_freeze_test.go:213).
- pipelinerun_jobs_test.go: `TestBuildGatewaySidecarEnvIncludesIterationIDWhenImageMatchesIterForm` (543) + `...OmitsIterationIDWhenImageHasNoIterTag` (554). The parsing is pinned by `TestIterTagFromImage`, so these only need to prove the env var wiring, as 2 rows.
- pipelinerun_freeze_test.go: `TestFreeze_ValidRun_ResolvedSpecAndJobImage` (160) + `TestFreeze_OmittedVersion_LatestStable` (191).
- resource_clamp_test.go: `TestApplyDefaultsThenClamp_LimitsCPUOverMax` (46) + `..._RequestsClampedToMax` (73) + `..._EphemeralStorageOverMax` (109) -> one table. The same clamp is applied to three resource names.
DELETE: none outright. The jobs and componentdefinition merges above are the savings.
Estimated LOC after: ~2120

## pkg/pipeline/validate  (5 files, 1612 LOC, 39 Test funcs)
KEEP: ~22. The keepers pin: the `validate_test.go` table, the policy tables, `ReferencedSecrets`, `TestValidatePipelineDocEffectiveName` (regression), `TestUpstreamInputDemotedToWarning`, and the Resolve happy and deprecated cases.
MERGE:
- schema_test.go: `TestValidateConfig_RequiredMissing` (64) + `_WrongType` (79) + `_EnumFieldWithRef_NoFinding` (95) + `_IntegerFieldWithRef_NonStringFinding` (109) + `_SecretFieldWithPlaintext_Finding` (130) + `_SecretFieldWithRef_NoFinding` (146) + `_AdditionalPropsSecret_Plaintext_Finding` (165) + `_AdditionalPropsSecret_Ref_NoFinding` (183) + `_AdditionalPropsInteger_Ref_NonStringFinding` (196) + `_UnknownProperty_Finding` (223) + `_Valid_NoFindings` (235) -> one {schema, cfg} -> {wantPath, wantSubstr} table. Each test repeats the same compile-and-validate boilerplate. This keeps every plaintext-secret rejection case.
- schema_test.go: `TestCompileSchema_InvalidSchemaString` (51) + `TestCompileSchema_IgnoresXDatupletSecret` (57).
- registry_test.go: `TestStaticRegistry_Resolve_UnknownComponent` (26) + `_UnknownVersion` (43) + `_NoStableNoPin` (60) + `_DefaultVersionNotRegistered` (79) + `_InvalidPhase` (99) -> one {registry, name, version} -> {path, severity} table.
- registry_test.go: `TestValidateTyped_IOCapability_InputsNone_RejectsDeclaredInputs` (258) + `_InputsRequired_RejectsMissingInputs` (276) + `_OutputsNone_RejectsDeclaredOutputs` (294) + `_OutputsRequired_RejectsMissingOutputs` (312) + `_OptionalDefault_NoFindings` (330) -> one table asserting path + severity. They currently assert exact `f.Message ==` strings, e.g. "component stdout-writer takes no outputs". This also removes the `stdout-writer` references (see the list at the end).
- validate_test.go: `TestValidatePipelineDocNameContext` (633) into `TestValidatePipelineDocEffectiveName` (648). Its "mismatch" subtest already covers it.
DELETE:
- `TestValidatePipelineDocEnvelopeRejected` (validate_test.go:628, ~5): `TestParseRejectsLegacyEnvelope` (config/parser_test.go:60) pins the same behavior at the layer that implements it.
- `TestValidatePipeline_ParsesNestedConfig` (validate_test.go:566, ~22): duplicates `TestParseNestedConfig` (config/parser_test.go:11) and the "valid nested config" row at validate_test.go:39.
Optional trims inside the `TestValidatePipeline_TableCases` table (validate_test.go:31): the positive twin rows "output table with no columns still valid" (264), "input valid since" (392), "input valid sinceDays" (432) and "all component names unique" (498). That is about 60 LOC.
Estimated LOC after: ~1350

## pkg/pipeline/config  (3 files, 294 LOC, 10 Test funcs)
KEEP: 6. The keepers pin: the lossless Doc/CR round trip, the golden render, strict unknown-field rejection (nested and top-level), and `ParseSinceDuration`.
MERGE:
- parser_test.go: `TestParseRejectsUnknownComponentField` (39) + `TestParseRejectsUnknownTopLevelField` (79).
DELETE:
- `TestParseEnvelopeFreeDoc` (parser_test.go:68, ~10): the same shape as `TestParseNestedConfig` (11) and the render round-trip (render_test.go:113).
- `TestRenderYAMLDeterministic` (render_test.go:97, ~16): the byte-exact golden test at render_test.go:80 would flap if render were nondeterministic.
Estimated LOC after: ~240

## pkg/pipeline/schemalint  (2 files, 353 LOC, 14 Test funcs)
KEEP: 14, as the Form-Subset convention. `TestLint_BuiltinsCleanZeroIssues` is the CI gate for every `components/*/schema.json`.
MERGE (optional, drops no rule coverage): `TestLint_Rule1_MissingType` (131) + `_Rule1_MultiTypeAccepted` (146) + `_Rule2_ForbiddenKeyword` (161) + `_Rule2_ArrayOfArraysAccepted` (179) + `_Rule3_MissingDescription` (197) + `_Rule3_EmptyDescription` (211) + `_Rule4_RequiredWithDefault` (225) + `_Rule5_UnknownAnnotation` (240) + `_Rule5_ProducesNotRoot` (254) + `_Rule5_ProducesAtRootAccepted` (275) + `TestLint_PassingFixture` (117) + `TestLint_InvalidJSON` (124) -> one {schema} -> {want rule, forbid rule} table. Fold `TestLint_IssuePathPresent` (291) in as a post-condition over every issue.
DELETE: none.
Estimated LOC after: ~260

## pkg/catalogwriter  (4 files, 1548 LOC, 48 Test funcs)
KEEP: ~30. The keepers pin: the redaction tests (`TestS3CredsStringRedacts` creds_test.go:39, `TestGCSCredsStringRedacts` 62, all 5 `TestScrubBody*` at vended_creds_test.go:621-666), the `parseCreds` fail-closed confused-deputy rejections, the floor and renewal-failure TTL behavior, the concurrent Get tests, retry/conflict semantics including the jitter regression (retry_test.go:139), and `TestReplaceDataFiles_DelegatesToTxn`.
MERGE:
- vended_creds_test.go: `TestVendedCreds_FifteenMinuteTTL` (73) + `TestVendedCreds_FiveMinuteTTL` (134) -> one {ttl, probe offsets} table. Same 50% rule at two TTLs.
- vended_creds_test.go: `TestParseCredsRejectsMixed` (440) + `TestParseCredsRejectsMixedGCSExpected` (465), as a table over expected type.
- vended_creds_test.go: `TestParseCredsRejectsWrongFamily` (454) + `TestParseCredsRejectsWrongFamilyGCSExpected` (478) + `TestParseCredsRejectsEmpty` (488) + `TestParseCredsUnsetExpectedRejects` (516) -> one reject table. Assert `err != nil`, not the `Contains(...)` wording.
- vended_creds_test.go: `TestParseGCSCredsHappyPath` (526) + `TestParseGCSCredsMissingTokenRejects` (557) + `TestParseGCSCredsFallbackExpiry` (568) + `TestParseGCSCredsCredsExpirationMsFallback` (585) + `TestParseCredsHappyPathS3` (496) + `TestParseCredsHappyPathGCS` (605).
- vended_creds_test.go: the five `TestScrubBody*` (621, 632, 642, 653, 666) -> one {in, mustNotContain, mustContain} table. Keep every row.
- vended_creds_test.go: `TestCredsRefreshMetrics_SuccessCounterS3` (685) + `_SuccessCounterGCS` (726).
- client_test.go: `TestClient_DropTable_NoPurge` (146) + `TestClient_PurgeTable_RestCatalog` (191) -> one table over `purgeRequested`. Both have the same fixture and a 20-line inline handler.
- client_test.go: `TestClient_DropNamespace_HappyPath` (264) + `_NonEmpty_Error` (301).
DELETE:
- `TestS3CredsImplementsCreds` (creds_test.go:10, ~3) and `TestGCSCredsImplementsCreds` (creds_test.go:14, ~3): compile-time interface checks. The package already uses these as `Creds`.
- `TestS3CredsType` (creds_test.go:18, ~10) and `TestGCSCredsType` (creds_test.go:28, ~10): trivial getters; `parseCreds` tests assert `.Type()` already (496, 605).
Estimated LOC after: ~1270

## pkg/icebergjob  (2 files, 871 LOC, 21 Test funcs)
KEEP: ~16. The keepers pin: the audit unforgeability test (audit_test.go:185, injected run-mode claim ignored), unknown token kind, the commit happy paths, idempotency hit, snapshot-props stamping, and `RejectsCallerCommitKey`.
MERGE:
- audit_test.go: `TestBuildSnapshotSummary_EmptyToken` (95) + `TestBuildSnapshotSummary_MalformedToken` (105).
- commit_shared_test.go: `TestCommitTable_EmptyManifest_NoOp` (353) + `TestCommitTable_MissingManifest_NoOp` (380).
- commit_shared_test.go: `TestParseFilesManifest_V1Migration` (420) + `TestParseFilesManifest_V2` (440) + `TestParseFilesManifest_Empty` (591).
DELETE:
- `TestBuildSnapshotSummary_EnvVarDoesNotControlRunMode` (audit_test.go:219, ~30): by its own comment it "documents" a structural fact. It sets no env var, and its assertion is identical to `TestBuildSnapshotSummary_LocalCLIToken` (66).
- `TestRunModeFromTokenKind` (audit_test.go:151, ~34): every row is already covered through `TestBuildSnapshotSummary_RunToken` (30), `_LocalCLIToken` (66) and `_UnknownTokenKind` (128).
- `TestCommitTableFiles_SucceedsWithInMemoryPaths` (commit_shared_test.go:457, ~33): `TestCommitTable_AppendHappyPath` (178) and `TestCommitTableFiles_StampsSnapshotProps` (490) cover the same append path.
Estimated LOC after: ~690

## sdk/go  (4 files, 555 LOC, 11 Test funcs)
KEEP: 4. The keepers pin: `FailureDetail` (the incident table), `TestConfig_OutputTableColumns`, `TestWriteBatching_ThresholdFlush`, and `WriteChunkPreservesOrder`.
MERGE:
- writer_batching_test.go: `TestWriteBatching_SmallWritesAccumulate` (78) + `TestWriteBatching_ThresholdFlush` (102) + `TestWriteBatching_DisabledByOption` (213) -> one {threshold, writes} -> {POST count} table.
- config_test.go: `TestConfig_OutputTableColumns` (12) + storage_bootstrap_test.go: `TestStorageBootstrap` (10), as a `Client` accessor table. Note `TestStorageBootstrap` is a trivial-getter test but the sole test of a public function, so merge it rather than delete it.
DELETE:
- `TestWriteBatching_CloseDrains` (writer_batching_test.go:156, ~28): it never calls `Close` (its own comment says so). It calls `Flush`, so it duplicates `TestWriteBatching_FlushDrains` (130).
- `TestWriteBatching_RoundtripReduction` (writer_batching_test.go:235, ~32): 10K writes to re-prove the threshold flush already covered by `_ThresholdFlush` (102). It also asserts "100x" in a fixture-dependent way.
Estimated LOC after: ~410

## sdk/go/arrow  (4 files, 2161 LOC, 58 Test funcs)
KEEP: ~34. The keepers pin the core `Sink` lifecycle contract in typed_sink_test.go (batch boundaries and self-contained IPC streams, lazy open, open failure is `WriteError`, Finish idempotent, sticky write failure drops no rows, close-after-failure only once, `BytesShipped`). They also pin the inference lattice, exact numeric fidelity, `TypeViolationError` vs `WriteError`, unknown-key tracking and caps, the `ArrowTypeFor` vocabulary, `reader_test.go`, and the façade-routing tests that prove StringSink and InferringSink go through the core's `Finish`.
`StringSink` and `InferringSink` are thin façades over the typed `Sink` core (sink.go:106 `core *Sink`; inferring_sink.go:323 `buildCore`). Re-running the whole core lifecycle contract on each façade is the main duplication.
MERGE:
- inferring_sink_test.go: the 8 "Declared*" value tests -> one {declared type, value} -> {arrow type, cell value | TypeViolationError} table:
  - `...DeclaredLongFromJSONNumberExact` (668)
  - `...DeclaredIntIs32Bit` (690)
  - `...DeclaredIntOverflowIsTypeViolation` (718)
  - `...DeclaredFloatFromJSONNumber` (749)
  - `...DeclaredStringFromJSONNumberKeepsExactText` (771)
  - `...DeclaredStringValueViolatesIntColumn` (793)
  - `...DeclaredBoolValueViolatesStringColumn` (813)
  - `...DeclaredNestedValueFitsStringColumnAsCompactJSON` (827)
- sink_test.go: `TestStringSink_UnknownKeysCapHoldsRecordCountExact` (577) + `TestStringSink_UnknownKeysUnderCapNotAtCap` (612).
- typed_sink_test.go: `TestSink_BytesShippedAgreesWithWriterReceipts` (277) + `TestSink_BytesShippedZeroForZeroRows` (398) + `TestSink_BytesShippedNotAdvancedByFailedWrite` (412).
DELETE (each re-tests behavior owned by the core `Sink`; the surviving test is the equivalent `TestSink_*` in typed_sink_test.go):
- `TestStringSink_BatchBoundariesAndSelfContainedStreams` (sink_test.go:174, ~46) -> `TestSink_BatchBoundariesAndSelfContainedStreams` (typed_sink_test.go:77).
- `TestStringSink_ZeroRecordsNeverOpensWriter` (sink_test.go:255, ~18) -> `TestSink_ZeroRowsNeverOpensWriter` (typed_sink_test.go:147).
- `TestStringSink_OpenFailureIsWriteError` (sink_test.go:273, ~11) -> `TestSink_OpenFailureIsWriteError` (typed_sink_test.go:166).
- `TestStringSink_FinishIsIdempotent` (sink_test.go:284, ~24) -> `TestSink_FinishIsIdempotent` (typed_sink_test.go:179).
- `TestStringSink_AddAfterFinishErrors` (sink_test.go:308, ~22; it also asserts exact error text) -> `TestSink_RowDoneAfterFinishErrors` (typed_sink_test.go:203).
- `TestInferringSink_BatchBoundariesAndSelfContainedStreams` (inferring_sink_test.go:420, ~42) -> typed_sink_test.go:77.
- `TestInferringSink_ZeroRecordsNeverOpensWriter` (inferring_sink_test.go:462, ~20) -> typed_sink_test.go:147.
- `TestInferringSink_OpenFailureIsWriteError` (inferring_sink_test.go:482, ~12) -> typed_sink_test.go:166.
- `TestInferringSink_FinishIsIdempotent` (inferring_sink_test.go:494, ~25) -> typed_sink_test.go:179.
- `TestInferringSink_AddAfterFinishErrors` (inferring_sink_test.go:519, ~24) -> typed_sink_test.go:203.
- `TestInferringSink_DeclaredZeroRecordsNeverOpensWriter` (inferring_sink_test.go:852, ~21) -> typed_sink_test.go:147.
- `TestInferringSink_DeclaredBatchBoundariesAndSelfContainedStreams` (inferring_sink_test.go:873, ~37) -> typed_sink_test.go:77.
- `TestInferringSink_DeclaredOpenFailureIsWriteError` (inferring_sink_test.go:910, ~10) -> typed_sink_test.go:166.
Total about 310 LOC. Keep `TestStringSink_WriteFailureIsStickyAndDropsNoRowsSilently` (330) and `..._FinishClosesWriterOnStickyWriteFailure` (383). They pin façade routing through the core's `Finish`; see the comment at sink.go:264.
Estimated LOC after: ~1700

## components/queryengine (all packages)  (untagged: 9 files, 2628 LOC, 70 Test funcs; `duckdb_arrow`-tagged: 6 files, 1517 LOC, 27 Test funcs; integration files excluded)
KEEP: ~75. The keepers pin:
- The whole JWT verification matrix (`query-worker/verify_test.go`) and `TestConstantsParity`.
- The key cache, server 401/429/408/413/400 mapping, and `TestSanitizeEngineError`.
- The SQL-echo strip test (server_test.go:688) and the params shape violations.
- `TestValidateParams` and `TestPlaceholderSpans`.
- All `duckdb_arrow` lockdown, attach-redaction, params-bind and single-connection security tests (engine_open_test.go:38, `lockdown_deny_test.go`, `attach_test.go`, `params_bind_test.go`, `sqlescape_test.go`).
MERGE:
- cmd/query-worker/server_test.go: `TestServer_TimeoutClamped` (354) + `TestServer_MaxRowsMaxBytesClamped` (380) + `TestServer_MaxRowsMaxBytesUnderCeiling` (410) + `TestServer_ZeroTimeoutUsesDefault` (889) -> one {body, cfg} -> {runner request limits} table.
- cmd/query-worker/server_test.go: `TestServer_MissingSQL_Returns400` (471) + `TestServer_MissingCatalogJWT_Returns400` (490) + `TestServer_MissingWarehouse_Returns400` (505) + `TestServer_MalformedJSON_Returns400` (538).
- cmd/query-worker/server_test.go: `TestServer_RunnerTimeout_Returns408` (297) + `TestServer_RunnerResultTooLarge_Returns413` (316) + `TestServer_RunnerPlainError_Returns400` (335).
- cmd/query-worker/server_test.go: `TestServer_MissingToken_Returns401` (208) + `TestServer_GarbageToken_Returns401` (229). This is a merge only; it stays a security-boundary test.
- cmd/query-worker/server_test.go: `TestServer_Params_ForwardedToRunner` (721) + `TestServer_NoParams_Regression` (764).
- cmd/query-worker/verify_test.go: the claim-mutation tests (100, 116, 130, 145, 160, 174, 202, 219, 236, 248, 261, 302, 316, 332, 347, 362) -> one {mutate claims} -> accept/reject table. Every case stays; this removes about 140 LOC of boilerplate.
- cmd/datuplet-query/config_test.go: `TestLoadConfig_MissingCluster` (62) + `_MissingAPIToken` (73) + `_NoProjects` (84) + `_EmptyAPITokenFile` (93).
- cmd/datuplet-query/mint_test.go: `TestMintQueryToken_PolicyOff` (40) + `_ServerError` (58) + `_EmptyToken` (74).
- cmd/datuplet-query/flags_test.go: `TestParseFlags_Defaults` (10) + `TestParseFlags_InvalidFormat` (26).
DELETE:
- `TestMaxSafeInteger` (params_test.go:700, ~65) and `TestFloatSafeInteger` (params_test.go:766, ~65): every row (max/min safe, 2^53, 1e300, float 2^53 +/- 0.5) is already a row of `TestValidateParams` (10), at the "int64 max safe", "int64 exceeds max safe" and "integral float64 ..." rows (params_test.go:278-341).
- `TestRequestDefaults` (types_test.go:5, ~6) and `TestColumnRoundsTrip` (types_test.go:12, ~5): they assert a struct literal's own fields.
Estimated LOC after: ~2160 untagged + ~1480 `duckdb_arrow` = ~3640

## components/data-generator  (5 files, 987 LOC, 59 Test funcs)
KEEP: ~25. The keepers pin: the `TestBuildArrowSchema_TypedNonNullable` incident (arrow_writer_test.go:15), the leak-fix test (`TestFinishRandomQuietly_ClosesWriterBeforeExit` leak_test.go:63), seed determinism, `TestShouldStop_OR_FirstToTrip`, and the config-validation error classes.
MERGE:
- config_test.go: `TestParseAndValidate_EmptyTables` (22) + `_MissingName` (29) + `_InvalidIdentifier` (41) + `_BothModesSet` (53) + `_NeitherModeSet` (62) + `_UnknownColumnType` (70) + `_RandomSchemaMissing` (98) + `_LimitMissing` (110) + `_LimitAllZero` (122) + `_NegativeLimitRowsCount` (134) + `_NegativeLimitSizeInBytes` (146) + `_NegativeLimitTimeoutInSeconds` (158) + `_NegativeRowInsertSpeed` (170) + `_LiteralEmptyRows` (200) + `_LiteralColumnsRequired` (244) + `_MultipleTableErrors` (253) -> one {cfg} -> {wantErrSubstr} table. Fold `_AllKnownTypes` (82) and `_ValidRandomMode` (186) in as the valid rows.
- literal_test.go: `TestValidateLiteral_MixedTypes` (66) + `_AllNullColumn` (76) + `_ArityMismatch` (86) + `_ValidRows` (96) + `_SingleRow` (105) + `_IntAndDouble` (115) + `_ColumnsArityMismatch` (125) + `_ColumnsDuplicate` (135) + `_ColumnsInvalidName` (145) + `_ColumnsWithDot` (155) + `TestValidateRandom_InvalidColumnName` (165) -> one table. `TestInferLiteralSchema_*` (11, 40, 50) -> one table.
- generator_test.go: `TestGenerateValue_NonNil` (50) + `_StringLength` (63) + `_DateFormat` (79) + `_TimestampFormat` (94) + `_UUIDFormat` (109).
- generator_test.go: `TestShouldStop_RowsOnly` (126) + `_BytesOnly` (142) + `_TimeOnly` (155) + `_NilLimit` (181), keeping `_OR_FirstToTrip` (170).
- generator_test.go: `TestErrInjectionPoint_RowsBased` (200) + `_NoMessage` (220) + `_NilLimit` (232) + `_NilSpec` (243) + `_TimeBased` (251).
- leak_test.go: `TestFinishRandomQuietly_SafeAfterFailedFlush` (123) overlaps `TestSink_FinishTwiceAfterFailureClosesOnlyOnce` (sdk/go/arrow/typed_sink_test.go:351). Keep one of them.
DELETE:
- `TestParseAndValidate_LiteralMixedTypes` (config_test.go:209, ~9), `_LiteralArityMismatch` (config_test.go:218, ~9), `_LiteralAllNullColumn` (config_test.go:227, ~9), `_LiteralValidWithNullable` (config_test.go:236, ~8): exact duplicates of `TestValidateLiteral_MixedTypes` / `_ArityMismatch` / `_AllNullColumn` / `_ValidRows` (literal_test.go:66, 86, 76, 96). Those already go through `ParseAndValidate`.
- `TestNewRNG_EmptyRunID` (generator_test.go:44, ~5): asserts only "does not panic".
- `TestSortStrings` (generator_test.go:188, ~11): a trivial insertion sort.
Estimated LOC after: ~680

## components/http-json-extractor  (6 files, 617 LOC, 18 Test funcs)
KEEP: ~13. The keepers pin: the streaming decode shapes, `UseNumber` fidelity, malformed-remainder guards, `NewExtractorSink_ModeSelection`, `BadDeclaredType`, `ComposesDeclaredColumns`, and the config validation table.
MERGE:
- stream_test.go: `TestDecodeRecords_Errors` (69) into `TestDecodeRecords_ErrorText` (78).
- parse_test.go: `TestParseJSON_Shapes` (5) + `TestParseJSON_Invalid` (34) into `TestDecodeRecords_Shapes` (stream_test.go:20). The first 4 rows are identical to that table, and `parseJSON` is still the buffered path (main.go:450), so run both implementations from one table.
- columns_test.go: `TestFinishQuietly_ClosesAlreadyOpenedWriter` (60) + `TestFinishQuietly_UntouchedSinkNoWriter` (79).
DELETE:
- `TestUnknownKeysWarning` (warn_test.go:17, 58 LOC): asserts the text of the warning string ("capped", the cap number, the dropped count, the batch size, key names). That is log-text only. The cap behavior itself is pinned in sdk/go/arrow (sink_test.go:577, 612).
Estimated LOC after: ~515

## components/sql-transform  (0 files, 0 LOC, 0 Test funcs)
No `*_test.go` exists. There is nothing to cut, but it is untested.
Estimated LOC after: 0

## References to components being deleted
- `stdout-writer` is referenced as a generic component name in pkg/pipeline/validate/registry_test.go at lines 296, 301, 307, 314, 319 and 325. These are `TestValidateTyped_IOCapability_OutputsNone_RejectsDeclaredOutputs` (294) and `..._OutputsRequired_RejectsMissingOutputs` (312). Replace with a neutral name when merging; the exact-message asserts at 307 and 325 embed the string.
- `stdout-writer` also appears at tests/e2e/scenarios_test.go:122 (the `read-back` scenario description: "read back via stdout-writer (K8s)"). This is outside the packages you assigned but will break when the component is deleted.
- `finnhub`, `pandas` and the Python SDK have NO references in any `*_test.go` I covered, or in any `*_test.go` repo-wide (excluding `.claude/worktrees`).
- Not a test: pkg/datagateway/format/arrow_ipc.go:21 has a comment mentioning Python (PyArrow). `sdk/python` and `sdk/python-sandbox` directories exist but contain no Go tests.

## Totals
Current test LOC across the scoped, non-`integration` packages: about 30,960 (this includes about 880 LOC of the two skipped membench files). Estimated after the proposals above: about 23,500, a reduction of about 7,400 LOC (about 24%). Test funcs drop by roughly 280 of about 790. Many of those are folded into table-driven tests rather than lost as coverage.

Per-package current -> after (LOC):

| Package | Current | After |
|---|---|---|
| cmd/datuplet | 4806 | 3260 |
| cmd/pipeline-api | 1284 | 940 |
| pkg/datagateway (top) | 3447 | 2440 |
| backend | 1600 | 1300 |
| format | 1840 | 1335 |
| buffer | 1336 | 760 |
| schema | 982 | 600 |
| controllers | 2524 | 2120 |
| validate | 1612 | 1350 |
| config | 294 | 240 |
| schemalint | 353 | 260 |
| catalogwriter | 1548 | 1270 |
| icebergjob | 871 | 690 |
| sdk/go | 555 | 410 |
| sdk/go/arrow | 2161 | 1700 |
| queryengine | 4145 | 3640 |
| data-generator | 987 | 680 |
| http-json-extractor | 617 | 515 |
| sql-transform | 0 | 0 |

## Top 5 highest-value deletions (LOC, confidence, zero lost coverage)
1. Both membench files, about 880 LOC: pkg/datagateway/membench_test.go (425) and pkg/datagateway/buffer/membench_test.go (456). Both `t.Skip` unless `DUPLET_MEMBENCH_ROWS` is set, so CI never runs them, and they have no pass/fail assertion on memory. Delete, or convert to benchmarks.
2. The façade lifecycle duplicates in sdk/go/arrow, about 310 LOC. These are 5 tests in sink_test.go (174-329) and 8 tests in inferring_sink_test.go (420-542, 852-919). The core `Sink` already owns this contract in typed_sink_test.go (77, 147, 166, 179, 203).
3. The `components/queryengine/params_test.go` `TestMaxSafeInteger` and `TestFloatSafeInteger` pair (700-830), about 130 LOC. Every row is already in `TestValidateParams`.
4. The dead or non-asserting tests in pkg/datagateway/server_v2_test.go and server_v2_streaming_test.go, about 190 LOC: `TestCommit_WithExternalFiles` (994), `TestInjectFieldIDsIntoArrowSchema` (896, a `t.Skip`), `TestValidateSchemaMatchesFieldIDs` (873), `TestSchemaFieldIDAutoAssignment` (971), `TestServerV2_Log` (666), and `TestParseDataFormat_ArrowIPCAlias` (streaming_test.go:188).
5. The cmd/datuplet/apps_test.go collapses, about 230 LOC deleted and about 60 added: the 6 `*_InvalidNameRejectedLocally` tests become 1 table, the 7 arity tests become 1 table, and the 8 not-found / status tests become 1 table. The two redundant exit-20 tests (apps_test.go:1088, 1099) and the empty-list wording test (1896) go too.

Runner-up: the 5 `TestGenerateGatewayConfig_*` tests in pkg/k8s/controllers/pipelinerun_jobs_test.go (247-540) and the 3 pull-policy tests (570-660) collapse into 2 tables for about 160 LOC.
