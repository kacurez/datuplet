# Pipeline preview mode — design

Date: 2026-11-05. Status: revision 2 after a fresh architecture review and a
builder check; awaiting maintainer approval.

## 1. Intent

An agent that is asked to "set up an ETL pipeline in Datuplet" works by trial:
it guesses a source config, looks at what comes out, fixes the config, looks
again, pins column types, adds a transform, looks again. Today every trial is
the full production loop — `pipeline put` (overwrites the stored pipeline),
`trigger`, wait, `storage sample` — and every trial leaves marks: a snapshot on
a real table (APPEND accumulates duplicates, FULL_LOAD replaces production
data), a `runs` row, a Pipeline CR, and namespaces/tables created on first
write that nothing removes.

Preview mode gives the agent a **cheap, disposable trial run**: it submits a
PipelineDoc inline (nothing is saved), the platform runs it for real on the
cluster but writes every output into per-preview scratch namespaces with a row
cap, and returns one JSON document the agent can reason about — per output
table: inferred schema, row count, whether the cap was hit, a sample of rows,
and a ready-to-paste `columns:` block — plus per-component log tails on
failure. The scratch tables are also queryable with ordinary `datuplet query`
SQL for deeper checks. Everything a preview creates expires on a TTL and is
purged by pipeline-api itself. When the agent is satisfied it does
`pipeline put` + `trigger` exactly as today.

Several agents can work in one project at once. Each preview belongs to a
**workspace**, a bounded space per agent session: listing, concurrency caps
and bulk teardown are scoped to it, and scratch namespaces are keyed by
preview id, so two agents never see or touch each other's trials.

### Why not a "dry run without commit"

The Iceberg commit fires inside the sidecar at `CloseWriter`, the table is
created in Lakekeeper at `OpenWriter`, and parquet files land under the real
table's `data/` prefix. A no-commit flag would still create catalog tables,
would leave orphan parquet with no cleanup path (none exists), and would leave
pipeline-api with no way to read the produced rows (it holds no storage
credentials; the query worker reads only catalog tables). Committing to a
scratch namespace keeps the trust model, makes results readable through
surfaces that already exist, and makes cleanup one `purgeRequested` drop per
table.

## 2. Goals, non-goals, anti-goals

**Goals**

- G1. Run any PipelineDoc, or a prefix of its stages, without saving it and
  without touching any non-scratch table. Hard guarantee, enforced in the
  gateway, not just by convention.
- G2. One call returns everything an agent needs to decide the next edit:
  per-output schema, row count, cap flag, sample rows, `columns:` hint,
  per-stage phases and messages, component and gateway log tails on failure.
- G3. Bounded cost per trial: row cap per output table (default 1 000),
  wall-clock cap (default 5 min), no retry pod, concurrency caps.
- G4. Zero residue after the TTL on a stock install: scratch tables purged
  (files included), PipelineRun + Secrets + ConfigMaps gone, FGA tuple gone
  at terminal phase. No `runs` row, no `pipelines` row, no Pipeline CR ever
  created.
- G5. Two agents in one project cannot intersect: previews are listed,
  capped and torn down per workspace; scratch names never depend on the
  workspace label being unique.
- G6. Agent-first CLI: `datuplet preview -f doc.yaml --json` blocks and prints
  the result; exit codes an agent can branch on.

**Non-goals (v1)**

- No reuse of an earlier preview's outputs for a later stage. With the row
  cap the extract stage is seconds; `--until-stage` plus a re-run covers
  iteration. Reuse is the first follow-up if that proves wrong (§8).
- No browser UI for previews. Scratch namespaces are visible in the storage
  browser like any other table (clearly named, expiring).
- No limit on *input* rows read from real project tables. A transform over a
  large real table is as slow in preview as in a run; `since`/`sinceDays` are
  the existing knobs.
- No warm pod pool. Pod start + image pull is the latency floor; images are
  cached after the first run (`IfNotPresent`).
- No promotion of preview outputs into real tables.
- No persistence of logs beyond the Job's 1 h TTL.

**Anti-goals**

- A preview that can write, replace, or truncate a non-scratch table — even by
  a buggy or malicious component hard-coding a bucket name.
- A preview that shows up as a run anywhere, or that overwrites
  `Pipeline/<name>`.
- Scratch data or an FGA tuple that outlives its preview because a cleanup
  step depends on something that is off by default (the reaper CronJob ships
  suspended) or on a user who may have lost access.

## 3. Constraints (verified in code)

- A trigger needs a saved pipeline: `runs.pipeline_id NOT NULL`, the observer
  joins `runs ⋈ pipelines` to verify the PipelineRun name, cancel looks the
  name up by pipeline id. Previews get their own table and observer branch.
- `PipelineRunSpec` has only `pipelineRef`, `runId`, `runTokenRef`; the CRD
  manifest lists `pipelineRef` as required; `handlePending` threads the
  `*Pipeline` into `snapshotRunSecrets`, `ValidateTyped`,
  `PipelineGeneration` and the secrets condition (four call sites). Stages run
  strictly in array order; there is no stage subset.
- Inputs/outputs reach the sidecar only through the controller-generated
  `gateway.yaml`; table identity is `(bucket = Iceberg namespace, table)`;
  the gateway honours a request-supplied bucket (`OpenWriterToBucket`) and
  today enforces **nothing** against `output_buckets`.
- The run-token validator checks `sub == run_id == $RUN_ID`; `pipeline_name`
  is not checked. A run whose doc writes gets a project-wide `editor` tuple.
- `ValidateTyped` requires a name; the rewrite synthesizes one.
- Bucket names must match `^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`; storage
  identifier validation and `QuoteIdent` accept hyphens.
- Warehouses are registered with Lakekeeper's `hard` delete profile;
  `catalogwriter.PurgeTable` removes data files; `DropNamespace` needs the
  namespace empty.
- pipeline-api already has: an RS256 signer, `tokens.MintServiceToken`, the
  bootstrap service identity (`sub=pipeline-api-bootstrap`, `project_admin`
  on every project it creates), a Lakekeeper client, an FGA authorizer, a K8s
  client, a query-worker proxy (`queryproxy.Core.Preview`, 100 rows / 1 MiB /
  30 s, 1 in-flight per principal), and a catalog proxy with
  `listAllTables` / `loadTable` / `purgeTable`.
- The reaper CronJob is `suspend: true` with no way to flip it, carries no
  signing key or Lakekeeper URL by design, and `ReapRunTuples` deletes any
  `run_tuples` row older than 30 min that has no `runs` row and no live
  PipelineRun with the run-id label. Previews must not touch `run_tuples`.
- `MintImpersonation` takes its subject from the authenticated request
  context; there is no request context in a background sweep.
- The cancel watcher aborts every open writer; only tables committed by
  already-finished writers survive a cancel.
- A Job killed by `activeDeadlineSeconds` yields no exit code and classifies
  as `FailedApplication`; `BackoffLimit 1` re-runs the pod.
- pipeline-api's K8s RBAC is a ClusterRole (`pods: list, patch`); the only
  per-namespace Role is the secrets one from `EnsureProjectNamespace`.
- PUT/validate reject `resources` for non-superadmins and apply the gateway
  bounds policy; previews must apply the same gate.
- Datuplet is always installed fresh: a new table is a new migration file,
  no upgrade path.

## 4. The design

### 4.1 Overview

```
agent ──POST /previews {doc, workspace,…}──▶ pipeline-api
            │ validate (unchanged validator + PUT's resources/policy gate)
            │ rewrite output buckets → pv-<id8>-<bucket>; name → preview-<id8>
            │ previews row, FGA tuple, run token, PipelineRun (inline spec + preview block)
            ▼
   pipeline-operator ── stages ≤ until_stage, BackoffLimit 0, deadline from startTime → FailedUser
            │ gateway.yaml: preview.row_limit  (fence = declared buckets, always on)
            ▼
   component + DG sidecar ── capped rows → pv-<id8>-<bucket>.<table>, ordinary commit
   pipeline-observer ── mirrors status into previews row
   pipeline-api ticker (60 s, advisory-locked) ──
            terminal: drop FGA tuple, assemble + cache result (schema, rows, sample, log tails)
            expired : cancel+wait, purge tables, drop namespaces, delete PipelineRun → cleaned_at
agent ◀──GET /previews/{id}── cached result document
```

A preview is a real PipelineRun with three differences: its spec is inline,
its outputs are redirected, and its sidecar caps rows. Scheduling, secrets
snapshot, cancellation, status mirroring: existing machinery.

### 4.2 API

All routes under `/api/v1/projects/{pid}`; auth as pipeline PUT
(`data_admin`), plus PUT's `resources` gate and gateway-bounds policy.

`POST /previews` — body (JSON or YAML, 1 MiB cap):

```json
{
  "doc": { "...": "an envelope-free PipelineDoc; name optional" },
  "workspace": "etl-orders",  // [a-z0-9][a-z0-9-]{0,62}; default "default"
  "row_limit": 1000,          // per output table; 1..100000; default 1000
  "timeout_seconds": 300,     // whole preview, from Running; 30..3600; default 300
  "ttl_seconds": 3600,        // after terminal phase; 60..86400; default 3600
  "until_stage": "transform"  // optional: run stages[0..i] inclusive
}
```

Response `202 {id, workspace, phase:"Pending", scratch_namespaces:[…],
expires_at}`. Errors: `400 {error, findings}` for validation findings (same
shape as PUT); `409 too_many_previews {running_in_workspace, running_in_project}`
when a running cap is hit.

`GET /previews/{id}` — the result document (§4.5).
`GET /previews?workspace=&limit=&cursor=` — list; `workspace` defaults to
`default`, `*` lists all workspaces of the project.
`DELETE /previews/{id}` — cancel if running, purge now, 204 (idempotent;
`502 purge_failed` leaves the row for the ticker).
`DELETE /previews?workspace=X` — tear down a whole workspace; `202
{deleted:[ids]}`, purge proceeds synchronously per preview and the
response lists failures.
`POST /previews/{id}/cancel` — cancel only; tables committed by stages that
finished before the cancel stay inspectable until TTL (open writers are
aborted, so the stage being cancelled leaves nothing).

Previews never appear under `/runs`.

### 4.3 Storage: the `previews` table

New migration (fresh-install only):

```
previews(
  id uuid pk, project_id uuid fk, triggered_by uuid, workspace text,
  doc jsonb,                  -- the ORIGINAL doc as submitted (canonical JSON)
  row_limit int, timeout_seconds int, ttl_seconds int, until_stage text null,
  scratch_namespaces text[],  -- every pv-<id8>-<bucket> this preview may write
  phase text, current_stage text, message text, stage_statuses jsonb,
  observed_rv bigint,
  fga_tuple_deleted_at timestamptz null,
  result jsonb null,          -- cached result document body (outputs, log tails)
  created_at, started_at, completed_at, expires_at, cleaned_at timestamptz null)
index (project_id, workspace, created_at desc); index (expires_at) where cleaned_at is null
```

`expires_at` is set at creation to `created_at + timeout + ttl` (so an
abandoned or never-observed preview still expires) and reset to
`completed_at + ttl` when the observer records a terminal phase.

### 4.4 Doc rewrite and the write fence

`pkg/pipelineapi/preview.Rewrite(doc, id)` returns `(rewritten, namespaces)`:

- `id8` = first 8 hex of the preview UUID; scratch bucket for logical bucket
  `b` is `pv-<id8>-<b>`, cut to 63 chars (12-char prefix, bucket tail
  truncated); two logical buckets that collide after truncation are rejected
  with a finding at creation.
- Every `outputs.defaultBucket`, `outputs.buckets[].name`, and
  `outputs.tables[].bucket` is mapped.
- Every `inputs.buckets[]` / `inputs.tables[].bucket` that names a bucket
  **produced by an earlier stage of the same doc** is mapped to the same
  scratch bucket. Buckets not produced earlier stay as they are (real project
  tables are read as-is). This mirrors the validator's `availableBuckets`
  rule. Divergence to document: if bucket `raw` both exists in the project and
  is produced by stage 1, a run's stage 2 sees the whole real table while a
  preview's stage 2 sees only the capped scratch rows.
- Table names are never changed, so dynamic (`defaultBucket`) and explicit
  (`tables[]`) outputs land under the same scheme.
- With `until_stage`, later stages are dropped before conversion.
- The synthesized name is `preview-<id8>`.

The rewritten doc is validated again with `ValidatePipelineDoc` (a rewrite
breaking a regex is a bug → 500). The original doc is what gets stored and
what the result document echoes.

**Write fence (gateway, unconditional).** In `openWriter`, after bucket
resolution and *before* `LoadOrCreateForWrite` (which creates the namespace
as a side effect), the resolved bucket must be one of `default_bucket`,
`output_buckets[]`, or an `output_tables[].bucket`; otherwise
`PermissionDenied: bucket "x" is not a declared output of this component`.
For a preview the declared set *is* the scratch set, so no extra config is
needed. This is also a hardening of normal runs: a component writing to an
undeclared bucket, which today silently succeeds, becomes a `FailedUser`.
Recorded in CHANGELOG + known-limitations as a behaviour change.

### 4.5 Row cap and the result document

**Row cap (gateway).** `gateway.yaml` gains `preview: {id, row_limit}`. The
gateway keeps one counter per `(bucket, table)` across writers; in
`processParsedRecord`, the chunk that crosses the cap is sliced to fit, later
rows for that table are discarded, and the response carries
`limit_reached: true` (gRPC `WriteChunkResponse` field; HTTP: JSON field and
header `X-Datuplet-Limit-Reached: true`). Nothing errors: a component that
ignores the signal still produces a correct capped table, it just wastes
time. Partitioned writers are unaffected (slicing happens before the plane
splits by tuple).

**SDK.** Once a writer has seen `limit_reached`, its `Write`/`WriteChunk`
returns `sdk.ErrRowLimitReached` after accepting what was accepted (for the
batching `Write` this surfaces on the flush that crossed the cap). The
built-ins: http-json-extractor stops paginating (the speed win) and
finishes normally; data-generator stops generating; sql-transform needs no
change (one parquet chunk; the gateway slices it).

**Cap flag without a new channel.** The result document reports
`at_cap = (total-records >= row_limit)` from the current snapshot summary
(`total-records`, correct across several commits to one table). `at_cap`
means "the table holds the cap; the source may have more". No new snapshot
properties and no change to `icebergjob`.

**Timeout and retries (controller).** `spec.preview.timeoutSeconds` is
enforced controller-side from `status.startTime`: on expiry the controller
annotates pods with the cancel annotation, fails the run `FailedUser` with
message `preview timed out after Ns during stage <s>`, and lets the normal
cancel path tear the Jobs down. Preview Jobs get `BackoffLimit 0` (no retry
pod, which would APPEND a second copy into scratch).

**Result document** (`GET /previews/{id}`), assembled **once** by the ticker
when it first sees a terminal phase, stored in `result`, and served from the
row thereafter (outputs are immutable after completion; the query worker is
hit once per table, not once per GET):

```json
{
  "id": "…", "workspace": "etl-orders", "phase": "Succeeded", "message": "",
  "created_at": "…", "started_at": "…", "completed_at": "…", "expires_at": "…",
  "row_limit": 1000, "until_stage": null, "doc_name": "events-etl",
  "stages": [
    {"name": "extract", "phase": "Succeeded", "duration_ms": 8300, "message": "",
     "components": [{"name": "json-extractor", "phase": "Succeeded", "exit_code": 0,
                     "message": "", "log_tail": null, "gateway_log_tail": null}]}
  ],
  "outputs": [
    {"stage": "extract", "component": "json-extractor",
     "bucket": "raw", "table": "data",                 // logical, as in the doc
     "scratch_table": "pv-ab12cd34-raw.data",          // for `datuplet query`
     "rows": 1000, "at_cap": true,
     "schema": [{"name": "id", "type": "long", "nullable": true}, …],   // Iceberg types
     "columns_hint": [{"name": "id", "type": "long"}, …],              // PipelineDoc `columns:`
     "sample": {"columns": [{"name": "id", "type": "BIGINT"}, …],
                "rows": [[1, "…"], …], "truncated": false}}
  ],
  "warnings": ["sample skipped for pv-ab12cd34-raw.data: query_disabled"]
}
```

- `stages` from the mirrored `stage_statuses`. `log_tail` (last 200 lines of
  the component container) and `gateway_log_tail` (last 200 lines of the
  sidecar; it carries the `writer schema:` line, which is the only schema an
  agent gets when a stage failed before committing) are collected for every
  component whose phase is a failure, while the Job pods exist (≤1 h after
  finish). Otherwise `null`. pipeline-api gets `pods get/list` and
  `pods/log get` in the **per-project-namespace Role** created by
  `EnsureProjectNamespace` (extend `secret_rbac.go`), not in the ClusterRole.
- `outputs` by listing each scratch namespace through the catalog proxy
  (covers dynamic table names), mapping `pv-<id8>-<b>` back to `b`; the
  producing stage/component comes from the doc when declared, else `null`.
  Per table: schema from `LoadTable`; `rows` and `at_cap` from the snapshot
  summary; `sample` of 20 rows via `queryproxy.Core.Preview` (skipped with a
  warning when the query worker is disabled or busy; a later `GET
  ?resample=true` retries sampling for tables whose sample is missing).
  Deeper inspection is `datuplet query` against `scratch_table`.
- `columns_hint` maps Iceberg types to the PipelineDoc vocabulary
  (`int, long, float, double, boolean, string, variant`); anything else
  (timestamp, date, decimal, nested) is hinted `string` with a `warnings`
  entry naming the column.
- Note for the docs: APPEND vs FULL_LOAD is unobservable in a preview
  (scratch starts empty every time).

### 4.6 Workspaces

- `workspace` is a label on the preview, chosen by the client
  (`--workspace`, env `DATUPLET_WORKSPACE`, default `default`). An agent
  uses one per task or session.
- Scoped to the workspace: `GET /previews` listing, the running cap
  (`previews.maxRunningPerWorkspace`, default 2), and bulk teardown
  (`DELETE /previews?workspace=`).
- Project-wide backstops: `previews.maxRunningPerProject` (default 8) and
  `previews.maxPerProject` (default 50 rows; on create, the oldest *cleaned*
  rows beyond it are deleted, and if still over, the oldest terminal ones are
  purged eagerly before the 409 check).
- Isolation itself never rests on the label: scratch namespaces, PipelineRun
  names and tuples are keyed by preview id.

### 4.7 Lifecycle and cleanup

- **Create** (pipeline-api): validate + gates → rewrite → caps → insert
  `previews` row → FGA tuple `(user:oidc~<preview-uuid>, editor,
  project:<lk>)` written directly (no `run_tuples` breadcrumb) → mint run
  token with `run_id = preview id` → `CreateRunResources` with a PipelineRun
  named `preview-<id>`, label `datuplet.io/preview-id`, no
  `datuplet.io/run-id` label on the PipelineRun (Jobs/Pods/ConfigMaps keep
  `datuplet.io/run-id=<preview id>` so the cancel annotation path works
  unchanged), `spec.pipelineSpec` (inline, `DocToCR(rewritten)`),
  `spec.preview {id, rowLimit, timeoutSeconds}`. CRD manifest: `pipelineRef`
  no longer required, `oneOf` pipelineRef / pipelineSpec; `DeepCopyInto` for
  the two new pointer fields.
- **Run** (operator): `handlePending` builds the `*Pipeline` value from
  `spec.pipelineSpec` when present and skips the CR lookup; the four
  downstream call sites take that value unchanged. `startStage` sets
  `BackoffLimit 0` for previews; `handleRunning` enforces the deadline;
  `generateGatewayConfig` emits the `preview` block.
- **Observe**: the observer's informer selects on either label; preview
  objects update `previews` (namespace + expected name `preview-<id>`
  checked, same guards as runs, `GuardTerminal`), resetting `expires_at` on
  terminal phases.
- **Ticker** (`pipeline-api serve`, every 60 s, `pg_try_advisory_lock` so
  only one replica sweeps): 
  1. *Terminal, tuple present* → delete the FGA tuple, set
     `fga_tuple_deleted_at` (blast radius ≤ 15 s, same as cancel).
  2. *Terminal, `result IS NULL`* → assemble the result document (§4.5),
     store it.
  3. *`expires_at < now`, `cleaned_at IS NULL`* (and on `DELETE`): if
     non-terminal, cancel (tuple, pod annotation, delete PipelineRun) and
     **wait until the PipelineRun is gone** (a purge under a live writer
     does not stop the writer); purge every table in each scratch namespace
     (`PurgeTable`, "not found" is success); `DropNamespace`; delete the
     PipelineRun if still present (cascades Secrets/ConfigMaps/Jobs); verify
     the namespaces list no tables; set `cleaned_at`, and `phase = Expired`
     if it was non-terminal.
  Lakekeeper calls use `MintServiceToken` with the bootstrap service
  identity (`project_admin` on every project pipeline-api created); the
  builder verifies in the FGA model that it can drop tables/namespaces
  before relying on it. Any failing step logs, leaves the row for the next
  tick, and increments `pipelineapi_preview_sweep_failures_total`
  (labelled by step). Rows are retained after cleanup for audit
  (`triggered_by`, `workspace`, `doc`) until the project cap evicts them.
- **Stuck states**: a PipelineRun deleted by hand leaves `phase=Running`;
  `expires_at` from creation covers it and the sweep marks it `Expired`.

### 4.8 CLI

```
datuplet preview [-f doc.yaml|-] [--workspace W] [--rows 1000] [--until-stage S]
                 [--timeout 5m] [--ttl 1h] [--no-wait] [--json]
datuplet preview get <id> [--json]
datuplet preview list [--workspace W|'*'] [--json]
datuplet preview delete <id> | --workspace W --all
datuplet preview cancel <id>
```

`datuplet preview -f …` creates, polls every 2 s (same loop as `trigger
--wait`; cancels on SIGINT/timeout), waits until the result document is
cached (`result != null`), then prints it (`--json`) or a human summary:
per stage phase/duration, per output `bucket.table rows=… at_cap=… →
scratch_table`, the schema as a table, and the sample. Exit codes: `0`
Succeeded, `1` FailedUser/Cancelled/timed out (the agent's config or data),
`20` FailedApplication or transport failure (platform) — the same split
`datuplet apps render` already uses.

`docs/agent-quickstart.md`, `docs/pipeline-api.md`, and the
`datuplet-operator` skill get a new step between validate and put: "preview,
read `outputs[].columns_hint`, pin types, re-preview; then put + trigger",
and the guidance to pick a workspace per task.

## 5. The agent loop after this

```bash
export DATUPLET_WORKSPACE=orders-etl-$(date +%s)
datuplet components get http-json-extractor --schema
datuplet pipeline validate -f etl.yaml --json
datuplet preview -f etl.yaml --until-stage extract --json     # 1 000 rows, ~10 s + pod start
#   → "created" came out as string; add columns: … or fields: …
datuplet preview -f etl.yaml --json                            # whole pipeline, capped
datuplet query --sql 'select count(*), min(created) from "pv-ab12cd34-curated"."daily"'
datuplet pipeline put -f etl.yaml && datuplet trigger --wait --json etl
datuplet preview delete --workspace "$DATUPLET_WORKSPACE" --all
```

## 6. Decisions (and alternatives rejected)

| Decision | Alternative rejected | Why |
|---|---|---|
| Scratch namespace per logical bucket, `pv-<id8>-<bucket>`, table names unchanged | One namespace per preview with `<bucket>__<table>` tables | Dynamic-bucket outputs pick table names at runtime; per-bucket namespaces keep both output modes identical |
| Rewrite in pipeline-api + unconditional declared-bucket fence in the gateway | Preview-only `allowed_write_namespaces` list | The declared set already equals the scratch set; one check, no extra config, and runs get the same hardening |
| Commit to scratch tables | No-commit / discard mode | Commit fires at `CloseWriter`, tables are created at `OpenWriter`, no orphan GC, pipeline-api could not read rows |
| Separate `previews` table + `preview-id` label | Reuse `runs` with nullable `pipeline_id` | Keeps every run surface untouched; previews never look like runs |
| Inline `spec.pipelineSpec` on PipelineRun | Throwaway Pipeline CR | Nothing to clobber, nothing to delete |
| Soft row cap per table, gateway slices, `limit_reached` signal | gRPC error on overflow | Components that ignore it still produce a correct table; built-ins honour it for speed |
| `at_cap` derived from `total-records` | New snapshot summary properties | Zero DG→API channel, zero `icebergjob` change, correct across multiple commits |
| Sweep as a ticker in pipeline-api under the bootstrap service identity | Reaper CronJob + creator impersonation | The CronJob ships suspended with no key or Lakekeeper access; impersonation needs a request context and fails forever once the creator loses access |
| FGA tuple dropped at terminal phase, tracked on the row | `run_tuples` breadcrumb | The orphan sweep would revoke a live preview at 30 min; runs' own tuple lifetime is a pre-existing gap, not copied |
| Controller-side deadline → `FailedUser`, `BackoffLimit 0` | `activeDeadlineSeconds` | A deadline-killed Job has no exit code and classifies as platform failure; a retry pod double-appends |
| Result assembled once and cached | Live assembly per GET | Query worker allows 1 in-flight per principal; live assembly collides with the agent's own queries |
| `pods/log` in the per-project Role | ClusterRole | Cluster-wide would expose lakekeeper/postgres/openfga logs |
| Workspaces as a label with scoped list/caps/teardown | A first-class session resource with its own TTL | Each preview already expires; a label gives the isolation without a second lifecycle |
| Reuse deferred | Reuse in v1 | Cross-preview expiry coupling, eager-purge exclusions and a 409 contract for a case a capped re-extract already makes cheap |

## 7. Left to the builder

- Exact Iceberg → `columns:` type table and the warning text for unmapped
  types.
- The precise `limit_reached` wire shape on the HTTP write path (field and
  header both is simplest for the SDK).
- Human (non-JSON) rendering of the result in the CLI.
- Advisory lock key and the ticker's per-step timeouts.
- Where the preview endpoints live in `pkg/pipelineapi/http` (own file,
  own store package `pkg/pipelineapi/preview`).
- The e2e scenario's exact assertions beyond those in §9.

## 8. Follow-ups (not in this change)

- Reuse of an earlier preview's outputs for a later stage (`--reuse`).
- `input_row_limit` for reads from real tables.
- Dropping runs' FGA tuples at terminal phase too (today they wait for the
  suspended reaper).
- Persisting run/preview logs beyond the Job TTL.

## 9. Verification

- Unit: rewrite (explicit/dynamic/cross-stage/until_stage/name truncation),
  fence (declared vs undeclared bucket, before namespace creation), row cap
  (per table across writers, chunk boundary, partitioned, sql-transform's
  single parquet chunk), SDK sentinel on batching `Write`, result assembly
  from fakes incl. query worker disabled, ticker state machine (tuple step,
  assemble step, purge with a failing table once, cancel-and-wait), caps and
  eviction order, workspace scoping, deadline → `FailedUser`.
- Integration (`integration` tag, Lakekeeper + silo): a preview writer's
  commit lands in `pv-*`; `PurgeTable` removes the data files including
  uncommitted parquet left by an aborted writer; the bootstrap service
  identity can purge and drop namespaces created by a run identity.
- e2e (`tests/e2e`, OrbStack): preview of `examples/pipelines/etl-duckdb.yaml`
  capped at 50 rows in workspace `a`; assert the result document (`at_cap`,
  schema, sample, `columns_hint`), that `raw.data` does not exist,
  `datuplet query` over the scratch table works, a second workspace `b`
  cannot list `a`'s preview, `--until-stage extract` runs one stage, a
  failing SQL preview returns `FailedUser` with a `log_tail`, `DELETE`
  leaves no table, and a 60 s TTL preview is purged by the ticker. Wall-clock
  of one capped preview is recorded in the PR.
- `make lint`; CRD manifest + `DeepCopyInto`; `make k8s-reload-crds`;
  `make sync-component-schemas` (no schema change expected).

## 10. Recon summary (facts this design rests on)

- `handleTriggerRun` loads only stored docs; `TriggerRequest` is doc-based
  (`runbackend/backend.go`); `CreateRunResources` builds the Secret +
  PipelineRun (`pkg/pipelineapi/k8s/run_create.go`).
- Controller: `handlePending` → `snapshotRunSecrets` → `ValidateTyped` →
  freeze `resolvedSpec`; `generateGatewayConfig` (`pipelinerun_jobs.go`)
  emits the sidecar YAML (`pkg/datagateway/gatewayconfig.go`); Jobs
  `BackoffLimit 1`, TTL 3600 s, native sidecar init container.
- Gateway: `openWriter` resolves bucket from request → `output_tables` →
  `default_bucket` with no enforcement; `LoadOrCreateForWrite` creates
  namespace + table; `finalizeAndDispatch` at `CloseWriter` commits via
  `icebergjob.CommitTableFiles`; `processParsedRecord` parses every chunk to
  Arrow before the plane writer.
- Result surfaces: catalog proxy for schema/snapshots (`total-records`);
  `queryproxy.Core.Preview` for rows; `catalogwriter.PurgeTable` /
  `DropNamespace` for cleanup; `tokens.MintServiceToken` + bootstrap identity
  for the platform principal.
- Observer: `DBRunUpdater.Update` keyed by `datuplet.io/run-id`, verifies
  namespace and expected name, mirrors `stage_statuses`.
