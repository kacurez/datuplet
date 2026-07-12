# pipeline-api

The pipeline-api is Datuplet's central product REST service. It owns:
- User session auth (cluster mode) + single-user local mode
- Project + pipeline + run management
- JWT minting for run tokens + impersonation tokens (for lakekeeper)
- OIDC discovery + JWKS publication (lakekeeper polls this)
- Browser UI at `/ui/*`
- Storage browse proxy at `/api/v1/storage/*` (lakekeeper-backed)
- FGA authorization (OpenFGA) — per-project grants enforced on every write and read

For the end-to-end token lifecycle (session cookie → run JWT → impersonation JWT → FGA legs), see [`docs/auth-flow.md`](auth-flow.md).

## Run locally

### 1. Start Postgres

```bash
docker run -d --name datuplet-pg -e POSTGRES_PASSWORD=dev \
  -p 5432:5432 postgres:16
export DATABASE_URL="postgres://postgres:dev@localhost:5432/postgres?sslmode=disable"
```

### 2. Build

```bash
make build-pipeline-api
```

### 3. Seed an admin user and project

```bash
./bin/pipeline-api admin create-user --email=you@example.com --password=changeme
./bin/pipeline-api admin create-project --name=acme
./bin/pipeline-api admin grant --user=you@example.com --project=acme --role=admin
```

### 4. Run the server

```bash
SIGNING_KEY_FILE=/path/to/priv.pem \
./bin/pipeline-api serve --addr=:8081
```

### 5. Try the API

```bash
# login → sets cookie in /tmp/cookies
curl -sS -c /tmp/cookies -X POST -H 'Content-Type: application/json' \
  -d '{"email":"you@example.com","password":"changeme"}' \
  http://127.0.0.1:8081/api/v1/auth/login -i

# whoami
curl -sS -b /tmp/cookies http://127.0.0.1:8081/api/v1/auth/me

# list your projects
curl -sS -b /tmp/cookies http://127.0.0.1:8081/api/v1/projects

# logout
curl -sS -b /tmp/cookies -X POST http://127.0.0.1:8081/api/v1/auth/logout
```

## Config

| Variable | Default | Purpose |
|---|---|---|
| `PIPELINE_API_ADDR` | `:8081` | HTTP listen address |
| `DATABASE_URL` | (required in cluster mode) | Postgres DSN |
| `PIPELINE_API_COOKIE_SECURE` | `false` | Require HTTPS for session cookie (set `true` in prod) |
| `SIGNING_KEY_FILE` | — | Path to RS256 private key PEM. Required for run-token minting and OIDC discovery. |
| `PIPELINE_API_PUBLIC_URL` | — | Public base URL advertised in OIDC discovery doc (`/.well-known/openid-configuration`). Lakekeeper polls this. |
| `DATUPLET_OPENFGA_URL` | — | OpenFGA gRPC address (e.g. `openfga:8081`). Required for FGA checks. |
| `DATUPLET_OPENFGA_STORE_ID` | — | OpenFGA store ID. |
| `DATUPLET_LAKEKEEPER_URL` | — | Lakekeeper REST catalog base URL. Required for storage browse proxy. |
| `PIPELINE_API_UI_DIR` | — | Filesystem path to the `ui/product/` directory. Set to `/app/ui/product` in the Docker image. |

When `DATABASE_URL` is unset the server runs in reduced mode exposing only `/healthz` and (when `SIGNING_KEY_FILE` is set) `/api/v1/auth/jwks.json` and `/.well-known/openid-configuration`.

## Auth

Three token types are in play. See [`docs/auth-flow.md`](auth-flow.md) for the full lifecycle.

| Token | Minted by | Lifetime | Used by |
|-------|-----------|----------|---------|
| Session cookie | `POST /api/v1/auth/login` | 24 h sliding | Browser — all human-facing API routes |
| Run JWT (RS256) | `tokens.MintRunToken` at trigger | 24 h | DG sidecar + TableCommit; lakekeeper verifies |
| Impersonation JWT (RS256) | `tokens.MintImpersonation` per storage request | 5 min | Storage browse proxy → lakekeeper |

**No `revoked_tokens` denylist.** Cancellation revokes the FGA tuple for the synthetic run identity (blast radius ≤ 15 s from cancel to DG stopping storage I/O).

## JWT + JWKS

pipeline-api publishes its RS256 public key at `GET /api/v1/auth/jwks.json`. Lakekeeper fetches the OIDC discovery doc at `/.well-known/openid-configuration` and from there the JWKS endpoint.

### Generating a keypair

```bash
pipeline-api admin keygen --private-out priv.pem --public-out pub.pem
```

Writes a 2048-bit RSA keypair. `priv.pem` is mode 0400, `pub.pem` is mode 0444. Pass `--bits 4096` for a larger key; `--force` to overwrite existing files.

### Enabling JWKS

```bash
SIGNING_KEY_FILE=/etc/pipeline-api/priv.pem \
PIPELINE_API_PUBLIC_URL=https://pipeline-api.example.com \
  ./bin/pipeline-api serve --addr=:8081
```

```bash
curl -s http://127.0.0.1:8081/api/v1/auth/jwks.json
# → {"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":"key-1","n":"...","e":"AQAB"}]}

curl -s http://127.0.0.1:8081/.well-known/openid-configuration
# → {"issuer":"https://…","jwks_uri":"https://…/api/v1/auth/jwks.json","…"}
```

When `SIGNING_KEY_FILE` is unset, both endpoints return 404.

### Key rotation

To rotate without downtime:
1. Write the new PEM to the `pipeline-api-signing-key` Secret under a fresh key.
2. Update `SIGNING_KEY_FILE` + `SIGNING_KEY_ID` in the Deployment; `kubectl rollout restart`.
3. Lakekeeper re-fetches the JWKS at its configured polling interval.
4. Tokens minted by the old key remain valid until they expire (24 h for run tokens). Schedule rotations so the old key is advertised at least `RunTokenLifetime` past the switchover.

## Admin CLI

```bash
pipeline-api admin create-user --email EMAIL --password PW
pipeline-api admin create-project --name NAME
pipeline-api admin grant --user EMAIL --project NAME --role admin|user
pipeline-api admin keygen --private-out priv.pem --public-out pub.pem
pipeline-api admin lakekeeper-bootstrap  # bootstrap lakekeeper warehouse (cluster deploy)
```

All admin commands (except `keygen`) require `DATABASE_URL`. `create-user` and `create-project` error on re-run (unique constraint); `grant` upserts the role.

## Runs

### Create a project with its K8s namespace + lakekeeper Project

```bash
./bin/pipeline-api admin create-project --name=acme
./bin/pipeline-api admin grant --user=you@example.com --project=acme --role=admin
```

Project provisioning creates the `datuplet-<uuid>` K8s namespace (labelled `datuplet.io/project-id`), creates the lakekeeper Project, and writes FGA tuples for the admin user.

### Upload a pipeline

> The example files under `examples/pipelines/` bundle a `Pipeline` document
> followed by a `PipelineRun` document (the `PipelineRun` is there so
> `kubectl apply -f ...` works standalone). `PUT` parses only the first YAML
> document, so it stores the `Pipeline` and silently ignores the bundled
> `PipelineRun`; runs are triggered via the `POST .../runs` call below.

```bash
PID=<project-uuid>
curl -sS -b /tmp/cookies -X PUT \
  --data-binary @examples/pipelines/simple-http-extract.yaml \
  -H 'Content-Type: application/yaml' \
  http://127.0.0.1:8081/api/v1/projects/$PID/pipelines/simple-pipeline -i
```

### Trigger a run

```bash
curl -sS -b /tmp/cookies -X POST \
  http://127.0.0.1:8081/api/v1/projects/$PID/pipelines/simple-pipeline/runs \
  -H 'Content-Type: application/json' -d '{}'
# → 202 {"id":"…","status":"Pending"}
```

What happens behind the scenes (see `K8sBackend.TriggerRun`):

1. FGA check: `mustHaveRelation("data_admin")` on the project.
2. `INSERT run_tuples (committed=false)` — crash-recovery breadcrumb.
3. `authzr.WriteTuples(...)` — grants the synthetic run identity `editor` on `project:<lkP>`.
4. Mint RS256 run JWT (`sub=<run-uuid>`, `aud=datuplet-catalog`, 24 h). Create K8s Secret + PipelineRun CR.
5. `UPDATE run_tuples (committed=true)` — best-effort observability marker.
6. The pipeline-operator picks up the PipelineRun, launches one Job per component. DG sidecar mounts the run JWT at `/var/run/secrets/datuplet-runtoken/token` and uses it for every lakekeeper call.
7. The pipeline-api observer mirrors PipelineRun status into the `runs` Postgres table.
8. On completion or cancel, the FGA tuple for the synthetic run identity is deleted.

### List + inspect runs

```bash
curl -sS -b /tmp/cookies http://127.0.0.1:8081/api/v1/projects/$PID/runs
curl -sS -b /tmp/cookies http://127.0.0.1:8081/api/v1/projects/$PID/runs/<id>
```

`GET /api/v1/projects/{pid}/runs` returns a paged envelope, not a bare array:

```json
{
  "runs": [
    {
      "id": "…", "project_id": "…", "pipeline_id": "…", "pipeline_name": "daily-sync",
      "phase": "Succeeded", "current_stage": "load", "message": "",
      "created_at": "2026-07-05T12:00:00Z",
      "started_at": "2026-07-05T12:00:01Z",
      "completed_at": "2026-07-05T12:03:44Z"
    }
  ],
  "next_cursor": "eyJ0IjoiMjAyNi0wNy0wNVQxMjowMDowMFoiLCJpIjoiLi4uIn0="
}
```

Each run object gains `started_at`, `completed_at` (both omitted until set), and `pipeline_name` (joined from the pipeline row).

Query parameters (all optional):

| Param | Meaning |
|-------|---------|
| `limit` | Page size, clamped to 1..200 (default 50). |
| `cursor` | Opaque keyset cursor copied from a prior response's `next_cursor`. Omit for page 1. An invalid/tampered cursor returns 400. |
| `pipeline` | Case-insensitive substring match on pipeline name. |
| `pipeline_id` | Exact pipeline UUID. Invalid UUID returns 400. |
| `phase` | One of `Pending`, `Running`, `Succeeded`, `FailedUser`, `FailedApplication`, `Cancelled`, `Expired`. Unknown value returns 400. |

Pages are ordered newest-first. `next_cursor` is `null` (JSON) once there are no more rows.

`GET /api/v1/projects/{pid}/runs/{id}` returns the same run object plus a `timeline` array reconstructed from the persisted stage-status snapshot:

```json
{
  "id": "…", "phase": "Succeeded", "…": "…",
  "timeline": [
    {
      "name": "extract", "phase": "Succeeded",
      "started_at": "2026-07-05T12:00:01Z", "completed_at": "2026-07-05T12:01:10Z",
      "duration_ms": 69000, "message": "",
      "imported": [{ "kind": "table", "bucket": "raw", "table": "orders", "label": "raw.orders" }],
      "exported": [{ "kind": "bucket", "bucket": "staging", "label": "staging" }]
    }
  ]
}
```

`imported`/`exported` entries describe tables or buckets declared in the pipeline YAML for that stage; `kind` is `"table"` or `"bucket"`. `timeline` is `null` when no stage-status snapshot has been recorded yet for the run.

### Cancel a run

```bash
curl -sS -b /tmp/cookies -X POST \
  http://127.0.0.1:8081/api/v1/projects/$PID/runs/<id>/cancel
```

Cancel order: FGA tuple deleted first → pod annotated `datuplet.io/cancel=true` → PipelineRun CR deleted → Postgres phase set to Cancelled. DG's next lakekeeper call returns 403 within ≤15 s.

## Config additions (K8s cluster mode)

| Variable | Default | Purpose |
|---|---|---|
| `KUBECONFIG` | — | Path to kubeconfig. Ignored in-cluster. |
| `PIPELINE_API_IN_CLUSTER` | `false` | Set `true` when running as a K8s Pod (uses in-cluster config). |

## Browser UI

`ui/product/` is the browser UI, served by pipeline-api at `/ui/*` when `PIPELINE_API_UI_DIR` is set. The Docker image COPYs the directory to `/app/ui/product`; the K8s Deployment sets the env var accordingly. Leaving the env var unset makes `/ui/` return 404.

### Pages

| Path | Purpose |
|------|---------|
| `/ui/login` | Email + password → `POST /api/v1/auth/login` (session cookie) |
| `/ui/pipelines` | List the active project's pipelines |
| `/ui/pipelines/_new` | Create a new pipeline (registry-driven builder or raw YAML) |
| `/ui/pipelines/:name` | View / edit / delete a pipeline; registry-driven builder (schema form or raw YAML) |
| `/ui/pipelines/:name/trigger` | `POST .../runs` and jump to run detail |
| `/ui/runs` | 100 most-recent runs; refreshes every 5s |
| `/ui/runs/:id` | Live run detail; polls every 2s until terminal; Cancel |
| `/ui/storage` | Browse Iceberg tables in the data lake |
| `/ui/components` | Component catalog: registered components, versions table, and config-schema docs |
| `/ui/settings/secrets` | Write-only project secret management (key names + timestamps; values never shown) |

### Registry-driven UI

The catalog and the pipeline builder are driven by the component registry
(the `GET /api/v1/components` endpoints below).

- **Component catalog** (`/ui/components`) lists every registered component,
  with a deprecated badge where applicable. Opening one
  (`/ui/components?name=<n>`) shows a versions table (version, image,
  prerelease flag) and the rendered config-schema documentation for one
  version — the `defaultVersion` if set, else the first non-prerelease version,
  else the first listed.
- **Pipeline builder** (`/ui/pipelines/:name`): pick a component and either
  fill a **schema-generated form** — one control per config field, derived
  from that version's `configSchema` — or **edit the YAML** directly. The form
  is a one-way emitter: **Insert as YAML** splices the generated block into the
  editor. **Two-way form↔YAML sync is intentionally not provided** — the
  **Edit as YAML…** toggle is one-way and confirm-guarded; a hand-edited block
  cannot be pulled back into the form.
- **Secret fields** — config properties flagged `x-datuplet-secret` — render as
  a picker of `$[<key>]` references. The key names come from the project's
  write-only secrets API (`GET /api/v1/projects/{pid}/secrets`); a **manage
  secrets** link points to `/ui/settings/secrets`. A plaintext secret cannot be
  entered.
- **Inputs / outputs pickers** — **input** tables are chosen from the storage
  catalog (existing lake tables); **outputs** are new tables the run creates, so
  they are entered as a bucket / optional table name / write-mode select (not
  picked from existing tables). Both fold into the emitted component block.
- The **`resources` field is YAML-only** — the builder never renders a control
  for it and never emits it. Changing any component's `resources` requires
  superadmin: on save, the diff-gate rejects (403) a non-superadmin whose save
  *adds or modifies* a `resources` block (an unchanged resubmission passes), and
  any value over the registry `resources.max` is rejected (400). (On the direct
  `kubectl apply` path the controller instead clamps over-max values to that
  ceiling — the two enforcement layers of RFC 026 §4.4.)

### Architecture notes

- **No build step.** Vanilla HTML + native ES modules. Editing the UI is one save-and-refresh.
- **Server-side routing**: `mux.Handle("GET /ui/", …)`. `pkg/pipelineapi/http/static_handler.go` serves files; missing paths fall back to `index.html` (client-side router handles shapes like `/ui/pipelines/foo`). A traversal guard rejects requests escaping the configured root.
- **Auth** is the session cookie. The UI never touches a JWT. Pages never hold a password past the login POST.
- **Polling**: `pages/runs.js` refreshes every 5s; `pages/run-detail.js` polls every 2s, auto-stops on terminal phases. Interval handles are stashed on `window.__datupletPoller`; `app.js` clears on every navigation.

### Access

- In-cluster: `http://pipeline-api.datuplet.svc.cluster.local:8081/ui/`
- OrbStack host: `http://localhost:30081/ui/` via the NodePort service.
- Production: front the pipeline-api Service with an Ingress + TLS; disable the NodePort.

### Full K8s bootstrap (OrbStack)

**Prerequisites:** OrbStack with Kubernetes enabled (the `orbstack` kubectl context must resolve). `make deploy-local` builds images straight into OrbStack's Docker daemon (no registry push) and installs the charts with `image.pullPolicy=IfNotPresent`; plain `kind` isn't supported unless you separately `kind load docker-image` every built image. MinIO's data now lives on a chart-managed PVC (`charts/datuplet-infra` `minio.persistence`, default 5Gi) rather than a host-path mount — it does not survive `make undeploy-local`.

```bash
make deploy-local
```

What it does (see the `deploy-local` / `deploy-local-helm` Makefile targets):

1. Builds every service image (`datuplet/<name>:latest`) and the built-in component images, which `build-components-local` also re-tags as `datuplet/<name>:$(COMPONENT_TAG)` (the chart's `components.tag`, e.g. `v0.8.0`) so the ComponentDefinitions resolve (`docker-build-k8s` + `build-components-local`).
2. Helm-installs the four charts in phase order, each waited on before the next: `datuplet-operators` (CRDs, RBAC, the CNPG operator), `datuplet-infra` (Postgres cluster, OpenFGA, MinIO), `datuplet-app` (pipeline-api, pipeline-operator, pipeline-observer — with `image.pullPolicy=IfNotPresent` and `components.registry=datuplet` so the built-in ComponentDefinitions point at the locally-built images), then `datuplet-lakekeeper`.
3. Runs `./scripts/register.sh --namespace datuplet`, which `kubectl exec`s into the `pipeline-api` Pod and runs five idempotent `pipeline-api admin` steps: `lakekeeper-bootstrap` (creates the warehouse + server-admin FGA tuple), `create-user` (default admin `admin@datuplet.local` / `changeme`), `create-project` (default project `default`), `attach-warehouse`, and `grant --role admin`.

`register.sh` already seeds a default admin user + project (printed at the end of its output). To create an additional user/project instead:

```bash
kubectl -n datuplet exec deploy/pipeline-api -- pipeline-api admin create-user \
  --email you@example.com --password 'CHANGEME'
kubectl -n datuplet exec deploy/pipeline-api -- pipeline-api admin create-project \
  --name demo --creator-email you@example.com
kubectl -n datuplet exec deploy/pipeline-api -- pipeline-api admin grant \
  --user you@example.com --project demo --role admin
```

Open `http://localhost:30081/ui/` and log in.

### Undeploy / redeploy

`make undeploy-local` helm-uninstalls all four charts, removes the cluster-scoped ComponentDefinitions, and deletes the `datuplet` namespace — taking the MinIO and Postgres PVCs, the signing-key Secret, and the CRD instances with it. It is a full teardown: the object store and database are wiped, so a subsequent `make deploy-local` starts from an empty data lake.

## Components endpoints

The component registry (RFC 026) backs the catalog page and the pipeline
builder. Both routes are behind `auth.WithUser` only — any authenticated user
can read the catalog (it *is* the shared component picker); there is no
project-scoped authz check. They register when the registry is wired. Handlers:
`pkg/pipelineapi/http/component_handlers.go`.

### List components

`GET /api/v1/components`

Returns the registered components (deprecated ones stay listed, flagged):

```json
[
  {
    "name": "sql-transform",
    "displayName": "SQL Transform",
    "description": "Run user SQL in an embedded DuckDB engine.",
    "deprecated": false,
    "defaultVersion": "1.2.0",
    "versions": [
      { "version": "1.2.0", "prerelease": false, "image": "ghcr.io/datuplet/sql-transform:1.2.0" }
    ]
  }
]
```

### Get one component

`GET /api/v1/components/{name}`

Same shape as a list entry, plus `configSchema` on each version — a JSON-Schema
(draft 2020-12) string the UI renders as config docs and as the
schema-generated builder form:

```json
{
  "name": "sql-transform",
  "displayName": "SQL Transform",
  "description": "Run user SQL in an embedded DuckDB engine.",
  "deprecated": false,
  "defaultVersion": "1.2.0",
  "versions": [
    {
      "version": "1.2.0",
      "prerelease": false,
      "image": "ghcr.io/datuplet/sql-transform:1.2.0",
      "configSchema": "{\"type\":\"object\",\"properties\":{ ... }}"
    }
  ]
}
```

## Storage endpoints

Browse Iceberg tables produced by pipeline runs. When `DATUPLET_LAKEKEEPER_URL` is set, storage browse routes through the lakekeeper proxy (`pkg/pipelineapi/storage/catalog_proxy.go`) using a 5-minute impersonation JWT minted per request (see `docs/auth-flow.md` Leg 4). Without it, the endpoints fall back to a directory walker (`pkg/lib/datalake`) — filesystem and MinIO warehouses both supported.

All four routes sit behind `auth.WithUser` (session-cookie auth, cluster mode) + `resolveProject()` (FGA `datuplet_member` check) before forwarding.

Identifier rules: `{ns}` and `{t}` must match `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`; `{pid}` must be a UUID. The resolved table path must not escape the warehouse root (symlink rejection, canonical containment check).

### List tables

`GET /api/v1/storage/projects/{pid}/tables`

Returns every directory under the project's `tables/` prefix that contains a valid Iceberg `metadata/` subtree. Unknown projects return 200 with an empty array.

```json
{
  "tables": [
    { "namespace": "raw", "name": "products", "current_snapshot_id": 1777057648904 }
  ]
}
```

### Table info

`GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/info`

```json
{
  "metadata_location": "s3://datuplet/…/metadata/v1.metadata.json",
  "current_snapshot_id": 1777057648904,
  "snapshots": [
    { "id": 1777057648904, "timestamp_ms": 1777057648904, "operation": "append" }
  ],
  "row_count": 10,
  "data_file_count": 1
}
```

`row_count` and `data_file_count` come from the current snapshot's summary
totals (`total-records` / `total-data-files`) — no manifest walk. Both are
`null` when the snapshot summary lacks these properties (e.g. a foreign
writer that didn't populate them).

### Table schema

`GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/schema`

```json
{
  "columns": [
    { "id": 1, "name": "id", "type": "long", "nullable": false },
    { "id": 2, "name": "name", "type": "string", "nullable": false }
  ]
}
```

### Preview

`GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/preview`

Returns up to 100 rows / 1 MiB. Sets `truncated: true` if either cap is hit.
The read runs as `SELECT * FROM lk."{ns}"."{t}" LIMIT 100` inside the
query-worker sandbox (RFC 025 §4.1) — pipeline-api never touches parquet
bytes for this endpoint. Column `type` values are DuckDB type names (e.g.
`"INTEGER"`, `"VARCHAR"`), not Iceberg/Arrow ones — a v3 change from the
prior in-process Arrow scan.

```json
{
  "columns": [{ "name": "id", "type": "INTEGER" }, { "name": "name", "type": "VARCHAR" }],
  "rows": [[1, "Laptop Pro"], [2, "Wireless Mouse"]],
  "truncated": false
}
```

Preview requires the query service (`queryWorker.enabled=true`); when it
isn't configured, the endpoint returns 501 with `{"error": "...", "kind":
"query_disabled"}`. Query-service preview errors carry this
`{error, kind}` envelope, with `kind` one of: `query_disabled` (query
service not wired), `rate_limited` (a preview is already running for this
user — the per-principal preview gate caps at 1 concurrent preview),
`capacity` (query-worker is busy), `result_too_large` (schema/rows exceed
the preview byte cap), `sql_error` (lakekeeper/DuckDB rejected the
generated statement), `timeout` (query exceeded its time budget). The
shared request prologue's own errors — invalid project id (400),
unauthenticated (401), forbidden (403), and backend-not-configured (503) —
return a bare `{"error": "..."}` without a `kind`.

### Errors

| Status | Meaning |
|--------|---------|
| 400 | Invalid project UUID, `ns`/`t` fails identifier regex, resolved path escapes the warehouse root, or (preview only) `sql_error` — lakekeeper/DuckDB rejected the statement |
| 401 | No valid session cookie (cluster mode only) |
| 403 | Not a member of the requested project |
| 404 | Table or its metadata not found |
| 408 | Preview only: `timeout` — query exceeded its time budget |
| 413 | Preview only: `result_too_large` — table too wide/large to preview |
| 429 | Preview only: `rate_limited` — a preview is already running for this user |
| 500 | Unexpected error reading warehouse or scanning table |
| 501 | Preview only: `query_disabled` — query service not configured |
| 503 | Storage service disabled (missing env config), OpenFGA unreachable, or (preview only) query-worker `capacity` |

## Observer + reaper split

The pipeline-observer process runs as a separate Deployment (single replica). It hosts a PipelineRun controller backed by an in-memory coalesce layer (`coalesce.go`), and the reaper runs as its own `pipeline-api-reaper` CronJob.

**Observer.** One `manager.Manager` hosts a PipelineRun controller filtered by the `datuplet.io/run-id` label. On every change the reconciler parses `metadata.resourceVersion`, builds a `RunStatus`, and hands it to the coalesce decorator. Coalesce drops identity writes. Writes that make it through hit `store.UpdateRunPhase`, gated on `($rv = 0 OR $rv > observed_rv)`. The DELETE event handler calls `coalesce.Forget(key)` synchronously.

`runServe` blocks on `obs.WaitForCacheSync(ctx)` (2-minute timeout) before opening the HTTP listener.

**Reaper CronJob.** `pipeline-api reap-once` is a subcommand that opens the DB pool + in-cluster client, runs `k8s.ReapOnce`, and exits. It exits with code 2 on schema-version mismatch. The CronJob (`charts/datuplet-app/templates/reaper/cronjob.yaml`) runs every 30 minutes with `concurrencyPolicy: Forbid`. `ReapOnce` deletes stale PipelineRuns (listed cluster-wide via the ClusterRole) and sweeps orphaned run Secrets by iterating project namespaces (`namespaces: list`). Secret deletion needs per-project-namespace Secret access, which the Helm chart provides by running the reaper under the `pipeline-api` ServiceAccount — project provisioning binds that SA to the per-namespace `datuplet-secrets` Role (RFC 026 §4.9; no cluster-wide Secret verbs).

**Deploy order.** Always roll pipeline-api first (it owns migrations), then apply the CronJob. On rollback, suspend the CronJob first:

```bash
kubectl -n datuplet patch cronjob pipeline-api-reaper \
  -p '{"spec":{"suspend":true}}'
# …roll pipeline-api back…
kubectl -n datuplet patch cronjob pipeline-api-reaper \
  -p '{"spec":{"suspend":false}}'
```

**Metrics.** `/metrics` on :8081:

- `pipelineapi_reconcile_events_total`
- `pipelineapi_db_updates_total{outcome=applied|coalesced|stale|error}`
- `pipelineapi_informer_cache_size`
- `pipelineapi_reconcile_duration_seconds`

## What's NOT here yet

- Secrets management REST API (placeholder kubectl recipe in the UI)
- YAML editor with syntax highlighting (plain textarea for MVP)
- NetworkPolicy isolating per-project namespaces
- Per-project S3 buckets (single shared bucket today)

See `docs/auth-flow.md` for the current token lifecycle.
