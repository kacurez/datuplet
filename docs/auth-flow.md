# Auth-token lifecycle — Datuplet pipeline-api

This document walks the end-to-end token lifecycle from a user creating a
pipeline through data being committed to a table. Each section covers one
*leg* of the journey: who acts, what token they carry, where the trust
boundary sits, which FGA relation is checked, and what can go wrong.

For the FGA model that governs the grant chains described here, see
[`pkg/pipelineapi/authz/fga_model.fga`](../pkg/pipelineapi/authz/fga_model.fga).

---

## Background — identities and objects

Datuplet manages two classes of identity:

- **Human users** — Alice, Bob. They authenticate to pipeline-api with a
  session cookie (cluster) or are the single hard-coded user (local mode).
  Their UUID is stored in the `users` table.
- **Synthetic run users** — one ephemeral identity per pipeline run. Its
  UUID is the run UUID. It never signs in; its only credential is the
  run-token JWT minted at trigger time.

Both classes carry the `oidc~` prefix when pipeline-api writes FGA tuples —
matching the normalization lakekeeper hard-codes for OIDC subjects. Example:

```
user:oidc~<alice-uuid>    (Alice's FGA identity)
user:oidc~<run-uuid>      (synthetic run identity)
```

Every FGA grant targets a *lakekeeper Project UUID* (`project:<lkP>`), **not**
the Datuplet project UUID. The mapping is stored in `projects.lakekeeper_project_id`.

---

## Leg 1 — Pipeline creation

`PUT /api/v1/projects/{P}/pipelines/{name}`

```
Alice (browser)                         pipeline-api
       |                                     |
       |-- PUT /projects/{P}/pipelines/foo   |
       |   Cookie: session=<opaque>          |
       |                                     |
       |   auth.WithUser resolves cookie --> user record
       |                                     |
       |   mustHaveRelation("data_admin")    |
       |           |--> OpenFGA Check -----> FGA store
       |           |    (user:oidc~<alice>,  |
       |           |     data_admin,         |
       |           |     project:<lkP>)      |
       |           |<-- allowed=true --------+
       |                                     |
       |<-- 200 (pipeline stored) -----------|
```

**Actor:** Alice. In cluster mode her identity is the session cookie resolved
by `auth.WithUser` middleware. In local mode the server hard-codes a single
user (no login).

**Token shape:** HTTP session cookie (cluster) or identity derived from
`pkg/pipelineapi/localmode` constants (local). No JWT on this leg.

**Trust boundary:** `mustHaveRelation` in `pkg/pipelineapi/http/pipeline_handlers.go`.
It parses `{P}`, fetches `projects.lakekeeper_project_id`, then calls
`authzr.Check(user, "data_admin", authz.ProjectObject(lkP))`.

**FGA check:** `(user:oidc~<alice>, data_admin, project:<lkP>)`.
The `editor` leaf tuple written at project grant time chains into `data_admin`
through the lakekeeper FGA model union: `editor → project_admin → data_admin`.
A `viewer` tuple does NOT satisfy `data_admin` — only `describe`.

**Result:** Pipeline YAML stored in Postgres (`pipelines` table) in cluster
mode, or written to `<dir>/pipelines/<name>.yaml` in local mode.

**Failure modes:**

| Status | Cause |
|--------|-------|
| 401 | No session cookie or expired session |
| 403 | Alice has only `viewer` — `data_admin` check returns false |
| 404 | Project not found in `projects` table |
| 503 | OpenFGA unreachable (`authz.ErrAuthzUnavailable`) |
| 503 | `lakekeeper_project_id` not yet populated for this project |

---

## Leg 2 — Pipeline trigger

`POST /api/v1/projects/{P}/pipelines/{name}/runs`

```
Alice                         pipeline-api                    Postgres / K8s / OpenFGA
  |                               |                                  |
  |-- POST .../runs               |                                  |
  |   Cookie: session=<opaque>    |                                  |
  |                               |                                  |
  |   mustHaveRelation("data_admin") --> FGA Check(alice, data_admin)
  |                               |<-- allowed                       |
  |                               |                                  |
  |   Step 1: INSERT run_tuples (committed=false) ------------------>|
  |   Step 2: WriteTuples(oidc~<run-uuid>, editor, project:<lkP>) -->|
  |   Step 3: INSERT runs row + create K8s Secret + PipelineRun CR ->|
  |       + MintRunToken(sub=<run-uuid>, aud=datuplet-catalog, 24h)  |
  |   Step 4: UPDATE run_tuples (committed=true) ------------------->|
  |                               |                                  |
  |<-- 202 {run_id: <uuid>} ------|                                  |
```

**Actor:** Alice (same as Leg 1).

**Token shape:** Session cookie inbound. The outbound run-token JWT carries:
- `sub = <run-uuid>` (raw UUID; lakekeeper prepends `oidc~` when normalising)
- `actor = <alice-uuid>` (the triggering user; derived from ctx by
  `tokens.MintRunToken` — never passed as a parameter)
- `iss = datuplet-api`
- `aud = datuplet-catalog`
- `exp = now + 24h`

**Trust boundary:** same `mustHaveRelation` as Leg 1, then the four-step
compensating write in `K8sBackend.TriggerRun`
(`pkg/pipelineapi/runbackend/k8s.go`).

**FGA check (pipeline-api side):** same `data_admin` on `project:<lkP>` for
Alice.

**Compensating write ordering (crash safety):**

The four steps are NOT an ACID transaction — OpenFGA is an HTTP service, not
a Postgres participant. The ordering is:

1. `INSERT run_tuples (committed=false)` — recovery breadcrumb before any
   side-effects. The reaper uses this row to sweep orphaned tuples.
2. `authzr.WriteTuples(...)` — grants the synthetic run identity `editor` on
   `project:<lkP>`. From this point the run user can call lakekeeper.
3. `INSERT runs row` + mint run JWT + create K8s Secret + PipelineRun CR. If
   this fails, the reaper sweeps the committed=false row and the dangling FGA
   tuple at age > 30 min.
4. `UPDATE run_tuples (committed=true)` — observability marker; best-effort.
   The reaper self-heals if this step is missed.

**Failure modes:**

| Stage | Failure | Recovery |
|-------|---------|----------|
| Step 1 | Postgres down | Returns error to Alice; no side-effects |
| Step 2 | OpenFGA down | Deletes the run_tuples row; returns error |
| Step 3 | K8s API down | `MarkFailed` flips the run to FailedApplication; FGA tuple + run_tuples row stranded until reaper sweeps |
| Step 4 | Postgres flake | Non-fatal log; reaper bumps committed=true on next sweep |

---

## Leg 3 — Run executes (DG sidecar)

```
DG sidecar (run identity)             lakekeeper REST          OpenFGA (lakekeeper-embedded)
       |                                    |                         |
       | LoadOrCreateForWrite("raw","tbl")  |                         |
       |-- Authorization: Bearer <run-jwt> ->                         |
       |                                    | verify JWT:             |
       |                                    |  sig + iss + aud + exp  |
       |                                    |-- FGA Check ----------> |
       |                                    |   (oidc~<run>,          |
       |                                    |    can_write_data,      |
       |                                    |    table:<uuid>)        |
       |                                    |<-- allowed ------------ |
       |<-- Table.Location() + /credentials |                         |
       |    STS creds scoped to table prefix|                         |
       |                                    |                         |
       | write parquet via STS creds        |                         |
       | write files.json via STS creds     |                         |
```

**Actor:** Synthetic run user (`user:oidc~<run-uuid>`).

**Token shape:** The run-token JWT is mounted at
`/var/run/secrets/datuplet-runtoken/token` on the DG sidecar container only
(never on the component container). The DG sidecar reads it at startup and
validates it against pipeline-api's JWKS before proceeding.

**Trust boundary:** Lakekeeper validates the JWT locally using a cached
public key polled from pipeline-api's JWKS endpoint:
`${PIPELINE_API_PUBLIC_URL}/api/v1/auth/jwks.json`
(discovery doc at `/.well-known/openid-configuration`).
Lakekeeper verifies: RS256 signature + `iss=datuplet-api` (allow-listed via
`LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS`) + `aud=datuplet-catalog` + `exp`.

**FGA check (lakekeeper side):** The grant chain from Leg 2's
`WriteTuples` call resolves at table level:
```
(user:oidc~<run>, editor, project:<lkP>)
  → data_admin on project
  → modify on namespaces owned by lkP
  → modify on tables in those namespaces
  → can_write_data on table:<uuid>
```
Readers (read-only pipeline stages) would have a `viewer` tuple, which chains
to `can_read_data`.

**Vended credentials:** Lakekeeper's `/v1/credentials` returns STS credentials
scoped to the table's storage prefix (`<warehouse>/<storage-uuid>/<table-uuid>/`).
`pkg/catalogwriter.VendedCreds` caches them and renews when 50% of the issued
TTL has elapsed (hard floor: 60 s between renewals). DG holds zero long-lived
MinIO/S3 credentials on the data path.

**Failure modes:**

| Cause | DG behaviour |
|-------|-------------|
| JWT expired (> 24h) | lakekeeper returns 401; DG fails the writer RPC |
| FGA denies (cancel in progress) | lakekeeper returns 403; DG fails within ≤15 s of cancel |
| Vended-creds renewal failure | Cached creds keep working until expiry, then S3 writes fail with 403 |
| lakekeeper unreachable | DG fails the writer RPC with retryable error |

---

## Leg 4 — Browse data (storage proxy)

`GET /api/v1/storage/projects/{P}/tables`
`GET /api/v1/storage/projects/{P}/tables/{ns}/{tbl}/preview`

```
Alice (browser)                pipeline-api storage proxy          lakekeeper
       |                               |                               |
       |-- GET /storage/projects/{P}/tables                           |
       |   Cookie: session=<opaque>    |                               |
       |                               |                               |
       |  resolveProject(): FGA Check(alice, datuplet_member, project:<lkP>)
       |                               |<-- allowed                    |
       |                               |                               |
       |   MintImpersonation(ctx)      |                               |
       |   sub=<alice-uuid>, 60 sec    |                               |
       |                               |                               |
       |                               |-- GET /v1/namespaces          |
       |                               |   Authorization: Bearer <imp-jwt>
       |                               |   x-project-id: <lkP>        |
       |                               |                               |
       |                               |  lakekeeper: FGA Check(       |
       |                               |    oidc~<alice>,              |
       |                               |    describe, project:<lkP>)   |
       |                               |<-- 200 + namespace list ------|
       |<-- 200 {tables: [...]} -------|                               |
```

**Actor:** Alice (NOT the synthetic run identity).

**Token shape — inbound:** Session cookie, resolved by `auth.WithUser`.

**Token shape — to lakekeeper:** `MintImpersonation(ctx, signer)` in
`pkg/pipelineapi/tokens` produces a short-lived impersonation JWT:
- `sub = <alice-uuid>` (raw UUID; lakekeeper normalises to `oidc~<alice>`)
- `actor = <alice-uuid>` (same — Alice is acting on her own behalf)
- `iss = datuplet-api`
- `aud = datuplet-catalog`
- `exp = now + 60 sec` (`tokens.ImpersonationLifetime`)

`ImpersonationToken` is a redacting wrapper — `String()` and `%v` return
`[REDACTED]`; the raw JWT is only accessible via `tok.Reveal()`.

**Trust boundary (pipeline-api side):** `resolveProject()` in
`pkg/pipelineapi/storage/handlers.go` calls:
```
authzr.Check(user, "datuplet_member", authz.ProjectObject(lkP))
```
This is a broad membership gate — it blocks users who have no relation at all
on the project from reaching lakekeeper. `datuplet_member` is satisfied by
any of `viewer`, `editor`, `project_admin`.

**Trust boundary (lakekeeper side):** Lakekeeper validates the impersonation
JWT the same way it validates run-token JWTs (JWKS + iss + aud + exp), then
runs its own FGA check: `describe` on `project:<lkP>`. This means the check
is done twice — first by pipeline-api (`datuplet_member`) and then by
lakekeeper (`describe`). They agree because both chain from Alice's `viewer`
or `editor` tuple.

**`x-project-id` header:** Every catalog request from the proxy carries
`x-project-id: <lkP>`. Without this header lakekeeper applies grants on its
*default* project rather than the Datuplet-owned lakekeeper Project. The
header is how per-project FGA isolation is achieved.

**Failure modes:**

| Cause | Response |
|-------|----------|
| Alice has no relation on `lkP` | 403 from `datuplet_member` check |
| OpenFGA unreachable | 503 from `resolveProject` |
| Impersonation JWT expired (60 sec) | lakekeeper returns 401; unlikely due to short hop |
| lakekeeper 403 (FGA mismatch) | proxy surfaces 403 to Alice |

---

## Leg 5 — Run completion and cancel

### Normal completion

On successful run completion the pipeline-api observer transitions the run to
`Succeeded` and `K8sBackend.CompleteRun` cleans up:

1. `authzr.DeleteTuples(ctx, tuples)` — revoke the synthetic run user's FGA grant.
2. `store.DeleteRunTuples(ctx, db, runID)` — delete the recovery breadcrumb.
3. Update `runs.phase = Succeeded` in Postgres.

### Cancel path

```
Alice                    pipeline-api                      K8s / OpenFGA
  |                           |                                |
  |-- POST .../runs/{id}/cancel                               |
  |   Cookie: session=<opaque>|                               |
  |                           |-- terminal-phase guard -----> Postgres
  |                           |   (idempotent — skip if already terminal)
  |                           |                               |
  |                           |-- authzr.DeleteTuples() ---> OpenFGA
  |                           |   (FIRST — before K8s delete) |
  |                           |                               |
  |                           |-- annotate pods cancel=true ->K8s
  |                           |-- delete PipelineRun CR ----->K8s
  |                           |-- UPDATE runs.phase=Cancelled >Postgres
  |                           |-- DELETE run_tuples row ----->Postgres
  |                           |                               |
  |<-- 200 OK ----------------|                               |
```

**Why FGA delete is first:** DG polls lakekeeper every ~5–15 s (STS renewal
cadence + catalog REST polls). After `DeleteTuples`, lakekeeper's FGA check
for the synthetic run user returns 403. The DG sidecar's next lakekeeper call
fails, it exits on the cancel signal, and any in-progress parquet write is
aborted. **Blast radius ≤ 15 s** from cancel to DG stopping new storage I/O.

**Race conditions (idempotent):**

- `DeleteTuples` on a tuple that was already deleted: `authz.isMissingTupleErr`
  swallows the "tuple not found" error — safe to re-run.
- Reaper picks up a terminal run with leftover tuples: same `DeleteTuples`
  call, same swallowed error. The metric
  `pipelineapi_reaper_run_tuples_terminal_with_tuples_total` increments when
  this happens — a non-zero value indicates a primary-path cancel didn't clean
  up and the reaper saved the day.
- Pipeline-api crash mid-cancel (after `DeleteTuples` but before phase update):
  The reaper's 5-minute sweep sees the run in a terminal-but-uncommitted state
  and completes the cleanup.

**Failure modes:**

| Stage | Failure | Recovery |
|-------|---------|----------|
| `DeleteTuples` fails | Log + continue; tuples remain active | Reaper sweeps within 5 min |
| K8s delete fails | Run stuck in `Cancelling` | Reaper + observer reconcile |
| Postgres update fails | `runs.phase` stale | Observer's next reconcile corrects it |

---

## Trust boundaries — at-a-glance

| Boundary | Verifier | Identity carrier | Check | Failure mode |
|----------|----------|-----------------|-------|--------------|
| pipeline-api ingress (browser) | `auth.WithUser` — session cookie or local-mode hardcode | Cookie / local constant | `mustHaveRelation` → FGA Check via `authzr` | 401 no session / 403 FGA denied |
| pipeline-api storage proxy | `resolveProject()` — same session | Cookie | `datuplet_member` on `project:<lkP>` | 403 FGA denied / 503 OpenFGA down |
| lakekeeper REST (run identity) | JWKS-cached pubkey (`datuplet-api` allow-listed issuer) | Run JWT `sub=<run-uuid>` | `can_write_data` / `can_read_data` — chains from `editor`/`viewer` on `project:<lkP>` | 401 expired / 403 FGA denied (cancel) |
| lakekeeper REST (storage proxy) | Same JWKS | Impersonation JWT `sub=<alice-uuid>` | `describe` on `project:<lkP>` | 401 expired / 403 FGA denied |
| OpenFGA | No separate auth — lakekeeper gates all calls | Tuple inputs | Tuple-graph reachability | n/a — lakekeeper is the only caller |
| S3 / MinIO data path | STS policy attached to vended creds | STS credentials (scoped to `<table-uuid>/`) | S3 bucket policy on path prefix | 403 stale creds (renewed at 50% TTL) |

---

## Where to read more

- **[`pkg/pipelineapi/authz/fga_model.fga`](../pkg/pipelineapi/authz/fga_model.fga)** —
  the OpenFGA DSL model showing how `editor → data_admin → can_write_data` etc.
  chain.
- **`pkg/pipelineapi/runbackend/k8s.go::TriggerRun`** — the four-step
  compensating write implementation, including step comments and reaper
  invariants.
- **`pkg/pipelineapi/storage/handlers.go::resolveProject`** — `datuplet_member`
  check + `x-project-id` plumbing for the storage browse leg.
- **`pkg/pipelineapi/tokens/mint.go`** — `MintRunToken` + `MintImpersonation`
  source, including the redacting `ImpersonationToken` wrapper.
