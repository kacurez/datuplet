# RFC 028 — User Apps: Serverless Dashboards on a WASM Worker Pool

**Status:** Draft v10 — further plan-review reconciliations (Codex round 6,
2026-07-24): (1) §5.4 + §5.2 now keep the **JWT `sub`** and the **OpenFGA
subject** distinct (the impersonation token's `sub` is the app's raw JWT
subject; the tuple/authorization target is `user:oidc~app-<uuid>`) — the
prior single `user:oidc~app-<uuid>` "subject" wording risked a double-prefix
the implementation plan explicitly guards against; (2) the §12 resolved
note now records **bundle upload format = JSON+base64**. Draft v9 (Codex round 2): two spec/plan
conflicts reconciled toward the implementation decision — (a) §6.2 +
Appendix A: authors still write an ES module, but the CLI delivers it as an
esbuild **IIFE** (`--global-name=__dtp_app`) that the QuickJS engine evals,
avoiding module-namespace export plumbing in C; (b) §5.5: CLI project
resolution uses the existing `--project` / `DATUPLET_PROJECT` /
`cluster.json` chain rather than a new `config set project` command. (The
`Date`/real-clock item was reconciled on the plan side — spec §6.2 was
already correct.) Draft v8 history: external design
review round 6 folded in (Codex, GPT-5 Codex, high; all 6 tracked
round-5/carryover items resolved; 1 new minor): the render principal
taxonomy generalized — `principal_kind: viewer_token | platform_user`, where
`platform_user` covers both bearer (CLI) and session (UI `@draft` preview)
author renders with one `(app_id, user_id)` rate key (§5.3/§7/§9). Round-5
history (Draft v7): render-log records carry `request_id`
with a lookup route (§5.1/§6.6), single machine-readable `apps render -o
json` failure payload (§5.5), multi-bucket `Retry-After` (§5.3), render
access log gains `principal_kind`/`principal_id` (§9), §9 wording
"per-principal". Round-4 history (Draft v6): author-supplied Vega
`config` **rejected entirely in v1** (closes the R2-9/R3-5 allowlist
partial for good, §6.4), explicit render response matrix (§6.3), CLI
diagnostics via `apps logs --request-id` (§5.5), bearer-render rate-limit
key (§5.3/§7), `Retry-After` calculation (§5.3), safe-integer rationale
(§6.1), CSP framing wording (§6.4). Round-3 history (Draft v5): shell CSP allows same-origin framing for
the draft preview (§6.4), author/CLI JSON render mode + bearer-auth CSRF
exemption (§4.2/§5.3/§5.5), uniform CLI project resolution (§5.5), worked
example fixed for the unreferenced-param rule (Appendix A), Vega-Lite
allowlists fully enumerated + `usermeta` rejected (§6.4, closes the R2-9
partial), `Referrer-Policy`/`Referer` redaction on all `/apps/*` responses
and logs (§5.3), concrete per-token/per-app render rate limits (§5.3/§7),
single numeric-bind precision rule (§6.1). Round-2 history (Draft v4): round 2 verified 29/31 round-1 findings
resolved and raised 11 new findings (1 blocker, 9 majors, 1 minor) + 2
round-1 partials — all accepted and folded: draft-preview session
verification (§5.2/§5.3), resolve-before-authenticate flow order (§4.2),
cookie↔app binding (§5.3), token log-redaction (§5.3), concrete
verify-cache/rate-limit values (§5.3), JSON→DuckDB bind-type table +
placeholder grammar (§6.1), sub-second timeout + cancellation propagation
(§6.2), POST body caps (§6.5), enumerated Vega-Lite subset (§6.4),
`bad_request` error kind (§8), CSV formula-injection escaping (§6.3).
Round-1 history (Draft v3): 31 findings (3 blockers, 24 majors, 4 minors),
all accepted; blocker resolutions: **bound parameters added to the
query-service contract** (§6.1, maintainer decision), viewer cookie design
fully specified (§5.3), Vega-Lite network lockdown + CSP (§6.4).
Post-round-2 maintainer additions: authoring surfaces — management UI +
agent-first CLI (R6, §5.5) — and the **POC greenfield posture** (§2): build
as if from scratch, no data migration, no back-compat obligations. Draft v2 history: brainstorm output with the maintainer
(2026-07-22); approach and all design sections approved in-session; v2
folded the maintainer Q&A round (shell interactivity, tabs, Vega-Lite-first,
draft/promote channels, restart/rollout, scaling, PDF-via-print-CSS, worked
example, engine alternates). Custom styling/CSS was discussed and
deliberately **not** specced (exploratory only). No code written yet. Next
gate: maintainer spec review → implementation plan.

**Scope:** Let platform users publish their own read-only dashboard apps
(charts, tables, metrics, filters) over Datuplet data — without a dedicated
always-on pod per app. User JS runs server-side inside per-request WASM
sandboxes (wazero + QuickJS) on a shared, horizontally scalable worker pool;
data flows exclusively through the server query service; output is a
declarative block document rendered by a trusted browser shell.

**Builds on:** RFC 022/025 (server query service + query-worker data plane)
— **with one contract extension**: the query endpoint gains optional bound
parameters (§6.1). Also reuses the synthetic-identity + FGA-tuple pattern
from run tokens and the 60 s impersonation-JWT path used by interactive
storage browse (`tokens.MintImpersonation`); neither changes.

---

## 1. Problem

Users want custom dashboards over warehouse data ("show me a graph of X,
filterable by Y"). The naive hosting model — one Streamlit/Node pod per app —
has the wrong economics and the wrong shape:

- **Idle cost per app.** A dashboard must be reachable at any time, so its pod
  never scales to zero. Ten dashboards ≈ ten always-on pods.
- **Not replicable.** Session-oriented app servers (Streamlit's websocket
  model) pin viewers to one pod; the pod is a pet, not cattle.
- **Trust.** The pod runs arbitrary user code with whatever network/creds the
  namespace gives it.

## 2. Requirements (locked with maintainer)

| # | Decision | Consequence |
|---|---|---|
| R1 | **Read-only dashboards** in v1: every interaction is request/response re-render. No server-side session state. | Workers can be fully stateless. |
| R2 | **Viewers are not necessarily Datuplet users.** v1 auth = opaque per-app tokens verified at the serving edge; basic auth / SSO later at the same choke point. | The **app** carries the data grant, not the viewer. |
| R3 | **JS/TS authoring** in v1. | Mature JS-in-WASM engines exist; Python deferred. |
| R4 | **Data exclusively via the server query service** (`POST /api/v1/projects/{pid}/query`), within its caps (10 k rows / 10 MiB / 300 s) and extended with bound parameters (§6.1). | No new data plane; apps hold zero storage credentials. User apps **hard-depend** on the query service (§8). |
| R5 | **Server-side execution only.** App source, SQL, and schema details never ship to viewers. | Browser-side execution (stlite-style) is ruled out. |
| R6 | **Two first-class authoring surfaces:** a management UI for humans and an agent-operable CLI — an agent must be able to implement, deploy, test, and promote an app end-to-end via `datuplet` with no interactive step (RFC 027 posture). | §5.5: UI + CLI contract, JSON in/out, headless auth, structured errors. |

**Posture (maintainer decision):** POC greenfield — design and build as if
from scratch. No data migration, no back-compat shims, no legacy formats;
new tables and contracts are created fresh. (The §6.1 query-service `params`
extension is additive regardless.)

## 3. Approaches considered

**A — WASM worker pool (chosen).** Shared stateless `app-worker` Deployment;
per-request instantiation of a JS engine compiled to WASM, executed under
wazero (pure Go, no cgo). Strongest practical sandbox (capability-based: the
guest gets exactly the host functions we export, nothing else), ~ms
instantiation, marginal cost per app ≈ zero, no new infra dependencies.
Trade-off: no JIT — engine-in-WASM JS is ~10–50× slower than V8, acceptable
because heavy lifting stays in DuckDB (R4); npm support is "pure-JS libraries
via esbuild bundle", no Node APIs.

**B — V8/Deno isolate pool (named fallback).** Same pool shape, real V8 speed.
Rejected for v1: weaker isolation boundary (isolate escape = process
compromise → would re-add gVisor/seccomp hardening), `v8go` drags cgo and has
a spotty maintenance history, Deno subprocesses cost ~30–50 ms + a process
table. Revisit if the engine spike (§12) shows QuickJS perf/DX is unacceptable.

**C — Scale-to-zero per-app pods (KEDA HTTP add-on / Knative).** Rejected for
v1: 2–10 s cold starts per idle dashboard, a per-app image build pipeline,
K8s object churn per app, and a heavyweight dependency injected into the
intentionally lean four-chart install. Remains the natural later path for
"pro apps" (arbitrary containers, native deps, stateful frameworks) and does
not conflict with A.

## 4. Architecture

New pieces mirror the existing control-plane/data-plane split
(pipeline-api ↔ query-worker):

- **`app-worker`** — new stateless Deployment; `cmd/app-worker` +
  `pkg/appengine/` in the root Go module (wazero is pure Go — unlike
  `components/queryengine` there is no cgo reason for a separate module).
  Replicas are identical; any replica serves any app; HPA on CPU. Idle
  platform footprint: 1–2 small pods **total**.
- **App bundles in Postgres, served by pipeline-api.** An app is a single
  esbuild-bundled ES module (≤ 5 MB) plus a manifest row. Bundles are
  immutable, content-addressed versions (SHA-256), stored compressed;
  workers fetch via the internal API (§5.2) and cache by content hash,
  verifying the hash on fetch. app-worker therefore holds **zero
  object-storage credentials** (credentials-clean pattern, as with
  `sql-transform`). Storage policy: per-app retained-version cap (default
  20; oldest unreferenced versions GC'd) and per-project bundle quota
  (default 200 MB), both operator-tunable.
- **Trusted viewer shell** — static, platform-owned HTML/JS (vanilla ES
  modules or Preact+htm — both build-step-free, `ui/product` style) that
  renders the app's declarative output in the viewer's browser. The shell is
  a block-renderer registry over a closed vocabulary (§6.3), with Vega-Lite
  and DOMPurify/marked vendored. The shell page is served with a strict CSP
  (§6.4). Apps never emit executable JS to viewers. The pattern is
  server-driven UI: the server returns a typed block document, not HTML; one
  shared shell owns all client behavior.
- **Engine**: one shared `engine.wasm` (QuickJS baseline; see §12) compiled
  once per pod via wazero's compilation cache; instantiated fresh per request
  (~ms); the instance evals the app bundle, calls `render`, and is discarded.

### 4.1 Identifiers & routing

- App `name` **is** the slug: DNS-label rules
  (`[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`), unique per project, validated at
  `PUT` (consistent with the storage UI's strict-identifier posture).
- Public routes use the project UUID: `/apps/{pid}/{name}` — unambiguous and
  consistent with `{pid}` in the API routes (`docs/ad-hoc-query.md`). Pretty
  per-project aliases are a future nicety, not v1.
- **Routing shape (resolved):** path prefix `/apps/…` on the existing
  ingress, no new hostname/cert surface. Viewer cookies are `Path`-scoped
  per app (§5.3) and the shell carries its own CSP, which together address
  the isolation concerns that motivated a dedicated host; a dedicated
  `apps.<domain>` remains an optional hardening for origin-level isolation
  from `/ui`, documented but not default.

### 4.2 Render flow

Route: `GET/POST /apps/{pid}/{name}` on the ingress → app-worker Service.

1. app-worker **resolves first**: `(pid, name, channel)` → `{app_id,
   version_hash}` via the internal resolve endpoint (§5.2), cached ≤15 s.
   (Resolve must precede authentication — token verification is keyed by
   the resolved `app_id`.)
2. app-worker authenticates the **viewer against the resolved app** (§5.3):
   session cookie (whose `app_id` must equal the resolved one), one-time
   `?token=` exchange → cookie + redirect, or — for `@draft` — platform-
   session verification (§5.2). Then fetches the bundle from the local
   content-hash cache (else internal API, hash-verified).
3. Instantiates `engine.wasm`, evals the bundle, calls exported
   `render({path, params})` under the render deadline (§7).
4. Guest calls the **only host capability**: `query(sql, params, opts)` →
   app-worker forwards to `POST /api/v1/projects/{pid}/query` (bound params,
   §6.1) under the app's impersonation JWT (§5.4). No sockets, no fs, no
   env.
5. Guest returns a JSON **OutputDoc** (§6.2). Worker discards the instance,
   validates the OutputDoc against the versioned schema, and responds per
   the **response matrix** (normative):

   | Request | Response |
   |---|---|
   | navigation (no `Accept: application/json`) | shell HTML embedding the full OutputDoc; a `block` param is ignored |
   | `Accept: application/json` | full OutputDoc JSON (shell full re-render; author/CLI mode, §5.5) |
   | `Accept: application/json` + `block=<id>` | that single block as JSON; unknown id → 400 `bad_request` |
   | any error | §8 envelope — JSON for JSON requests, minimal HTML page otherwise |
6. "Dynamic graphs": a filter change POSTs new params to the same endpoint →
   full stateless re-render. No websockets, no server sessions (R1).

Cost model: an idle app costs a Postgres row. Nothing app-specific lives in
any pod, so the original "the pod cannot be replicated and must stay up"
problem dissolves.

## 5. Control plane, identity, viewer auth

### 5.1 Author-facing control plane (pipeline-api)

Author routes are governed by normal pipeline-api auth + project membership:

- `PUT /api/v1/projects/{pid}/apps/{name}` — upsert manifest + bundle.
  Bundles are immutable, content-addressed versions; the upload moves the
  app's **`draft` channel pointer** to the new version. Production is never
  touched by an upload. Response includes the version's content hash.
- `POST …/apps/{name}/promote` — body `{"version": "<content-hash>",
  "expectedProduction": "<hash|null>"}`. Atomically repoints **`production`**
  to the named version; `expectedProduction` (optional) is a compare-and-swap
  precondition — mismatch → 409. Promoting "whatever draft is now" without
  naming a version is deliberately not supported (concurrent-upload
  footgun). Rollback is the same call naming a retained prior version.
- `GET` / `DELETE` / list; `GET …/apps/{name}/logs[?request_id=<id>]` —
  author-facing render logs (§6.6); with `request_id`, returns exactly that
  render's record, or 404 when no record with that `request_id` exists
  (expired from the ring buffer or never existed).
- `POST …/apps/{name}/tokens`, `DELETE …/tokens/{id}` — viewer-token
  management (§5.3). Viewer tokens bind to the **app**, not a version — they
  keep working across promotes.
- Authoring surfaces — the CLI and the management UI — are first-class and
  specified in §5.5.

**Channels & preview.** `/apps/{pid}/{name}` serves `production`;
`/apps/{pid}/{name}@draft` serves the draft and requires an authenticated
**platform session** (project member, verified via the `sessions/verify`
internal endpoint, §5.2) — never a viewer token. Both channels
run under the same read-only app identity, so a broken draft can do nothing
production couldn't. **Promote is eventually consistent:** workers cache
slug→version resolution for ≤15 s, so replicas may serve mixed versions for
up to that window after a promote/rollback; the promote response documents
this. (Versions are immutable and any version is safe to serve, so the race
is cosmetic; active invalidation is a non-goal for v1.)

### 5.2 Internal API (pipeline-api ↔ app-worker)

app-worker authenticates with one platform **service credential** (K8s
Secret) whose power is exactly these six endpoints:

| Endpoint | Semantics |
|---|---|
| `GET /internal/v1/apps/{pid}/{name}/resolve?channel=production\|draft` | → `{app_id, version_hash}`; 404 unknown app/channel. Worker-cached ≤15 s. |
| `GET /internal/v1/bundles/{hash}` | → bundle bytes; content-addressed, immutable (`Cache-Control: immutable`); worker verifies SHA-256 = `{hash}` on receipt. |
| `POST /internal/v1/viewer-tokens/verify` | body `{app_id, token_id, secret}` → `{ok}` (constant-time compare against the salted hash for that `(app_id, token_id)` row); also used for revocation re-checks by `(app_id, token_id)`. |
| `POST /internal/v1/sessions/verify` | body `{pid}` + the forwarded platform session credential (cookie/bearer) → `{user_id, project_member: bool}`. **This is how app-worker authorizes `@draft` previews** — it holds no session-validation logic itself. Result cached ≤15 s per session. |
| `POST /internal/v1/impersonate` | body `{app_id}` → 60 s impersonation JWT. **The subject is derived server-side** from the app row — the JWT `sub` is the app's raw JWT subject, whose authorization maps to the app's FGA subject (`user:oidc~app-<uuid>`), the two kept distinct per §5.4; the worker cannot name an arbitrary identity. Every mint is audited (service principal, app, jti). |
| `POST /internal/v1/apps/{app_id}/logs` | append a render-log record (size-capped, §6.6). |

All internal endpoints return the standard error envelope; 401/403 on a bad
or out-of-scope service credential.

### 5.3 Viewer auth (v1 = tokens, per R2)

**Token format & storage.** A viewer token is `vw_<token_id>.<secret>` —
`token_id` is a lookup key; `secret` is ~32 bytes from `crypto/rand`,
base64url. pipeline-api stores only `hash(salt + secret)` per row; the
plaintext is returned exactly once at mint. Opaque by design: no claims, no
signing keys, meaningless without its row, instantly revocable (delete the
row).

**One-time exchange.** The viewer opens `/apps/{pid}/{name}?token=…` once.
app-worker verifies via §5.2, then:

1. Sets a **signed session cookie** — HMAC over `{app_id, token_id, exp}`
   (key from the platform Secret; ~24 h expiry) with attributes `HttpOnly;
   Secure; SameSite=Lax; Path=/apps/{pid}/{name}`.
2. **302-redirects to the token-free URL** so the plaintext leaves the
   address bar, history, and logs immediately.
3. The shell page sets `Referrer-Policy: no-referrer` as belt-and-braces.

Subsequent requests are cookie-only: local signature check, **a mandatory
`cookie.app_id == resolved app_id` comparison** (path scoping is a
convenience, never the authorization control), and a revocation check keyed
by `(app_id, token_id)` against a ≤15 s positive cache — so deleting a
viewer token kills its sessions within ≤15 s despite the cookie being
otherwise stateless. Expired cookie → the viewer re-opens their original
tokened link.

**Token log-redaction.** The exchange request is the one place the plaintext
transits a URL, so it must never reach an access log: the ingress config
shipped by the chart drops/redacts query strings **and `Referer` headers**
on `/apps/*` access-log lines (documented in `values.yaml`), app-worker
never logs request URLs or referers (structured fields only), and the
render access log records a params hash computed **after** reserved-name
stripping (§6.5) — `token` can never enter it. **Every** `/apps/*` response
— including the 302 exchange response itself, not just the shell page —
carries `Referrer-Policy: no-referrer`, so a tokened URL can never ride a
`Referer` header out of the exchange.

**Abuse controls (concrete).** Positive verify results cache ≤15 s (the
revocation bound). Failed verifications are negative-cached 60 s per
`(app_id, token_id)` and rate-limited to 10 failures/min per (client IP,
app) → 429 with `Retry-After: 60` — the `secret` half is not brute-forceable
in practice, but the failure path must not hammer Postgres. **Render rate
limits (concrete):** per viewer token 60 renders/min sustained with burst
10, keyed `(app_id, token_id)`; **platform-user renders** — author renders
authenticated by either a bearer api-token (CLI, §5.5) or a platform
session (UI `@draft` preview, §5.2 `sessions/verify`) — get the same 60/min
burst 10 keyed `(app_id, user_id)`, since no `token_id` exists on those
paths; per app 300 renders/min, keyed `app_id`. Exceeding any → 429 `rate_limited` with
`Retry-After: ceil(max over all violated buckets of seconds until that
bucket admits one render)`, minimum 1.
These bound *rate*; the per-app in-flight cap (§7) bounds *concurrency* —
both apply.

**CSRF.** Cookie-authenticated re-render `POST`s are accepted only with the
shell's custom header (`X-Datuplet-App-Render: 1`) and a same-origin
`Origin`/`Sec-Fetch-Site` check; `SameSite=Lax` already blocks cross-site
cookie sends on POST. **Requests authenticated by a bearer credential**
(platform api-token — the CLI/agent path, §5.5) **are exempt from the
browser CSRF checks**: CSRF is an ambient-cookie-authority attack, and
bearer tokens carry no ambient authority. Cross-site CSRF against a
read-only render burns capacity, not data — but capacity is worth
protecting.

### 5.4 App identity (reuses synthetic-identity machinery)

- At app creation: a synthetic app identity gets an FGA **`viewer`**
  (read-only) tuple on the project — same pattern as run identities,
  narrower than their `editor`. **Two subject forms, kept distinct** (so no
  double-prefixing): the **OpenFGA subject** the tuple/authorization check
  targets is the app's FGA subject (the run-identity form,
  `user:oidc~app-<app-uuid>`), while the **JWT `sub`** the impersonation
  token carries is the app's raw JWT subject — the exact relationship
  (whether the catalog layer composes `user:oidc~<sub>` from the raw claim,
  or the claim already carries the prefix) mirrors the existing run-token
  path and is pinned during implementation. Delete/disable = FGA tuple
  deletion; because FGA is evaluated per query, even an already-minted
  impersonation JWT authorizes nothing afterwards — the same ≤15 s
  blast-radius property as run cancellation.
- Per render, app-worker obtains a **60 s impersonation JWT** for that
  identity (§5.2) and attaches it to `query()` calls. Every statement lands
  in the existing `query_audit` log attributed to the app identity. The
  query route must accept impersonation-kind JWTs as principals (it already
  does for interactive storage browse; the contract requirement is stated
  here explicitly).
- **v1 authz posture (explicit):** an app can read **everything a project
  viewer can read**. There is no per-app table scoping in v1; authors and
  operators must treat "install an app + hand out its viewer tokens" as
  "expose anything the app queries from this project". Per-app scoped
  grants (per-namespace/table FGA tuples) are future work (§10).

**Separation of concerns:** viewer tokens gate *who sees rendered output*;
FGA gates *what data the app touches*. A leaked viewer token exposes that
app's rendered dashboards only — never SQL, never other tables, never the
platform API.

### 5.5 Authoring surfaces: CLI (agent-first) and management UI (R6)

**CLI — the primary authoring surface, agent-operable end-to-end.** Every
subcommand is non-interactive, works with the existing headless auth
(`datuplet login --remote` api-token), supports `-o json`, returns
structured errors (the §8 envelope shape), and uses the standard exit-code
contract. The full loop an agent (or CI) runs:

| Command | Semantics |
|---|---|
| `datuplet apps init <name>` | scaffold: `app.js` template + esbuild config + the SDK's TypeScript declarations (`datuplet.d.ts` for `ctx`, `datuplet.query`, OutputDoc block types) — the machine-readable contract an agent codes against. |
| `datuplet apps put <name> --bundle bundle.js` | upload → draft; prints `{app_id, version_hash}`. |
| `datuplet apps render <name> --channel draft --param k=v …` | server-side render of the draft under the author's bearer credential. Success: prints the OutputDoc JSON. Failure with `-o json`: prints **one** machine-readable object `{error, kind, request_id, author_log}` — the §8 envelope plus the matching render-log record fetched via `apps logs --request-id` (`author_log: null` if the lookup 404s). Failure in text mode: the envelope fields plus the log excerpt, human-formatted, on stderr. **This is the agent's test step** — implement → put → render → assert on JSON → iterate, no browser needed. |
| `datuplet apps logs <name> [--request-id <id>]` | recent render logs (JSON); `--request-id` returns the log record for one render. |
| `datuplet apps promote <name> --version <hash>` | CAS promote (§5.1). |
| `datuplet apps token create/list/delete <name>` | viewer-token lifecycle. |
| `datuplet apps get/list/delete` | metadata + channels + versions. |

**Project resolution (uniform rule):** every `datuplet apps` command
resolves the project via the CLI's existing chain — `--project <pid>` flag,
then `DATUPLET_PROJECT`, then the project recorded in
`~/.datuplet/cluster.json`; none resolvable → a deterministic error naming
those remedies. (This reuses the current CLI behavior rather than adding a
new persisted-default command; a `config set project` UX can come later.)
App names are unique per project (§4.1), so `(project, name)` is always
explicit or resolved, never guessed.

`apps render` is a thin wrapper over the `@draft` route: it sends the
author's bearer api-token with `Accept: application/json`, which selects
the JSON response mode (§4.2 step 5) and — being bearer-authenticated — is
exempt from browser CSRF checks (§5.3). Same route, same engine, same
limits as production; only the response framing differs, so CLI-tested
behavior is exactly production behavior.

**Management UI** — `/ui/apps` in the product SPA (`ui/product` style,
vanilla ES modules, no build step; reuses RFC 027 form patterns): catalog
(apps + channel/version status), app detail with bundle upload, draft
preview (iframe of `@draft`), promote/rollback with CAS confirmation,
viewer-token management (shown-once secrets, revocation), and the render-log
viewer. The UI drives the same §5.1 routes as the CLI — no privileged UI
path. In-browser code editing (editor + esbuild-wasm bundling) is future
work (§10); v1 authoring happens in the author's editor/agent, the UI
manages lifecycle.

## 6. Runtime contract

### 6.1 Query-service contract extension: bound parameters

`POST /api/v1/projects/{pid}/query` gains an optional `params` field
(maintainer decision, resolving review blocker #1):

```json
{
  "sql": "SELECT * FROM sales.orders WHERE country = $country AND order_date >= current_date - $days",
  "params": {"country": "DE", "days": 30},
  "timeout_s": 30, "max_rows": 1000, "max_bytes": 1048576
}
```

- Named `$param` placeholders, executed as a DuckDB prepared statement —
  values are **never parsed as SQL**.
- **Placeholder grammar:** `$[A-Za-z_][A-Za-z0-9_]{0,63}`, case-sensitive.
  A placeholder may repeat (same bound value). Every placeholder must have a
  matching key in `params`, and every `params` key must be referenced by at
  least one placeholder — unreferenced keys are rejected (typo defence).
  Positional (`?`) placeholders are not supported.
- Param values are JSON scalars only (string, number, boolean, null) in v1;
  arrays/structs are out. **Bind-type mapping:**

  | JSON value | DuckDB bind |
  |---|---|
  | string | `VARCHAR` (dates/timestamps arrive as ISO strings; cast in SQL, e.g. `CAST($d AS DATE)`) |
  | number, integral, `\|n\| ≤ 2^53−1` | `BIGINT` |
  | number, non-integral | `DOUBLE` |
  | number, integral, `\|n\| > 2^53−1` | rejected — 400 `bad_request` (**single precision rule**: values outside the JavaScript safe-integer range, `\|n\| > Number.MAX_SAFE_INTEGER`, are never silently bound; pass big integers/decimals as strings + explicit `CAST`, e.g. `CAST($id AS BIGINT)`) |
  | boolean | `BOOLEAN` |
  | null | untyped SQL `NULL`; where DuckDB cannot infer the type, the SQL must cast explicitly |

- Missing/unknown/unreferenced/mistyped params → 400 `bad_request`; bind
  failures at prepare/execute time surface as 400 `sql_error`.
- Backward compatible: `params` is optional; existing callers unchanged.
  `docs/ad-hoc-query.md` and the RFC 022 contract get amended; the query
  console and CLI may adopt params later.

This is the injection defence: app code decides query *structure*
(trusted-ish, author-written); viewer-controlled *values* (`ctx.params`)
enter only as bound parameters. The SDK documentation forbids interpolating
`ctx.params` into SQL strings, and Appendix A models the correct pattern.

### 6.2 Guest ABI (v1)

Authors **write** an ES module exporting `render(ctx) -> OutputDoc`. The CLI
(§5.5) bundles it with esbuild to a single self-contained **IIFE** exposing
the module on a global (`--format=iife --global-name=__dtp_app`), and that
IIFE bundle is the artifact uploaded and evaluated by the engine — the
QuickJS shim reaches the entry at `globalThis.__dtp_app.render` without a
module loader. (Implementation decision from the plan's engine design: IIFE
delivery avoids QuickJS module-namespace export plumbing in C; the
author-facing contract is unchanged — you still author an ES module.)

Available inside the sandbox:

- `datuplet.query(sql, params?, opts?)` → `{schema, rows, truncated, stats}`
  (the query service response, verbatim). `opts` maps 1:1 onto the service
  fields: `{maxRows → max_rows, maxBytes → max_bytes, timeoutS →
  timeout_s}`; values are clamped server-side exactly as documented in
  `docs/ad-hoc-query.md`, and `timeoutS` is additionally clamped to the
  **remaining render deadline** (§7): the effective value is
  `min(timeoutS, floor(remaining_seconds))`; if the remaining budget is
  <1 s, the host throws `timeout` locally without calling the service. On
  render-deadline expiry the host aborts the in-flight HTTP request;
  query-worker must cancel DuckDB execution on client disconnect
  (implementation requirement — its own `timeout_s` remains the backstop).
  A query can never outlive its render.
- **Error semantics:** on a non-200 from the query service,
  `datuplet.query` **throws** a JS `Error` with `{kind, message}` mirroring
  the service's error envelope (`sql_error`, `timeout`, `rate_limited`,
  `capacity`, …). Apps may catch it to degrade gracefully (e.g. render a
  partial dashboard); an uncaught throw fails the render — full error +
  stack to the author log, generic failure to the viewer (§8).
- Queries execute **sequentially** — QuickJS is single-threaded and the host
  serializes `query()` calls. This keeps each render within the query
  service's per-principal in-flight cap by construction (§7).
- `console.log(...)` — captured to the author log, size-capped (§6.6).
- `ctx.path`, `ctx.params` — request routing + filter state (normalization
  in §6.5).
- `ctx.now` — render-start timestamp (ms epoch).
- **Global environment (explicit):** standard ES2023 built-ins. `Date` and
  `Math.random` work, backed by host imports (real clock, host-seeded PRNG)
  — no determinism guarantees and none needed (this is not durable
  execution). No `fetch`, no timers, no fs, no env, no dynamic `import`.

Authors may bundle pure-JS npm libraries via esbuild; Node APIs are
documented as unsupported.

### 6.3 OutputDoc + shell interactivity

`{outputDoc: 1, title, blocks: […], refreshInterval?}` — the `outputDoc`
integer versions the format. Block types: `markdown | metric | table |
chart | filter | tabs`. **Every block carries a required `id`** (author-
assigned, unique per doc) — the partial-render key. The full per-block field
schema is a **versioned JSON Schema shipped with the shell and enforced by
app-worker** on every render (validation failure = render error, not
best-effort display). Structural caps: ≤64 blocks/doc, OutputDoc ≤2 MiB.

All dynamics live in the platform-owned shell; app code stays
request/response. The shell guarantees:

- **Client-side, no round trip:** chart tooltips/zoom/series-toggling
  (Vega-Lite handles these on the delivered spec), table sort/search/
  pagination over the returned rows, CSV export. CSV export applies
  spreadsheet-formula-injection escaping: any cell starting with `=`, `+`,
  `-`, `@`, tab, or CR is prefixed with `'` (OWASP CSV-injection guidance).
- **Loading states:** skeleton blocks on first load; on any re-render the
  stale dashboard stays visible, dimmed, with a spinner, and blocks swap
  when the response lands. Authors do nothing to get this.
- **Filters & deep links:** filter changes set URL params and re-render;
  params arrive in `ctx.params`, values bound via §6.1 — any filter state is
  a shareable link.
- **Declarative cross-filtering:** a chart block may declare
  `onClick: {param: "…"}`; the shell sets the param and re-renders. Config,
  not code.
- **Partial refresh:** `POST …?block=<id>` with `Accept: application/json`
  re-renders and returns that single block as JSON per the §4.2 response
  matrix (the render still executes `render()` fully; partial-ness is a
  response filter in v1 — an optimization seam, not a semantic change).
- **Auto-refresh:** `refreshInterval` seconds, **clamped to [15, 3600]**,
  ±10 % jitter, paused while the tab is hidden, exponential backoff on
  429/503. A bundle cannot request continuous re-rendering.
- **Tabs:** either a `tabs` block (all blocks delivered, shell switches
  client-side) or route-based via `ctx.path` sub-paths (only the visible
  tab's queries run). Authors choose per app.
- **Modals:** a block/table-row/button may declare `modal: {title, blocks}`
  (inline, shown client-side) or `modal: {param}` (lazy: sets the param,
  fetches content as a partial render — modal state deep-links). Modal
  content is the same block vocabulary; v1 modals display and set params
  only (no side-effectful submits — that's writebacks, §10).
- **PDF export:** print stylesheet + `window.print()`. Server-generated
  PDFs are future work (§10).

Deliberately absent: server push (websockets/SSE) and app-authored browser
JS — the latter would break the XSS boundary. Progressive block streaming is
future work (§10).

### 6.4 Output security: encoding, Vega lockdown, CSP

The boundary property — "untrusted code, trusted output" — holds because
the only thing crossing from guest to viewer is declarative data, and the
shell treats every string in it as hostile:

- **Text fields** (titles, labels, metric values, table cells): rendered via
  `textContent`, never `innerHTML`.
- **Markdown blocks**: rendered by marked, then sanitized by DOMPurify with
  a fixed allowlist config (no raw HTML pass-through, no `style`
  attributes); links restricted to `https:`/`http:`/`mailto:` schemes and
  stamped `rel="noopener nofollow"`.
- **Vega-Lite specs (locked down):** the shell and app-worker share one
  checked-in **restricted subset JSON Schema** — the normative,
  machine-checkable artifact, at `pkg/appengine/vegaspec/schema.json`,
  vendored into the shell; it is a required deliverable of the
  implementation plan and may only *narrow* what this section defines.
  The subset, enumerated:
  - **Top-level keys:** `title, description, width, height, data, mark,
    encoding, transform` (plus `$schema` pinned to the vendored Vega-Lite
    version). **Single-view only in v1** — `layer`, `facet`, `concat`,
    `repeat`, `resolve`, `params`, `usermeta`, **and `config`** are
    **rejected** (composition is future vocabulary growth, §10;
    author-supplied `config` is rejected because chart styling comes from
    the platform theme, which the *shell* applies at embed time — a
    platform-owned artifact, never part of an app's spec — and rejecting it
    outright eliminates the config-allowlist recursion entirely).
  - **`data`:** exactly `{values: […]}` — `data.url`, named datasets, and
    generator sources rejected; Vega loaders disabled at embed time
    regardless.
  - **`mark`:** string or object; `type ∈ {bar, line, area, point, circle,
    square, tick, rect, rule, text, arc}` (**no `image`**); object keys
    limited to `type, tooltip, point, interpolate, opacity, filled, size,
    cornerRadius, orient, fontSize` (**no `href`**).
  - **Encoding channels:** `x, y, xOffset, yOffset, color, opacity, size,
    shape, theta, radius, text, tooltip, order, detail` (**no `href`, no
    `url`**). Channel-def keys: `field, type, value, aggregate, bin,
    timeUnit, sort, stack, title, format, scale, axis, legend, condition`;
    `scale` keys `{type, domain, range, scheme, zero, nice}` with `scheme`
    from Vega's named-scheme list only; `axis`/`legend` keys limited to
    presentation fields (`title, format, labelAngle, grid, orient,
    tickCount, values`); `condition` = `{test, value|field}`.
  - **Transforms:** `calculate{calculate, as}`, `filter{filter}`,
    `aggregate{aggregate[{op, field, as}], groupby}`, `bin{bin, field, as}`,
    `timeUnit{timeUnit, field, as}`, `window{window[{op, field, as}],
    groupby, sort, frame}`, `fold{fold, as}`, `pivot{pivot, value, groupby,
    op}` — **no `lookup`** (can carry URL data), no `loess`/`regression`/
    `density`/`quantile` in v1.
  - **Expressions:** Vega *expression strings* are permitted only inside
    `calculate`, `filter`, and `condition.test` — they run in Vega's
    sandboxed expression interpreter with no DOM or network access.

  Mandatory negative tests: `data.url`, `data.name`, `href` encoding,
  `image` mark, `mark.href`, `lookup` transform, `layer`/`facet`
  composition, `usermeta`, and any `config` key at all (§11). This closes the
  exfiltration channel review blocker #15 identified: without it, a
  malicious spec could make the *viewer's browser* fetch attacker-chosen
  URLs carrying query results.
- **CSP (defence in depth):** the shell page ships
  `default-src 'none'; script-src 'self'; style-src 'self'; connect-src
  'self'; img-src 'self' data:; base-uri 'none'; form-action 'self';
  frame-ancestors 'self'` — even a validator miss cannot reach the network.
  `frame-ancestors 'self'` (not `'none'`) is deliberate: it permits framing
  by any same-origin platform page — CSP cannot path-scope this — with the
  `/ui/apps` draft preview iframe (§5.5) as the intended v1 use, while
  still blocking all cross-origin embedding/clickjacking.
- Charts are Vega-Lite-first: `{type: "chart", library: "vega-lite", spec}`
  — the `library` field is a hedge for a second curated library later.
  Rationale for Vega-Lite over ECharts: data-only is its native idiom
  (published JSON Schema → mechanical validation; sandboxed expression
  strings instead of JS-function formatters; built-in transforms offload
  reshaping from the interpreter; declarative selections map onto `onClick`
  bindings). Austere defaults are fixed with a shipped platform theme
  config.

### 6.5 Request-input normalization (`ctx.params`, `ctx.path`)

- `ctx.params` is a flat `string → string` map. Sources: URL query
  parameters, merged with (and overridden by) the JSON object body of a
  re-render POST. Duplicate keys: last wins. No arrays, no nesting, no type
  coercion — apps parse their own numbers.
- Re-render POSTs require `Content-Type: application/json` (else 400
  `bad_request`) and a **hard pre-parse body cap of 16 KiB**; larger bodies
  are rejected before JSON parsing. Malformed JSON → 400 `bad_request`.
- Limits: ≤32 keys, key ≤64 chars, value ≤1 KiB, total URL ≤8 KiB; excess →
  400 `bad_request`.
- Reserved names stripped before delivery: `token`, `block`.
- Note: `ctx.params` keys are arbitrary strings; the `params` object an app
  passes to `datuplet.query` must use keys matching the placeholder grammar
  (§6.1) — apps select/rename explicitly, as Appendix A models.
- `ctx.path` is the sub-path after `/apps/{pid}/{name}`, normalized (no
  `..`, no encoded separators), ≤256 chars.

### 6.6 Render logs

`console.log` output + render errors/stacks are captured per render (≤64 KiB
per render), appended via §5.2 to a per-app ring buffer in Postgres (default:
last 200 renders or 14 days, whichever first; operator-tunable). **Record
schema:** `{request_id, app_id, version_hash, channel, started_at,
duration_ms, outcome, log_text, error?}` — `request_id` is the same §8
request-id returned to clients, which is what makes
`apps logs --request-id` (§5.5) work. No secret
can leak into logs by construction — the guest never sees the impersonation
JWT, the viewer token, or any credential — but authors are documented as
responsible for what they `console.log` from query results.

## 7. Limits, scheduling & scaling

Defaults, operator-tunable via `values.yaml` (`appWorker.render.*`),
mirroring `queryWorker.query.*`:

| Limit | Default | Cap |
|---|---|---|
| wall clock per render | 10 s | 30 s |
| WASM memory per instance | 128 MiB | 256 MiB |
| queries per render | 10 | 25 |
| OutputDoc size | 2 MiB | — |
| bundle size | 5 MB | — |
| blocks per OutputDoc | 64 | — |
| per-app in-flight renders | 2 | — |
| per-principal render rate — viewer token `(app_id, token_id)` or platform user (bearer/session) `(app_id, user_id)` (§5.3) | 60/min, burst 10 | — |
| per-app render rate (§5.3) | 300/min | — |
| `refreshInterval` | — | clamped [15 s, 3600 s] |

**Deadline coupling.** The render deadline is the outer bound: each
`query()`'s `timeout_s` is clamped to the remaining render budget, and the
render context cancels in-flight queries on expiry. A query can never
outlive its render (resolves the 10 s-render vs 60 s-query mismatch).

**Preemption.** Renders run under a Go context with
`wazero.WithCloseOnContextDone` — deadline/cancel halts guest execution even
in a non-cooperative `while(true){}` loop; the WASM memory limit is enforced
by the runtime's memory pages cap. Both paths carry explicit tests (§11).

**Cap alignment.** Guest queries are sequential (§6.2), so per-app
concurrent queries = per-app in-flight renders (default 2) — exactly the
query service's per-principal in-flight default. The two knobs are
documented as coupled; raising one without the other either starves renders
(429s from the query service) or idles capacity. A worker-side query
scheduler becomes necessary only if a parallel query API is ever added.

**Memory model (pod budget).** Per render, host-side memory ≈ WASM cap
(≤128 MiB default) + one buffered query response (≤`max_bytes`, 10 MiB cap)
+ OutputDoc buffer (≤2 MiB); per pod, add the compiled engine (~tens of MB)
and the bundle LRU (capped, default 256 MiB). Render slots per pod =
`(pod_mem − fixed overhead) / per-render budget`; the render semaphore
enforces it (same 429-vs-503 distinction as the query service).

**Capacity scales with concurrent renders, not app count.** An idle app is a
Postgres row + an FGA tuple; bundle caches are LRU'd by content hash
(realistic bundles 100–500 KB). A render is query-dominated (~0.5–2 s), so a
pod with 8–16 slots sustains ~10–30 renders/s; 100 published apps is
comfortably 2–4 pods at peak, and 1 000 changes nothing structural. The real
downstream pressure is the query service (each render fans 1–5 DuckDB
queries into query-worker); per-app FGA principals give per-app rate limits
for free. **These figures are pre-spike assumptions** — the §12 engine spike
(eval time, 10 MiB JSON throughput) gates final sizing; treat them as
planning numbers, not commitments.

**Rollout requirements:** engine compile at boot **before** readiness (never
on a viewer's first request); preStop drain with termination grace > drain
slack (as fixed for query-worker in RFC 025); a PodDisruptionBudget. With
those, a Deployment restart is a non-event: no sessions exist, viewer
cookies are stateless (signature + revocation check), impersonation JWTs are
per-render — only warm caches are lost.

## 8. Failure modes & error envelope

Viewer-facing errors have **one schema**: partial-render/JSON fetches get
`{"error": "...", "kind": "<kind>", "request_id": "..."}`; full-page
navigations get a minimal HTML error page carrying the same kind +
request-id. Kinds: `bad_request` (input-normalization failures, §6.5),
`unauthorized`, `app_not_found`, `render_error`, `timeout`, `rate_limited`,
`capacity`, `unavailable`.

- **Guest trap / OOM / deadline** → viewer `render_error`/`timeout` +
  request-id; real error, stack, and captured logs to the author log.
- **Query errors** → author log detail; viewer sees `render_error` only if
  the app didn't catch and degrade.
- **Worker pod death mid-render** → viewer retry; zero state lost (R1).
- **Bundle fetch / resolve failure** → `unavailable`.
- **pipeline-api unavailable** → mint/verify fail closed; renders
  `unavailable`.
- **Query service disabled** (`queryWorker.enabled=false`): user apps
  **hard-depend** on it — chart values validation fails an install that
  enables `appWorker` without `queryWorker`, and at runtime `query()` fails
  closed with `unavailable` (the dependency is stated in `values.yaml`
  docs). No degraded chart-less mode.

## 9. Security model

| Threat | Containment |
|---|---|
| Malicious app author (arbitrary JS) | WASM capability sandbox: only `query()`/`log` exported; no sockets/fs/env; context-done preemption for hot loops. Data access read-only via FGA `viewer` tuple; per-statement audit via `query_audit`. **v1 posture: app reads anything a project viewer can** (§5.4) — per-app scoping is future work. |
| SQL injection via viewer-controlled params | Bound parameters (§6.1): `ctx.params` values enter queries as prepared-statement binds, never as SQL text. |
| Malicious output (XSS on viewers) | Closed block vocabulary + versioned schema validation in app-worker; `textContent` rendering; DOMPurify'd markdown; restricted Vega-Lite subset. |
| Data exfiltration via chart specs | Inline `data.values` only; Vega loaders disabled; shell CSP `connect-src 'self'` — the viewer's browser cannot be steered to attacker hosts (§6.4). |
| Leaked viewer token | Exposes that app's rendered output only; revocable per token (sessions die ≤15 s later); failed verifications rate-limited per (IP, app). |
| Stolen session cookie | HMAC-signed, ~24 h expiry, bound server-side to one app (`cookie.app_id == resolved app_id` on every request; path scoping is convenience only), revocation-checked by `(app_id, token_id)`. |
| CSRF | `SameSite=Lax` + custom header + Origin check on re-render POSTs (§5.3). |
| Worker over-minting identities | Impersonation subject derived server-side from the app row; worker can never name an arbitrary principal; every mint audited (§5.2). |
| Resource exhaustion | Render deadline, WASM memory cap, sequential queries + aligned in-flight caps, `refreshInterval` clamps + hidden-tab pause + backoff, per-app/per-principal (viewer-token and platform-user) render rate limits, pool semaphore, HPA. |
| Compromised app-worker pod | Holds one service credential (resolve/fetch/verify/mint/log scope) + transient 60 s impersonation JWTs; no storage credentials, no long-lived data-plane tokens. Standard pod hardening applies (no SA token automount, dropped capabilities, seccomp). |

**Audit.** Three layers: (1) existing `query_audit` per statement (app
principal, jti, statement hash); (2) a per-request **render access log**:
app, version hash, channel, `principal_kind` (`viewer_token` |
`platform_user`) + `principal_id` (the `token_id` — never the secret — or
the platform `user_id`; `platform_user` covers both bearer CLI renders and
session-authenticated UI `@draft` previews), path, params hash, outcome,
duration, client IP; (3) control-plane events: token
mint/delete, promote/rollback (actor, from→to version), impersonation mints.
Prometheus counters mirror the query service's (`…_render_requests_total`
by outcome).

## 10. Out of scope for v1 (future work)

- **Writebacks — named v2 milestone candidate** (concrete use case: survey /
  form apps; also external-action apps à la release dashboards). Shape: a
  `form` block type + side-effectful modal submits, one new host capability
  (`datuplet.append(table, row)` or a dedicated ingest endpoint), a per-app
  FGA grant scoped to a single response table; external calls need
  `datuplet.fetch` with an author-declared, operator-approved egress
  allowlist. Pool, sandbox, and statelessness are unchanged.
- **Per-app data scoping** — narrow the v1 project-wide `viewer` grant to
  per-namespace/table tuples declared in the app manifest.
- **Response caching** per `(version hash, params)` with author-set TTL —
  the big cost/latency lever, and the answer to synchronized-viewer
  stampedes.
- **Python authoring** — CPython-WASI or componentized Pyodide behind the
  same pool + ABI.
- **"Pro apps"** — Approach C (per-app containers, scale-to-zero) for apps
  that outgrow the sandbox; coexists with A.
- **SSO / basic auth at the viewer edge** (same middleware seam as tokens).
- **Headless renderer service** — optional Deployment (headless Chromium)
  authenticating as a viewer; covers server-side PDF export, scheduled
  snapshots, emailed reports.
- **Progressive block streaming** over SSE (`render()` yielding blocks).
- Pretty per-project URL aliases; block-vocabulary growth (`badge`,
  `progress`, `countdown`, `loadMore` pagination); Vega-Lite composition
  (`layer`/`facet`/`concat`) as a widening of the §6.4 subset.
- **In-browser code editing** in `/ui/apps` (editor + esbuild-wasm bundling
  + live draft preview); v1 UI is lifecycle management only (§5.5).

## 11. Testing

- **Unit:** ABI marshalling incl. bound-params mapping and clamps; the
  JSON→DuckDB bind-type table (int64 boundary, 2^53 rejection, null casts);
  placeholder grammar + unreferenced-param rejection; OutputDoc schema
  validation (required `id`s, block caps); Vega subset validator negative
  cases (`data.url`, `data.name`, `href` encoding, `image` mark,
  `mark.href`, `lookup` transform, `layer`/`facet`, `usermeta`, any
  `config` key); markdown
  sanitizer config; CSV formula-injection escaping; `ctx.params`
  normalization + limits, POST content-type + 16 KiB pre-parse cap;
  deadline coupling (clamps, <1 s local `timeout` throw); cookie
  sign/verify + `cookie.app_id == resolved app_id` binding + revocation
  by `(app_id, token_id)`; promote CAS (409 on `expectedProduction`
  mismatch); the §4.2 response matrix (HTML vs full-doc JSON vs single
  `block=<id>` vs error envelope, bearer-auth CSRF exemption vs cookie-auth
  header checks); per-principal (viewer-token and platform-user) + per-app
  render rate limits incl. `Retry-After` values; `Referrer-Policy` on the
  302 + `Referer` redaction in access logs.
- **Integration:** golden bundles against a fake query backend; engine
  trap/OOM/deadline paths incl. a non-cooperative `while(true){}` halted by
  context-done; `datuplet.query` throw semantics (caught → partial render;
  uncaught → `render_error`); bundle hash verification on fetch; verify-
  cache TTL + negative-cache rate limiting.
- **e2e (`tests/e2e/`):** upload a sample app (lands in draft) → draft
  renders under a platform session (via `sessions/verify`) and NOT under a
  viewer token → promote (by hash) → mint viewer token → token exchange
  sets cookie + redirects token-free (and the token appears in no access
  log) → render through ingress → app-worker → query service (bound
  params) → real warehouse table; assert chart data, render access log, and
  `query_audit` attribution; an injection attempt via `?country=' UNION…`
  returns data-free `sql_error`/bound-value behavior, never extra rows; a
  cookie minted for app A replayed against app B → `unauthorized`; delete
  the viewer token → session dies ≤15 s; promote a second version → all
  workers serve it ≤15 s. An **agent-flow e2e** runs the full §5.5 loop
  non-interactively: `apps init` → esbuild → `put` → `render --channel
  draft -o json` (assert OutputDoc) → `promote` → `token create` → viewer
  render — proving R6 end-to-end. Chart/Deployment changes require
  `make e2e-k8s` on OrbStack per repo policy.

## 12. Open questions (to resolve during planning)

1. **Engine spike:** QuickJS (via wazero) vs StarlingMonkey — measure eval
   time and JSON throughput on a representative 10 MiB result set; pick on
   numbers, with Approach B as the named fallback if both disappoint. Note
   StarlingMonkey targets the Component Model / wasmtime, so it would pull
   `wasmtime-go` (cgo) — eroding the pure-Go advantage. Alternates if
   quickjs-ng disappoints on spec compliance: XS (Moddable; ES2023-complete,
   embedded lineage). Pragmatic simplification if the WASM boundary is
   judged overkill for project-member authors: goja/Sobek (pure-Go JS
   interpreter, k6 lineage) — one notch weaker isolation, zero WASM
   toolchain. **§7 capacity figures are gated on this spike.**
2. **Eval-per-request vs precompiled apps:** v1 evals the JS bundle in a
   fresh engine instance; a later optimization is per-app precompiled WASM
   (Javy/componentize-js lineage) if eval cost shows up in p95.
3. **RFC number:** claimed 028 here; confirm against the RFC ledger before
   merge.

(Resolved since v2: routing shape — path prefix on the existing ingress with
path-scoped cookies + CSP, dedicated host optional (§4.1); chart library —
Vega-Lite-first (§6.4); bound params — query-service contract extension
(§6.1); **bundle upload format — JSON + base64 on the PUT route**, chosen in
the implementation plan for uniform CLI + UI handling.)

---

## Appendix A — Worked example (author → wire → viewer)

The complete author-side artifact for a "Sales overview" app — one file:

```js
// sales-overview/app.js
export async function render(ctx) {
  const days = Number(ctx.params.days ?? 30);
  const country = ctx.params.country ?? "ALL";
  const where = country === "ALL" ? "" : "AND country = $country";

  // Bind params must match the placeholders actually present (§6.1):
  // $country only exists when the clause does, so include it conditionally.
  const bind = country === "ALL" ? { days } : { days, country };

  const kpi = await datuplet.query(`
    SELECT count(*) AS orders, sum(amount) AS revenue
    FROM sales.orders
    WHERE order_date >= current_date - $days ${where}`,
    bind);

  const daily = await datuplet.query(`
    SELECT order_date, sum(amount) AS revenue
    FROM sales.orders
    WHERE order_date >= current_date - $days ${where}
    GROUP BY 1 ORDER BY 1`,
    bind);

  const top = await datuplet.query(`
    SELECT product, sum(amount) AS revenue, count(*) AS orders
    FROM sales.orders
    WHERE order_date >= current_date - $days ${where}
    GROUP BY 1 ORDER BY 2 DESC LIMIT 5`,
    bind, { maxRows: 5 });

  const [orders, revenue] = kpi.rows[0];

  return {
    outputDoc: 1,
    title: "Sales overview",
    blocks: [
      { id: "filters", type: "filter", fields: [
        { name: "days", label: "Window", kind: "select", value: days,
          options: [{ value: 7,  label: "Last 7 days"  },
                    { value: 30, label: "Last 30 days" },
                    { value: 90, label: "Last 90 days" }] },
        { name: "country", label: "Country", kind: "select", value: country,
          options: ["ALL", "CZ", "DE", "US"] },
      ]},
      { id: "kpis", type: "metric", items: [
        { label: "Revenue",   value: revenue,          format: "currency:EUR" },
        { label: "Orders",    value: orders },
        { label: "Avg order", value: revenue / orders, format: "currency:EUR" },
      ]},
      { id: "daily-revenue", type: "chart", library: "vega-lite",
        title: "Daily revenue",
        spec: {
          mark: "bar",
          data: { values: daily.rows.map(r => ({ date: r[0], revenue: r[1] })) },
          encoding: {
            x: { field: "date", type: "ordinal" },
            y: { field: "revenue", type: "quantitative" },
          },
        }},
      { id: "top-products", type: "table", title: "Top products",
        columns: ["Product", "Revenue", "Orders"], rows: top.rows },
      { id: "footer", type: "markdown",
        text: `_${daily.stats.rows_scanned.toLocaleString()} rows scanned · rendered server-side at ${new Date(ctx.now).toISOString().slice(0, 16)}Z_` },
    ],
  };
}
```

Note the injection-safety pattern: `ctx.params` values (`days`, `country`)
are passed **only** through the `params` argument — bound server-side as
prepared-statement parameters (§6.1) — while the author-written code decides
query *structure* (the conditional `where` clause references `$country`; it
never splices the value), and the bind object mirrors the placeholders
actually present (unreferenced keys are rejected, §6.1).

Shipping and sharing (`--project <pid>` shown once; the remaining commands
use it or the configured default project, §5.5):

```
esbuild app.js --bundle --format=iife --global-name=__dtp_app --outfile=bundle.js
datuplet apps put sales-overview --project <pid> --bundle bundle.js   # -> draft, prints version hash
datuplet apps render sales-overview --project <pid> --channel draft --param days=7 -o json   # smoke-test: OutputDoc or structured error
# (humans: preview at /apps/<pid>/sales-overview@draft — platform session required)
datuplet apps promote sales-overview --project <pid> --version <hash> # -> production
datuplet apps token create sales-overview --project <pid>             # prints vw_… once
# share: https://<host>/apps/<pid>/sales-overview?token=vw_…
```

This same sequence, minus the browser preview, is the agent loop (R6/§5.5):
implement → `put` → `render … -o json` → assert → `promote`.

What crosses the wire to the viewer is never the code above — only the
evaluated OutputDoc; e.g. the chart block arrives as inert data:

```json
{ "id": "daily-revenue", "type": "chart", "library": "vega-lite",
  "title": "Daily revenue",
  "spec": { "mark": "bar",
            "data": { "values": [{ "date": "2026-06-23", "revenue": 3120 }] },
            "encoding": { "…": "…" } } }
```

Points the example demonstrates: aggregation happens in DuckDB (the app only
reshapes ≤10 k-row results — why a no-JIT engine suffices); `ctx.params`
comes from the URL, so `?days=7&country=DE` deep-links a filter state, with
values bound, never concatenated; the author never touches credentials or
manifests — write `render()`, `put`, `promote`, mint a token.
