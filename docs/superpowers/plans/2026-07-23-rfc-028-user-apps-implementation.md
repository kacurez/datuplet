# RFC 028 — User Apps on a WASM Worker Pool: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement RFC 028 Draft v10 end-to-end: read-only user dashboard
apps (author-written JS) executed server-side in per-request WASM sandboxes
on a shared stateless `app-worker` pool, data exclusively via the query
service (which gains bound parameters), declarative OutputDoc rendered by a
trusted Vega-Lite shell, draft/production channels, viewer tokens, an
**agent-operable CLI** (`datuplet apps init/put/render/promote/token/logs`),
and a **management UI** at `/ui/apps`.

**Architecture:** Eight parts. Part 0 de-risks the engine (QuickJS-on-wazero
spike — HARD maintainer gate). Part 1 extends the query service with bound
params (§6.1) — independently mergeable value. Part 2 builds the control
plane in pipeline-api (migration 013, author routes, six internal
endpoints, FGA app identities). Part 3 builds `app-worker` (engine host,
viewer auth, limits, response matrix). Part 4 is the trusted viewer shell.
Part 5 the CLI, Part 6 the `/ui/apps` UI, Part 7 charts + e2e + docs.

**Tech Stack:** Go 1.25; `github.com/tetratelabs/wazero` (new root dep, pure
Go); quickjs-ng compiled to wasm32-wasi via wasi-sdk in a Docker build
(`make engine-wasm`, artifact committed);
`github.com/santhosh-tekuri/jsonschema/v6` (OutputDoc + Vega subset
validation); `golang.org/x/time/rate`; vendored vega/vega-lite/vega-embed +
DOMPurify + marked in the shell; vanilla ES modules (no build step) for
shell + UI; esbuild is **author-side only** (never a repo build dep).

**Spec (authoritative):** `docs/superpowers/specs/2026-07-22-rfc-028-user-apps-wasm-workers-design.md` (Draft v10)

## Parts

| Part | Phase | Tasks | Starts when |
|---|---|---|---|
| 0 | Engine spike (QuickJS/wazero) — **HARD maintainer gate** | E0–E2 | now |
| 1 | Query service bound params (§6.1) | Q0–Q3 | commit 0 done (∥ Part 0 — file-disjoint) |
| 2 | Control plane: migration, author routes, internal API, FGA | P0–P6 | Part 1 gate |
| 3 | app-worker: engine host, auth, limits, response matrix | W0–W7 | Part 0 GO + Part 2 gate |
| 4 | Trusted viewer shell | V0–V4 | Part 3 gate |
| 5 | CLI `datuplet apps` (agent loop) | C0–C3 | Part 3 gate (∥ Part 4 — file-disjoint) |
| 6 | Management UI `/ui/apps` | U0–U3 | Parts 3+4 gates |
| 7 | Charts, e2e, docs | D0–D4 | Parts 1–6 gates |
| SQ | **Side quest** — `datuplet secrets` CLI (independent of RFC 028) | SQ1 | anytime (own branch/PR) |

## Branching model

All implementation lands as **sequential commits on one feature branch**
`feat/rfc-028-user-apps` off `main`. Never push `main`, never tag (repo
rule). Commit 0 (before E0): commit the spec (currently untracked) + this
plan so subagents read them from the tree. The Part 1 gate (Q3) opens **the
single draft PR** (`gh pr create --draft --base main --title "RFC 028: user
apps on a WASM worker pool"`); every later Part's gate pushes and **appends
its phase summary to that PR body** — it does NOT open a new PR. At each
Part start the orchestrator records `git rev-parse HEAD` as `<phase-start
SHA>` (base of that Part's cumulative Codex review). Tasks marked
`Parallel: yes` are file-disjoint and may run in separate subagent
worktrees; serializing is always safe and is the default when in doubt.

**Deployment guard:** the branch is never deployed to a long-lived
environment — only disposable e2e/OrbStack clusters. POC greenfield (spec
§2 posture): no migrations-of-migrations, no back-compat shims.

## Cross-part interface contract (fixed — Parts reference these names verbatim)

**Engine ABI (Parts 0/3):**
- Committed artifact `pkg/appengine/embed/engine.wasm` (quickjs-ng + C shim,
  wasm32-wasi); build recipe `utils/docker/engine-wasm.Dockerfile` + `make
  engine-wasm` (regenerates the artifact; CI does NOT rebuild it).
- Shim exports: `dtp_alloc(size u32) -> u32`; `dtp_render(script_ptr,
  script_len, ctx_ptr, ctx_len u32) -> u64` (guest ptr<<32|len of result
  JSON).
- Shim imports (module `dtp_host`): `query(req_ptr, req_len u32) -> u64`
  (host writes response into guest memory via `dtp_alloc`); `log(ptr, len
  u32)`.
- Result JSON: `{"ok":true,"doc":<OutputDoc>}` or
  `{"ok":false,"error":"<msg>","stack":"<js stack>"}`.
- Script = Go-side concatenation: `prelude.js` + `";\n"` + app bundle.
  Authors write ESM `export async function render(ctx)`; the CLI bundles
  with `esbuild --bundle --format=iife --global-name=__dtp_app`, so the
  shim/prelude reaches the entry at `globalThis.__dtp_app.render`. The shim
  evals the script (JS_EVAL_TYPE_GLOBAL), then calls prelude-defined
  `globalThis.__dtp_run(ctxJson)` (which does not return the render Promise
  — it stores the settled outcome in `globalThis.__dtp_result` and sets
  `globalThis.__dtp_settled=true` in a `.finally()`), then drains
  `JS_ExecutePendingJob` until the job queue empties. **Settled-flag
  protocol:** after draining, the shim reads `__dtp_settled`; if false
  (queue emptied with the render Promise still pending — e.g. a
  never-resolving await) it packs an `ok:false` "render did not settle"
  result. The wazero wall-clock deadline is the backstop for a guest that
  never yields.
- Go API (`pkg/appengine`):
  - `func NewEngine(ctx context.Context, memoryPages uint32) (*Engine, error)`
    — compiles the embedded module once; sets the runtime memory limit
    (engine-level, spec §7); concurrent-safe across `Render` calls.
  - `type QueryFunc func(ctx context.Context, reqJSON []byte) ([]byte, error)`
  - `type Limits struct { WallClock time.Duration; MaxQueries, MaxOutputBytes, MaxLogBytes int }` — memory is engine-level (`NewEngine`), not per-render.
  - `type RenderInput struct { Bundle []byte; Path string; Params map[string]string; Now time.Time; Query QueryFunc; Limits Limits }`
  - `type Result struct { Doc json.RawMessage; Log []byte }`
  - `func (e *Engine) Render(ctx context.Context, in RenderInput) (*Result, *RenderError)`
  - `type RenderError struct { Kind, Msg, Stack string; Log []byte }` — Kind ∈ `render_error|timeout|bad_request`.

**Query service (Part 1):**
- `queryRequest` (`components/queryengine/cmd/query-worker/server.go:129`)
  gains `Params map[string]any \`json:"params,omitempty"\``.
- `components/queryengine/params.go`:
  `func ValidateParams(sql string, params map[string]any) ([]string, error)`
  — placeholder grammar `\$([A-Za-z_][A-Za-z0-9_]{0,63})` scanned OUTSIDE
  single/double-quoted spans; both-ways referenced/provided check; scalar
  types only; integral numbers `|n| > 2^53−1` rejected. Returns the ordered
  distinct placeholder names.
- Binding: `sql.Named(name, value)` args to the prepared statement
  (go-duckdb named-arg support verified in Q0; fallback documented in Q2).
- Errors: param-shape violations → 400 `bad_request`; bind/prepare failures
  → 400 `sql_error`. `params` optional — existing callers unchanged.
- queryproxy passthrough: `pkg/pipelineapi/queryproxy/client.go` request
  struct + `handler.go` gain the same optional field, forwarded verbatim.

**Control plane (Part 2):**
- Migration `pkg/pipelineapi/db/migrations/013_user_apps.sql` — tables
  `apps(id, project_id, name UNIQUE(project_id,name), fga_registered, created_at)`,
  `app_versions(id, app_id FK CASCADE, hash char(64) UNIQUE(app_id,hash), bundle bytea, size_bytes, created_at)`,
  `app_channels(app_id, channel CHECK IN ('production','draft'), version_id FK, updated_at, PK(app_id,channel))`,
  `app_viewer_tokens(token_id uuid PK, app_id FK CASCADE, salt bytea, secret_hash bytea, created_at, revoked_at)`,
  `app_render_logs(request_id uuid PK, app_id FK CASCADE, version_hash, channel, principal_kind, principal_id, started_at, duration_ms, outcome, log_text, error)`.
- Package `pkg/pipelineapi/apps/`: `store.go` (Postgres), `handlers.go`
  (author routes), `internal.go` (worker-facing routes), `tokens.go`
  (mint/verify), `identity.go` (FGA + impersonation glue).
- Store API (handlers depend on these exact names):
  `Create(ctx, projectID, name) (*App, error)`;
  `PutVersion(ctx, appID string, bundle []byte) (*Version, error)` (hash =
  hex SHA-256 of the **raw** bundle; stores it **gzip-compressed** in the
  `bytea` column with `size_bytes` = raw size; idempotent on same hash;
  moves draft channel; enforces the 5 MB raw cap `ErrBundleTooLarge` **and**
  the per-project 200 MB stored-quota `ErrProjectQuota` — spec §4);
  `Promote(ctx, appID, versionHash, expectedProduction string) error`
  (CAS; `ErrCASMismatch` → 409);
  `Resolve(ctx, projectID, name, channel string) (*Resolved, error)` where
  `Resolved{AppID, VersionID, VersionHash string}`;
  `GetBundle(ctx, hash string) ([]byte, error)` (decompresses on read);
  `MintToken(ctx, appID string) (tokenID, secret string, err error)` (secret
  = 32 bytes crypto/rand base64url; stored `SHA-256(salt||secret)`);
  `VerifyToken(ctx, appID, tokenID, secret string) (bool, error)`
  (constant-time; false for revoked);
  `AppendRenderLog(ctx, RenderLogRecord) error` + ring-buffer trim (keep
  newest 200/app **and** drop records older than the retention age, default
  14 days — both bounds, operator-tunable via `appWorker`/store config,
  spec §6.6); `GetRenderLogs(ctx, appID string, requestID string, limit int)`;
  version GC keeps the newest 20 unreferenced versions per app (spec §4).
- Author routes (registered beside existing pipeline routes):
  `PUT/GET/DELETE /api/v1/projects/{pid}/apps/{name}`, `GET …/apps`,
  `POST …/apps/{name}/promote` body `{"version":"<hash>","expectedProduction":"<hash|null>"}`,
  `GET …/apps/{name}/logs[?request_id=<id>]` (404 on unknown request_id),
  `POST …/apps/{name}/tokens`, `DELETE …/apps/{name}/tokens/{token_id}`.
- Internal routes (service-credential bearer, constant-time compare):
  `GET /internal/v1/apps/{pid}/{name}/resolve?channel=…` → `{app_id, version_hash}`;
  `GET /internal/v1/bundles/{hash}` (immutable, `Cache-Control: public, max-age=31536000, immutable`);
  `POST /internal/v1/viewer-tokens/verify` `{app_id, token_id, secret}` → `{ok}`;
  `POST /internal/v1/sessions/verify` `{pid}` + forwarded `Cookie`/`Authorization` → `{user_id, project_member}`;
  `POST /internal/v1/impersonate` `{app_id}` → `{token}` — a **fresh
  per-call** JWT (no caching anywhere; every mint audited). The JWT `sub` is
  `AppJWTSubject(appUUID)` (see identity below); the downstream OpenFGA
  check that results must map to `AppFGASubject(appUUID)` — the two helpers
  exist precisely so no double-prefix occurs. **P0 determines the mint
  mechanism**:
  whether `tokens.MintImpersonation` can be given an arbitrary subject
  string, or a new `tokens.MintAppToken(appUUID, projectID) (string, error)`
  is needed (the existing mint derives its subject from a platform
  `store.User`, which an app is not). The token's audience matches whatever
  the query route + downstream catalog path require (P0 records it from the
  interactive-storage-browse mint); P4 tests assert the minted JWT `sub`
  equals `AppJWTSubject(appUUID)`, and the resulting OpenFGA tuple/check
  target equals `AppFGASubject(appUUID)`.
  `POST /internal/v1/apps/{app_id}/logs` (append one RenderLogRecord).
- **App identity model** (P0 records the run-identity writer + exact subject
  format so apps mirror it):
  - **Two subject helpers, not one** (round-2 finding 1 — do NOT conflate
    the JWT claim with the FGA subject): `identity.AppJWTSubject(appUUID)
    string` = the raw `sub` claim the mint puts in the JWT, and
    `identity.AppFGASubject(appUUID) string` = the OpenFGA user string the
    `viewer` tuple is written for. **P0 determines their relationship** from
    the run-identity code: if Lakekeeper composes `user:oidc~<sub>` from a
    raw `sub`, then `AppJWTSubject` returns `app-<uuid>` and `AppFGASubject`
    returns `user:oidc~app-<uuid>` (no double-prefix); if the raw `sub`
    already carries the prefix, they coincide. Tests assert BOTH: the JWT
    claim value AND the FGA check target that results.
  - `type IdentityManager interface { Register(ctx, appID, projectID) error;
    Unregister(ctx, appID, projectID) error; Mint(ctx, appID, projectID) (token string, err error) }`
    — the **interface + a `recorderIdentity` fake are created in P1**
    (`pkg/pipelineapi/apps/identity.go` holds the interface + helpers; the
    concrete FGA/mint methods are left unimplemented/`panic("P4")` stubs).
    P2's store/handlers and P3's internal API depend only on the interface
    (the recorder fake asserts call order); **P4 fills in the concrete
    FGA+mint implementation** in the same file. This ordering is why P2, P3,
    P4 are sequential (see task index).
  - Create → `Register` writes the `viewer` tuple on
    `project:<lakekeeper-project-id>`; Delete → `Unregister` deletes the
    tuple FIRST, then the rows.
- **Query-route app principal (P5) — more than "accept the kind":** the
  route today mints a catalog credential from an authenticated platform
  `store.User`. For app renders it must instead accept the app
  impersonation JWT presented by app-worker, set the **app** as the audited
  principal (`query_audit.principal` = app subject, carrying its `jti`), and
  use/forward that token as the catalog credential (no re-mint from a
  non-existent user session). P0 records the exact query-route auth symbol;
  P5 adds the app-principal branch and an integration test through
  pipeline-api → query-worker (not a gate unit alone).

**app-worker (Part 3):**
- Binary `cmd/app-worker/` (root module) + `pkg/appengine/` (engine, above)
  + `pkg/appengine/outputdoc/` (schema.json v1 + validator) +
  `pkg/appengine/vegaspec/` (schema.json subset + validator).
- Cookie: name `datuplet_app_session`; value =
  `base64url(json_payload) + "." + base64url(hmac)` where `json_payload =
  {"app_id","token_id","exp"}` (JSON avoids delimiter ambiguity — a binary
  HMAC must never be `|`-split) and `hmac = HMAC-SHA256(key,
  base64url(json_payload))`; verify recomputes the MAC over the received
  first segment with `hmac.Equal` (constant-time) before parsing;
  attributes `HttpOnly; Secure; SameSite=Lax; Path=/apps/{pid}/{name}`;
  TTL 24 h.
- Env: `DATUPLET_API_URL`, `DATUPLET_APPWORKER_LISTEN` (default `:8090`),
  `DATUPLET_APPWORKER_SERVICE_TOKEN_FILE`,
  `DATUPLET_APPWORKER_COOKIE_KEY_FILE`, `DATUPLET_APPWORKER_*` limit
  overrides mirroring `appWorker.render.*` values.
- Error envelope `{error, kind, request_id}`; kinds `bad_request |
  unauthorized | app_not_found | render_error | timeout | rate_limited |
  capacity | unavailable`. JSON for `Accept: application/json`, minimal
  HTML page otherwise. Every `/apps/*` response sets
  `Referrer-Policy: no-referrer`.
- Response matrix (spec §4.2): navigation → shell HTML (block ignored);
  `Accept: application/json` → full OutputDoc; + `block=<id>` → single
  block; unknown id → `bad_request`.
- Rate limits: per-principal 60/min burst 10 keyed `(app_id, token_id)` or
  `(app_id, user_id)`; per-app 300/min; per-app in-flight 2; pool
  semaphore; 429/503 + `Retry-After: ceil(max over violated buckets)`, min 1.

**OutputDoc v1 (Parts 3/4/5):** `{outputDoc:1, title, blocks[],
refreshInterval?}`; block types `markdown|metric|table|chart|filter|tabs`;
every block has required unique `id`; ≤64 blocks; doc ≤2 MiB; chart =
`{type:"chart", library:"vega-lite", spec}` (subset per spec §6.4 — inline
`data.values` only, no `config`/`usermeta`/composition/`href`/`image`/`lookup`).

**CLI (Part 5):** `cmd/datuplet/apps.go`; subcommands `init put render logs
promote token get list delete`; uniform `--project <pid>` (flag > configured
default via existing cluster config > deterministic error); `-o json`
everywhere; `render` failure prints ONE object
`{error, kind, request_id, author_log}` (author_log null if lookup 404s).

**UI (Part 6):** `ui/product/pages/apps.js` (catalog) +
`ui/product/pages/app-detail.js`; routes `#/apps`, `#/apps/{name}`;
`ui/product/api.js` gains the author-route calls.

**Chart (Part 7):** `charts/datuplet-app/templates/app-worker/{deployment,service,networkpolicy}.yaml`
(mirror `templates/query-worker/`); `values.yaml` block `appWorker:
{enabled: true, replicas: 2, render: {timeoutS: 10, maxTimeoutS: 30,
memoryMiB: 128, maxMemoryMiB: 256, queriesPerRender: 10, maxQueriesPerRender: 25,
outputDocMaxBytes: 2097152, bundleMaxBytes: 5242880, perAppInflight: 2,
concurrency: 8}}`; template `fail` guard when `appWorker.enabled` and not
`queryWorker.enabled`; PDB `minAvailable: 1`; readiness only after engine
compile.

## Global Constraints (every task implicitly includes these)

- **Never push `main`, never `git tag`.** All work on
  `feat/rfc-028-user-apps`; lands via the one draft PR.
- `go build ./... && go test ./...` green **before every commit**; `make
  tidy` (never bare `go mod tidy`) after any `go.mod` change — multi-module
  repo (root, `tests/e2e/`, `components/*/`, `sdk/go*`), drift fails CI.
- Part 1 touches `components/queryengine/` — its own Go module: run Go
  commands with `-C components/queryengine`.
- **Never use `filepath.Join`/`path.Join` for storage paths** (repo rule;
  not expected here, stated for completeness).
- POC greenfield (spec §2): no data migration, no back-compat shims, no
  legacy formats.
- Spec limits verbatim (§7): render wall clock 10 s default / 30 s cap; WASM
  memory 128 MiB default / 256 MiB cap; 10 queries/render default / 25 cap;
  OutputDoc ≤2 MiB; bundle ≤5 MB; ≤64 blocks; per-app in-flight 2;
  `refreshInterval` clamped [15 s, 3600 s]; per-principal 60/min burst 10;
  per-app 300/min; verify cache ≤15 s; negative cache 60 s; 10 verify
  failures/min per (IP, app) → 429 `Retry-After: 60`; resolve cache ≤15 s;
  POST body cap 16 KiB; params ≤32 keys, key ≤64 chars, value ≤1 KiB, URL
  ≤8 KiB; log ≤64 KiB/render; ring buffer 200 renders or 14 days.
- Security invariants: viewer plaintext token transits exactly once
  (302-exchange); no request URLs or `Referer` in app-worker logs; guest
  never sees JWTs/credentials; app-worker holds zero storage credentials.
- macOS/BSD host: use file-edit tooling, never `sed -i`/GNU-only flags.
- Conventional commits, one logical commit per task.
- Chart/controller changes require `make e2e-k8s` on OrbStack before the PR
  is marked ready (Part 7 gate).
- The spec (Draft v10) is authoritative. If a code anchor (file:line) has
  drifted, follow the *named symbol* and note the drift in the commit
  message.

## Harness notes (orchestrator contract)

- **Subagent dispatch:** give each subagent its full task text verbatim,
  plus Global Constraints + the Cross-part interface contract + branch
  name, and nothing else. Model per task is in each Part's index — pass it
  explicitly (an omitted model silently inherits the most expensive one).
- **Per-task Codex gate (after the subagent commits, before the next
  task):** the orchestrator runs the review **in-session** via the
  `mcp__codex__codex` tool (NOT a subagent, NOT the CLI) — default account
  model (resolves to GPT-5 Codex), `config: {"model_reasoning_effort":
  "high"}`, `sandbox: "read-only"`, `cwd` = repo root. Prompt: review the
  task's diff (`git show <SHA>` / `git diff <base>..<SHA>`) for correctness
  and spec/contract compliance against
  `docs/superpowers/specs/2026-07-22-rfc-028-user-apps-wasm-workers-design.md`
  and this plan; reply on the same `threadId` for subsequent gates to keep
  context. Acceptance: **zero blocker/major findings on the task's diff**.
- **Finding protocol (maintainer's standing rule):** on any Critical/
  Important finding, STOP and present it to the maintainer — no fix is
  dispatched without explicit approval; after an approved fix, Codex
  re-reviews the task. A clean review needs no check-in — proceed. Minor
  findings are recorded in a ledger and rolled up to the final gate.
- **Phase gate (last task of each Part):** cumulative Codex review with
  base `<phase-start SHA>`, then the PR step per the Branching model.
- **Part 0 is a HARD maintainer gate:** after E2, present the numbers and
  STOP for an explicit GO/NO-GO (GO = QuickJS; NO-GO = StarlingMonkey or
  Approach B per spec §12.1). Parts 1–2 may proceed while waiting; Part 3
  may not start without GO.

---

# Part 0 — Engine spike (E)

**Goal:** Prove QuickJS-on-wazero end-to-end (compile → instantiate → eval
→ host call → promise settle) and measure the §7 assumptions: instantiate
time, eval time of a ~300 KB bundle, JSON round-trip of a 10 MiB result.
Produces the committed `engine.wasm` + build recipe + spike report.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| E0 | Commit 0 (spec+plan) + engine-wasm build recipe | sonnet | — | no |
| E1 | `pkg/appengine` minimal host + green integration test | opus | E0 | no |
| E2 | Benchmarks + spike report + **maintainer GO/NO-GO** | sonnet | E1 | no |

### Task E0: Commit 0 + engine.wasm build recipe

**Files:**
- Create: `utils/docker/engine-wasm.Dockerfile`, `pkg/appengine/shim/shim.c`,
  `pkg/appengine/shim/Makefile` note in root `Makefile` (`engine-wasm` target)
- Commits: `docs/superpowers/specs/2026-07-22-rfc-028-*.md`, this plan.

**Interfaces:**
- Produces: `make engine-wasm` → writes `pkg/appengine/embed/engine.wasm`;
  the shim ABI exactly as in the Cross-part contract.

- [ ] **Step 1:** `git checkout -b feat/rfc-028-user-apps main`; commit the
  spec + plan: `git add docs/superpowers && git commit -m "docs: RFC 028 spec (v10) + implementation plan (commit 0)"`.
- [ ] **Step 2:** Write `pkg/appengine/shim/shim.c` — the complete C shim:

```c
// shim.c — QuickJS-in-WASI shim for Datuplet app-worker (RFC 028).
// Exports: dtp_alloc, dtp_render. Imports (module "dtp_host"): query, log.
#include <stdlib.h>
#include <string.h>
#include "quickjs.h"

__attribute__((import_module("dtp_host"), import_name("query")))
extern unsigned long long host_query(const char *ptr, unsigned int len);
__attribute__((import_module("dtp_host"), import_name("log")))
extern void host_log(const char *ptr, unsigned int len);

__attribute__((export_name("dtp_alloc")))
void *dtp_alloc(unsigned int size) { return malloc(size); }

static JSValue js_host_query(JSContext *ctx, JSValueConst this_val,
                             int argc, JSValueConst *argv) {
    size_t len; const char *req = JS_ToCStringLen(ctx, &len, argv[0]);
    if (!req) return JS_EXCEPTION;
    unsigned long long packed = host_query(req, (unsigned int)len);
    JS_FreeCString(ctx, req);
    const char *resp = (const char *)(unsigned int)(packed >> 32);
    unsigned int rlen = (unsigned int)packed;
    JSValue out = JS_NewStringLen(ctx, resp, rlen);
    free((void *)resp);
    return out;
}

static JSValue js_host_log(JSContext *ctx, JSValueConst this_val,
                           int argc, JSValueConst *argv) {
    size_t len; const char *msg = JS_ToCStringLen(ctx, &len, argv[0]);
    if (!msg) return JS_EXCEPTION;
    host_log(msg, (unsigned int)len);
    JS_FreeCString(ctx, msg);
    return JS_UNDEFINED;
}

static char *pack_result(const char *s, unsigned long long *out_len) {
    *out_len = strlen(s);
    char *buf = malloc(*out_len);
    memcpy(buf, s, *out_len);
    return buf;
}

// Renders script (prelude+bundle) with ctx JSON; returns ptr<<32|len of
// result JSON ({"ok":true,"doc":...} | {"ok":false,"error":...,"stack":...}).
__attribute__((export_name("dtp_render")))
unsigned long long dtp_render(const char *script, unsigned int script_len,
                              const char *ctx_json, unsigned int ctx_len) {
    JSRuntime *rt = JS_NewRuntime();
    JS_SetMemoryLimit(rt, (size_t)-1); // real cap = wazero memory pages
    JSContext *ctx = JS_NewContext(rt);
    JSValue global = JS_GetGlobalObject(ctx);
    JS_SetPropertyStr(ctx, global, "__dtp_host_query",
        JS_NewCFunction(ctx, js_host_query, "__dtp_host_query", 1));
    JS_SetPropertyStr(ctx, global, "__dtp_host_log",
        JS_NewCFunction(ctx, js_host_log, "__dtp_host_log", 1));

    char *result = NULL;
    JSValue v = JS_Eval(ctx, script, script_len, "app.js", JS_EVAL_TYPE_GLOBAL);
    if (JS_IsException(v)) goto exception;
    JS_FreeValue(ctx, v);

    JSValue run = JS_GetPropertyStr(ctx, global, "__dtp_run");
    JSValue arg = JS_NewStringLen(ctx, ctx_json, ctx_len);
    v = JS_Call(ctx, run, JS_UNDEFINED, 1, &arg);
    JS_FreeValue(ctx, run); JS_FreeValue(ctx, arg);
    if (JS_IsException(v)) goto exception;
    JS_FreeValue(ctx, v);

    // Drain microtasks until the prelude stored the settled result.
    for (;;) {
        JSContext *pctx; int r = JS_ExecutePendingJob(rt, &pctx);
        if (r < 0) goto exception;
        if (r == 0) break;
    }
    JSValue res = JS_GetPropertyStr(ctx, global, "__dtp_result");
    const char *s = JS_ToCString(ctx, res);
    unsigned long long len; result = pack_result(s ? s : "{\"ok\":false,\"error\":\"no result\"}", &len);
    JS_FreeCString(ctx, s); JS_FreeValue(ctx, res); JS_FreeValue(ctx, global);
    JS_FreeContext(ctx); JS_FreeRuntime(rt);
    return ((unsigned long long)(unsigned int)result << 32) | (unsigned int)len;

exception:;
    JSValue exc = JS_GetException(ctx);
    JSValue stackv = JS_GetPropertyStr(ctx, exc, "stack");
    const char *emsg = JS_ToCString(ctx, exc);
    const char *stk = JS_IsUndefined(stackv) ? "" : JS_ToCString(ctx, stackv);
    JSValue eobj = JS_NewObject(ctx);
    JS_SetPropertyStr(ctx, eobj, "ok", JS_NewBool(ctx, 0));
    JS_SetPropertyStr(ctx, eobj, "error", JS_NewString(ctx, emsg ? emsg : "unknown"));
    JS_SetPropertyStr(ctx, eobj, "stack", JS_NewString(ctx, stk ? stk : ""));
    JSValue ejson = JS_JSONStringify(ctx, eobj, JS_UNDEFINED, JS_UNDEFINED);
    const char *es = JS_ToCString(ctx, ejson);
    unsigned long long elen; result = pack_result(es, &elen);
    // (free omitted-for-brevity values; runtime torn down next)
    JS_FreeContext(ctx); JS_FreeRuntime(rt);
    return ((unsigned long long)(unsigned int)result << 32) | (unsigned int)elen;
}
```

> **The block above is illustrative, not final.** Because this C compiles
> into the committed engine artifact, the implementer MUST make it complete
> and leak-clean before committing (Codex round-1 finding 5): every
> `JS_ToCString` paired with `JS_FreeCString`, every `JS_NewString`/
> `JS_GetPropertyStr`/`JS_JSONStringify` value freed on **both** the success
> and exception paths, null-checks before any `strlen`/`memcpy` (a failed
> `JS_JSONStringify` returns an exception, not a C null string — guard it),
> and no `JS_FreeContext` while values are still live. Factor one
> `pack_error(ctx, JSValue exc)` helper so success and exception paths share
> the frees. `TestExceptionPathStable` (E1) triggers the exception path
> 200× as the acceptance proof.

> **Author contract vs delivered form (finding 3).** Spec §6.2 says authors
> write "an ES module exporting `render(ctx)`" — that stays true: authors
> author ESM. The *delivered bundle* the engine evals is the esbuild output
> `--format=iife --global-name=__dtp_app` (§5.5 CLI / Appendix A), so the
> shim reaches the entry at `globalThis.__dtp_app.render` via
> `JS_EVAL_TYPE_GLOBAL` — no QuickJS module-loader/export-plumbing needed.
> This is a build-step transform, not a change to the author-facing
> contract; the CLI `apps init` scaffold (C1) and docs (D3) state it
> explicitly so no author is surprised.

- [ ] **Step 3:** Write `utils/docker/engine-wasm.Dockerfile` (wasi-sdk +
  quickjs-ng pinned tag, compiles `quickjs*.c` + `shim.c` with
  `-O2 -D_WASI_EMULATED_SIGNAL -lwasi-emulated-signal
  -Wl,--export=dtp_alloc,--export=dtp_render,--no-entry
  -mexec-model=reactor`) and the root `Makefile` target:

```make
engine-wasm: ## Build pkg/appengine/embed/engine.wasm (Docker, wasi-sdk)
	docker build -f utils/docker/engine-wasm.Dockerfile -o pkg/appengine/embed .
```

- [ ] **Step 4:** Run `make engine-wasm`; commit Dockerfile + shim +
  `pkg/appengine/embed/engine.wasm` (~1–2 MB) —
  `feat: QuickJS engine.wasm build recipe + shim (RFC 028 E0)`.

### Task E1: `pkg/appengine` minimal host + integration test

**Files:**
- Create: `pkg/appengine/engine.go`, `pkg/appengine/prelude.js`,
  `pkg/appengine/engine_test.go`, `pkg/appengine/embed/embed.go`
  (`//go:embed engine.wasm`)

**Interfaces:**
- Produces: the full Go API from the Cross-part contract (`NewEngine`,
  `Engine.Render`, `RenderInput`, `Limits`, `Result`, `RenderError`,
  `QueryFunc`). W-tasks consume it unchanged.

- [ ] **Step 1: Write `prelude.js`** (embedded via `//go:embed prelude.js`):

```js
"use strict";
(() => {
  const q = globalThis.__dtp_host_query, l = globalThis.__dtp_host_log;
  delete globalThis.__dtp_host_query; delete globalThis.__dtp_host_log;
  const cap = [];
  globalThis.console = { log: (...a) => cap.push(a.map(String).join(" ")) };
  globalThis.datuplet = {
    query(sql, params, opts) {
      const resp = JSON.parse(q(JSON.stringify({ sql, params: params ?? null, opts: opts ?? null })));
      if (resp.error) { const e = new Error(resp.error.message); e.kind = resp.error.kind; return Promise.reject(e); }
      return Promise.resolve(resp.result);
    },
  };
  globalThis.__dtp_settled = false;
  globalThis.__dtp_run = (ctxJson) => {
    const ctx = JSON.parse(ctxJson);
    Promise.resolve()
      .then(() => globalThis.__dtp_app.render(ctx))
      .then((doc) => { globalThis.__dtp_result = JSON.stringify({ ok: true, doc, log: cap.join("\n") }); })
      .catch((e) => { globalThis.__dtp_result = JSON.stringify({ ok: false, error: String(e && e.message || e), kind: e && e.kind || "", stack: String(e && e.stack || ""), log: cap.join("\n") }); })
      .finally(() => { globalThis.__dtp_settled = true; });
  };
})();
```

> **Handshake soundness (finding 4).** The shim's drain loop
> (`JS_ExecutePendingJob` until the queue empties) is not sufficient on its
> own: a `render` that awaits a promise which never enqueues a job (e.g. a
> never-resolving Promise) empties the job queue with `__dtp_settled ===
> false`. After the drain loop the shim MUST check `__dtp_settled`; if
> false, return an `ok:false` result with `error:"render did not settle"`
> — and the wazero wall-clock deadline (E1 `Render`) is the backstop that
> kills a guest that never yields at all. Tests in E1 cover: (a) `render`
> awaiting the host query (settles), (b) nested `await Promise.resolve()`
> microtasks (settles), (c) a never-resolving `await new Promise(()=>{})`
> (drain empties, `__dtp_settled` false → mapped to `render_error`), and
> (d) an infinite `for(;;){}` (deadline → `timeout`).

- [ ] **Step 2: Write the failing integration test** (`engine_test.go`):

```go
func TestRenderRoundTrip(t *testing.T) {
	e, err := NewEngine(context.Background(), 2048)
	if err != nil { t.Fatal(err) }
	bundle := []byte(`var __dtp_app = { render: async (ctx) => {
		const r = await datuplet.query("SELECT $x", {x: ctx.params.x});
		console.log("got rows");
		return { outputDoc: 1, title: "t", blocks: [{ id: "b1", type: "markdown", text: "rows: " + r.rows.length }] };
	}};`)
	q := func(_ context.Context, req []byte) ([]byte, error) {
		return []byte(`{"result":{"schema":[],"rows":[[1]],"truncated":false,"stats":{}}}`), nil
	}
	res, rerr := e.Render(context.Background(), RenderInput{
		Bundle: bundle, Path: "/", Params: map[string]string{"x": "1"},
		Now: time.Unix(1753228800, 0), Query: q,
		Limits: Limits{WallClock: 5 * time.Second, MaxQueries: 10, MaxOutputBytes: 2 << 20, MaxLogBytes: 64 << 10},
	})
	if rerr != nil { t.Fatalf("render error: %+v", rerr) }
	if !strings.Contains(string(res.Doc), `"rows: 1"`) { t.Fatalf("doc: %s", res.Doc) }
	if !strings.Contains(string(res.Log), "got rows") { t.Fatalf("log: %s", res.Log) }
}

func TestRenderInfiniteLoopIsKilled(t *testing.T) {
	e, _ := NewEngine(context.Background(), 2048)
	_, rerr := e.Render(context.Background(), RenderInput{
		Bundle: []byte(`var __dtp_app = { render: () => { for(;;){} } };`),
		Query:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
		Limits: Limits{WallClock: 300 * time.Millisecond, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
	})
	if rerr == nil || rerr.Kind != "timeout" { t.Fatalf("want timeout, got %+v", rerr) }
}

func TestRenderOOMIsTrapped(t *testing.T) {
	e, _ := NewEngine(context.Background(), 64) // 4 MiB engine memory limit
	_, rerr := e.Render(context.Background(), RenderInput{
		Bundle: []byte(`var __dtp_app = { render: () => { let a=[]; for(;;){ a.push(new Array(100000).fill(0)); } } };`),
		Query:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
		Limits: Limits{WallClock: 5 * time.Second, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
	})
	if rerr == nil || rerr.Kind != "render_error" { t.Fatalf("want render_error (memory trap), got %+v", rerr) }
}

func TestGuestGlobals(t *testing.T) {
	e, _ := NewEngine(context.Background(), 2048)
	// Real clock (WASI), Math.random present, ctx.now explicit, no fetch/timers.
	bundle := []byte(`var __dtp_app = { render: (ctx) => {
		const facts = { nowNum: (typeof ctx.now === "number"),
			dateOK: (typeof new Date().getUTCFullYear() === "number"),
			rnd: (typeof Math.random() === "number"),
			nofetch: (typeof fetch === "undefined"),
			notimer: (typeof setTimeout === "undefined") };
		return { outputDoc:1, title:"g", blocks:[{id:"b",type:"markdown",text: JSON.stringify(facts)}] };
	}};`)
	res, rerr := e.Render(context.Background(), RenderInput{
		Bundle: bundle, Now: time.Unix(1753228800, 0),
		Query:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
		Limits: Limits{WallClock: 5 * time.Second, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
	})
	if rerr != nil { t.Fatal(rerr) }
	for _, want := range []string{`"nowNum":true`, `"dateOK":true`, `"rnd":true`, `"nofetch":true`, `"notimer":true`} {
		if !strings.Contains(string(res.Doc), want) { t.Fatalf("missing %s in %s", want, res.Doc) }
	}
}

func TestExceptionPathStable(t *testing.T) {
	e, _ := NewEngine(context.Background(), 2048)
	// Exercise the shim's exception serialization repeatedly; a leak or
	// double-free in the C error path shows up as a crash/among-runs growth.
	for i := 0; i < 200; i++ {
		_, rerr := e.Render(context.Background(), RenderInput{
			Bundle: []byte(`var __dtp_app = { render: () => { throw new Error("boom") } };`),
			Query:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
			Limits: Limits{WallClock: 2 * time.Second, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
		})
		if rerr == nil || rerr.Kind != "render_error" || !strings.Contains(rerr.Msg, "boom") {
			t.Fatalf("iter %d: want render_error boom, got %+v", i, rerr)
		}
	}
}
```

Note the globals contract (spec §6.2 — **real clock, not deterministic**,
round-2 finding 7): `Date` and `Math.random` work via the instantiated WASI
functions (`clock_time_get`, `random_get`) that QuickJS's libc uses — no
manual prelude wiring and no determinism guarantee (spec: "none needed").
`ctx.now` is a **separate explicit field** (render-start ms, for stable
footers), independent of `Date`. `fetch`/`setTimeout`/`setInterval` are
absent (QuickJS provides no host I/O and the prelude adds none). The test
asserts `typeof new Date().getUTCFullYear() === "number"`,
`typeof Math.random() === "number"`, `ctx.now` is a number, and
`fetch`/`setTimeout` are `undefined` — it does NOT tie `Date` to `ctx.now`.
(The snippets use the final API: `NewEngine(ctx, memoryPages)` — `2048`
normally, `64` for the OOM test — and `Limits` has no `MemoryPages` field.)

- [ ] **Step 3:** Run `go test ./pkg/appengine/ -run TestRender -v` — FAIL
  (nothing implemented).
- [ ] **Step 4: Implement `engine.go`.** ONE concrete wazero lifecycle
  (finding 5 — no open choice left):
  - `NewEngine(ctx, memoryPages uint32)`: create ONE
    `wazero.NewRuntimeWithConfig(ctx,
    wazero.NewRuntimeConfig().WithCloseOnContextDone(true).WithMemoryLimitPages(memoryPages))`
    — **memory is a per-deployment config value, not per-render-varying**
    (spec §7's "128 MiB default / 256 MiB cap" is the operator-set value and
    its allowed ceiling; app-worker passes the configured pages here, having
    clamped to 4096 = 256 MiB). Instantiate WASI
    (`wasi_snapshot_preview1.MustInstantiate`), register the `dtp_host` host
    module ONCE (its functions read per-render state from the call
    `context.Context`, see below), and `runtime.CompileModule(engineWasm)`
    ONCE. Store runtime + compiled module on `*Engine`; concurrency-safe,
    shared across renders. (Memory pages are engine-level — there is no
    `Limits` memory field; OOM tests construct a low-memory engine, e.g.
    `NewEngine(ctx, 64)`.)
  - **Per-render host state via context (finding 5):** define
    `type renderState struct { q QueryFunc; queriesLeft int; log *capBuf;
    mu sync.Mutex }`. `Render` puts `*renderState` into the ctx
    (`context.WithValue`); the `dtp_host.query`/`log` host functions
    retrieve it with `ctx.Value` — this is how one shared host module
    serves concurrent renders without shared mutable state.
  - `Render`: `ctx, cancel := context.WithTimeout(ctx, in.Limits.WallClock)`;
    `defer cancel()`; `ctx = context.WithValue(ctx, stateKey, rs)`.
    Instantiate a **fresh module instance per render**
    (`runtime.InstantiateModule(ctx, compiled,
    wazero.NewModuleConfig().WithName(""))` — anonymous name so instances
    don't collide); `defer instance.Close(ctx)` ALWAYS. Write prelude+bundle
    + ctx JSON via `dtp_alloc`, call `dtp_render`, read the result. Fresh
    instance = fresh linear memory, reclaimed on Close; the runtime memory
    limit is the OOM bound (OOM test above is the proof).
  - host `dtp_host.query`: pull `rs` from ctx; read guest memory →
    `rs.q(ctx, …)` → `dtp_alloc` in the guest → write response → return
    packed u64; decrement `rs.queriesLeft` (0 → return an error envelope,
    kind `bad_request`); `rs.mu` serializes (assert no concurrent host call).
  - host `dtp_host.log`: pull `rs` from ctx; append to `rs.log` (capped at
    `MaxLogBytes`).
  - Result mapping: `ok:false` **with `ctx.Err() != nil`** → Kind `timeout`;
    `ok:false` otherwise (incl. `__dtp_settled==false` and memory traps) →
    `render_error`; enforce `MaxOutputBytes` on `doc` (over → `render_error`).
    A wazero trap (memory limit) surfaces as an instantiate/call error →
    `render_error`.
- [ ] **Step 5:** `go test ./pkg/appengine/ -v` — PASS; root
  `go build ./...` green (wazero dep added → `make tidy`).
- [ ] **Step 6:** Commit `feat: appengine QuickJS-on-wazero host (RFC 028 E1)`.

### Task E2: Benchmarks + spike report + maintainer GO/NO-GO

**Files:**
- Create: `pkg/appengine/bench_test.go`,
  `docs/superpowers/specs/2026-07-23-rfc-028-engine-spike-report.md`

- [ ] **Step 1:** Benchmarks: `BenchmarkInstantiate` (fresh instance, no
  eval), `BenchmarkEval300KB` (generated 300 KB bundle), `BenchmarkJSON10MiB`
  (render whose query returns a 10 MiB rows payload; measures parse +
  reshape + stringify inside the guest).
- [ ] **Step 2:** `go test ./pkg/appengine/ -bench . -benchmem -run XXX`;
  write the report: numbers vs §7 assumptions (render slot math), memory
  high-water, artifact size, and a recommendation.
- [ ] **Step 3:** Commit `docs: RFC 028 engine spike report (E2)`.
- [ ] **Step 4 (orchestrator):** Present numbers to the maintainer. **STOP.
  Part 3 does not start without explicit GO.** On NO-GO: spec §12.1
  fallbacks (StarlingMonkey via wasmtime-go, or Approach B) — new plan
  revision required for Part 3 only (Parts 1–2 are engine-independent).

---

# Part 1 — Query service bound params (Q)

**Goal:** `POST /api/v1/projects/{pid}/query` accepts `params` per spec
§6.1; queryproxy forwards; docs updated. Independently mergeable.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| Q0 | Preflight: anchors + go-duckdb named-arg probe | sonnet | commit 0 | no |
| Q1 | `ValidateParams` (grammar, both-ways check, types) | haiku | Q0 | no |
| Q2 | Worker wiring: request field + prepared exec + errors | opus | Q1 | no |
| Q3 | Proxy passthrough + docs + **Part 1 gate** (opens draft PR) | sonnet | Q2 | no |

### Task Q0: Preflight

- [ ] **Step 1:** Record anchors:

```bash
grep -n "type queryRequest" components/queryengine/cmd/query-worker/server.go   # ~:129
grep -n "func.*Exec\|QueryContext\|PrepareContext" components/queryengine/engine*.go | head
grep -n "type queryRequest\|sql\|timeout_s" pkg/pipelineapi/queryproxy/client.go | head
grep -n "bad_request\|sql_error" pkg/pipelineapi/queryproxy/*.go components/queryengine/cmd/query-worker/*.go | head
```

- [ ] **Step 2:** Named-arg probe (throwaway test in
  `components/queryengine/`): prepare `SELECT $a::INT + 1` and execute with
  `sql.Named("a", 1)` against the embedded engine. Record PASS/FAIL in the
  task report. FAIL ⇒ Q2 uses the quoted-span-aware rewrite of validated
  `$name` tokens to positional `?` (the scanner from Q1 already yields
  spans).
- [ ] **Step 3:** No commit (throwaway probe deleted) unless anchors
  drifted — then update this plan's contract section and commit
  `docs(plan): Q0 anchor drift`.

### Task Q1: `ValidateParams`

**Files:**
- Create: `components/queryengine/params.go`,
  `components/queryengine/params_test.go`

**Interfaces:**
- Produces: `func ValidateParams(sql string, params map[string]any) ([]string, error)`
  and `type ParamError struct{ Msg string }` (worker maps it → 400
  `bad_request`). Also `func placeholderSpans(sql string) []span` reused by
  the positional fallback.

- [ ] **Step 1: Failing tests** — table-driven, the complete §6.1 matrix:
  grammar accept/reject (`$a`, `$a_1`, `$A`, 64-char name ok / 65 reject,
  `$1x` reject); `$name` inside `'…'`/`"…"` NOT a placeholder (test
  `SELECT '$x' WHERE a = $y` → refs `[y]`); repeated placeholder = one ref;
  missing param → error naming it; unreferenced key → error naming it;
  value types: string/bool/int within safe range OK; float OK;
  `float64(1<<53)` + `-(1<<53)` rejected (message contains
  `MAX_SAFE_INTEGER`); nested map/array rejected; null OK.
- [ ] **Step 2:** Run `go test -C components/queryengine -run TestValidateParams -v` — FAIL.
- [ ] **Step 3:** Implement: single-pass scanner tracking quote state
  (`'`, `"`, doubled-quote escapes); regex-free name capture; ordered
  distinct names; both-ways set check; type switch per §6.1 table
  (`json.Number`-aware — the worker decodes with `UseNumber()`).
- [ ] **Step 4:** PASS; commit
  `feat(queryengine): bound-parameter validation (RFC 028 Q1)`.

### Task Q2: Worker wiring

**Files:**
- Modify: `components/queryengine/cmd/query-worker/server.go` (request
  struct + handler), `components/queryengine/engine.go` /
  `engine_open.go` (exec path takes `[]sql.NamedArg`)
- Test: extend `components/queryengine/integration_test.go`

- [ ] **Step 1: Failing integration tests:** params bind and return correct
  rows (`SELECT $a::INT AS v` with `{"a": 7}` → `[[7]]`); string date cast
  works; missing param → HTTP 400 kind `bad_request`; type-violating param
  → 400 `bad_request`; DuckDB bind error (e.g. `$a` used as identifier) →
  400 `sql_error`; NO params field → existing behavior (regression guard,
  reuse an existing passing case); **cancellation regression: a slow
  params query is cancelled by request-context timeout exactly as the
  no-params path is** (finding 8 — assert the same interrupt/watchdog
  behavior, not just that an error is returned).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement — **preserve the existing interrupt-safe
  execution path**. Q0 records how the engine runs SQL today (the current
  path deliberately uses a single `QueryContext` for cancellation safety);
  the params path MUST use the same `QueryContext(ctx, sql, args...)` call
  with the named args appended, NOT a separate `Prepare`/`ExecContext`
  route that could escape cancellation. Decode the request with
  `UseNumber()`; call `ValidateParams`; convert values (`json.Number` →
  int64/float64 per §6.1; string/bool/nil passthrough) into `sql.Named`
  args passed straight to `QueryContext`. If Q0's probe showed named args
  are unsupported, apply the quoted-span-aware `$name`→`?` rewrite (spans
  from `placeholderSpans`) and pass positional args to the SAME
  `QueryContext` call. Error mapping per Q1.
- [ ] **Step 4:** `go test -C components/queryengine ./... -v` PASS; commit
  `feat(queryengine): execute bound params via prepared statements (RFC 028 Q2)`.

### Task Q3: Proxy passthrough + docs + Part 1 gate

**Files:**
- Modify: `pkg/pipelineapi/queryproxy/client.go`, `handler.go` (+ tests),
  `docs/ad-hoc-query.md` (§ request body: `params` field, grammar, bind
  table, error kinds — copy the §6.1 table verbatim)

- [ ] **Step 1:** Failing handler test: POST body with `params` reaches the
  fake worker verbatim; response passthrough unchanged.
- [ ] **Step 2:** Implement passthrough (one optional field on both
  structs); PASS.
- [ ] **Step 3:** Docs edit; root `go build ./... && go test ./...`.
- [ ] **Step 4:** Commit `feat(queryproxy): forward bound params + docs (RFC 028 Q3)`.
- [ ] **Step 5 (gate):** cumulative Codex review (base = Part-1 start SHA);
  on clean: push branch, `gh pr create --draft` (THE PR), report PR number.

---

# Part 2 — Control plane (P)

**Goal:** pipeline-api stores apps/versions/channels/tokens/logs, serves
the author routes and the six internal endpoints, registers FGA app
identities, and accepts impersonation principals on the query route.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| P0 | Preflight: route registration, FGA writer, session resolver anchors | sonnet | Part 1 gate | no |
| P1 | Migration 013 + store + `IdentityManager` interface/recorder + unit tests | sonnet | P0 | no |
| P2 | Author routes (CRUD, promote CAS, logs, tokens) | opus | P1 | no |
| P3 | Internal API (6 endpoints, service credential) | opus | P2 | no |
| P4 | FGA identity + app-token mint + audit events | opus | P3 | no |
| P5 | Query route — app impersonation principal | sonnet | P4 | no |
| P6 | **Part 2 gate** | sonnet | P1–P5 | no |

(P3 is **not** parallel with P2 — both modify the pipeline-api route
registration file, so they serialize per the file-disjoint rule, round-2
finding 3. P4 follows P3 because it fills in the `IdentityManager` stub P1
created and P2/P3 consume.)

### Task P0: Preflight

- [ ] **Step 1:** Record anchors (report exact symbols to the orchestrator):

```bash
grep -rn "HandleFunc\|mux\|Route(" pkg/pipelineapi/http/*.go | grep -i "pipelines" | head -5   # author-route registration point
grep -rn "oidc~" pkg/pipelineapi/ | head -5                                                    # run-identity FGA tuple writer
grep -n "func MintImpersonation" pkg/pipelineapi/tokens/*.go                                    # exact signature
grep -n "SessionCookieName\|func.*Resolve" pkg/pipelineapi/auth/middleware.go | head           # session resolver for sessions/verify
ls pkg/pipelineapi/db/migrations/ | tail -2                                                     # expect 012_* latest
grep -rn "token_kind" pkg/pipelineapi/ | head -5                                                # principal kinds accepted today
```

- [ ] **Step 2:** If `MintImpersonation`'s signature differs from
  `(subject, projectID string, ttl time.Duration)`-shaped assumptions, or
  the FGA writer isn't reusable as-is, update the P3/P4 task notes in the
  committed plan (plan is code) and commit `docs(plan): P0 anchor drift`.

### Task P1: Migration 013 + store + IdentityManager seam

**Files:**
- Create: `pkg/pipelineapi/db/migrations/013_user_apps.sql` (DDL verbatim
  from the Cross-part contract), `pkg/pipelineapi/apps/store.go`,
  `pkg/pipelineapi/apps/store_test.go`, `pkg/pipelineapi/apps/identity.go`
  (the `IdentityManager` interface + `AppJWTSubject`/`AppFGASubject`
  helpers + a `recorderIdentity` test fake; concrete methods are `panic
  ("implemented in P4")` stubs), `pkg/pipelineapi/apps/identity_test.go`
  (subject-helper unit tests only)

- [ ] **Step 1: Failing store tests** (against the test-Postgres harness
  the existing store tests use — P0 confirms the pattern): create app;
  duplicate name → error; `PutVersion` sets draft channel + returns
  64-hex hash of the RAW bundle; **stored bytes are gzip-compressed and
  `GetBundle` round-trips to the exact raw bundle**; same-bytes re-put
  idempotent (same version id); >5 MB raw bundle rejected
  (`ErrBundleTooLarge`); **per-project stored quota: putting versions past
  200 MB → `ErrProjectQuota`**; `Promote` happy path; CAS mismatch →
  `ErrCASMismatch`; promote unknown hash → error; `Resolve` per channel
  (`production` unset → `ErrNotFound`); `MintToken` returns
  `vw_`-composable parts, `VerifyToken` true / false-after-revoke /
  constant-time hash compare; `AppendRenderLog` + **trim by BOTH bounds:
  newest-200 AND records older than the retention age (test with an
  injected clock/`started_at`, default 14 days) dropped** + `GetRenderLogs`
  by request_id (miss → `ErrNotFound`); version GC keeps newest 20
  unreferenced (test the 21st is collected).
- [ ] **Step 2:** FAIL → implement (`crypto/rand` salt+secret;
  `sha256(salt||secret)`; gzip on write / gunzip on read; quota accounting;
  dual-bound log trim; version GC) → PASS.
- [ ] **Step 3:** Commit `feat(pipelineapi): user-apps store, migration 013, identity seam (RFC 028 P1)`.

### Task P2: Author routes

**Files:**
- Create: `pkg/pipelineapi/apps/handlers.go` + `handlers_test.go`
- Modify: route registration file found in P0

- [ ] **Step 1: Failing httptest cases:** PUT multipart? NO — body is JSON
  `{"bundle_base64": "...", }` (spec §12 resolved note: JSON+base64,
  simplest for CLI and UI alike; document in handler comment); name
  validated against `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`; PUT → 200
  `{app_id, version_hash}`; GET detail (channels + recent versions); list;
  DELETE (tuple-then-rows order asserted via a recorder fake of the
  `IdentityManager` interface — defined in the contract, so P2 depends only
  on the interface, not on P4's implementation); promote 200 / 409 CAS /
  400 unknown-version;
  logs list + `?request_id` 404; token create returns
  `{token_id, token: "vw_<token_id>.<secret>"}` exactly once; token
  delete; ALL routes 401 without auth, 403 for non-member (reuse existing
  authz middleware pattern — P0 anchor).
- [ ] **Step 2:** FAIL → implement → PASS → commit
  `feat(pipelineapi): app author routes (RFC 028 P2)`.

### Task P3: Internal API

**Files:**
- Create: `pkg/pipelineapi/apps/internal.go` + `internal_test.go`
- Modify: route registration; `charts` NOT touched here (D-part owns values)

- [ ] **Step 1: Failing httptest cases** per the Cross-part contract table:
  each endpoint's happy path; every endpoint 401 on missing/wrong service
  token (constant-time compare against
  `DATUPLET_APPS_INTERNAL_TOKEN_FILE` contents); `resolve` 404 unknown;
  `bundles/{hash}` immutable header + 404; `viewer-tokens/verify` ok:false
  on bad secret + revoked; `sessions/verify` forwards the caller's Cookie
  header to the existing session resolver and returns
  `{user_id, project_member}` (fake resolver in test); `impersonate`
  calls `IdentityManager.Mint` (recorder fake here; the real mint is P4)
  and refuses unknown app_id; `logs` append. **Every endpoint returns the
  standard error envelope** on failure (assert `{error, kind}` shape, not
  just status). **v1 uses ONE unscoped internal service credential** — so
  there is a 401 path (missing/wrong token) but no 403 scoped-credential
  path; state this explicitly in the handler comment so the omission is
  intentional, not forgotten.
- [ ] **Step 2:** FAIL → implement → PASS → commit
  `feat(pipelineapi): app-worker internal API (RFC 028 P3)`.

### Task P4: FGA identity + app-token mint + audit events

**Files:**
- Fill in: `pkg/pipelineapi/apps/identity.go` — implement the concrete
  `IdentityManager` methods (the interface + `AppJWTSubject`/`AppFGASubject`
  helpers + recorder fake were created in P1); + test
- Modify: `handlers.go` create/delete paths to call `IdentityManager`

**Interfaces:**
- Fills in: `IdentityManager` implementation (`Register`/`Unregister`/`Mint`)
  over the P1 helpers `AppFGASubject(appUUID)` (the OpenFGA user string the
  `viewer` tuple targets) and `AppJWTSubject(appUUID)` (the raw JWT `sub`).
- `Mint` uses the mechanism P0 chose (extended `MintImpersonation` or new
  `MintAppToken`) and returns a fresh 60 s JWT whose `sub` ==
  `AppJWTSubject`.

- [ ] **Step 1:** Failing tests with the recorded-FGA fake used by run
  identities (P0 names the exact helper; if it is not an ordering recorder,
  create a local `recorderFGA` wrapping it): `Register` writes the `viewer`
  tuple for **`AppFGASubject(appUUID)`**; `Unregister`/Delete removes the
  tuple FIRST then rows (order asserted via recorder); **`Mint` produces a
  JWT whose `sub` equals `AppJWTSubject(appUUID)`**, and the resulting
  OpenFGA authorization check targets `AppFGASubject(appUUID)` (assert both;
  no double-`user:oidc~` prefix — decode with the tokens package's test JWKS
  helper P0 names) with a 60 s TTL; unknown app → error; audit log lines
  `app_identity_created`, `app_identity_deleted`,
  `impersonation_minted{app_id, jti}`, `app_promoted{from,to}`,
  `viewer_token_{minted,revoked}` (structured slog, assert via capture).
- [ ] **Step 2:** FAIL → implement → PASS → commit
  `feat(pipelineapi): app FGA identity, app-token mint, control-plane audit (RFC 028 P4)`.

### Task P5: Query route — app impersonation principal (not just "accept the kind")

**Files:**
- Modify: the query-route auth path (P0 records the exact symbol — the
  route today derives a `store.User` and mints a catalog credential from
  it; `pkg/pipelineapi/queryproxy/gate.go` + wherever the catalog cred is
  produced)
- Test: extend `pkg/pipelineapi/queryproxy/gate_test.go` **and** add an
  integration test through pipeline-api → query-worker

**Why this is not a one-liner (finding 1):** an app impersonation JWT is
not a platform API user session. The route must (a) accept
`token_kind=impersonation` with an app subject, (b) set the **app** as the
audited principal so `query_audit.principal` = the app subject and carries
its `jti`, and (c) use/forward that impersonation token as the catalog
credential rather than trying to re-mint one from a non-existent
`store.User`.

- [ ] **Step 1:** Failing tests: a JWT with `token_kind=impersonation`,
  `sub` = `identity.AppJWTSubject(appUUID)` (whose FGA mapping is
  `AppFGASubject(appUUID)`), valid
  exp/aud passes the gate; `query_audit` records that app subject + jti;
  the catalog credential handed downstream is that same token (assert via a
  fake catalog/queryproxy backend that echoes the presented credential);
  a run-kind token still refused if that's today's behavior (regression
  guard — P0 records the current matrix).
- [ ] **Step 2:** Integration test: app-worker's `APIClient.Query` (or a
  direct HTTP call mimicking it) → pipeline-api query route → a fake/real
  query-worker; assert the app principal reaches audit and a blocking query
  is cancelled when the caller's context is cancelled (ties to W5 finding 9).
- [ ] **Step 3:** FAIL → implement the app-principal branch → PASS → commit
  `feat(queryproxy): app impersonation principal + catalog credential (RFC 028 P5)`.

### Task P6: Part 2 gate

- [ ] Full `go build ./... && go test ./...` + `make tidy`; cumulative
  Codex review (base = phase-start SHA); STOP on Critical/Important per
  protocol; push; append phase summary to the PR body.

---

# Part 3 — app-worker (W)

**Goal:** the stateless render service: resolve→auth→render→respond, with
every §5.3/§7 limit enforced and the §4.2 response matrix.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| W0 | Preflight + binary skeleton + config/env | sonnet | Part 0 GO + Part 2 gate | no |
| W1 | OutputDoc schema + validator (`outputdoc/`) | haiku | W0 | with W2 |
| W2 | Vega-Lite subset schema + validator (`vegaspec/`) | opus | W0 | with W1 |
| W3 | pipeline-api client + caches (resolve/bundle/verify/session) | sonnet | W0 | no |
| W4 | Viewer auth: exchange, cookie, CSRF, rate limits | opus | W3 | no |
| W5 | Render pipeline: query host fn, deadline coupling, semaphore | opus | W1, W3 | with W4 |
| W6 | HTTP wiring: response matrix, envelopes, access log, /readyz | opus | W4, W5 | no |
| W7 | **Part 3 gate** | sonnet | W0–W6 | no |

### Task W0: Skeleton

**Files:**
- Create: `cmd/app-worker/main.go` (flag/env parsing → `appworker.Serve`),
  `pkg/appworker/config.go` (+ test: env parsing, defaults = spec table,
  caps enforced), `pkg/appworker/server.go` (mux stub returning 503
  `unavailable`), `utils/docker/app-worker.Dockerfile` (mirror an existing
  service Dockerfile incl. BuildKit cache mounts)

**Interfaces:**
- Produces: `func (c Config) MemoryPages() uint32` — converts configured
  `render.memoryMiB` to wazero pages (`memoryMiB * 16`, since a page is
  64 KiB), clamped to the 256 MiB cap (4096 pages). `Serve` calls
  `appengine.NewEngine(ctx, cfg.MemoryPages())` at boot **before** the
  readiness gate flips (W6), so a fresh pod never renders on an uncompiled
  engine.

- [ ] Steps: failing config test (defaults/caps **and**
  `MemoryPages()`: 128 MiB → 2048, 512 MiB clamps → 4096) → implement →
  a test asserting `Serve` passes `cfg.MemoryPages()` into `NewEngine`
  (inject a fake engine constructor) → build image locally (`docker build`)
  → commit `feat(app-worker): binary skeleton + config (RFC 028 W0)`.

### Task W1: OutputDoc validation

**Files:**
- Create: `pkg/appengine/outputdoc/schema.json`, `outputdoc/validate.go`,
  `outputdoc/validate_test.go`

**Interfaces:**
- Produces: `func Validate(doc []byte) error` (returns first violation,
  message names the JSON pointer) — used by W5 and by the shell docs.
  Schema id `https://datuplet.io/schemas/outputdoc-v1.json`.

- [ ] **Step 1:** Failing tests: valid doc passes; missing `outputDoc:1`
  fails; unknown block type; missing/duplicate `id`; >64 blocks; block
  extra fields rejected (`additionalProperties: false` everywhere); filter
  field schema; tabs shape; `refreshInterval` bounds [15,3600]; chart block
  requires `library:"vega-lite"` + `spec` object (subset validated
  separately in W2).
- [ ] **Step 2:** Write `schema.json` (draft 2020-12; santhosh-tekuri v6
  compiles it) covering every §6.3 block field enumerated in the spec;
  implement `Validate`; PASS; commit
  `feat(appengine): OutputDoc v1 schema + validation (RFC 028 W1)`.

### Task W2: Vega subset

**Files:**
- Create: `pkg/appengine/vegaspec/schema.json`, `vegaspec/validate.go`,
  `vegaspec/validate_test.go`

- [ ] **Step 1:** Failing tests = the spec's mandatory negative list
  verbatim: `data.url`, `data.name`, `href` encoding channel, `image`
  mark, `mark.href`, `lookup` transform, `layer`/`facet`/`concat`/`repeat`,
  `resolve`, `params`, `usermeta`, any `config` key, and the
  explicitly-excluded transforms `loess`/`regression`/`density`/`quantile`
  → all rejected; the Appendix A bar-chart spec → accepted; every §6.4
  allowlisted mark type, encoding channel, channel-def key, transform
  accepted (table-driven from the §6.4 lists copied verbatim).
- [ ] **Step 2:** Write the subset `schema.json` exactly as §6.4 enumerates
  (top-level `title, description, width, height, data, mark, encoding,
  transform, $schema`; `additionalProperties: false` at every level);
  implement; PASS; commit
  `feat(appengine): Vega-Lite restricted subset schema (RFC 028 W2)`.

### Task W3: pipeline-api client + caches

**Files:**
- Create: `pkg/appworker/apiclient.go` + `apiclient_test.go` (httptest fake
  pipeline-api)

**Interfaces:**
- Produces: `type APIClient` with `Resolve(ctx, pid, name, channel) (Resolved, error)`
  (15 s TTL cache, keyed pid|name|channel), `Bundle(ctx, hash) ([]byte, error)`
  (content-addressed LRU ≤256 MiB, SHA-256 verified — mismatch = error),
  `VerifyToken(ctx, appID, tokenID, secret) (bool, error)` (positive cache
  15 s, negative 60 s), `VerifySession(ctx, pid string, cookies, authz string) (SessionInfo, error)`
  (cache 15 s per session), `Impersonate(ctx, appID) (string, error)`
  (**never cached — one fresh mint per render**, spec §5.4: per-render 60 s
  JWT, every mint audited server-side; reusing a jti across renders is a
  spec violation), `AppendLog(ctx, RenderLogRecord) error`
  (async, drop-on-full queue, never blocks a render), `Query(ctx, pid string, jwt string, body []byte) (*http.Response, error)`.

- [ ] Steps: table of failing tests per method (TTL behavior with a fake
  clock; hash-verify failure; negative-cache path) → implement → PASS →
  commit `feat(app-worker): pipeline-api client + caches (RFC 028 W3)`.

### Task W4: Viewer auth

**Files:**
- Create: `pkg/appworker/auth.go` + `auth_test.go`

**Interfaces:**
- Produces: `func (s *Server) authenticate(w, r, resolved) (principal, ok)`
  where `principal{Kind string /* viewer_token|platform_user */; ID string}`;
  `signCookie(key, appID, tokenID string, exp time.Time) string` +
  `parseCookie` (used in tests and W6); rate-limiter registry
  `allowRender(principalKey, appKey string) (ok bool, retryAfter int)`.

- [ ] **Step 1: Failing tests:**
  - `?token=vw_<id>.<secret>` + Verify ok → 302 to token-free URL,
    `Set-Cookie` with exact attributes
    (`HttpOnly; Secure; SameSite=Lax; Path=/apps/<pid>/<name>`, 24 h exp),
    `Referrer-Policy: no-referrer` on the 302 itself.
  - malformed/unknown/revoked token → 403 envelope; 11th failure in a
    minute from one IP+app → 429 `Retry-After: 60`.
  - valid cookie for app A replayed on app B → 401 `unauthorized`
    (`cookie.app_id == resolved.app_id` enforced).
  - revoked token: cookie still signature-valid → next request re-checks
    `(app_id, token_id)` after 15 s TTL → 401.
  - `@draft`: no viewer token accepted, ever; platform session via
    `VerifySession` → principal `{platform_user, user_id}`; non-member →
    403.
  - bearer `Authorization` (CLI) → `VerifySession` path too (bearer
    forwarded), CSRF checks skipped; cookie-auth POST without
    `X-Datuplet-App-Render: 1` + same-origin `Origin` → 403.
  - rate limiter: 61st render in a minute per token → 429 with computed
    `Retry-After ≥ 1`; per-app 301st → 429; burst 10 honored
    (`golang.org/x/time/rate`, fake clock).
- [ ] **Step 2:** FAIL → implement → PASS → commit
  `feat(app-worker): viewer auth, cookies, CSRF, rate limits (RFC 028 W4)`.

### Task W5: Render pipeline

**Files:**
- Create: `pkg/appworker/render.go` + `render_test.go` (fake APIClient +
  real appengine Engine)

**Interfaces:**
- Produces: `func (s *Server) render(ctx, resolved, path, params, principal) (doc json.RawMessage, rerr *appengine.RenderError)`
  — wires `appengine.Engine` with a `QueryFunc` that: enforces the query
  budget; clamps `timeoutS` to `min(opts.timeoutS, floor(remaining))`;
  remaining <1 s → local `timeout` error WITHOUT an HTTP call; forwards to
  `APIClient.Query` with the impersonation JWT; maps the query-service
  error envelope onto the guest error `{kind, message}`. After render:
  `outputdoc.Validate` + `vegaspec.Validate` on every chart block +
  MaxOutputBytes; then `AppendLog` (request_id, principal fields, outcome).
  Two admission gates with **explicit, distinct acquisition policies**
  (round-6 finding 2 — do not conflate blocking with `capacity`):
  - **Per-app in-flight** (`perAppInflight`, default 2): a per-`app_id`
    counter acquired **non-blocking** (`TryAcquire`); on failure → return
    `rate_limited` immediately (this is the app's own concurrency ceiling,
    the caller should back off).
  - **Pool semaphore** (`concurrency`, whole-pod): acquired with a **short
    bounded wait** (e.g. `ctx` with a 250 ms sub-deadline); if it cannot be
    acquired within that window → return `capacity` (the pod is saturated;
    HPA/other replicas should take it). Never an unbounded block, so a busy
    pod sheds load instead of queueing forever.
  Both return the standard 429/503 envelope + `Retry-After` (§5.3).

- [ ] **Step 1: Failing tests:** happy path (Appendix-A-like bundle against
  a fake query backend) → doc validates; query error caught by app → doc
  still renders (bundle catches and emits markdown); uncaught → Kind
  `render_error` with stack captured in log record; deadline expiry
  cancels in-flight query (fake Query blocks on ctx — assert ctx canceled);
  <1 s remaining → no HTTP call recorded; 11th query in one render →
  guest error kind `bad_request`; oversized doc → `render_error`; chart
  with `data.url` → `render_error` naming the pointer; **per-app gate:
  in-flight 2 + a 3rd concurrent render for the same app → `rate_limited`
  (non-blocking, observed immediately); pool gate: `concurrency` 1 + a 2nd
  render for a different app, with the first still holding the slot past the
  250 ms window → `capacity`** (both outcomes are actually observed, not
  just "the second waits").
- [ ] **Step 2:** FAIL → implement → PASS → commit
  `feat(app-worker): render pipeline with limits + validation (RFC 028 W5)`.

### Task W6: HTTP wiring

**Files:**
- Modify: `pkg/appworker/server.go`; Create: `server_test.go`

- [ ] **Step 1: Failing tests** (full httptest server, fake pipeline-api):
  - Routing: `GET /apps/{pid}/{name}` (+ `@draft` suffix parsing, sub-path
    → ctx.path ≤256 chars, `..` rejected).
  - Resolve-first order asserted (recorder: resolve called before verify).
  - Response matrix: navigation → HTML containing `<div id="dtp-root">` +
    embedded doc JSON + CSP header string from the contract; JSON accept →
    full doc; JSON + `block=kpis` → that block only; unknown block id →
    400 `bad_request`; every error as envelope (JSON) or HTML page by
    Accept.
  - Params normalization: >32 keys / key >64 / value >1 KiB / URL >8 KiB →
    400; duplicate key last-wins; POST requires `application/json`, body
    ≤16 KiB pre-parse; reserved `token`/`block` stripped from ctx.params.
  - Headers on every `/apps/*` response: `Referrer-Policy: no-referrer` +
    CSP on HTML; no URL/Referer in captured access logs; access-log record
    carries principal_kind/principal_id/params-hash (hash computed AFTER
    stripping).
  - `/readyz` 503 before `NewEngine` completes, 200 after (engine compile
    at boot, gated readiness); `/healthz` always 200.
  - **Metrics (spec §9, finding 17):** `/metrics` exposes
    `datuplet_appworker_render_requests_total{outcome}` (mirrors the query
    service's `pipelineapi_query_requests_total` labeling) incremented on
    every render terminal outcome; test asserts the counter moves per
    outcome label (`ok`, `render_error`, `timeout`, `rate_limited`,
    `capacity`, `unauthorized`, `bad_request`, `unavailable`).
- [ ] **Step 2:** FAIL → implement (shell HTML template is a W6 stub —
  `<!doctype html>` + root div + doc JSON in a `<script type="application/json">`
  tag; Part 4 replaces the asset) → PASS → commit
  `feat(app-worker): HTTP server, response matrix, access log, metrics (RFC 028 W6)`.

### Task W7: Part 3 gate

- [ ] Full build+test; `make tidy`; cumulative Codex review; STOP protocol;
  push + PR body update. Also: run the W5 happy path against a REAL
  pipeline-api + query-worker on OrbStack manually (orchestrator smoke, no
  new code) and note the result in the PR body.

---

# Part 4 — Trusted viewer shell (V)

**Goal:** the platform-owned browser shell: block renderers, interactivity
contract (§6.3), print CSS, CSV escaping — **and it should look good**:
reuse `ui/product/style.css` design tokens for a consistent product feel.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| V0 | Shell skeleton: embed.FS, index.html, boot, vendored libs | sonnet | Part 3 gate | no |
| V1 | Block renderers: markdown/metric/table/chart | opus | V0 | no |
| V2 | Interactivity: filters, tabs, modals, partial refresh, refreshInterval | opus | V1 | no |
| V3 | Export + polish: CSV escaping, print CSS, loading/stale states, theme | sonnet | V2 | no |
| V4 | **Part 4 gate** (incl. manual browser smoke checklist) | sonnet | V0–V3 | no |

### Task V0: Shell skeleton

**Files:**
- Create: `ui/appshell/index.html`, `ui/appshell/shell.js`,
  `ui/appshell/theme.css`, `ui/appshell/vendor/{vega.min.js,
  vega-lite.min.js, vega-embed.min.js, purify.min.js, marked.min.js}`
  (pinned versions recorded in `ui/appshell/vendor/VERSIONS`),
  `ui/appshell/vegaspec.schema.json`
- Modify: `pkg/appworker/server.go` — `//go:embed` the shell dir; serve
  `/apps/-/shell/*` static routes; W6's HTML stub becomes the real
  `index.html` render (doc JSON embedded, scripts `self`-hosted only).

**Shared Vega schema (finding 18):** the restricted subset schema is ONE
source of truth. `ui/appshell/vegaspec.schema.json` is a byte-copy of
`pkg/appengine/vegaspec/schema.json` (W2). Add a `make sync-appshell-schema`
target that copies it and a CI drift check (`git diff --exit-code` after
sync — mirror `make sync-component-schemas` from RFC 027), plus a Go test
`TestVegaSchemaInSyncWithShell` comparing the two files' bytes. The shell
validates specs client-side against this copy as defense-in-depth; the
server (W2/W5) remains the authoritative gate.

- [ ] Steps: write boot (`shell.js` reads the embedded doc, renders title,
  iterates blocks through a `RENDERERS` registry rendering `unknown block`
  placeholders); Go test asserting the HTML references only same-origin
  assets (CSP compliance); vega-embed initialized with
  `{actions:false, loader:{load:()=>Promise.reject(new Error("loading disabled"))}}`;
  commit `feat(appshell): shell skeleton + vendored libs (RFC 028 V0)`.

### Task V1: Block renderers

**Files:**
- Create: `ui/appshell/blocks/markdown.js`, `blocks/metric.js`,
  `blocks/table.js`, `blocks/chart.js`
- Test: `pkg/appworker/shell_test.go` (Go-side golden: rendered HTML for a
  fixture doc contains sanitized markdown — `<script>` stripped, links
  `rel="noopener nofollow"`; textContent-only titles) + a small JS
  assertion runner is NOT added (no build step) — browser checks land in
  V4's manual checklist.

- [ ] Steps per renderer: markdown = `marked` → `DOMPurify.sanitize` with
  the fixed allowlist config (no raw HTML, no `style`, schemes
  http/https/mailto); metric = tiles with `format:
  "currency:EUR"|"number"|none` via `Intl.NumberFormat`; table = sortable
  `<table>`, tabular-nums right-aligned numerics, client search; chart =
  `vegaEmbed(el, spec, {config: THEME_CONFIG})` where `THEME_CONFIG` is the
  platform Vega theme (fonts + `ui/product` palette). Commit
  `feat(appshell): block renderers (RFC 028 V1)`.

### Task V2: Interactivity

**Files:**
- Create: `ui/appshell/blocks/filter.js`, `blocks/tabs.js`,
  `ui/appshell/interact.js` (param state, re-render fetch, modals)

- [ ] Steps: URL params ↔ filter controls; re-render `POST` with
  `X-Datuplet-App-Render: 1` + `Accept: application/json`, stale-dim +
  spinner overlay, block swap; `onClick: {param}` chart bindings via
  vega-embed signal listener; `block=<id>` partial fetch for lazy modals;
  `refreshInterval` clamped [15,3600] + ±10 % jitter +
  `document.visibilitychange` pause + exponential backoff on 429/503
  honoring `Retry-After`; tabs (client `tabs` block + `ctx.path` nav links).
  Commit `feat(appshell): filters, tabs, modals, auto-refresh (RFC 028 V2)`.

### Task V3: Export + polish

- [ ] Steps: CSV export with OWASP escaping (prefix `'` when cell starts
  `=`, `+`, `-`, `@`, tab, CR — unit-tested in a tiny Go test evaluating
  the exported string via the golden fixture); `@media print` stylesheet
  (hide chrome/filters, page-break between blocks, force light);
  skeleton-block first paint; empty-state and error-card styling; dark/light
  via `prefers-color-scheme` using `ui/product` tokens. Commit
  `feat(appshell): export, print, loading polish (RFC 028 V3)`.

### Task V4: Part 4 gate

- [ ] Cumulative Codex review + push + PR body. Manual browser smoke
  (orchestrator, OrbStack): sample app renders; filter change re-renders
  with spinner; tooltip works; CSV downloads; print preview sane; devtools
  network shows zero cross-origin requests. Record checklist in PR body.

---

# Part 5 — CLI (C) — the agent loop

**Goal:** an agent implements, tests, and ships an app end-to-end with no
browser: `init → put → render -o json → promote → token create`.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| C0 | Preflight: CLI plumbing anchors (`--project`, api client, `-o json`) | sonnet | Part 3 gate | no |
| C1 | `apps init` scaffold + `put` + `get/list/delete` | sonnet | C0 | no |
| C2 | `apps render` + `logs [--request-id]` | opus | C1 | no |
| C3 | `promote` + `token` + **Part 5 gate** | sonnet | C2 | no |

### Task C0: Preflight

- [ ] Record: **the exact project-resolution the CLI uses today** — Draft
  v10 §5.5 already matches this (no `config set project` command): `--project`
  flag > `DATUPLET_PROJECT` env > the project recorded in
  `~/.datuplet/cluster.json`. **Reuse that existing chain verbatim.** Also
  record: the shared API-client helper used by `components.go`; the `-o
  json` convention (RFC 027 C-part); where subcommands register in
  `main.go`. Update plan on drift.

### Task C1: init/put/get/list/delete

**Files:**
- Create: `cmd/datuplet/apps.go`, `cmd/datuplet/apps_scaffold/` (embedded
  scaffold, spec §5.5: `app.js` template = Appendix A example trimmed to
  one query; `datuplet.d.ts` typing `ctx`, `datuplet.query`, OutputDoc
  blocks; **`esbuild.mjs` build script + `package.json` with a `build`
  script** = the `esbuild --bundle --format=iife --global-name=__dtp_app`
  invocation so the author/agent has a working one-command bundle, not just
  a README line; `README.md`), `cmd/datuplet/apps_test.go`

- [ ] **Step 1:** Failing tests (httptest fake API): `init` writes the
  scaffold files (refuses non-empty dir), including the esbuild build
  script; `put --bundle` base64s the file, prints `{app_id, version_hash}`
  (json mode) / table (text); bundle >5 MB → local error before upload;
  `get/list/delete` against fake; every command resolves the project via
  `--project` > `DATUPLET_PROJECT` > cluster default (C0's chain), with a
  deterministic error naming those remedies when none resolves.
- [ ] **Step 2:** FAIL → implement → PASS → commit
  `feat(cli): datuplet apps init/put/get/list/delete (RFC 028 C1)`.

### Task C2: render + logs

- [ ] **Step 1:** Failing tests: `render --channel draft --param days=7 -o
  json` sends bearer + `Accept: application/json` to
  `/apps/{pid}/{name}@draft?days=7`, prints the doc verbatim on 200; on
  error envelope, fetches `logs?request_id=<id>` and prints ONE object
  `{error, kind, request_id, author_log}` (author_log null on 404); exit
  code 1 on render failure (user error class), 20 on transport failure;
  `logs` lists, `--request-id` prints one record or exits 1 with
  `not found`.
- [ ] **Step 2:** FAIL → implement → PASS → commit
  `feat(cli): datuplet apps render + logs (RFC 028 C2)`.

### Task C3: promote + token + gate

- [ ] `promote --version <hash> [--expected-production <hash>]` (409 →
  clear CAS message); `token create` prints the `vw_…` secret ONCE with a
  "store it now" note (json: `{token_id, token}`); `token list/delete`.
  Update `docs/agent-quickstart.md` with the apps loop. Part 5 gate:
  cumulative Codex review + push + PR body.

---

# Part 6 — Management UI (U)

**Goal:** `/ui/apps` — catalog + app detail that a human uses for the whole
lifecycle. Nice = consistent with the existing product UI, real empty/
loading/error states, zero new frameworks.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| U0 | api.js calls + routes + catalog page | sonnet | Parts 3+4 gates | no |
| U1 | App detail: channels, upload, promote/rollback CAS | opus | U0 | no |
| U2 | Tokens (shown-once) + logs viewer + draft preview iframe | sonnet | U1 | no |
| U3 | **Part 6 gate** (manual UI smoke) | sonnet | U0–U2 | no |

### Task U0: Catalog

**Files:**
- Modify: `ui/product/api.js` (author-route wrappers), `ui/product/app.js`
  (register `#/apps` + `#/apps/{name}` routes + nav entry)
- Create: `ui/product/pages/apps.js`

- [ ] Steps: catalog table (name, production version short-hash, draft
  short-hash, updated, viewer-link copy button when production is set);
  "New app" panel: name input (client-side slug validation mirroring the
  server regex) + bundle file input (`FileReader` → base64 PUT); empty
  state ("No apps yet — create one or use `datuplet apps init`");
  loading skeleton + error toast per existing `overlay.js` patterns.
  Commit `feat(ui): /ui/apps catalog (RFC 028 U0)`.

### Task U1: Detail page

**Files:**
- Create: `ui/product/pages/app-detail.js`

- [ ] Steps: header (name, channel badges); versions list (hash, size,
  created, which channel points here); upload-new-bundle control (moves
  draft); **Promote** button — confirmation modal shows draft hash →
  production hash and sends `expectedProduction` = the hash currently
  displayed (CAS: on 409 show "someone promoted meanwhile — refresh");
  **Rollback** = promote-with-old-hash from the versions list; delete app
  (typed-name confirm). Commit `feat(ui): app detail + promote/rollback (RFC 028 U1)`.

### Task U2: Tokens, logs, preview

- [ ] Steps: tokens section (create → modal showing the `vw_…` secret
  exactly once with copy button + "won't be shown again"; list shows
  token_id + created + revoke); render-log viewer (recent renders table:
  time, outcome, duration; row click → log_text + error detail; filter by
  request-id); draft preview: `<iframe src="/apps/{pid}/{name}@draft">`
  panel (works because CSP `frame-ancestors 'self'` + same-origin session).
  Commit `feat(ui): app tokens, logs, draft preview (RFC 028 U2)`.

### Task U3: Part 6 gate

- [ ] Cumulative Codex review + push + PR body; manual smoke on OrbStack:
  create app in UI → upload → preview draft in iframe → promote → mint
  token → open viewer link in private window → dashboard renders.

---

# Part 7 — Charts, e2e, docs (D)

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| D0 | Preflight: keygen/secret pattern, ingress anchor, values layout | sonnet | Parts 1–6 gates | no |
| D1 | Chart: app-worker Deployment/Service/NP, values, guards, secrets | opus | D0 | no |
| D2 | e2e: viewer flow + security scenarios | opus | D1 | no |
| D3 | e2e: agent-flow CLI scenario + docs sweep | sonnet | D2 | no |
| D4 | **FINAL gate**: `make e2e-k8s`, whole-branch Codex review, PR ready-notes | sonnet | D0–D3 | no |

### Task D0: Preflight

- [ ] Record concretely (these are security-critical, so name the exact
  files/mechanisms — findings 13, 14):
  - **Secret generation:** which existing pattern creates service Secrets
    (the infra keygen Job that makes `pg-lakekeeper` et al., or a
    helm-random). The shared `service-token` (consumed by BOTH pipeline-api
    internal routes AND app-worker) and the `cookie-hmac-key` (app-worker
    only) must be generated the SAME way. Record the generating file, the
    Secret name, and where pipeline-api's Deployment already mounts service
    Secrets so the internal-token value reaches both sides.
  - **Ingress/route resource:** the actual object that routes `/api` + `/ui`
    today (Ingress? a reverse proxy in pipeline-api? Gateway API?). If
    there is no ingress template (traffic terminates in pipeline-api), then
    query-string/Referer redaction is NOT an ingress concern and the
    primary control is app-worker's own logging (which already logs
    structured fields only, never URLs/Referer, per §5.3) — record which
    it is, because D1's redaction step depends on it.
  - How `queryWorker.enabled` gates templates (copy the pattern); e2e
    harness helpers for HTTP + CLI scenarios
    (`tests/e2e/scenarios_agent_loop_test.go` is the model). Update plan on
    drift.

### Task D1: Chart

**Files:**
- Create: `charts/datuplet-app/templates/app-worker/{deployment,service,networkpolicy}.yaml`;
  the Secret template/keygen addition found in D0
- Modify: `charts/datuplet-app/values.yaml` (the `appWorker:` block from
  the contract), the ingress/route resource found in D0 (`/apps` →
  app-worker), pipeline-api Deployment (mount the shared `service-token`),
  `Makefile` docker-build targets

- [ ] **Secrets step (finding 13):** using D0's recorded pattern, generate
  a Secret `datuplet-app-worker` with keys `service-token` (32 random
  bytes) and `cookie-hmac-key` (32 random bytes). Mount `service-token`
  into BOTH app-worker (`DATUPLET_APPWORKER_SERVICE_TOKEN_FILE`) and
  pipeline-api (as the internal-endpoint credential it checks — same value,
  one Secret) and `cookie-hmac-key` into app-worker
  (`DATUPLET_APPWORKER_COOKIE_KEY_FILE`). Assert both mounts in the
  `helm template` render test.
- [ ] **Deployment step:** mirrors query-worker (resources, hardening:
  `automountServiceAccountToken: false`, drop capabilities, seccomp;
  readiness `/readyz`, liveness `/healthz`, `preStop` sleep < termination
  grace, PDB `minAvailable: 1`, 2 replicas); env wiring from values +
  Secret file mounts; template `fail` guard:
  `{{- if and .Values.appWorker.enabled (not .Values.queryWorker.enabled) }}{{- fail "appWorker requires queryWorker.enabled=true (RFC 028 §8)" }}{{- end }}`.
- [ ] **Redaction step (finding 14):** per D0's finding — IF there is an
  ingress that logs requests, add the query-string + `Referer` redaction
  config for `/apps/*` there (name the ingress class's exact knob in a
  values comment) AND add a `helm template` assertion for it; IF traffic
  terminates in app-worker (no ingress log layer), record that app-worker's
  own structured logging (no URL/Referer fields, §5.3, tested in W6) is the
  complete control and no chart change is needed. Either way the plan must
  end with a concrete, testable redaction guarantee — not a "document the
  knob" placeholder.
- [ ] `helm template` golden/lint test (D0 records whether the repo has
  one). Commit `feat(chart): app-worker deployment, secrets, values (RFC 028 D1)`.

### Task D2: e2e security + viewer flow

**Files:**
- Create: `tests/e2e/scenarios_apps_test.go` (+ fixture app bundle under
  `tests/e2e/scenarios/testdata/apps/sales/app.js` pre-bundled — esbuild is
  NOT run in CI; commit the bundled artifact with a header comment naming
  the source)

- [ ] Scenario steps (one test, staged subtests, real cluster):
  upload via author API (draft) → draft renders with platform session,
  403 with viewer token → promote by hash → mint token → GET with
  `?token=` → assert 302 strips token + cookie attributes → follow →
  HTML renders → JSON render returns doc with expected rows from a real
  warehouse table (seeded like `scenarios_query_test.go` does) →
  `query_audit` line attributes the app principal → injection subtest:
  `?country=' UNION SELECT ...` yields bound-literal behavior (zero rows,
  no error leak) → cookie replay against second app → 401 → revoke token
  → ≤15 s later 401 → promote v2 → all replicas serve v2 ≤15 s (poll) →
  render-log record exists with request_id + principal fields.
- [ ] Commit `test(e2e): user-apps viewer + security scenarios (RFC 028 D2)`.

### Task D3: agent-flow e2e + docs

- [ ] e2e subtest driving the REAL CLI binary (pattern from
  `scenarios_agent_loop_test.go`): `apps init` in temp dir → (pre-bundled
  fixture substituted for esbuild) → `put` → `render --channel draft -o
  json` asserts OutputDoc JSON → failure case: bundle that throws →
  render exits 1 and emits `{error, kind, request_id, author_log}` with
  non-null author_log → `promote` → `token create` → viewer curl 200.
- [ ] Docs: `docs/user-apps.md` (author guide: contract, limits table,
  CLI loop, viewer links, UI walkthrough); `docs/ad-hoc-query.md` already
  updated (Q3) — verify; `CLAUDE.md` key-directories rows
  (`pkg/appengine/`, `pkg/appworker/`, `ui/appshell/`) + non-obvious
  conventions bullet (engine.wasm committed artifact + `make engine-wasm`);
  `docs/known-limitations.md` (project-wide read grant §5.4, eventual
  promote consistency, no writebacks/Python/SSO yet).
- [ ] Commit `test(e2e)+docs: agent loop scenario + user-apps docs (RFC 028 D3)`.

### Task D4: FINAL gate

- [ ] `make e2e-k8s` on OrbStack — full suite green including the new
  scenarios (chart/deployment changes make this mandatory, repo rule).
- [ ] Whole-branch Codex review (base = commit 0). STOP on findings per
  protocol. Minor-findings ledger resolved or recorded in PR body.
- [ ] Update PR body: phase summaries, spike numbers, smoke checklists,
  limitations. Leave PR as **draft** — maintainer flips ready + merges +
  releases (repo rule; version bump via `make bump-version` happens on
  maintainer's release, not in this branch).

---

# Side quest — `datuplet secrets` CLI (SQ)

**Goal:** give an agent (and humans) the ability to provision project secrets
from the CLI, so pipelines/components that reference `$[key]` values can be
set up headlessly — without the browser secrets UI. **Independent of RFC 028**
(user apps are credentials-clean and need no secrets); it only wraps the
**existing** project-secrets API, adds no new storage, and is not on any RFC
028 gate's critical path.

**Lands on its own branch** `feat/datuplet-secrets-cli` off `main` with its
own draft `gh pr create --draft` — it shares no files with the RFC 028
branch, so keep it separate (never push main, never tag — repo rule). The
orchestrator may run it at any time (it's a good early warm-up: tiny, and it
unblocks agents that need to seed secrets for later testing).

**Existing API this wraps** (verified — `pkg/pipelineapi/http/secret_handlers.go`,
`ui/product/pages/settings-secrets.js`, `docs/secrets.md`):
- `GET  /api/v1/projects/{pid}/secrets` → `[{key, updatedAt}]` (**values are
  never returned** — list is key + timestamp only, by design).
- `PUT  /api/v1/projects/{pid}/secrets/{key}` body `{"value":"..."}` → 204.
- `DELETE /api/v1/projects/{pid}/secrets/{key}` → 204.
- All three require the `data_admin` role (server-enforced); secrets are
  stored in the per-project managed `datuplet-project-secrets` Secret and
  referenced in `component.config` as whole-scalar `$[key]`.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| SQ1 | `datuplet secrets set/list/delete` | sonnet | — (only the existing secrets API) | independent of all RFC 028 parts |

### Task SQ1: `datuplet secrets` command

**Files:**
- Create: `cmd/datuplet/secrets.go`, `cmd/datuplet/secrets_test.go`
- Modify: `cmd/datuplet/main.go` (register the `secrets` subcommand next to
  the others), `docs/secrets.md` (add a "CLI" section)

**Interfaces / conventions (reuse existing CLI plumbing — do NOT reinvent):**
- Auth + transport: `doAuthedRequest(ctx, method, urlStr, apiToken,
  contentType, body)` (`cmd/datuplet/pipeline.go:219`).
- Project + remote + token resolution: the existing chain (`--project` flag
  > `DATUPLET_PROJECT` > `~/.datuplet/cluster.json`; `--remote`/`--token-file`
  as elsewhere) — copy the flag-parsing pattern from `components.go`.
- Output: a `-json` bool flag (the repo convention, e.g.
  `main.go` `triggerCmd.Bool("json", …)`); default human table.

**Secret-value input (security-critical — the value must NOT land in argv or
shell history):**
- `datuplet secrets set <key>` reads the value from **stdin by default**
  (e.g. `printf %s "$TOKEN" | datuplet secrets set api_key`), or from
  `--value-file <path>` (`-` = stdin). **There is deliberately no
  `--value <literal>` flag** (it would leak via `ps`/history). Document this
  in the help text and `docs/secrets.md`.
- The CLI never prints or logs the value; on success it prints only
  `{key, updatedAt}` (json) or a one-line confirmation (text).

- [ ] **Step 1: Write failing tests** (`secrets_test.go`, httptest fake API —
  mirror `components_test.go`'s fake-server pattern):

```go
func TestSecretsSet_ReadsStdin_PutsValue(t *testing.T) {
	var gotBody []byte
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	err := runSecretsSet([]string{"api_key"}, secretsDeps{
		remote: srv.URL, apiToken: "t", projectID: "p1",
		stdin: strings.NewReader("s3cr3t"),
	})
	if err != nil { t.Fatal(err) }
	if gotMethod != http.MethodPut { t.Fatalf("method %s", gotMethod) }
	if gotPath != "/api/v1/projects/p1/secrets/api_key" { t.Fatalf("path %s", gotPath) }
	if string(gotBody) != `{"value":"s3cr3t"}` { t.Fatalf("body %s", gotBody) }
}

func TestSecretsList_PrintsKeysNoValues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet { t.Fatalf("method %s", r.Method) }
		_, _ = w.Write([]byte(`[{"key":"api_key","updatedAt":"2026-07-24T10:00:00Z"}]`))
	}))
	defer srv.Close()
	out := &strings.Builder{}
	if err := runSecretsList([]string{}, secretsDeps{remote: srv.URL, apiToken: "t", projectID: "p1", out: out, asJSON: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"api_key"`) || strings.Contains(out.String(), "s3cr3t") {
		t.Fatalf("list leaked or missing key: %s", out.String())
	}
}

func TestSecretsDelete_CallsDelete(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	if err := runSecretsDelete([]string{"api_key"}, secretsDeps{remote: srv.URL, apiToken: "t", projectID: "p1"}); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/api/v1/projects/p1/secrets/api_key" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
}

func TestSecretsSet_NoValueFlagExists(t *testing.T) {
	// Guard against a future --value literal flag sneaking in.
	if secretsSetFlagSet().Lookup("value") != nil {
		t.Fatal("--value literal flag must not exist (leaks via argv/history)")
	}
}
```

- [ ] **Step 2:** Run `go test ./cmd/datuplet/ -run TestSecrets -v` — FAIL
  (nothing implemented).
- [ ] **Step 3: Implement `secrets.go`.** A `secretsDeps` struct (remote,
  apiToken, projectID, `stdin io.Reader`, `out io.Writer`, `asJSON bool`,
  `valueFile string`) so the run* funcs are unit-testable; `runSecrets`
  dispatches `set|list|delete` and resolves remote/token/project via the
  shared helpers. `set`: read value from `--value-file` (or stdin), PUT
  `{"value":...}` (JSON-encode so quotes/newlines are safe), treat 204 as
  success, map 403 → a clear "requires data_admin" message, 404 → project
  not found. `list`: GET, render key + updatedAt (json or table), never a
  value. `delete`: DELETE, 204 success, 404 → "no such key". Register in
  `main.go`.
- [ ] **Step 4:** Run `go test ./cmd/datuplet/ -run TestSecrets -v` — PASS;
  `go build ./...`.
- [ ] **Step 5:** Update `docs/secrets.md` with a CLI section (the three
  commands, the stdin/`--value-file` value-input rule and why there's no
  `--value` flag, the `data_admin` requirement, and a
  `printf %s "$TOKEN" | datuplet secrets set api_key` example).
- [ ] **Step 6:** Commit `feat(cli): datuplet secrets set/list/delete`.
- [ ] **Step 7 (gate):** in-session `mcp__codex__codex` review of the diff
  (read-only, high effort) — same mechanism as the RFC 028 gates; STOP on
  any blocker/major per the standing rule. Then push `feat/datuplet-secrets-cli`
  and open its own draft PR.

## Plan self-review record (author-run)

- **Spec coverage:** §4 architecture → W/V parts; §4.1 identifiers → P2
  regex + routing in W6; §4.2 matrix → W6; §5.1 author routes + channels →
  P1/P2; §5.2 six internal endpoints → P3 (sessions/verify included); §5.3
  viewer auth incl. redaction/rate values → W4/W6/D1 (ingress redaction);
  §5.4 identity/impersonation/query-route acceptance → P4/P5; §5.5 CLI →
  C1–C3, UI → U0–U2; §6.1 params → Q1–Q3; §6.2 ABI + deadline + globals →
  E1/W5; §6.3 OutputDoc + interactivity → W1/V1/V2; §6.4 Vega subset + CSP
  → W2/V0/W6; §6.5 normalization → W6; §6.6 log schema → P1/W5; §7 limits
  table → config W0 + enforcement W4/W5 + values D1; §8 envelope + hard
  query-service dependency → W6/D1 fail guard; §9 audit layers →
  P4 (control-plane), W6 (access log), existing query_audit (P5 asserts);
  §10 future work — none implemented (YAGNI); §11 test list → mapped
  across Q/P/W/V/D tasks incl. every mandatory negative Vega case; §12.1
  spike → E0–E2 hard gate; §12.2 eval-per-request (chosen, precompile
  deferred); upload format resolved = JSON+base64 (P2, spec §12 resolved
  note); §12.3 RFC number check → D3 docs sweep.
- **Placeholder scan:** no TBD/TODO; every code step carries code or an
  exact mechanical instruction + verification command; C-shim
  free-omission is annotated in-code as a deliberate note, not a gap.
- **Type consistency:** `appengine` API names identical across E1/W5;
  store API names identical across P1/P2/P3; cookie fields/attrs identical
  across W4/W6/D2; error kinds list identical across contract/W6/C2;
  `principal_kind` values identical across P1 DDL/W4/W6/D2.
- **Right-sizing check:** every task ends in an independently reviewable
  commit with its own tests; parallel-marked tasks are file-disjoint.

## Codex plan-review log

- **Round 1** (GPT-5 Codex, high, in-session): 24 findings (3 blocker, 16
  major, 5 minor). All folded. Blockers: app-impersonation mint + query-
  route app-principal path (contract + P4/P5 rewritten), ESM-vs-IIFE
  author-contract reconciliation (E0 note). Notable majors: removed the
  impersonation-JWT cache (per-render mint), JSON cookie payload (no `|`
  split), bundle gzip + project quota + 14-day log retention, shared Vega
  schema with drift check, render Prometheus counter, `IdentityManager`
  interface so P2 no longer depends on P4, concrete secret generation +
  redaction guarantee, QueryContext cancellation preserved for params,
  async-handshake `__dtp_settled` guard, wazero lifecycle + OOM test, guest
  globals tests, in-session `mcp__codex__codex` gate mechanism.
  Maintainer-flagged reconciliations (conservative calls, open to veto):
  ESM authoring preserved but delivered as esbuild IIFE; CLI keeps the
  existing `--project`/`DATUPLET_PROJECT`/cluster-default chain instead of a
  new `config set project`.
- **Round 2** (GPT-5 Codex, high, in-session): 12/24 round-1 items fully
  verified, 11 partial, 1 divergent; 9 fresh blocker+major. All folded.
  Two sequencing bugs fixed (`IdentityManager` interface+recorder moved to
  P1; P3 no longer parallel with P2 — both touch route registration).
  Contract-vs-body coverage closed (P1 now tests gzip round-trip, project
  quota, dual-bound log retention). The single subject helper was split into
  `AppJWTSubject`/`AppFGASubject` (no double-prefix). Concrete single wazero
  lifecycle (one runtime with engine-level memory limit, per-render instance,
  per-render host state via `context.Value`); `NewEngine(ctx, memoryPages)`
  with the per-render memory field removed from `Limits`; async settled-flag
  protocol written into the contract. **Three spec-vs-plan conflicts reconciled by bumping the spec to
  Draft v9** (maintainer-flagged): ESM→IIFE delivery (§6.2/App A), CLI
  project chain (§5.5); the `Date` real-clock item was fixed on the plan
  side (spec §6.2 was already correct).
- **Round 3** (GPT-5 Codex, high, in-session): 7/9 round-2 items resolved,
  2 partial; 5 fresh (3 blocker+major, 2 minor) — all the "contract updated
  but task body stale" residue of the round-2 edits. Folded: P4/P5 now use
  `AppFGASubject` (tuple/audit) vs `AppJWTSubject` (JWT `sub`) — the old
  single-helper form removed; internal `/impersonate` contract aligned to
  the split; the stray per-render memory-field parenthetical deleted; the E1
  test snippets updated in place to the final API (`NewEngine(ctx, pages)`,
  no per-render memory field); W0 gains `Config.MemoryPages()` + a test that
  `Serve` passes it to `NewEngine`. Blocker+major after fold: 0 pending
  (verify next round).
- **Round 4** (GPT-5 Codex, high, in-session): round-3 items verified (2
  partial — leftover sentences from my own edits); 3 fresh (1 major, 1
  minor, 1 nit), all wording residue. Folded: the stale
  `sub == FGA tuple byte-for-byte` sentence in the impersonate contract
  rewritten to the JWT/FGA split; the last per-render memory-field prose
  removed; review-log mentions of the old single subject helper reworded.
  Codex confirmed
  `NewEngine(ctx, memoryPages)` is consistent across contract/E1/W0/W5/W6.
  Remaining blocker+major: 1 → folded.
- **Round 5** (GPT-5 Codex, high, in-session): **0 blocker+major — verdict
  "ready to execute."** Round-4 items verified; 2 nits, both about the
  review log still literally containing the old token names while describing
  their removal. Folded (log reworded); a whole-document grep now confirms 0
  bare `App`+`Subject`, 0 per-render memory field, and consistent
  `NewEngine` arity. Convergence reached (no majors) at round 5.
- **Round 6** (GPT-5 Codex, high, in-session): round-5 nits verified; fresh
  end-to-end read found 2 majors + 2 minors (all real, not residue).
  Folded: **spec bumped to Draft v10** — §5.4/§5.2 now keep JWT `sub` vs
  OpenFGA subject distinct (the spec still carried the old single-subject
  wording), and the §12 resolved note records the JSON+base64 upload
  decision; W5's pool
  semaphore given explicit acquisition policies (per-app non-blocking →
  `rate_limited`; pool short-bounded-wait → `capacity`) with both outcomes
  tested; C0 wording updated (Draft v10 already matches the CLI chain).
  Batch 2 (rounds 4–6) ended with majors → batch 3 (rounds 7–9) follows.
- **Round 7** (GPT-5 Codex, high, in-session): **0 blocker+major — "ready to
  execute."** Round-6 items verified; 2 minors (stale "Draft v9" in C0; spec
  §12 numbering drift after the upload-format resolution). Folded (C0 → v10;
  §12 renumbered to 1/2/3; plan self-review reference updated).
- **Round 8** (GPT-5 Codex, high, in-session): **0 blocker, 0 major, 0
  minor, 1 nit — "ready to execute."** Round-7 items verified; the one nit
  (stale `§12.3`-as-upload-format cross-references) folded. **Convergence:
  rounds 7 and 8 both at zero blocker+major; review closed here.**

**Review outcome:** 8 Codex rounds (GPT-5 Codex, high, in-session via
`mcp__codex__codex`). Finding arc 24 → 9 → 3 → 1 → 0(nits) → 2(fresh deep
read) → 0 → 0. All folded. Spec advanced v8 → v10 (3 plan-surfaced
reconciliations). Verdict: **ready to execute**, starting Part 0 (engine
spike, hard GO/NO-GO gate).
