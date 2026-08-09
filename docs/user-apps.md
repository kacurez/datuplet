# User apps (RFC 028)

**Experimental.** User apps are a v1 feature. APIs, limits, and the guest
ABI may change between 0.x releases.

---

A **user app** is a small, server-rendered dashboard: author-written
JavaScript that queries the project's warehouse and returns a declarative
document the platform renders — no author-controlled HTML, no browser JS
from the app, no long-lived credentials in the app's hands. Apps run
untrusted code in a sandboxed WASM engine (`pkg/appengine/`), inside a
dedicated `app-worker` service that holds **zero storage credentials** of
its own.

This doc is the author/operator reference: the app contract, the limits
table, the CLI loop, viewer links and tokens, and a short UI walkthrough.
For a fully worked, copy-pasteable CLI session (with real, verified output),
see the ["Apps quickstart" section of docs/agent-quickstart.md](agent-quickstart.md#apps-quickstart-the-agent-build-test-ship-loop-rfc-028).

---

## The app contract

An author writes an **ES module** exporting one function:

```js
export async function render(ctx) {
  const result = await datuplet.query(
    `SELECT count(*) AS n FROM sales.orders WHERE country = $country`,
    { country: ctx.params.country ?? "DE" });
  return {
    outputDoc: 1,
    title: "Orders",
    blocks: [
      { id: "kpi", type: "metric", items: [{ label: "Orders", value: result.rows[0][0] }] },
    ],
  };
}
```

- **Input — `ctx`:** `ctx.params` (a flat `string → string` map: URL query
  merged with a re-render POST body, last-key-wins, no arrays/nesting/type
  coercion — apps parse their own numbers), `ctx.path` (the sub-path after
  `/apps/{pid}/{name}`), `ctx.now` (render-start timestamp, ms epoch).
- **The only host capability: `datuplet.query(sql, params?, opts?)`.** Same
  shape as [`POST /api/v1/projects/{pid}/query`](ad-hoc-query.md) — `sql`
  with `$name` placeholders, `params` bound as a prepared statement, never
  string-spliced. **This is the whole injection story:** the query
  *structure* is author-written; viewer-controlled *values* (`ctx.params`)
  enter only as bound parameters. Never interpolate `ctx.params` into a SQL
  string — that reintroduces the exact hole bound params close. A non-200
  from the query service makes `datuplet.query` **throw** a JS `Error` with
  `{kind, message}`; an app may catch it and degrade, or let it fail the
  render.
- **Output — the OutputDoc:** `{outputDoc: 1, title, blocks: [...],
  refreshInterval?}`. Block types: `markdown | metric | table | chart |
  filter | tabs`. Every block has an author-assigned, doc-unique `id`. Charts
  are Vega-Lite specs (`{type:"chart", library:"vega-lite", spec}`) against a
  **restricted subset** — no `data.url`, no `layer`/`facet`/`concat`, no
  `href`/`url` encoding channels, no `image` mark, no `lookup` transform, no
  `config` — enforced by a checked-in JSON Schema
  (`pkg/appengine/vegaspec/schema.json`) shared by the engine and the shell.
  Markdown is rendered via `marked` then sanitized by DOMPurify (no raw HTML,
  no `style`, links restricted to `https:`/`http:`/`mailto:`). Every text
  field reaches the DOM via `textContent`, never `innerHTML`.
- **Bundling — IIFE, not the ES module directly.** The engine's QuickJS
  guest has no module loader, so the CLI bundles your ES module with esbuild
  into a single self-contained IIFE exposing the module on a global:
  `esbuild app.js --bundle --format=iife --global-name=__dtp_app`. That
  bundle — not the source `app.js` — is the artifact `apps put` uploads;
  the engine reaches your `render` at `globalThis.__dtp_app.render`. esbuild
  is **author-side only** — the CLI's scaffold ships a working
  `esbuild.mjs` + `package.json` (`npm install && npm run build`); esbuild
  itself is never a server or CI dependency.
- **Sandbox environment:** ES2023 built-ins, `Date`, `Math.random`
  (host-seeded — no determinism guarantees, none needed). No `fetch`, no
  timers, no filesystem, no env vars, no dynamic `import`, no network beyond
  `datuplet.query`. Pure-JS npm libraries may be bundled in; Node APIs are
  unsupported.
- **Shell-owned dynamics (nothing an author codes for):** chart
  tooltip/zoom/legend interaction and table sort/search/pagination/CSV
  export run client-side against the delivered data; filter changes and
  chart `onClick` bindings set URL params and trigger a full stateless
  re-render (`POST …?block=<id>` for a single-block partial); auto-refresh
  via `refreshInterval` (clamped 15–3600s, jittered, paused when hidden);
  loading/stale-dashboard states; PDF export via the browser print
  stylesheet. There is no websocket/SSE channel and no app-authored browser
  JS — the latter would break the untrusted-code/trusted-output boundary.

---

## Limits

Defaults as shipped in `charts/datuplet-app/values.yaml`'s `appWorker.render.*`
block (mirrored in `pkg/appworker/config.go`'s `Default*`/`HardCap*`
constants). Each `max*` sibling is itself clamped to the hard cap at
app-worker boot — raising a values override past the cap is a no-op, not an
error.

| Limit | Default | Hard cap | `values.yaml` key |
|---|---|---|---|
| Render wall clock | 10 s | 30 s | `appWorker.render.timeoutS` / `.maxTimeoutS` |
| WASM memory per render | 128 MiB | 256 MiB | `appWorker.render.memoryMiB` / `.maxMemoryMiB` |
| `datuplet.query` calls per render | 10 | 25 | `appWorker.render.queriesPerRender` / `.maxQueriesPerRender` |
| OutputDoc size | — | 2 MiB | `appWorker.render.outputDocMaxBytes` |
| Bundle size (`apps put --bundle`) | — | 5 MB | `appWorker.render.bundleMaxBytes` (pipeline-api's store is the authoritative enforcer; the CLI also rejects locally before any upload) |
| Blocks per OutputDoc | — | 64 | (structural, not operator-tunable) |
| In-flight renders per app | 2 | — | `appWorker.render.perAppInflight` |
| Render slots per pod | 8 | — | `appWorker.render.concurrency` |
| `refreshInterval` | — | clamped to [15 s, 3600 s] | (structural) |

Fixed (not `values.yaml`-tunable — per-principal/per-app identity, not a pod
resource):

| Limit | Value | Keyed by |
|---|---|---|
| Render rate, per viewer token or per platform user | 60/min, burst 10 | `(app_id, token_id)` or `(app_id, user_id)` |
| Render rate, per app | 300/min | `app_id` |
| Viewer-token verify failures | 10/min, then 429 `Retry-After: 60` | `(client IP, app)` |

**Deadline coupling.** A query's `timeout_s` is clamped to the *remaining*
render budget (`min(timeoutS, floor(remaining_seconds))`) and the render
context cancels in-flight queries on expiry — a query can never outlive its
render. Queries run **sequentially** (one QuickJS thread), so per-app
concurrent queries equal per-app in-flight renders by construction; raising
`perAppInflight` without also raising the query service's per-principal
in-flight cap ([docs/ad-hoc-query.md](ad-hoc-query.md)) starves renders with
429s instead of adding real capacity.

---

## The CLI agent loop

`datuplet apps` is a second CLI surface, independent of the pipeline
commands, using the same headless auth and project resolution
(`--project`/`$DATUPLET_PROJECT`, `--remote`/`$DATUPLET_REMOTE`,
`--token-file`/`$DATUPLET_API_TOKEN`) — every subcommand except `init`
resolves through that identical chain.

| Step | Command | Result |
|---|---|---|
| Scaffold | `datuplet apps init <dir>` | writes `app.js`, `datuplet.d.ts`, `esbuild.mjs` + `package.json`, `README.md`. No network, no `--project`. |
| Build | `cd <dir> && npm install && npm run build` | runs the scaffold's esbuild config; writes `bundle.js`. |
| Ship to draft | `datuplet apps put <name> --bundle bundle.js` | new immutable, content-addressed version; moves the app's `draft` channel only. Prints `{app_id, version_hash}` (`--json`). |
| Render + assert | `datuplet apps render <name> --channel draft [--param k=v ...] --json` | **the agent's test step.** Success: the OutputDoc JSON, exit `0`. Failure: **one** object `{error, kind, request_id, author_log}` (the author log fetched automatically via the matching `request_id`, or `null` if it hasn't landed / aged out), exit `1`. Could not reach the service at all, or got back something that isn't this envelope: exit `20` — the split an agent loop branches on. |
| Iterate | edit → build → `put` → `render --channel draft --json` → assert → repeat | `datuplet apps logs <name> --json` lists recent render records without a specific `request_id`. |
| Promote | `datuplet apps promote <name> --version <hash> [--expected-production <old-hash>]` | atomically repoints `production` to an explicit content hash — never "whatever draft is now". `--expected-production` is a compare-and-swap precondition; a 409 (someone else promoted concurrently) surfaces as a **distinct exit code `9`**, separate from the generic `1`. |
| Share | `datuplet apps token create <name>` | mints `vw_<token_id>.<secret>`, printed **exactly once** — no command can ever show it again. |
| Manage tokens | `datuplet apps token list <name>` / `datuplet apps token delete <name> <token_id>` | ids + created/revoked timestamps (never a secret) / revoke by UUID. |

Exit codes in play: `0` success; `1` generic local/user error or a render
failure (the app's own fault); `9` promote CAS conflict; `20` render
transport failure (the platform is unreachable — retry or escalate). Every
input (app name, `--channel`, `--version`, `--param`, token id) is validated
**locally**, before any network call, so a bad value never depends on
ambient `~/.datuplet` state to fail deterministically.

See [the worked walkthrough in docs/agent-quickstart.md](agent-quickstart.md#apps-quickstart-the-agent-build-test-ship-loop-rfc-028)
for the full loop with real, verified command output at every step.

---

## Viewer links and tokens

Public routes are project-UUID-qualified: `/apps/{pid}/{name}` serves
`production`; `/apps/{pid}/{name}@draft` serves the draft and requires an
authenticated **platform session or bearer api-token** — never a viewer
token (a `?token=` on `@draft` is rejected outright, before any
verification). Both channels run under the same read-only app identity, so a
broken draft can do nothing production couldn't.

**Sharing a link.** `datuplet apps token create <name>` mints a token; the
share URL is `https://<host>/apps/<project>/<name>?token=<token>`. The first
open is a one-time exchange:

1. app-worker verifies the token against pipeline-api and sets a **signed
   session cookie** (`HttpOnly; Secure; SameSite=Lax;
   Path=/apps/{pid}/{name}`, ~24 h expiry).
2. **302-redirects to the token-free URL** — the plaintext never lands in
   the address bar, browser history, or any log line beyond that first
   request; every `/apps/*` response (the redirect included) carries
   `Referrer-Policy: no-referrer`.
3. Every later request is cookie-only: signature check, a mandatory
   `cookie.app_id == resolved app_id` comparison, and a revocation check
   against a ≤15 s cache — deleting a token kills its live sessions within
   that window despite the cookie itself being stateless.

**Revoking access.** `datuplet apps token delete <name> <token_id>` deletes
the token row; every session bound to it stops working within ≤15 s. Viewer
tokens bind to the **app**, not a version — a token keeps working across
promotes.

**What a leaked token exposes.** A viewer token gates *who sees rendered
output*; OpenFGA gates *what data the app can touch* — and that FGA grant is
**project-wide, not row-scoped** (see "User apps (RFC 028)" in
[known-limitations.md](known-limitations.md)). A leaked token exposes that
one app's rendered dashboards — never the underlying SQL, other tables, or
the platform API.

**Reaching `/apps/*` at all.** No chart-shipped Ingress fronts `/apps/*` —
see "User apps (RFC 028)" in [known-limitations.md](known-limitations.md)
for what your own Ingress/reverse-proxy must do before a viewer link works.

---

## Management UI

`/ui/apps` (the product SPA — vanilla ES modules, same style as the rest of
`ui/product/`) is the catalog: every app in the current project, with each
channel's current version at a glance. Opening an app goes to its detail
page, which covers the full lifecycle:

- **Upload** a new bundle (moves `draft`) and see the full version history.
- **Draft preview** — an inline iframe pointed at the app's own `@draft`
  route, **on the same origin as `/ui`** (never an absolute/external URL),
  authenticated by your existing platform session cookie — no viewer token
  involved. This only works because `/apps` and `/ui` share a host; see the
  known-limitations note linked above.
- **Promote / rollback**, both the same compare-and-swap call the CLI uses:
  the page captures the production hash on screen at confirm-time and sends
  it as `expectedProduction`; a 409 (someone promoted meanwhile) shows a
  "refresh and retry" message instead of silently retrying.
- **Viewer tokens** — mint (shown once, never persisted past its one-time
  modal), list (ids + timestamps only), revoke.
- **Render logs** — the same records `apps logs` prints, including a
  render's captured `console.log` output and, for a failure, the guest error
  and stack. This text is author-code output, not platform text, and is
  escaped like any other untrusted string when displayed.
- **Delete**, with a typed-name confirmation (rows + the app's FGA identity).

The rendered app itself (what a viewer actually sees) is served by
`ui/appshell/` — a separate, minimal shell distinct from the `/ui/product`
author SPA: it turns an OutputDoc into DOM (Vega-Lite via `vega-embed`,
Markdown via `marked` + DOMPurify), and ships with the CSP `default-src
'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src
'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'self'` —
so even a validator miss in the block-rendering path cannot reach the
network. `frame-ancestors 'self'` is what makes the draft-preview iframe
above legal at all (same-origin framing only).

---

## See also

- [docs/agent-quickstart.md](agent-quickstart.md#apps-quickstart-the-agent-build-test-ship-loop-rfc-028) — the full CLI loop with real, verified output.
- [docs/ad-hoc-query.md](ad-hoc-query.md) — the query service `datuplet.query` calls into, including the bound-`params` contract apps rely on for injection safety.
- [docs/known-limitations.md](known-limitations.md) — the project-wide read grant, eventual promote consistency, the same-host `/apps`/`/ui` routing requirement, and what v1 does not do yet (writebacks, Python, SSO).
