// ui/appshell/interact.js — RFC 028 Part 4 (V2) shell interactivity hub.
//
// Platform-owned, trusted code (spec §6.4). This module owns everything that
// is NOT a per-block renderer: the param state (URL query <-> ctx.params), the
// stateless re-render fetch, modals, the auto-refresh scheduler, and the
// Vega-Lite onClick cross-filter binding. Apps stay pure request/response
// (spec §6.3 "All dynamics live in the platform-owned shell") — none of the
// behavior here is app-authored.
//
// IMPORTANT security invariants this module upholds (spec §6.3/§6.4/§5.3):
//   - Every fetch is SAME-ORIGIN: it targets `location.pathname` (the current
//     app URL) only — never an absolute or external URL — so the shell CSP
//     (`connect-src 'self'`) is never even tested. No string in this file is a
//     protocol-absolute or protocol-relative URL.
//   - Cookie-authenticated re-render POSTs carry the shell's CSRF proof
//     (`X-Datuplet-App-Render: 1`) with `Accept: application/json`; the browser
//     supplies the same-origin `Origin`/`Sec-Fetch-Site` the server also
//     checks (auth.go checkCSRF). We never weaken those headers.
//   - Re-render bodies are string->string ONLY (§6.5: "no type coercion — apps
//     parse their own numbers"; W6 now REJECTS non-string body values), which
//     is guaranteed by construction: params originate from URLSearchParams,
//     whose values are always strings.
//   - Partial (modal) content fetched via `block=<id>` is rendered through the
//     SAME renderer registry the initial page uses (deps.renderBlock) — there
//     is no second, unsanitized render path.
//   - Nothing here assigns innerHTML; all text reaches the DOM via textContent
//     and all structure via createElement/appendChild.

// Auto-refresh bounds (spec §6.3: refreshInterval clamped to [15, 3600]). The
// server already clamps; MIN_REFRESH_SECONDS is a client-side floor so a bundle
// can NEVER drive continuous sub-15s re-rendering even if a doc slipped a
// smaller value past the server. We never schedule a poll below this.
const MIN_REFRESH_SECONDS = 15;
const MAX_REFRESH_SECONDS = 3600;
// ±10% jitter spreads polls so many viewers of one app do not stampede the
// worker on the same tick (spec §6.3 "±10 % jitter").
const REFRESH_JITTER = 0.1;
// Exponential-backoff ceiling: 2^6 = 64x the base interval, capped separately
// at MAX_REFRESH_SECONDS.
const MAX_BACKOFF_SHIFTS = 6;

// MODAL_PARAM is the reserved URL key that deep-links an open (lazy) modal. It
// lives in its own `__dtp_`-prefixed namespace so it can NEVER collide with an
// app filter param: opening a modal writes `?__dtp_modal=<block-id>` and closing
// deletes that key, without ever touching an app param name. It is stripped from
// the ctx.params view on BOTH sides — getParams() below and app-worker's
// readParams (modalStateParam) — so it never reaches the guest.
const MODAL_PARAM = "__dtp_modal";

// Reserved param names the platform owns (spec §6.5). Stripped from the
// ctx.params VIEW sent to the server; app filters can never author them. Note
// setParams preserves them in the URL (they are platform bookkeeping) — only
// the getParams() view drops them.
const RESERVED_PARAMS = ["token", "block", MODAL_PARAM];

// Injected by shell.js's initInteract — the render primitives that live with
// the RENDERERS registry. Kept as injected deps (rather than importing
// shell.js) so this module has no import cycle with the module that imports it.
let deps = {
  // applyDoc(doc): clear #dtp-root and render the doc's title + blocks.
  applyDoc: null,
  // renderBlock(block) -> Node: dispatch one block through RENDERERS.
  renderBlock: null,
};

// The most recently applied OutputDoc — the scheduler reads refreshInterval
// off it, and popstate/modal-restore re-derive from it.
let currentDoc = null;

// Render sequencing (fix: out-of-order responses must never overwrite a newer
// render). Every FULL re-render (filter change, onClick, auto-refresh tick,
// ctx.path nav) takes a monotonic token and, if it is superseded before its
// response lands, drops that response instead of applying it. The in-flight
// request of a superseded render is aborted (its abort is treated as
// "superseded", never as an error state). renderSeq is shared by all full
// renders so a user render and an auto-refresh poll can never both apply.
let renderSeq = 0;
let activeRenderController = null;

// beginRender advances the sequence, aborts the previous full render's in-flight
// fetch, and returns this render's token + AbortSignal.
function beginRender() {
  renderSeq += 1;
  if (activeRenderController) activeRenderController.abort();
  activeRenderController = typeof AbortController === "function" ? new AbortController() : null;
  return { seq: renderSeq, signal: activeRenderController ? activeRenderController.signal : undefined };
}

// isAbortError reports whether a rejection is a fetch abort (a superseded
// render), which must NOT surface as an error state.
function isAbortError(error) {
  return !!error && (error.name === "AbortError" || error.code === 20);
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

// initInteract stores the render primitives shell.js owns. Called once, before
// boot(), so every function below can rely on deps being present.
export function initInteract(injected) {
  deps = { applyDoc: injected.applyDoc, renderBlock: injected.renderBlock };
}

// onBooted runs once after the initial in-page render (shell.js boot()): it
// installs the process-lifetime listeners and starts the refresh cadence, then
// re-opens any modal the URL deep-links to.
export function onBooted(doc) {
  currentDoc = doc;
  installVisibilityHandler();
  installPopstateHandler();
  installNavLinkHandler();
  installModalKeyHandler();
  configureAutoRefresh(doc);
  restoreModalDeepLink(doc);
}

// ---------------------------------------------------------------------------
// Param state — URL query string is the single source of truth (spec §6.3
// "filter changes set URL params and re-render … any filter state is a
// shareable link").
// ---------------------------------------------------------------------------

// readAllParams reads the FULL URL query as a string->string map, INCLUDING the
// reserved platform keys — the mutation base for setParams, so a filter change
// preserves `__dtp_modal` (and vice versa). Not sent to the server.
function readAllParams() {
  const usp = new URLSearchParams(location.search);
  const out = {};
  for (const [k, v] of usp.entries()) out[k] = v;
  return out;
}

// getParams is the ctx.params VIEW: readAllParams with the reserved names
// (token/block/__dtp_modal) stripped. Values are strings by construction
// (URLSearchParams), which is exactly what the re-render POST body must carry.
export function getParams() {
  const all = readAllParams();
  const out = {};
  for (const key of Object.keys(all)) {
    if (RESERVED_PARAMS.indexOf(key) !== -1) continue;
    out[key] = all[key];
  }
  return out;
}

// setParams merges a patch into the URL params (deep-link), then — unless
// opts.refresh === false — triggers a re-render. A null/undefined patch value
// deletes that key; everything else is coerced to a string, so the URL (and
// therefore the next re-render body) is always string->string. It operates over
// readAllParams so a change to one key never drops the reserved modal key.
export function setParams(patch, opts) {
  const refresh = !opts || opts.refresh !== false;
  const params = readAllParams();
  for (const key of Object.keys(patch)) {
    const v = patch[key];
    if (v === null || v === undefined) delete params[key];
    else params[key] = String(v);
  }
  history.pushState({}, "", pathWithParams(params));
  if (refresh) requestRerender();
}

// setParam is the single-key convenience filters and onClick use.
export function setParam(name, value, opts) {
  setParams({ [name]: value }, opts);
}

// pathWithParams builds a same-origin, path-relative URL for the current app
// path plus the given params. Never absolute — deep links stay on this origin.
function pathWithParams(params) {
  const usp = new URLSearchParams();
  for (const key of Object.keys(params)) usp.set(key, params[key]);
  const qs = usp.toString();
  return location.pathname + (qs ? "?" + qs : "");
}

// ---------------------------------------------------------------------------
// Re-render fetch (spec §4.2 response matrix; §6.3 "filter change POSTs new
// params to the same endpoint → full stateless re-render").
// ---------------------------------------------------------------------------

// fetchRender POSTs the current param state to the current app URL and returns
// the parsed JSON. With no block it returns the full OutputDoc; with a block it
// returns that single block (the §4.2 `block=<id>` partial-render used by lazy
// modals). The request is same-origin and carries the CSRF proof + string-only
// body described in the module header.
async function fetchRender(opts) {
  const block = opts && opts.block;
  const signal = opts && opts.signal;
  // Same-origin, path-relative: the current app URL, nothing else. `block` is
  // the one selector that rides the query string (matches §4.2 `?block=<id>`).
  let url = location.pathname;
  if (block) url += "?block=" + encodeURIComponent(block);

  const resp = await fetch(url, {
    method: "POST",
    credentials: "same-origin",
    signal,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Datuplet-App-Render": "1",
    },
    // string->string by construction (getParams) — §6.5 forbids coercion and
    // W6 rejects any non-string body value.
    body: JSON.stringify(getParams()),
  });
  if (!resp.ok) {
    const err = new Error("re-render failed with status " + resp.status);
    err.status = resp.status;
    err.retryAfter = resp.headers.get("Retry-After");
    throw err;
  }
  return resp.json();
}

// doFullRender is the shared core of every full re-render (filter change,
// onClick, auto-refresh). It shows the stale-while-revalidating overlay (spec
// §6.3 "the stale dashboard stays visible, dimmed, with a spinner"), fetches
// the whole doc, and swaps the blocks in on success. It is SEQUENCED: it takes
// a render token and, if a newer render started before its response lands (or
// its fetch was aborted by that newer render), it DROPS the response — a slow
// earlier response can never overwrite a newer one. On a real failure the stale
// content is left untouched. Returns one of {ok,doc} | {superseded} | {error}.
async function doFullRender() {
  const { seq, signal } = beginRender();
  showRefreshOverlay();
  try {
    const doc = await fetchRender({ signal });
    if (seq !== renderSeq) return { superseded: true }; // a newer render won
    if (deps.applyDoc) deps.applyDoc(doc);
    currentDoc = doc;
    setBaseIntervalFromDoc(doc);
    restoreModalDeepLink(doc);
    return { ok: true, doc };
  } catch (error) {
    if (isAbortError(error) || seq !== renderSeq) return { superseded: true };
    return { error };
  } finally {
    hideRefreshOverlay();
  }
}

// requestRerender is the user-initiated full re-render (filters, onClick). Its
// beginRender() cancels any in-flight auto-refresh poll, so the two can never
// both apply. On success it resets backoff and re-arms the cadence around the
// fresh render; on a real error it re-arms at the base cadence (a user-render
// failure must not silently kill auto-refresh) and surfaces a transient note; a
// SUPERSEDED result (a newer render already started) is a no-op — that newer
// render owns rescheduling.
export async function requestRerender() {
  const result = await doFullRender();
  if (result.superseded) return result;
  if (result.ok) refreshFailures = 0;
  else showTransientError();
  scheduleNextRefresh();
  return result;
}

// ---------------------------------------------------------------------------
// Auto-refresh scheduler (spec §6.3: refreshInterval seconds, ±10 % jitter,
// paused while the tab is hidden, exponential backoff on 429/503 honoring
// Retry-After).
// ---------------------------------------------------------------------------

let refreshTimer = null;
let refreshBaseSeconds = 0; // 0 = auto-refresh disabled (no refreshInterval)
let refreshFailures = 0;
let visibilityInstalled = false;

// configureAutoRefresh (re)initializes the cadence from a freshly applied doc.
function configureAutoRefresh(doc) {
  setBaseIntervalFromDoc(doc);
  refreshFailures = 0;
  scheduleNextRefresh();
}

// setBaseIntervalFromDoc reads refreshInterval and applies the [15, 3600]
// clamp. The server already clamps; this is the client-side floor that makes
// sub-15s re-rendering impossible regardless of what a doc carries. A missing
// or non-positive value disables auto-refresh.
function setBaseIntervalFromDoc(doc) {
  const raw = doc && typeof doc.refreshInterval === "number" ? doc.refreshInterval : 0;
  if (!(raw > 0)) {
    refreshBaseSeconds = 0;
    return;
  }
  refreshBaseSeconds = Math.min(MAX_REFRESH_SECONDS, Math.max(MIN_REFRESH_SECONDS, raw));
}

function clearRefreshTimer() {
  if (refreshTimer !== null) {
    clearTimeout(refreshTimer);
    refreshTimer = null;
  }
}

// scheduleNextRefresh arms the next poll. With no override it uses the base
// interval plus jitter; an override (backoff) is already bounded by
// backoffSeconds. Either way the delay is clamped to [base, MAX_REFRESH_SECONDS]
// here as a final guard, so it is never below the 15s floor and — critically —
// never large enough to overflow setTimeout's ~2^31 ms limit (a huge
// Retry-After that wrapped would otherwise fire almost immediately).
function scheduleNextRefresh(overrideSeconds) {
  clearRefreshTimer();
  if (refreshBaseSeconds <= 0) return; // disabled
  let seconds = overrideSeconds != null ? overrideSeconds : jitteredInterval(refreshBaseSeconds);
  if (!(seconds >= refreshBaseSeconds)) seconds = refreshBaseSeconds; // floor (also catches NaN)
  seconds = Math.min(MAX_REFRESH_SECONDS, seconds); // ceiling — no setTimeout overflow
  refreshTimer = setTimeout(refreshTick, seconds * 1000);
}

// jitteredInterval returns base ± up to REFRESH_JITTER (10%).
function jitteredInterval(base) {
  const factor = 1 + (Math.random() * 2 - 1) * REFRESH_JITTER;
  return base * factor;
}

// refreshTick is the poll body. It pauses (fetches nothing) while the tab is
// hidden, and on a 429/503 backs off exponentially while honoring Retry-After.
async function refreshTick() {
  if (document.visibilityState === "hidden") {
    // Paused: do not fetch while hidden; just re-arm so we resume promptly.
    scheduleNextRefresh();
    return;
  }
  const result = await doFullRender();
  if (result.superseded) return; // a newer render (e.g. a user filter change) owns rescheduling
  if (result.ok) {
    refreshFailures = 0;
    scheduleNextRefresh();
    return;
  }
  refreshFailures += 1;
  scheduleNextRefresh(backoffSeconds(result.error));
}

// backoffSeconds computes the next delay after a failed poll. It is always
// >= the base interval (so it can only slow the cadence, never breach the 15s
// floor) and, on a 429/503 carrying Retry-After, always >= that value (so the
// server's back-pressure is honored — we wait at least as long as it asked).
function backoffSeconds(error) {
  const base = refreshBaseSeconds > 0 ? refreshBaseSeconds : MIN_REFRESH_SECONDS;
  const shifts = Math.min(refreshFailures, MAX_BACKOFF_SHIFTS);
  let seconds = Math.min(MAX_REFRESH_SECONDS, base * Math.pow(2, shifts));
  if (error && (error.status === 429 || error.status === 503)) {
    const retryAfter = parseRetryAfter(error.retryAfter);
    if (retryAfter != null) seconds = Math.max(seconds, retryAfter);
  }
  // Cap the honored Retry-After too (fix: an unbounded/hostile Retry-After must
  // not overflow setTimeout's ~2^31 ms limit and wrap to fire immediately). A
  // server asking for longer than the ceiling just gets MAX_REFRESH_SECONDS —
  // still >= base, so "wait at least base" is honored and we never busy-loop.
  seconds = Math.min(MAX_REFRESH_SECONDS, seconds);
  return Math.max(base, seconds);
}

// parseRetryAfter accepts either form the HTTP spec allows: delta-seconds (an
// integer, which is what app-worker sends) or an HTTP-date. Returns seconds
// from now, or null if unparseable.
function parseRetryAfter(value) {
  if (!value) return null;
  const asInt = Number(value);
  if (Number.isFinite(asInt) && asInt >= 0) return asInt;
  const when = Date.parse(value);
  if (!Number.isNaN(when)) {
    const delta = (when - Date.now()) / 1000;
    return delta > 0 ? delta : 0;
  }
  return null;
}

// installVisibilityHandler resumes the cadence the moment the tab is shown
// again (spec §6.3 "paused while the tab is hidden").
function installVisibilityHandler() {
  if (visibilityInstalled) return;
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") scheduleNextRefresh();
  });
  visibilityInstalled = true;
}

// ---------------------------------------------------------------------------
// ctx.path navigation (spec §6.3: "route-based via ctx.path sub-paths"). Links
// the doc emits that point within THIS app are intercepted and turned into an
// in-shell full re-render for the new path; everything else (external links,
// other apps) navigates normally. GET is a safe method — CSRF-exempt (auth.go)
// — and stays same-origin by construction (we only intercept same-app paths).
// ---------------------------------------------------------------------------

let navLinkInstalled = false;
let popstateInstalled = false;

// appBasePath is `/apps/{pid}/{name}` — the first three path segments after the
// leading slash. pid and name are each a single segment (name may carry the
// @draft suffix), so the base is unambiguous from the URL alone.
function appBasePath() {
  const parts = location.pathname.split("/"); // ["", "apps", pid, name, ...sub]
  return "/" + parts.slice(1, 4).join("/");
}

function installNavLinkHandler() {
  if (navLinkInstalled) return;
  document.addEventListener("click", (event) => {
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }
    const anchor = event.target && event.target.closest ? event.target.closest("a[href]") : null;
    if (!anchor) return;
    const href = anchor.getAttribute("href");
    if (!href) return;
    const url = new URL(href, location.href);
    if (url.origin !== location.origin) return; // external → let the browser handle it
    const base = appBasePath();
    if (url.pathname !== base && url.pathname.indexOf(base + "/") !== 0) return; // other app/page
    // Same-app ctx.path navigation: keep it in-shell.
    event.preventDefault();
    loadPath(url.pathname + url.search, true);
  });
  navLinkInstalled = true;
}

function installPopstateHandler() {
  if (popstateInstalled) return;
  window.addEventListener("popstate", () => {
    loadPath(location.pathname + location.search, false);
  });
  popstateInstalled = true;
}

// loadPath performs an in-shell navigation to a same-app path: a GET full
// re-render (Accept: application/json) plus a history push when `push` is set.
// SEQUENCED like doFullRender (a nav during an in-flight filter re-render must
// win, and a stale nav response must never overwrite a newer render).
async function loadPath(pathAndQuery, push) {
  if (push) history.pushState({}, "", pathAndQuery);
  const { seq, signal } = beginRender();
  showRefreshOverlay();
  try {
    const resp = await fetch(pathAndQuery, {
      method: "GET",
      credentials: "same-origin",
      signal,
      headers: { Accept: "application/json" },
    });
    if (!resp.ok) throw new Error("navigation failed with status " + resp.status);
    const doc = await resp.json();
    if (seq !== renderSeq) return; // superseded by a newer render
    if (deps.applyDoc) deps.applyDoc(doc);
    currentDoc = doc;
    configureAutoRefresh(doc);
    restoreModalDeepLink(doc);
  } catch (error) {
    if (isAbortError(error) || seq !== renderSeq) return; // superseded — not an error state
    showTransientError();
  } finally {
    hideRefreshOverlay();
  }
}

// ---------------------------------------------------------------------------
// Modals (spec §6.3). Two forms: inline `{title, blocks}` (content already
// present, shown client-side) and lazy `{param}` (sets the deep-link param and
// fetches its content as a `block=<id>` partial render). v1 modals only display
// and set params — no side-effectful submits (that is writebacks, §10).
//
// Contract for the lazy form: the declared `param` is the id of the block the
// app emits as the modal body (the app, seeing the modal open, includes a block
// whose id === param). Opening records that id in the RESERVED deep-link key
// `?__dtp_modal=<param>` — its own `__dtp_`-namespace, so it can NEVER clobber
// an app filter param of the same name — and partial-fetches `block=<param>`.
// This makes "modal state deep-links via a URL param" true while keeping the
// URL bookkeeping entirely off the app's param namespace (and the reserved key
// is stripped from ctx.params on both sides, so the guest never sees it).
// ---------------------------------------------------------------------------

let activeModal = null;
let modalKeyInstalled = false;

// openModal dispatches a modal spec to the inline or lazy path.
export function openModal(modalSpec) {
  if (!modalSpec || typeof modalSpec !== "object") return;
  if (Array.isArray(modalSpec.blocks)) {
    openInlineModal(modalSpec.title, modalSpec.blocks);
  } else if (typeof modalSpec.param === "string") {
    openLazyModal(modalSpec.param, { title: modalSpec.title });
  }
}

// attachModalTrigger wires an open affordance for a block or table row that
// declares a modal. A block gets a small "Details" button; a row (opts.asRow)
// becomes clickable in its entirety.
export function attachModalTrigger(node, modalSpec, opts) {
  if (!node || !modalSpec || typeof modalSpec !== "object") return;
  if (opts && opts.asRow) {
    node.classList.add("dtp-has-modal");
    node.addEventListener("click", () => openModal(modalSpec));
    return;
  }
  const button = document.createElement("button");
  button.type = "button";
  button.className = "dtp-modal-trigger";
  button.textContent = (opts && opts.label) || "Details";
  button.addEventListener("click", () => openModal(modalSpec));
  node.appendChild(button);
}

function openInlineModal(title, blocks) {
  const modal = openModalShell(title, false);
  const frag = document.createDocumentFragment();
  const list = Array.isArray(blocks) ? blocks : [];
  for (const block of list) {
    if (deps.renderBlock) frag.appendChild(deps.renderBlock(block));
  }
  modal.setBody(frag);
}

async function openLazyModal(param, opts) {
  const title = opts && typeof opts.title === "string" ? opts.title : "";
  // Deep-link via the RESERVED modal key (never an app param name) — no full
  // re-render; the modal fetches only its own block.
  setParams({ [MODAL_PARAM]: param }, { refresh: false });
  const modal = openModalShell(title, true);
  modal.setLoading();
  try {
    const block = await fetchRender({ block: param });
    // Same renderer path as the page — partial content is sanitized identically.
    modal.setBody(deps.renderBlock ? deps.renderBlock(block) : document.createTextNode(""));
  } catch (_error) {
    modal.setError();
  }
}

// openModalShell builds the backdrop + dialog and returns a small controller.
// `lazy` (a deep-linked lazy modal) causes the reserved MODAL_PARAM to be
// cleared from the URL on close, so the deep link closes with the modal.
function openModalShell(title, lazy) {
  closeActiveModal();

  const backdrop = document.createElement("div");
  backdrop.className = "dtp-modal-backdrop";
  backdrop.addEventListener("click", (event) => {
    if (event.target === backdrop) closeActiveModal();
  });

  const dialog = document.createElement("div");
  dialog.className = "dtp-modal";
  dialog.setAttribute("role", "dialog");
  dialog.setAttribute("aria-modal", "true");

  const header = document.createElement("div");
  header.className = "dtp-modal-header";
  const titleEl = document.createElement("div");
  titleEl.className = "dtp-modal-title";
  titleEl.textContent = typeof title === "string" ? title : "";
  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "dtp-modal-close";
  closeBtn.setAttribute("aria-label", "Close");
  closeBtn.textContent = "×"; // ×
  closeBtn.addEventListener("click", () => closeActiveModal());
  header.appendChild(titleEl);
  header.appendChild(closeBtn);

  const body = document.createElement("div");
  body.className = "dtp-modal-body";

  dialog.appendChild(header);
  dialog.appendChild(body);
  backdrop.appendChild(dialog);
  document.body.appendChild(backdrop);

  activeModal = { backdrop, lazy };

  return {
    setLoading() {
      body.textContent = "Loading…";
    },
    setError() {
      body.textContent = "This content could not be displayed.";
    },
    setBody(node) {
      body.textContent = "";
      body.appendChild(node);
    },
  };
}

function closeActiveModal() {
  if (!activeModal) return;
  const { backdrop, lazy } = activeModal;
  activeModal = null;
  // Finalize any Vega view rendered into the modal before its DOM is dropped
  // (fix: views leak otherwise — see finalizeVegaViewsWithin).
  if (backdrop) finalizeVegaViewsWithin(backdrop);
  if (backdrop && backdrop.parentNode) backdrop.parentNode.removeChild(backdrop);
  // Closing a deep-linked (lazy) modal cleans the RESERVED modal key out of the
  // URL — never an app param.
  if (lazy) setParams({ [MODAL_PARAM]: null }, { refresh: false });
}

function installModalKeyHandler() {
  if (modalKeyInstalled) return;
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && activeModal) closeActiveModal();
  });
  modalKeyInstalled = true;
}

// restoreModalDeepLink re-opens the lazy modal named by the RESERVED modal key
// in the URL on load / after navigation, so a shared deep link lands with the
// modal open. The key is read from readAllParams (getParams strips it), and the
// title (if any) comes from the doc's matching declaration.
function restoreModalDeepLink(doc) {
  if (activeModal) return; // do not fight an already-open modal
  const openId = readAllParams()[MODAL_PARAM];
  if (openId == null) return;
  const match = collectLazyModalParams(doc).find((entry) => entry.param === openId);
  openLazyModal(openId, { title: match ? match.title : "" });
}

// collectLazyModalParams walks the doc (root blocks, tabs, table rows, and
// nested inline-modal blocks) collecting every lazy `modal: {param}`
// declaration — mirroring the server's block walk.
function collectLazyModalParams(doc) {
  const found = [];
  const visitModal = (modal) => {
    if (modal && typeof modal === "object" && typeof modal.param === "string" && !Array.isArray(modal.blocks)) {
      found.push({ param: modal.param, title: typeof modal.title === "string" ? modal.title : "" });
    }
  };
  const visitBlocks = (blocks) => {
    if (!Array.isArray(blocks)) return;
    for (const block of blocks) {
      if (!block || typeof block !== "object") continue;
      visitModal(block.modal);
      if (block.modal && Array.isArray(block.modal.blocks)) visitBlocks(block.modal.blocks);
      if (block.type === "tabs" && Array.isArray(block.tabs)) {
        for (const tab of block.tabs) visitBlocks(tab && tab.blocks);
      }
      if (block.type === "table" && Array.isArray(block.rows)) {
        for (const row of block.rows) {
          if (row && typeof row === "object" && !Array.isArray(row)) visitModal(row.modal);
        }
      }
    }
  };
  visitBlocks(doc && doc.blocks);
  return found;
}

// ---------------------------------------------------------------------------
// Vega-Lite onClick cross-filter binding (spec §6.3: "a chart block may declare
// onClick: {param}; the shell sets the param and re-renders. Config, not code").
// ---------------------------------------------------------------------------

// bindChartOnClick attaches a click listener to the Vega view (the object
// vega-embed resolves) that sets `param` from the clicked datum and re-renders.
// The value is the clicked datum's field matching `param` when present, else
// the datum's first non-internal field (Vega prefixes internal fields with
// "_"). Called by blocks/chart.js after mountVegaLiteChart resolves. RETURNS the
// listener handle so it can be removed at finalize time (fix: leak).
export function bindChartOnClick(view, param) {
  if (!view || typeof view.addEventListener !== "function" || typeof param !== "string" || !param) return undefined;
  const handler = (_event, item) => {
    if (!item || !item.datum) return;
    const datum = item.datum;
    let value;
    if (Object.prototype.hasOwnProperty.call(datum, param)) value = datum[param];
    else value = firstDatumValue(datum);
    if (value === undefined || value === null) return;
    setParam(param, String(value));
  };
  view.addEventListener("click", handler);
  return handler;
}

function firstDatumValue(datum) {
  for (const key of Object.keys(datum)) {
    if (key.charAt(0) === "_") continue; // skip Vega internal fields
    return datum[key];
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Vega view lifecycle (fix: views leaked on every swap). applyDoc clears
// #dtp-root with textContent="", which drops DOM nodes but never calls
// vega-embed's result.finalize() — so each re-render/auto-refresh leaked the
// prior view's listeners/timers/dataflow, unbounded on a long-lived
// auto-refreshing dashboard. chart.js registers each mounted view here; shell.js
// (applyDoc) and closeActiveModal drain the relevant ones BEFORE dropping the DOM.
// ---------------------------------------------------------------------------

// liveVegaViews holds {result, listener, mount} for every currently-mounted
// chart. mount is the chart's mount element, used to scope finalization.
const liveVegaViews = new Set();

// registerVegaView records a mounted chart so it can be finalized before its
// DOM is removed. Called by blocks/chart.js once vega-embed resolves.
export function registerVegaView(entry) {
  if (entry && entry.result) liveVegaViews.add(entry);
}

// finalizeVegaEntry finalizes one embed result and removes its click listener.
function finalizeVegaEntry(entry) {
  try {
    if (entry.result && entry.result.view && entry.listener && typeof entry.result.view.removeEventListener === "function") {
      entry.result.view.removeEventListener("click", entry.listener);
    }
  } catch (_e) {
    // ignore — best-effort teardown
  }
  try {
    if (entry.result && typeof entry.result.finalize === "function") entry.result.finalize();
  } catch (_e) {
    // ignore — best-effort teardown
  }
  liveVegaViews.delete(entry);
}

// finalizeVegaViewsWithin finalizes every registered view whose mount is inside
// `container` (about to be cleared) OR is already detached from the document (a
// mount that resolved after its container was cleared — a race orphan). A view
// mounted elsewhere (e.g. a chart in an OPEN modal during a background
// re-render) is left alone. Called with #dtp-root before applyDoc clears it,
// and with the modal backdrop before a modal closes.
export function finalizeVegaViewsWithin(container) {
  for (const entry of Array.from(liveVegaViews)) {
    const mount = entry.mount;
    const inContainer = container && mount && typeof container.contains === "function" && container.contains(mount);
    const orphaned = !mount || mount.isConnected === false;
    if (inContainer || orphaned) finalizeVegaEntry(entry);
  }
}

// ---------------------------------------------------------------------------
// Loading state (spec §6.3 "Loading states": stale content dimmed + spinner).
// ---------------------------------------------------------------------------

let overlayDepth = 0;

function shellRoot() {
  return document.getElementById("dtp-root");
}

function showRefreshOverlay() {
  overlayDepth += 1;
  const root = shellRoot();
  if (root) root.classList.add("dtp-refreshing");
  if (!document.getElementById("dtp-refresh-overlay")) {
    const overlay = document.createElement("div");
    overlay.id = "dtp-refresh-overlay";
    overlay.className = "dtp-refresh-overlay";
    const spinner = document.createElement("div");
    spinner.className = "dtp-spinner";
    overlay.appendChild(spinner);
    document.body.appendChild(overlay);
  }
}

function hideRefreshOverlay() {
  overlayDepth = Math.max(0, overlayDepth - 1);
  if (overlayDepth > 0) return;
  const root = shellRoot();
  if (root) root.classList.remove("dtp-refreshing");
  const overlay = document.getElementById("dtp-refresh-overlay");
  if (overlay && overlay.parentNode) overlay.parentNode.removeChild(overlay);
}

function showTransientError() {
  const existing = document.getElementById("dtp-refresh-error");
  if (existing && existing.parentNode) existing.parentNode.removeChild(existing);
  const note = document.createElement("div");
  note.id = "dtp-refresh-error";
  note.className = "dtp-refresh-error";
  note.textContent = "Could not refresh. Showing the last result.";
  document.body.appendChild(note);
  setTimeout(() => {
    if (note.parentNode) note.parentNode.removeChild(note);
  }, 5000);
}
