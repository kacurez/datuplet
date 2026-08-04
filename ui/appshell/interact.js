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

// Reserved param names the platform owns (spec §6.5). Stripped from ctx.params
// on both the read side (here) and the server; a filter can never author them.
const RESERVED_PARAMS = ["token", "block"];

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

// getParams reads ctx.params out of the current URL: a flat string->string map
// with the reserved names stripped. Values are strings by construction
// (URLSearchParams), which is exactly what the re-render POST body must carry.
export function getParams() {
  const usp = new URLSearchParams(location.search);
  const out = {};
  for (const [k, v] of usp.entries()) {
    if (RESERVED_PARAMS.indexOf(k) !== -1) continue;
    out[k] = v;
  }
  return out;
}

// setParams merges a patch into the URL params (deep-link), then — unless
// opts.refresh === false — triggers a re-render. A null/undefined patch value
// deletes that key; everything else is coerced to a string, so the URL (and
// therefore the next re-render body) is always string->string.
export function setParams(patch, opts) {
  const refresh = !opts || opts.refresh !== false;
  const params = getParams();
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
  // Same-origin, path-relative: the current app URL, nothing else. `block` is
  // the one selector that rides the query string (matches §4.2 `?block=<id>`).
  let url = location.pathname;
  if (block) url += "?block=" + encodeURIComponent(block);

  const resp = await fetch(url, {
    method: "POST",
    credentials: "same-origin",
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
// the whole doc, and swaps the blocks in on success; on failure the stale
// content is left untouched.
async function doFullRender() {
  showRefreshOverlay();
  try {
    const doc = await fetchRender({});
    if (deps.applyDoc) deps.applyDoc(doc);
    currentDoc = doc;
    setBaseIntervalFromDoc(doc);
    restoreModalDeepLink(doc);
    return { ok: true, doc };
  } catch (error) {
    return { ok: false, error };
  } finally {
    hideRefreshOverlay();
  }
}

// requestRerender is the user-initiated full re-render (filters, onClick). It
// resets the backoff and re-arms the auto-refresh cadence around the fresh
// render so a manual change does not stack an extra poll right behind it.
export async function requestRerender() {
  const result = await doFullRender();
  if (result.ok) {
    refreshFailures = 0;
    scheduleNextRefresh();
  } else {
    showTransientError();
  }
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
// interval plus jitter; an override (backoff) is used verbatim. Either way the
// delay is never below the base interval (>= 15s), so the clamp always holds.
function scheduleNextRefresh(overrideSeconds) {
  clearRefreshTimer();
  if (refreshBaseSeconds <= 0) return; // disabled
  const seconds = overrideSeconds != null ? overrideSeconds : jitteredInterval(refreshBaseSeconds);
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
async function loadPath(pathAndQuery, push) {
  if (push) history.pushState({}, "", pathAndQuery);
  showRefreshOverlay();
  try {
    const resp = await fetch(pathAndQuery, {
      method: "GET",
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    });
    if (!resp.ok) throw new Error("navigation failed with status " + resp.status);
    const doc = await resp.json();
    if (deps.applyDoc) deps.applyDoc(doc);
    currentDoc = doc;
    configureAutoRefresh(doc);
    restoreModalDeepLink(doc);
  } catch (_error) {
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
// Contract for the lazy form: the declared `param` names BOTH the deep-link URL
// param and the id of the block the app emits as the modal body. Opening sets
// `?<param>=1` (a presence marker for the deep link) and partial-fetches
// `block=<param>`; the app, seeing the param in ctx.params, includes a block
// whose id === param. This makes "modal state deep-links via a URL param"
// literally true and reuses the §4.2 partial-render matrix.
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
  const modal = openModalShell(title, null);
  const frag = document.createDocumentFragment();
  const list = Array.isArray(blocks) ? blocks : [];
  for (const block of list) {
    if (deps.renderBlock) frag.appendChild(deps.renderBlock(block));
  }
  modal.setBody(frag);
}

async function openLazyModal(param, opts) {
  const title = opts && typeof opts.title === "string" ? opts.title : "";
  // Deep-link: mark the param present (without a full re-render — the modal
  // fetches only its own block).
  if (getParams()[param] == null) setParams({ [param]: "1" }, { refresh: false });
  const modal = openModalShell(title, param);
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
// lazyParam, when set, is cleared from the URL on close so the deep link
// closes with the modal.
function openModalShell(title, lazyParam) {
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

  activeModal = { backdrop, lazyParam };

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
  const { backdrop, lazyParam } = activeModal;
  activeModal = null;
  if (backdrop && backdrop.parentNode) backdrop.parentNode.removeChild(backdrop);
  // Closing a deep-linked (lazy) modal cleans its param out of the URL.
  if (lazyParam) setParams({ [lazyParam]: null }, { refresh: false });
}

function installModalKeyHandler() {
  if (modalKeyInstalled) return;
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && activeModal) closeActiveModal();
  });
  modalKeyInstalled = true;
}

// restoreModalDeepLink re-opens a lazy modal whose param is present in the URL
// on load / after navigation, so a shared deep link lands with the modal open.
function restoreModalDeepLink(doc) {
  if (activeModal) return; // do not fight an already-open modal
  const params = getParams();
  const lazy = collectLazyModalParams(doc);
  for (const entry of lazy) {
    if (params[entry.param] != null) {
      openLazyModal(entry.param, { title: entry.title });
      return; // one modal at a time
    }
  }
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
// "_"). Called by blocks/chart.js after mountVegaLiteChart resolves.
export function bindChartOnClick(view, param) {
  if (!view || typeof view.addEventListener !== "function" || typeof param !== "string" || !param) return;
  view.addEventListener("click", (_event, item) => {
    if (!item || !item.datum) return;
    const datum = item.datum;
    let value;
    if (Object.prototype.hasOwnProperty.call(datum, param)) value = datum[param];
    else value = firstDatumValue(datum);
    if (value === undefined || value === null) return;
    setParam(param, String(value));
  });
}

function firstDatumValue(datum) {
  for (const key of Object.keys(datum)) {
    if (key.charAt(0) === "_") continue; // skip Vega internal fields
    return datum[key];
  }
  return undefined;
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
