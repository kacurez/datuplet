// ui/appshell/shell.js — RFC 028 Part 4 trusted viewer shell boot loader.
//
// This file is platform-owned, trusted code (spec §6.4 "untrusted code,
// trusted output"). The ONLY app-controlled input is the declarative
// OutputDoc embedded by app-worker in the #dtp-doc <script
// type="application/json"> island (already HTML-escaped server-side —
// pkg/appworker's escapeJSONForScript — so it round-trips through
// JSON.parse to the author's original bytes). Every string that doc carries
// is untrusted and MUST be rendered via textContent (or, for markdown in
// V1, marked + DOMPurify with a fixed allowlist) — NEVER innerHTML or
// insertAdjacentHTML with doc-derived text.
//
// Scope: read the doc, render the title, and dispatch each top-level block to a
// renderer looked up in RENDERERS by its `type` (spec §6.3: markdown | metric |
// table | chart | filter | tabs). All six renderers are registered (V1: the
// four data blocks; V2: filter + tabs). Interactivity — filters, tabs, modals,
// partial refresh, and auto-refresh — lives in interact.js (spec §6.3 "All
// dynamics live in the platform-owned shell"); this module owns the mount
// primitives (RENDERERS, renderBlock, applyDoc, boot) and hands interact.js the
// two it needs so there is no import cycle.
//
// V3 additions: renderEmptyState (shared "nothing to show" placeholder, used
// here for a zero-block doc and reused by blocks/table.js + blocks/chart.js
// for "no data"), an aria-busy handoff with index.html's first-paint skeleton,
// and mountVegaLiteChart now forces the SVG renderer so theme.css's print
// stylesheet can recolor chart text for paper (see that function's comment).

import { renderMarkdown } from "./blocks/markdown.js";
import { renderMetric } from "./blocks/metric.js";
import { renderTable } from "./blocks/table.js";
import { renderChart } from "./blocks/chart.js";
import { renderFilter } from "./blocks/filter.js";
import { renderTabs } from "./blocks/tabs.js";
import { initInteract, onBooted, attachModalTrigger, finalizeVegaViewsWithin } from "./interact.js";

const ROOT_ID = "dtp-root";
const DOC_SCRIPT_ID = "dtp-doc";

/**
 * RENDERERS maps an OutputDoc block `type` to a function `(block) => Node`.
 * All six block types of the v1 vocabulary are registered; each lives in its
 * own ui/appshell/blocks/ module. A block whose type is somehow absent from
 * RENDERERS still falls back to renderUnknownBlock, so a doc the shell doesn't
 * fully understand renders every OTHER block instead of failing the whole page.
 *
 * Each renderer returns a Node synchronously; markdown and chart return a
 * container with a loading state and fill it in once their lazily-imported
 * dependency resolves (see blocks/markdown.js, blocks/chart.js).
 *
 * @type {Record<string, (block: Record<string, unknown>) => Node>}
 */
export const RENDERERS = {
  markdown: renderMarkdown,
  metric: renderMetric,
  table: renderTable,
  chart: renderChart,
  filter: renderFilter,
  tabs: renderTabs,
};

/**
 * renderBlock dispatches one block through RENDERERS (falling back to
 * renderUnknownBlock) and, if the block declares a `modal` (spec §6.3), wires
 * an open affordance for it via interact.js. This is the single dispatch point
 * every mount site uses — top-level blocks, tabs' nested blocks, and modal
 * bodies — so modal wiring and the unknown-type fallback are applied uniformly.
 *
 * @param {Record<string, unknown>} block
 * @returns {Node}
 */
export function renderBlock(block) {
  const type = block && typeof block === "object" ? block.type : undefined;
  const renderer = (typeof type === "string" && RENDERERS[type]) || renderUnknownBlock;
  const node = renderer(block);
  if (block && typeof block === "object" && block.modal && typeof block.modal === "object") {
    attachModalTrigger(node, block.modal);
  }
  return node;
}

/**
 * renderUnknownBlock is the fallback for any block type absent from
 * RENDERERS. It never interprets block content as markup: only the block's
 * own `type`/`id` — structurally constrained by the OutputDoc schema, never
 * HTML — reach the DOM, and only via textContent.
 *
 * @param {Record<string, unknown>} block
 * @returns {HTMLElement}
 */
function renderUnknownBlock(block) {
  const el = document.createElement("div");
  el.className = "dtp-block dtp-block-unknown";
  const label = document.createElement("p");
  const type = block && typeof block === "object" ? block.type : undefined;
  const id = block && typeof block === "object" ? block.id : undefined;
  label.textContent = "unknown block: " + String(type) + " (id: " + String(id) + ")";
  el.appendChild(label);
  return el;
}

/**
 * mountVegaLiteChart is the ONE place a chart renderer may call vega-embed.
 * Network loading is disabled here — even though the shared
 * pkg/appengine/vegaspec / ui/appshell/vegaspec.schema.json subset already
 * rejects `data.url` and every other remote-reference shape (spec §6.4) —
 * as defense-in-depth: a validator miss must still never let the viewer's
 * browser fetch an attacker-chosen URL. `actions:false` also hides
 * vega-embed's built-in "open in Vega editor / view source / export" menu,
 * which would otherwise let a spec round-trip through an external site.
 *
 * V1's chart renderer (blocks/chart.js) routes through this function rather
 * than calling `vegaEmbed` directly, so the lockdown can never be forgotten
 * at a second call site. `config` is the PLATFORM Vega theme, authored by
 * the chart renderer and passed in here (spec §6.4: chart config comes from
 * the platform, never the app — an author `config` key is rejected by the
 * vegaspec subset schema outright, so `spec` can carry none). The lockdown
 * (`actions:false` + the load-rejecting loader) stays hardcoded here so a
 * caller cannot override it.
 *
 * `renderer:"svg"` (RFC 028 V3) is likewise hardcoded here: vega-embed
 * defaults to a canvas renderer, an opaque bitmap baked at mount time from
 * whatever theme (possibly dark) was active on screen — no CSS rule,
 * including theme.css's `@media print` override, can repaint its pixels.
 * SVG text is real DOM the print stylesheet CAN recolor, so this is
 * load-bearing for chart legibility on paper, not a rendering preference.
 *
 * @param {Element} el
 * @param {Record<string, unknown>} spec - already validated against
 *   vegaspec.schema.json (client-side defense-in-depth; app-worker is the
 *   authoritative gate, spec §6.4).
 * @param {Record<string, unknown>} [config] - platform-owned Vega config
 *   (theme). Never derived from `spec`.
 * @returns {Promise<unknown>}
 */
export function mountVegaLiteChart(el, spec, config) {
  return window.vegaEmbed(el, spec, {
    actions: false,
    loader: { load: () => Promise.reject(new Error("loading disabled")) },
    renderer: "svg",
    config,
  });
}

/**
 * readEmbeddedDoc parses the OutputDoc out of the #dtp-doc JSON island.
 * @returns {Record<string, unknown>}
 */
function readEmbeddedDoc() {
  const script = document.getElementById(DOC_SCRIPT_ID);
  if (!script) {
    throw new Error("dtp: missing #" + DOC_SCRIPT_ID + " doc island");
  }
  return JSON.parse(script.textContent);
}

/**
 * renderTitle sets the page heading. textContent only — never innerHTML
 * (spec §6.4): `doc.title` is app-authored text, not markup.
 * @param {Element} root
 * @param {Record<string, unknown>} doc
 */
function renderTitle(root, doc) {
  const heading = document.createElement("h1");
  heading.className = "dtp-title";
  heading.textContent = typeof doc.title === "string" ? doc.title : "";
  root.appendChild(heading);
}

/**
 * renderEmptyState builds the shared "nothing to show" placeholder (RFC 028
 * V3, spec brief: "a doc with zero blocks, or a table/chart with no data").
 * `message` is platform-authored (never app-controlled text reaches this
 * function directly) but still goes through textContent like everything
 * else the shell renders. Exported so blocks/table.js and blocks/chart.js
 * can reuse the exact same placeholder rather than each inventing their own
 * (shell.js<->table.js and shell.js<->chart.js are already deferred-safe
 * cycles — see shell.js's module header and the V1/V2 reports — because
 * every cross-reference is used only inside a function body, never at
 * module top-level; this is one more such reference, not a new cycle risk).
 * @param {string} message
 * @returns {HTMLElement}
 */
export function renderEmptyState(message) {
  const el = document.createElement("div");
  el.className = "dtp-empty-state";
  const p = document.createElement("p");
  p.textContent = message;
  el.appendChild(p);
  return el;
}

/**
 * renderBlocks dispatches every top-level block through renderBlock, or — a
 * doc with zero blocks — shows the shared empty state instead of an empty
 * (and confusing) blank page.
 * @param {Element} root
 * @param {Record<string, unknown>} doc
 */
function renderBlocks(root, doc) {
  const blocks = Array.isArray(doc.blocks) ? doc.blocks : [];
  if (blocks.length === 0) {
    root.appendChild(renderEmptyState("This dashboard has no blocks to display."));
    return;
  }
  const container = document.createElement("div");
  container.className = "dtp-blocks";
  for (const block of blocks) {
    container.appendChild(renderBlock(block));
  }
  root.appendChild(container);
}

/**
 * applyDoc renders an OutputDoc into #dtp-root: it clears the mount and mounts
 * the title + blocks. This is the swap step of every render — the initial page
 * (boot) and every re-render (interact.js after a filter change, onClick,
 * navigation, or auto-refresh poll) call it with the doc to show. It performs
 * NO fetch and starts NO scheduler, so interact.js can call it freely.
 *
 * @param {Record<string, unknown>} doc
 */
export function applyDoc(doc) {
  const root = document.getElementById(ROOT_ID);
  if (!root) {
    throw new Error("dtp: missing #" + ROOT_ID + " mount point");
  }
  // Finalize every live Vega view inside the root BEFORE clearing it — a bare
  // textContent="" drops the DOM but leaks the view's listeners/timers/dataflow
  // (spec §6.4 boundary is unaffected; this is a resource-leak fix on re-render).
  finalizeVegaViewsWithin(root);
  root.textContent = ""; // clear the previous render / pre-render skeleton
  // The first paint (index.html) marks #dtp-root aria-busy="true" around its
  // static skeleton (RFC 028 V3) — clear it now that real content is mounted.
  // A no-op on every re-render after the first (the attribute is already gone).
  root.removeAttribute("aria-busy");
  renderTitle(root, doc);
  renderBlocks(root, doc);
}

/**
 * boot reads the embedded OutputDoc, mounts it, and hands off to interact.js
 * to start auto-refresh and restore any deep-linked modal. Re-renders do NOT
 * go through boot — they call applyDoc directly with a freshly fetched doc —
 * so the JSON island is read exactly once, at initial load.
 */
export function boot() {
  const doc = readEmbeddedDoc();
  applyDoc(doc);
  onBooted(doc);
}

// Hand interact.js the two mount primitives it needs (kept as an injection
// rather than an import, so interact.js has no cycle with this module), then
// mount the initial doc.
initInteract({ applyDoc, renderBlock });
boot();
