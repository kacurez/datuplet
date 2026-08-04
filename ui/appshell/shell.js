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
// V0 scope: read the doc, render the title, and dispatch each top-level
// block to a renderer looked up in RENDERERS by its `type` (spec §6.3:
// markdown | metric | table | chart | filter | tabs). No renderer is
// registered yet — V1 adds them one type at a time — so every block renders
// through renderUnknownBlock for now. Interactivity (filters, tabs, modals,
// auto-refresh, CSV export) is V1+; this module has no re-render / fetch
// logic.

import { renderMarkdown } from "./blocks/markdown.js";
import { renderMetric } from "./blocks/metric.js";
import { renderTable } from "./blocks/table.js";
import { renderChart } from "./blocks/chart.js";

const ROOT_ID = "dtp-root";
const DOC_SCRIPT_ID = "dtp-doc";

/**
 * RENDERERS maps an OutputDoc block `type` to a function `(block) => Node`.
 * V1 registers the four data-block renderers here; each lives in its own
 * ui/appshell/blocks/ module. The remaining vocabulary (filter, tabs) is
 * later work — a block whose type is absent from RENDERERS falls back to
 * renderUnknownBlock, so a doc the shell doesn't fully understand yet still
 * renders every OTHER block instead of failing the whole page.
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
};

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
 * renderBlocks dispatches every top-level block through RENDERERS,
 * falling back to renderUnknownBlock for any type not yet registered.
 * @param {Element} root
 * @param {Record<string, unknown>} doc
 */
function renderBlocks(root, doc) {
  const blocks = Array.isArray(doc.blocks) ? doc.blocks : [];
  const container = document.createElement("div");
  container.className = "dtp-blocks";
  for (const block of blocks) {
    const type = block && typeof block === "object" ? block.type : undefined;
    const renderer = (typeof type === "string" && RENDERERS[type]) || renderUnknownBlock;
    container.appendChild(renderer(block));
  }
  root.appendChild(container);
}

/**
 * boot reads the embedded OutputDoc and mounts it into #dtp-root. Exported
 * so V1+ can re-invoke it (e.g. after a partial re-render swaps the JSON
 * island's content) without re-running this module's top-level side effect.
 */
export function boot() {
  const root = document.getElementById(ROOT_ID);
  if (!root) {
    throw new Error("dtp: missing #" + ROOT_ID + " mount point");
  }
  root.textContent = ""; // clear the pre-render skeleton/loading state
  const doc = readEmbeddedDoc();
  renderTitle(root, doc);
  renderBlocks(root, doc);
}

boot();
