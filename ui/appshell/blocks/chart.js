// ui/appshell/blocks/chart.js — RFC 028 Part 4 (V1) chart block renderer.
//
// Security (spec §6.4): a chart's Vega-Lite `spec` is UNTRUSTED. Before it is
// handed to Vega it is validated CLIENT-SIDE against the vendored restricted
// subset schema (ui/appshell/vegaspec.schema.json — the same artifact W5
// validated with authoritatively on the server; this pass is defense-in-depth).
// Vega is embedded through shell.js's mountVegaLiteChart, the ONE sanctioned
// call site, which disables Vega's network loader and hides its action menu.
// The platform theme (buildThemeConfig) is applied at embed time — chart styling
// is PLATFORM-OWNED, never app-supplied (an author `config` key is rejected by
// the subset schema outright, spec §6.4).
//
// Lazy loading (CSP `script-src 'self'` + RFC 028 V1 maintainer decision): the
// vega / vega-lite / vega-embed trio is dynamic-import()ed on the FIRST chart,
// from the SAME-ORIGIN /apps/-/shell/vendor/ path only — never a CDN — in the
// required order (vega → vega-lite → vega-embed), and cached so later charts
// reuse the one load. The schema fetch is same-origin (CSP `connect-src 'self'`)
// and likewise cached.
//
// V3 additions: a schema-valid but empty inline dataset ({values: []}) shows
// the shared empty state instead of mounting a blank plot; any render
// failure (validation reject or a mount/load error) marks the mount with the
// shared dtp-error-card class for a distinct, intentional look.

import { mountVegaLiteChart } from "../shell.js";
import { renderEmptyState } from "../shell.js";
import { bindChartOnClick, registerVegaView } from "../interact.js";

// ui/product-derived categorical palette (spec §6.4 "the `ui/product` palette").
// Anchored on ui/product's accent (#3ecf8e) and status hues (#eab308 warning,
// #ef4444 fail, #6b7280 pending), extended with complementary hues for
// categorical breadth.
const CATEGORY_PALETTE = [
  "#3ecf8e", "#6366f1", "#eab308", "#ef4444", "#06b6d4",
  "#a855f7", "#f97316", "#14b8a6", "#ec4899", "#6b7280",
];

// buildThemeConfig is the platform Vega theme (THEME_CONFIG). It reads the
// shell's own CSS custom properties (theme.css) so the chart tracks the viewer's
// light/dark scheme, and applies the ui/product palette. PLATFORM-OWNED — never
// derived from the app's spec (spec §6.4: chart config comes from the platform,
// never the app).
function buildThemeConfig() {
  const css = getComputedStyle(document.documentElement);
  const val = (name, fallback) => {
    const v = css.getPropertyValue(name).trim();
    return v || fallback;
  };
  const fg = val("--dtp-fg", "#1a1c1f");
  const muted = val("--dtp-fg-muted", "#5c6066");
  const border = val("--dtp-border", "#d8dbe0");
  const font = val("--dtp-font", '-apple-system, "Segoe UI", Roboto, system-ui, sans-serif');
  return {
    background: "transparent",
    font,
    view: { stroke: "transparent" },
    title: { color: fg, font, fontSize: 14, fontWeight: 600, anchor: "start" },
    axis: {
      labelColor: muted, titleColor: muted, labelFont: font, titleFont: font,
      gridColor: border, domainColor: border, tickColor: border,
    },
    legend: { labelColor: muted, titleColor: muted, labelFont: font, titleFont: font },
    range: { category: CATEGORY_PALETTE },
  };
}

let vegaPromise = null;

// loadVega dynamic-imports the vega trio in the required order on first use,
// from the same-origin vendor path only. The imports attach the vega globals as
// a side effect; shell.js's mountVegaLiteChart is the ONE place that reads the
// vega-embed global — this renderer never touches it directly. Cached so a
// second chart never re-imports.
function loadVega() {
  if (vegaPromise) return vegaPromise;
  vegaPromise = (async () => {
    await import("/apps/-/shell/vendor/vega.min.js");
    await import("/apps/-/shell/vendor/vega-lite.min.js");
    await import("/apps/-/shell/vendor/vega-embed.min.js");
  })();
  return vegaPromise;
}

let schemaPromise = null;

// loadSubsetSchema fetches the vendored restricted-subset schema (same-origin;
// CSP `connect-src 'self'` permits it). Cached.
function loadSubsetSchema() {
  if (schemaPromise) return schemaPromise;
  schemaPromise = fetch("/apps/-/shell/vegaspec.schema.json").then((r) => {
    if (!r.ok) throw new Error("vegaspec schema fetch failed: " + r.status);
    return r.json();
  });
  return schemaPromise;
}

// renderChart returns the block container synchronously (with a loading state)
// and, once the vega trio + schema resolve, validates the spec and — only if it
// passes — embeds it through the sanctioned mountVegaLiteChart chokepoint.
export function renderChart(block) {
  const el = document.createElement("div");
  el.className = "dtp-block dtp-block-chart";

  if (block && typeof block.title === "string" && block.title.length > 0) {
    const title = document.createElement("div");
    title.className = "dtp-chart-title";
    title.textContent = block.title;
    el.appendChild(title);
  }

  const mount = document.createElement("div");
  mount.className = "dtp-chart-mount";
  mount.textContent = "Loading chart…";
  el.appendChild(mount);

  const spec = block ? block.spec : undefined;

  Promise.all([loadVega(), loadSubsetSchema()])
    .then(([, schema]) => {
      const err = validateAgainstSchema(spec, schema, schema);
      if (err) {
        // Defense-in-depth reject: the server already validated with the same
        // schema, so this only fires on a client/server validator gap — fail
        // closed, never embed an unvetted spec into Vega.
        showChartError(mount);
        return undefined;
      }
      if (isEmptyChartData(spec)) {
        // A schema-valid spec with an inline dataset present but empty
        // ({values: []}) — mounting Vega would just be a blank plot with no
        // explanation (spec brief: "a … chart with no data").
        mount.textContent = "";
        mount.appendChild(renderEmptyState("No data to display."));
        return undefined;
      }
      mount.textContent = "";
      return mountVegaLiteChart(mount, spec, buildThemeConfig()).then((result) => {
        // Cross-filter binding (spec §6.3 onClick: {param}). Only when the
        // block declares it; the shell sets the param + re-renders — config,
        // not code. The vega view is reached via vega-embed's resolved result;
        // the binding itself lives in interact.js (the one owner of param state).
        let listener;
        const param = block && block.onClick ? block.onClick.param : undefined;
        if (typeof param === "string" && result && result.view) {
          listener = bindChartOnClick(result.view, param);
        }
        // Register the mounted view so interact.js finalizes it (and removes the
        // click listener) before the next re-render/auto-refresh swaps the DOM —
        // otherwise each swap leaks the view (fix). `mount` scopes finalization.
        registerVegaView({ result, listener, mount });
        return result;
      });
    })
    .catch(() => {
      showChartError(mount);
    });

  return el;
}

// isEmptyChartData reports whether a schema-valid spec's inline dataset is a
// present-but-empty array. A validator pass only means the spec is
// STRUCTURALLY sound — it says nothing about whether there is anything to
// plot.
function isEmptyChartData(spec) {
  return !!(spec && spec.data && Array.isArray(spec.data.values) && spec.data.values.length === 0);
}

// showChartError marks the mount as a failed-render error card (spec brief:
// "reuse V1/V2's error states, make them look intentional") and sets its
// inert fallback text. Never assigns innerHTML.
function showChartError(mount) {
  mount.textContent = "This chart could not be displayed.";
  mount.classList.add("dtp-error-card");
}

// ---------------------------------------------------------------------------
// Client-side restricted-subset validator (spec §6.4 defense-in-depth).
//
// A compact evaluator for exactly the JSON-Schema keyword subset
// vegaspec.schema.json uses: $ref (to #/$defs/*), type, enum, const, pattern,
// minimum, required, properties + additionalProperties:false, items, oneOf. It
// is deliberately NOT a general JSON-Schema engine — it faithfully interprets
// THIS vendored schema, so there is nothing to hand-copy and drift from the
// normative artifact. Returns an error string on the first violation, or ""
// when the value conforms. Exported for isolated review/verification.
// ---------------------------------------------------------------------------

export function validateAgainstSchema(value, schema, root) {
  if (schema == null || typeof schema !== "object") return "";

  if (typeof schema.$ref === "string") {
    const resolved = resolveRef(schema.$ref, root);
    if (!resolved) return "unresolved $ref " + schema.$ref;
    const e = validateAgainstSchema(value, resolved, root);
    if (e) return e;
  }

  if (Array.isArray(schema.oneOf)) {
    let matches = 0;
    for (const sub of schema.oneOf) {
      if (!validateAgainstSchema(value, sub, root)) matches++;
    }
    if (matches !== 1) {
      return "value matches " + matches + " allowed forms (want exactly 1)";
    }
  }

  if (schema.type !== undefined && !matchesType(value, schema.type)) {
    return "expected type " + JSON.stringify(schema.type);
  }

  if (schema.enum !== undefined && !schema.enum.some((e) => e === value)) {
    return "value not in enum";
  }

  if (schema.const !== undefined && value !== schema.const) {
    return "value !== const " + JSON.stringify(schema.const);
  }

  if (typeof schema.pattern === "string" && typeof value === "string") {
    if (!new RegExp(schema.pattern).test(value)) return "value does not match pattern";
  }

  if (typeof schema.minimum === "number" && typeof value === "number") {
    if (value < schema.minimum) return "value below minimum";
  }

  if (jsonType(value) === "object") {
    if (Array.isArray(schema.required)) {
      for (const key of schema.required) {
        if (!(key in value)) return "missing required key " + key;
      }
    }
    if (schema.properties || schema.additionalProperties === false) {
      const props = schema.properties || {};
      for (const key of Object.keys(value)) {
        if (Object.prototype.hasOwnProperty.call(props, key)) {
          const e = validateAgainstSchema(value[key], props[key], root);
          if (e) return "at ." + key + ": " + e;
        } else if (schema.additionalProperties === false) {
          return "unexpected key " + key;
        }
      }
    }
  }

  if (jsonType(value) === "array" && schema.items) {
    for (let i = 0; i < value.length; i++) {
      const e = validateAgainstSchema(value[i], schema.items, root);
      if (e) return "at [" + i + "]: " + e;
    }
  }

  return "";
}

// resolveRef resolves a local "#/…" JSON pointer against the root schema.
function resolveRef(ref, root) {
  if (!ref.startsWith("#/")) return null;
  let cur = root;
  for (const part of ref.slice(2).split("/")) {
    if (cur == null) return null;
    cur = cur[part];
  }
  return cur;
}

// jsonType maps a parsed-JSON value onto a JSON Schema type name.
function jsonType(v) {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  return typeof v; // "object" | "number" | "string" | "boolean"
}

function matchesType(value, type) {
  const t = jsonType(value);
  const ok = (one) => t === one;
  return Array.isArray(type) ? type.some(ok) : ok(type);
}
