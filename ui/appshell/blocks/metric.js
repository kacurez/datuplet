// ui/appshell/blocks/metric.js — RFC 028 Part 4 (V1) metric block renderer.
//
// Security (spec §6.4): metric labels and values are UNTRUSTED text fields —
// every string reaches the DOM via textContent, NEVER innerHTML. Numeric values
// are formatted with Intl.NumberFormat per the item's `format` field
// ("currency:EUR" | "number" | absent); an unparseable value or unknown format
// degrades to the raw value as plain text (a bad format is never a broken tile).

// renderMetric renders the block's items[] as a row of tiles.
export function renderMetric(block) {
  const el = document.createElement("div");
  el.className = "dtp-block dtp-block-metric";

  const grid = document.createElement("div");
  grid.className = "dtp-metric-grid";

  const items = block && Array.isArray(block.items) ? block.items : [];
  for (const item of items) {
    const tile = document.createElement("div");
    tile.className = "dtp-metric-tile";

    const value = document.createElement("div");
    value.className = "dtp-metric-value";
    value.textContent = formatMetricValue(item ? item.value : undefined, item ? item.format : undefined);

    const label = document.createElement("div");
    label.className = "dtp-metric-label";
    label.textContent = item && typeof item.label === "string" ? item.label : "";

    tile.appendChild(value);
    tile.appendChild(label);
    grid.appendChild(tile);
  }

  el.appendChild(grid);
  return el;
}

// formatMetricValue applies the spec §6.3 metric format vocabulary via
// Intl.NumberFormat. It NEVER throws: a non-numeric value or an invalid currency
// code falls through to the raw value as a plain string. The result is always
// assigned via textContent by the caller.
export function formatMetricValue(value, format) {
  if (typeof format === "string" && format.length > 0) {
    const sep = format.indexOf(":");
    const kind = sep === -1 ? format : format.slice(0, sep);
    const arg = sep === -1 ? "" : format.slice(sep + 1);
    const n = typeof value === "number" ? value : Number(value);
    if (Number.isFinite(n)) {
      try {
        if (kind === "currency" && arg) {
          return new Intl.NumberFormat(undefined, { style: "currency", currency: arg }).format(n);
        }
        if (kind === "number") {
          return new Intl.NumberFormat().format(n);
        }
      } catch (_e) {
        // Invalid currency code etc. → fall through to the raw value.
      }
    }
  }
  // No format, unknown format, or non-numeric value → raw value as text.
  if (value === null || value === undefined) return "";
  return String(value);
}
