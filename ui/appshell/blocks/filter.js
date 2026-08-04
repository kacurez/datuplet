// ui/appshell/blocks/filter.js — RFC 028 Part 4 (V2) filter block renderer.
//
// Security (spec §6.4): a filter field's label, option labels/values, and the
// current value are UNTRUSTED text — every one reaches the DOM via textContent
// or an element's `.value` property (never innerHTML). Option `<option>` text
// is textContent; the control value is a string property assignment.
//
// Interactivity (spec §6.3 "Filters & deep links: filter changes set URL params
// and re-render; params arrive in ctx.params … any filter state is a shareable
// link"): each control reads its INITIAL value from the URL params (falling
// back to the field's server-supplied `value`), and on change writes that param
// back to the URL and triggers a full re-render — both via interact.js, the one
// place that owns param state and the re-render fetch. The re-render body is
// string->string (§6.5), which is why control values are always sent as strings.
//
// Field shape (W1 filterBlock schema): {name, label, kind, value?, options?}.
// `kind` is a free string in the schema (only "select" appears in Appendix A);
// this renderer maps the common kinds and defaults unknown kinds to a text
// input, so a new kind degrades to an editable field rather than breaking.

import { getParams, setParam } from "../interact.js";

// renderFilter renders the block's fields[] as a labeled row of controls.
export function renderFilter(block) {
  const el = document.createElement("div");
  el.className = "dtp-block dtp-block-filter";

  const row = document.createElement("div");
  row.className = "dtp-filter-row";

  const fields = block && Array.isArray(block.fields) ? block.fields : [];
  const params = getParams();

  for (const field of fields) {
    if (!field || typeof field !== "object" || typeof field.name !== "string") continue;

    const wrap = document.createElement("label");
    wrap.className = "dtp-filter-field";

    const labelText = document.createElement("span");
    labelText.className = "dtp-filter-label";
    labelText.textContent = typeof field.label === "string" ? field.label : field.name;
    wrap.appendChild(labelText);

    // Initial value: URL param wins (so the control matches a shared deep link
    // even if the app did not echo it back), else the field's own `value`.
    const initial = Object.prototype.hasOwnProperty.call(params, field.name)
      ? params[field.name]
      : field.value === null || field.value === undefined
      ? ""
      : String(field.value);

    wrap.appendChild(buildControl(field, initial));
    row.appendChild(wrap);
  }

  el.appendChild(row);
  return el;
}

// buildControl returns the input element for one field, pre-set to `initial`
// and wired so a change writes the param (as a string) and re-renders.
function buildControl(field, initial) {
  const kind = typeof field.kind === "string" ? field.kind : "text";
  const name = field.name;

  if (kind === "select") {
    const select = document.createElement("select");
    select.className = "dtp-filter-control";
    const options = Array.isArray(field.options) ? field.options : [];
    for (const opt of options) {
      const norm = normalizeOption(opt);
      const option = document.createElement("option");
      option.value = norm.value; // string property, not markup
      option.textContent = norm.label; // untrusted text via textContent
      if (norm.value === initial) option.selected = true;
      select.appendChild(option);
    }
    select.addEventListener("change", () => setParam(name, select.value));
    return select;
  }

  if (kind === "checkbox" || kind === "boolean") {
    const input = document.createElement("input");
    input.type = "checkbox";
    input.className = "dtp-filter-control dtp-filter-checkbox";
    input.checked = initial === "true";
    // Value is still a STRING param ("true"/"false") — §6.5 forbids coercion,
    // the app parses its own booleans.
    input.addEventListener("change", () => setParam(name, input.checked ? "true" : "false"));
    return input;
  }

  const input = document.createElement("input");
  input.className = "dtp-filter-control";
  input.type = kind === "number" ? "number" : kind === "date" ? "date" : "text";
  input.value = initial;
  // `change` (not `input`) so we re-render on commit, not on every keystroke.
  input.addEventListener("change", () => setParam(name, input.value));
  return input;
}

// normalizeOption accepts BOTH W1 filterOption forms — a scalar (string /
// number / boolean) or an object {value, label} — and returns string
// {value, label} suitable for an <option>. The value is stringified because
// URL params (and therefore ctx.params) are strings.
function normalizeOption(opt) {
  if (opt && typeof opt === "object") {
    return {
      value: opt.value === null || opt.value === undefined ? "" : String(opt.value),
      label: typeof opt.label === "string" ? opt.label : String(opt.value),
    };
  }
  const s = opt === null || opt === undefined ? "" : String(opt);
  return { value: s, label: s };
}
