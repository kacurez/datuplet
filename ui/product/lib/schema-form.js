// schema-form.js — JSON Schema (draft 2020-12) → HTML form → plain JS object.
//
// Pure module: build into a container, read back with getValue(). No network
// (secret keys are injected via opts), no routing. Only top-level properties
// get real controls; nested objects fall back to a JSON subeditor textarea
// (RFC 026 §4.7 item 3 — two-way object editing is explicitly out of scope).
//
// Every schema-derived string is esc()'d before it reaches innerHTML.

import { esc } from '/ui/api.js';

/**
 * @param {HTMLElement} container
 * @param {string} schemaStr   JSON Schema as a string (component configSchema)
 * @param {object} initialValue  existing config object (may be {})
 * @param {{secretKeys?: string[], listSecretsFn?: () => Promise<Array<{key:string}>>}} [opts]
 * @returns {{getValue: () => object, getErrors: () => Array<{path:string,message:string}>, destroy: () => void}}
 */
export function buildSchemaForm(container, schemaStr, initialValue = {}, opts = {}) {
  let schema;
  try {
    schema = JSON.parse(schemaStr || '{}');
  } catch {
    container.innerHTML = `<div class="callout callout--warn">Component config schema is not valid JSON — use the “Edit as YAML” view.</div>`;
    return degenerate(initialValue);
  }
  const props = schema && typeof schema.properties === 'object' ? schema.properties : null;
  if (!props || Object.keys(props).length === 0) {
    // No declared properties → nothing to render as a form. Signal caller to
    // fall back to raw editing.
    container.innerHTML = `<div class="callout">This component has no structured schema; edit its config as YAML.</div>`;
    return degenerate(initialValue);
  }
  const required = new Set(Array.isArray(schema.required) ? schema.required : []);
  const secretKeys = Array.isArray(opts.secretKeys) ? opts.secretKeys : [];

  // fieldReaders: key → () => { present:boolean, value:any, error?:string }
  // Null-prototype so a schema property literally named "__proto__" (or
  // "constructor"/"prototype") is a normal own key: it iterates under
  // Object.keys and cannot poison Object.prototype via assignment.
  const fieldReaders = Object.create(null);
  const parts = Object.keys(props).map((key) => {
    const node = props[key] || {};
    const isReq = required.has(key);
    const init = initialValue == null ? undefined : initialValue[key];
    const built = buildField(key, node, isReq, init, secretKeys);
    fieldReaders[key] = built.read;
    return built.html;
  });

  container.innerHTML = `<div class="sform">${parts.join('')}</div>`;
  // Post-render wiring (e.g. array textarea autosize) can attach here via
  // container.querySelector using stable data-attributes set in buildField.

  function collect() {
    // Null-prototype: `out["__proto__"] = value` sets an own key instead of
    // mutating the prototype chain.
    const out = Object.create(null);
    const errors = [];
    for (const key of Object.keys(fieldReaders)) {
      let r;
      try {
        r = fieldReaders[key](container);
      } catch {
        // Never throw from getValue/getErrors — a broken reader is skipped.
        continue;
      }
      if (r.error) errors.push({ path: key, message: r.error });
      if (r.present) out[key] = r.value;
    }
    return { out, errors };
  }
  return {
    getValue: () => collect().out,
    getErrors: () => collect().errors,
    destroy: () => { container.innerHTML = ''; },
  };
}

function degenerate(initialValue) {
  // A handle that just echoes the initial value (used when there's no usable
  // schema). getValue returns a shallow copy so callers can't mutate state.
  return {
    getValue: () => (initialValue && typeof initialValue === 'object' ? { ...initialValue } : {}),
    getErrors: () => [],
    destroy: () => {},
  };
}

function buildField(key, node, isReq, init, secretKeys) {
  const label = fieldLabel(key, node, isReq);
  const id = `sf-${key}`;
  const dk = esc(key);

  // --- secret picker (highest precedence: x-datuplet-secret) ---
  if (node['x-datuplet-secret'] === true) {
    return secretField(key, isReq, init, secretKeys, label, id, dk);
  }
  // --- enum → select ---
  if (Array.isArray(node.enum)) {
    return enumField(key, node, isReq, init, label, id, dk);
  }
  // --- boolean → checkbox ---
  if (node.type === 'boolean') {
    return booleanField(key, isReq, init, label, id, dk);
  }
  // --- number / integer ---
  if (node.type === 'number' || node.type === 'integer') {
    return numberField(key, node, isReq, init, label, id, dk);
  }
  // --- array of scalars ---
  if (node.type === 'array' && node.items && isScalarType(node.items.type)) {
    return scalarArrayField(key, node, isReq, init, label, id, dk);
  }
  // --- object / unknown-nested → JSON subeditor (explicit fallback) ---
  if (node.type === 'object' || node.type === undefined) {
    return objectSubeditor(key, isReq, init, label, id, dk);
  }
  // --- default: string text input ---
  return stringField(key, node, isReq, init, label, id, dk);
}

function isScalarType(t) { return t === 'string' || t === 'number' || t === 'integer'; }

function fieldLabel(key, node, isReq) {
  const req = isReq ? ' <span class="sform-req" title="required">*</span>' : '';
  const desc = node.description ? `<span class="sform-desc">${esc(node.description)}</span>` : '';
  return `<span class="sform-label"><code class="mono">${esc(key)}</code>${req}</span>${desc}`;
}

// Representative branch: plain string.
function stringField(key, node, isReq, init, label, id, dk) {
  const val = init == null ? '' : String(init);
  const multiline = key === 'sql' || node['x-datuplet-multiline'] === true;
  const control = multiline
    ? `<textarea class="input textarea input--mono" id="${esc(id)}" data-sf-key="${dk}" ${isReq ? 'required' : ''}>${esc(val)}</textarea>`
    : `<input class="input" type="text" id="${esc(id)}" data-sf-key="${dk}" value="${esc(val)}" ${isReq ? 'required' : ''}>`;
  return {
    html: `<label class="field sform-field">${label}${control}</label>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      const v = el ? el.value.trim() : '';
      if (v === '') return { present: false, value: undefined, error: isReq ? 'required' : undefined };
      return { present: true, value: v };
    },
  };
}

// enum → single select. First option is a "— choose —" placeholder unless the
// field is required (then the first enum value is the effective default).
function enumField(key, node, isReq, init, label, id, dk) {
  const initStr = init == null ? null : String(init);
  const placeholder = isReq ? '' : `<option value="">— choose —</option>`;
  const options = node.enum.map((e, i) => {
    const ev = String(e);
    const sel = initStr !== null && initStr === ev ? ' selected' : '';
    // Key the option by its index, not String(e): otherwise enum:[1,"1"] can't
    // round-trip (both stringify to "1"). The visible label is still escaped.
    return `<option value="${i}"${sel}>${esc(ev)}</option>`;
  }).join('');
  const control = `<select class="input" id="${esc(id)}" data-sf-key="${dk}" ${isReq ? 'required' : ''}>${placeholder}${options}</select>`;
  return {
    html: `<label class="field sform-field">${label}${control}</label>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      const v = el ? el.value : '';
      if (v === '') return { present: false, value: undefined, error: isReq ? 'required' : undefined };
      // v is the enum index; return the ORIGINAL typed entry so integers,
      // strings, and objects all round-trip. Guard against a bad index.
      const idx = Number(v);
      if (!Number.isInteger(idx) || idx < 0 || idx >= node.enum.length) {
        return { present: false, value: undefined, error: isReq ? 'required' : undefined };
      }
      return { present: true, value: node.enum[idx] };
    },
  };
}

// boolean → checkbox. A checkbox has a definite state, so it is always present.
function booleanField(key, isReq, init, label, id, dk) {
  const checked = init === true ? ' checked' : '';
  const control = `<input type="checkbox" id="${esc(id)}" data-sf-key="${dk}"${checked}>`;
  return {
    html: `<label class="field sform-field sform-field--check">${control}${label}</label>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      return { present: true, value: el ? el.checked : false };
    },
  };
}

// number / integer → <input type=number>. Integers add step="1" + numeric hint.
function numberField(key, node, isReq, init, label, id, dk) {
  const isInt = node.type === 'integer';
  const val = init == null ? '' : String(init);
  const extra = isInt ? ' step="1" inputmode="numeric"' : '';
  const control = `<input class="input" type="number" id="${esc(id)}" data-sf-key="${dk}" value="${esc(val)}"${extra} ${isReq ? 'required' : ''}>`;
  return {
    html: `<label class="field sform-field">${label}${control}</label>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      const v = el ? el.value.trim() : '';
      if (v === '') return { present: false, value: undefined, error: isReq ? 'required' : undefined };
      if (isInt) {
        // parseInt would silently truncate "1.9"→1, "1e2"→1, "12abc"→12;
        // require a full integer match instead.
        if (!/^[+-]?\d+$/.test(v)) return { present: false, value: undefined, error: 'must be an integer' };
        return { present: true, value: Number(v) };
      }
      const n = Number(v);
      if (Number.isNaN(n)) return { present: false, value: undefined, error: 'must be a number' };
      return { present: true, value: n };
    },
  };
}

// array of scalars → one-value-per-line textarea (no add/remove button state).
function scalarArrayField(key, node, isReq, init, label, id, dk) {
  const itemType = node.items && node.items.type ? node.items.type : 'string';
  const val = Array.isArray(init) ? init.map((x) => (x == null ? '' : String(x))).join('\n') : '';
  const control = `<textarea class="input textarea input--mono" id="${esc(id)}" data-sf-key="${dk}" spellcheck="false">${esc(val)}</textarea>`;
  return {
    html: `<label class="field sform-field">${label}<span class="sform-hint">one per line</span>${control}</label>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      const raw = el ? el.value : '';
      const lines = raw.split('\n').map((s) => s.trim()).filter((s) => s !== '');
      if (lines.length === 0) return { present: false, value: undefined, error: isReq ? 'required' : undefined };
      if (itemType === 'number' || itemType === 'integer') {
        const out = [];
        for (let i = 0; i < lines.length; i++) {
          let n;
          if (itemType === 'integer') {
            // parseInt would silently accept "12abc"→12; require a full match.
            if (!/^[+-]?\d+$/.test(lines[i])) return { present: false, value: undefined, error: `line ${i + 1} is not a number` };
            n = Number(lines[i]);
          } else {
            n = Number(lines[i]);
            if (Number.isNaN(n)) return { present: false, value: undefined, error: `line ${i + 1} is not a number` };
          }
          out.push(n);
        }
        return { present: true, value: out };
      }
      return { present: true, value: lines };
    },
  };
}

// secret picker → <select> of $[<key>] refs. The control structurally prevents
// plaintext (§4.9); read() still guards the stale-init path defensively.
function secretField(key, isReq, init, secretKeys, label, id, dk) {
  const initRef = typeof init === 'string' ? init : '';
  const known = new Set(secretKeys.map((k) => `$[${k}]`));
  const options = [`<option value="">— none —</option>`];
  for (const k of secretKeys) {
    const ref = `$[${k}]`;
    const sel = initRef === ref ? ' selected' : '';
    options.push(`<option value="${esc(ref)}"${sel}>${esc(ref)}</option>`);
  }
  // Survive a stale list: keep an existing $[...] ref that's no longer offered.
  if (initRef && /^\$\[[^\]]+\]$/.test(initRef) && !known.has(initRef)) {
    options.push(`<option value="${esc(initRef)}" selected>${esc(initRef)}</option>`);
  }
  const control = `<select class="input" id="${esc(id)}" data-sf-key="${dk}" ${isReq ? 'required' : ''}>${options.join('')}</select>`;
  return {
    html: `<label class="field sform-field">${label}${control} <a href="/ui/settings/secrets" class="sform-manage">manage secrets…</a></label>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      const v = el ? el.value : '';
      if (v === '') return { present: false, value: undefined, error: isReq ? 'a secret reference is required' : undefined };
      if (!/^\$\[[^\]]+\]$/.test(v)) return { present: false, value: undefined, error: 'must be a $[secret] reference' };
      return { present: true, value: v };
    },
  };
}

// Representative branch: object → collapsible JSON subeditor.
function objectSubeditor(key, isReq, init, label, id, dk) {
  const pretty = init == null ? '' : safePretty(init);
  return {
    html: `
      <div class="field sform-field">
        ${label}
        <details class="sform-json" ${pretty ? 'open' : ''}>
          <summary>edit as JSON</summary>
          <textarea class="input textarea input--mono" id="${esc(id)}" data-sf-key="${dk}" spellcheck="false" placeholder="{ }">${esc(pretty)}</textarea>
        </details>
      </div>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      const raw = el ? el.value.trim() : '';
      if (raw === '') return { present: false, value: undefined, error: isReq ? 'required' : undefined };
      try {
        return { present: true, value: JSON.parse(raw) };
      } catch (e) {
        return { present: false, value: undefined, error: `not valid JSON (${e.message})` };
      }
    },
  };
}

function safePretty(v) { try { return JSON.stringify(v, null, 2); } catch { return ''; } }

// cssEsc: escape a property key for use inside a [data-sf-key="..."] selector.
// Property names are schema-controlled but may contain characters that break
// an attribute selector; prefer CSS.escape when available.
function cssEsc(s) {
  if (window.CSS && typeof window.CSS.escape === 'function') return window.CSS.escape(s);
  return String(s).replace(/["\\\]]/g, '\\$&');
}
