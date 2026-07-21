// schema-form.js — JSON Schema (draft 2020-12) → HTML form → plain JS object.
//
// Pure module: build into a container, read back with getValue(). No network
// (secret keys are injected via opts), no routing.
//
// v2 (RFC 027 U1): recursive renderer for the full Form Subset (spec §4.2) at
// ANY nesting depth — nested objects (collapsible groups), array-of-objects
// (repeater cards), scalar arrays (one-per-line), array-of-arrays (nested list
// editor), additionalProperties maps (key→value rows), and multi-type `type`
// arrays (shape picker). No JSON-textarea fallback for built-ins.
//
// v2.1 (RFC 027 U2): display-metadata decorations layered onto the U1
// structure — `default`/`examples[0]` placeholders, tri-state boolean
// <select> when `default` is present (unset = absent from getValue()),
// x-datuplet-doc hover icons, x-datuplet-advanced grouped into one trailing
// collapsed <details> per object, and a per-node JSON sub-editor fallback for
// out-of-subset constructs (oneOf/anyOf/allOf/not/$ref/$defs/if/then/else/
// patternProperties/const) — see renderJsonFallback / outOfSubsetKey.
//
// Hardening (unchanged from v1, now enforced at every depth):
//   * esc() every schema-derived string before it reaches innerHTML.
//   * Object.create(null) accumulators wherever a schema-controlled key (which
//     could literally be "__proto__" / "constructor" / "prototype") builds a
//     plain object — a real anti-prototype-pollution measure, not decoration.
//   * cssEsc() when a property path is placed inside a [data-sf-path="…"] CSS
//     attribute selector.
//   * Stable data-sf-path attributes for post-render DOM wiring at any depth.
//
// Architecture: renderNode(node, path, required, init, ctx) → {el, read}. Each
// composite (group/list/map/multi-type) owns its child readers and composes
// their results; getValue()/getErrors() call the root read() once, which
// recurses. Reads capture their own DOM subtree (robust to repeater add/remove,
// which mutate the live DOM), so the output stays sparse: empty / unset fields
// are absent from the result, never defaulted.

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

  const ctx = {
    secretKeys: Array.isArray(opts.secretKeys) ? opts.secretKeys : [],
    listSecretsFn: typeof opts.listSecretsFn === 'function' ? opts.listSecretsFn : null,
  };

  // The schema root is itself an object node; render it flat (path === []).
  const root = renderNode(schema, [], true, initialValue, ctx);
  container.innerHTML = '';
  const wrap = document.createElement('div');
  wrap.className = 'sform';
  wrap.appendChild(root.el);
  container.appendChild(wrap);

  function collect() {
    let r;
    try {
      r = root.read();
    } catch {
      // Never throw from getValue/getErrors — a broken reader yields empty.
      return { out: Object.create(null), errors: [] };
    }
    // Root always yields an object (possibly empty) so getValue() is never null.
    const out = r && r.present ? r.value : Object.create(null);
    const errors = (r && Array.isArray(r.errors)) ? r.errors : [];
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

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

// renderNode: the recursive core. Returns { el, read } where read() →
// { present:boolean, value:any, errors:Array<{path,message}> }.
function renderNode(node, path, required, init, ctx) {
  node = node || {};

  // 0. Out-of-subset constructs (oneOf/anyOf/allOf/not/$ref/$defs/if/then/else/
  //    patternProperties/const) render as a JSON sub-editor for JUST this node,
  //    never a whole-form fallback. Built-ins lint-clean and never hit this path
  //    (RFC 027 R5); it exists for third-party / operator-registered schemas.
  const oosKey = outOfSubsetKey(node);
  if (oosKey) return renderJsonFallback(node, path, required, init, oosKey);

  // 1. Secret picker has highest precedence (x-datuplet-secret).
  if (node['x-datuplet-secret'] === true) return renderSecret(node, path, required, init, ctx);

  // 2. enum → select (regardless of declared type).
  if (Array.isArray(node.enum)) return renderScalar(node, path, required, init);

  // 3. Multi-type `type` arrays (spec: in-subset). `null` is not a control;
  //    drop it. If more than one real type remains, render a shape picker.
  if (Array.isArray(node.type)) {
    const types = node.type.filter((t) => t !== 'null');
    if (types.length > 1) return renderMultiType(node, path, required, init, ctx, types);
    node = Object.assign(Object.create(null), node, { type: types[0] || 'string' });
  }

  const t = node.type;

  if (t === 'boolean' || t === 'number' || t === 'integer') return renderScalar(node, path, required, init);

  if (t === 'object' || (t === undefined && node.properties)) {
    if (isMap(node)) return renderMap(node, path, required, init, ctx);
    return renderGroup(node, path, required, init, ctx);
  }

  if (t === 'array') {
    const items = node.items || {};
    // array-of-objects and array-of-arrays both compose recursively: each
    // element is rendered by renderNode(items). Scalar arrays use the compact
    // one-per-line editor.
    if (items.type === 'object' || items.type === 'array' || Array.isArray(items.type)) {
      return renderList(node, path, required, init, ctx);
    }
    return renderScalarArray(node, path, required, init);
  }

  // string / unknown → text input (or multiline textarea).
  return renderScalar(node, path, required, init);
}

// isMap: an object acting as a key→value map — additionalProperties is a value
// schema and there are no fixed properties. `additionalProperties: false`
// (typeof 'boolean') is correctly excluded.
function isMap(node) {
  return node.type === 'object'
    && node.additionalProperties
    && typeof node.additionalProperties === 'object'
    && (!node.properties || Object.keys(node.properties).length === 0);
}

function isScalarType(t) { return t === 'string' || t === 'number' || t === 'integer'; }

// ---------------------------------------------------------------------------
// Labels & paths
// ---------------------------------------------------------------------------

// fmtPath: render a path array as a stable, human-readable string used for both
// data-sf-path attributes and error `path` fields (e.g. tables[0].name).
function fmtPath(path) {
  let s = '';
  for (const p of path) {
    if (typeof p === 'number') s += `[${p}]`;
    else s += s ? `.${p}` : String(p);
  }
  return s;
}

// labelHTML: the property key is the label (mono). `title`, when present, is the
// human label with the key as a mono suffix. `description` is the always-visible
// one-line hint. Every schema string is esc()'d before entering innerHTML.
function labelHTML(key, node, required) {
  const title = typeof node.title === 'string' && node.title ? node.title : '';
  const req = required ? ' <span class="sform-req" title="required">*</span>' : '';
  const doc = typeof node['x-datuplet-doc'] === 'string' && node['x-datuplet-doc']
    ? ` <span class="doci" title="${esc(node['x-datuplet-doc'])}">ⓘ</span>` : '';
  const desc = node.description ? `<span class="sform-desc">${esc(node.description)}</span>` : '';
  const name = title
    ? `${esc(title)} <code class="mono">${esc(key)}</code>`
    : `<code class="mono">${esc(key)}</code>`;
  return `<span class="sform-label">${name}${req}${doc}</span>${desc}`;
}

function keyOf(path) { return path.length ? path[path.length - 1] : ''; }

// rebasePath: rewrite the leading `oldBase` segment of an error path string to
// `newBase`, matching only at a segment boundary (end / '.' / '['), so
// `tables[2]` rebases `tables[2].name` but never `tables[20]`. Used to keep
// error paths accurate when a repeater row's live index diverges from the index
// it was created with (after a middle removal), and to map a map-value's `*`
// placeholder segment onto the row's actual key.
function rebasePath(p, oldBase, newBase) {
  if (typeof p !== 'string' || oldBase === newBase) return p;
  if (p === oldBase) return newBase;
  if (p.startsWith(oldBase + '.') || p.startsWith(oldBase + '[')) return newBase + p.slice(oldBase.length);
  return p;
}

// ---------------------------------------------------------------------------
// Composite: object group
// ---------------------------------------------------------------------------

function renderGroup(node, path, required, init, ctx) {
  const key = keyOf(path);
  const iv = (init && typeof init === 'object' && !Array.isArray(init)) ? init : null;
  const entries = Object.entries(node.properties || {});
  const reqSet = new Set(Array.isArray(node.required) ? node.required : []);

  const children = entries.map(([k, child]) => {
    const cr = renderNode(child || {}, path.concat(k), reqSet.has(k), iv ? iv[k] : undefined, ctx);
    return { k, cr, advanced: !!(child && child['x-datuplet-advanced']) };
  });

  let el;
  let body;
  if (path.length === 0) {
    // Root: render flat, no <details> wrapper.
    el = document.createElement('div');
    el.className = 'sform-group sform-group--root';
    body = el;
  } else {
    // Nested: collapsible group.
    const d = document.createElement('details');
    d.className = 'sform-group';
    d.open = true;
    const summary = document.createElement('summary');
    summary.innerHTML = labelHTML(key, node, required);
    d.appendChild(summary);
    body = document.createElement('div');
    body.className = 'sform-group-body';
    d.appendChild(body);
    el = d;
  }
  // x-datuplet-advanced: all such properties of THIS object render together,
  // in one collapsed <details> at the end of the group — not one per property.
  // Partition preserves each side's original property order.
  const normal = children.filter((c) => !c.advanced);
  const advanced = children.filter((c) => c.advanced);
  for (const c of normal) body.appendChild(c.cr.el);
  if (advanced.length) {
    const det = document.createElement('details');
    det.className = 'sform-group sform-advanced';
    const summary = document.createElement('summary');
    summary.textContent = 'Advanced';
    det.appendChild(summary);
    const advBody = document.createElement('div');
    advBody.className = 'sform-group-body';
    for (const c of advanced) advBody.appendChild(c.cr.el);
    det.appendChild(advBody);
    body.appendChild(det);
  }

  const declared = node.properties || {};
  const read = () => {
    // Null-prototype: out["__proto__"] = v sets an own key, never mutates the
    // prototype chain.
    const out = Object.create(null);
    const childErrors = [];
    let renderedPresent = false;
    for (const c of children) {
      const r = c.cr.read();
      if (Array.isArray(r.errors)) childErrors.push(...r.errors);
      if (r.present) { out[c.k] = r.value; renderedPresent = true; }
    }
    // Hidden-subtree preservation (spec §6), applied at EVERY object depth (not
    // just the form root): keys present in THIS object's original init value
    // but NOT declared in node.properties are ones the form rendered no control
    // for. Merge them back verbatim so getValue() never silently drops an
    // unrendered nested key. out is null-prototype, so out["__proto__"] = v
    // creates a safe own property rather than mutating the prototype chain.
    if (iv) {
      for (const k of Object.keys(iv)) {
        if (!Object.prototype.hasOwnProperty.call(declared, k)
          && !Object.prototype.hasOwnProperty.call(out, k)) {
          out[k] = iv[k];
        }
      }
    }
    const present = Object.keys(out).length > 0;
    const isRoot = path.length === 0;
    // JSON Schema `required` only applies when the object itself is present.
    // An untouched optional group is simply absent, so its descendants' errors
    // (including their own required-missing) are moot — drop them. The root is
    // conceptually always present (it is the document), so it always surfaces
    // its children's errors; a present group does too. An absent but required
    // group reports a single required-missing at its own path rather than a
    // noisy cascade of descendant errors. Error gating keys off RENDERED
    // children only: a group that exists solely because of a preserved
    // unrendered key is not "touched" by the user, so it must not manufacture
    // required-field errors for the controls they left empty.
    // The group's OWN required-missing check must use the SAME merged
    // presence (`present`, computed above from rendered children OR
    // preserved hidden keys) — not renderedPresent alone. Otherwise a
    // required object that's non-empty solely because of a preserved
    // undeclared key is reported present by getValue() yet still flagged as
    // a missing-required error here, contradicting itself.
    const errors = [];
    if (renderedPresent || isRoot) errors.push(...childErrors);
    else if (required && !present) errors.push({ path: fmtPath(path), message: 'required' });
    return { present, value: out, errors };
  };
  return { el, read };
}

// ---------------------------------------------------------------------------
// Composite: list (array-of-objects repeater OR array-of-arrays nested list)
// ---------------------------------------------------------------------------

function renderList(node, path, required, init, ctx) {
  const key = keyOf(path);
  const itemsSchema = node.items || {};

  const el = document.createElement('div');
  el.className = 'field sform-field sform-list';

  const head = document.createElement('div');
  head.className = 'sform-list-head';
  head.innerHTML = labelHTML(key, node, required);
  const count = document.createElement('span');
  count.className = 'sform-list-count';
  head.appendChild(count);
  el.appendChild(head);

  const cardsWrap = document.createElement('div');
  cardsWrap.className = 'sform-cards';
  el.appendChild(cardsWrap);

  const rows = []; // { card, read, titleEl }

  function renumber() {
    rows.forEach((r, i) => { r.titleEl.textContent = `${key}[${i}]`; });
    count.textContent = ` ${rows.length} item${rows.length === 1 ? '' : 's'}`;
  }

  function addCard(itemInit) {
    const idx = rows.length;
    const card = document.createElement('div');
    card.className = 'sform-card';

    const cardHead = document.createElement('div');
    cardHead.className = 'sform-card-head';
    const titleEl = document.createElement('span');
    titleEl.className = 'sform-card-title mono';
    const rm = document.createElement('button');
    rm.type = 'button';
    rm.className = 'sform-rm';
    rm.title = 'Remove item';
    rm.textContent = '✕';
    cardHead.appendChild(titleEl);
    cardHead.appendChild(rm);
    card.appendChild(cardHead);

    // Each element is rendered recursively. items.type === 'object' → a nested
    // group; items.type === 'array' → a scalar-array editor (this is how
    // array-of-arrays, e.g. data-generator literal.rows, falls out naturally).
    const body = renderNode(itemsSchema, path.concat(idx), false, itemInit, ctx);
    card.appendChild(body.el);

    // basePath captures the index this row's subtree was rendered with. After a
    // middle removal the row's live position (below) diverges from it, so read()
    // rebases child-error paths from basePath onto the current index. (Value
    // reads stay correct without rebasing — each reader is scoped to its own
    // card subtree, so a stale data-sf-path index never collides.)
    const row = { card, read: body.read, titleEl, basePath: fmtPath(path.concat(idx)) };
    rm.addEventListener('click', () => {
      const i = rows.indexOf(row);
      if (i >= 0) { rows.splice(i, 1); card.remove(); renumber(); }
    });
    rows.push(row);
    cardsWrap.appendChild(card);
    renumber();
  }

  if (Array.isArray(init)) init.forEach((item) => addCard(item));

  const add = document.createElement('button');
  add.type = 'button';
  add.className = 'sform-add';
  add.textContent = `+ Add ${key} item`;
  add.addEventListener('click', () => addCard(undefined));
  el.appendChild(add);

  const read = () => {
    const arr = [];
    const errors = [];
    rows.forEach((row, i) => {
      const r = row.read();
      if (Array.isArray(r.errors)) {
        const newBase = fmtPath(path.concat(i));
        for (const e of r.errors) {
          errors.push(row.basePath === newBase ? e : { ...e, path: rebasePath(e.path, row.basePath, newBase) });
        }
      }
      if (r.present) arr.push(r.value);
    });
    const minItems = required ? Math.max(1, node.minItems || 1) : (node.minItems || 0);
    if (arr.length < minItems) {
      errors.push({
        path: fmtPath(path),
        message: required && arr.length === 0 ? 'required' : `at least ${minItems} item${minItems === 1 ? '' : 's'} required`,
      });
    }
    return { present: arr.length > 0, value: arr, errors };
  };
  return { el, read };
}

// ---------------------------------------------------------------------------
// Composite: additionalProperties map (key → value rows)
// ---------------------------------------------------------------------------

function renderMap(node, path, required, init, ctx) {
  const key = keyOf(path);
  const valueSchema = node.additionalProperties && typeof node.additionalProperties === 'object'
    ? node.additionalProperties : {};

  const el = document.createElement('div');
  el.className = 'field sform-field sform-map';
  const head = document.createElement('div');
  head.className = 'sform-list-head';
  head.innerHTML = labelHTML(key, node, required);
  el.appendChild(head);

  const rowsWrap = document.createElement('div');
  rowsWrap.className = 'sform-map-rows';
  el.appendChild(rowsWrap);

  const rows = []; // { rowEl, keyInput, valRead }

  function addRow(k, v) {
    const rowEl = document.createElement('div');
    rowEl.className = 'sform-map-row';

    const keyInput = document.createElement('input');
    keyInput.className = 'input sform-map-key';
    keyInput.type = 'text';
    keyInput.placeholder = 'key';
    if (k != null) keyInput.value = String(k);

    // The value control is the FULL recursive renderer applied to the map's
    // value schema, so any value shape works: a secret picker
    // (x-datuplet-secret), enum, nested object/array, multi-type, etc. — not
    // just plain scalars. The key is unknown until the user types it, so the
    // value renders under a `*` placeholder segment; read() rebases its error
    // paths onto the actual key.
    const valCtl = renderNode(valueSchema, path.concat('*'), false, v, ctx);

    const rm = document.createElement('button');
    rm.type = 'button';
    rm.className = 'sform-rm';
    rm.title = 'Remove entry';
    rm.textContent = '✕';

    rowEl.appendChild(keyInput);
    rowEl.appendChild(valCtl.el);
    rowEl.appendChild(rm);

    const row = { rowEl, keyInput, valRead: valCtl.read };
    rm.addEventListener('click', () => {
      const i = rows.indexOf(row);
      if (i >= 0) { rows.splice(i, 1); rowEl.remove(); }
    });
    rows.push(row);
    rowsWrap.appendChild(rowEl);
  }

  if (init && typeof init === 'object' && !Array.isArray(init)) {
    for (const k of Object.keys(init)) addRow(k, init[k]);
  }

  const add = document.createElement('button');
  add.type = 'button';
  add.className = 'sform-add';
  add.textContent = `+ Add ${key} entry`;
  add.addEventListener('click', () => addRow('', undefined));
  el.appendChild(add);

  const starBase = fmtPath(path.concat('*'));
  const read = () => {
    const out = Object.create(null); // schema-controlled keys → null-proto.
    const errors = [];
    for (const row of rows) {
      const k = row.keyInput.value.trim();
      if (k === '') continue; // sparse: an empty key contributes nothing.
      const vr = row.valRead();
      const kBase = fmtPath(path.concat(k)); // `*` placeholder → the actual key.
      if (Array.isArray(vr.errors)) {
        for (const e of vr.errors) {
          errors.push(starBase === kBase ? e : { ...e, path: rebasePath(e.path, starBase, kBase) });
        }
      }
      if (vr.present) out[k] = vr.value;
      else errors.push({ path: kBase, message: 'value required' });
    }
    const keys = Object.keys(out);
    if (required && keys.length === 0) errors.push({ path: fmtPath(path), message: 'required' });
    return { present: keys.length > 0, value: out, errors };
  };
  return { el, read };
}

// ---------------------------------------------------------------------------
// Composite: multi-type `type` arrays (shape picker)
// ---------------------------------------------------------------------------

// Handles nodes whose `type` is an array of allowed types (spec: in-subset),
// e.g. pandas-transform operations[].columns is ["array","object"] (array-of-
// strings for select/drop, object-map for rename) and operations[].value is
// ["string","number","boolean","array"]. Strategy: a small shape picker chooses
// which type is active; the matching control is rendered into a slot. The active
// type is auto-detected from the initial value's JS type, defaulting to the
// first listed type when empty. read() reads only the active control, so the
// value round-trips as whatever shape the user picked.
function renderMultiType(node, path, required, init, ctx, types) {
  const key = keyOf(path);
  const el = document.createElement('div');
  el.className = 'field sform-field sform-multitype';

  const head = document.createElement('div');
  head.className = 'sform-list-head';
  head.innerHTML = labelHTML(key, node, required);
  const picker = document.createElement('select');
  picker.className = 'input sform-typepick';
  for (const tp of types) {
    const o = document.createElement('option');
    o.value = tp;
    o.textContent = tp;
    picker.appendChild(o);
  }
  head.appendChild(picker);
  el.appendChild(head);

  const slot = document.createElement('div');
  slot.className = 'sform-multitype-slot';
  el.appendChild(slot);

  const detected = detectType(init, types);
  picker.value = detected;

  let active = null; // { type, read }

  function subNode(tp) {
    // Synthesize a single-type node, preserving items/additionalProperties/enum.
    return Object.assign(Object.create(null), node, { type: tp });
  }

  function build(tp, withInit) {
    slot.innerHTML = '';
    const sn = subNode(tp);
    let child;
    if (isScalarType(tp) || tp === 'boolean' || Array.isArray(sn.enum)) {
      child = bareControl(sn, path, withInit);
    } else if (tp === 'array') {
      // A bare scalar-array editor (no key label of its own).
      child = renderScalarArray(sn, path, false, withInit, /* bare */ true);
    } else if (tp === 'object') {
      child = isMap(sn) ? renderMap(sn, path, false, withInit, ctx) : renderGroup(sn, path, false, withInit, ctx);
    } else {
      child = bareControl(sn, path, withInit);
    }
    slot.appendChild(child.el);
    active = { type: tp, read: child.read };
  }

  build(detected, init);
  picker.addEventListener('change', () => build(picker.value, undefined));

  const read = () => {
    const r = active.read();
    const errors = Array.isArray(r.errors) ? r.errors.slice() : [];
    if (required && !r.present) errors.push({ path: fmtPath(path), message: 'required' });
    return { present: r.present, value: r.value, errors };
  };
  return { el, read };
}

function detectType(v, types) {
  let jsType = null;
  if (Array.isArray(v)) jsType = 'array';
  else if (v !== null && typeof v === 'object') jsType = 'object';
  else if (typeof v === 'boolean') jsType = 'boolean';
  else if (typeof v === 'number') jsType = types.includes('integer') && Number.isInteger(v) ? 'integer' : 'number';
  else if (typeof v === 'string') jsType = 'string';
  if (jsType && types.includes(jsType)) return jsType;
  if (jsType === 'number' && types.includes('integer')) return 'integer';
  if (jsType === 'integer' && types.includes('number')) return 'number';
  return types[0];
}

// ---------------------------------------------------------------------------
// Leaf: scalar (string / number / integer / boolean / enum) with a key label
// ---------------------------------------------------------------------------

function renderScalar(node, path, required, init) {
  const key = keyOf(path);
  // A boolean WITH a default becomes a tri-state <select> (unset/true/false),
  // laid out like any other labeled control; a boolean WITHOUT a default stays
  // a plain checkbox (unchanged from U1/v1) with the checkbox-first layout.
  const isCheck = node.type === 'boolean' && !Array.isArray(node.enum) && node.default === undefined;
  const label = document.createElement('label');
  label.className = isCheck ? 'field sform-field sform-field--check' : 'field sform-field';

  const ctl = bareControl(node, path, init);
  if (isCheck) {
    // checkbox before label text
    label.appendChild(ctl.el);
    const span = document.createElement('span');
    span.innerHTML = labelHTML(key, node, required);
    label.appendChild(span);
  } else {
    label.insertAdjacentHTML('beforeend', labelHTML(key, node, required));
    label.appendChild(ctl.el);
  }

  const read = () => {
    const r = ctl.read();
    const errors = Array.isArray(r.errors) ? r.errors.slice() : [];
    if (required && !r.present) errors.push({ path: fmtPath(path), message: 'required' });
    return { present: r.present, value: r.value, errors };
  };
  return { el: label, read };
}

// ---------------------------------------------------------------------------
// Bare controls (no key label) — reused by scalar leaves, map values, and
// multi-type slots.
// ---------------------------------------------------------------------------

// setDefaultPlaceholder: `default` (display metadata only, spec §4.4) wins as
// the input placeholder (`default: <v>`); `examples[0]` is a fallback shown
// only when `default` is absent, as a plain value with no prefix. Neither is
// ever copied into the stored value — placeholders don't populate .value.
function setDefaultPlaceholder(el, node) {
  if (node.default !== undefined) {
    el.placeholder = `default: ${node.default}`;
  } else if (Array.isArray(node.examples) && node.examples.length > 0 && node.examples[0] !== undefined) {
    el.placeholder = String(node.examples[0]);
  }
}

// bareControl: enum → select; boolean → checkbox; number/integer → number input;
// string/unknown → text input or (x-datuplet-multiline) textarea. Returns
// { el, read }; el is a wrapper whose single control carries data-sf-path, so
// read() locates it via cssEsc()'d attribute selector scoped to that wrapper.
function bareControl(node, path, init) {
  const ps = fmtPath(path);
  const wrap = document.createElement('span');
  wrap.className = 'sform-ctl';

  if (Array.isArray(node.enum)) {
    const sel = document.createElement('select');
    sel.className = 'input';
    sel.setAttribute('data-sf-path', ps);
    const ph = document.createElement('option');
    ph.value = '';
    ph.textContent = node.default !== undefined ? `— default (${String(node.default)}) —` : '— choose —';
    sel.appendChild(ph);
    const initStr = init == null ? null : String(init);
    node.enum.forEach((e, i) => {
      const o = document.createElement('option');
      // Key each option by index, not String(e): enum:[1,"1"] must round-trip.
      o.value = String(i);
      o.textContent = String(e);
      if (initStr !== null && initStr === String(e)) o.selected = true;
      sel.appendChild(o);
    });
    wrap.appendChild(sel);
    return {
      el: wrap,
      read: () => {
        const el = ctlOf(wrap, ps);
        const v = el ? el.value : '';
        if (v === '') return { present: false, value: undefined, errors: [] };
        const idx = Number(v);
        if (!Number.isInteger(idx) || idx < 0 || idx >= node.enum.length) {
          return { present: false, value: undefined, errors: [] };
        }
        return { present: true, value: node.enum[idx], errors: [] };
      },
    };
  }

  if (node.type === 'boolean') {
    if (node.default !== undefined) {
      // Tri-state: the "unset" option means absent from getValue() — the
      // component applies its own default at runtime (spec §4.2/§4.4). Only
      // an explicit true/false choice is ever stored.
      const sel = document.createElement('select');
      sel.className = 'input';
      sel.setAttribute('data-sf-path', ps);
      const unset = document.createElement('option');
      unset.value = '';
      unset.textContent = `— default (${String(node.default)}) —`;
      sel.appendChild(unset);
      const t = document.createElement('option');
      t.value = 'true';
      t.textContent = 'true';
      sel.appendChild(t);
      const f = document.createElement('option');
      f.value = 'false';
      f.textContent = 'false';
      sel.appendChild(f);
      if (init === true) sel.value = 'true';
      else if (init === false) sel.value = 'false';
      else sel.value = '';
      wrap.appendChild(sel);
      return {
        el: wrap,
        read: () => {
          const el = ctlOf(wrap, ps);
          const v = el ? el.value : '';
          if (v === '') return { present: false, value: undefined, errors: [] };
          return { present: true, value: v === 'true', errors: [] };
        },
      };
    }
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.setAttribute('data-sf-path', ps);
    if (init === true) cb.checked = true;
    wrap.appendChild(cb);
    return {
      el: wrap,
      // A checkbox has a definite state, so it is always present.
      read: () => { const el = ctlOf(wrap, ps); return { present: true, value: el ? el.checked : false, errors: [] }; },
    };
  }

  if (node.type === 'number' || node.type === 'integer') {
    const isInt = node.type === 'integer';
    const inp = document.createElement('input');
    inp.className = 'input';
    inp.type = 'number';
    inp.setAttribute('data-sf-path', ps);
    if (isInt) { inp.setAttribute('step', '1'); inp.setAttribute('inputmode', 'numeric'); }
    setDefaultPlaceholder(inp, node);
    if (init != null) inp.value = String(init);
    wrap.appendChild(inp);
    return {
      el: wrap,
      read: () => {
        const el = ctlOf(wrap, ps);
        const v = el ? el.value.trim() : '';
        if (v === '') return { present: false, value: undefined, errors: [] };
        let num;
        if (isInt) {
          // parseInt would silently truncate "1.9"→1, "12abc"→12; require a full match.
          if (!/^[+-]?\d+$/.test(v)) return { present: false, value: undefined, errors: [{ path: ps, message: 'must be an integer' }] };
          num = Number(v);
        } else {
          num = Number(v);
          if (Number.isNaN(num)) return { present: false, value: undefined, errors: [{ path: ps, message: 'must be a number' }] };
        }
        if (typeof node.minimum === 'number' && num < node.minimum) {
          return { present: true, value: num, errors: [{ path: ps, message: `must be ≥ ${node.minimum}` }] };
        }
        return { present: true, value: num, errors: [] };
      },
    };
  }

  // string / unknown → text input, or textarea when x-datuplet-multiline is set.
  // x-datuplet-multiline is a STRING naming the language (e.g. "sql", "python");
  // the language is preserved on data-lang for a later task (U2) to consume.
  const ml = node['x-datuplet-multiline'];
  const lang = typeof ml === 'string' ? ml : (ml === true ? '' : null);
  let ctl;
  if (lang !== null) {
    ctl = document.createElement('textarea');
    ctl.className = 'input textarea input--mono';
    ctl.setAttribute('spellcheck', 'false');
    ctl.setAttribute('rows', '10');
    if (lang) ctl.setAttribute('data-lang', lang);
  } else {
    ctl = document.createElement('input');
    ctl.className = 'input';
    ctl.type = 'text';
  }
  ctl.setAttribute('data-sf-path', ps);
  setDefaultPlaceholder(ctl, node);
  if (init != null) ctl.value = String(init);
  wrap.appendChild(ctl);
  return {
    el: wrap,
    read: () => {
      const el = ctlOf(wrap, ps);
      const v = el ? el.value.trim() : '';
      if (v === '') return { present: false, value: undefined, errors: [] };
      return { present: true, value: v, errors: [] };
    },
  };
}

// ---------------------------------------------------------------------------
// Leaf: array of scalars (one-value-per-line textarea). `bare` omits the key
// label (used inside a multi-type slot).
// ---------------------------------------------------------------------------

function renderScalarArray(node, path, required, init, bare = false) {
  const key = keyOf(path);
  const ps = fmtPath(path);
  const itemType = node.items && node.items.type ? node.items.type : undefined;

  const el = document.createElement(bare ? 'span' : 'label');
  el.className = bare ? 'sform-ctl sform-scalar-array' : 'field sform-field sform-scalar-array';
  if (!bare) el.insertAdjacentHTML('beforeend', labelHTML(key, node, required));

  const hint = document.createElement('span');
  hint.className = 'sform-hint';
  hint.textContent = 'one per line';
  el.appendChild(hint);

  const ta = document.createElement('textarea');
  ta.className = 'input textarea input--mono';
  ta.setAttribute('spellcheck', 'false');
  ta.setAttribute('data-sf-path', ps);
  if (Array.isArray(init)) ta.value = init.map((x) => scalarToLine(x)).join('\n');
  el.appendChild(ta);

  const read = () => {
    const elx = ctlOf(el, ps);
    const raw = elx ? elx.value : '';
    const lines = raw.split('\n').map((s) => s.trim()).filter((s) => s !== '');
    if (lines.length === 0) {
      const errors = [];
      const minItems = required ? Math.max(1, node.minItems || 1) : (node.minItems || 0);
      if (minItems > 0) errors.push({ path: ps, message: required ? 'required' : `at least ${minItems} item${minItems === 1 ? '' : 's'} required` });
      return { present: false, value: undefined, errors };
    }
    const errors = [];
    const out = [];
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (itemType === 'integer') {
        if (!/^[+-]?\d+$/.test(line)) { errors.push({ path: ps, message: `line ${i + 1} is not an integer` }); continue; }
        out.push(Number(line));
      } else if (itemType === 'number') {
        const n = Number(line);
        if (Number.isNaN(n)) { errors.push({ path: ps, message: `line ${i + 1} is not a number` }); continue; }
        out.push(n);
      } else if (itemType === 'string') {
        out.push(line);
      } else {
        // Unconstrained (e.g. array-of-arrays cells, or a multi-type array):
        // parse each line as JSON (numbers/booleans/null/quoted), else keep raw.
        out.push(parseCell(line));
      }
    }
    if (node.minItems && out.length < node.minItems) {
      errors.push({ path: ps, message: `at least ${node.minItems} item${node.minItems === 1 ? '' : 's'} required` });
    }
    return { present: out.length > 0, value: out, errors };
  };
  return { el, read };
}

function scalarToLine(x) {
  if (x == null) return '';
  if (typeof x === 'object') { try { return JSON.stringify(x); } catch { return ''; } }
  return String(x);
}

function parseCell(s) {
  try { return JSON.parse(s); } catch { return s; }
}

// ---------------------------------------------------------------------------
// Leaf: secret picker (x-datuplet-secret) — preserved verbatim from v1.
// ---------------------------------------------------------------------------

// A <select> of $[<key>] refs; the control structurally prevents plaintext
// (§4.9), and read() still guards the stale-init path defensively.
function renderSecret(node, path, required, init, ctx) {
  const key = keyOf(path);
  const ps = fmtPath(path);
  const secretKeys = ctx.secretKeys;
  const initRef = typeof init === 'string' ? init : '';
  const known = new Set(secretKeys.map((k) => `$[${k}]`));

  const label = document.createElement('label');
  label.className = 'field sform-field';
  label.insertAdjacentHTML('beforeend', labelHTML(key, node, required));

  const sel = document.createElement('select');
  sel.className = 'input';
  sel.setAttribute('data-sf-path', ps);
  if (required) sel.setAttribute('required', '');
  const none = document.createElement('option');
  none.value = '';
  none.textContent = '— none —';
  sel.appendChild(none);
  for (const k of secretKeys) {
    const ref = `$[${k}]`;
    const o = document.createElement('option');
    o.value = ref;
    o.textContent = ref;
    if (initRef === ref) o.selected = true;
    sel.appendChild(o);
  }
  // Survive a stale list: keep an existing $[...] ref that's no longer offered.
  if (initRef && /^\$\[[^\]]+\]$/.test(initRef) && !known.has(initRef)) {
    const o = document.createElement('option');
    o.value = initRef;
    o.textContent = initRef;
    o.selected = true;
    sel.appendChild(o);
  }
  label.appendChild(sel);
  label.insertAdjacentHTML('beforeend', ' <a href="/ui/settings/secrets" class="sform-manage">manage secrets…</a>');

  const read = () => {
    const el = ctlOf(label, ps);
    const v = el ? el.value : '';
    if (v === '') return { present: false, value: undefined, errors: required ? [{ path: ps, message: 'a secret reference is required' }] : [] };
    if (!/^\$\[[^\]]+\]$/.test(v)) return { present: false, value: undefined, errors: [{ path: ps, message: 'must be a $[secret] reference' }] };
    return { present: true, value: v, errors: [] };
  };
  return { el: label, read };
}

// ---------------------------------------------------------------------------
// Leaf: out-of-subset JSON fallback (spec §4.2). A node using any of these
// keywords is outside the Form Subset; render a JSON sub-editor for JUST that
// node (never a whole-form fallback) with a callout naming the specific
// construct. Built-ins lint-clean and never reach this path (RFC 027 R5); it
// exists for third-party / operator-registered schemas only.
// ---------------------------------------------------------------------------

const OUT_OF_SUBSET_KEYS = [
  'oneOf', 'anyOf', 'allOf', 'not', '$ref', '$defs',
  'if', 'then', 'else', 'patternProperties', 'const',
];

// outOfSubsetKey: the first out-of-subset keyword found directly on `node`, or
// null. Own-property check only — inherited Object.prototype keys never match.
function outOfSubsetKey(node) {
  for (const k of OUT_OF_SUBSET_KEYS) {
    if (Object.prototype.hasOwnProperty.call(node, k)) return k;
  }
  return null;
}

function renderJsonFallback(node, path, required, init, unsupportedKey) {
  const key = keyOf(path);
  const ps = fmtPath(path);
  const el = document.createElement('div');
  el.className = 'field sform-field sform-json-fallback';

  // The schema root itself can be out-of-subset (no key to label); every
  // nested occurrence (property, array item, map value) has one.
  if (path.length > 0) el.insertAdjacentHTML('beforeend', labelHTML(key, node, required));

  const warn = document.createElement('div');
  warn.className = 'callout callout--warn';
  warn.innerHTML = `This property uses <code class="mono">${esc(unsupportedKey)}</code>, `
    + `which isn't supported by the form editor — edit as JSON.`;
  el.appendChild(warn);

  const ta = document.createElement('textarea');
  ta.className = 'input textarea input--mono';
  ta.setAttribute('spellcheck', 'false');
  ta.setAttribute('data-sf-path', ps);
  if (init !== undefined) {
    try { ta.value = JSON.stringify(init, null, 2); } catch { /* leave blank */ }
  }
  el.appendChild(ta);

  const read = () => {
    const elx = ctlOf(el, ps);
    const raw = elx ? elx.value.trim() : '';
    if (raw === '') {
      return { present: false, value: undefined, errors: required ? [{ path: ps, message: 'required' }] : [] };
    }
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (e) {
      // Never throw out of read()/getValue() — a bad edit surfaces via getErrors().
      return { present: false, value: undefined, errors: [{ path: ps, message: `invalid JSON: ${e.message}` }] };
    }
    return { present: true, value: parsed, errors: [] };
  };
  return { el, read };
}

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

// ctlOf: locate a control by its data-sf-path attribute, scoped to its own
// wrapper element. Scoping keeps lookups correct after repeater add/remove.
function ctlOf(root, ps) {
  return root.querySelector(`[data-sf-path="${cssEsc(ps)}"]`);
}

// cssEsc: escape a path for use inside a [data-sf-path="..."] selector. Paths
// derive from schema-controlled property names and may contain characters that
// break an attribute selector; prefer CSS.escape when available.
function cssEsc(s) {
  if (typeof window !== 'undefined' && window.CSS && typeof window.CSS.escape === 'function') return window.CSS.escape(s);
  return String(s).replace(/["\\\]]/g, '\\$&');
}
