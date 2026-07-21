// Pipeline detail: the form-first pipeline builder (RFC 027 §6).
//
// Route params:
//   /ui/pipelines/_new        → empty builder; name field is editable.
//                                Underscore is invalid in DNS-1123 so
//                                this sentinel can never collide with
//                                a real pipeline name.
//   /ui/pipelines/:name       → loaded builder; name is locked (the
//                                server enforces the URL name as the
//                                authoritative resource name).
//
// The page edits the FULL stored PipelineDoc in memory (module-level
// `doc`). The form renders and mutates only the paths it has controls for;
// everything the form has no control for — notably each component's
// `resources` block, and any config subtree the schema doesn't declare —
// rides along UNTOUCHED and survives a form-mode JSON save byte-for-byte
// (spec §6, "Hidden-subtree preservation"). Dropping an unrendered field is
// a bug, not a normalization.
//
// Save PUTs the doc as application/json; Validate POSTs it to
// …/pipelines/validate. Both surface the server's RFC 026 §7 findings.
//
// This is the U3 shell: outline + catalog modal + editor (name / version /
// config form) + gateway settings + Save/Validate. The Inputs picker (U4),
// Outputs editor (U5) and YAML mode (U6) build on the state contract below
// (`doc`, `sel`, renderOutline, renderEditor, saveDoc, validateDoc,
// getComponentMeta).

import { api, esc, getComponents, getComponent, getStorageCatalog, listSecrets, putPipelineYAML } from '/ui/api.js';
import { timeTag, phaseToPillClass, formatDuration, durationFrom } from '/ui/format.js';
import { renderFindings } from '/ui/lib/findings.js';
import { buildSchemaForm } from '/ui/lib/schema-form.js';
import { resolveProduces } from '/ui/lib/produces.js';
import { load as yamlLoad, dump as yamlDump } from '/ui/vendor/js-yaml.mjs';

// ---- Module-level builder state (state contract for U4–U6) ----------------
// `doc` is the FULL stored doc — see the hidden-subtree note above. `sel` is
// the selected {stage, component} indices (or null). The rest is page context
// the module-level render functions close over.
let doc = { name: '', stages: [{ name: 'stage-1', components: [] }] };
let sel = null;

// Builder surface: 'form' (schema-driven) or 'yaml' (raw text over the doc).
// The doc is the ONLY bridge between the two — there is no hand-rolled YAML
// serializer; →YAML dumps the live doc via js-yaml, →form parses the textarea
// back onto the doc wholesale (RFC 027 §6, "YAML mode"). Form is the default.
let mode = 'form';

let pid = null;
let pipelineName = '';
let isNew = false;
let pagePath = '';

let catalog = [];                 // getComponents() list (each carries `io`)
let secretKeys = [];
const detailCache = new Map();    // component name → getComponent() detail
// The live schema-form for the selected component: { handle, comp, schemaStr }.
// commitForm() reads it to merge the form's value back into comp.config.
let activeForm = null;
// Bumped on every renderEditor() call; async continuations bail if it changed.
let editorToken = 0;

const GATEWAY_FIELDS = [
  ['chunkSize', 'Chunk size', 'default: 33554432 (32 MiB)'],
  ['bufferSize', 'Buffer size', 'default: 67108864 (64 MiB)'],
  ['rowGroupSize', 'Row group size', 'default: same as bufferSize (64 MiB)'],
  ['targetFileSize', 'Target file size', 'default: 134217728 (128 MiB)'],
];
const GATEWAY_KEYS = GATEWAY_FIELDS.map(([k]) => k);

// isStale reports whether the user navigated away since this page render
// started; every post-await DOM write guards on it.
function isStale() {
  return window.location.pathname !== pagePath;
}

export async function renderPipelineDetail(ctx) {
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  pid = window.__datupletActiveProjectID;
  if (!pid) {
    if (head) head.innerHTML = '';
    app.innerHTML = `<p>No active project.</p>`;
    return;
  }
  pipelineName = ctx.params[0];
  isNew = pipelineName === '_new';
  pagePath = window.location.pathname;

  // Reset module state for this page instance.
  doc = { name: '', stages: [{ name: 'stage-1', components: [] }] };
  sel = null;
  mode = 'form';
  activeForm = null;
  detailCache.clear();

  let updatedAt = '';
  let pipelineID = '';
  if (!isNew) {
    const pipe = await api(`/api/v1/projects/${encodeURIComponent(pid)}/pipelines/${encodeURIComponent(pipelineName)}`);
    if (isStale()) return;
    // `pipe.doc` is the full canonical PipelineDoc (RFC 027 §5.1) — keep it
    // whole; the form mutates only the subpaths it renders.
    doc = normalizeDoc(pipe.doc);
    updatedAt = pipe.updated_at;
    pipelineID = pipe.id;
  }

  if (head) {
    const titleHTML = isNew
      ? `<h1>New pipeline</h1>`
      : `<h1><code class="inline">${esc(pipelineName)}</code></h1>`;
    const actions = isNew
      ? ''
      : `
        <a class="btn btn--primary" href="/ui/pipelines/${encodeURIComponent(pipelineName)}/trigger">Trigger run</a>
        <button type="button" id="delete-btn" class="btn btn--secondary">Delete</button>
      `;
    head.innerHTML = `
      ${titleHTML}
      <div class="actions">${actions}</div>
    `;
  }

  app.innerHTML = `
    ${updatedAt ? `<p style="color:var(--fg-2);"><small>Updated ${timeTag(updatedAt)}</small></p>` : ''}
    <div class="builder-topbar">
      ${isNew ? `
        <label class="field">Name
          <input class="input input--mono" type="text" id="builder-name" placeholder="my-pipeline"
            spellcheck="false" pattern="[a-z0-9]([-a-z0-9.]*[a-z0-9])?"
            title="Lowercase DNS-1123 subdomain."
            value="${esc(doc.name || '')}">
        </label>` : ''}
      <div class="seg" id="builder-mode" role="tablist" aria-label="Editor mode">
        <button type="button" id="mode-form" class="on" role="tab" aria-selected="true">Form</button>
        <button type="button" id="mode-yaml" role="tab" aria-selected="false">YAML</button>
      </div>
      <div class="builder-topbar-actions">
        <button type="button" class="btn btn--secondary" id="builder-validate">Validate</button>
        <button type="button" class="btn btn--primary" id="builder-save">Save</button>
      </div>
    </div>
    <div id="builder-msg" class="builder-msg"></div>
    <div class="builder-shell" id="builder-form-view">
      <div class="builder-outline">
        <details class="pipeline-settings">
          <summary>Pipeline settings</summary>
          <div class="pipeline-settings-body" id="pipeline-settings-body"></div>
        </details>
        <div id="outline"></div>
      </div>
      <div class="builder-editor" id="editor"></div>
    </div>
    <div id="builder-yaml-view" style="display:none;">
      <p class="hint">Raw mode — the same document, serialized. No <code>apiVersion</code>, <code>kind</code>, or <code>metadata</code> envelope. Switching back to Form parses it (round-trip); YAML comments don't survive a form-side save.</p>
      <div id="yaml-err"></div>
      <textarea id="yaml-ta" class="input--mono" spellcheck="false" autocapitalize="off" autocomplete="off"
        style="width:100%;min-height:60vh;resize:vertical;padding:var(--s-3);border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-1);color:var(--fg-0);white-space:pre;overflow:auto;"></textarea>
    </div>
    <div id="catalog-host"></div>
    ${isNew ? '' : `
    <section style="margin-top:var(--s-5);">
      <h2>Recent runs</h2>
      <div id="pipeline-runs"><p><small>Loading…</small></p></div>
    </section>`}
  `;

  // Topbar wiring.
  if (isNew) {
    const nameInput = document.getElementById('builder-name');
    nameInput.addEventListener('input', () => { doc.name = nameInput.value.trim(); });
  }
  document.getElementById('builder-save').addEventListener('click', saveDoc);
  document.getElementById('builder-validate').addEventListener('click', validateDoc);
  document.getElementById('mode-form').addEventListener('click', () => setMode('form'));
  document.getElementById('mode-yaml').addEventListener('click', () => setMode('yaml'));

  if (!isNew) {
    const delBtn = document.getElementById('delete-btn');
    if (delBtn) {
      delBtn.addEventListener('click', async () => {
        if (!confirm(`Delete pipeline "${pipelineName}"? Runs that reference it by ID stay in history, but you won't be able to trigger new ones.`)) return;
        try {
          await api(`/api/v1/projects/${encodeURIComponent(pid)}/pipelines/${encodeURIComponent(pipelineName)}`, { method: 'DELETE' });
          window.history.replaceState({}, '', '/ui/pipelines');
          if (typeof window.renderRoute === 'function') window.renderRoute();
        } catch (err) {
          if (String(err.message) !== 'not authenticated') {
            document.getElementById('builder-msg').innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
          }
        }
      });
    }
  }

  if (!isNew && pipelineID) {
    // Best-effort: a slow/failing runs fetch must not block the builder above.
    loadRecentRuns(pid, pipelineID).catch((err) => {
      if (String(err.message) === 'not authenticated') return;
      const container = document.getElementById('pipeline-runs');
      if (container) container.innerHTML = `<p><small>Couldn't load recent runs: ${esc(err.message)}</small></p>`;
    });
  }

  // Secret keys populate the config form's $[...] pickers (optional).
  try {
    const s = await listSecrets(pid);
    if (isStale()) return;
    secretKeys = (s || []).map((x) => x.key);
  } catch { /* secrets optional */ }

  // Component catalog: powers the "+ Add component" modal and getComponentMeta.
  try {
    catalog = await getComponents();
  } catch {
    // Swallow the centralized 401 redirect; on any other error leave the
    // catalog empty (the "+ Add component" modal then shows an empty state).
    catalog = [];
  }
  if (isStale()) return;

  renderGateway();
  renderOutline();
  renderEditor();
}

// normalizeDoc keeps the whole stored doc but guarantees the shape the
// renderers assume (a non-empty stages array, each with a components array).
// It never strips unknown top-level keys — they ride along on save.
function normalizeDoc(d) {
  const out = (d && typeof d === 'object' && !Array.isArray(d)) ? d : {};
  if (!Array.isArray(out.stages) || out.stages.length === 0) {
    out.stages = [{ name: 'stage-1', components: [] }];
  }
  for (const st of out.stages) {
    if (!Array.isArray(st.components)) st.components = [];
  }
  return out;
}

// getComponentMeta returns the catalog entry for a component (incl. its `io`
// capability), or null. U4/U5 use `io` to decide whether to show the
// Inputs/Outputs sections at all.
export function getComponentMeta(name) {
  return catalog.find((c) => c.name === name) || null;
}

// pickDocVersion selects the version to build a form for: the declared
// default, else the first non-prerelease (stable), else the first listed.
// Works on both a catalog list entry and a getComponent() detail (both carry
// `defaultVersion` and `versions[]`).
function pickDocVersion(c) {
  const versions = (c && c.versions) || [];
  return versions.find((x) => x.version === c.defaultVersion)
    || versions.find((x) => !x.prerelease)
    || versions[0];
}

// schemaOf resolves the configSchema string for a component detail at a given
// version, defaulting via pickDocVersion when the version isn't found.
function schemaOf(detail, version) {
  const versions = (detail && detail.versions) || [];
  const v = versions.find((x) => x.version === version) || pickDocVersion(detail);
  return (v && v.configSchema) || '{}';
}

// getComponentDetail fetches (and caches) a component's per-version detail,
// which carries the configSchema the list endpoint omits.
async function getComponentDetail(name) {
  if (detailCache.has(name)) return detailCache.get(name);
  const d = await getComponent(name);
  detailCache.set(name, d);
  return d;
}

// unrenderedKeys returns the subset of `config`'s own keys that are NOT
// declared in the schema's `properties` — i.e. keys the form has no control
// for and therefore never touched. These are preserved verbatim through a
// save (spec §6). Out-of-subset config subtrees the form DOES render (as a
// JSON fallback node) are handled by the form itself, so they are not
// preserved here — they come back through getValue().
function unrenderedKeys(config, schemaStr) {
  // Null-prototype accumulator: a stored config key literally named
  // "__proto__" (or "constructor"/"prototype") copied via out[k] = … becomes a
  // safe own property here instead of mutating the object's prototype chain —
  // the same anti-prototype-pollution measure schema-form.js uses. The spread
  // that consumes this in commitForm() ({...unrenderedKeys(...)}) copies own
  // keys with CreateDataProperty semantics, so the safety carries through.
  const out = Object.create(null);
  if (!config || typeof config !== 'object' || Array.isArray(config)) return out;
  let props = null;
  try {
    const s = JSON.parse(schemaStr || '{}');
    props = s && typeof s.properties === 'object' ? s.properties : null;
  } catch { props = null; }
  for (const k of Object.keys(config)) {
    if (!props || !Object.prototype.hasOwnProperty.call(props, k)) out[k] = config[k];
  }
  return out;
}

// commitForm merges the live form value back into the selected component's
// config, preserving the keys the form never rendered (see unrenderedKeys).
// Merge, don't replace: keys the form renders are overwritten with the form's
// (sparse) value; keys it doesn't know about ride along unchanged.
function commitForm() {
  if (!activeForm || !activeForm.handle) return;
  const { handle, comp, schemaStr } = activeForm;
  comp.config = { ...unrenderedKeys(comp.config, schemaStr), ...handle.getValue() };
}

// ---- Gateway (Pipeline settings) ------------------------------------------

function renderGateway() {
  const host = document.getElementById('pipeline-settings-body');
  if (!host) return;
  const g = doc.gateway || {};
  host.innerHTML = GATEWAY_FIELDS.map(([key, label, ph]) => `
    <label class="field">
      <span class="sform-label">${esc(label)} <code class="mono">${esc(key)}</code></span>
      <input class="input" type="number" min="0" step="1" spellcheck="false"
        data-gwkey="${esc(key)}" placeholder="${esc(ph)}"
        value="${g[key] != null ? esc(String(g[key])) : ''}">
    </label>`).join('');
  host.querySelectorAll('[data-gwkey]').forEach((inp) => inp.addEventListener('input', syncGateway));
}

// syncGateway rebuilds doc.gateway from the four inputs. Defaults are
// metadata, not stored values: an empty field contributes nothing, and when
// all four are empty `doc.gateway` is dropped entirely (never `{}`). Any
// gateway key the form doesn't render is preserved (hidden-subtree rule).
function syncGateway() {
  const host = document.getElementById('pipeline-settings-body');
  if (!host) return;
  const cur = doc.gateway || {};
  // Null-prototype accumulator: `cur` keys are doc-derived, so a stored gateway
  // key named "__proto__" copied via g[k] = … must land as a safe own property,
  // not mutate the prototype chain (same hardening as unrenderedKeys).
  const g = Object.create(null);
  for (const k of Object.keys(cur)) {
    if (!GATEWAY_KEYS.includes(k)) g[k] = cur[k]; // preserve unknown keys
  }
  host.querySelectorAll('[data-gwkey]').forEach((inp) => {
    const v = inp.value.trim();
    if (v === '' || !/^\d+$/.test(v)) return;
    g[inp.dataset.gwkey] = parseInt(v, 10);
  });
  if (Object.keys(g).length) doc.gateway = g;
  else delete doc.gateway;
}

// ---- Outline (stages → component cards) -----------------------------------

export function renderOutline() {
  const el = document.getElementById('outline');
  if (!el) return;
  let h = '';
  doc.stages.forEach((st, si) => {
    h += `<div class="stage" data-si="${si}">
      <div class="stage-h">STAGE
        <input value="${esc(st.name || '')}" data-stname="${si}" spellcheck="false" aria-label="Stage name">
        <button type="button" class="icon-btn" data-delstage="${si}" title="Delete stage">✕</button>
      </div>`;
    st.components.forEach((c, ci) => {
      const s = sel && sel.s === si && sel.c === ci ? ' sel' : '';
      h += `<div class="ccard${s}" data-sel="${si}:${ci}">
        <span><span class="cn">${esc(c.name || '·')}</span><br><span class="ct">${esc(c.component || '')}</span></span>
        <button type="button" class="icon-btn ccard-x" data-del="${si}:${ci}" title="Remove component">✕</button>
      </div>`;
    });
    h += `<button type="button" class="btn btn--ghost" data-add="${si}">+ Add component</button></div>`;
  });
  h += `<button type="button" class="btn btn--ghost" id="add-stage">+ Add stage</button>`;
  el.innerHTML = h;

  // Select a component (ignore clicks on its remove button).
  el.querySelectorAll('[data-sel]').forEach((n) => n.addEventListener('click', (e) => {
    if (e.target.closest('[data-del]')) return;
    const [s, c] = n.dataset.sel.split(':').map(Number);
    sel = { s, c };
    renderOutline();
    renderEditor();
  }));
  // Remove a component.
  el.querySelectorAll('[data-del]').forEach((n) => n.addEventListener('click', (e) => {
    e.stopPropagation();
    const [s, c] = n.dataset.del.split(':').map(Number);
    doc.stages[s].components.splice(c, 1);
    // Adjust the selection so it keeps pointing at the SAME component. Only the
    // selected component's own removal clears the selection; removing an
    // earlier sibling in the same stage shifts the selected index down by one;
    // a removal in another stage (or after the selected one) leaves sel alone.
    if (sel && sel.s === s) {
      if (sel.c === c) sel = null;
      else if (sel.c > c) sel = { s, c: sel.c - 1 };
    }
    renderOutline();
    renderEditor();
  }));
  // Add a component via the catalog modal.
  el.querySelectorAll('[data-add]').forEach((n) => n.addEventListener('click', () => openCatalog(Number(n.dataset.add))));
  // Delete a stage (confirm when it still holds components).
  el.querySelectorAll('[data-delstage]').forEach((n) => n.addEventListener('click', () => {
    const si = Number(n.dataset.delstage);
    const st = doc.stages[si];
    if (st.components.length && !confirm(`Delete stage "${st.name}" and its ${st.components.length} component(s)?`)) return;
    doc.stages.splice(si, 1);
    if (!doc.stages.length) doc.stages.push({ name: 'stage-1', components: [] });
    sel = null;
    renderOutline();
    renderEditor();
  }));
  // Inline stage rename (live).
  el.querySelectorAll('[data-stname]').forEach((n) => n.addEventListener('input', () => {
    doc.stages[Number(n.dataset.stname)].name = n.value;
  }));
  // Add a stage (auto-numbered).
  const addStage = document.getElementById('add-stage');
  if (addStage) addStage.addEventListener('click', () => {
    // Pick the smallest stage-N not already in use, so deleting/renaming stages
    // can't make a fresh stage collide with an existing name (length + 1 would:
    // with stage-1 and stage-3 present, length is 2 → stage-3, a duplicate).
    const used = new Set(doc.stages.map((st) => st.name));
    let n = 1;
    while (used.has(`stage-${n}`)) n++;
    doc.stages.push({ name: `stage-${n}`, components: [] });
    renderOutline();
  });
}

// renderOutlineSoft updates the selected card's name label in place, so the
// instance-name input keeps focus while typing (a full renderOutline would
// rebuild the DOM and blur it).
function renderOutlineSoft() {
  const cn = document.querySelector('.ccard.sel .cn');
  if (cn && sel) cn.textContent = doc.stages[sel.s].components[sel.c].name || '·';
}

// allInstanceNames / uniqueInstanceName keep a freshly-added component's
// default instance name unique across the whole doc, so two of the same
// component don't collide on the server's duplicate-name check.
function allInstanceNames() {
  const s = new Set();
  for (const st of doc.stages) for (const c of st.components) if (c.name) s.add(c.name);
  return s;
}
function uniqueInstanceName(base) {
  const names = allInstanceNames();
  base = base || 'component';
  if (!names.has(base)) return base;
  let i = 2;
  while (names.has(`${base}-${i}`)) i++;
  return `${base}-${i}`;
}

// ---- Catalog modal --------------------------------------------------------

function openCatalog(si) {
  const host = document.getElementById('catalog-host');
  if (!host) return;
  const items = catalog.length
    ? catalog.map((c) => `
        <div class="cat-item" data-pick="${esc(c.name)}">
          <span class="cin">${esc(c.name)}${c.deprecated ? ' <span class="cdep">(deprecated)</span>' : ''}</span>
          <span class="cid">${esc(c.description || c.displayName || '')}</span>
        </div>`).join('')
    : `<p style="color:var(--fg-2);">No components are registered in this project.</p>`;
  host.innerHTML = `
    <div class="catalog-back" id="catalog-back">
      <div class="catalog" role="dialog" aria-label="Add component">
        <h3>Add component</h3>
        ${items}
      </div>
    </div>`;
  const back = document.getElementById('catalog-back');
  back.addEventListener('click', (e) => { if (e.target === back) host.innerHTML = ''; });
  host.querySelectorAll('[data-pick]').forEach((n) => n.addEventListener('click', () => {
    const cname = n.dataset.pick;
    const meta = getComponentMeta(cname);
    const version = meta ? (pickDocVersion(meta) || {}).version : undefined;
    const comp = { name: uniqueInstanceName(cname), component: cname, config: {} };
    if (version) comp.version = version;
    doc.stages[si].components.push(comp);
    sel = { s: si, c: doc.stages[si].components.length - 1 };
    host.innerHTML = '';
    renderOutline();
    renderEditor();
  }));
}

// ---- Editor (selected component: name + version + config form) ------------

export function renderEditor() {
  const el = document.getElementById('editor');
  if (!el) return;
  const token = ++editorToken;

  // Commit and tear down the previous form before rebuilding.
  commitForm();
  if (activeForm && activeForm.handle) { activeForm.handle.destroy(); }
  activeForm = null;

  if (!sel || !doc.stages[sel.s] || !doc.stages[sel.s].components[sel.c]) {
    el.innerHTML = `<div class="builder-empty">Pick a component on the left, or add one from the catalog.</div>`;
    return;
  }
  const comp = doc.stages[sel.s].components[sel.c];
  const meta = getComponentMeta(comp.component);
  const title = (meta && meta.displayName) || comp.component;
  const desc = (meta && meta.description) || '';

  el.innerHTML = `
    <div class="ed-head">
      <h2>${esc(title)}</h2>
      <span class="ed-version">version
        <select class="input" id="ed-version"><option>Loading…</option></select>
      </span>
    </div>
    ${desc ? `<p class="ed-desc">${esc(desc)}</p>` : ''}
    <label class="field">
      <span class="sform-label">name <span class="sform-desc">instance name in this pipeline</span></span>
      <input class="input input--mono" id="ed-name" spellcheck="false" value="${esc(comp.name || '')}">
    </label>
    <div class="section-h">Config</div>
    <div id="cfg-form"><p aria-busy="true">Loading schema…</p></div>`;

  document.getElementById('ed-name').addEventListener('input', (e) => {
    comp.name = e.target.value;
    renderOutlineSoft();
  });

  // Load the component detail (versions + configSchema), then build the form.
  buildConfigForm(comp, token).catch((e) => {
    if (token !== editorToken || isStale()) return;
    const form = document.getElementById('cfg-form');
    if (form) form.innerHTML = `<div class="callout callout--warn">${esc(String(e && e.message || e))}</div>`;
  });

  renderInputsSection(el, comp, meta, sel.s, token);
  renderOutputsSection(el, comp, meta, sel.s, token);
}

// ---- Inputs (RFC 027 U4) ---------------------------------------------------
//
// Rendered only when the component can consume tables at all (io.inputs !==
// 'none' — data-generator / http-json-extractor / finnhub-extractor all
// declare 'none' and get no Inputs section, spec §6). Current inputs show as
// chips; "+ Add input table…" opens a modal picker over two groups — tables
// produced by earlier stages, and tables already in project storage.
function renderInputsSection(el, comp, meta, si, token) {
  const io = (meta && meta.io) || {};
  if (io.inputs === 'none') return;

  const wrap = document.createElement('div');
  wrap.innerHTML = `
    <div class="section-h">Inputs<span class="sform-desc"> — tables this component reads${io.inputs === 'required' ? ' (required)' : ''}</span></div>
    <div id="inputs-chips"></div>
    <button type="button" class="btn" id="inputs-add">+ Add input table…</button>`;
  el.appendChild(wrap);

  renderInputsChips(comp);

  document.getElementById('inputs-add').addEventListener('click', () => {
    openInputTablePicker(comp, si, token).catch((e) => {
      if (token !== editorToken || isStale()) return;
      const host = document.getElementById('catalog-host');
      if (host) host.innerHTML = `<div class="catalog-back" id="pickback"><div class="catalog"><div class="callout callout--warn">${esc(String(e && e.message || e))}</div><button type="button" class="btn" id="pick-cancel">Close</button></div></div>`;
      const cancel = document.getElementById('pick-cancel');
      if (cancel) cancel.addEventListener('click', () => { host.innerHTML = ''; });
    });
  });
}

// renderInputsChips (re)draws the current comp.inputs.tables as chips with a
// remove control. Removing the last table drops the now-empty `tables` key
// (and `inputs` itself only if it's now fully empty) — hidden-subtree rule:
// other inputs-level fields (e.g. `buckets`) and other per-table fields (e.g.
// `since`, `as`) on entries we don't touch ride along untouched.
function renderInputsChips(comp) {
  const host = document.getElementById('inputs-chips');
  if (!host) return;
  const tables = (comp.inputs && Array.isArray(comp.inputs.tables)) ? comp.inputs.tables : [];
  host.innerHTML = tables.map((t, i) => `
    <span class="chip chip--table">
      <code class="mono">${esc(t.bucket)}.${esc(t.table)}</code>
      <button type="button" class="icon-btn" data-rminput="${i}" title="Remove input">✕</button>
    </span>`).join('');
  host.querySelectorAll('[data-rminput]').forEach((n) => n.addEventListener('click', () => {
    const i = Number(n.dataset.rminput);
    comp.inputs.tables.splice(i, 1);
    if (comp.inputs.tables.length === 0) delete comp.inputs.tables;
    if (comp.inputs && Object.keys(comp.inputs).length === 0) delete comp.inputs;
    renderInputsChips(comp);
  }));
}

// resolveComponentProducesPath fetches (via the cached component detail) the
// schema root's `x-datuplet-produces` annotation for a component at a given
// version, or '' if the component/version/annotation can't be resolved.
async function resolveComponentProducesPath(componentName, version) {
  let detail;
  try {
    detail = await getComponentDetail(componentName);
  } catch {
    return '';
  }
  const schemaStr = schemaOf(detail, version);
  try {
    const s = JSON.parse(schemaStr || '{}');
    return typeof s['x-datuplet-produces'] === 'string' ? s['x-datuplet-produces'] : '';
  } catch {
    return '';
  }
}

// upstreamTables enumerates candidate input tables produced by stages BEFORE
// stage index `si` (spec §6):
//   (a) explicit outputs.tables entries — used verbatim (bucket + name);
//   (b) a dynamic output (outputs.defaultBucket, no explicit outputs.tables)
//       whose component schema declares x-datuplet-produces — resolved via
//       resolveProduces() over that component's config, paired with
//       defaultBucket;
//   (c) a dynamic output with no resolvable produces path (unknown component,
//       no annotation, or the path resolves to nothing) — surfaced as a
//       single disabled `<bucket> (dynamic)` row, not selectable.
async function upstreamTables(si) {
  const out = [];
  for (let i = 0; i < si; i++) {
    for (const comp of doc.stages[i].components) {
      const o = comp.outputs || {};
      if (Array.isArray(o.tables) && o.tables.length) {
        for (const t of o.tables) {
          if (t && t.bucket && t.name) {
            out.push({ bucket: t.bucket, table: t.name, from: doc.stages[i].name, disabled: false });
          }
        }
      } else if (o.defaultBucket) {
        const producesPath = await resolveComponentProducesPath(comp.component, comp.version);
        const names = producesPath ? resolveProduces(producesPath, comp.config || {}) : [];
        if (names.length) {
          for (const name of names) {
            if (typeof name === 'string' && name) {
              out.push({ bucket: o.defaultBucket, table: name, from: doc.stages[i].name, disabled: false });
            }
          }
        } else {
          out.push({ bucket: o.defaultBucket, from: doc.stages[i].name, disabled: true });
        }
      }
    }
  }
  return out;
}

// storageTableRows fetches the project's storage catalog and reshapes it to
// {bucket, table} pairs (the catalog's `namespace` IS the pipeline doc's
// `bucket` — see catalog_proxy.go). Best-effort: an unreachable catalog just
// yields an empty "IN STORAGE" group rather than blocking the picker.
async function storageTableRows() {
  try {
    const resp = await getStorageCatalog(pid);
    const tables = Array.isArray(resp.tables) ? resp.tables : [];
    return tables.map((t) => ({ bucket: t.namespace, table: t.name }));
  } catch {
    return [];
  }
}

// openInputTablePicker gathers the two groups (upstream + storage) then hands
// off to openInputPicker to render the modal. `token` guards against a stale
// async continuation after the editor rebuilt for a different component.
async function openInputTablePicker(comp, si, token) {
  const have = (comp.inputs && Array.isArray(comp.inputs.tables))
    ? comp.inputs.tables.map((t) => ({ bucket: t.bucket, table: t.table }))
    : [];
  const host = document.getElementById('catalog-host');
  if (host) host.innerHTML = `<div class="catalog-back" id="pickback"><div class="catalog"><p aria-busy="true">Loading tables…</p></div></div>`;
  const [upstreamRows, storageRows] = await Promise.all([upstreamTables(si), storageTableRows()]);
  if (token !== editorToken || isStale()) return;
  openInputPicker(upstreamRows, storageRows, have, (list) => {
    comp.inputs = comp.inputs || {};
    comp.inputs.tables = comp.inputs.tables || [];
    for (const x of list) comp.inputs.tables.push({ bucket: x.bucket, table: x.table });
    renderInputsChips(comp);
  });
}

// openInputPicker renders the "+ Add input table…" modal (ported interaction
// pattern from the RFC 027 UX mockup's openTablePicker): two groups —
// PRODUCED UPSTREAM (already-resolved upstreamRows, including disabled
// `<bucket> (dynamic)` rows) and IN STORAGE (storageRows) — with a search
// filter and multi-select. `have` is the already-wired [{bucket,table}] list
// (used to exclude already-added tables); onAdd receives the picked list.
function openInputPicker(upstreamRows, storageRows, have, onAdd) {
  const hasIt = (b, t) => have.some((x) => x.bucket === b && x.table === t);
  const ups = upstreamRows.filter((u) => u.disabled || !hasIt(u.bucket, u.table));
  const ex = storageRows.filter((u) => !hasIt(u.bucket, u.table));
  const picked = new Set();
  const host = document.getElementById('catalog-host');
  if (!host) return;

  const row = (u) => {
    if (u.disabled) {
      return `<div class="cat-item cat-item--disabled" aria-disabled="true">
        <span class="cin">${esc(u.bucket)} (dynamic)</span>
        <span class="cid">produced by stage ${esc(u.from)} — table names can't be resolved</span></div>`;
    }
    return `<div class="cat-item" data-t="${esc(u.bucket)}.${esc(u.table)}">
      <span class="cin">${esc(u.bucket)}.${esc(u.table)}</span>
      <span class="cid">${u.from ? `produced by stage ${esc(u.from)}` : 'in storage'}</span></div>`;
  };

  host.innerHTML = `<div class="catalog-back" id="pickback"><div class="catalog" role="dialog" aria-label="Add input tables">
    <h3>Add input tables</h3>
    <input class="input" id="pick-search" placeholder="Filter tables…" spellcheck="false">
    ${ups.length ? '<p class="hint">PRODUCED UPSTREAM</p>' + ups.map(row).join('') : ''}
    ${ex.length ? '<p class="hint">IN STORAGE</p>' + ex.map(row).join('') : ''}
    ${!ups.length && !ex.length ? '<p class="hint">No tables available yet.</p>' : ''}
    <div class="cat-actions">
      <button type="button" class="btn btn--primary" id="pick-add" disabled>Add selected</button>
      <button type="button" class="btn" id="pick-cancel">Cancel</button>
    </div>
  </div></div>`;

  const sync = () => {
    const b = document.getElementById('pick-add');
    b.disabled = picked.size === 0;
    b.textContent = picked.size ? `Add ${picked.size} table${picked.size > 1 ? 's' : ''}` : 'Add selected';
  };
  host.querySelectorAll('[data-t]').forEach((n) => n.addEventListener('click', () => {
    const k = n.dataset.t;
    if (picked.has(k)) { picked.delete(k); n.classList.remove('cat-item--picked'); }
    else { picked.add(k); n.classList.add('cat-item--picked'); }
    sync();
  }));
  const back = document.getElementById('pickback');
  back.addEventListener('click', (e) => { if (e.target === back) host.innerHTML = ''; });
  document.getElementById('pick-search').addEventListener('input', (e) => {
    const q = e.target.value.trim().toLowerCase();
    host.querySelectorAll('.cat-item').forEach((n) => {
      const key = (n.dataset.t || n.textContent || '').toLowerCase();
      n.style.display = key.includes(q) ? '' : 'none';
    });
  });
  document.getElementById('pick-cancel').addEventListener('click', () => { host.innerHTML = ''; });
  document.getElementById('pick-add').addEventListener('click', () => {
    const list = [];
    for (const k of picked) {
      const dot = k.indexOf('.');
      list.push({ bucket: k.slice(0, dot), table: k.slice(dot + 1) });
    }
    host.innerHTML = '';
    onAdd(list);
  });
}

// ---- Outputs (RFC 027 U5) --------------------------------------------------
//
// Rendered only when the component can produce tables at all (io.outputs !==
// 'none' — stdout-writer declares 'none' and gets no Outputs section, spec
// §6). Dual-mode, mirroring the RFC 027 UX mockup's renderOutputs(box, comp):
// "Dynamic — bucket only" (outputs.defaultBucket [+ defaultWriteMode]) vs
// "Explicit tables" (outputs.tables[] rows of {name, bucket, writeMode?}).
// Switching modes carries the first table's bucket (+ writeMode) over, and
// vice versa — same as the mockup.
//
// A stored doc can carry outputs.buckets[] — a legacy/advanced multi-bucket
// shape this editor has no mode for. That subtree is preserved verbatim
// (hidden-subtree rule, same convention as unrenderedKeys/renderInputsChips):
// the section renders a read-only note instead of the toggle, and nothing in
// renderOutputs ever reads or writes comp.outputs while buckets[] is present.
function renderOutputsSection(el, comp, meta, si, token) {
  const io = (meta && meta.io) || {};
  if (io.outputs === 'none') return;

  const wrap = document.createElement('div');
  el.appendChild(wrap);
  renderOutputs(wrap, comp, si, token);
}

// setOutMode stashes the dynamic/explicit toggle choice as a NON-enumerable
// own property. It has to live somewhere that survives re-renders of the same
// comp object, but comp is part of `doc` and rides straight into
// JSON.stringify(doc) on Save/Validate (§6) — a plain enumerable
// `comp.__outMode = …` (as the mockup does) would leak into the saved
// PipelineDoc. Non-enumerable keeps it invisible to JSON.stringify, object
// spreads, and Object.keys, while remaining readable via comp.__outMode.
function setOutMode(comp, mode) {
  Object.defineProperty(comp, '__outMode', { value: mode, enumerable: false, configurable: true, writable: true });
}

function renderOutputs(box, comp, si, token) {
  const o = comp.outputs || {};

  // outputs.buckets[] preservation (see header comment) — read-only note,
  // no toggle, comp.outputs untouched.
  if (Array.isArray(o.buckets)) {
    box.innerHTML = `<div class="section-h">Outputs<span class="sform-desc"> — tables this component writes</span></div>
      <p class="hint">multi-bucket outputs — edit in YAML mode</p>`;
    return;
  }

  if (!comp.__outMode) setOutMode(comp, (Array.isArray(o.tables) && o.tables.length) ? 'explicit' : 'dynamic');
  const mode = comp.__outMode;
  box.innerHTML = `<div class="section-h">Outputs<span class="sform-desc"> — tables this component writes</span></div>
    <div class="seg">
      <button type="button" id="om-dyn" class="${mode === 'dynamic' ? 'on' : ''}">Dynamic — bucket only</button>
      <button type="button" id="om-exp" class="${mode === 'explicit' ? 'on' : ''}">Explicit tables</button>
    </div>
    <div id="out-body"></div>`;
  box.querySelector('#om-dyn').addEventListener('click', () => {
    if (comp.__outMode === 'dynamic') return;
    setOutMode(comp, 'dynamic');
    const first = ((comp.outputs || {}).tables || [])[0];
    if (first && first.bucket) comp.outputs = { defaultBucket: first.bucket, ...(first.writeMode ? { defaultWriteMode: first.writeMode } : {}) };
    else delete comp.outputs;
    renderOutputs(box, comp, si, token);
  });
  box.querySelector('#om-exp').addEventListener('click', () => {
    if (comp.__outMode === 'explicit') return;
    setOutMode(comp, 'explicit');
    const b = (comp.outputs || {}).defaultBucket, m = (comp.outputs || {}).defaultWriteMode;
    comp.outputs = { tables: b ? [{ name: '', bucket: b, ...(m ? { writeMode: m } : {}) }] : [] };
    renderOutputs(box, comp, si, token);
  });

  const body = box.querySelector('#out-body');
  if (mode === 'dynamic') {
    body.innerHTML = `<p class="hint">The component decides table names at runtime (e.g. from its config); they all land in this bucket.</p>
      <div class="grid2">
        <label class="field"><b>bucket</b><input class="input input--mono" id="ob" value="${esc(o.defaultBucket || '')}" placeholder="raw" spellcheck="false"></label>
        <label class="field"><b>writeMode</b><select class="input" id="omw">
          <option value=""${!o.defaultWriteMode ? ' selected' : ''}>— default (FULL_LOAD) —</option>
          <option${o.defaultWriteMode === 'APPEND' ? ' selected' : ''}>APPEND</option>
          <option${o.defaultWriteMode === 'FULL_LOAD' ? ' selected' : ''}>FULL_LOAD</option></select></label>
      </div><p class="field" id="ochips"></p>`;
    const sync = () => {
      const b = body.querySelector('#ob').value.trim(), m = body.querySelector('#omw').value;
      if (!b && !m) delete comp.outputs;
      else comp.outputs = { ...(b ? { defaultBucket: b } : {}), ...(m ? { defaultWriteMode: m } : {}) };
      body.querySelector('#ochips').innerHTML = b ? `<span class="chip">${esc(b)} (dynamic)</span>` : '';
    };
    body.querySelector('#ob').addEventListener('input', sync);
    body.querySelector('#omw').addEventListener('change', sync);
    sync();
  } else {
    const tables = (comp.outputs && Array.isArray(comp.outputs.tables)) ? comp.outputs.tables : [];
    comp.outputs = { tables };
    body.innerHTML = `<p class="hint">One mapping per table — bucket + table name. Name a new table, or select an existing one to write into.</p>
      <div id="orows"></div>
      <div class="out-actions">
        <button type="button" class="btn" id="o-new">+ New table</button>
        <button type="button" class="btn" id="o-exist">Select existing…</button>
      </div>`;
    const orows = body.querySelector('#orows');
    tables.forEach((t, i) => {
      const r = document.createElement('div'); r.className = 'maprow';
      const bi = document.createElement('input'); bi.className = 'input input--mono'; bi.placeholder = 'bucket'; bi.value = t.bucket || ''; bi.spellcheck = false;
      const ti = document.createElement('input'); ti.className = 'input input--mono'; ti.placeholder = 'table name'; ti.value = t.name || ''; ti.spellcheck = false;
      const ws = document.createElement('select'); ws.className = 'input';
      ws.innerHTML = `<option value=""${!t.writeMode ? ' selected' : ''}>— default (FULL_LOAD) —</option>
        <option${t.writeMode === 'APPEND' ? ' selected' : ''}>APPEND</option>
        <option${t.writeMode === 'FULL_LOAD' ? ' selected' : ''}>FULL_LOAD</option>`;
      const rm = document.createElement('button'); rm.type = 'button'; rm.className = 'icon-btn'; rm.title = 'Remove mapping'; rm.textContent = '✕';
      bi.addEventListener('input', () => { t.bucket = bi.value.trim(); });
      ti.addEventListener('input', () => { t.name = ti.value.trim(); });
      ws.addEventListener('change', () => { if (ws.value) t.writeMode = ws.value; else delete t.writeMode; });
      rm.addEventListener('click', () => { tables.splice(i, 1); renderOutputs(box, comp, si, token); });
      r.append(bi, ti, ws, rm); orows.appendChild(r);
    });
    body.querySelector('#o-new').addEventListener('click', () => { tables.push({ name: '', bucket: '' }); renderOutputs(box, comp, si, token); });
    body.querySelector('#o-exist').addEventListener('click', () => {
      openOutputTablePicker(tables, si, token, () => renderOutputs(box, comp, si, token)).catch((e) => {
        if (token !== editorToken || isStale()) return;
        const host = document.getElementById('catalog-host');
        if (host) host.innerHTML = `<div class="catalog-back" id="pickback"><div class="catalog"><div class="callout callout--warn">${esc(String(e && e.message || e))}</div><button type="button" class="btn" id="pick-cancel">Close</button></div></div>`;
        const cancel = document.getElementById('pick-cancel');
        if (cancel) cancel.addEventListener('click', () => { host.innerHTML = ''; });
      });
    });
  }
}

// openOutputTablePicker gathers the two groups (upstream + storage) then
// hands off to openInputPicker — the same shared modal U4 built for the
// Inputs section's "+ Add input table…" (the RFC 027 UX mockup uses a single
// openTablePicker for both Inputs and Outputs) — so picking an existing table
// appends a {name, bucket} mapping to this component's explicit
// outputs.tables list. `token` guards against a stale async continuation
// after the editor rebuilt for a different component.
async function openOutputTablePicker(tables, si, token, onAdded) {
  const have = tables.map((t) => ({ bucket: t.bucket, table: t.name }));
  const host = document.getElementById('catalog-host');
  if (host) host.innerHTML = `<div class="catalog-back" id="pickback"><div class="catalog"><p aria-busy="true">Loading tables…</p></div></div>`;
  const [upstreamRows, storageRows] = await Promise.all([upstreamTables(si), storageTableRows()]);
  if (token !== editorToken || isStale()) return;
  openInputPicker(upstreamRows, storageRows, have, (list) => {
    for (const x of list) tables.push({ name: x.table, bucket: x.bucket });
    onAdded();
  });
}

async function buildConfigForm(comp, token) {
  let detail;
  try {
    detail = await getComponentDetail(comp.component);
  } catch {
    if (token !== editorToken || isStale()) return;
    // Unknown component (not in the registry): its config can't be shown as a
    // form, but the stored config still rides along on save (§6).
    const vwrap = document.querySelector('#editor .ed-version');
    if (vwrap) vwrap.style.display = 'none';
    const form = document.getElementById('cfg-form');
    if (form) {
      form.innerHTML = `<div class="callout callout--warn">Component <code class="inline">${esc(comp.component)}</code> is not in the registry, so its config can't be edited as a form here. Its stored config is preserved on save.</div>`;
    }
    return;
  }
  if (token !== editorToken || isStale()) return;

  const versions = (detail && detail.versions) || [];
  const defaultV = pickDocVersion(detail);
  const selectedV = comp.version || (defaultV && defaultV.version) || '';

  const vsel = document.getElementById('ed-version');
  if (vsel) {
    vsel.innerHTML = versions.length
      ? versions.map((v) => `<option value="${esc(v.version)}"${v.version === selectedV ? ' selected' : ''}>${esc(v.version)}${v.prerelease ? ' (prerelease)' : ''}</option>`).join('')
      : `<option value="">—</option>`;
    vsel.addEventListener('change', () => {
      // Preserve config across the version switch (schema may differ), then
      // rebuild against the newly-selected version's schema.
      commitForm();
      comp.version = vsel.value;
      renderEditor();
    });
  }

  const schemaStr = schemaOf(detail, selectedV);
  const formEl = document.getElementById('cfg-form');
  if (!formEl) return;
  formEl.innerHTML = '';
  const handle = buildSchemaForm(
    formEl,
    schemaStr,
    comp.config || {},
    { secretKeys, listSecretsFn: () => listSecrets(pid) },
  );
  activeForm = { handle, comp, schemaStr };
  // buildSchemaForm has no change callback — read it on every input/change so
  // doc stays live (Save/Validate/YAML mode all read the current doc).
  const sync = () => { comp.config = { ...unrenderedKeys(comp.config, schemaStr), ...handle.getValue() }; };
  formEl.addEventListener('input', sync);
  formEl.addEventListener('change', sync);
}

// ---- Mode toggle (Form ⇄ YAML) --------------------------------------------
//
// The doc is the ONLY bridge between the two surfaces (RFC 027 §6). There is
// no hand-rolled serializer: →YAML dumps the live in-memory doc (reflecting
// unsaved form edits, so commitForm() first) via js-yaml; →form parses the
// textarea's current text back onto the doc WHOLESALE. A parse error keeps us
// in YAML mode with the message shown inline and the user's text untouched —
// nothing lost, nothing auto-corrected.
export function setMode(m) {
  if (m === mode) return;
  if (m === 'yaml') {
    // Flush any live form edits into the doc, then serialize it verbatim.
    commitForm();
    const ta = document.getElementById('yaml-ta');
    if (ta) ta.value = yamlDump(doc, { noRefs: true, sortKeys: false });
    const err = document.getElementById('yaml-err');
    if (err) err.innerHTML = '';
    applyMode('yaml');
    return;
  }
  // →form: parse the textarea. On failure, STAY in YAML (don't switch, don't
  // touch the text) and surface the parser's message.
  const ta = document.getElementById('yaml-ta');
  const err = document.getElementById('yaml-err');
  let parsed;
  try {
    parsed = yamlLoad(ta ? ta.value : '');
  } catch (e) {
    if (err) err.innerHTML = `<div class="callout callout--warn">YAML parse error: ${esc(String(e && e.message || e))}</div>`;
    return; // stay in YAML mode; textarea text is left exactly as-is
  }
  if (err) err.innerHTML = '';
  // Wholesale replace — the user may have restructured the doc in ways a merge
  // couldn't reconcile. normalizeDoc guarantees the shape the renderers assume
  // (and coerces a scalar/empty document to an empty doc).
  doc = normalizeDoc(parsed);
  sel = null;
  activeForm = null;
  // Keep the (new-pipeline) name field in sync with the parsed doc.
  const nameInput = document.getElementById('builder-name');
  if (nameInput) nameInput.value = doc.name || '';
  applyMode('form');
  renderGateway();
  renderOutline();
  renderEditor();
}

// applyMode flips the visible surface and the toggle's pressed state.
function applyMode(m) {
  mode = m;
  const formBtn = document.getElementById('mode-form');
  const yamlBtn = document.getElementById('mode-yaml');
  const formView = document.getElementById('builder-form-view');
  const yamlView = document.getElementById('builder-yaml-view');
  if (formBtn) { formBtn.classList.toggle('on', m === 'form'); formBtn.setAttribute('aria-selected', String(m === 'form')); }
  if (yamlBtn) { yamlBtn.classList.toggle('on', m === 'yaml'); yamlBtn.setAttribute('aria-selected', String(m === 'yaml')); }
  if (formView) formView.style.display = m === 'form' ? '' : 'none';
  if (yamlView) yamlView.style.display = m === 'yaml' ? '' : 'none';
}

// ---- Save / Validate ------------------------------------------------------

export async function saveDoc() {
  const msg = document.getElementById('builder-msg');
  // In YAML mode the textarea is authoritative; the server parses/validates
  // it (S6 content negotiation). Do NOT commitForm (the hidden form is stale)
  // and do NOT re-serialize — PUT the user's literal YAML text.
  const yamlMode = mode === 'yaml';
  if (!yamlMode) commitForm();
  const targetName = isNew ? String(doc.name || '').trim() : pipelineName;
  if (!targetName) {
    if (msg) msg.innerHTML = `<div class="callout callout--warn">Name is required.</div>`;
    return;
  }
  const btn = document.getElementById('builder-save');
  if (btn) btn.disabled = true;
  try {
    const res = yamlMode
      ? await putPipelineYAML(pid, targetName, document.getElementById('yaml-ta').value)
      : await putPipelineDoc(pid, targetName, doc);
    if (res.ok && (!res.findings || res.findings.length === 0)) {
      msg.innerHTML = `<div class="callout">Saved.</div>`;
    } else if (res.ok) {
      msg.innerHTML = `<div class="callout">Saved with warnings.</div>` + renderFindings(res.findings);
    } else {
      msg.innerHTML = renderFindings(res.findings);
    }
    if (res.ok && isNew) {
      // Fresh create → jump to the detail view for that name.
      window.history.replaceState({}, '', `/ui/pipelines/${encodeURIComponent(targetName)}`);
      if (typeof window.renderRoute === 'function') window.renderRoute();
    }
  } catch (err) {
    if (String(err.message) !== 'not authenticated') {
      msg.innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
    }
  } finally {
    if (btn) btn.disabled = false;
  }
}

export async function validateDoc() {
  // Mode-aware, mirroring saveDoc: YAML mode sends the raw textarea text with
  // application/yaml (server is authoritative); form mode POSTs the doc as
  // JSON. The validate endpoint negotiates both (S6/S7).
  const yamlMode = mode === 'yaml';
  if (!yamlMode) commitForm();
  const msg = document.getElementById('builder-msg');
  const targetName = isNew ? String(doc.name || '').trim() : pipelineName;
  const btn = document.getElementById('builder-validate');
  if (btn) btn.disabled = true;
  try {
    const qs = targetName ? `?name=${encodeURIComponent(targetName)}` : '';
    const r = await fetch(
      `/api/v1/projects/${encodeURIComponent(pid)}/pipelines/validate${qs}`,
      {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': yamlMode ? 'application/yaml' : 'application/json' },
        body: yamlMode ? document.getElementById('yaml-ta').value : JSON.stringify(doc),
      },
    );
    if (r.status === 401) {
      if (typeof window.__datupletGoToLogin === 'function') window.__datupletGoToLogin();
      throw new Error('not authenticated');
    }
    const ct = r.headers.get('content-type') || '';
    const raw = await r.text();
    let body = null;
    if (ct.includes('application/json') && raw) {
      try { body = JSON.parse(raw); } catch { /* leave null */ }
    }
    const findings = body && Array.isArray(body.findings) ? body.findings : null;
    // A non-2xx WITHOUT a findings body is a genuine transport/server error.
    if (!r.ok && !findings) {
      throw new Error(`${r.status}: ${(body && body.error) || raw || r.statusText}`);
    }
    if (!findings || findings.length === 0) {
      msg.innerHTML = `<div class="callout">Valid — no errors or warnings.</div>`;
    } else {
      msg.innerHTML = renderFindings(findings);
    }
  } catch (err) {
    if (String(err.message) !== 'not authenticated') {
      msg.innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
    }
  } finally {
    if (btn) btn.disabled = false;
  }
}

// putPipelineDoc PUTs the full doc as application/json and resolves the RFC
// 026 §7 findings contract as data (mirrors api.js putPipelineYAML, but JSON):
//   { ok:true }                  — 204 clean save
//   { ok:true,  findings:[...] } — 200 saved with warnings
//   { ok:false, findings:[...] } — 400 rejected
// Throws on 401 (→ login redirect) and on any other non-2xx with no findings
// body (so genuine server/transport errors still surface).
async function putPipelineDoc(projectId, name, docObj) {
  const r = await fetch(
    `/api/v1/projects/${encodeURIComponent(projectId)}/pipelines/${encodeURIComponent(name)}`,
    {
      method: 'PUT',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(docObj),
    },
  );
  if (r.status === 401) {
    if (typeof window.__datupletGoToLogin === 'function') window.__datupletGoToLogin();
    throw new Error('not authenticated');
  }
  if (r.status === 204) return { ok: true };
  const ct = r.headers.get('content-type') || '';
  const raw = await r.text();
  let body = null;
  if (ct.includes('application/json') && raw) {
    try { body = JSON.parse(raw); } catch { /* leave null */ }
  }
  const findings = body && Array.isArray(body.findings) ? body.findings : null;
  if (r.status === 200) return { ok: true, findings: findings || [] };
  if (r.status === 400 && findings) return { ok: false, findings };
  throw new Error(`${r.status}: ${(body && body.error) || raw || r.statusText}`);
}

// ---- Recent runs (kept verbatim from the previous page) -------------------

// loadRecentRuns fetches the pipeline's most recent runs via the paged runs
// API (filtered by pipeline_id) and renders a compact table into
// #pipeline-runs. Fired once on page load — no live poll here.
async function loadRecentRuns(pid, pipelineID) {
  const resp = await api(`/api/v1/projects/${encodeURIComponent(pid)}/runs?pipeline_id=${encodeURIComponent(pipelineID)}&limit=10`);
  const container = document.getElementById('pipeline-runs');
  if (!container) return; // navigated away
  const runs = resp.runs || [];
  if (runs.length === 0) {
    container.innerHTML = '<p><small>No runs yet.</small></p>';
    return;
  }
  const rows = runs.map((r) => {
    const id = String(r.id || '');
    const href = `/ui/runs/${encodeURIComponent(id)}`;
    const pill = phaseToPillClass(r.phase);
    const started = r.started_at ? timeTag(r.started_at) : '<span class="muted">—</span>';
    const dur = r.completed_at || r.started_at
      ? formatDuration(durationFrom(r.started_at, r.completed_at))
      : '—';
    return `
      <tr>
        <td><a href="${href}"><code class="mono">${esc(id.slice(0, 8))}</code></a></td>
        <td><span class="pill ${pill}">${esc(r.phase)}</span></td>
        <td>${started}</td>
        <td>${dur}</td>
      </tr>`;
  }).join('');
  const viewAll = resp.next_cursor
    ? `<p><a href="/ui/runs">View all &rarr;</a></p>`
    : '';
  container.innerHTML = `
    <table class="table">
      <thead>
        <tr>
          <th>Run ID</th>
          <th>Phase</th>
          <th>Started</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    ${viewAll}
  `;
}
