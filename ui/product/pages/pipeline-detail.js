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

import { api, esc, getComponents, getComponent, listSecrets } from '/ui/api.js';
import { timeTag, phaseToPillClass, formatDuration, durationFrom } from '/ui/format.js';
import { renderFindings } from '/ui/lib/findings.js';
import { buildSchemaForm } from '/ui/lib/schema-form.js';

// ---- Module-level builder state (state contract for U4–U6) ----------------
// `doc` is the FULL stored doc — see the hidden-subtree note above. `sel` is
// the selected {stage, component} indices (or null). The rest is page context
// the module-level render functions close over.
let doc = { name: '', stages: [{ name: 'stage-1', components: [] }] };
let sel = null;

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
      <div class="builder-topbar-actions">
        <button type="button" class="btn btn--secondary" id="builder-validate">Validate</button>
        <button type="button" class="btn btn--primary" id="builder-save">Save</button>
      </div>
    </div>
    <div id="builder-msg" class="builder-msg"></div>
    <div class="builder-shell">
      <div class="builder-outline">
        <details class="pipeline-settings">
          <summary>Pipeline settings</summary>
          <div class="pipeline-settings-body" id="pipeline-settings-body"></div>
        </details>
        <div id="outline"></div>
      </div>
      <div class="builder-editor" id="editor"></div>
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

// ---- Save / Validate ------------------------------------------------------

export async function saveDoc() {
  commitForm();
  const msg = document.getElementById('builder-msg');
  const targetName = isNew ? String(doc.name || '').trim() : pipelineName;
  if (!targetName) {
    if (msg) msg.innerHTML = `<div class="callout callout--warn">Name is required.</div>`;
    return;
  }
  const btn = document.getElementById('builder-save');
  if (btn) btn.disabled = true;
  try {
    const res = await putPipelineDoc(pid, targetName, doc);
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
  commitForm();
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
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(doc),
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
