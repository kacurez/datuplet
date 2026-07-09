// Components catalog (RFC 026 §4.7 item 1). Two views off one route:
//   /ui/components            → list of registered ComponentDefinitions
//   /ui/components?name=<n>   → detail: versions table + rendered config docs
//
// The catalog is readable by any authenticated project member — it IS the
// shared component picker. Deprecated components get a badge but stay listed.

import { esc, getComponents, getComponent } from '/ui/api.js';
import { emptyState, skeletonRows, spinner } from '/ui/components.js';
import * as icons from '/ui/icons.js';

export async function renderComponents(ctx) {
  const head = document.getElementById('page-head');
  const app = document.getElementById('app');
  const path = window.location.pathname;
  const search = window.location.search;
  const aborted = () => window.location.pathname !== path || window.location.search !== search;

  const selected = new URLSearchParams(search).get('name');
  if (selected) {
    await renderDetail(head, app, selected, aborted);
  } else {
    await renderList(head, app, aborted);
  }
}

async function renderList(head, app, aborted) {
  head.innerHTML = `
    <h1>Components</h1>
    <div class="actions">
      <input class="input" data-role="page-search" type="search" placeholder="Filter components…" />
    </div>`;
  app.innerHTML = `<table class="table"><thead><tr>
      <th>Component</th><th>Description</th><th>Default version</th><th></th>
    </tr></thead><tbody>${skeletonRows(4, 4)}</tbody></table>`;

  let comps;
  try {
    comps = await getComponents();
    if (aborted()) return;
  } catch (e) {
    if (aborted()) return;
    if (String(e.message) === 'not authenticated') return;
    app.innerHTML = `<p style="color:var(--status-fail-fg);">Failed to load components: ${esc(e.message)}</p>`;
    return;
  }
  if (!Array.isArray(comps) || comps.length === 0) {
    app.innerHTML = emptyState({ icon: icons.database, text: 'No components registered.' });
    return;
  }

  const rows = (list) => list.map(rowHTML).join('')
    || `<tr><td colspan="4" style="color:var(--fg-2);text-align:center;padding:var(--s-5);">No components match.</td></tr>`;
  app.innerHTML = `<table class="table"><thead><tr>
      <th>Component</th><th>Description</th><th>Default version</th><th></th>
    </tr></thead><tbody id="comp-body">${rows(comps)}</tbody></table>`;

  const searchInput = head.querySelector('[data-role="page-search"]');
  searchInput?.addEventListener('input', () => {
    const q = searchInput.value.trim().toLowerCase();
    const filtered = q ? comps.filter((c) =>
      `${c.name} ${c.displayName || ''} ${c.description || ''}`.toLowerCase().includes(q)) : comps;
    document.getElementById('comp-body').innerHTML = rows(filtered);
  });
}

function rowHTML(c) {
  const dep = c.deprecated ? ` <span class="badge badge--deprecated">deprecated</span>` : '';
  const href = `/ui/components?name=${encodeURIComponent(c.name)}`;
  const def = c.defaultVersion || (c.versions && c.versions.length ? '(latest)' : '—');
  return `<tr>
    <td><a href="${href}"><code class="inline">${esc(c.name)}</code></a>${dep}</td>
    <td>${esc(c.description || c.displayName || '')}</td>
    <td><code class="mono">${esc(def)}</code></td>
    <td><a class="btn btn--ghost" href="${href}">Details</a></td>
  </tr>`;
}

async function renderDetail(head, app, name, aborted) {
  head.innerHTML = `
    <h1><code class="inline">${esc(name)}</code></h1>
    <div class="actions"><a class="btn btn--secondary" href="/ui/components">Back to catalog</a></div>`;
  app.innerHTML = `<p aria-busy="true">${spinner()} Loading…</p>`;

  let c;
  try {
    c = await getComponent(name);
    if (aborted()) return;
  } catch (e) {
    if (aborted()) return;
    if (String(e.message) === 'not authenticated') return;
    app.innerHTML = `<p style="color:var(--status-fail-fg);">Failed to load ${esc(name)}: ${esc(e.message)}</p>`;
    return;
  }

  const dep = c.deprecated
    ? `<div class="callout callout--warn">${icons.alertTriangle || ''} This component is deprecated.</div>` : '';
  const versions = Array.isArray(c.versions) ? c.versions : [];
  const versionRows = versions.map((v) => `
    <tr>
      <td><code class="mono">${esc(v.version)}</code>${v.version === c.defaultVersion ? ' <span class="badge">default</span>' : ''}</td>
      <td>${v.prerelease ? '<span class="badge badge--pre">prerelease</span>' : 'stable'}</td>
      <td><code class="mono">${esc(v.image)}</code></td>
    </tr>`).join('') || `<tr><td colspan="3" style="color:var(--fg-2);">No versions.</td></tr>`;

  // Config docs come from the default (or first) version's schema.
  const docVersion = versions.find((v) => v.version === c.defaultVersion)
    || versions.find((v) => !v.prerelease) || versions[0];
  const docsHTML = docVersion && docVersion.configSchema
    ? schemaDocs(docVersion.configSchema)
    : `<p style="color:var(--fg-2);">No config schema.</p>`;

  app.innerHTML = `
    ${dep}
    <p>${esc(c.description || '')}</p>
    <h3 class="section-h">Versions</h3>
    <table class="table"><thead><tr><th>Version</th><th>Channel</th><th>Image</th></tr></thead>
      <tbody>${versionRows}</tbody></table>
    <h3 class="section-h">Config schema${docVersion ? ` <code class="inline">${esc(docVersion.version)}</code>` : ''}</h3>
    ${docsHTML}`;
}

// schemaDocs renders a JSON Schema (draft 2020-12) string as a read-only
// definition list of its top-level properties. Best-effort: unparseable or
// non-object schemas degrade to a note. Never throws.
//
// Exported so the pipeline builder's docs panel renders the same schema view
// as the catalog (RFC 026 Phase 4). typeSummary stays module-private — it's
// only reached through schemaDocs.
export function schemaDocs(schemaStr) {
  let schema;
  try {
    schema = JSON.parse(schemaStr);
  } catch {
    return `<p style="color:var(--status-fail-fg);">Config schema is not valid JSON.</p>`;
  }
  if (!schema || typeof schema !== 'object') {
    return `<p style="color:var(--fg-2);">Config schema is empty.</p>`;
  }
  const props = schema.properties && typeof schema.properties === 'object' ? schema.properties : {};
  const required = Array.isArray(schema.required) ? schema.required : [];
  const keys = Object.keys(props);
  if (keys.length === 0) {
    return `<p style="color:var(--fg-2);">This component accepts arbitrary config (no declared properties).</p>`;
  }
  const rows = keys.map((k) => {
    const p = props[k] || {};
    const isReq = required.includes(k);
    const secret = p['x-datuplet-secret'] === true;
    const enumVals = Array.isArray(p.enum)
      ? ` <span class="schema-enum">one of: ${p.enum.map((e) => esc(String(e))).join(', ')}</span>` : '';
    return `
      <div class="schema-prop">
        <div class="schema-prop-head">
          <code class="mono">${esc(k)}</code>
          <span class="schema-type">${esc(typeSummary(p))}</span>
          ${isReq ? '<span class="badge badge--req">required</span>' : ''}
          ${secret ? '<span class="badge badge--secret">secret</span>' : ''}
        </div>
        ${p.description ? `<div class="schema-desc">${esc(p.description)}</div>` : ''}
        ${enumVals}
      </div>`;
  }).join('');
  const extra = schema.additionalProperties === false
    ? '' : `<p class="schema-note">Additional properties are allowed.</p>`;
  return `<div class="schema-docs">${rows}${extra}</div>`;
}

// typeSummary → a short human string for a property schema node.
function typeSummary(p) {
  if (p.enum) return 'enum';
  const t = p.type;
  if (t === 'array') {
    const items = p.items && p.items.type ? p.items.type : 'any';
    return `array<${items}>`;
  }
  if (Array.isArray(t)) return t.join(' | ');
  return t || 'any';
}
