// pages/storage-table.js — Info / Schema / Preview tabs for one
// Iceberg table at /ui/storage/t/{ns}/{name}?tab=info|schema|preview.
//
// Tabs fetch independently on activation. URL deep-link is supported
// via the ?tab= query string. Default tab is info.
//
// The router (app.js renderRoute) passes ctx = { params: [...] } where
// params[0] = namespace, params[1] = name.

import { esc, getTableInfo, getTableSchema, getTablePreview } from '/ui/api.js';
import { skeletonRows } from '/ui/components.js';
import { timeTag } from '/ui/format.js';
import * as icons from '/ui/icons.js';
import { renderSnapshotHistory } from '/ui/storage-snapshots.js';

const TABS = ['info', 'schema', 'preview'];

// Per-page generation counter — incremented on every renderActiveTab
// call so a slow earlier fetch can detect that it's been superseded
// and skip its DOM write. Without this, rapid tab switches can let
// an earlier response land last and clobber the active tab body.
let _tabGen = 0;

// Path captured at renderStorageTable entry. Tab fetches check this
// after every await so a response that lands after the user has
// navigated to a different route doesn't clobber the destination
// page. _tabGen handles same-page tab switches; _aborted() handles
// full route changes. Module scope mirrors the existing _tabGen
// shape and lets the per-tab renderers see it via closure.
let _path = '';
const _aborted = () => window.location.pathname !== _path;

export async function renderStorageTable(ctx) {
  _path = window.location.pathname;
  // Router preserves URL-encoded path segments in ctx.params; the
  // api.js helpers re-encode before fetching, so without this decode
  // we'd send double-encoded paths (e.g. %2520) and also display the
  // encoded form in the breadcrumb. Decode once at entry, pass the
  // raw values from here on.
  const ns = decodeURIComponent(ctx.params[0]);
  const name = decodeURIComponent(ctx.params[1]);
  const projectId = window.__datupletActiveProjectID;
  if (!projectId) {
    document.getElementById('page-head').innerHTML = '<h1>Storage</h1>';
    document.getElementById('app').innerHTML = '<p>Pick a project to view tables.</p>';
    return;
  }

  const initialTab = readTabFromURL();
  renderHeader(ns, name, initialTab);
  await renderActiveTab(projectId, ns, name, initialTab);
}

function readTabFromURL() {
  const params = new URL(window.location.href).searchParams;
  const t = params.get('tab') || 'info';
  return TABS.includes(t) ? t : 'info';
}

function renderHeader(ns, name, activeTab) {
  const head = document.getElementById('page-head');
  head.innerHTML = `
    <h1><a href="/ui/storage" class="muted">Storage</a> / <code class="mono">${esc(ns)}.${esc(name)}</code></h1>
    <div class="actions" id="tab-nav">
      ${TABS.map((t) => {
        const cls = t === activeTab ? 'btn btn--secondary' : 'btn btn--ghost';
        const label = t.charAt(0).toUpperCase() + t.slice(1);
        return `<button class="${cls}" data-tab="${t}">${esc(label)}</button>`;
      }).join('')}
    </div>
  `;
  document.getElementById('tab-nav').addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-tab]');
    if (!btn) return;
    const tab = btn.dataset.tab;
    if (!TABS.includes(tab)) return;
    const url = new URL(window.location.href);
    url.searchParams.set('tab', tab);
    // Cheap tab switch — keep the route, just update the query and
    // rerender the body. Avoid renderRoute() so we don't re-run nav.
    window.history.replaceState({}, '', url.pathname + url.search);
    renderHeader(ns, name, tab);
    const projectId = window.__datupletActiveProjectID;
    renderActiveTab(projectId, ns, name, tab);
  });
}

async function renderActiveTab(projectId, ns, name, tab) {
  const myGen = ++_tabGen;
  const app = document.getElementById('app');
  if (tab === 'info')    return renderInfo(myGen, app, projectId, ns, name);
  if (tab === 'schema')  return renderSchema(myGen, app, projectId, ns, name);
  if (tab === 'preview') return renderPreview(myGen, app, projectId, ns, name);
}

async function renderInfo(gen, app, projectId, ns, name) {
  app.innerHTML = '<p style="color: var(--fg-2);">Loading…</p>';
  let info;
  try {
    info = await getTableInfo(projectId, ns, name);
  } catch (e) {
    if (_aborted() || gen !== _tabGen) return; // stale — superseded fetch or route change
    app.innerHTML = `<p style="color: var(--status-fail-fg);">Failed to load: ${esc(e.message)}</p>`;
    return;
  }
  if (_aborted() || gen !== _tabGen) return; // stale — superseded fetch or route change
  const kv = `
    <dl class="kv">
      <dt>Metadata location</dt><dd>${esc(info.metadata_location || '')}</dd>
      <dt>Current snapshot</dt><dd>${esc(String(info.current_snapshot_id ?? ''))}</dd>
      <dt>Snapshot count</dt><dd>${(info.snapshots || []).length}</dd>
      <dt>Data files</dt><dd>${info.data_file_count == null ? '—' : esc(String(info.data_file_count))}</dd>
      <dt>Row count</dt><dd>${info.row_count == null ? '—' : esc(String(info.row_count))}</dd>
    </dl>
  `;

  const snapshots = info.snapshots || [];
  let snapshotTable = '';
  if (snapshots.length > 0) {
    snapshotTable = `
      <h2 style="font-size: var(--text-md); margin-bottom: var(--s-3); color: var(--fg-1);">Snapshots</h2>
      <table class="table">
        <thead><tr><th>ID</th><th>Parent</th><th>Operation</th><th>Time</th></tr></thead>
        <tbody>
          ${snapshots.map((s) => `
            <tr>
              <td><code class="mono">${esc(String(s.id))}</code></td>
              <td><code class="mono">${esc(String(s.parent_id ?? '—'))}</code></td>
              <td>${esc(s.operation || '')}</td>
              <td>${timeTag(new Date(s.timestamp_ms || 0).toISOString())}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  }

  app.innerHTML = kv + snapshotTable;

  // Append the RFC-013 audit-key snapshot history section after the
  // existing summary. renderSnapshotHistory appends to `app` rather than
  // replacing it, so the KV block + legacy snapshot table stay visible.
  await renderSnapshotHistory(app, projectId, ns, name, () => _aborted() || gen !== _tabGen);
}

async function renderSchema(gen, app, projectId, ns, name) {
  app.innerHTML = `<table class="table"><thead><tr><th>#</th><th>ID</th><th>Name</th><th>Type</th><th>Required</th></tr></thead><tbody>${skeletonRows(5, 5)}</tbody></table>`;
  let resp;
  try {
    resp = await getTableSchema(projectId, ns, name);
  } catch (e) {
    if (_aborted() || gen !== _tabGen) return; // stale — superseded fetch or route change
    app.innerHTML = `<p style="color: var(--status-fail-fg);">Failed to load: ${esc(e.message)}</p>`;
    return;
  }
  if (_aborted() || gen !== _tabGen) return; // stale — superseded fetch or route change
  const cols = resp.columns || [];
  app.innerHTML = `
    <table class="table">
      <thead><tr><th>#</th><th>ID</th><th>Name</th><th>Type</th><th>Required</th></tr></thead>
      <tbody>
        ${cols.map((c, i) => `
          <tr>
            <td>${i + 1}</td>
            <td><code class="mono">${esc(String(c.id))}</code></td>
            <td><code class="mono">${esc(c.name)}</code></td>
            <td><code class="mono">${esc(c.type)}</code></td>
            <td>${c.nullable
              ? `<span style="color:var(--fg-2);" title="nullable">${icons.x}</span>`
              : `<span style="color:var(--status-ok-fg);" title="required">${icons.check}</span>`}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
}

async function renderPreview(gen, app, projectId, ns, name) {
  app.innerHTML = `<table class="table"><tbody>${skeletonRows(5, 4)}</tbody></table>`;
  let resp;
  try {
    resp = await getTablePreview(projectId, ns, name);
  } catch (e) {
    if (_aborted() || gen !== _tabGen) return; // stale — superseded fetch or route change
    if (e.kind === 'query_disabled') {
      app.innerHTML = '<div class="callout callout--warn">Preview needs the query service. Ask your operator to set <code>queryWorker.enabled=true</code>.</div>';
      return;
    }
    if (e.status === 429 || e.status === 503) {
      app.innerHTML = '<div class="callout callout--warn">The query service is busy — try again in a moment.</div>';
      return;
    }
    if (e.kind === 'result_too_large') {
      app.innerHTML = `<p style="color: var(--fg-2);">${esc(e.message)}</p>`;
      return;
    }
    app.innerHTML = `<p style="color: var(--status-fail-fg);">Failed to load: ${esc(e.message)}</p>`;
    return;
  }
  if (_aborted() || gen !== _tabGen) return; // stale — superseded fetch or route change
  const cols = resp.columns || [];
  const rows = resp.rows || [];
  const truncated = !!resp.truncated;
  const callout = truncated
    ? `<div class="callout callout--warn">Preview truncated to ${rows.length} rows. Use a sandbox query for full data.</div>`
    : '';

  if (rows.length === 0) {
    app.innerHTML = callout + '<p style="color: var(--fg-2);">No rows in this table.</p>';
    return;
  }

  app.innerHTML = `
    ${callout}
    <table class="table">
      <thead>
        <tr>${cols.map((c) => `<th><code class="mono">${esc(c.name)}</code><div style="color:var(--fg-2);font-weight:400;">${esc(c.type)}</div></th>`).join('')}</tr>
      </thead>
      <tbody>
        ${rows.map((r) => `
          <tr>${r.map((cell) => {
            if (cell === null || cell === undefined) {
              return `<td style="color:var(--fg-2);font-style:italic;">null</td>`;
            }
            return `<td>${esc(typeof cell === 'object' ? JSON.stringify(cell) : String(cell))}</td>`;
          }).join('')}</tr>
        `).join('')}
      </tbody>
    </table>
  `;
}
