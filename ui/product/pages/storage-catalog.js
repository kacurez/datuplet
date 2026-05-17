// Storage catalog: list of Iceberg tables for the active project.
//
// Hits GET /api/v1/storage/projects/{pid}/tables (RFC 005 A1) and
// renders a dense table. Each Name cell is a real <a> linking to the
// table-detail page (lands in C-3) so keyboard + middle-click + open-
// in-new-tab work. Mouse row-click is a convenience handler that
// skips when the click target is itself a control (matches B-7's
// accessibility fix).
//
// Filter is a simple substring match on `namespace.name` driven by an
// input the global '/' hotkey can focus via [data-role="page-search"].

import { esc, getStorageCatalog } from '/ui/api.js';
import { emptyState, skeletonRows } from '/ui/components.js';
import * as icons from '/ui/icons.js';

export async function renderStorageCatalog() {
  const head = document.getElementById('page-head');
  const app = document.getElementById('app');
  const projectId = window.__datupletActiveProjectID;

  // Snapshot the path we started rendering for. If the user navigates
  // away while getStorageCatalog is in flight, the late response would
  // otherwise overwrite #app on the destination page. Mirrors the
  // pattern app.js renderRoute uses around its own awaits.
  const path = window.location.pathname;
  const aborted = () => window.location.pathname !== path;

  head.innerHTML = `
    <h1>Storage</h1>
    <div class="actions">
      <input class="input" data-role="page-search" type="search" placeholder="Filter tables…" />
    </div>
  `;

  if (!projectId) {
    app.innerHTML = emptyState({
      icon: icons.database,
      text: 'Pick a project to browse its tables.',
    });
    return;
  }

  // Show loading state while the fetch is in flight.
  app.innerHTML = `
    <table class="table">
      <thead>
        <tr><th>Namespace</th><th>Name</th><th>Snapshot</th></tr>
      </thead>
      <tbody>${skeletonRows(5, 3)}</tbody>
    </table>
  `;

  let tables;
  try {
    const resp = await getStorageCatalog(projectId);
    if (aborted()) return;
    tables = Array.isArray(resp.tables) ? resp.tables : [];
  } catch (e) {
    if (aborted()) return;
    app.innerHTML = `<p style="color: var(--status-fail-fg);">Failed to load tables: ${esc(e.message)}</p>`;
    return;
  }

  if (tables.length === 0) {
    app.innerHTML = emptyState({
      icon: icons.database,
      text: 'No tables yet. Run a pipeline to populate the warehouse.',
    });
    return;
  }

  const renderRows = (rows) => rows.map((t) => {
    const href = `/ui/storage/t/${encodeURIComponent(t.namespace)}/${encodeURIComponent(t.name)}`;
    return `
      <tr>
        <td>${esc(t.namespace)}</td>
        <td><a href="${href}"><code class="inline">${esc(t.name)}</code></a></td>
        <td><code class="mono">${esc(String(t.current_snapshot_id ?? ''))}</code></td>
      </tr>`;
  }).join('') || `<tr><td colspan="3" style="color: var(--fg-2); text-align:center; padding: var(--s-5);">No tables match.</td></tr>`;

  app.innerHTML = `
    <table class="table">
      <thead>
        <tr><th>Namespace</th><th>Name</th><th>Snapshot</th></tr>
      </thead>
      <tbody id="tables-body">${renderRows(tables)}</tbody>
    </table>
  `;

  const searchInput = head.querySelector('[data-role="page-search"]');
  searchInput.addEventListener('input', () => {
    const q = searchInput.value.trim().toLowerCase();
    const filtered = q
      ? tables.filter((t) => `${t.namespace}.${t.name}`.toLowerCase().includes(q))
      : tables;
    document.getElementById('tables-body').innerHTML = renderRows(filtered);
  });

  const tbody = document.getElementById('tables-body');
  tbody.addEventListener('click', (e) => {
    if (e.target.closest('a, button, input, select')) return;
    const row = e.target.closest('tr');
    if (!row) return;
    const link = row.querySelector('a[href^="/ui/storage/t/"]');
    if (!link) return;
    window.history.pushState({}, '', link.getAttribute('href'));
    window.renderRoute();
  });
}
