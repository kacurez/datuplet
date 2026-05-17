// Runs list for the active project. Server returns the 100 most recent
// runs (store.ListRunsForProject limit=100). The table auto-refreshes
// every 5 seconds so a fresh trigger appears without a manual reload.
// The interval is cleared by the router before navigating away (see
// window.__datupletPoller below) so we don't leak pollers across
// navigations.

import { api, esc } from '/ui/api.js';
import { timeTag, phaseToPillClass } from '/ui/format.js';
import * as icons from '/ui/icons.js';
import { emptyState, skeletonRows } from '/ui/components.js';

// Runs table has 5 columns: Run ID, Stage, Phase, Started, Pipeline.
const RUNS_COLS = 5;

function clearExistingPoller() {
  if (window.__datupletPoller) {
    clearInterval(window.__datupletPoller);
    window.__datupletPoller = null;
  }
}

export async function renderRuns() {
  clearExistingPoller();
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  const pid = window.__datupletActiveProjectID;
  if (!pid) {
    if (head) head.innerHTML = '';
    app.innerHTML = `<p>No active project.</p>`;
    return;
  }

  if (head) {
    head.innerHTML = `
      <h1>Runs</h1>
      <div class="actions">
        <input type="search" class="input" placeholder="Filter…" data-role="page-search" id="runs-filter" />
      </div>
    `;
  }

  let lastRuns = [];
  let filterText = '';

  function matchesFilter(r) {
    if (!filterText) return true;
    const f = filterText.toLowerCase();
    return (
      String(r.id || '').toLowerCase().includes(f) ||
      String(r.phase || '').toLowerCase().includes(f) ||
      String(r.current_stage || '').toLowerCase().includes(f) ||
      String(r.pipeline_id || '').toLowerCase().includes(f)
    );
  }

  function renderRows(runs) {
    const filtered = runs.filter(matchesFilter);
    if (filtered.length === 0) {
      const text = runs.length === 0 ? 'No runs yet.' : 'No runs match filter.';
      return `<tr><td colspan="${RUNS_COLS}">${emptyState({ icon: icons.activity, text })}</td></tr>`;
    }
    return filtered.map((r) => {
      const id = String(r.id || '');
      const short = id.slice(0, 8);
      const href = `/ui/runs/${encodeURIComponent(id)}`;
      const pill = phaseToPillClass(r.phase);
      return `
        <tr data-href="${href}">
          <td><a href="${href}"><code class="mono">${esc(short)}</code></a></td>
          <td>${esc(r.current_stage || '')}</td>
          <td><span class="pill ${pill}">${esc(r.phase)}</span></td>
          <td>${timeTag(r.created_at)}</td>
          <td>${esc(r.pipeline_id || '')}</td>
        </tr>
      `;
    }).join('');
  }

  function paintRows() {
    const tbody = document.getElementById('runs-tbody');
    if (!tbody) return;
    tbody.innerHTML = renderRows(lastRuns);
    // Row-level click navigation for mouse users. Clicks on interactive
    // controls (links, buttons, inputs, selects) inside the row bail out
    // so the native element handles the action — important for
    // keyboard nav, open-in-new-tab, copy-link, screen readers, etc.
    tbody.querySelectorAll('tr[data-href]').forEach((tr) => {
      tr.addEventListener('click', (e) => {
        if (e.target.closest('a, button, input, select')) return;
        const href = tr.getAttribute('data-href');
        if (!href) return;
        window.history.pushState({}, '', href);
        if (typeof window.renderRoute === 'function') window.renderRoute();
      });
    });
  }

  async function paint() {
    const runs = await api(`/api/v1/projects/${encodeURIComponent(pid)}/runs`);
    if (!document.getElementById('runs-table')) return; // navigated away
    lastRuns = runs || [];
    paintRows();
  }

  app.innerHTML = `
    <table class="table" id="runs-table">
      <thead>
        <tr>
          <th>Run ID</th>
          <th>Stage</th>
          <th>Phase</th>
          <th>Started</th>
          <th>Pipeline</th>
        </tr>
      </thead>
      <tbody id="runs-tbody">${skeletonRows(5, RUNS_COLS)}</tbody>
    </table>
  `;

  const filterEl = document.getElementById('runs-filter');
  if (filterEl) {
    filterEl.addEventListener('input', () => {
      filterText = filterEl.value || '';
      paintRows();
    });
  }

  await paint().catch((err) => {
    if (String(err.message) !== 'not authenticated') {
      app.innerHTML = `<article><h3>Error</h3><pre class="code">${esc(err.message)}</pre></article>`;
    }
  });
  // Poll every 5s while we're on this page.
  window.__datupletPoller = setInterval(() => paint().catch(() => {}), 5000);
}
