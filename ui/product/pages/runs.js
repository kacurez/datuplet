// Runs list for the active project. Server returns a keyset-paged
// envelope { runs, next_cursor } (pipeline-api RFC 023 Task 8/9),
// filtered server-side by pipeline-name search + phase. Rows are
// accumulated in-memory (keyed by id) across "Load more" pages and
// merged in place by a 5s poll that re-fetches page 1 with the current
// filters — existing rows update in place, new ones prepend, and
// already-loaded older rows are never dropped.
//
// The interval is cleared by the router before navigating away (see
// window.__datupletPoller below) so we don't leak pollers across
// navigations.

import { api, esc } from '/ui/api.js';
import { timeTag, phaseToPillClass, formatDuration, durationFrom } from '/ui/format.js';
import * as icons from '/ui/icons.js';
import { emptyState, skeletonRows } from '/ui/components.js';

// Runs table has 5 columns: Run ID, Pipeline, Phase, Started, Duration.
const RUNS_COLS = 5;
const POLL_MS = 5000;
const SEARCH_DEBOUNCE_MS = 300;
const PHASE_OPTIONS = ['All', 'Running', 'Succeeded', 'FailedApplication', 'FailedUser', 'Pending', 'Cancelled', 'Expired'];

function clearExistingPoller() {
  if (window.__datupletPoller) {
    clearInterval(window.__datupletPoller);
    window.__datupletPoller = null;
  }
}

function buildQuery({ search, phase, cursor, limit = 50 }) {
  const p = new URLSearchParams();
  p.set('limit', String(limit));
  if (cursor) p.set('cursor', cursor);
  if (search) p.set('pipeline', search);
  if (phase) p.set('phase', phase);
  return p.toString();
}

async function fetchPage(pid, params) {
  const resp = await api(`/api/v1/projects/${encodeURIComponent(pid)}/runs?${buildQuery(params)}`);
  return { runs: resp.runs || [], nextCursor: resp.next_cursor || '' };
}

function renderRow(r) {
  const id = String(r.id || '');
  const href = `/ui/runs/${encodeURIComponent(id)}`;
  const pill = phaseToPillClass(r.phase);
  const started = r.started_at ? timeTag(r.started_at) : '<span class="muted">—</span>';
  const dur = r.completed_at || r.started_at
    ? formatDuration(durationFrom(r.started_at, r.completed_at))
    : '—';
  return `
    <tr data-href="${href}" data-id="${esc(id)}">
      <td><a href="${href}"><code class="mono">${esc(id.slice(0, 8))}</code></a></td>
      <td>${esc(r.pipeline_name || r.pipeline_id || '')}</td>
      <td><span class="pill ${pill}">${esc(r.phase)}</span></td>
      <td>${started}</td>
      <td>${dur}</td>
    </tr>`;
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
        <input type="search" class="input" placeholder="Filter by pipeline…" data-role="page-search" id="runs-search" />
        <select class="input" id="runs-phase">
          ${PHASE_OPTIONS.map((p) => `<option value="${p === 'All' ? '' : esc(p)}">${esc(p)}</option>`).join('')}
        </select>
      </div>
    `;
  }

  // Accumulated rows, keyed by id. Insertion order (Map) tracks display
  // order: newest-first page 1, older pages appended via Load more.
  let rows = new Map();
  let order = []; // array of ids in display order
  let cursor = '';
  let nextCursor = '';
  let search = '';
  let phase = '';
  let loadingMore = false;

  function upsertRow(r, { prepend = false } = {}) {
    const id = String(r.id || '');
    if (!rows.has(id)) {
      if (prepend) order.unshift(id);
      else order.push(id);
    }
    rows.set(id, r);
  }

  function renderRowsHTML() {
    if (order.length === 0) {
      return `<tr><td colspan="${RUNS_COLS}">${emptyState({ icon: icons.activity, text: 'No runs yet.' })}</td></tr>`;
    }
    return order.map((id) => renderRow(rows.get(id))).join('');
  }

  function paintRows() {
    const tbody = document.getElementById('runs-tbody');
    if (!tbody) return;
    tbody.innerHTML = renderRowsHTML();
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
    paintLoadMore();
  }

  function paintLoadMore() {
    const container = document.getElementById('runs-load-more-container');
    if (!container) return;
    if (!nextCursor) {
      container.innerHTML = '';
      return;
    }
    container.innerHTML = `<button class="btn" id="runs-load-more">${loadingMore ? 'Loading…' : 'Load more'}</button>`;
    const btn = document.getElementById('runs-load-more');
    if (btn) btn.addEventListener('click', () => { loadMore().catch(() => {}); });
  }

  async function loadFirstPage() {
    const { runs, nextCursor: nc } = await fetchPage(pid, { search, phase, cursor: '' });
    rows = new Map();
    order = [];
    runs.forEach((r) => upsertRow(r));
    cursor = nc;
    nextCursor = nc;
    if (!document.getElementById('runs-table')) return; // navigated away
    paintRows();
  }

  async function loadMore() {
    if (!nextCursor || loadingMore) return;
    loadingMore = true;
    paintLoadMore();
    try {
      const { runs, nextCursor: nc } = await fetchPage(pid, { search, phase, cursor: nextCursor });
      runs.forEach((r) => {
        if (!rows.has(String(r.id || ''))) upsertRow(r);
      });
      nextCursor = nc;
      if (!document.getElementById('runs-table')) return; // navigated away
      paintRows();
    } finally {
      loadingMore = false;
      paintLoadMore();
    }
  }

  // Poll: re-fetch page 1 (no cursor) with current filters, merge by id.
  // Existing rows update in place; genuinely new rows prepend. Rows only
  // reachable via "Load more" (older pages) are never dropped.
  async function poll() {
    const { runs } = await fetchPage(pid, { search, phase, cursor: '' });
    runs.slice().reverse().forEach((r) => {
      const id = String(r.id || '');
      if (rows.has(id)) {
        rows.set(id, r); // update in place, preserve position
      } else {
        order.unshift(id); // genuinely new — prepend
        rows.set(id, r);
      }
    });
    if (!document.getElementById('runs-table')) return; // navigated away
    paintRows();
  }

  async function resetAndLoad() {
    const tbody = document.getElementById('runs-tbody');
    if (tbody) tbody.innerHTML = skeletonRows(5, RUNS_COLS);
    await loadFirstPage();
  }

  app.innerHTML = `
    <table class="table" id="runs-table">
      <thead>
        <tr>
          <th>Run ID</th>
          <th>Pipeline</th>
          <th>Phase</th>
          <th>Started</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody id="runs-tbody">${skeletonRows(5, RUNS_COLS)}</tbody>
    </table>
    <div id="runs-load-more-container"></div>
  `;

  const searchEl = document.getElementById('runs-search');
  let searchTimer = null;
  if (searchEl) {
    searchEl.addEventListener('input', () => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => {
        search = searchEl.value || '';
        resetAndLoad().catch((err) => {
          if (String(err.message) !== 'not authenticated') {
            app.innerHTML = `<article><h3>Error</h3><pre class="code">${esc(err.message)}</pre></article>`;
          }
        });
      }, SEARCH_DEBOUNCE_MS);
    });
  }

  const phaseEl = document.getElementById('runs-phase');
  if (phaseEl) {
    phaseEl.addEventListener('change', () => {
      phase = phaseEl.value || '';
      resetAndLoad().catch((err) => {
        if (String(err.message) !== 'not authenticated') {
          app.innerHTML = `<article><h3>Error</h3><pre class="code">${esc(err.message)}</pre></article>`;
        }
      });
    });
  }

  await loadFirstPage().catch((err) => {
    if (String(err.message) !== 'not authenticated') {
      app.innerHTML = `<article><h3>Error</h3><pre class="code">${esc(err.message)}</pre></article>`;
    }
  });
  // Poll every 5s while we're on this page.
  window.__datupletPoller = setInterval(() => poll().catch(() => {}), POLL_MS);
}
