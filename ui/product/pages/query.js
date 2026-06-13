// Query console (RFC 022 §5.6) — three-pane ad-hoc SQL console.
//
// Layout: schema tree (left) | SQL editor (top-right) | results (bottom-right).
//
// Key design decisions:
//  - PATH-SNAPSHOT abort pattern guards every await (project-switch-mid-query).
//  - sessionStorage keyed by "datuplet.query.<email>.<projectId>" so buffers
//    survive page reloads but are scoped per-user per-project. On logout app.js
//    calls redirect('/ui/login') which drops the page; we clear stale keys for
//    principals other than the current one at render time.
//  - UI display cap of 2000 rows is SEPARATE from the API max_rows cap (server).
//  - ⌘↵ / Ctrl↵ runs the query while focus is in the textarea.
//  - '/' hotkey focuses [data-role="page-search"] for the schema filter.
//  - getTableSchema is called lazily when a namespace expands.
//  - runQuery errors surface by .kind (§10 codes); unrecognised codes → generic.

import { esc, runQuery, getStorageCatalog, getTableSchema } from '/ui/api.js';
import { emptyState, spinner, skeletonRows } from '/ui/components.js';
import * as icons from '/ui/icons.js';

// ---- Constants -------------------------------------------------------------

const UI_ROW_CAP = 2000;        // Client-side display cap (distinct from API max_rows)
const STORAGE_PREFIX = 'datuplet.query.';

// ---- sessionStorage helpers ------------------------------------------------

function storageKey(email, projectId) {
  // Separate with '|' (illegal in an email address and absent from project
  // UUIDs/names) so the email↔projectId boundary is unambiguous — '.' would
  // collide with dots in real emails (e.g. tomas.kacur@keboola.com) and break
  // the principal match in clearStaleBuffers.
  return `${STORAGE_PREFIX}${email}|${projectId}`;
}

function loadBuffer(email, projectId) {
  try {
    return sessionStorage.getItem(storageKey(email, projectId)) || '';
  } catch {
    return '';
  }
}

function saveBuffer(email, projectId, sql) {
  try {
    sessionStorage.setItem(storageKey(email, projectId), sql);
  } catch { /* ignore quota errors */ }
}

// Remove sessionStorage keys for any principal other than the current email.
// This handles the case where a different user logs in on the same tab.
function clearStaleBuffers(currentEmail) {
  try {
    const prefix = STORAGE_PREFIX;
    const keysToRemove = [];
    for (let i = 0; i < sessionStorage.length; i++) {
      const k = sessionStorage.key(i);
      if (k && k.startsWith(prefix)) {
        // Key format: datuplet.query.<email>|<projectId>. The '|' separator is
        // unambiguous (illegal in emails), so a key belongs to the current
        // principal iff the part after the prefix starts with "<email>|".
        const rest = k.slice(prefix.length);
        if (!rest.startsWith(currentEmail + '|')) {
          keysToRemove.push(k);
        }
      }
    }
    keysToRemove.forEach((k) => sessionStorage.removeItem(k));
  } catch { /* ignore */ }
}

// ---- RFC §10 error message mapping -----------------------------------------

function queryErrorMessage(err) {
  // err has .status, .kind, .retryAfter from api.js::runQuery
  if (err.kind === 'sql_error') {
    return `SQL error: ${esc(err.message)}`;
  }
  if (err.kind === 'forbidden' || err.status === 403) {
    return 'Not authorized for that table or warehouse.';
  }
  if (err.kind === 'timeout' || err.status === 408) {
    return 'Query timed out. Try a more selective WHERE clause or a lower LIMIT.';
  }
  if (err.kind === 'rate_limited' || err.status === 429) {
    const ra = err.retryAfter ? ` Retry after ${esc(String(err.retryAfter))}s.` : '';
    return `Too many queries in flight — please wait and retry.${ra}`;
  }
  if (err.status >= 500) {
    return `Query service error (${err.status}). Try again in a moment.`;
  }
  // Generic fallback
  return esc(err.message || 'Query failed.');
}

// ---- Schema pane -----------------------------------------------------------
//
// State is kept in a plain object that the event handlers close over.
// We never re-render the whole pane on filter change — we show/hide rows.

function buildSchemaPane(tables) {
  // Group tables by namespace.
  const byNs = new Map();
  for (const t of tables) {
    if (!byNs.has(t.namespace)) byNs.set(t.namespace, []);
    byNs.get(t.namespace).push(t);
  }
  return byNs;
}

function renderSchemaTree(byNs, filter) {
  const q = filter.trim().toLowerCase();
  let html = '';
  for (const [ns, tables] of byNs) {
    const matched = q
      ? tables.filter((t) => `${ns}.${t.name}`.toLowerCase().includes(q))
      : tables;
    if (matched.length === 0) continue;
    html += `
      <div class="qc-ns" data-ns="${esc(ns)}">
        <button class="qc-ns-toggle" aria-expanded="false" data-ns="${esc(ns)}">
          ${icons.chevronRight}<code class="mono">${esc(ns)}</code>
          <span class="qc-ns-count">${matched.length}</span>
        </button>
        <ul class="qc-ns-tables" hidden>
          ${matched.map((t) => `
            <li>
              <button class="qc-table-item" data-ns="${esc(t.namespace)}" data-name="${esc(t.name)}">
                ${icons.database}<code class="mono">${esc(t.name)}</code>
              </button>
              <div class="qc-col-list" data-ns="${esc(t.namespace)}" data-name="${esc(t.name)}" hidden>
                <div class="qc-col-loading">${spinner()} Loading columns…</div>
              </div>
            </li>
          `).join('')}
        </ul>
      </div>
    `;
  }
  return html || `<p class="qc-empty">No tables match.</p>`;
}

// ---- TSV / CSV export helpers ----------------------------------------------

function rowsToTSV(schema, rows) {
  const header = schema.map((c) => c.name).join('\t');
  const body = rows.map((r) => r.map((cell) => {
    if (cell === null || cell === undefined) return '';
    const s = typeof cell === 'object' ? JSON.stringify(cell) : String(cell);
    // Escape tabs and newlines in cell values for TSV
    return s.replace(/\t/g, ' ').replace(/\n/g, ' ');
  }).join('\t')).join('\n');
  return header + '\n' + body;
}

function rowsToCSV(schema, rows) {
  const csvCell = (v) => {
    if (v === null || v === undefined) return '';
    const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
    // Quote if contains comma, quote, or newline
    if (s.includes(',') || s.includes('"') || s.includes('\n')) {
      return '"' + s.replaceAll('"', '""') + '"';
    }
    return s;
  };
  const header = schema.map((c) => csvCell(c.name)).join(',');
  const body = rows.map((r) => r.map(csvCell).join(',')).join('\n');
  return header + '\n' + body;
}

// ---- Main render -----------------------------------------------------------

export async function renderQuery() {
  const head = document.getElementById('page-head');
  const app = document.getElementById('app');
  const projectId = window.__datupletActiveProjectID;

  // PATH-SNAPSHOT: guard against project-switch-mid-query landing its
  // response on the wrong page. aborted() is checked after EVERY await.
  const path = window.location.pathname;
  const aborted = () => window.location.pathname !== path;

  // ---- Page header ----------------------------------------------------------
  head.innerHTML = `
    <h1>${icons.terminal} Query</h1>
    <div class="actions">
      <input class="input" data-role="page-search" type="search"
             placeholder="Filter schema…" aria-label="Filter schema tree" />
    </div>
  `;

  if (!projectId) {
    app.innerHTML = emptyState({
      icon: icons.terminal,
      text: 'Pick a project to run ad-hoc SQL against its warehouse.',
    });
    return;
  }

  // ---- Fetch principal (for sessionStorage key) ----------------------------
  let principal = '';
  try {
    const meResp = await fetch('/api/v1/auth/me', { credentials: 'include' });
    if (aborted()) return;
    if (meResp.ok) {
      const me = await meResp.json();
      if (aborted()) return;
      principal = me.email || me.sub || me.id || '';
    }
  } catch { /* non-fatal — key degrades to no-email prefix */ }
  if (aborted()) return;

  // Remove sessionStorage entries for other principals
  if (principal) clearStaleBuffers(principal);

  const savedSQL = loadBuffer(principal, projectId);

  // ---- Initial three-pane shell -------------------------------------------
  app.innerHTML = `
    <div class="qc-layout">
      <aside class="qc-schema-pane" aria-label="Schema browser">
        <div class="qc-schema-inner" id="qc-schema-tree">
          ${skeletonRows(4, 1)}
        </div>
      </aside>
      <div class="qc-right">
        <div class="qc-editor-pane">
          <textarea
            id="qc-editor"
            class="textarea qc-editor"
            placeholder="SELECT * FROM namespace.table LIMIT 100"
            spellcheck="false"
            autocorrect="off"
            autocapitalize="off"
            aria-label="SQL editor"
          >${esc(savedSQL)}</textarea>
          <div class="qc-editor-toolbar">
            <button id="qc-run-btn" class="btn btn--primary">
              ${icons.play} Run <kbd>⌘↵</kbd>
            </button>
            <span id="qc-run-status" class="qc-run-status"></span>
          </div>
        </div>
        <div class="qc-results-pane" id="qc-results">
          <p class="qc-empty">Run a query to see results.</p>
        </div>
      </div>
    </div>
  `;

  // ---- Wire the '/' filter to the page-search input -----------------------
  // The global hotkeys.js already handles '/' → focus [data-role="page-search"].
  // We wire the schema tree filter here.
  const searchInput = head.querySelector('[data-role="page-search"]');

  // ---- Load schema (non-blocking, renders into the pane) ------------------
  const schemaTree = document.getElementById('qc-schema-tree');
  let byNs = new Map(); // populated once getStorageCatalog resolves
  // Cache fetched columns by "<ns>.<name>". The schema tree DOM is rebuilt on
  // every filter keystroke (which resets the [hidden] first-open guard), so
  // without this cache re-expanding after filtering would re-fetch columns.
  const colCache = new Map();

  const refreshSchema = async () => {
    schemaTree.innerHTML = `${skeletonRows(4, 1)}`;
    let tables;
    try {
      const resp = await getStorageCatalog(projectId);
      if (aborted()) return;
      tables = Array.isArray(resp?.tables) ? resp.tables : [];
    } catch (e) {
      if (aborted()) return;
      schemaTree.innerHTML = `
        <div class="qc-schema-error">
          <p style="color:var(--status-fail-fg)">Failed to load schema: ${esc(e.message)}</p>
          <button class="btn btn--ghost" id="qc-refresh-schema">Refresh schema</button>
        </div>
      `;
      document.getElementById('qc-refresh-schema')?.addEventListener('click', refreshSchema);
      return;
    }

    if (tables.length === 0) {
      byNs = new Map();
      schemaTree.innerHTML = `<p class="qc-empty">No tables in this project.</p>`;
      return;
    }

    byNs = buildSchemaPane(tables);
    schemaTree.innerHTML = renderSchemaTree(byNs, searchInput.value);
    attachSchemaHandlers();
  };

  // ---- Schema filter wiring -----------------------------------------------
  searchInput.addEventListener('input', () => {
    if (byNs.size === 0) return;
    schemaTree.innerHTML = renderSchemaTree(byNs, searchInput.value);
    attachSchemaHandlers();
  });

  // ---- Schema tree event handlers -----------------------------------------
  // Delegation from the pane so we can re-attach cheaply after re-render.
  function attachSchemaHandlers() {
    // Namespace toggle: expand/collapse table list, lazy-load columns.
    schemaTree.querySelectorAll('.qc-ns-toggle').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const ns = btn.dataset.ns;
        const nsDiv = btn.closest('.qc-ns');
        const ul = nsDiv?.querySelector('.qc-ns-tables');
        if (!ul) return;
        const expanded = btn.getAttribute('aria-expanded') === 'true';
        if (expanded) {
          btn.setAttribute('aria-expanded', 'false');
          ul.hidden = true;
        } else {
          btn.setAttribute('aria-expanded', 'true');
          ul.hidden = false;
          // Lazy-load columns for each table in this namespace, serving from
          // colCache on re-expand (the [hidden] guard alone resets on filter
          // re-render, so it can't prevent re-fetches by itself).
          ul.querySelectorAll('.qc-col-list[hidden]').forEach(async (colDiv) => {
            const tns = colDiv.dataset.ns;
            const tname = colDiv.dataset.name;
            colDiv.hidden = false; // show loading spinner
            const cacheKey = `${tns}.${tname}`;

            let cols = colCache.get(cacheKey);
            if (cols === undefined) {
              try {
                const schResp = await getTableSchema(projectId, tns, tname);
                if (aborted()) return;
                cols = schResp?.columns || [];
                colCache.set(cacheKey, cols); // cache only on success
              } catch (e) {
                if (aborted()) return;
                colDiv.innerHTML = `
                  <span class="qc-col-item" style="color:var(--status-fail-fg)">
                    Could not load columns — <button class="btn btn--ghost qc-retry-cols">retry</button>
                  </span>
                `;
                colDiv.querySelector('.qc-retry-cols')?.addEventListener('click', () => {
                  // Reset to loading state so the next expand re-fetches (no
                  // cache entry was stored on failure).
                  colDiv.hidden = true;
                  colDiv.innerHTML = `<div class="qc-col-loading">${spinner()} Loading columns…</div>`;
                });
                return;
              }
            }

            colDiv.innerHTML = cols.length === 0
              ? `<span class="qc-col-item qc-empty-cols">(no columns)</span>`
              : cols.map((c) => `
                <span class="qc-col-item" title="${esc(c.type)}">
                  <code class="mono">${esc(c.name)}</code>
                  <span class="qc-col-type">${esc(c.type)}</span>
                </span>
              `).join('');
          });
        }
      });
    });

    // Table click: insert "namespace.table" at textarea cursor.
    schemaTree.querySelectorAll('.qc-table-item').forEach((btn) => {
      btn.addEventListener('click', () => {
        const ns = btn.dataset.ns;
        const name = btn.dataset.name;
        const insert = `${ns}.${name}`;
        const ed = document.getElementById('qc-editor');
        if (!ed) return;
        const start = ed.selectionStart;
        const end = ed.selectionEnd;
        const before = ed.value.slice(0, start);
        const after = ed.value.slice(end);
        ed.value = before + insert + after;
        ed.selectionStart = ed.selectionEnd = start + insert.length;
        ed.focus();
        saveBuffer(principal, projectId, ed.value);
      });
    });
  }

  // Kick off schema load (non-blocking — editor is usable immediately).
  refreshSchema();

  // ---- Editor wiring -------------------------------------------------------
  const editor = document.getElementById('qc-editor');
  const runBtn = document.getElementById('qc-run-btn');
  const runStatus = document.getElementById('qc-run-status');
  const resultsPane = document.getElementById('qc-results');

  // Persist buffer on every keystroke.
  editor.addEventListener('input', () => {
    saveBuffer(principal, projectId, editor.value);
  });

  // ⌘↵ / Ctrl↵ triggers run from within the textarea.
  editor.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      if (!runBtn.disabled) runBtn.click();
    }
  });

  // ---- Run handler ---------------------------------------------------------
  let inFlight = false;

  async function executeQuery() {
    if (inFlight) return;
    const sql = editor.value.trim();
    if (!sql) return;

    inFlight = true;
    editor.disabled = true;
    runBtn.disabled = true;
    runStatus.innerHTML = `${spinner()} Running…`;
    resultsPane.innerHTML = `<p class="qc-empty">${spinner()} Executing query…</p>`;

    // Re-snapshot path in case user attempted a project switch just before clicking Run.
    // If already navigated away, bail immediately.
    if (aborted()) {
      resetRunState();
      return;
    }

    let result;
    try {
      result = await runQuery(projectId, sql, {});
    } catch (e) {
      // Reset run state even when aborting: if aborted() is a false positive
      // (page still live) NOT resetting would leave the editor permanently
      // disabled with inFlight stuck true.
      resetRunState();
      if (aborted()) return; // late response on a navigated-away page — discard
      resultsPane.innerHTML = `
        <div class="callout callout--warn">
          ${icons.alertTriangle} ${queryErrorMessage(e)}
        </div>
      `;
      return;
    }

    resetRunState();
    if (aborted()) return; // discard late response
    renderResults(result);
  }

  function resetRunState() {
    inFlight = false;
    editor.disabled = false;
    runBtn.disabled = false;
    runStatus.innerHTML = '';
  }

  runBtn.addEventListener('click', executeQuery);

  // ---- Results renderer ----------------------------------------------------
  function renderResults(result) {
    const schema = Array.isArray(result.schema) ? result.schema : [];
    const allRows = Array.isArray(result.rows) ? result.rows : [];
    const truncated = !!result.truncated;
    const stats = result.stats || {};
    const durationMs = stats.duration_ms != null ? stats.duration_ms : null;

    // UI display cap — distinct from API server cap.
    const displayRows = allRows.slice(0, UI_ROW_CAP);
    const displayCapped = allRows.length > UI_ROW_CAP;

    const truncatedBanner = truncated
      ? `<div class="callout callout--warn">${icons.alertTriangle} Results truncated at the server cap — not all rows are shown.</div>`
      : '';
    const capNote = displayCapped
      ? `<div class="callout">Showing ${UI_ROW_CAP.toLocaleString()} of ${allRows.length.toLocaleString()} returned rows (UI display cap).</div>`
      : '';

    const downloadLabel = (truncated || displayCapped)
      ? 'Download returned rows (capped)'
      : 'Download CSV';

    if (schema.length === 0 && displayRows.length === 0) {
      resultsPane.innerHTML = `
        ${truncatedBanner}${capNote}
        <p class="qc-empty">Query ran successfully — no rows returned.</p>
        <div class="qc-results-footer">
          ${durationMs != null ? `<span class="qc-stat">${icons.clock} ${durationMs.toLocaleString()} ms</span>` : ''}
        </div>
      `;
      return;
    }

    const headerCells = schema.map((c) => `
      <th>
        <code class="mono">${esc(c.name)}</code>
        <div style="color:var(--fg-2);font-weight:400;">${esc(c.type)}</div>
      </th>
    `).join('');

    const bodyRows = displayRows.map((row) => `
      <tr>${row.map((cell) => {
        if (cell === null || cell === undefined) {
          return `<td class="qc-null" aria-label="null">∅</td>`;
        }
        const s = typeof cell === 'object' ? JSON.stringify(cell) : String(cell);
        return `<td>${esc(s)}</td>`;
      }).join('')}</tr>
    `).join('');

    const rowCountLabel = displayCapped
      ? `Showing ${UI_ROW_CAP.toLocaleString()} of ${allRows.length.toLocaleString()} rows`
      : `${displayRows.length.toLocaleString()} row${displayRows.length !== 1 ? 's' : ''}`;

    resultsPane.innerHTML = `
      ${truncatedBanner}${capNote}
      <div class="qc-results-scroll">
        <table class="table qc-results-table">
          <thead><tr>${headerCells}</tr></thead>
          <tbody>${bodyRows}</tbody>
        </table>
      </div>
      <div class="qc-results-footer">
        <span class="qc-stat">${rowCountLabel}</span>
        ${durationMs != null ? `<span class="qc-stat">${icons.clock} ${durationMs.toLocaleString()} ms</span>` : ''}
        <div class="qc-export-actions">
          <button id="qc-copy-tsv" class="btn btn--ghost">Copy TSV</button>
          <button id="qc-download-csv" class="btn btn--ghost">${esc(downloadLabel)}</button>
        </div>
      </div>
    `;

    // Capture rows/schema for export closures (avoid re-reading DOM).
    const exportSchema = schema;
    const exportRows = displayRows;

    document.getElementById('qc-copy-tsv')?.addEventListener('click', () => {
      const tsv = rowsToTSV(exportSchema, exportRows);
      navigator.clipboard.writeText(tsv).catch(() => {
        // Fallback: select the table text (silent failure — clipboard API needs focus).
      });
    });

    document.getElementById('qc-download-csv')?.addEventListener('click', () => {
      const csv = rowsToCSV(exportSchema, exportRows);
      const blob = new Blob([csv], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `query-results-${projectId}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    });
  }
}
