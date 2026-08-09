// Apps catalog (RFC 028 §5.5, Part 6, task U0). Lists the active
// project's apps — name, production/draft short-hash, last touched —
// and hosts the "New app" upload panel (name + bundle -> PUT, which
// creates the app on first upload and always repoints its draft
// channel). App detail (version history, promote/rollback, viewer
// tokens, render logs) is pages/app-detail.js (task U1); this page only
// links to it — app.js wires a placeholder route until U1 lands.
//
// Backed by pkg/pipelineapi/apps/handlers.go's author routes:
//   GET /api/v1/projects/{pid}/apps          -> [{app_id, name, created_at, channels}]
//   PUT /api/v1/projects/{pid}/apps/{name}    {bundle_base64} -> {app_id, version_hash}
// `channels` is keyed by "production"/"draft" (channelJSON{version_hash,
// updated_at}); an absent key means that channel has never been pointed
// at a version. See api.js's listApps/putApp doc comments for the full
// shape.
//
// Every server-returned string (name, hash, timestamps) reaches the DOM
// through esc() or a DOM text property — never innerHTML built from raw
// interpolation — matching every other catalog page in this directory.

import { esc, listApps, putApp } from '/ui/api.js';
import { emptyState, skeletonRows } from '/ui/components.js';
import { timeTag, formatBytes } from '/ui/format.js';
import * as icons from '/ui/icons.js';

// Mirrors pkg/pipelineapi/apps.appNamePattern (handlers.go), also
// hand-mirrored in cmd/datuplet/apps.go. A UX pre-check ONLY — the
// server re-validates on every write and is the sole authority; this
// just saves the common typo a round trip.
const APP_NAME_PATTERN = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;

// Mirrors pkg/pipelineapi/apps.MaxBundleBytes (store.go) — a friendly
// client-side pre-check so an obviously-oversize file never leaves the
// browser. The server enforces this authoritatively (413) regardless.
const MAX_BUNDLE_BYTES = 5 * 1024 * 1024;

// Table has 5 columns: Name, Production, Draft, Updated, (viewer link).
const APPS_COLS = 5;

export async function renderApps() {
  const head = document.getElementById('page-head');
  const app = document.getElementById('app');
  const pid = window.__datupletActiveProjectID;

  if (head) head.innerHTML = `<h1>Apps</h1>`;

  if (!pid) {
    app.innerHTML = emptyState({ icon: icons.database, text: 'Pick a project to manage its apps.' });
    return;
  }

  // Snapshot the path we started rendering for, mirroring
  // storage-catalog.js: a late response after the user has navigated
  // away must not paint over the destination page.
  const path = window.location.pathname;
  const aborted = () => window.location.pathname !== path;

  app.innerHTML = `
    <table class="table">
      <thead>
        <tr><th>Name</th><th>Production</th><th>Draft</th><th>Updated</th><th></th></tr>
      </thead>
      <tbody id="apps-body">${skeletonRows(4, APPS_COLS)}</tbody>
    </table>

    <h3 class="section-h">New app</h3>
    <p style="color: var(--fg-2);">
      Upload an esbuild-bundled <code class="inline">app.js</code> (see
      <code class="inline">datuplet apps init</code>). The name is
      permanent once created — it appears in the app's viewer URL.
    </p>
    <div id="apps-new-msg"></div>
    <form id="apps-new-form">
      <label class="field">Name
        <input class="input" type="text" name="name" placeholder="my-dashboard" required
          pattern="[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?"
          title="Lowercase letters, digits, and '-'; must not start or end with '-' (1-63 chars).">
      </label>
      <label class="field">Bundle
        <input class="input" type="file" name="bundle" accept=".js" required>
      </label>
      <div class="actions" style="margin-top: var(--s-3);">
        <button type="submit" class="btn btn--primary">Create / upload</button>
      </div>
    </form>
  `;

  const body = document.getElementById('apps-body');
  let apps = [];

  function renderRows() {
    if (!apps || apps.length === 0) {
      body.innerHTML = `
        <tr><td colspan="${APPS_COLS}">
          <div class="empty-state">
            ${icons.database}
            <p>No apps yet — create one or use <code class="inline">datuplet apps init</code>.</p>
          </div>
        </td></tr>`;
      return;
    }
    body.innerHTML = apps.map(rowHTML).join('');
    wireCopyButtons(body, pid);
  }

  async function reload() {
    try {
      apps = await listApps(pid);
      if (aborted()) return;
    } catch (err) {
      if (aborted() || String(err.message) === 'not authenticated') return;
      body.innerHTML = `<tr><td colspan="${APPS_COLS}"><div class="callout callout--warn">Failed to load apps: ${esc(err.message)}</div></td></tr>`;
      return;
    }
    renderRows();
  }

  // Row click navigates to the detail page. Delegated on the tbody
  // (not per-row) because reload() replaces body.innerHTML wholesale on
  // every list refresh — same reasoning as storage-catalog.js's tbody
  // listener. The interactive-descendant guard runs first so the "Copy
  // viewer link" button and the name <a> keep their own click behavior.
  body.addEventListener('click', (e) => {
    if (e.target.closest('a, button, input, select')) return;
    const tr = e.target.closest('tr[data-href]');
    if (!tr) return;
    window.history.pushState({}, '', tr.getAttribute('data-href'));
    window.renderRoute();
  });

  await reload();

  const form = document.getElementById('apps-new-form');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = document.getElementById('apps-new-msg');
    msg.innerHTML = '';
    const fd = new FormData(form);
    const name = String(fd.get('name') || '').trim();
    const file = fd.get('bundle');

    if (!APP_NAME_PATTERN.test(name)) {
      msg.innerHTML = `<div class="callout callout--warn">Invalid name: must be lowercase letters, digits, and '-', not starting or ending with '-' (1-63 chars).</div>`;
      return;
    }
    if (!(file instanceof File) || file.size === 0) {
      msg.innerHTML = `<div class="callout callout--warn">Choose a bundle file.</div>`;
      return;
    }
    if (file.size > MAX_BUNDLE_BYTES) {
      msg.innerHTML = `<div class="callout callout--warn">Bundle is ${formatBytes(file.size)}; the server caps uploads at ${formatBytes(MAX_BUNDLE_BYTES)}.</div>`;
      return;
    }

    const btn = form.querySelector('button[type=submit]');
    btn.disabled = true;
    try {
      const bundleBase64 = await fileToBase64(file);
      if (aborted()) return;
      await putApp(pid, name, bundleBase64);
      if (aborted()) return;
      form.reset();
      msg.innerHTML = `<div class="callout">Uploaded — draft updated.</div>`;
      await reload();
    } catch (err) {
      if (String(err.message) !== 'not authenticated') {
        msg.innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
      }
    } finally {
      btn.disabled = false;
    }
  });
}

// rowHTML renders one catalog row. Every server-supplied string (name,
// hash, timestamps) goes through esc() before interpolation — even
// though the name already satisfies appNamePattern and hashes are hex
// — as defense in depth, not a control this page depends on.
function rowHTML(a) {
  const href = `/ui/apps/${encodeURIComponent(a.name)}`;
  const channels = a.channels || {};
  const production = channels.production;
  const draft = channels.draft;
  const updatedIso = (draft && draft.updated_at) || (production && production.updated_at) || a.created_at;
  const viewerCell = production
    ? `<button type="button" class="btn btn--ghost" data-copy-viewer data-app="${esc(a.name)}">Copy viewer link</button>`
    : '';
  return `
    <tr data-href="${href}">
      <td><a href="${href}"><code class="inline">${esc(a.name)}</code></a></td>
      <td>${hashCell(production)}</td>
      <td>${hashCell(draft)}</td>
      <td>${timeTag(updatedIso)}</td>
      <td>${viewerCell}</td>
    </tr>`;
}

function hashCell(channel) {
  if (!channel || !channel.version_hash) return '<span style="color: var(--fg-2);">—</span>';
  return `<code class="mono" title="${esc(channel.version_hash)}">${esc(shortHash(channel.version_hash))}</code>`;
}

// shortHash mirrors cmd/datuplet/apps.go's shortHash (12 hex chars) so
// the CLI and the UI abbreviate the same hash the same way.
function shortHash(h) {
  return h.length > 12 ? h.slice(0, 12) : h;
}

// wireCopyButtons attaches the viewer-link copy action. Re-called after
// every renderRows() since body.innerHTML replaces the buttons.
function wireCopyButtons(container, pid) {
  container.querySelectorAll('[data-copy-viewer]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const appName = btn.getAttribute('data-app') || '';
      const original = btn.textContent;
      try {
        await navigator.clipboard.writeText(viewerURL(pid, appName));
        btn.textContent = 'Copied';
      } catch {
        btn.textContent = 'Copy failed';
      }
      setTimeout(() => { btn.textContent = original; }, 1500);
    });
  });
}

// viewerURL builds the app-worker's viewer route from known-safe pieces
// — the browser-provided origin plus the active project id and the
// app's own name, both already server-validated identifiers — never
// from a string concatenated out of arbitrary/untrusted input.
function viewerURL(projectId, appName) {
  return `${window.location.origin}/apps/${encodeURIComponent(projectId)}/${encodeURIComponent(appName)}`;
}

// fileToBase64 reads a File's raw bytes and resolves the base64 text
// putApp's {"bundle_base64": "..."} body expects. readAsDataURL is the
// simplest cross-browser route to base64 for a file this size; the
// "data:<mime>;base64," prefix is sliced off before resolving.
function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error('could not read the bundle file'));
    reader.onload = () => {
      const result = String(reader.result || '');
      const comma = result.indexOf(',');
      resolve(comma >= 0 ? result.slice(comma + 1) : result);
    };
    reader.readAsDataURL(file);
  });
}
