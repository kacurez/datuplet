// App detail (RFC 028 §5.5, Part 6, task U1). One app's lifecycle
// surface for project members: which version each channel points at,
// the full version history, uploading a new draft bundle, and the
// CAS-guarded promote/rollback + typed-name delete.
//
// Backed by pkg/pipelineapi/apps/handlers.go's author routes (via the
// api.js wrappers):
//   GET    /api/v1/projects/{pid}/apps/{name}          -> detail
//   PUT    /api/v1/projects/{pid}/apps/{name}          {bundle_base64} -> draft
//   POST   /api/v1/projects/{pid}/apps/{name}/promote  {version, expectedProduction}
//   DELETE /api/v1/projects/{pid}/apps/{name}          -> 204
//
// The detail shape is { app_id, name, created_at,
//   channels: { production?: {version_hash, updated_at},
//               draft?:      {version_hash, updated_at} },
//   versions: [{hash, size_bytes, created_at}, ...] }  (newest first).
// A channel key is absent (never null) until something points at it.
//
// --- The CAS discipline (the correctness heart of this page) ---
// Promote (and its sibling rollback) is a compare-and-swap: the server
// repoints `production` to the chosen version ONLY IF production still
// points where the author last saw it. This page captures
// `expectedProduction` = the production hash currently rendered on
// screen at the moment the confirm modal opens, and passes it verbatim
// to promoteApp. If a teammate promoted in the meantime, the server
// answers 409 and we do NOT silently retry: we surface a "refresh"
// message and reload the detail so the author re-confirms against the
// new production hash. This is the same guard the store-level CAS (P1)
// and the CLI's exit-9 (C3) enforce — the UI is the third consumer.
//
// Every server-returned string (name, hashes, sizes, timestamps) reaches
// the DOM through esc() or a DOM text property — never innerHTML built
// from raw interpolation — matching every other page in this directory.

import { esc, getApp, putApp, deleteApp, promoteApp } from '/ui/api.js';
import { emptyState, skeletonRows } from '/ui/components.js';
import { timeTag, formatBytes } from '/ui/format.js';
import * as icons from '/ui/icons.js';

// Mirrors pkg/pipelineapi/apps.MaxBundleBytes (store.go) — a friendly
// client-side pre-check so an obviously-oversize file never leaves the
// browser. The server enforces this authoritatively (413) regardless.
const MAX_BUNDLE_BYTES = 5 * 1024 * 1024;

// Versions table has 5 columns: Version, Size, Created, Channels, action.
const VERSION_COLS = 5;

export async function renderAppDetail(ctx) {
  const head = document.getElementById('page-head');
  const app = document.getElementById('app');
  const pid = window.__datupletActiveProjectID;
  // App names are DNS-label-safe (^[a-z0-9-]+$), so the path segment is
  // never percent-encoded — ctx.params[0] is the real name. Matches
  // pipeline-detail.js, which likewise uses ctx.params[0] verbatim.
  const name = ctx.params[0];

  if (head) {
    head.innerHTML = `
      <h1><code class="inline">${esc(name)}</code></h1>
      <div class="actions">
        <a class="btn btn--secondary" href="/ui/apps">Back to catalog</a>
        ${pid ? `<button type="button" id="app-delete-btn" class="btn btn--danger">Delete app</button>` : ''}
      </div>`;
  }

  if (!pid) {
    app.innerHTML = emptyState({ icon: icons.database, text: 'Pick a project to manage its apps.' });
    return;
  }

  // Snapshot the path we started rendering for, mirroring apps.js /
  // storage-catalog.js: a late response after the user navigated away
  // must not paint over the destination page.
  const path = window.location.pathname;
  const aborted = () => window.location.pathname !== path;

  app.innerHTML = `
    <div id="app-detail-msg"></div>

    <h2 class="section-h">Channels</h2>
    <div id="app-channels"><p aria-busy="true">Loading…</p></div>

    <h2 class="section-h">Versions</h2>
    <p class="hint">Newest first. Promote any earlier version to roll production back to it — the same compare-and-swap as promoting the draft.</p>
    <table class="table">
      <thead>
        <tr><th>Version</th><th>Size</th><th>Created</th><th>Channels</th><th></th></tr>
      </thead>
      <tbody id="app-versions-body">${skeletonRows(3, VERSION_COLS)}</tbody>
    </table>

    <h3 class="section-h">Upload new bundle</h3>
    <p class="hint">
      Uploads an esbuild-bundled <code class="inline">app.js</code> to the
      <b>draft</b> channel; production is untouched until you promote. See
      <code class="inline">datuplet apps put</code>.
    </p>
    <div id="app-upload-msg"></div>
    <form id="app-upload-form">
      <label class="field">Bundle
        <input class="input" type="file" name="bundle" accept=".js" required>
      </label>
      <div class="actions" style="margin-top: var(--s-3);">
        <button type="submit" class="btn btn--primary">Upload bundle</button>
      </div>
    </form>

    <div id="app-modal-host"></div>
  `;

  const channelsEl = document.getElementById('app-channels');
  const versionsBody = document.getElementById('app-versions-body');
  const detailMsg = document.getElementById('app-detail-msg');
  const modalHost = document.getElementById('app-modal-host');

  // The single source of truth for what's on screen. reload() replaces
  // it wholesale; renderChannels/renderVersions and the promote CAS all
  // read from it, so `expectedProduction` is always the hash the author
  // is currently looking at.
  let appData = null;

  const setDetailMsg = (html) => { detailMsg.innerHTML = html; };

  function prodHash() {
    return (appData && appData.channels && appData.channels.production && appData.channels.production.version_hash) || null;
  }
  function draftHash() {
    return (appData && appData.channels && appData.channels.draft && appData.channels.draft.version_hash) || null;
  }

  function renderChannels() {
    const ch = (appData && appData.channels) || {};
    const prod = ch.production;
    const draft = ch.draft;
    const canPromoteDraft = draft && draft.version_hash && draft.version_hash !== (prod && prod.version_hash);
    channelsEl.innerHTML = `
      <div class="app-channel-row" style="display:flex; align-items:center; gap: var(--s-3); margin-bottom: var(--s-2); flex-wrap: wrap;">
        <span class="pill pill--ok">production</span>
        ${channelTarget(prod)}
      </div>
      <div class="app-channel-row" style="display:flex; align-items:center; gap: var(--s-3); flex-wrap: wrap;">
        <span class="pill pill--running">draft</span>
        ${channelTarget(draft)}
        ${canPromoteDraft ? `<button type="button" id="promote-draft-btn" class="btn btn--primary">Promote to production</button>` : ''}
      </div>`;
    const promoteDraftBtn = document.getElementById('promote-draft-btn');
    if (promoteDraftBtn) {
      promoteDraftBtn.addEventListener('click', () => openPromoteModal(draftHash()));
    }
  }

  function renderVersions() {
    const versions = (appData && Array.isArray(appData.versions)) ? appData.versions : [];
    if (versions.length === 0) {
      versionsBody.innerHTML = `<tr><td colspan="${VERSION_COLS}" style="color: var(--fg-2); text-align:center; padding: var(--s-5);">No versions uploaded yet.</td></tr>`;
      return;
    }
    const ch = (appData && appData.channels) || {};
    versionsBody.innerHTML = versions.map((v) => versionRowHTML(v, ch)).join('');
  }

  // Delegated on the tbody (attached once here; reload() only replaces
  // its innerHTML, not the node) — same reasoning as apps.js. A row's
  // Promote button carries the full target hash in data-promote.
  versionsBody.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-promote]');
    if (!btn) return;
    openPromoteModal(btn.getAttribute('data-promote'));
  });

  async function reload() {
    try {
      appData = await getApp(pid, name);
      if (aborted()) return;
    } catch (err) {
      if (aborted() || String(err.message) === 'not authenticated') return;
      channelsEl.innerHTML = `<div class="callout callout--warn">Failed to load app: ${esc(err.message)}</div>`;
      versionsBody.innerHTML = `<tr><td colspan="${VERSION_COLS}"></td></tr>`;
      return;
    }
    renderChannels();
    renderVersions();
  }

  // --- Promote / rollback (shared CAS-guarded modal) -----------------
  // targetHash is the version being promoted TO. expectedProduction is
  // captured HERE, at modal-open, from the currently-rendered appData —
  // so it is exactly the production hash the author sees. That value is
  // frozen in this closure and sent on confirm; a concurrent promote is
  // then caught by the server as a 409 rather than silently clobbered.
  function openPromoteModal(targetHash) {
    if (!targetHash) return;
    const expectedProduction = prodHash(); // may be null (first promote)
    if (targetHash === expectedProduction) return; // already live — no-op
    const fromLabel = expectedProduction ? shortHash(expectedProduction) : 'none';

    const modal = showModal(modalHost, 'Promote to production', `
      <h3>Promote to production</h3>
      <p class="hint">Production repoints to the selected version; viewers get the new bundle immediately.</p>
      <p style="margin: var(--s-3) 0; font-size: var(--text-md);">
        <code class="inline" title="${esc(expectedProduction || '')}">${esc(fromLabel)}</code>
        <span style="color: var(--fg-2);"> &rarr; </span>
        <code class="inline" title="${esc(targetHash)}">${esc(shortHash(targetHash))}</code>
      </p>
      <div class="cat-actions">
        <button type="button" class="btn btn--primary" id="promote-confirm">Promote</button>
        <button type="button" class="btn" id="promote-cancel">Cancel</button>
      </div>`);

    modal.panel.querySelector('#promote-cancel').addEventListener('click', modal.close);
    const confirmBtn = modal.panel.querySelector('#promote-confirm');
    confirmBtn.addEventListener('click', async () => {
      confirmBtn.disabled = true;
      try {
        await promoteApp(pid, name, targetHash, expectedProduction);
        if (aborted()) return;
        modal.close();
        setDetailMsg(`<div class="callout">Production now points at <code class="inline">${esc(shortHash(targetHash))}</code>.</div>`);
        await reload();
      } catch (err) {
        if (String(err.message) === 'not authenticated') return;
        modal.close();
        if (isConflict(err)) {
          // CAS mismatch: someone promoted meanwhile. Do NOT retry — the
          // whole point is that the author re-confirms against the new
          // state. Refresh the view so the next promote captures the
          // now-current production hash.
          setDetailMsg(`<div class="callout callout--warn">Production changed since this page loaded — someone promoted meanwhile. The view has been refreshed; review the current production version and promote again if you still want to.</div>`);
          await reload();
        } else {
          setDetailMsg(`<div class="callout callout--warn">${esc(err.message)}</div>`);
        }
      }
    });
  }

  // --- Delete (typed-name confirmation) ------------------------------
  // The typed-name gate is a footgun-guard (UX), not a security control
  // — the server authorizes the DELETE via FGA data_admin regardless.
  function openDeleteModal() {
    const modal = showModal(modalHost, 'Delete app', `
      <h3>Delete <code class="inline">${esc(name)}</code>?</h3>
      <p class="hint">This permanently removes the app, every uploaded version, its production and draft channels, and all viewer tokens. Viewers lose access immediately. This cannot be undone.</p>
      <label class="field">Type <code class="inline">${esc(name)}</code> to confirm
        <input class="input input--mono" type="text" id="delete-confirm-name" autocomplete="off" spellcheck="false" placeholder="${esc(name)}">
      </label>
      <div id="delete-modal-msg"></div>
      <div class="cat-actions">
        <button type="button" class="btn btn--danger" id="delete-confirm" disabled>Delete app</button>
        <button type="button" class="btn" id="delete-cancel">Cancel</button>
      </div>`);

    const input = modal.panel.querySelector('#delete-confirm-name');
    const confirmBtn = modal.panel.querySelector('#delete-confirm');
    modal.panel.querySelector('#delete-cancel').addEventListener('click', modal.close);
    // Enable delete only on an exact name match.
    input.addEventListener('input', () => { confirmBtn.disabled = input.value !== name; });
    input.focus();

    confirmBtn.addEventListener('click', async () => {
      if (input.value !== name) return; // guard against a programmatic click
      confirmBtn.disabled = true;
      try {
        await deleteApp(pid, name);
        // Navigate back to the catalog. replaceState (not push) so the
        // browser Back button doesn't return to a now-404 detail page.
        window.history.replaceState({}, '', '/ui/apps');
        if (typeof window.renderRoute === 'function') window.renderRoute();
      } catch (err) {
        if (String(err.message) === 'not authenticated') return;
        const dmsg = modal.panel.querySelector('#delete-modal-msg');
        if (dmsg) dmsg.innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
        confirmBtn.disabled = false;
      }
    });
  }

  const deleteBtn = document.getElementById('app-delete-btn');
  if (deleteBtn) deleteBtn.addEventListener('click', openDeleteModal);

  await reload();

  // --- Upload (moves the draft channel) ------------------------------
  const form = document.getElementById('app-upload-form');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = document.getElementById('app-upload-msg');
    msg.innerHTML = '';
    const file = new FormData(form).get('bundle');

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

// channelTarget renders a channel's target: the short hash (full hash on
// hover) + when it was set, or a muted note when the channel is unset.
function channelTarget(channel) {
  if (!channel || !channel.version_hash) {
    return `<span style="color: var(--fg-2);">not set</span>`;
  }
  return `
    <code class="inline" title="${esc(channel.version_hash)}">${esc(shortHash(channel.version_hash))}</code>
    ${channel.updated_at ? `<span style="color: var(--fg-2);">${timeTag(channel.updated_at)}</span>` : ''}`;
}

// versionRowHTML renders one version-history row. Every server value
// (hash, size, timestamp) is escaped even though hashes are hex — defence
// in depth, not a control this page relies on. `ch` is the app's channel
// map: the row whose hash is production is already live (no button, a
// production pill); every other row gets a Promote button (promoting an
// EARLIER row = rollback, the same CAS-guarded call).
function versionRowHTML(v, ch) {
  const current = (ch.production && ch.production.version_hash) || null;
  const isProd = current && v.hash === current;
  const action = isProd
    ? ''
    : `<button type="button" class="btn btn--ghost" data-promote="${esc(v.hash)}">Promote</button>`;
  return `
    <tr>
      <td><code class="mono" title="${esc(v.hash)}">${esc(shortHash(v.hash))}</code></td>
      <td>${esc(formatBytes(v.size_bytes))}</td>
      <td>${timeTag(v.created_at)}</td>
      <td>${channelPillsForHash(v.hash, ch)}</td>
      <td>${action}</td>
    </tr>`;
}

// channelPillsForHash returns the production/draft pills for whichever of
// the app's channels point at `hash` (a version may be both).
function channelPillsForHash(hash, ch) {
  const pills = [];
  if (ch.production && ch.production.version_hash === hash) pills.push('<span class="pill pill--ok">production</span>');
  if (ch.draft && ch.draft.version_hash === hash) pills.push('<span class="pill pill--running">draft</span>');
  return pills.join(' ');
}

// shortHash mirrors cmd/datuplet/apps.go's shortHash (12 hex chars) and
// apps.js's, so the CLI and both UI pages abbreviate identically.
function shortHash(h) {
  return h && h.length > 12 ? h.slice(0, 12) : (h || '');
}

// isConflict reports whether an api() error is a 409 CAS mismatch. api()
// throws Error(`${status}: ${body}`), so the message always begins with
// the numeric status; `^409\b` matches the "409:" prefix precisely.
function isConflict(err) {
  return /^409\b/.test(String(err && err.message));
}

// fileToBase64 reads a File's raw bytes and resolves the base64 text
// putApp's {"bundle_base64": "..."} body expects — identical to apps.js.
// readAsDataURL is the simplest cross-browser route to base64 for a file
// this size; the "data:<mime>;base64," prefix is sliced off.
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

// showModal renders a dismissible modal into `host` using the same
// .catalog-back/.catalog idiom pipeline-detail.js uses, so the look and
// click-outside-to-close behaviour match the existing catalog modal.
// ariaLabel is a fixed string (escaped defensively). Returns { close,
// panel } — panel is the .catalog element for wiring the modal's controls.
function showModal(host, ariaLabel, innerHTML) {
  host.innerHTML = `
    <div class="catalog-back" data-modal-back>
      <div class="catalog" role="dialog" aria-modal="true" aria-label="${esc(ariaLabel)}">${innerHTML}</div>
    </div>`;
  const back = host.querySelector('[data-modal-back]');
  const close = () => { host.innerHTML = ''; };
  back.addEventListener('click', (e) => { if (e.target === back) close(); });
  return { close, panel: host.querySelector('.catalog') };
}
