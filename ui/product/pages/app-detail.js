// App detail (RFC 028 §5.5, Part 6, tasks U1 + U2). One app's lifecycle
// surface for project members: which version each channel points at,
// the full version history, uploading a new draft bundle, the
// CAS-guarded promote/rollback + typed-name delete (U1), plus viewer-token
// management, the render-log viewer, and an inline draft preview (U2).
//
// Backed by pkg/pipelineapi/apps/handlers.go's author routes (via the
// api.js wrappers):
//   GET    /api/v1/projects/{pid}/apps/{name}          -> detail
//   PUT    /api/v1/projects/{pid}/apps/{name}          {bundle_base64} -> draft
//   POST   /api/v1/projects/{pid}/apps/{name}/promote  {version, expectedProduction}
//   DELETE /api/v1/projects/{pid}/apps/{name}          -> 204
//   GET    /api/v1/projects/{pid}/apps/{name}/logs[?request_id=]   -> render log(s)
//   POST   /api/v1/projects/{pid}/apps/{name}/tokens   -> {token_id, token} (once)
//   GET    /api/v1/projects/{pid}/apps/{name}/tokens   -> [{token_id, created_at, revoked_at}]
//   DELETE /api/v1/projects/{pid}/apps/{name}/tokens/{token_id}    -> 204
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
// This applies with extra force to two U2 fields: a viewer token's
// plaintext secret (`vw_<id>.<secret>`, returned exactly once by
// createAppToken and never persisted past its one-time modal) and a
// render log's `log_text`/`error` (captured console output from
// author-supplied app code — the guest is untrusted, so this text is
// hostile by construction and is escaped like any other attacker-
// controlled string, never treated as trusted just because it came from
// "our own" app).
//
// The draft-preview iframe (U2) points at the app-worker's `@draft`
// route on THIS SAME ORIGIN — never an absolute/external URL — which is
// exactly what the shell's CSP `frame-ancestors 'self'` (spec §6.4)
// permits: same-origin framing only, using the viewer's platform session
// cookie (no viewer token involved). See draftPreviewURL() below.

import {
  esc, getApp, putApp, deleteApp, promoteApp,
  createAppToken, listAppTokens, deleteAppToken, getAppLogs,
} from '/ui/api.js';
import { emptyState, skeletonRows } from '/ui/components.js';
import { timeTag, formatBytes } from '/ui/format.js';
import * as icons from '/ui/icons.js';

// Mirrors pkg/pipelineapi/apps.MaxBundleBytes (store.go) — a friendly
// client-side pre-check so an obviously-oversize file never leaves the
// browser. The server enforces this authoritatively (413) regardless.
const MAX_BUNDLE_BYTES = 5 * 1024 * 1024;

// Versions table has 5 columns: Version, Size, Created, Channels, action.
const VERSION_COLS = 5;

// Tokens table has 4 columns: Token ID, Created, Status, action.
const TOKENS_COLS = 4;

// Logs table has 4 columns: Time, Outcome, Duration, (expand toggle).
const LOGS_COLS = 4;

// Default page size for the "recent renders" list. Deliberately small:
// each record's log_text can be up to 64 KiB (spec §6.6), and the list
// route has no limit unless one is passed (server default is
// MaxRenderLogsPerApp = 200 — see pkg/pipelineapi/apps/store.go) — this
// caps the worst-case payload/DOM footprint for the default view.
// getAppLogs(...,{requestId}) (the filter path) is unaffected by this.
const LOGS_LIST_LIMIT = 20;

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

    <h2 class="section-h">Draft preview</h2>
    <p class="hint">
      Live, inline render of the <b>draft</b> channel — the same route
      <code class="inline">datuplet apps render --channel draft</code> hits.
      Works because the app's page allows same-origin framing and this
      browser already carries your platform session.
    </p>
    <div id="app-draft-preview"><p aria-busy="true">Loading…</p></div>

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

    <h2 class="section-h">Viewer tokens</h2>
    <p class="hint">
      Bearer tokens for viewer access outside a platform session (embedding,
      <code class="inline">curl</code>, <code class="inline">datuplet apps render</code>).
      A token's secret is shown exactly once, right after you create it —
      copy it then; it cannot be displayed again. See
      <code class="inline">datuplet apps token</code>.
    </p>
    <div id="app-token-msg"></div>
    <table class="table">
      <thead>
        <tr><th>Token ID</th><th>Created</th><th>Status</th><th></th></tr>
      </thead>
      <tbody id="app-tokens-body">${skeletonRows(2, TOKENS_COLS)}</tbody>
    </table>
    <div class="actions" style="margin-top: var(--s-3);">
      <button type="button" id="app-token-create-btn" class="btn btn--primary">Create token</button>
    </div>

    <h2 class="section-h">Render logs</h2>
    <p class="hint">Most recent renders, newest first. Click a row for its captured console output and error detail.</p>
    <form id="app-logs-filter-form" style="display:flex; align-items:flex-end; gap: var(--s-3); flex-wrap: wrap; margin-bottom: var(--s-3);">
      <label class="field" style="flex: 1; min-width: 220px; margin-bottom: 0;">Filter by request ID
        <input class="input input--mono" type="text" id="app-logs-filter-input" name="request_id" placeholder="e.g. from a CLI error or an author_log lookup" autocomplete="off" spellcheck="false">
      </label>
      <button type="submit" class="btn btn--secondary">Search</button>
      <button type="button" id="app-logs-clear-btn" class="btn btn--ghost" hidden>Clear filter</button>
    </form>
    <table class="table">
      <thead>
        <tr><th>Time</th><th>Outcome</th><th>Duration</th><th></th></tr>
      </thead>
      <tbody id="app-logs-body">${skeletonRows(3, LOGS_COLS)}</tbody>
    </table>

    <div id="app-modal-host"></div>
  `;

  const channelsEl = document.getElementById('app-channels');
  const versionsBody = document.getElementById('app-versions-body');
  const detailMsg = document.getElementById('app-detail-msg');
  const modalHost = document.getElementById('app-modal-host');
  const previewEl = document.getElementById('app-draft-preview');
  const tokensBody = document.getElementById('app-tokens-body');
  const logsBody = document.getElementById('app-logs-body');

  // The single source of truth for what's on screen. reload() replaces
  // it wholesale; renderChannels/renderVersions/renderDraftPreview and the
  // promote CAS all read from it, so `expectedProduction` is always the
  // hash the author is currently looking at.
  let appData = null;
  // U2 state — each independent of appData, fetched by its own route.
  let tokenList = [];           // listAppTokens result; refreshed after create/revoke.
  let logRecords = [];          // getAppLogs result (list or single-record, always an array here).
  let previewedDraftHash;       // undefined until first paint; see renderDraftPreview().
  let currentLogFilter = null;  // null = recent-renders list; a string = the active request-id filter.

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

  // renderDraftPreview (re)paints the draft-preview iframe ONLY when the
  // draft's version_hash actually changed since the last paint (tracked
  // via previewedDraftHash), not on every reload(). This is deliberate,
  // not an optimization afterthought: replacing the iframe's innerHTML
  // always creates a brand-new <iframe> element, and a fresh element with
  // an src attribute always issues a fresh navigation — even when the src
  // string is byte-identical to before (recreating the node, not mutating
  // .src, is what forces the reload). That means an unconditional repaint
  // on every reload() would re-render the app every time this page loads
  // or a promote happens — and every render here is itself a real render
  // the app-worker appends to the very render-log ring buffer this page's
  // Render Logs section shows, so unconditional repainting would spam it.
  // Gating on an actual hash change means the preview reloads exactly
  // when a same-page upload changes what "draft" means, and stays put on
  // reloads that don't (e.g. a promote, which only moves production).
  function renderDraftPreview() {
    const hash = draftHash();
    if (hash === previewedDraftHash) return;
    previewedDraftHash = hash;
    if (!hash) {
      previewEl.innerHTML = `<p class="hint">No draft version yet — upload a bundle below to preview it here.</p>`;
      return;
    }
    // esc() around an encodeURIComponent-built URL is a defensive no-op
    // (draftPreviewURL's own segments can never contain " < > &), applied
    // uniformly per this file's existing "escape every interpolated value"
    // discipline rather than because this specific value needs it.
    previewEl.innerHTML = `
      <iframe
        title="Draft preview of ${esc(name)}"
        src="${esc(draftPreviewURL(pid, name))}"
        style="width:100%; height:480px; border:1px solid var(--border); border-radius: var(--radius); background: var(--bg-1);"
      ></iframe>`;
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
      previewEl.innerHTML = '';
      previewedDraftHash = undefined; // force a real repaint once loading recovers, even to the same hash
      return;
    }
    renderChannels();
    renderVersions();
    renderDraftPreview();
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

  // --- Viewer tokens ---------------------------------------------------
  // Independent of appData — its own route (listAppTokens), its own
  // local list — refreshed only after a create/revoke on THIS page, same
  // no-polling posture as the rest of the page.
  function renderTokenRows() {
    if (!tokenList || tokenList.length === 0) {
      tokensBody.innerHTML = `<tr><td colspan="${TOKENS_COLS}" style="color: var(--fg-2); text-align:center; padding: var(--s-5);">No viewer tokens yet.</td></tr>`;
      return;
    }
    tokensBody.innerHTML = tokenList.map(tokenRowHTML).join('');
  }

  async function loadTokens() {
    try {
      tokenList = await listAppTokens(pid, name);
      if (aborted()) return;
    } catch (err) {
      if (aborted() || String(err.message) === 'not authenticated') return;
      tokensBody.innerHTML = `<tr><td colspan="${TOKENS_COLS}"><div class="callout callout--warn">Failed to load tokens: ${esc(err.message)}</div></td></tr>`;
      return;
    }
    renderTokenRows();
  }

  // Delegated on the tbody (loadTokens() replaces its innerHTML wholesale
  // on every refresh, same reasoning as versionsBody above).
  tokensBody.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-revoke-token]');
    if (!btn) return;
    openRevokeTokenModal(btn.getAttribute('data-revoke-token'));
  });

  async function createToken() {
    const msg = document.getElementById('app-token-msg');
    msg.innerHTML = '';
    const btn = document.getElementById('app-token-create-btn');
    btn.disabled = true;
    try {
      const created = await createAppToken(pid, name);
      if (aborted()) return;
      // created.token (`vw_<id>.<secret>`) is handed straight to the
      // one-time modal as a plain function argument — it is never
      // assigned to appData/tokenList or any variable that outlives this
      // call, never logged, and never placed in a URL. The modal's own
      // closure is its only home, and that closure (and the DOM it's
      // wired to) is discarded when the modal closes.
      openTokenCreatedModal(created.token);
      await loadTokens();
    } catch (err) {
      if (String(err.message) !== 'not authenticated') {
        msg.innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
      }
    } finally {
      btn.disabled = false;
    }
  }

  // openTokenCreatedModal shows a freshly minted token's plaintext EXACTLY
  // ONCE — the 201 response from createAppToken is the only place it ever
  // transits (spec §5.3; task-P2-report.md). `secret` lives only in this
  // function's parameter and the two click closures below it; nothing
  // here writes it into appData/tokenList, console, or a URL, and the
  // whole subtree (input + buttons + their listeners) is torn down when
  // the modal closes.
  function openTokenCreatedModal(secret) {
    const modal = showModal(modalHost, 'Viewer token created', `
      <h3>Viewer token created</h3>
      <div class="callout callout--warn">This secret is shown once. Copy it now — it cannot be shown again. If you lose it, revoke this token below and create a new one.</div>
      <label class="field">Token
        <input class="input input--mono" type="text" id="new-token-value" readonly spellcheck="false">
      </label>
      <div class="cat-actions">
        <button type="button" class="btn btn--primary" id="token-copy-btn">Copy</button>
        <button type="button" class="btn" id="token-done-btn">Done</button>
      </div>`);

    // Set via the DOM property rather than baked into the HTML string, so
    // the secret never round-trips through esc()/HTML-attribute quoting
    // at all — one less place that has to handle it correctly (esc()
    // would also have been safe here; this is simply the more direct
    // path for a value this sensitive).
    const input = modal.panel.querySelector('#new-token-value');
    input.value = secret;
    input.addEventListener('focus', () => input.select());

    const copyBtn = modal.panel.querySelector('#token-copy-btn');
    const originalLabel = copyBtn.textContent;
    copyBtn.addEventListener('click', async () => {
      try {
        await navigator.clipboard.writeText(secret);
        copyBtn.textContent = 'Copied';
      } catch {
        copyBtn.textContent = 'Copy failed';
      }
      setTimeout(() => { copyBtn.textContent = originalLabel; }, 1500);
    });
    modal.panel.querySelector('#token-done-btn').addEventListener('click', modal.close);
  }

  // openRevokeTokenModal mirrors openPromoteModal's confirm-then-act shape:
  // revoking cuts off live viewer access within ~15s (spec §5.3) — a real,
  // if reversible-via-a-new-token, consequence — so it gets the same
  // confirm step as promote rather than a bare click-to-revoke.
  function openRevokeTokenModal(tokenId) {
    const modal = showModal(modalHost, 'Revoke viewer token', `
      <h3>Revoke token <code class="inline">${esc(tokenId)}</code>?</h3>
      <p class="hint">Anyone using this token loses access within about 15 seconds. This cannot be undone — create a new token if the viewer still needs access.</p>
      <div id="revoke-modal-msg"></div>
      <div class="cat-actions">
        <button type="button" class="btn btn--danger" id="revoke-confirm">Revoke</button>
        <button type="button" class="btn" id="revoke-cancel">Cancel</button>
      </div>`);

    modal.panel.querySelector('#revoke-cancel').addEventListener('click', modal.close);
    const confirmBtn = modal.panel.querySelector('#revoke-confirm');
    confirmBtn.addEventListener('click', async () => {
      confirmBtn.disabled = true;
      try {
        await deleteAppToken(pid, name, tokenId);
        if (aborted()) return;
        modal.close();
        await loadTokens();
      } catch (err) {
        if (String(err.message) === 'not authenticated') return;
        const rmsg = modal.panel.querySelector('#revoke-modal-msg');
        if (rmsg) rmsg.innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
        confirmBtn.disabled = false;
      }
    });
  }

  // --- Render logs -------------------------------------------------------
  // logRecords always holds an array of renderLogJSON-shaped records.
  // getAppLogs(...,{requestId}) resolves to a single object (not an
  // array) for that route — loadLogs wraps it in a one-element array so
  // renderLogRows has exactly one shape to render regardless of source.
  // Both the list and the by-id record already carry the full
  // log_text/error (spec §6.6) — there is no separate per-row fetch on
  // row expand, it's already client-side.
  function renderLogRows() {
    if (!logRecords || logRecords.length === 0) {
      logsBody.innerHTML = `<tr><td colspan="${LOGS_COLS}" style="color: var(--fg-2); text-align:center; padding: var(--s-5);">No renders yet.</td></tr>`;
      return;
    }
    // Auto-expand the sole row when it's the result of an explicit
    // request-id search — the author searched for ONE specific record,
    // so showing its detail immediately beats requiring a second click.
    const autoExpand = logRecords.length === 1 && currentLogFilter !== null;
    logsBody.innerHTML = logRecords.map((rec) => logRowHTML(rec, autoExpand)).join('');
  }

  async function loadLogs({ requestId } = {}) {
    try {
      if (requestId) {
        const rec = await getAppLogs(pid, name, { requestId });
        if (aborted()) return;
        logRecords = [rec];
      } else {
        logRecords = await getAppLogs(pid, name, { limit: LOGS_LIST_LIMIT });
        if (aborted()) return;
      }
    } catch (err) {
      if (aborted() || String(err.message) === 'not authenticated') return;
      const prefix = requestId ? `No render log for request ID “${esc(requestId)}”: ` : 'Failed to load render logs: ';
      logsBody.innerHTML = `<tr><td colspan="${LOGS_COLS}"><div class="callout callout--warn">${prefix}${esc(err.message)}</div></td></tr>`;
      return;
    }
    currentLogFilter = requestId || null;
    const clearBtn = document.getElementById('app-logs-clear-btn');
    if (clearBtn) clearBtn.hidden = !currentLogFilter;
    renderLogRows();
  }

  // Delegated on the tbody (renderLogRows() replaces its innerHTML
  // wholesale on every load/filter, same reasoning as versionsBody
  // above). Each record renders as a fixed [summary row, detail row]
  // pair with the detail directly following its summary, so toggling
  // needs no separate index lookup — just the clicked row's next sibling.
  logsBody.addEventListener('click', (e) => {
    const row = e.target.closest('tr[data-log-row]');
    if (!row) return;
    const detail = row.nextElementSibling;
    if (!detail) return;
    detail.hidden = !detail.hidden;
    const indicator = row.querySelector('[data-log-toggle]');
    if (indicator) indicator.textContent = detail.hidden ? 'Details' : 'Hide';
  });

  const deleteBtn = document.getElementById('app-delete-btn');
  if (deleteBtn) deleteBtn.addEventListener('click', openDeleteModal);

  const createTokenBtn = document.getElementById('app-token-create-btn');
  createTokenBtn.addEventListener('click', createToken);

  const logsFilterForm = document.getElementById('app-logs-filter-form');
  const logsFilterInput = document.getElementById('app-logs-filter-input');
  logsFilterForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const requestId = logsFilterInput.value.trim();
    await loadLogs(requestId ? { requestId } : {});
  });
  document.getElementById('app-logs-clear-btn').addEventListener('click', async () => {
    logsFilterInput.value = '';
    await loadLogs();
  });

  // Independent fetches — none depends on another's result — so they run
  // concurrently. Each is guarded by its own aborted() check internally.
  await Promise.all([reload(), loadTokens(), loadLogs()]);

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

// tokenRowHTML renders one viewer-token row. t is a tokenSummaryJSON
// object — {token_id, created_at, revoked_at} — which NEVER carries a
// secret or salt (the SQL backing listAppTokens does not even select
// those columns; task-C3b-tokenlist-report.md). token_id is a UUID, not
// a content hash, so — unlike version hashes — it is shown in full, not
// shortened.
function tokenRowHTML(t) {
  const active = !t.revoked_at;
  const status = active
    ? '<span class="pill pill--ok">active</span>'
    : '<span class="pill pill--cancelled">revoked</span>';
  const action = active
    ? `<button type="button" class="btn btn--ghost" data-revoke-token="${esc(t.token_id)}">Revoke</button>`
    : '';
  return `
    <tr>
      <td><code class="mono">${esc(t.token_id)}</code></td>
      <td>${timeTag(t.created_at)}</td>
      <td>${status}</td>
      <td>${action}</td>
    </tr>`;
}

// logRowHTML renders one render-log record (spec §6.6) as a [summary row,
// detail row] pair — the detail row starts `hidden` unless autoExpand.
// request_id/version_hash/channel/outcome/started_at/duration_ms are
// constrained, server-controlled shapes (UUID/hex/enum/timestamp/int);
// log_text and error are NOT — they are console output and error text
// from author-supplied, untrusted app code (the guest never sees a
// credential, per spec §6.6, but is otherwise free to log anything,
// including markup) — so both go through esc() exactly like every other
// value here, but are called out specifically because treating "our own
// app's output" as safe-by-association is the mistake this guards
// against. Nothing here uses innerHTML with an unescaped interpolation.
function logRowHTML(rec, autoExpand) {
  const outcomeClass = rec.outcome === 'ok' ? 'pill--ok' : 'pill--fail';
  const channelPillClass = rec.channel === 'production' ? 'pill--ok' : 'pill--running';
  const logBody = rec.log_text
    ? esc(rec.log_text)
    : '<span style="color: var(--fg-2);">No output captured.</span>';
  return `
    <tr data-log-row style="cursor:pointer;">
      <td>${timeTag(rec.started_at)}</td>
      <td><span class="pill ${outcomeClass}">${esc(rec.outcome)}</span></td>
      <td>${esc(formatMs(rec.duration_ms))}</td>
      <td><span data-log-toggle style="color: var(--fg-1); font-size: var(--text-xs);">${autoExpand ? 'Hide' : 'Details'}</span></td>
    </tr>
    <tr data-log-detail ${autoExpand ? '' : 'hidden'}>
      <td colspan="${LOGS_COLS}">
        <div style="margin-bottom: var(--s-2); color: var(--fg-2); font-size: var(--text-xs);">
          Request <code class="inline">${esc(rec.request_id)}</code>
          &middot; version <code class="inline" title="${esc(rec.version_hash)}">${esc(shortHash(rec.version_hash))}</code>
          &middot; channel <span class="pill ${channelPillClass}">${esc(rec.channel)}</span>
        </div>
        ${rec.error ? `<div class="callout callout--warn" style="margin-bottom: var(--s-2);">${esc(rec.error)}</div>` : ''}
        <pre style="white-space: pre-wrap; overflow-wrap: anywhere; font-family: var(--font-mono); font-size: var(--text-sm); background: var(--bg-1); border: 1px solid var(--border); border-radius: var(--radius); padding: var(--s-3); margin: 0; max-height: 320px; overflow-y: auto;">${logBody}</pre>
      </td>
    </tr>`;
}

// formatMs renders a render duration for display. Deliberately NOT
// format.js's formatDuration: that helper is built for pipeline RUN
// durations spanning seconds to hours and collapses anything under a
// second to "0:00", which would erase precision for the common case here
// — app renders are capped at 30s max (spec §7), so sub-second precision
// matters more than hour-scale range.
function formatMs(ms) {
  if (ms == null || ms < 0) return '—';
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

// draftPreviewURL builds the app-worker's @draft route from known-safe
// pieces only — pid and the (already route-validated) app name — mirroring
// cmd/datuplet/apps.go's appsRenderURL exactly: each segment is escaped
// FIRST, and the literal "@draft" marker is appended AFTER escaping, so
// '@' is never itself percent-encoded (the worker splits {name} on a
// literal '@', spec §4.1/W6 — running the whole "name@draft" through
// encodeURIComponent as one string would turn '@' into '%40'; matching
// the CLI's exact approach avoids relying on encode/decode round-tripping
// being lossless for that character). Root-relative — no origin prefix —
// because this URL is only ever used as this same page's own iframe src;
// the leading single '/' also guarantees the browser can only resolve it
// against the current origin (a leading '//' would be protocol-relative
// and could point elsewhere, which is exactly why pid/name are encoded
// per-segment rather than concatenated into one string that could smuggle
// a '/').
function draftPreviewURL(projectId, appName) {
  return `/apps/${encodeURIComponent(projectId)}/${encodeURIComponent(appName)}@draft`;
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
