// Thin fetch wrapper. Sends the session cookie, parses JSON, throws on
// non-2xx with a useful message. On 401 it redirects to /ui/login
// before throwing so every caller doesn't have to check auth itself.
//
// Imported by pages/*.js and by app.js. The app module exports
// `goToLogin` on window so api.js can call it without a circular
// import.

export async function api(path, opts = {}) {
  const init = {
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    ...opts,
  };
  const r = await fetch(path, init);
  if (r.status === 401) {
    if (typeof window.__datupletGoToLogin === 'function') {
      window.__datupletGoToLogin();
    }
    throw new Error('not authenticated');
  }
  if (!r.ok) {
    const body = await r.text();
    throw new Error(`${r.status}: ${body || r.statusText}`);
  }
  if (r.status === 204) return null;
  const ct = r.headers.get('content-type') || '';
  if (ct.includes('application/json')) return r.json();
  return r.text();
}

// putYAML sends a raw YAML body. Servers that want text/* MIME still
// see the body we pass; callers control the Content-Type for clarity.
export async function putYAML(path, yamlText) {
  const r = await fetch(path, {
    method: 'PUT',
    credentials: 'include',
    headers: { 'Content-Type': 'application/yaml' },
    body: yamlText,
  });
  if (r.status === 401) {
    if (typeof window.__datupletGoToLogin === 'function') {
      window.__datupletGoToLogin();
    }
    throw new Error('not authenticated');
  }
  if (!r.ok) throw new Error(`${r.status}: ${await r.text()}`);
  if (r.status === 204) return null;
  const ct = r.headers.get('content-type') || '';
  if (ct.includes('application/json')) return r.json();
  return null;
}

// putPipelineYAML wraps the pipeline-save PUT so callers get the RFC 026 §7
// findings contract as data, not a thrown string. Resolves:
//   { ok:true }                          — 204 (clean save)
//   { ok:true,  findings:[...] }         — 200 (saved with warnings)
//   { ok:false, findings:[...] }         — 400 { error, findings } (rejected)
// Throws (like api()) on 401 → login redirect, and on any other non-2xx
// with no findings body (so genuine server/transport errors still surface).
export async function putPipelineYAML(projectId, name, yamlText) {
  const r = await fetch(
    `/api/v1/projects/${encodeURIComponent(projectId)}/pipelines/${encodeURIComponent(name)}`,
    {
      method: 'PUT',
      credentials: 'include',
      headers: { 'Content-Type': 'application/yaml' },
      body: yamlText,
    },
  );
  if (r.status === 401) {
    if (typeof window.__datupletGoToLogin === 'function') window.__datupletGoToLogin();
    throw new Error('not authenticated');
  }
  if (r.status === 204) return { ok: true };
  const ct = r.headers.get('content-type') || '';
  const raw = await r.text();                 // read the stream ONCE
  let body = null;
  if (ct.includes('application/json') && raw) {
    try { body = JSON.parse(raw); } catch { /* non-JSON or malformed → leave null */ }
  }
  const findings = body && Array.isArray(body.findings) ? body.findings : null;
  if (r.status === 200) return { ok: true, findings: findings || [] };
  if (r.status === 400 && findings) return { ok: false, findings };
  // Non-findings error (413, 500, name mismatch returned as plain text, …).
  throw new Error(`${r.status}: ${(body && body.error) || raw || r.statusText}`);
}

// Small HTML-escape helper for innerHTML use. Not a substitute for
// content-aware templating, but safer than string concatenation.
export function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

// ----- Storage endpoints (RFC 005) -----
//
// All four require an authenticated session and project membership.
// The api() wrapper handles the 401 → redirect-to-login path and
// JSON-decodes the response. Each helper returns the parsed body
// (or throws on non-2xx).

/**
 * List Iceberg tables under the given project's warehouse prefix.
 * Returns { tables: [{ namespace, name, current_snapshot_id }, ...] }.
 */
export async function getStorageCatalog(projectId) {
  return api(`/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables`);
}

/**
 * Snapshot list, current snapshot, data-file paths, row count for a
 * single table. See docs/pipeline-api.md for the response shape.
 */
export async function getTableInfo(projectId, namespace, name) {
  return api(`/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/info`);
}

/**
 * Permanently delete a table: catalog metadata AND its data files. There is
 * no undo. Requires FGA data_admin on the project (a plain member gets 403),
 * and 501 when the instance has no lakekeeper catalog.
 *
 * The caller is responsible for confirming intent with the user first — the
 * server intentionally takes no confirmation parameter.
 */
export async function deleteTable(projectId, namespace, name) {
  return api(`/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`, {
    method: 'DELETE',
  });
}

/**
 * Iceberg schema fields for the table's current snapshot.
 * Returns { columns: [{ id, name, type, nullable }, ...] }.
 */
export async function getTableSchema(projectId, namespace, name) {
  return api(`/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/schema`);
}

/**
 * First N rows (server-capped at 100) plus column types. May set
 * truncated=true. Errors carry .status and .kind (query_disabled /
 * rate_limited / capacity / result_too_large / sql_error / timeout) so the
 * page can render actionable states instead of a generic failure.
 */
export async function getTablePreview(projectId, namespace, name) {
  const r = await fetch(
    `/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/preview`,
    { credentials: 'include' },
  );
  if (r.status === 401) {
    if (typeof window.__datupletGoToLogin === 'function') window.__datupletGoToLogin();
    throw new Error('not authenticated');
  }
  const ct = r.headers.get('content-type') || '';
  const payload = ct.includes('application/json') ? await r.json() : await r.text();
  if (!r.ok) {
    const err = new Error((payload && payload.error) || `preview failed (HTTP ${r.status})`);
    err.status = r.status;
    err.kind = payload && payload.kind;
    throw err;
  }
  return payload;
}

/**
 * Snapshot history for the table, sorted newest-first.
 * Returns [{snapshot_id, committed_at, actor, run_id, pipeline_api,
 *           added_records, added_files_size}, ...].
 * Pre-RFC-013 snapshots have empty audit fields.
 */
export async function getTableSnapshots(projectId, namespace, name) {
  return api(`/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/snapshots`);
}

// ----- Component registry (RFC 026 §4.1, §4.7) -----
//
// Read-only catalog of ComponentDefinitions. Readable by any authenticated
// project member (the catalog is the shared component picker). Both return
// the parsed body or throw via api() on non-2xx / 401.

/**
 * List registered components.
 * Returns [{ name, displayName, description, deprecated, defaultVersion,
 *            versions: [{ version, prerelease, image }] }, ...].
 */
export async function getComponents() {
  return api('/api/v1/components');
}

/**
 * One component with per-version configSchema (JSON Schema draft 2020-12,
 * as a string). Same top-level shape as a list item plus
 * versions[].configSchema.
 */
export async function getComponent(name) {
  return api(`/api/v1/components/${encodeURIComponent(name)}`);
}

// ----- Secrets endpoints (RFC 026 P1.5) -----
//
// Write-only: the server never returns secret values, only key +
// updatedAt. All three go through the api() wrapper for consistent
// auth/401 and error handling.

/**
 * List secret names + last-updated timestamps for a project.
 * Returns [{ key, updatedAt }, ...]. Values are never included.
 */
export async function listSecrets(projectId) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/secrets`);
}

/**
 * Create or overwrite a secret's value. 204 on success.
 */
export async function putSecret(projectId, key, value) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/secrets/${encodeURIComponent(key)}`, {
    method: 'PUT',
    body: JSON.stringify({ value }),
  });
}

/**
 * Delete a secret. 204 on success.
 */
export async function deleteSecret(projectId, key) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/secrets/${encodeURIComponent(key)}`, {
    method: 'DELETE',
  });
}

// runQuery submits ad-hoc SQL to the server-side query service (RFC 022 mode a)
// and returns the queryengine Result JSON: { schema:[{name,type}], rows:[[...]],
// truncated:bool, stats:{duration_ms,...} }.
//
// The route is project-scoped (RFC 025 §4.6): pipeline-api enforces FGA
// datuplet_member on `projectId` and resolves the lakekeeper warehouse for
// that project per request. `opts` may carry { timeoutS, maxRows, maxBytes }
// (clamped server-side).
//
// This does NOT use the api() wrapper: query errors (400 sql_error, 403,
// 408 timeout, 429 rate_limited) are EXPECTED outcomes the console renders
// inline, so we surface the HTTP status + the {error,kind} envelope rather than
// flattening everything to a string. It still reuses the wrapper's 401 →
// redirect-to-login contract. On non-2xx it throws an Error augmented with
// `.status`, `.kind`, and `.retryAfter` so the console can map RFC 022 §10 codes.
export async function runQuery(projectId, sql, opts = {}) {
  const body = { sql };
  if (opts.timeoutS != null) body.timeout_s = opts.timeoutS;
  if (opts.maxRows != null) body.max_rows = opts.maxRows;
  if (opts.maxBytes != null) body.max_bytes = opts.maxBytes;

  const r = await fetch(`/api/v1/projects/${encodeURIComponent(projectId)}/query`, {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (r.status === 401) {
    if (typeof window.__datupletGoToLogin === 'function') {
      window.__datupletGoToLogin();
    }
    throw new Error('not authenticated');
  }
  const ct = r.headers.get('content-type') || '';
  const payload = ct.includes('application/json') ? await r.json() : await r.text();
  if (!r.ok) {
    const err = new Error(
      (payload && payload.error) || `query failed (HTTP ${r.status})`,
    );
    err.status = r.status;
    err.kind = payload && payload.kind;
    err.retryAfter = r.headers.get('Retry-After');
    throw err;
  }
  return payload; // queryengine Result
}

// ----- User apps endpoints (RFC 028 §5.1, Part 6) -----
//
// Author-facing control-plane routes for user apps
// (pkg/pipelineapi/apps/handlers.go). All go through the api() wrapper
// above for consistent auth/401 handling and JSON (de)serialization —
// no second fetch path. See that file's wire-shape doc comments
// (appJSON/channelJSON/versionJSON/tokenResponse/tokenSummaryJSON/
// renderLogJSON) for the authoritative field list; summarized per
// function below.

/**
 * List apps in a project.
 * Returns [{ app_id, name, created_at,
 *            channels: { production?: {version_hash, updated_at},
 *                        draft?: {version_hash, updated_at} } }, ...].
 * A channel key is absent, never null, until something points at it.
 */
export async function listApps(projectId) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps`);
}

/**
 * One app's detail: channels (as above) plus the full version history,
 * newest first. Returns { app_id, name, created_at, channels,
 * versions: [{hash, size_bytes, created_at}, ...] }. Throws (404) if
 * the app doesn't exist.
 */
export async function getApp(projectId, name) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps/${encodeURIComponent(name)}`);
}

/**
 * Upload a bundle version: creates the app on first call, otherwise adds
 * a version and repoints the app's `draft` channel to it — `production`
 * is never touched here (see promoteApp). bundleBase64 is the RAW bundle
 * bytes, base64-encoded by the caller (pages/apps.js does this via
 * FileReader). Returns { app_id, version_hash }. Throws 413 if the raw
 * bundle exceeds the server's 5 MB cap, 409 on the per-project storage
 * quota.
 */
export async function putApp(projectId, name, bundleBase64) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps/${encodeURIComponent(name)}`, {
    method: 'PUT',
    body: JSON.stringify({ bundle_base64: bundleBase64 }),
  });
}

/**
 * Delete an app: revokes its FGA viewer identity first, then removes
 * every row (versions, channels, tokens, logs cascade). 204 on success.
 */
export async function deleteApp(projectId, name) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps/${encodeURIComponent(name)}`, {
    method: 'DELETE',
  });
}

/**
 * Compare-and-swap promote: repoints `production` to versionHash iff it
 * currently points at expectedProduction (pass null/undefined for a
 * first promote, when production isn't set yet). Returns
 * { production_version }. Throws 409 if production moved since
 * expectedProduction was read (re-fetch and retry), 400 for an unknown
 * version hash.
 */
export async function promoteApp(projectId, name, versionHash, expectedProduction) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps/${encodeURIComponent(name)}/promote`, {
    method: 'POST',
    body: JSON.stringify({ version: versionHash, expectedProduction: expectedProduction ?? null }),
  });
}

/**
 * Render logs. Omit requestId for the newest-first list (optionally
 * capped by limit); pass requestId for a single record — throws 404 if
 * no record with that id exists (never ran, or aged out of the per-app
 * ring buffer).
 */
export async function getAppLogs(projectId, name, { requestId, limit } = {}) {
  const params = new URLSearchParams();
  if (requestId) params.set('request_id', requestId);
  if (limit) params.set('limit', String(limit));
  const qs = params.toString();
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps/${encodeURIComponent(name)}/logs${qs ? `?${qs}` : ''}`);
}

/**
 * Mint a viewer token. The plaintext `token` (`vw_<token_id>.<secret>`)
 * is returned EXACTLY ONCE, here — no other route can ever surface it
 * again. Returns { token_id, token }.
 */
export async function createAppToken(projectId, name) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps/${encodeURIComponent(name)}/tokens`, {
    method: 'POST',
  });
}

/**
 * List viewer-token metadata — never a secret or salt.
 * Returns [{ token_id, created_at, revoked_at }, ...]; revoked_at is a
 * literal null for an active token, not an absent key.
 */
export async function listAppTokens(projectId, name) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps/${encodeURIComponent(name)}/tokens`);
}

/**
 * Revoke a viewer token. 204 on success; 404 if unknown or it belongs to
 * a different app.
 */
export async function deleteAppToken(projectId, name, tokenId) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/apps/${encodeURIComponent(name)}/tokens/${encodeURIComponent(tokenId)}`, {
    method: 'DELETE',
  });
}
