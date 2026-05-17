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
 * Iceberg schema fields for the table's current snapshot.
 * Returns { columns: [{ id, name, type, nullable }, ...] }.
 */
export async function getTableSchema(projectId, namespace, name) {
  return api(`/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/schema`);
}

/**
 * First N rows (server-capped at 100) plus column types. May set
 * truncated=true when row or byte cap fires.
 */
export async function getTablePreview(projectId, namespace, name) {
  return api(`/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/preview`);
}

/**
 * Snapshot history for the table, sorted newest-first.
 * Returns [{snapshot_id, committed_at, actor, run_id, run_mode,
 *           pipeline_api, added_records}, ...].
 * Pre-RFC-013 snapshots have empty audit fields.
 */
export async function getTableSnapshots(projectId, namespace, name) {
  return api(`/api/v1/storage/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/snapshots`);
}
