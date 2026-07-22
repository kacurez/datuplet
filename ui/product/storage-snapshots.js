// storage-snapshots.js — Snapshot history section for /ui/storage table pages.
//
// Exported function:
//   renderSnapshotHistory(root, projectId, namespace, table)
//
// Fetches /api/v1/storage/projects/{pid}/tables/{ns}/{t}/snapshots (newest-
// first, pre-sorted by the server) and appends a "Snapshot history" section
// to `root`.  Pre-RFC-013 snapshots (empty datuplet.* keys) render with
// placeholder dashes so history is always contiguous.

import { esc, getTableSnapshots } from '/ui/api.js';
import { timeTag, formatBytes } from '/ui/format.js';

/**
 * Fetch and render the snapshot history table into `root`.
 * Safe to call while the page is still loading other sections — any prior
 * content in `root` is not replaced; the section is appended.
 *
 * @param {Element} root       - Container to append the section into.
 * @param {string}  projectId  - Datuplet project UUID.
 * @param {string}  namespace  - Iceberg namespace.
 * @param {string}  tableName  - Iceberg table name.
 * @param {Function} [aborted] - Optional guard: if aborted() returns true the
 *                               render is skipped (stale route or tab switch).
 */
export async function renderSnapshotHistory(root, projectId, namespace, tableName, aborted) {
  const section = document.createElement('div');
  section.className = 'snapshot-history';
  section.innerHTML = '<h2 class="snapshot-history__title">Snapshot history</h2>' +
    '<p class="snapshot-history__loading" style="color: var(--fg-2);">Loading…</p>';
  root.appendChild(section);

  let rows;
  try {
    rows = await getTableSnapshots(projectId, namespace, tableName);
  } catch (e) {
    if (aborted && aborted()) return;
    section.innerHTML =
      '<h2 class="snapshot-history__title">Snapshot history</h2>' +
      `<p style="color: var(--status-fail-fg);">Failed to load snapshot history: ${esc(e.message)}</p>`;
    return;
  }
  if (aborted && aborted()) return;

  if (!rows || rows.length === 0) {
    section.innerHTML =
      '<h2 class="snapshot-history__title">Snapshot history</h2>' +
      '<p style="color: var(--fg-2);">No snapshots found.</p>';
    return;
  }

  const tbody = rows.map((s) => {
    // Prefer the human-readable email when the server resolved the
    // actor UUID against the users table; fall back to the UUID for
    // pre-RFC-013 snapshots, deleted users, or DB-resolution failures.
    let actor;
    if (s.actor_email) {
      actor = `<span title="${esc(s.actor)}">${esc(s.actor_email)}</span>`;
    } else if (s.actor) {
      actor = `<code class="mono">${esc(s.actor)}</code>`;
    } else {
      actor = '<span style="color:var(--fg-2);">—</span>';
    }
    // The snapshot's run_id is the run UUID — deep-link to its run detail
    // page (the SPA router intercepts /ui/ links). Empty for foreign /
    // pre-RFC-013 writers.
    const runId = s.run_id
      ? `<a href="/ui/runs/${encodeURIComponent(s.run_id)}"><code class="mono">${esc(s.run_id)}</code></a>`
      : '<span style="color:var(--fg-2);">—</span>';
    const records = typeof s.added_records === 'number'
      ? esc(String(s.added_records))
      : '<span style="color:var(--fg-2);">—</span>';
    const size = typeof s.added_files_size === 'number'
      ? `<span title="${esc(String(s.added_files_size))} bytes">${esc(formatBytes(s.added_files_size))}</span>`
      : '<span style="color:var(--fg-2);">—</span>';
    return `<tr>
      <td><code class="mono">${esc(String(s.snapshot_id))}</code></td>
      <td>${timeTag(s.committed_at)}</td>
      <td>${actor}</td>
      <td>${runId}</td>
      <td>${records}</td>
      <td>${size}</td>
    </tr>`;
  }).join('');

  section.innerHTML =
    '<h2 class="snapshot-history__title">Snapshot history</h2>' +
    '<table class="table snapshot-history__table">' +
    '<thead><tr>' +
    '<th>Snapshot ID</th>' +
    '<th>Committed at</th>' +
    '<th>Actor</th>' +
    '<th>Run ID</th>' +
    '<th>Added records</th>' +
    '<th>File size</th>' +
    '</tr></thead>' +
    `<tbody>${tbody}</tbody>` +
    '</table>';
}
