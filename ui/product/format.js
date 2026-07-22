// format.js — small formatting helpers shared across pages.

// relTime returns a short relative-time string like "3m ago" / "2h ago"
// / "yesterday" / "4d ago" for an ISO-8601 timestamp. Returns empty
// string if the input can't be parsed.
export function relTime(iso) {
  if (!iso) return '';
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return '';
  const diff = Math.max(0, Math.floor((Date.now() - then) / 1000));
  if (diff < 5) return 'just now';
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  if (diff < 172800) return 'yesterday';
  return `${Math.floor(diff / 86400)}d ago`;
}

// timeTag returns a <time> element with title=absolute and text=relative.
// Callers drop this directly into innerHTML.
export function timeTag(iso) {
  if (!iso) return '';
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  return `<time title="${esc(iso)}">${esc(relTime(iso))}</time>`;
}

// phaseToPillClass maps a pipeline run phase string to the pill
// modifier class. Keep the list in sync with pkg/pipelineapi/... phases:
// Pending, Running, Succeeded, Failed*, Cancelled.
export function phaseToPillClass(phase) {
  switch ((phase || '').toLowerCase()) {
    case 'succeeded': return 'pill--ok';
    case 'running':   return 'pill--running';
    case 'pending':   return 'pill--pending';
    case 'cancelled': return 'pill--cancelled';
    default:
      // Failed, FailedUser, FailedApplication, anything else → fail.
      return 'pill--fail';
  }
}

// formatDuration renders a millisecond span compactly: "0:42", "3m 48s",
// "1h 04m". Returns "—" for null/negative. Used by the runs list + run detail.
export function formatDuration(ms) {
  if (ms == null || ms < 0) return '—';
  const s = Math.floor(ms / 1000);
  if (s < 60) return `0:${String(s).padStart(2, '0')}`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, '0')}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${String(m % 60).padStart(2, '0')}m`;
}

// durationFrom returns ms between two ISO timestamps; if endIso is falsy,
// measures to now (for a live/Running row). Returns null if startIso is falsy.
export function durationFrom(startIso, endIso) {
  if (!startIso) return null;
  const start = new Date(startIso).getTime();
  if (Number.isNaN(start)) return null;
  const end = endIso ? new Date(endIso).getTime() : Date.now();
  return Math.max(0, end - start);
}

// formatBytes renders a byte count in IEC units (KiB/MiB/GiB…) with one
// decimal for values ≥ 1 KiB. Returns "—" for null/undefined/negative so
// a possibly-absent size can be passed straight through.
export function formatBytes(n) {
  if (n == null || n < 0) return '—';
  if (n < 1024) return `${n} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  let v = n / 1024;
  let u = 0;
  while (v >= 1024 && u < units.length - 1) { v /= 1024; u += 1; }
  return `${v.toFixed(1)} ${units[u]}`;
}

// storageFolderURI reduces a table's metadata-file location to the table's
// storage folder — the directory holding metadata/ + data. For example
// gs://bucket/a/b/metadata/00002-…metadata.json → gs://bucket/a/b. Falls
// back to the parent directory when there's no /metadata/ segment. Returns
// "" for empty input.
export function storageFolderURI(metadataLocation) {
  if (!metadataLocation) return '';
  const i = metadataLocation.lastIndexOf('/metadata/');
  if (i >= 0) return metadataLocation.slice(0, i);
  const j = metadataLocation.lastIndexOf('/');
  return j >= 0 ? metadataLocation.slice(0, j) : metadataLocation;
}

// gcsConsoleHref maps a gs:// folder URI to a Google Cloud console
// object-browser URL. Returns "" for non-gs:// URIs (S3/MinIO/local have no
// universal console URL — the caller shows the plain URI instead).
export function gcsConsoleHref(gsURI) {
  if (!gsURI || !gsURI.startsWith('gs://')) return '';
  const path = gsURI.slice('gs://'.length); // bucket/prefix…
  if (!path) return '';
  return `https://console.cloud.google.com/storage/browser/${path}`;
}
