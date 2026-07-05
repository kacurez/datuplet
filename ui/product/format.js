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
