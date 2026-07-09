// findings.js — renders the RFC 026 §7 validation findings array as an HTML
// string. Shared by the pipeline builder's inline error panel (and any other
// findings surface). No state, no DOM — returns a string; callers assign it
// to innerHTML. Every interpolated value is esc()'d (findings come from the
// server but paths/messages are still untrusted for HTML purposes).

import { esc } from '/ui/api.js';

const SEV_FG = {
  error: 'var(--status-fail-fg)',
  warning: 'var(--status-running-fg)',
};

/**
 * @param {Array<{path?:string, message:string, severity?:string}>} findings
 * @returns {string} HTML — '' when there are no findings.
 */
export function renderFindings(findings) {
  if (!Array.isArray(findings) || findings.length === 0) return '';
  const items = findings.map((f) => {
    const sev = f.severity === 'warning' ? 'warning' : 'error';
    const fg = SEV_FG[sev];
    const where = f.path ? esc(f.path) : '(root)';
    return `
      <li class="finding finding--${sev}">
        <code class="mono finding-path" style="color:${fg};">${where}</code>
        <span class="finding-msg">${esc(f.message)}</span>
      </li>`;
  }).join('');
  const errCount = findings.filter((f) => f.severity !== 'warning').length;
  const cls = errCount > 0 ? 'callout callout--warn' : 'callout';
  const heading = errCount > 0
    ? `${errCount} validation error${errCount !== 1 ? 's' : ''}`
    : 'Saved with warnings';
  return `
    <div class="${cls} findings-block">
      <strong>${esc(heading)}</strong>
      <ul class="findings-list">${items}</ul>
    </div>`;
}
