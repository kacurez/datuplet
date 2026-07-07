// Run detail with live polling.
//
// Polls /api/v1/projects/:pid/runs/:id every 2 seconds until the phase
// is terminal (Succeeded, FailedUser, FailedApplication, Cancelled,
// Expired), then stops. The poller handle is stored on window so the
// router clears it when the user navigates away.
//
// Cancel sends POST /runs/:id/cancel. The server deletes the
// PipelineRun CRD and writes DB phase Cancelled + revokes the token
// (Plan C3 Task 9). The polling loop picks the new phase up on the
// next tick.

import { api, esc } from '/ui/api.js';
import { timeTag, phaseToPillClass, formatDuration, durationFrom } from '/ui/format.js';

const TERMINAL = new Set([
  'Succeeded',
  'FailedUser',
  'FailedApplication',
  'Cancelled',
  'Expired',
]);

function renderStage(stage) {
  const pill = phaseToPillClass(stage.phase);
  const when = stage.started_at
    ? `${timeTag(stage.started_at)}${stage.completed_at ? ` → ${timeTag(stage.completed_at)}` : ''} · ${formatDuration(stage.duration_ms)}`
    : '';
  const chips = (list) => (list || []).map((c) =>
    `<span class="chip chip--${esc(c.kind)}" title="${esc(c.kind)}"><code class="mono">${esc(c.label)}</code></span>`
  ).join('') || '<span class="muted">—</span>';
  return `
    <li class="timeline-node timeline-node--${esc((stage.phase || '').toLowerCase())}">
      <div class="timeline-hd"><strong>${esc(stage.name)}</strong> <span class="pill ${pill}">${esc(stage.phase)}</span></div>
      <div class="timeline-meta muted">${when}</div>
      <div class="timeline-io"><span class="lbl">in</span> ${chips(stage.imported)}</div>
      <div class="timeline-io"><span class="lbl">out</span> ${chips(stage.exported)}</div>
      ${stage.message ? `<pre class="code">${esc(stage.message)}</pre>` : ''}
    </li>`;
}

function clearExistingPoller() {
  if (window.__datupletPoller) {
    clearInterval(window.__datupletPoller);
    window.__datupletPoller = null;
  }
}

// Shouts terminal-phase transitions into #a11y-live so screen readers
// get a single polite notification. We remember the last announced
// phase on the function object so repeated paint() calls with the same
// terminal phase don't re-announce.
function announceTerminal(shortID, phase) {
  const el = document.getElementById('a11y-live');
  if (!el) return;
  const key = `${shortID}:${phase}`;
  if (announceTerminal._last === key) return;
  announceTerminal._last = key;
  el.textContent = `Run ${shortID} ${phase}`;
}

export async function renderRunDetail(ctx) {
  clearExistingPoller();
  announceTerminal._last = null;
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  const pid = window.__datupletActiveProjectID;
  if (!pid) {
    if (head) head.innerHTML = '';
    app.innerHTML = `<p>No active project.</p>`;
    return;
  }
  const id = ctx.params[0];
  const shortID = String(id).slice(0, 8);

  async function paint() {
    let run;
    try {
      run = await api(`/api/v1/projects/${encodeURIComponent(pid)}/runs/${encodeURIComponent(id)}`);
    } catch (err) {
      if (String(err.message) === 'not authenticated') return true;
      if (head) head.innerHTML = '';
      app.innerHTML = `<article><h3>Error</h3><pre class="code">${esc(err.message)}</pre></article>`;
      return true; // stop polling
    }
    const terminal = TERMINAL.has(run.phase);
    const pill = phaseToPillClass(run.phase);

    if (head) {
      head.innerHTML = `
        <h1>Run <code class="mono">${esc(shortID)}</code></h1>
        <div class="actions">
          <span class="pill ${pill}">${esc(run.phase)}</span>
          ${terminal ? '' : `<button type="button" id="cancel-btn" class="btn btn--secondary">Cancel</button>`}
        </div>
      `;
    }

    app.innerHTML = `
      ${run.current_stage ? `<p>Stage <code class="inline">${esc(run.current_stage)}</code></p>` : ''}
      ${run.message ? `<pre class="code">${esc(run.message)}</pre>` : ''}
      <table class="table">
        <tbody>
          <tr><td style="width:140px;color:var(--fg-2);">Run ID</td><td class="mono">${esc(run.id)}</td></tr>
          <tr><td style="color:var(--fg-2);">Pipeline</td><td class="mono">${esc(run.pipeline_id || '')}</td></tr>
          <tr><td style="color:var(--fg-2);">Phase</td><td><span class="pill ${pill}">${esc(run.phase)}</span></td></tr>
          <tr><td style="color:var(--fg-2);">Created</td><td>${timeTag(run.created_at)}</td></tr>
          <tr><td style="color:var(--fg-2);">Started</td><td>${timeTag(run.started_at)}</td></tr>
          <tr><td style="color:var(--fg-2);">Duration</td><td>${formatDuration(durationFrom(run.started_at, run.completed_at))}</td></tr>
        </tbody>
      </table>
      ${Array.isArray(run.timeline) && run.timeline.length
        ? `<ol class="timeline">${run.timeline.map(renderStage).join('')}</ol>`
        : '<p class="muted">No stage timeline recorded.</p>'}
    `;

    if (!terminal) {
      const cancelBtn = document.getElementById('cancel-btn');
      if (cancelBtn) {
        cancelBtn.addEventListener('click', async () => {
          if (!confirm('Cancel this run? The PipelineRun CRD will be deleted and the token revoked.')) return;
          cancelBtn.disabled = true;
          try {
            await api(
              `/api/v1/projects/${encodeURIComponent(pid)}/runs/${encodeURIComponent(id)}/cancel`,
              { method: 'POST' },
            );
          } catch (err) {
            if (String(err.message) !== 'not authenticated') {
              alert(`Cancel failed: ${err.message}`);
            }
          } finally {
            cancelBtn.disabled = false;
          }
        });
      }
    } else {
      // RFC 005 accessibility escape valve: emit one polite status
      // sentence into #a11y-live so screen readers learn about the
      // terminal transition without the user having to read the pill.
      announceTerminal(shortID, run.phase);
    }
    return terminal;
  }

  const terminal = await paint();
  if (terminal) return;
  window.__datupletPoller = setInterval(async () => {
    const done = await paint();
    if (done) clearExistingPoller();
  }, 2000);
}
