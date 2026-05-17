// Trigger-run page: POST /api/v1/projects/:pid/pipelines/:name/runs
// with an empty body (the server re-parses the stored YAML to derive
// token capabilities — Task 4 in Plan C3). On success we redirect to
// the run detail page so the user can watch the status reconcile from
// Pending → Running → Succeeded/Failed.

import { api, esc } from '/ui/api.js';

export async function renderPipelineTrigger(ctx) {
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  const pid = window.__datupletActiveProjectID;
  if (!pid) {
    if (head) head.innerHTML = '';
    app.innerHTML = `<p>No active project.</p>`;
    return;
  }
  const name = ctx.params[0];

  if (head) {
    head.innerHTML = `
      <h1>Trigger run <code class="inline">${esc(name)}</code></h1>
      <div class="actions"></div>
    `;
  }

  app.innerHTML = `
    <p>This runs the latest saved YAML. The pipeline-api operator will
    create a PipelineRun and a scoped run-token for TableGateway;
    progress appears on the next page.</p>
    <div class="actions">
      <button type="button" id="trigger-btn" class="btn btn--primary">Trigger run</button>
      <a class="btn btn--ghost" href="/ui/pipelines/${encodeURIComponent(name)}">Cancel</a>
    </div>
    <div id="trigger-msg" style="margin-top:var(--s-4);"></div>
  `;

  document.getElementById('trigger-btn').addEventListener('click', async () => {
    const btn = document.getElementById('trigger-btn');
    const msg = document.getElementById('trigger-msg');
    msg.innerHTML = '';
    btn.disabled = true;
    try {
      const out = await api(
        `/api/v1/projects/${encodeURIComponent(pid)}/pipelines/${encodeURIComponent(name)}/runs`,
        { method: 'POST', body: '{}' },
      );
      if (out && out.id) {
        window.history.replaceState({}, '', `/ui/runs/${encodeURIComponent(out.id)}`);
        if (typeof window.renderRoute === 'function') window.renderRoute();
        return;
      }
      msg.innerHTML = `<div class="banner success">Triggered.</div>`;
    } catch (err) {
      if (String(err.message) !== 'not authenticated') {
        msg.innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
      }
    } finally {
      btn.disabled = false;
    }
  });
}
