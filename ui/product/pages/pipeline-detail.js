// Pipeline detail: view + edit the stored YAML, or create a new one.
//
// Route params:
//   /ui/pipelines/_new        → empty editor; name field is editable.
//                                Underscore is invalid in DNS-1123 so
//                                this sentinel can never collide with
//                                a real pipeline name.
//   /ui/pipelines/:name       → loaded editor; name field is locked
//                                (server enforces YAML metadata.name
//                                == URL name; changing the name here
//                                means creating a new pipeline)
//
// PUT is idempotent — the same endpoint both creates and replaces.
// Delete is a separate button with a confirm prompt.

import { api, putYAML, esc } from '/ui/api.js';
import { timeTag, phaseToPillClass, formatDuration, durationFrom } from '/ui/format.js';

const STARTER_YAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/csv-extractor:latest
          outputs:
            defaultBucket: raw
            defaultWriteMode: FULL_LOAD
`;

export async function renderPipelineDetail(ctx) {
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  const pid = window.__datupletActiveProjectID;
  if (!pid) {
    if (head) head.innerHTML = '';
    app.innerHTML = `<p>No active project.</p>`;
    return;
  }
  const name = ctx.params[0];
  const isNew = name === '_new';

  let yaml = STARTER_YAML;
  let updatedAt = '';
  let pipelineID = '';
  if (!isNew) {
    const pipe = await api(`/api/v1/projects/${encodeURIComponent(pid)}/pipelines/${encodeURIComponent(name)}`);
    yaml = pipe.yaml;
    updatedAt = pipe.updated_at;
    pipelineID = pipe.id;
  }

  if (head) {
    const titleHTML = isNew
      ? `<h1>New pipeline</h1>`
      : `<h1><code class="inline">${esc(name)}</code></h1>`;
    const actions = isNew
      ? ''
      : `
        <a class="btn btn--primary" href="/ui/pipelines/${encodeURIComponent(name)}/trigger">Trigger run</a>
        <button type="button" id="delete-btn" class="btn btn--secondary">Delete</button>
      `;
    head.innerHTML = `
      ${titleHTML}
      <div class="actions">${actions}</div>
    `;
  }

  app.innerHTML = `
    ${updatedAt ? `<p style="color:var(--fg-2);"><small>Updated ${timeTag(updatedAt)}</small></p>` : ''}
    <form id="pipeline-form">
      ${isNew ? `
        <label class="field">Name
          <input class="input" type="text" name="name" placeholder="my-pipeline" required
            pattern="[a-z0-9]([-a-z0-9.]*[a-z0-9])?"
            title="Lowercase DNS-1123 subdomain; must match metadata.name in the YAML.">
        </label>` : ''}
      <label class="field">YAML
        <textarea class="textarea input--mono" name="yaml" spellcheck="false" required>${esc(yaml)}</textarea>
      </label>
      <div id="pipeline-msg"></div>
      <div class="actions" style="margin-top:var(--s-3);">
        <button type="submit" class="btn btn--primary">${isNew ? 'Create' : 'Save'}</button>
      </div>
    </form>
    ${isNew ? '' : `
    <section style="margin-top:var(--s-4);">
      <h2>Recent runs</h2>
      <div id="pipeline-runs"><p><small>Loading…</small></p></div>
    </section>`}
  `;

  const form = document.getElementById('pipeline-form');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = document.getElementById('pipeline-msg');
    msg.innerHTML = '';
    const btn = form.querySelector('button[type=submit]');
    btn.disabled = true;
    try {
      // Read form inputs via FormData rather than `form.name` — on
      // HTMLFormElement, `.name` is the form's own (empty) name
      // attribute, not the <input name="name"> child, so accessing
      // .value would throw. FormData avoids that shadowing and also
      // normalizes string conversion.
      const fd = new FormData(form);
      const targetName = isNew ? String(fd.get('name') || '').trim() : name;
      if (!targetName) throw new Error('Name is required');
      const yamlText = String(fd.get('yaml') || '');
      await putYAML(
        `/api/v1/projects/${encodeURIComponent(pid)}/pipelines/${encodeURIComponent(targetName)}`,
        yamlText,
      );
      msg.innerHTML = `<div class="banner success">Saved.</div>`;
      if (isNew) {
        // For a fresh create, jump to the detail view for that name.
        window.history.replaceState({}, '', `/ui/pipelines/${encodeURIComponent(targetName)}`);
        if (typeof window.renderRoute === 'function') window.renderRoute();
      }
    } catch (err) {
      if (String(err.message) !== 'not authenticated') {
        msg.innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
      }
    } finally {
      btn.disabled = false;
    }
  });

  if (!isNew) {
    const delBtn = document.getElementById('delete-btn');
    if (delBtn) {
      delBtn.addEventListener('click', async () => {
        if (!confirm(`Delete pipeline "${name}"? Runs that reference it by ID stay in history, but you won't be able to trigger new ones.`)) return;
        try {
          await api(`/api/v1/projects/${encodeURIComponent(pid)}/pipelines/${encodeURIComponent(name)}`, { method: 'DELETE' });
          window.history.replaceState({}, '', '/ui/pipelines');
          if (typeof window.renderRoute === 'function') window.renderRoute();
        } catch (err) {
          if (String(err.message) !== 'not authenticated') {
            document.getElementById('pipeline-msg').innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
          }
        }
      });
    }
  }

  if (!isNew && pipelineID) {
    // Best-effort: a slow or failing runs fetch must not block the editor
    // above, which has already been painted and is usable.
    loadRecentRuns(pid, pipelineID).catch((err) => {
      if (String(err.message) === 'not authenticated') return;
      const container = document.getElementById('pipeline-runs');
      if (container) container.innerHTML = `<p><small>Couldn't load recent runs: ${esc(err.message)}</small></p>`;
    });
  }
}

// loadRecentRuns fetches the pipeline's most recent runs via the paged
// runs API (filtered by pipeline_id) and renders a compact table into
// #pipeline-runs. Fired once on page load — no live poll here (unlike
// the runs list page).
async function loadRecentRuns(pid, pipelineID) {
  const resp = await api(`/api/v1/projects/${encodeURIComponent(pid)}/runs?pipeline_id=${encodeURIComponent(pipelineID)}&limit=10`);
  const container = document.getElementById('pipeline-runs');
  if (!container) return; // navigated away
  const runs = resp.runs || [];
  if (runs.length === 0) {
    container.innerHTML = '<p><small>No runs yet.</small></p>';
    return;
  }
  const rows = runs.map((r) => {
    const id = String(r.id || '');
    const href = `/ui/runs/${encodeURIComponent(id)}`;
    const pill = phaseToPillClass(r.phase);
    const started = r.started_at ? timeTag(r.started_at) : '<span class="muted">—</span>';
    const dur = r.completed_at || r.started_at
      ? formatDuration(durationFrom(r.started_at, r.completed_at))
      : '—';
    return `
      <tr>
        <td><a href="${href}"><code class="mono">${esc(id.slice(0, 8))}</code></a></td>
        <td><span class="pill ${pill}">${esc(r.phase)}</span></td>
        <td>${started}</td>
        <td>${dur}</td>
      </tr>`;
  }).join('');
  const viewAll = resp.next_cursor
    ? `<p><a href="/ui/runs">View all &rarr;</a></p>`
    : '';
  container.innerHTML = `
    <table class="table">
      <thead>
        <tr>
          <th>Run ID</th>
          <th>Phase</th>
          <th>Started</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    ${viewAll}
  `;
}
