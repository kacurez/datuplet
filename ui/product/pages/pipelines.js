// Pipelines list. One row per pipeline name in the active project;
// links go to /ui/pipelines/:name for detail, plus a "New" button that
// opens an empty detail page for upload.

import { api, esc } from '/ui/api.js';
import { timeTag } from '/ui/format.js';
import * as icons from '/ui/icons.js';
import { emptyState, skeletonRows } from '/ui/components.js';

// Pipelines list has 3 columns: Name, Updated, Trigger.
const PIPELINE_COLS = 3;

export async function renderPipelines() {
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  const pid = window.__datupletActiveProjectID;
  if (!pid) {
    if (head) head.innerHTML = '';
    app.innerHTML = `<article><h3>No project</h3><p>You don't have any projects yet. Ask an admin to run <code class="inline">pipeline-api admin create-project</code>.</p></article>`;
    return;
  }

  if (head) {
    head.innerHTML = `
      <h1>Pipelines</h1>
      <div class="actions">
        <a class="btn btn--primary" href="/ui/pipelines/_new">New pipeline</a>
      </div>
    `;
  }

  // Paint a skeleton table so the layout doesn't flash empty while the
  // initial fetch is in flight.
  app.innerHTML = `
    <table class="table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Updated</th>
          <th></th>
        </tr>
      </thead>
      <tbody>${skeletonRows(5, PIPELINE_COLS)}</tbody>
    </table>
  `;

  const pipelines = await api(`/api/v1/projects/${encodeURIComponent(pid)}/pipelines`);

  if (!pipelines || pipelines.length === 0) {
    app.innerHTML = emptyState({
      icon: icons.database,
      text: 'No pipelines yet.',
      cta: { label: 'Create your first pipeline', href: '/ui/pipelines/_new' },
    });
    return;
  }

  const rows = pipelines.map((p) => {
    const name = esc(p.name);
    const nameHref = `/ui/pipelines/${encodeURIComponent(p.name)}`;
    const trigHref = `/ui/pipelines/${encodeURIComponent(p.name)}/trigger`;
    return `
      <tr data-href="${nameHref}">
        <td><a href="${nameHref}"><code class="inline">${name}</code></a></td>
        <td>${timeTag(p.updated_at)}</td>
        <td><a class="btn btn--ghost" href="${trigHref}">Trigger run</a></td>
      </tr>
    `;
  }).join('');

  app.innerHTML = `
    <table class="table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Updated</th>
          <th></th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;

  // Row-level click navigation for mouse users. Clicks on interactive
  // controls (links, buttons, inputs, selects) inside the row bail out
  // so the native element handles the action — important for
  // keyboard nav, open-in-new-tab, copy-link, screen readers, etc.
  app.querySelectorAll('tr[data-href]').forEach((tr) => {
    tr.addEventListener('click', (e) => {
      if (e.target.closest('a, button, input, select')) return;
      const href = tr.getAttribute('data-href');
      if (!href) return;
      window.history.pushState({}, '', href);
      if (typeof window.renderRoute === 'function') window.renderRoute();
    });
  });
}
