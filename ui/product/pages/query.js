// Query console (RFC 022 §5.6) — ad-hoc SQL against the active project's
// warehouse via POST /api/v1/query (api.js::runQuery).
//
// Task 4.1 scaffold: this is the routed stub (nav item + route resolve here).
// Task 4.2 fills in the three-pane console (schema tree, editor, results grid).
import { esc } from '/ui/api.js';

export async function renderQuery() {
  const head = document.getElementById('page-head');
  const app = document.getElementById('app');
  const projectId = window.__datupletActiveProjectID;

  head.innerHTML = `<h1>Query</h1>`;

  if (!projectId) {
    app.innerHTML = `<p class="empty">Pick a project to run ad-hoc SQL against its warehouse.</p>`;
    return;
  }
  app.innerHTML = `<p class="empty">Query console for ${esc(projectId)} — coming soon.</p>`;
}
