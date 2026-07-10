// app.js — History-API router + shared nav for the Datuplet UI.
//
// Every page module exports a `render(ctx)` function that writes into
// #app. The router matches the pathname against `routes`, renders the
// nav, then renders the matched page. renderNav queries /auth/me and
// /projects to populate the header; on 401 the request chain redirects
// here via goToLogin.

import { api, esc } from '/ui/api.js';
import { renderLogin } from '/ui/pages/login.js';
import { renderPipelines } from '/ui/pages/pipelines.js';
import { renderPipelineDetail } from '/ui/pages/pipeline-detail.js';
import { renderPipelineTrigger } from '/ui/pages/pipeline-trigger.js';
import { renderRuns } from '/ui/pages/runs.js';
import { renderRunDetail } from '/ui/pages/run-detail.js';
import { renderSecrets } from '/ui/pages/settings-secrets.js';
import { renderStorageCatalog } from '/ui/pages/storage-catalog.js';
import { renderStorageTable } from '/ui/pages/storage-table.js';
import { renderQuery } from '/ui/pages/query.js';
import { renderComponents } from '/ui/pages/components.js';
import { install as installHotkeys } from '/ui/hotkeys.js';
import { install as installOverlay } from '/ui/overlay.js';
import * as icons from '/ui/icons.js';

// Overlay listener is idempotent by design — install once at module load
// so `?` works from any page, including the public login page.
installOverlay();
installHotkeys((to) => {
  if (window.location.pathname !== to) {
    window.history.pushState({}, '', to);
  }
  renderRoute();
});

// Public routes skip the authenticated-nav render.
const routes = [
  { pattern: /^\/ui\/login\/?$/, render: renderLogin, public: true },
  { pattern: /^\/ui\/?$/, render: () => redirect('/ui/pipelines') },
  { pattern: /^\/ui\/pipelines\/?$/, render: renderPipelines },
  { pattern: /^\/ui\/pipelines\/([^/]+)\/trigger\/?$/, render: renderPipelineTrigger },
  { pattern: /^\/ui\/pipelines\/([^/]+)\/?$/, render: renderPipelineDetail },
  { pattern: /^\/ui\/runs\/?$/, render: renderRuns },
  { pattern: /^\/ui\/runs\/([^/]+)\/?$/, render: renderRunDetail },
  { pattern: /^\/ui\/storage\/?$/, render: renderStorageCatalog },
  { pattern: /^\/ui\/storage\/t\/([^/]+)\/([^/]+)\/?$/, render: renderStorageTable },
  { pattern: /^\/ui\/query\/?$/, render: renderQuery },
  { pattern: /^\/ui\/components\/?$/, render: renderComponents },
  { pattern: /^\/ui\/settings\/secrets\/?$/, render: renderSecrets },
];

function redirect(to) {
  window.history.replaceState({}, '', to);
  renderRoute();
}

// Exposed on window so api.js can trigger a login redirect on 401
// without needing a circular import of this module.
window.__datupletGoToLogin = () => {
  if (window.location.pathname !== '/ui/login') {
    redirect('/ui/login');
  }
};

// Top-level nav items rendered in the sidebar. Each item owns its icon so
// the markup below stays declarative.
const NAV_ITEMS = [
  { href: '/ui/pipelines',        label: 'Pipelines', icon: 'play'     },
  { href: '/ui/runs',             label: 'Runs',      icon: 'activity' },
  { href: '/ui/storage',          label: 'Storage',   icon: 'database' },
  { href: '/ui/query',            label: 'Query',      icon: 'terminal' },
  { href: '/ui/components',       label: 'Components', icon: 'database' },
  { href: '/ui/settings/secrets', label: 'Secrets',    icon: 'key'      },
];

function navItemsHTML(pathname) {
  return NAV_ITEMS.map((it) => {
    // Prefix-match so deeper routes (e.g. /ui/pipelines/foo) still mark
    // the parent nav item active.
    const active = pathname === it.href || pathname.startsWith(it.href + '/');
    const cls = active ? 'nav-item nav-item--active' : 'nav-item';
    return `<a href="${it.href}" class="${cls}">${icons[it.icon]}<span>${esc(it.label)}</span></a>`;
  }).join('');
}

async function renderNav(isPublic) {
  const sidebar = document.getElementById('sidebar');
  if (!sidebar) return null;
  if (isPublic) {
    sidebar.innerHTML = `
      <div class="sidebar-brand">${icons.terminal}<span>Datuplet</span></div>
    `;
    return null;
  }
  const me = await api('/api/v1/auth/me').catch(() => null);
  if (!me) {
    sidebar.innerHTML = `
      <div class="sidebar-brand">${icons.terminal}<span>Datuplet</span></div>
    `;
    return null;
  }
  const projects = await api('/api/v1/projects').catch(() => []);
  // Prefer the previously-selected project (persisted in localStorage)
  // so the nav dropdown survives a reload. Fall back to the first one.
  const savedID = localStorage.getItem('datuplet.activeProject');
  let active = projects.find((p) => p.id === savedID) || projects[0];
  if (!active) active = { id: '', name: '(no project)' };
  window.__datupletActiveProjectID = active.id;

  // For 2+ projects render a selector; for 1 or 0 just show the name.
  const projectBlock = projects.length > 1
    ? `<select id="project-select" class="input">${projects
        .map((p) => `<option value="${esc(p.id)}"${p.id === active.id ? ' selected' : ''}>${esc(p.name)}</option>`)
        .join('')}</select>`
    : `<span>${esc(active.name)}</span>`;

  sidebar.innerHTML = `
    <div class="sidebar-brand">${icons.terminal}<span>Datuplet</span></div>
    <nav class="sidebar-nav">${navItemsHTML(window.location.pathname)}</nav>
    <div class="sidebar-footer">
      <div class="sidebar-project">${projectBlock}</div>
      <div class="sidebar-user">
        ${icons.user}<span>${esc(me.email)}</span>
        <a href="#" id="logout" title="Log out" aria-label="Log out">${icons.logOut}</a>
      </div>
    </div>
  `;
  const sel = document.getElementById('project-select');
  if (sel) {
    sel.addEventListener('change', () => {
      localStorage.setItem('datuplet.activeProject', sel.value);
      window.__datupletActiveProjectID = sel.value;
      renderRoute();
    });
  }
  document.getElementById('logout').addEventListener('click', async (e) => {
    e.preventDefault();
    try {
      await fetch('/api/v1/auth/logout', { method: 'POST', credentials: 'include' });
    } catch {}
    localStorage.removeItem('datuplet.activeProject');
    redirect('/ui/login');
  });
  return me;
}

export async function renderRoute() {
  // Every page that sets a poller stashes it on window.__datupletPoller
  // so navigation here clears it. Without this, running-run detail keeps
  // fetching in the background after the user leaves the page.
  if (window.__datupletPoller) {
    clearInterval(window.__datupletPoller);
    window.__datupletPoller = null;
  }
  const app = document.getElementById('app');
  app.innerHTML = `<p aria-busy="true">Loading…</p>`;
  // Snapshot the path we started rendering for. If a later step (e.g.
  // a 401 from /auth/me triggering goToLogin, or a page's own redirect)
  // changes the URL, abort before calling r.render — otherwise we'd
  // paint the original protected page's output OVER the /ui/login view
  // that was already pushed.
  const path = window.location.pathname;
  const aborted = () => window.location.pathname !== path;
  for (const r of routes) {
    const m = path.match(r.pattern);
    if (m) {
      await renderNav(r.public);
      if (aborted()) return;
      try {
        await r.render({ params: m.slice(1) });
      } catch (err) {
        if (aborted()) return;
        // Most failures already redirect to login via api(); anything
        // else surfaces as a plain error message so the user isn't
        // left staring at a spinner.
        if (String(err.message) !== 'not authenticated') {
          app.innerHTML = `<article><h3>Error</h3><pre>${esc(err.message)}</pre></article>`;
        }
      }
      return;
    }
  }
  app.innerHTML = `<article><h3>404</h3><p>${esc(path)}</p><a href="/ui/">Home</a></article>`;
}

window.renderRoute = renderRoute;
window.addEventListener('popstate', renderRoute);

// Intercept internal /ui/* links and swap in a pushState navigation so
// the browser never does a full page load for SPA transitions.
document.addEventListener('click', (e) => {
  const a = e.target.closest('a');
  if (!a) return;
  const href = a.getAttribute('href');
  if (!href || !href.startsWith('/ui/')) return;
  if (a.target === '_blank' || e.metaKey || e.ctrlKey || e.shiftKey) return;
  e.preventDefault();
  // Compare the full current URL (path + query) against the target href so
  // query-clearing navigations (e.g. the components detail's "Back to
  // catalog" link, /ui/components?name=x → /ui/components) still push and
  // drop the stale query. Query-bearing and cross-path links push as before.
  const current = window.location.pathname + window.location.search;
  if (current !== href) {
    window.history.pushState({}, '', href);
  }
  renderRoute();
});

renderRoute();
