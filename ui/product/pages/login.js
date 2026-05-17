// Login page. POSTs email+password to /api/v1/auth/login which sets the
// session cookie (HttpOnly, SameSite=Lax, 24h sliding). On success we
// redirect to /ui/pipelines and let the router + nav render the
// authenticated shell.

import { esc } from '/ui/api.js';
import * as icons from '/ui/icons.js';

export function renderLogin() {
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  if (head) head.innerHTML = '';

  app.innerHTML = `
    <div class="login-wrap">
      <div class="login-card">
        <h1 class="brand">${icons.terminal}<span>Datuplet</span></h1>
        <form id="login-form">
          <label class="field">Email
            <input class="input" type="email" name="email" autocomplete="email" required autofocus>
          </label>
          <label class="field">Password
            <input class="input" type="password" name="password" autocomplete="current-password" required>
          </label>
          <button class="btn btn--primary" type="submit">Sign in</button>
          <div id="login-error" style="margin-top: var(--s-3);"></div>
        </form>
      </div>
    </div>
  `;

  const form = document.getElementById('login-form');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const errEl = document.getElementById('login-error');
    errEl.innerHTML = '';
    const btn = form.querySelector('button[type=submit]');
    btn.disabled = true;
    try {
      const body = {
        email: form.email.value.trim(),
        password: form.password.value,
      };
      const r = await fetch('/api/v1/auth/login', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        // Show a generic message for 401 — never leak whether the
        // email exists vs. the password was wrong.
        let msg = r.status === 401 ? 'Invalid email or password.' : `Server error (${r.status}).`;
        if (r.status >= 400 && r.status < 500 && r.status !== 401) {
          const text = await r.text();
          if (text) msg = text;
        }
        errEl.innerHTML = `<div style="color: var(--status-fail-fg); font-size: var(--text-sm);">${esc(msg)}</div>`;
        return;
      }
      // Login succeeded — go to pipelines and let app.js rerender the
      // nav (which will re-query /auth/me with the new cookie).
      window.history.replaceState({}, '', '/ui/pipelines');
      if (typeof window.renderRoute === 'function') window.renderRoute();
    } catch (err) {
      errEl.innerHTML = `<div style="color: var(--status-fail-fg); font-size: var(--text-sm);">${esc(err.message || String(err))}</div>`;
    } finally {
      btn.disabled = false;
    }
  });
}
