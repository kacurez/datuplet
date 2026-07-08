// Secrets page: write-only key management for the active project
// (RFC 026 P1.5). Backed by:
//   GET    /api/v1/projects/{pid}/secrets              -> [{key, updatedAt}]
//   PUT    /api/v1/projects/{pid}/secrets/{key} {value} -> 204
//   DELETE /api/v1/projects/{pid}/secrets/{key}         -> 204
//
// The server never returns a secret's value, and this page never
// echoes one back into the DOM either: the form is reset immediately
// after a successful save, and only `key` + `updatedAt` ever reach
// innerHTML. Secrets are referenced from a pipeline YAML via
// spec.secretsRef.name + $[key] substitutions — see docs/secrets.md.

import { esc, listSecrets, putSecret, deleteSecret } from '/ui/api.js';
import { timeTag } from '/ui/format.js';
import { skeletonRows } from '/ui/components.js';

const KEY_PATTERN = /^[A-Za-z0-9_-]+$/;

export async function renderSecrets() {
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  const pid = window.__datupletActiveProjectID;

  if (head) head.innerHTML = `<h1>Secrets</h1>`;

  if (!pid) {
    app.innerHTML = `<p>No active project.</p>`;
    return;
  }

  app.innerHTML = `
    <p style="color: var(--fg-1);">
      Reference these from a pipeline YAML via
      <code class="inline">spec.secretsRef.name</code> and
      <code class="inline">$[key]</code> substitutions. Values cannot be
      read back once saved — only the key and last-updated time are shown.
    </p>

    <table class="table" style="margin-top: var(--s-4);">
      <thead>
        <tr><th>Key</th><th>Updated</th><th></th></tr>
      </thead>
      <tbody id="secrets-body">${skeletonRows(3, 3)}</tbody>
    </table>

    <h3 style="margin-top: var(--s-5); margin-bottom: var(--s-2); font-size: var(--text-lg);">Add / update secret</h3>
    <form id="secret-form">
      <label class="field">Key
        <input class="input" type="text" name="key" placeholder="db_password" required
          pattern="[A-Za-z0-9_-]+" title="Letters, digits, underscore, and hyphen only.">
      </label>
      <label class="field">Value
        <input class="input" type="password" name="value" required autocomplete="off">
      </label>
      <p style="color: var(--fg-2); font-size: var(--text-sm);">Values cannot be read back — keep a copy somewhere safe before saving.</p>
      <div id="secret-msg"></div>
      <div class="actions" style="margin-top: var(--s-3);">
        <button type="submit" class="btn btn--primary">Save secret</button>
      </div>
    </form>
  `;

  const body = document.getElementById('secrets-body');
  let secrets = [];

  function renderRows() {
    if (!secrets || secrets.length === 0) {
      body.innerHTML = `<tr><td colspan="3" style="color: var(--fg-2); text-align:center; padding: var(--s-5);">No secrets yet.</td></tr>`;
      return;
    }
    body.innerHTML = secrets.map((s) => `
      <tr>
        <td><code class="inline">${esc(s.key)}</code></td>
        <td>${timeTag(s.updatedAt)}</td>
        <td><button type="button" class="btn btn--ghost" data-delete="${esc(s.key)}">Delete</button></td>
      </tr>
    `).join('');

    body.querySelectorAll('[data-delete]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const key = btn.getAttribute('data-delete');
        if (!confirm(`Delete secret "${key}"? Pipelines referencing $[${key}] will fail to resolve it.`)) return;
        btn.disabled = true;
        try {
          await deleteSecret(pid, key);
          await reload();
        } catch (err) {
          if (String(err.message) !== 'not authenticated') {
            document.getElementById('secret-msg').innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
          }
          btn.disabled = false;
        }
      });
    });
  }

  async function reload() {
    try {
      secrets = await listSecrets(pid);
    } catch (err) {
      if (String(err.message) !== 'not authenticated') {
        body.innerHTML = `<tr><td colspan="3" style="color: var(--status-fail-fg);">Failed to load secrets: ${esc(err.message)}</td></tr>`;
      }
      return;
    }
    renderRows();
  }

  await reload();

  const form = document.getElementById('secret-form');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = document.getElementById('secret-msg');
    msg.innerHTML = '';
    const fd = new FormData(form);
    const key = String(fd.get('key') || '').trim();
    const value = String(fd.get('value') || '');
    if (!KEY_PATTERN.test(key)) {
      msg.innerHTML = `<div class="banner error">Key must contain only letters, digits, underscore, and hyphen.</div>`;
      return;
    }
    if (!value) {
      msg.innerHTML = `<div class="banner error">Value is required.</div>`;
      return;
    }
    const btn = form.querySelector('button[type=submit]');
    btn.disabled = true;
    try {
      await putSecret(pid, key, value);
      // Clear the whole form — the value must never linger in the DOM
      // or be echoed back anywhere, successful save or not.
      form.reset();
      msg.innerHTML = `<div class="banner success">Saved.</div>`;
      await reload();
    } catch (err) {
      if (String(err.message) !== 'not authenticated') {
        msg.innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
      }
    } finally {
      btn.disabled = false;
    }
  });
}
