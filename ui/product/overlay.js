// overlay.js — simple modal overlay helper.
// Listens for datuplet:show-help + datuplet:dismiss-overlay events and
// renders into #overlay-root. Only one overlay is supported at a time.

const SHORTCUTS = [
  { keys: 'g p', desc: 'Go to Pipelines' },
  { keys: 'g r', desc: 'Go to Runs' },
  { keys: 'g s', desc: 'Go to Storage' },
  { keys: 'g k', desc: 'Go to Secrets' },
  { keys: '/',   desc: 'Focus page search' },
  { keys: '?',   desc: 'Show this help' },
  { keys: 'Esc', desc: 'Close overlays' },
];

function renderHelpMarkup() {
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const rows = SHORTCUTS.map((s) => `
    <tr>
      <td><kbd>${esc(s.keys)}</kbd></td>
      <td>${esc(s.desc)}</td>
    </tr>`).join('');
  return `
    <div class="overlay-panel" role="dialog" aria-modal="true" aria-labelledby="overlay-heading">
      <header class="overlay-head">
        <h2 id="overlay-heading">Keyboard shortcuts</h2>
      </header>
      <table class="table shortcuts-table">${rows}</table>
    </div>
  `;
}

function show() {
  const root = document.getElementById('overlay-root');
  if (!root) return;
  root.innerHTML = renderHelpMarkup();
  // Click outside the panel dismisses.
  root.addEventListener('click', onBackdropClick);
}

function dismiss() {
  const root = document.getElementById('overlay-root');
  if (!root) return;
  root.innerHTML = '';
  root.removeEventListener('click', onBackdropClick);
}

function onBackdropClick(e) {
  // If the user clicked the panel itself, keep the overlay; otherwise dismiss.
  if (e.target.id === 'overlay-root') dismiss();
}

export function install() {
  window.addEventListener('datuplet:show-help', show);
  window.addEventListener('datuplet:dismiss-overlay', dismiss);
}
