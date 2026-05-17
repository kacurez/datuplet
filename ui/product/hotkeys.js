// hotkeys.js — global keyboard shortcuts (k9s-style).
//
// Usage:
//   import { install } from '/ui/hotkeys.js';
//   install((to) => { window.history.pushState({}, '', to); renderRoute(); });
//
// `install(navFn)` attaches a single 'keydown' listener to `document`.
// navFn is called with the route string for the navigation leader (`g p`,
// `g r`, etc). If install is called more than once (shouldn't happen),
// the prior listener is removed so shortcuts don't fire twice.
//
// Focus-guard rule (mandatory, RFC 005 Visual design language > Keyboard):
// Every shortcut except Esc short-circuits when:
//   - event.target matches input/textarea/select/[contenteditable=true], OR
//   - any of Ctrl/Meta/Alt is held.
// Esc dispatches the dismiss event even when focus is in an input so
// modal dialogs can be dismissed while the user is typing into one.

const navMap = Object.freeze({
  p: '/ui/pipelines',
  r: '/ui/runs',
  s: '/ui/storage',
  k: '/ui/settings/secrets',
});

const LEADER_TIMEOUT_MS = 800;

let _handler = null;
let _leaderTimer = null;
let _leaderKey = null;

function inEditable(target) {
  if (!target) return false;
  // Text nodes don't expose matches/closest; jump to their parent element.
  if (target.nodeType === 3 /* Node.TEXT_NODE */ && target.parentElement) {
    target = target.parentElement;
  }
  if (typeof target.closest !== 'function') return false;
  return target.closest('input, textarea, select, [contenteditable="true"]') !== null;
}

function hasModifier(e) {
  return e.ctrlKey || e.metaKey || e.altKey;
}

function resetLeader() {
  _leaderKey = null;
  if (_leaderTimer !== null) {
    clearTimeout(_leaderTimer);
    _leaderTimer = null;
  }
}

export function install(navFn) {
  // Remove any previously-attached handler (defensive against accidental
  // double-install across router renders).
  if (_handler) {
    document.removeEventListener('keydown', _handler);
    _handler = null;
  }

  _handler = (e) => {
    // Esc: always fire, even inside inputs. No modifier check because
    // Ctrl+Esc / Cmd+Esc aren't meaningful here.
    if (e.key === 'Escape') {
      resetLeader();
      window.dispatchEvent(new CustomEvent('datuplet:dismiss-overlay'));
      return;
    }

    if (hasModifier(e) || inEditable(e.target)) return;

    // Two-key leader: `g` then `p/r/s/k`.
    if (_leaderKey === 'g' && typeof navMap[e.key] === 'string') {
      e.preventDefault();
      const to = navMap[e.key];
      resetLeader();
      navFn(to);
      return;
    }

    if (e.key === 'g') {
      _leaderKey = 'g';
      if (_leaderTimer !== null) clearTimeout(_leaderTimer);
      _leaderTimer = setTimeout(resetLeader, LEADER_TIMEOUT_MS);
      return;
    }

    // Any other key cancels the leader.
    if (_leaderKey) resetLeader();

    if (e.key === '/') {
      const el = document.querySelector('[data-role="page-search"]');
      if (el) {
        e.preventDefault();
        el.focus();
      }
      return;
    }

    if (e.key === '?') {
      e.preventDefault();
      window.dispatchEvent(new CustomEvent('datuplet:show-help'));
    }
  };

  document.addEventListener('keydown', _handler);
}

// Exported for tests. Not part of the public surface.
export function _test_resetLeader() { resetLeader(); }
