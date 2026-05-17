// ui/product/icons.js
// Lucide (MIT) icon subset inlined as SVG strings. Each export is a
// ready-to-insert template literal with no attributes that depend on
// runtime state — just drop into innerHTML.
//
// Size defaults to 16x16. If a caller needs a larger icon (e.g. for
// empty-states) they can scale via CSS `svg { width: N; height: N; }`
// on the parent selector.

const base = (body) =>
  `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${body}</svg>`;

export const database     = base(`<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v6c0 1.66 4 3 9 3s9-1.34 9-3V5"/><path d="M3 11v6c0 1.66 4 3 9 3s9-1.34 9-3v-6"/>`);
export const play         = base(`<polygon points="6 3 20 12 6 21 6 3"/>`);
export const activity     = base(`<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>`);
export const key          = base(`<path d="m21 2-9.6 9.6"/><circle cx="7.5" cy="15.5" r="5.5"/><path d="m15.5 7.5 3 3L22 7l-3-3"/>`);
export const search       = base(`<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>`);
export const user         = base(`<path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>`);
export const logOut       = base(`<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/>`);
export const chevronRight = base(`<polyline points="9 18 15 12 9 6"/>`);
export const check        = base(`<polyline points="20 6 9 17 4 12"/>`);
export const x            = base(`<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>`);
export const clock        = base(`<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>`);
export const alertTriangle= base(`<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>`);
export const terminal     = base(`<polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/>`);
export const menu         = base(`<line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="18" x2="20" y2="18"/>`);
export const info         = base(`<circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/>`);
