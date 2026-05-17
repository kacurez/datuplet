// components.js — shared small UI primitives.
// No framework, no state — each export returns an HTML string.

import { esc } from '/ui/api.js';

/**
 * Render a centered empty-state block.
 * @param {{ icon?: string, text: string, cta?: { label: string, href?: string, onClick?: string } }} opts
 */
export function emptyState({ icon, text, cta }) {
  const iconSvg = icon || '';
  const ctaHtml = cta
    ? (cta.href
        ? `<a class="btn btn--primary" href="${esc(cta.href)}">${esc(cta.label)}</a>`
        : `<button class="btn btn--primary" onclick="${esc(cta.onClick || '')}">${esc(cta.label)}</button>`)
    : '';
  return `<div class="empty-state">${iconSvg}<p>${esc(text)}</p>${ctaHtml}</div>`;
}

/**
 * Render one skeleton row for a table. `cells` controls how many <td>s
 * to emit so the skeleton aligns with the real table columns.
 */
export function skeletonRow(cells = 4) {
  return `<tr class="skeleton" aria-hidden="true">${'<td></td>'.repeat(cells)}</tr>`;
}

/**
 * Render several skeleton rows at once — handy default while a fetch
 * is in flight.
 */
export function skeletonRows(count = 4, cells = 4) {
  return Array.from({ length: count }, () => skeletonRow(cells)).join('');
}

/**
 * Render an inline spinner (12px). Used inside buttons or after
 * page titles while a background action is in flight.
 */
export function spinner() {
  return `<span class="spinner" role="status" aria-label="loading"></span>`;
}
