// ui/appshell/blocks/table.js — RFC 028 Part 4 (V1) table block renderer.
//
// Security (spec §6.4): column headers and every cell are UNTRUSTED text fields
// — all reach the DOM via textContent, NEVER innerHTML.
//
// Client-side, no round trip (spec §6.3): sortable columns (numeric columns
// sort numerically, others lexically; click toggles asc/desc) and a
// search/filter box, both operating over the delivered rows. Numeric columns
// are right-aligned with tabular-nums. Handles BOTH W1 row shapes — a plain
// cell array (["a", 1]) and the object form ({cells:[…], modal?}). A row-level
// modal (spec §6.3) makes that row clickable, opening the modal via interact.js
// (the sort/search/numeric logic still operates purely over the cell arrays).
//
// V3 additions (spec §6.3 "CSV export"): a "Download CSV" button next to the
// search box exports the CURRENTLY VISIBLE rows — after search, in the
// current sort order, what-you-see-is-what-you-download — via csv.js's pure
// buildCSV (OWASP formula-injection escaping + RFC 4180 quoting), downloaded
// as a same-origin Blob (spec §6.4: cell data is never injected into the DOM
// as markup, and this makes no network call). A table with zero rows shows
// the shared empty state instead of a pointless search/download bar over
// nothing; a search with no matches shows an inline "no rows match" row
// instead of leaving the body silently blank.

import { attachModalTrigger } from "../interact.js";
import { renderEmptyState } from "../shell.js";
import { buildCSV } from "../csv.js";

// renderTable builds a searchable, sortable table for the block.
export function renderTable(block) {
  const el = document.createElement("div");
  el.className = "dtp-block dtp-block-table";

  if (block && typeof block.title === "string" && block.title.length > 0) {
    const title = document.createElement("div");
    title.className = "dtp-table-title";
    title.textContent = block.title;
    el.appendChild(title);
  }

  const columns = block && Array.isArray(block.columns) ? block.columns.map(String) : [];
  // rowModals maps a row's cell-array reference -> its modal spec (object-form
  // rows only). Keyed by reference so it survives filter/sort, which reorder
  // but never clone the cell arrays.
  const rowModals = new WeakMap();
  const rows = normalizeRows(block && block.rows, rowModals);

  // Empty state (spec brief: "a table … with no data") — no data at all, not
  // just a search with no matches (that case is handled inside renderBody
  // below, once the toolbar/table below actually exist).
  if (rows.length === 0) {
    el.appendChild(renderEmptyState("No data to display."));
    return el;
  }

  const numeric = columns.map((_c, i) => isNumericColumn(rows, i));

  const toolbar = document.createElement("div");
  toolbar.className = "dtp-table-toolbar";

  const search = document.createElement("input");
  search.type = "search";
  search.className = "dtp-table-search";
  search.placeholder = "Search…";
  toolbar.appendChild(search);

  const download = document.createElement("button");
  download.type = "button";
  download.className = "dtp-table-download";
  download.textContent = "Download CSV";
  download.addEventListener("click", () => {
    downloadCSV(csvFilename(block), columns, visibleRows().map((row) => row.map(cellText)));
  });
  toolbar.appendChild(download);

  el.appendChild(toolbar);

  const scroll = document.createElement("div");
  scroll.className = "dtp-table-scroll";
  const table = document.createElement("table");
  table.className = "dtp-table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  const sortState = { col: -1, dir: 1 }; // dir: 1 asc, -1 desc

  columns.forEach((col, i) => {
    const th = document.createElement("th");
    if (numeric[i]) th.className = "dtp-num";
    const labelSpan = document.createElement("span");
    labelSpan.textContent = col;
    const caret = document.createElement("span");
    caret.className = "dtp-sort-caret";
    th.appendChild(labelSpan);
    th.appendChild(caret);
    th.addEventListener("click", () => {
      if (sortState.col === i) sortState.dir = -sortState.dir;
      else {
        sortState.col = i;
        sortState.dir = 1;
      }
      updateCarets();
      renderBody();
    });
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  table.appendChild(tbody);
  scroll.appendChild(table);
  el.appendChild(scroll);

  function updateCarets() {
    for (let i = 0; i < headRow.children.length; i++) {
      const caret = headRow.children[i].querySelector(".dtp-sort-caret");
      if (!caret) continue;
      caret.textContent = sortState.col === i ? (sortState.dir === 1 ? " ▲" : " ▼") : "";
    }
  }

  function visibleRows() {
    const q = search.value.trim().toLowerCase();
    let out = rows;
    if (q) {
      out = rows.filter((r) => r.some((cell) => cellText(cell).toLowerCase().includes(q)));
    }
    if (sortState.col >= 0) {
      const c = sortState.col;
      const num = numeric[c];
      out = out.slice().sort((a, b) => {
        let cmp;
        if (num) {
          cmp = Number(a[c]) - Number(b[c]);
          if (Number.isNaN(cmp)) cmp = 0;
        } else {
          cmp = cellText(a[c]).localeCompare(cellText(b[c]));
        }
        return cmp * sortState.dir;
      });
    }
    return out;
  }

  function renderBody() {
    tbody.textContent = "";
    const visible = visibleRows();
    if (visible.length === 0) {
      // The table HAS data (the rows.length===0 case returned early above) —
      // this is search finding no matches, not "no data". Keep the toolbar
      // visible (so the user can clear the search) and explain the blank body
      // instead of leaving it silently empty.
      const tr = document.createElement("tr");
      tr.className = "dtp-table-empty-row";
      const td = document.createElement("td");
      td.colSpan = Math.max(1, columns.length);
      td.textContent = "No rows match your search.";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }
    for (const row of visible) {
      const tr = document.createElement("tr");
      for (let i = 0; i < columns.length; i++) {
        const td = document.createElement("td");
        if (numeric[i]) td.className = "dtp-num";
        td.textContent = cellText(row[i]);
        tr.appendChild(td);
      }
      const modal = rowModals.get(row);
      if (modal) attachModalTrigger(tr, modal, { asRow: true });
      tbody.appendChild(tr);
    }
  }

  search.addEventListener("input", renderBody);
  renderBody();

  return el;
}

// normalizeRows accepts BOTH W1 row shapes and returns an array of cell arrays.
// For an object-form row carrying a modal, the modal is recorded in rowModals
// keyed by the returned cell-array reference (so renderBody can wire it without
// changing the search/sort/numeric logic, which stays purely over cell arrays).
function normalizeRows(rows, rowModals) {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => {
    if (Array.isArray(row)) return row;
    if (row && typeof row === "object" && Array.isArray(row.cells)) {
      if (rowModals && row.modal && typeof row.modal === "object") rowModals.set(row.cells, row.modal);
      return row.cells;
    }
    return [];
  });
}

// isNumericColumn reports whether every non-empty cell in column i is a number
// (a real number, or a finite numeric string) — the trigger for right-alignment
// + tabular-nums and numeric sort. An empty column is not numeric.
function isNumericColumn(rows, i) {
  let seen = false;
  for (const row of rows) {
    const v = row[i];
    if (v === null || v === undefined || v === "") continue;
    seen = true;
    if (typeof v === "number") continue;
    if (typeof v === "string" && v.trim() !== "" && Number.isFinite(Number(v))) continue;
    return false;
  }
  return seen;
}

// cellText is the single stringification point for a cell value — always a
// plain string destined for textContent (or, for CSV export below, csv.js's
// buildCSV), never markup.
function cellText(v) {
  return v === null || v === undefined ? "" : String(v);
}

// csvFilename derives a filesystem-safe .csv filename from the block's title
// (falling back to its id, then a generic default): lowercased, runs of
// non-[a-z0-9] characters collapsed to a single hyphen, leading/trailing
// hyphens trimmed. Never derived from cell DATA — only the block's own
// title/id, which (like every other text field) is still just app-controlled
// text, but a filename is a much smaller, easier-to-make-safe surface than a
// full CSV body.
function csvFilename(block) {
  const base = (block && (block.title || block.id)) || "table";
  const slug = String(base)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return (slug || "table") + ".csv";
}

// downloadCSV builds the CSV text (csv.js's buildCSV — OWASP formula-escaped,
// RFC 4180-quoted) and downloads it as a same-origin Blob: no network call,
// and the CSV text is never injected into the DOM as markup (spec §6.4) — it
// only ever exists as Blob content handed straight to the browser's download
// machinery via a throwaway, never-visible <a download>. The UTF-8 BOM
// prefix on the Blob (not part of the CSV text itself, so it never touches
// buildCSV's return value or the golden fixture) helps spreadsheet apps that
// sniff encoding by BOM open non-ASCII content correctly.
function downloadCSV(filename, columns, rows) {
  const csv = buildCSV(columns, rows);
  // Built via fromCharCode rather than a literal character in source, so no
  // invisible byte ever sits in this file (diff/editor safety).
  const BOM = String.fromCharCode(0xfeff);
  const blob = new Blob([BOM, csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  // Deferred rather than immediate: some browsers need the object URL to
  // stay valid a tick past the synchronous click() hand-off.
  setTimeout(() => URL.revokeObjectURL(url), 0);
}
