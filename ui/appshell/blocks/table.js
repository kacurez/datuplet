// ui/appshell/blocks/table.js — RFC 028 Part 4 (V1) table block renderer.
//
// Security (spec §6.4): column headers and every cell are UNTRUSTED text fields
// — all reach the DOM via textContent, NEVER innerHTML.
//
// Client-side, no round trip (spec §6.3): sortable columns (numeric columns
// sort numerically, others lexically; click toggles asc/desc) and a
// search/filter box, both operating over the delivered rows. Numeric columns
// are right-aligned with tabular-nums. Handles BOTH W1 row shapes — a plain
// cell array (["a", 1]) and the object form ({cells:[…], modal?}); the modal is
// not surfaced in v1 (modal interactivity is later work, spec §6.3).

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
  const rows = normalizeRows(block && block.rows);
  const numeric = columns.map((_c, i) => isNumericColumn(rows, i));

  const search = document.createElement("input");
  search.type = "search";
  search.className = "dtp-table-search";
  search.placeholder = "Search…";
  el.appendChild(search);

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
    for (const row of visibleRows()) {
      const tr = document.createElement("tr");
      for (let i = 0; i < columns.length; i++) {
        const td = document.createElement("td");
        if (numeric[i]) td.className = "dtp-num";
        td.textContent = cellText(row[i]);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  }

  search.addEventListener("input", renderBody);
  renderBody();

  return el;
}

// normalizeRows accepts BOTH W1 row shapes and returns an array of cell arrays.
function normalizeRows(rows) {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => {
    if (Array.isArray(row)) return row;
    if (row && typeof row === "object" && Array.isArray(row.cells)) return row.cells;
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
// plain string destined for textContent, never markup.
function cellText(v) {
  return v === null || v === undefined ? "" : String(v);
}
