// ui/appshell/csv.js — RFC 028 Part 4 (V3) CSV export encoding.
//
// Pure string-building module: NO DOM access, NO fetch, NO import — every
// function here is a total, side-effect-free function of its string inputs.
// blocks/table.js is the only caller: it stringifies each cell (via its own
// cellText — the single stringification point for a table cell) and hands
// the resulting string[][] to buildCSV, then downloads the result as a Blob
// (spec §6.4: the CSV text is downloaded, never injected into the DOM).
//
// Security (spec §6.3 "CSV export applies spreadsheet-formula-injection
// escaping", §6.4): table cell values are app-controlled/untrusted. A cell
// whose text starts with =, +, -, @, TAB, or CR can be interpreted by a
// spreadsheet as a formula when the CSV is opened — CSV/formula injection.
// OWASP's mitigation is to prefix such a cell with a single quote so the
// spreadsheet treats it as inert text. This runs BEFORE standard CSV quoting
// (RFC 4180): the prefixed text is itself just more field content, so if it
// also contains a comma/quote/newline it is the already-'-prefixed text that
// gets wrapped in double quotes (apostrophe included).
//
// There is no JS runtime in the Go test suite (pkg/appworker/shell_test.go)
// to execute this file directly, so the exact same rule set is mirrored
// there as a Go "golden" reference (TestCSVGolden_*) checked against a
// documented input->output fixture, and pinned to the constants/logic below
// at the source level (TestCSVModule_*) — see that file's V3 section for the
// full rationale.

// FORMULA_TRIGGER_CHARS is the exact OWASP CSV-injection trigger set spec
// §6.3 names — no more, no less. A cell is escaped only when its FIRST
// character is one of these six; this is NOT a general "starts with
// punctuation" rule. Note "-" is included per the spec text even though it
// also matches ordinary negative numbers ("-42") — an intentional, accepted
// false-positive cost of the OWASP guidance (some spreadsheets treat a
// leading "-" as the start of a formula too).
const FORMULA_TRIGGER_CHARS = ["=", "+", "-", "@", "\t", "\r"];

// escapeFormula prefixes `text` with a single quote if its first character
// is a formula trigger. An empty string has no leading character to trigger
// on and is returned unchanged.
function escapeFormula(text) {
  return text.length > 0 && FORMULA_TRIGGER_CHARS.indexOf(text.charAt(0)) !== -1 ? "'" + text : text;
}

// NEEDS_QUOTING matches a comma, double quote, or newline (CR or LF)
// anywhere in a field — the RFC 4180 condition for wrapping a field in
// double quotes.
const NEEDS_QUOTING = /[",\r\n]/;

// quoteField applies RFC 4180 quoting: wrap in double quotes, doubling any
// embedded double quote, when the field contains a comma, double quote, or
// newline. Applied AFTER escapeFormula, so a formula-escaped value that also
// contains one of these is quoted whole — leading apostrophe included.
function quoteField(text) {
  return NEEDS_QUOTING.test(text) ? '"' + text.replace(/"/g, '""') + '"' : text;
}

// csvCell is the per-cell pipeline every exported cell (header or data) goes
// through: OWASP formula-escape, then RFC 4180 quote. `text` must already be
// a plain string — this module never inspects a value's original type (see
// blocks/table.js's cellText, the single stringification point).
//
// @param {string} text
// @returns {string}
export function csvCell(text) {
  return quoteField(escapeFormula(text));
}

// csvRow joins one row's already-escaped cells with a comma.
function csvRow(cells) {
  return cells.map(csvCell).join(",");
}

// buildCSV renders a header row (`columns`) plus one row per entry in `rows`
// into RFC 4180 CSV text, CRLF-terminated (including a trailing CRLF after
// the last row — the wider-compatibility choice for spreadsheet consumers).
// Pure function of its inputs — the "exported string" the golden-fixture Go
// test in pkg/appworker/shell_test.go checks against.
//
// @param {string[]} columns
// @param {string[][]} rows
// @returns {string}
export function buildCSV(columns, rows) {
  const lines = [csvRow(columns)];
  for (const row of rows) lines.push(csvRow(row));
  return lines.join("\r\n") + "\r\n";
}
