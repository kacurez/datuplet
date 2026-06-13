package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/datuplet/datuplet/components/queryengine"
)

// Exit codes follow the repo contract (RFC 022 §10 + CLAUDE.md):
//
//	0   Succeeded (incl. truncated results — still a successful query)
//	1   FailedUser  (bad user SQL, FGA denial, result-too-large)
//	>=20 FailedApplication (timeout, mint/infra failures)
const (
	exitSuccess     = 0
	exitUserFailure = 1
	exitAppFailure  = 20
)

// exitCodeFor maps a queryengine.Run (or pipeline) error onto the exit-code
// contract:
//   - nil                       → 0
//   - ErrTimeout                → 20 (infra/application: the deadline fired)
//   - ErrResultTooLarge         → 1  (the user asked for too much; their fix)
//   - any other Run error       → 1  (DuckDB Binder/Catalog error or FGA denial
//     surfaced by lakekeeper — a user-correctable SQL/permission problem)
//
// Mint/config/infra errors are mapped to exitAppFailure at the call site in
// main.go (they are not Run errors and never reach this function).
func exitCodeFor(err error) int {
	switch {
	case err == nil:
		return exitSuccess
	case errors.Is(err, queryengine.ErrTimeout):
		return exitAppFailure
	case errors.Is(err, queryengine.ErrResultTooLarge):
		return exitUserFailure
	default:
		return exitUserFailure
	}
}

// render writes res in the chosen format ("table" | "csv" | "json") to out.
// Truncation is NOT noted here — main.go writes that note to stderr so it
// never corrupts the stdout data stream.
func render(out io.Writer, res *queryengine.Result, format string) error {
	switch format {
	case "json":
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	case "csv":
		return renderCSV(out, res)
	case "table":
		return renderTable(out, res)
	default:
		return fmt.Errorf("invalid format %q (want table|csv|json)", format)
	}
}

// columnNames extracts the schema column names.
func columnNames(res *queryengine.Result) []string {
	cols := make([]string, len(res.Schema))
	for i, c := range res.Schema {
		cols[i] = c.Name
	}
	return cols
}

// renderTable writes an aligned, human-readable text table. nil → "NULL".
func renderTable(out io.Writer, res *queryengine.Result) error {
	cols := columnNames(res)
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	cells := make([][]string, len(res.Rows))
	for r, row := range res.Rows {
		cells[r] = make([]string, len(cols))
		for i := range cols {
			cells[r][i] = cellString(valueAt(row, i))
			if w := len(cells[r][i]); w > widths[i] {
				widths[i] = w
			}
		}
	}
	writeRow := func(vals []string) {
		for i, v := range vals {
			if i > 0 {
				fmt.Fprint(out, "  ")
			}
			fmt.Fprintf(out, "%-*s", widths[i], v)
		}
		fmt.Fprintln(out)
	}
	writeRow(cols)
	sep := make([]string, len(cols))
	for i := range sep {
		sep[i] = strings.Repeat("-", widths[i])
	}
	writeRow(sep)
	for _, c := range cells {
		writeRow(c)
	}
	return nil
}

// renderCSV writes RFC4180-ish CSV with a header row. nil → empty field.
func renderCSV(out io.Writer, res *queryengine.Result) error {
	cols := columnNames(res)
	w := csv.NewWriter(out)
	if err := w.Write(cols); err != nil {
		return err
	}
	rec := make([]string, len(cols))
	for _, row := range res.Rows {
		for i := range cols {
			v := valueAt(row, i)
			if v == nil {
				rec[i] = ""
			} else {
				rec[i] = fmt.Sprintf("%v", v)
			}
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// valueAt safely indexes a row, returning nil for a short row.
func valueAt(row []any, i int) any {
	if i < len(row) {
		return row[i]
	}
	return nil
}

// cellString renders a value for the table form. nil → "NULL".
func cellString(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}
