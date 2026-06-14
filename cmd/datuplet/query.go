package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// queryHTTPClient is dedicated to the `datuplet query` server-routing path.
// The 5-minute timeout is a defensive ceiling on top of the server's own
// per-query clamp (RFC 022 §5.2 default 60s, max 300s) so a slow-responding
// pipeline-api can't hang the CLI forever. CheckRedirect refuses ALL redirects
// so the api-token bearer (and the SQL body on a 307) can never be forwarded to
// a redirect target (mirrors pkg/pipelineapi/queryproxy/client.go).
var queryHTTPClient = &http.Client{
	Timeout: 5 * time.Minute,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return fmt.Errorf("datuplet query: unexpected redirect from pipeline-api (refused)")
	},
}

// queryMaxResponseBytes caps the server-routed query response. The server
// enforces its own ≤10 MiB byte cap on the result envelope (§5.2); 16 MiB
// gives generous headroom for the JSON framing on top of that.
const queryMaxResponseBytes = 16 << 20

// resolveQuerySQL resolves the user's SQL from the three input sources in
// strict precedence: --sql wins, else -f FILE, else stdin. A whitespace-only
// --sql is treated as unset so it falls through to the next source. Returns a
// clear error when no source yields any SQL.
func resolveQuerySQL(sqlFlag, filePath string, stdin io.Reader) (string, error) {
	if strings.TrimSpace(sqlFlag) != "" {
		return sqlFlag, nil
	}
	if filePath != "" {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("read SQL file %s: %w", filePath, err)
		}
		if strings.TrimSpace(string(data)) == "" {
			return "", fmt.Errorf("SQL file %s is empty", filePath)
		}
		return string(data), nil
	}
	data, err := io.ReadAll(stdin)
	if err != nil {
		return "", fmt.Errorf("read SQL from stdin: %w", err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return "", fmt.Errorf("no SQL provided (use --sql, -f FILE, or pipe SQL on stdin)")
	}
	return string(data), nil
}

// serverQueryRequest mirrors the client-facing body for POST /api/v1/query
// (pkg/pipelineapi/queryproxy/handler.go::queryRequest). Only sql is required;
// the server clamps the optional limit fields.
type serverQueryRequest struct {
	SQL string `json:"sql"`
}

// serverQuery submits the SQL to the server-side query service
// (POST /api/v1/query on pipeline-api) and renders the returned
// queryengine.Result in the chosen format. This is the default route when
// the operator's allowClientSideQuery policy is off (RFC 022 §4.1): compute
// and credentials stay in-cluster and the user gets identical results.
//
// SECURITY: apiToken is the pipeline-api bearer (aud=datuplet-api). It is sent
// only in the Authorization header and is NEVER logged or echoed.
func serverQuery(remote, apiToken, sql, format string, out io.Writer) error {
	reqBody, err := json.Marshal(serverQueryRequest{SQL: sql})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	url := strings.TrimRight(remote, "/") + "/api/v1/query"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiToken)

	resp, err := queryHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, queryMaxResponseBytes))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("server query failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// The 200 body is the worker's queryengine.Result JSON
	// ({schema,rows,truncated,stats}), forwarded verbatim by the proxy.
	if format == "json" {
		return prettyPrintJSONTo(out, body)
	}
	return renderServerResult(body, format, out)
}

// renderServerResult decodes the server's Result JSON and renders it as a
// table or CSV. The wire shape is queryengine.Result; we decode only the
// fields we render here so the root binary stays duckdb-free.
func renderServerResult(body []byte, format string, out io.Writer) error {
	var res struct {
		Schema []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"schema"`
		Rows      [][]any `json:"rows"`
		Truncated bool    `json:"truncated"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		// Not the expected shape — pass the bytes through rather than fail.
		_, werr := out.Write(body)
		return werr
	}
	cols := make([]string, len(res.Schema))
	for i, c := range res.Schema {
		cols[i] = c.Name
	}
	switch format {
	case "csv":
		if err := writeCSV(out, cols, res.Rows); err != nil {
			return err
		}
	default: // "table"
		if err := writeTable(out, cols, res.Rows); err != nil {
			return err
		}
	}
	if res.Truncated {
		fmt.Fprintln(os.Stderr, "note: result truncated by the server row/byte cap")
	}
	return nil
}

// localQueryNotAvailable is invoked for `datuplet query --local`. The root
// datuplet binary is deliberately duckdb-FREE (it cannot run DuckDB), so local
// execution requires the separately-installed, duckdb-tagged `datuplet-query`
// binary. This returns a clear, actionable error.
func localQueryNotAvailable(_ io.Writer) error {
	return fmt.Errorf(`local query requires the separate duckdb-enabled binary.
The root 'datuplet' binary is duckdb-free and cannot run DuckDB locally.
Install 'datuplet-query' (built from components/queryengine/cmd/datuplet-query)
and run:  datuplet-query --sql "..."  (or -f FILE / stdin)
Without --local, 'datuplet query' routes the SQL to the server-side query service`)
}

// prettyPrintJSONTo re-encodes raw JSON with indentation to the given writer.
// Mirrors prettyPrintJSON (storage.go) but is writer-injectable for tests.
func prettyPrintJSONTo(out io.Writer, raw []byte) error {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		_, werr := out.Write(raw)
		return werr
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// runQuery implements `datuplet query`. By default it routes to the server-side
// query service; with --local it errors clearly (the duckdb-tagged
// `datuplet-query` binary is required for local execution).
func runQuery(remote, tokenFile, project, sqlFlag, filePath, format string, local bool) error {
	switch format {
	case "table", "csv", "json":
	default:
		return fmt.Errorf("invalid --format %q (want table|csv|json)", format)
	}

	if local {
		return localQueryNotAvailable(os.Stdout)
	}

	sql, err := resolveQuerySQL(sqlFlag, filePath, os.Stdin)
	if err != nil {
		return err
	}

	args, err := storageBaseArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	return serverQuery(args.Remote, args.APIToken, sql, format, os.Stdout)
}

// writeTable renders rows as an aligned, human-readable text table.
func writeTable(out io.Writer, cols []string, rows [][]any) error {
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	cells := make([][]string, len(rows))
	for r, row := range rows {
		cells[r] = make([]string, len(cols))
		for i := range cols {
			var v any
			if i < len(row) {
				v = row[i]
			}
			cells[r][i] = renderCell(v)
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

// renderCell renders a single value for human/CSV output. nil → "NULL" for
// the table form; CSV uses an empty field (handled in writeCSV).
func renderCell(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

// writeCSV renders rows as RFC4180-ish CSV with a header row. nil values are
// emitted as empty fields (distinct from the table form's "NULL" so CSV
// consumers see a true empty cell).
func writeCSV(out io.Writer, cols []string, rows [][]any) error {
	w := csv.NewWriter(out)
	if err := w.Write(cols); err != nil {
		return err
	}
	rec := make([]string, len(cols))
	for _, row := range rows {
		for i := range cols {
			var v any
			if i < len(row) {
				v = row[i]
			}
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
