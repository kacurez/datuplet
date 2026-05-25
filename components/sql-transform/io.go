//go:build duckdb_arrow

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// registerInput opens a parquet byte-passthrough stream from DG over the
// given input table, drains each source parquet file 1:1 to local
// ephemeral disk, then exposes the file list to user SQL as a DuckDB
// view via `read_parquet([...])`. Returns a release func that the
// caller MUST defer.
//
// Wire shape: DG's gcsReader.readParquetFile emits each source parquet
// file as ONE DataChunk{Format: parquet, Data: <whole file bytes>}.
// server_v2_reading.go's same-format passthrough branch sends those
// bytes verbatim — no decode/encode hops between GCS and the component.
// Earlier we routed this same data through FORMAT_ARROW_IPC, which
// forced the gateway to decode parquet→Arrow row groups + re-encode
// them as IPC, and the component to re-decode IPC + re-encode as
// parquet. That round-trip ate ~50% of CPU on the staging phase per
// Pyroscope.
//
// Each chunk in this stream is one self-contained source parquet file
// (one element of lakekeeper's iceberg snapshot data-file list), so we
// dump each chunk to its own local path under `workdir/in_<name>_NNN.parquet`
// and feed the list to DuckDB's read_parquet(['…','…']) — which
// natively scans across multiple files and is multi-pass-safe.
//
// Empty input (lakekeeper returned no data files): the gRPC stream
// closes immediately with no chunks. We still create the DuckDB view
// over an empty file list so the user SQL sees an empty relation
// without erroring.
func registerInput(ctx context.Context, conn *sql.Conn, client *sdk.Client, workdir string, in sdk.TableRef) (release func(), err error) {
	viewIdent := in.LogicalName
	if viewIdent == "" {
		viewIdent = in.Table
	}
	userName, err := sanitizeIdent(viewIdent)
	if err != nil {
		return nil, err
	}

	sdkReader, err := client.OpenReader(ctx, in.Bucket, in.Table,
		sdk.WithOutputFormat(pb.DataFormat_FORMAT_PARQUET),
	)
	if err != nil {
		return nil, fmt.Errorf("OpenReader: %w", err)
	}
	// HTTP, not gRPC. The gateway's gRPC ReadChunk caps at 64 MiB
	// (SDK's MaxCallRecvMsgSize); large source parquet files in iceberg
	// snapshots routinely exceed that (we've observed 246 MiB single
	// files). The HTTP /data/read/{readerID} endpoint has no
	// message-size cap — each GET returns one chunk; the gateway sets
	// X-Datuplet-Is-Last on the final response to terminate the loop.
	endpoint := sdkReader.HTTPEndpoint()
	if endpoint == "" {
		sdkReader.Close(ctx) //nolint:errcheck
		return nil, fmt.Errorf("OpenReader: gateway returned empty HTTP endpoint (required for parquet passthrough — large source files exceed gRPC 64 MiB cap)")
	}
	httpClient := &http.Client{}

	// Drain chunks → one local parquet file per chunk. Track everything
	// we wrote so a partial-failure cleanup unlinks all stages, and so
	// the success-path release func has the list.
	var stagedFiles []string
	cleanupStaged := func() {
		for _, p := range stagedFiles {
			os.Remove(p) //nolint:errcheck
		}
	}
	failStage := func(err error, what string) error {
		cleanupStaged()
		sdkReader.Close(ctx) //nolint:errcheck
		return fmt.Errorf("%s: %w", what, err)
	}

	for {
		isLast, n, err := readNextChunkToFile(ctx, httpClient, endpoint, workdir, userName, len(stagedFiles))
		if err != nil {
			return nil, failStage(err, "parquet stream")
		}
		if n > 0 {
			stagedFiles = append(stagedFiles, filepath.Join(workdir, fmt.Sprintf("in_%s_%04d.parquet", userName, len(stagedFiles))))
		}
		if isLast {
			break
		}
	}
	// Close the sdk.Reader so the gateway drops the readerState entry
	// and releases the underlying backend reader (which Cancels the
	// prefetcher goroutines). Idempotent.
	sdkReader.Close(ctx) //nolint:errcheck

	client.Log(ctx, "INFO", fmt.Sprintf("staged input %s.%s -> %d file(s) in %s",
		in.Bucket, in.Table, len(stagedFiles), workdir)) //nolint:errcheck

	// Build CREATE VIEW. DuckDB's read_parquet([...]) accepts a
	// single-quoted list of paths. We single-quote-escape each path
	// against pathological filenames (workdir is mktemp'd so this is
	// belt-and-braces — should never contain a quote).
	var viewSQL string
	if len(stagedFiles) == 0 {
		// No data files: emit a SELECT NULL-shaped view that satisfies
		// user-SQL FROM lookups but resolves to zero rows. Use the
		// gateway-reported schema if available; otherwise an empty
		// SELECT will do — DuckDB will surface a column-not-found error
		// from user SQL, which is the same behaviour as a real
		// zero-row table when columns are referenced.
		viewSQL = fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM (SELECT 1) WHERE 1=0", userName)
	} else {
		quoted := make([]string, len(stagedFiles))
		for i, p := range stagedFiles {
			quoted[i] = "'" + escapeSQL(p) + "'"
		}
		viewSQL = fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet([%s])",
			userName, strings.Join(quoted, ", "))
	}
	if _, err := conn.ExecContext(ctx, viewSQL); err != nil {
		cleanupStaged()
		return nil, fmt.Errorf("CREATE VIEW %s: %w", userName, err)
	}

	return func() {
		// Drop the view first so DuckDB releases its handles on the
		// staged files before we unlink them. Background ctx so
		// cleanup still runs if the parent ctx was cancelled.
		_, _ = conn.ExecContext(context.Background(), "DROP VIEW IF EXISTS "+userName)
		cleanupStaged()
	}, nil
}

// readNextChunkToFile GETs one chunk from the gateway's HTTP read
// endpoint and streams the response body straight to disk. Returns
// (isLast, bytesWritten, error). bytesWritten is 0 when the gateway
// signals end-of-stream with an empty body (X-Datuplet-Is-Last=true and
// no content); in that case no file is created and the caller should
// just terminate the loop.
//
// Streaming via io.Copy keeps component memory bounded — even on a
// 250 MiB source parquet file we only hold the HTTP transport's
// internal read buffer (~32 KiB) plus the os.File buffer in memory,
// not the whole file. This is the key memory win vs. a naive
// http.ReadAll → os.WriteFile loop.
func readNextChunkToFile(ctx context.Context, hc *http.Client, endpoint, workdir, userName string, idx int) (isLast bool, n int64, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, 0, fmt.Errorf("build GET %s: %w", endpoint, err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return false, 0, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Drain a small amount of the body for the error message
		// without copying multi-MB of error pages into memory.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, 0, fmt.Errorf("GET %s: status %d body=%q", endpoint, resp.StatusCode, string(body))
	}
	isLast = resp.Header.Get("X-Datuplet-Is-Last") == "true"

	// Special case: gateway signals end-of-stream with isLast=true and
	// no body (Content-Length=0). Don't create a zero-byte file in
	// that case — DuckDB rejects empty parquet (no footer).
	if resp.ContentLength == 0 {
		return isLast, 0, nil
	}

	path := filepath.Join(workdir, fmt.Sprintf("in_%s_%04d.parquet", userName, idx))
	f, err := os.Create(path)
	if err != nil {
		return false, 0, fmt.Errorf("create %s: %w", path, err)
	}
	n, err = io.Copy(f, resp.Body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path) //nolint:errcheck
		return false, 0, fmt.Errorf("write %s: %w", path, err)
	}
	if n == 0 {
		// Header said body, gateway sent nothing — same handling as
		// ContentLength=0: don't keep the empty file.
		os.Remove(path) //nolint:errcheck
		return isLast, 0, nil
	}
	return isLast, n, nil
}

// writeOutput materializes one DuckDB table to local parquet then streams
// it to DG via OpenWriter + WriteChunk. The DG writer handles parquet
// re-emission to the iceberg target prefix using lakekeeper-vended creds
// and writes the per-target files.json the post-stage iceberg-job consumes.
//
// Returns the row count of the produced output (best-effort — DG also
// reports total_rows on Close).
//
// Note: writeOutput takes *sql.Conn (not *sql.DB) because main.go pins a
// single *sql.Conn under MaxOpenConns=1 — using db.ExecContext here would
// deadlock waiting for the held conn.
func writeOutput(ctx context.Context, conn *sql.Conn, client *sdk.Client, workdir string, out sdk.OutputTableRef) (int64, error) {
	srcTable, err := sanitizeIdent(out.Name)
	if err != nil {
		return 0, fmt.Errorf("output %s.%s: %w", out.Bucket, out.Table, err)
	}

	// Stage parquet locally so we can stream chunks to DG.
	stagePath := filepath.Join(workdir, "out_"+out.Bucket+"_"+out.Table+".parquet")
	copyStmt := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
		srcTable, escapeSQL(stagePath))
	if _, err := conn.ExecContext(ctx, copyStmt); err != nil {
		return 0, fmt.Errorf("COPY TO: %w", err)
	}
	defer os.Remove(stagePath) //nolint:errcheck

	data, err := os.ReadFile(stagePath)
	if err != nil {
		return 0, fmt.Errorf("read staged parquet: %w", err)
	}
	if len(data) == 0 {
		return 0, fmt.Errorf("DuckDB COPY TO produced empty parquet for %s", srcTable)
	}

	// Open a DG writer expecting parquet input.
	writer, err := client.OpenWriterToBucket(ctx, out.Bucket, out.Table,
		sdk.WithFormat(pb.DataFormat_FORMAT_PARQUET),
	)
	if err != nil {
		return 0, fmt.Errorf("OpenWriter: %w", err)
	}

	// Single-shot WriteChunk: DG's HTTP endpoint has no size limit; gRPC
	// caps at 4 MiB so very large outputs would need chunking. POC
	// posture: rely on HTTP being available in cluster + docker modes;
	// fail clearly otherwise.
	if writer.HTTPEndpoint() == "" && len(data) > 3*1024*1024 {
		return 0, fmt.Errorf("output %s.%s: %d bytes exceeds gRPC chunk limit and DG returned no HTTP endpoint",
			out.Bucket, out.Table, len(data))
	}
	res, err := writer.WriteChunk(ctx, data)
	if err != nil {
		return 0, fmt.Errorf("WriteChunk: %w", err)
	}

	closeRes, err := writer.Close(ctx)
	if err != nil {
		return 0, fmt.Errorf("Close: %w", err)
	}

	rowsAccepted := res.RowsAccepted
	if closeRes != nil && closeRes.TotalRows > 0 {
		rowsAccepted = closeRes.TotalRows
	}
	return rowsAccepted, nil
}
