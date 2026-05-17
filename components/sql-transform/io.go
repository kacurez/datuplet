//go:build duckdb_arrow

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
	sdkArrow "github.com/datuplet/datuplet/sdk/go/arrow"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// registerInput opens an Arrow IPC stream from DG over the given input table,
// registers it as a hidden DuckDB view via Arrow.RegisterView (arrow_scan),
// then materializes the stream into a real DuckDB table that the user SQL
// references by `viewName`. Returns a release func that the caller MUST defer.
//
// Why materialize? DuckDB's arrow_scan against a single-pass Arrow stream
// produces incorrect results for queries that require multiple scans of the
// same source (GROUP BY, JOIN, UNION ALL on the same input, etc.) — see
// duckdb/duckdb#19040. Materializing once into a CTAS-backed table fully
// drains the stream and gives subsequent SQL a normal multi-pass relation.
//
// Lifetime contract: release() MUST run AFTER all DuckDB SQL that may
// reference the registered view has completed AND BEFORE the *sql.Conn
// that hosts the view is closed. Calling release while a scan is still
// active risks a use-after-free in the C bindings. See arrowFromDB in
// duckdb.go for the recommended ordering.
//
// Empty inputs (parquet file has zero rows): the streaming Arrow reader
// returns io.EOF on the first ReadChunk; DuckDB's arrow_scan handles this as
// an empty relation, and the CTAS materializes a real but zero-row table with
// the correct schema. Cold-start tables (zero data files in the snapshot)
// currently error at OpenReader — that is pre-existing behavior on main and
// is pre-existing behavior that is not currently addressed.
func registerInput(ctx context.Context, conn *sql.Conn, arrowConn *duckdb.Arrow, client *sdk.Client, in sdk.TableRef) (release func(), err error) {
	viewIdent := in.LogicalName
	if viewIdent == "" {
		viewIdent = in.Table
	}
	userName, err := sanitizeIdent(viewIdent)
	if err != nil {
		return nil, err
	}

	// Hidden internal stream view name — must be SQL-safe.
	// `__rfc011_stream_` matches sanitizeIdent's `^[A-Za-z_][A-Za-z0-9_]*$`.
	streamName := "__rfc011_stream_" + userName

	sdkReader, err := client.OpenReader(ctx, in.Bucket, in.Table,
		sdk.WithOutputFormat(pb.DataFormat_FORMAT_ARROW_IPC),
	)
	if err != nil {
		return nil, fmt.Errorf("OpenReader: %w", err)
	}
	arrowReader, err := sdkArrow.NewReader(ctx, sdkReader)
	if err != nil {
		sdkReader.Close(ctx) //nolint:errcheck
		return nil, fmt.Errorf("sdkArrow.NewReader: %w", err)
	}

	// Step 1: Register the stream under a hidden name.
	rel, err := arrowConn.RegisterView(arrowReader, streamName)
	if err != nil {
		arrowReader.Release()
		return nil, fmt.Errorf("Arrow.RegisterView %q: %w", streamName, err)
	}

	// Step 2: Materialize once into a real table the user SQL can scan
	// multiple times. This consumes the single-pass stream into a
	// DuckDB-managed table that supports multi-pass operations
	// (GROUP BY, JOIN, UNION ALL, etc.) correctly.
	// Workaround for duckdb/duckdb#19040.
	materializeStmt := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", userName, streamName)
	if _, err := conn.ExecContext(ctx, materializeStmt); err != nil {
		rel()
		arrowReader.Release()
		return nil, fmt.Errorf("materialize input %s: %w", userName, err)
	}

	// Step 3: Drop the hidden view — stream is fully consumed at this point.
	// Errors here are non-fatal (the view will be dropped when the connection
	// closes). The CTAS table is now the user-visible relation.
	dropStmt := fmt.Sprintf("DROP VIEW IF EXISTS %s", streamName)
	if _, err := conn.ExecContext(ctx, dropStmt); err != nil {
		client.Log(ctx, "WARN", fmt.Sprintf("drop hidden stream view %s: %v", streamName, err)) //nolint:errcheck
	}

	return func() {
		rel()                 // 1. drop DuckDB's internal stream ref
		arrowReader.Release() // 2. close gRPC stream + sdk.Reader
	}, nil
}

// writeOutput materializes one DuckDB table to local parquet then streams
// it to DG via OpenWriter + WriteChunk. The DG writer handles parquet
// re-emission to the iceberg target prefix using lakekeeper-vended creds
// and writes the per-target files.json the post-stage iceberg-job consumes.
//
// Returns the row count of the produced output (best-effort — DG also
// reports total_rows on Close).
//
// Note: writeOutput now takes *sql.Conn (not *sql.DB) because the
// arrowFromDB helper pins a single *sql.Conn under MaxOpenConns=1 — using
// db.ExecContext here would deadlock waiting for the held conn.
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
