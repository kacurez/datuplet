//go:build duckdb_arrow

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
	sdkArrow "github.com/datuplet/datuplet/sdk/go/arrow"
)

// registerInput opens an Arrow IPC stream from DG over the given input table,
// stream-writes each record batch to a temp parquet file on local ephemeral
// disk, then exposes the file to user SQL as a DuckDB view via
// `read_parquet(...)`. Returns a release func that the caller MUST defer.
//
// Why stage to disk? An earlier implementation registered the Arrow stream as
// a DuckDB view via arrow_scan and materialised it with
// `CREATE TABLE x AS SELECT * FROM <arrow_view>`. That loaded the entire
// input into DuckDB-managed in-memory storage and OOM'd on multi-GB inputs
// (5M-row tables exceeded 3.5 GiB of process RSS before user SQL even ran).
//
// Streaming the input to a snappy-compressed parquet file uses bounded RAM —
// only one record batch is alive at a time — and trades the cost for ephemeral
// disk. DuckDB's `read_parquet` reads row groups lazily and is fully
// multi-scan-safe (GROUP BY / JOIN / UNION ALL all work correctly across it),
// so this also drops the duckdb/duckdb#19040 workaround the old code carried.
//
// Empty inputs (parquet file has zero rows): Arrow stream returns no batches;
// the staged parquet file is still created with the schema, and DuckDB's
// `read_parquet` handles a zero-row file as an empty relation.
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

	// Stage the input as a snappy parquet file on local ephemeral disk.
	// Snappy is fast and keeps the staged file close to wire size — at
	// ~5M rows of the products schema we expect ~150-250 MiB on disk.
	stagePath := filepath.Join(workdir, "in_"+userName+".parquet")
	f, err := os.Create(stagePath)
	if err != nil {
		arrowReader.Release()
		return nil, fmt.Errorf("create staged input %q: %w", stagePath, err)
	}

	// Tight writer props tuned for the "staging file DuckDB reads once
	// and we delete" lifecycle. The triad below dropped peak inuse heap
	// from ~2.4 GB → ~150 MB on the 5M-row products test:
	//
	//   - WithCompression(Uncompressed): no codec. Pyroscope of the
	//     6m32s successful run showed s2.EncodeSnappyBetter +
	//     encodeBlockBetterSnappy accounted for ~48% of CPU during
	//     staging (24s of 50s total). Local ephemeral disk is fast
	//     (~500+ MB/s) and the file is read once by DuckDB then
	//     deleted, so the wire-size win of snappy doesn't pay back
	//     for the per-write encode cost. Staged file size grows from
	//     ~250 MiB → ~500 MiB on this dataset — comfortably under our
	//     16 GiB ephemeral-storage limit.
	//   - WithStats(false): DuckDB scans the file end-to-end via
	//     read_parquet — there's no predicate-pushdown user. Stats
	//     pages add ~150-250 MB of allocator churn at write time and
	//     buy nothing on read.
	//   - WithDictionaryDefault(false): disables dictionary encoding
	//     for ALL columns. A previous run's Pyroscope showed
	//     ~766 MB of hashing.HashTable.upsize in DictEncoder for the
	//     high-cardinality `name` column. Dict is only a wire-size win
	//     for low-cardinality columns; for this ephemeral file the
	//     wire-size delta is paid back in <1 s of extra IO, and DuckDB
	//     read_parquet handles plain-encoded columns identically.
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Uncompressed),
		parquet.WithStats(false),
		parquet.WithDictionaryDefault(false),
	)
	arrowProps := pqarrow.DefaultWriterProps()
	pqw, err := pqarrow.NewFileWriter(arrowReader.Schema(), f, writerProps, arrowProps)
	if err != nil {
		f.Close()           //nolint:errcheck
		os.Remove(stagePath) //nolint:errcheck
		arrowReader.Release()
		return nil, fmt.Errorf("pqarrow.NewFileWriter: %w", err)
	}

	// Drain the Arrow stream batch-by-batch into the parquet writer.
	// Each Arrow batch becomes one parquet row group (flushed
	// immediately via pqw.Write). Combined with dict+stats both off
	// in writerProps above, per-row-group state stays small even when
	// the file ends up with hundreds of row groups — Pyroscope on a
	// 5M-row test showed component peak inuse heap dropped from
	// ~2.4 GB to ~300 MB by going dict-off + per-batch writes.
	//
	// Earlier this code coalesced batches in-process and built a
	// single large arrow.Record before each Write. That made the
	// concat step (array.Concatenate per column) the new peak —
	// 1.7 GB live during the flush of a 256k-row coalesced batch
	// because variable-length string columns reallocate large
	// buffers — and forced a 766 MB dict-encoder hash table per
	// flush for the high-cardinality `name` column. Per-batch
	// writes + dict off avoids both.
	failStage := func(err error, what string) error {
		pqw.Close()          //nolint:errcheck
		f.Close()            //nolint:errcheck
		os.Remove(stagePath) //nolint:errcheck
		arrowReader.Release()
		return fmt.Errorf("%s: %w", what, err)
	}

	for arrowReader.Next() {
		rec := arrowReader.RecordBatch()
		if err := pqw.Write(rec); err != nil {
			return nil, failStage(err, "write parquet row group")
		}
	}
	if err := arrowReader.Err(); err != nil {
		return nil, failStage(err, "arrow stream")
	}

	// Close the Arrow stream first (drops gRPC reader + sdk.Reader), then
	// finalise the parquet file (writes the footer). pqw.Close() closes
	// the underlying io.Writer (our *os.File) too, so an explicit
	// f.Close() afterwards returns os.ErrClosed — don't double-close.
	arrowReader.Release()
	if err := pqw.Close(); err != nil {
		os.Remove(stagePath) //nolint:errcheck
		return nil, fmt.Errorf("pqarrow close: %w", err)
	}

	client.Log(ctx, "INFO", fmt.Sprintf("staged input %s.%s -> %s", in.Bucket, in.Table, stagePath)) //nolint:errcheck

	// Expose the staged file to user SQL as a multi-scan-safe view.
	// read_parquet lazy-streams row groups from disk on each scan.
	createViewStmt := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet('%s')",
		userName, escapeSQL(stagePath))
	if _, err := conn.ExecContext(ctx, createViewStmt); err != nil {
		os.Remove(stagePath) //nolint:errcheck
		return nil, fmt.Errorf("CREATE VIEW %s: %w", userName, err)
	}

	return func() {
		// Drop the view first so DuckDB releases its handle on the
		// staged file before we unlink it. Use a background ctx so
		// cleanup still runs if the parent ctx was cancelled.
		_, _ = conn.ExecContext(context.Background(), "DROP VIEW IF EXISTS "+userName)
		os.Remove(stagePath) //nolint:errcheck
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
