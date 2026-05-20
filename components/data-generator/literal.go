package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// runLiteral runs the literal-mode write loop for a single table.
// It opens a writer, emits each row from t.Literal.Rows as JSONL, then closes
// the writer. Returns the number of rows written and any infrastructure error.
//
// Pre-condition: validateLiteral has already been called and returned no
// errors, so arity, columns presence, and type consistency are guaranteed.
func runLiteral(ctx context.Context, client *sdk.Client, t *Table) (int, error) {
	l := t.Literal
	if len(l.Rows) == 0 {
		return 0, nil
	}

	arity := len(l.Rows[0])
	colNames := l.Columns

	// data-generator emits JSON-encoded rows (json.Marshal of the rowMap),
	// one per Write call separated by newlines — i.e. JSONL. Declare JSONL
	// explicitly so the gateway parses our bytes correctly (default is CSV).
	writer, err := client.OpenWriter(ctx, t.Name, sdk.WithFormat(pb.DataFormat_FORMAT_JSONL))
	if err != nil {
		return 0, fmt.Errorf("table %q: failed to open writer: %w", t.Name, err)
	}

	var buf bytes.Buffer
	rowsWritten := 0

	for rowIdx, row := range l.Rows {
		rowMap := make(map[string]any, arity)
		for colIdx, colName := range colNames {
			if colIdx < len(row) {
				rowMap[colName] = row[colIdx]
			}
		}

		lineBytes, err := json.Marshal(rowMap)
		if err != nil {
			return rowsWritten, fmt.Errorf("table %q: failed to encode rows[%d]: %w", t.Name, rowIdx, err)
		}

		buf.Write(lineBytes)
		buf.WriteByte('\n')

		if err := writer.Write(ctx, buf.Bytes()); err != nil {
			return rowsWritten, fmt.Errorf("table %q: failed to write rows[%d]: %w", t.Name, rowIdx, err)
		}
		buf.Reset()

		rowsWritten++

		if t.RowInsertSpeed > 0 {
			time.Sleep(time.Duration(t.RowInsertSpeed) * time.Millisecond)
		}
	}

	// See runRandom for rationale: writer.Stats() before Close so the
	// batching state is visible in pod logs regardless of Close outcome.
	stats := writer.Stats()
	client.Log(ctx, "INFO", fmt.Sprintf( //nolint:errcheck
		"table %q: writer stats rows=%d writes=%d posts=%d batch_threshold=%d bytes_in=%d bytes_shipped=%d",
		t.Name, rowsWritten, stats.WriteCalls, stats.UnderlyingPosts,
		stats.BatchThreshold, stats.BytesAccepted, stats.BytesShipped,
	))

	if _, err := writer.Close(ctx); err != nil {
		return rowsWritten, fmt.Errorf("table %q: failed to close writer: %w", t.Name, err)
	}

	return rowsWritten, nil
}
