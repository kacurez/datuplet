package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// seedForTable derives a deterministic uint64 seed from runID + tableName.
// This ensures the same (run, table) pair always produces the same row sequence.
func seedForTable(runID, tableName string) uint64 {
	h := sha256.New()
	h.Write([]byte(runID))
	h.Write([]byte{0}) // separator
	h.Write([]byte(tableName))
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8])
}

// newRNG constructs a math/rand/v2 PRNG seeded deterministically from
// runID + tableName. If runID is empty, falls back to a time-based seed.
func newRNG(runID, tableName string) *rand.Rand {
	if runID == "" {
		return rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
	}
	seed := seedForTable(runID, tableName)
	return rand.New(rand.NewPCG(seed, seed^0xdeadbeefcafe1234))
}

// generateValue is retained for the JSONL-based runLiteral path
// (literal.go) which builds map[string]any rows. The high-throughput
// random path now writes directly into an Arrow RecordBuilder via
// arrow_writer.go::appendGeneratedValue, which bypasses the
// map[string]any allocation entirely.
func generateValue(rng *rand.Rand, colType string) any {
	now := time.Now().UTC()
	switch colType {
	case "string":
		b := make([]byte, 8+rng.IntN(13)) // 8–20 random bytes -> 16-40 hex chars
		for i := range b {
			b[i] = byte(rng.IntN(256))
		}
		return hex.EncodeToString(b)

	case "int":
		return int32(rng.Int32())

	case "long":
		return rng.Int64()

	case "float":
		return rng.Float32()

	case "double":
		return rng.Float64()

	case "boolean":
		return rng.IntN(2) == 0

	case "date":
		// Random date within the last 365 days.
		daysBack := time.Duration(rng.IntN(365)) * 24 * time.Hour
		d := now.Add(-daysBack)
		return d.Format("2006-01-02")

	case "timestamp":
		// Random timestamp within the last 30 days, RFC 3339 with ms.
		msBack := rng.Int64N(30 * 24 * 60 * 60 * 1000)
		t := now.Add(-time.Duration(msBack) * time.Millisecond)
		return t.Format("2006-01-02T15:04:05.000Z07:00")

	case "now":
		// Current time at row-generation time; not deterministic.
		return now.Format("2006-01-02T15:04:05.000Z07:00")

	case "uuid":
		id, err := uuid.NewRandom()
		if err != nil {
			// Extremely unlikely; fall back to pseudo-random UUID.
			var b [16]byte
			binary.LittleEndian.PutUint64(b[:8], rng.Uint64())
			binary.LittleEndian.PutUint64(b[8:], rng.Uint64())
			b[6] = (b[6] & 0x0f) | 0x40 // version 4
			b[8] = (b[8] & 0x3f) | 0x80 // variant bits
			return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
		}
		return id.String()

	default:
		return nil
	}
}

// shouldStop returns true when any active limit has been reached.
// A limit field of 0 means "unbounded".
func shouldStop(limit *Limit, rowsWritten int, bytesWritten int, startTime time.Time) bool {
	if limit == nil {
		return false
	}
	if limit.RowsCount > 0 && rowsWritten >= limit.RowsCount {
		return true
	}
	if limit.SizeInBytes > 0 && bytesWritten >= limit.SizeInBytes {
		return true
	}
	if limit.TimeoutInSeconds > 0 && time.Since(startTime) >= time.Duration(limit.TimeoutInSeconds)*time.Second {
		return true
	}
	return false
}

// runRandom runs the random-mode write loop for a single table.
//
// Wire format: Arrow IPC. Each batch is a self-contained IPC stream
// (schema header + one record batch + EOS) of up to arrowBatchRows
// rows. The gateway decodes via ArrowIPCAdapter (zero-copy where
// possible) and accumulates batches in the BufferManager for parquet
// flush — see RFC 020 Phase 2b. This replaces the per-row JSONL +
// json.Marshal path which was the dominant CPU consumer (~65% of
// gateway-side write CPU per Pyroscope of run 07a2ddec).
//
// Behaviour preserved from the JSONL path:
//   - Deterministic RNG seeded by runID + tableName.
//   - User-error injection at a chosen row index fires BEFORE the
//     stop-limit check, so a userErrorMessage with rowsCount=N still
//     fires somewhere in [0, N-1].
//   - Limits: rowsCount, sizeInBytes (bytes accounted from the IPC
//     stream actually sent), timeoutInSeconds.
//   - Per-row throttle via RowInsertSpeed milliseconds.
//
// Behaviour differences from the JSONL path:
//   - `sizeInBytes` accounting uses the IPC stream size, NOT the raw
//     JSON size. Arrow IPC for the same logical data is smaller for
//     numeric columns and similar for strings; users tuning this knob
//     against the previous JSONL baseline may need to adjust.
//   - Column ORDER is the same (sortStrings(colNames)) and column
//     types map to native Arrow types (int->Int32, double->Float64,
//     etc.) where applicable. string/uuid/date/timestamp/now stay
//     STRING so the iceberg table schema matches what the JSONL path
//     produced via gateway-side inference.
func runRandom(ctx context.Context, client *sdk.Client, cfg *sdk.Config, t *Table) (int, error) {
	r := t.Random
	rng := newRNG(cfg.ExecutionID, t.Name)

	// Determine user-error injection point (if configured).
	errAt := errInjectionPoint(rng, r)

	// Build stable column order + Arrow schema.
	colNames := make([]string, 0, len(r.Schema))
	for name := range r.Schema {
		colNames = append(colNames, name)
	}
	sortStrings(colNames)
	arrowSchema := buildArrowSchema(colNames, r.Schema)

	// Open writer in Arrow IPC mode. We don't pass WithSchema — every
	// IPC chunk we send carries its own schema header, and the gateway
	// uses that to load-or-create the iceberg table on the first chunk.
	writer, err := client.OpenWriter(ctx, t.Name, sdk.WithFormat(pb.DataFormat_FORMAT_ARROW_IPC))
	if err != nil {
		return 0, fmt.Errorf("table %q: failed to open writer: %w", t.Name, err)
	}

	pool := memory.NewGoAllocator()
	// arrowBatch builds rows into typed arrow array builders. We
	// allocate one RecordBuilder for the lifetime of the table — it's
	// reset implicitly each NewRecord() call (which builds the current
	// rows into a Record and clears the internal builders for the next
	// batch). No per-row allocator churn.
	rb := array.NewRecordBuilder(pool, arrowSchema)
	defer rb.Release()

	startTime := time.Now()
	rowsWritten := 0
	rowsInBatch := 0
	bytesWritten := 0

	// flushBatch builds the accumulated rows into an Arrow record,
	// serialises as a complete IPC stream, ships via the SDK, and
	// updates bytesWritten. No-op when rowsInBatch == 0.
	flushBatch := func() error {
		if rowsInBatch == 0 {
			return nil
		}
		rec := rb.NewRecord() // builds + resets internal builders
		defer rec.Release()
		data, err := serializeRecordToIPC(rec, pool)
		if err != nil {
			return fmt.Errorf("table %q: serialise IPC batch: %w", t.Name, err)
		}
		if err := writer.Write(ctx, data); err != nil {
			return fmt.Errorf("table %q: write IPC batch (rows %d-%d): %w",
				t.Name, rowsWritten-rowsInBatch, rowsWritten-1, err)
		}
		bytesWritten += len(data)
		rowsInBatch = 0
		return nil
	}

	for {
		// User-error injection check FIRST — must always fire when set.
		// errInjectionPoint guarantees errAt is strictly less than the stop
		// boundary, so this check fires before shouldStop ever does.
		if r.UserErrorMessage != "" && errAt != nil && *errAt == rowsWritten {
			sdk.ExitUserError(r.UserErrorMessage)
		}

		// Check stop AFTER err check (so limit=0 still emits 0 rows
		// AND a userErrorMessage with rowsCount=N fires within [0,N-1]).
		if shouldStop(r.Limit, rowsWritten, bytesWritten, startTime) {
			break
		}

		// Append one row's typed values into the per-column builders.
		now := time.Now().UTC()
		for colIdx, name := range colNames {
			appendGeneratedValue(rb, colIdx, r.Schema[name], rng, now)
		}
		rowsWritten++
		rowsInBatch++

		// Flush batch boundary. arrowBatchRows is the per-batch row
		// count (see arrow_writer.go for the tuning rationale).
		if rowsInBatch >= arrowBatchRows {
			if err := flushBatch(); err != nil {
				return rowsWritten, err
			}
		}

		if t.RowInsertSpeed > 0 {
			time.Sleep(time.Duration(t.RowInsertSpeed) * time.Millisecond)
		}
	}

	// Flush the final partial batch (rowsInBatch may be 0).
	if err := flushBatch(); err != nil {
		return rowsWritten, err
	}

	// Capture writer stats BEFORE Close so we can report SDK-side activity
	// (batch threshold, underlying POST count) even if Close fails. These
	// numbers are the cheapest way to verify SDK batching is active.
	stats := writer.Stats()
	elapsed := time.Since(startTime)
	client.Log(ctx, "INFO", fmt.Sprintf( //nolint:errcheck
		"table %q: writer stats rows=%d elapsed=%s writes=%d posts=%d batch_threshold=%d bytes_in=%d bytes_shipped=%d",
		t.Name, rowsWritten, elapsed.Round(time.Millisecond),
		stats.WriteCalls, stats.UnderlyingPosts, stats.BatchThreshold,
		stats.BytesAccepted, stats.BytesShipped,
	))

	if _, err := writer.Close(ctx); err != nil {
		return rowsWritten, fmt.Errorf("table %q: failed to close writer: %w", t.Name, err)
	}

	return rowsWritten, nil
}

// sortStrings is a simple in-place sort for a small slice of strings without
// importing "sort" (avoids an additional dependency for a trivial need). For
// large column counts this is O(n²) but typical usage has < 50 columns.
func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j] < ss[j-1]; j-- {
			ss[j], ss[j-1] = ss[j-1], ss[j]
		}
	}
}
