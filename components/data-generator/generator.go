package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
	dgarrow "github.com/datuplet/datuplet/sdk/go/arrow"
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

// finishRandomQuietly calls sink.Finish() and discards the result. It
// exists because sdk.ExitUserError/ExitAppError call os.Exit, which skips
// all of runRandom's deferred cleanup — so an already-opened gateway writer
// (e.g. a run that flushed one or more batches before hitting the injected
// user error) would otherwise be left open. Call this immediately before
// any sdk.Exit* that can run after the sink was constructed. Finish is
// idempotent, so a later real Finish call (or another finishRandomQuietly
// call, e.g. via the deferred cleanup in runRandom) is a harmless no-op.
func finishRandomQuietly(sink *dgarrow.Sink) {
	_, _, _ = sink.Finish()
}

// runRandom runs the random-mode write loop for a single table via the
// dgarrow.Sink core (sdk/go/arrow/typed_sink.go): rows are appended
// directly into the sink's RecordBuilder (one typed value per column, via
// appendGeneratedValue) and RowDone() flushes a self-contained Arrow IPC
// stream every dgarrow.DefaultBatchRows rows.
//
// Batch-size tuning (established for this component before dgarrow.Sink
// existed; carried forward unchanged as dgarrow.DefaultBatchRows = 8192):
//   - Too small (< 1k): more HTTP POSTs / more schema-repeats per IPC
//     stream / less amortisation of pqarrow row-group costs.
//   - Too large (> 128k): per-batch RecordBuilder memory peaks; the final
//     NewRecord -> IPC.Write pair holds the whole batch live in two
//     places briefly.
//   - 8192 is the empirical sweet spot for the 5M-row products workload:
//     ~610 batches, each ~80-200 KB on the wire, sub-MB RecordBuilder
//     peak per batch.
//
// Wire format: Arrow IPC. The gateway decodes via ArrowIPCAdapter
// (zero-copy where possible) and accumulates batches in the BufferManager
// for parquet flush — see RFC 020 Phase 2b. This replaces the per-row
// JSONL + json.Marshal path which was the dominant CPU consumer (~65% of
// gateway-side write CPU per Pyroscope of run 07a2ddec).
//
// Behaviour preserved from the JSONL path:
//   - Deterministic RNG seeded by runID + tableName.
//   - User-error injection at a chosen row index fires BEFORE the
//     stop-limit check, so a userErrorMessage with rowsCount=N still
//     fires somewhere in [0, N-1].
//   - Limits: rowsCount, sizeInBytes (bytes accounted from the IPC
//     stream actually sent — sourced from sink.BytesShipped(), which
//     like the old counter only advances when a batch actually flushes,
//     so the limit still trips at batch granularity, not per row),
//     timeoutInSeconds.
//   - Per-row throttle via RowInsertSpeed milliseconds.
//
// Behaviour differences from the JSONL path:
//   - `sizeInBytes` accounting uses the IPC stream size, NOT the raw
//     JSON size. Arrow IPC for the same logical data is smaller for
//     numeric columns and similar for strings; users tuning this knob
//     against the previous JSONL baseline may need to adjust.
//   - Column ORDER is the same (sortStrings(colNames)) and column
//     types map to native Arrow types (int->Int64, double->Float64,
//     etc.) where applicable. string/uuid/date/timestamp/now stay
//     STRING so the iceberg table schema matches what the JSONL path
//     produced via gateway-side inference.
//   - The writer now opens lazily on the first flush (a dgarrow.Sink
//     guarantee) rather than eagerly before the loop starts. In
//     practice every valid config (validateRandom requires at least one
//     non-zero limit) generates at least one row before any limit can
//     trip, so this is unobservable outside of that theoretical
//     zero-row edge case.
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

	// Open writer in Arrow IPC mode lazily, on the sink's first flush. We
	// don't pass WithSchema — every IPC chunk we send carries its own
	// schema header, and the gateway uses that to load-or-create the
	// iceberg table on the first chunk.
	sink := dgarrow.NewSink(ctx, func() (dgarrow.ChunkWriter, error) {
		return client.OpenWriter(ctx, t.Name, sdk.WithFormat(pb.DataFormat_FORMAT_ARROW_IPC))
	}, arrowSchema)
	// Every return path below must close any writer the sink opened.
	// Finish is idempotent, so calling it again after the explicit call in
	// the normal-completion path (or after finishRandomQuietly, below) is a
	// harmless no-op. This is what fixes the sibling-goroutine leak:
	// main.go cancels the shared run context when any one table's
	// goroutine fails, and every other table's goroutine then observes
	// that cancellation as a RowDone/Finish WriteError and returns early —
	// previously with its writer left open.
	defer finishRandomQuietly(sink)

	startTime := time.Now()
	rowsWritten := 0 // total rows appended so far; drives the injection point + limit checks
	pendingRows := 0 // rows appended since the last successful flush

	for {
		// User-error injection check FIRST — must always fire when set.
		// errInjectionPoint guarantees errAt is strictly less than the stop
		// boundary, so this check fires before shouldStop ever does.
		if r.UserErrorMessage != "" && errAt != nil && *errAt == rowsWritten {
			// os.Exit (inside ExitUserError) skips the defer above, so
			// finish explicitly first — the sink may already own an open
			// writer from an earlier flushed batch.
			finishRandomQuietly(sink)
			sdk.ExitUserError(r.UserErrorMessage)
		}

		// Check stop AFTER err check (so limit=0 still emits 0 rows
		// AND a userErrorMessage with rowsCount=N fires within [0,N-1]).
		if shouldStop(r.Limit, rowsWritten, int(sink.BytesShipped()), startTime) {
			break
		}

		// Append one row's typed values directly into the sink's builder.
		now := time.Now().UTC()
		rb := sink.Builder()
		for colIdx, name := range colNames {
			appendGeneratedValue(rb, colIdx, r.Schema[name], rng, now)
		}
		rowsWritten++
		pendingRows++

		if err := sink.RowDone(); err != nil {
			// The batch containing pendingRows never landed — don't count
			// those rows as written (fixes a pre-port bug where a failed
			// flush's rows were still included in the returned count).
			return rowsWritten - pendingRows, fmt.Errorf("table %q: %w", t.Name, err)
		}
		if pendingRows == dgarrow.DefaultBatchRows {
			// RowDone crossed the batch boundary and its flush succeeded.
			pendingRows = 0
		}

		if t.RowInsertSpeed > 0 {
			time.Sleep(time.Duration(t.RowInsertSpeed) * time.Millisecond)
		}
	}

	// Flush the final partial batch and close the writer.
	rows, _, err := sink.Finish()

	// Writer stats: Sink.Writer() stays populated after Finish (even on
	// failure) as long as a writer was ever opened, so SDK-side batching
	// activity is still visible here. dgarrow.ChunkWriter intentionally
	// narrows away Stats() (it's only meaningful for the concrete SDK
	// writer, not the interface the core Sink deals in) — but the writer
	// our own `open` closure above returns is always *sdk.Writer, so the
	// type assertion is safe. Unlike the pre-port code, this can run
	// whether or not Finish's internal flush succeeded (Sink doesn't
	// distinguish a flush failure from a close failure), so this may log
	// even when the pre-port code would have skipped the log entirely.
	if w, ok := sink.Writer().(*sdk.Writer); ok {
		stats := w.Stats()
		elapsed := time.Since(startTime)
		client.Log(ctx, "INFO", fmt.Sprintf( //nolint:errcheck
			"table %q: writer stats rows=%d elapsed=%s writes=%d posts=%d batch_threshold=%d bytes_in=%d bytes_shipped=%d",
			t.Name, rowsWritten, elapsed.Round(time.Millisecond),
			stats.WriteCalls, stats.UnderlyingPosts, stats.BatchThreshold,
			stats.BytesAccepted, stats.BytesShipped,
		))
	}

	if err != nil {
		return rowsWritten - pendingRows, fmt.Errorf("table %q: %w", t.Name, err)
	}

	return int(rows), nil
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
