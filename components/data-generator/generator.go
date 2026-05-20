package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

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

// generateValue returns a random value for the given column type.
// rng must be non-nil.
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

// encodeRow encodes a map[string]any as a JSONL line (no trailing newline —
// the caller adds the separator).
func encodeRow(row map[string]any) ([]byte, error) {
	return json.Marshal(row)
}

// runRandom runs the random-mode write loop for a single table.
// It opens a writer, generates random rows until a limit is reached or a
// user-error fires, then closes the writer. Returns an error on infrastructure
// failures; calls sdk.ExitUserError directly for user-error injection.
func runRandom(ctx context.Context, client *sdk.Client, cfg *sdk.Config, t *Table) (int, error) {
	r := t.Random
	rng := newRNG(cfg.ExecutionID, t.Name)

	// Determine user-error injection point (if configured).
	errAt := errInjectionPoint(rng, r)

	// data-generator emits JSON-encoded rows (encodeRow uses json.Marshal),
	// one per Write call separated by newlines — i.e. JSONL. The SDK's
	// default input format is CSV, so we MUST declare JSONL explicitly or
	// the gateway will try to parse our JSON as CSV and fail at column 1.
	writer, err := client.OpenWriter(ctx, t.Name, sdk.WithFormat(pb.DataFormat_FORMAT_JSONL))
	if err != nil {
		return 0, fmt.Errorf("table %q: failed to open writer: %w", t.Name, err)
	}

	// Build ordered column names for stable JSON key ordering.
	colNames := make([]string, 0, len(r.Schema))
	for name := range r.Schema {
		colNames = append(colNames, name)
	}
	sortStrings(colNames)

	startTime := time.Now()
	rowsWritten := 0
	bytesWritten := 0

	var buf bytes.Buffer

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

		// Generate row.
		row := make(map[string]any, len(colNames))
		for _, name := range colNames {
			row[name] = generateValue(rng, r.Schema[name])
		}

		lineBytes, err := encodeRow(row)
		if err != nil {
			return rowsWritten, fmt.Errorf("table %q: failed to encode row %d: %w", t.Name, rowsWritten, err)
		}

		buf.Write(lineBytes)
		buf.WriteByte('\n')
		lineLen := len(lineBytes) + 1

		if err := writer.Write(ctx, buf.Bytes()); err != nil {
			return rowsWritten, fmt.Errorf("table %q: failed to write row %d: %w", t.Name, rowsWritten, err)
		}
		buf.Reset()

		rowsWritten++
		bytesWritten += lineLen

		if t.RowInsertSpeed > 0 {
			time.Sleep(time.Duration(t.RowInsertSpeed) * time.Millisecond)
		}
	}

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
