package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

// arrowBatchRows is the per-batch row count we accumulate in the
// array.RecordBuilder before flushing as a self-contained Arrow IPC
// stream to the SDK writer. Tuning:
//
//   - Too small (< 1k): more HTTP POSTs / more schema-repeats per
//     IPC stream / less amortisation of pqarrow row-group costs.
//   - Too large (> 128k): per-batch RecordBuilder memory peaks; the
//     final NewRecord -> IPC.Write pair holds the whole batch live in
//     two places briefly.
//   - 8192 is the empirical sweet spot for the 5M-row products
//     workload: ~610 batches, each ~80-200 KB on the wire, sub-MB
//     RecordBuilder peak per batch.
const arrowBatchRows = 8192

// arrowFieldFor returns the Arrow Field for a given column declared in
// the data-generator's user schema. Mapping:
//
//   int     -> Int32        (matches iceberg int)
//   long    -> Int64
//   float   -> Float32
//   double  -> Float64
//   boolean -> Bool
//   string, uuid, date, timestamp, now -> String
//
// date / timestamp / uuid stay STRING so the resulting iceberg table
// schema matches what the JSONL path produced via gateway-side schema
// inference (which sees strings). Switching to Arrow's native Date32 /
// Timestamp_ms here would break any downstream pipeline that already
// reads these columns as strings.
func arrowFieldFor(name, colType string) arrow.Field {
	var t arrow.DataType
	switch colType {
	case "int":
		t = arrow.PrimitiveTypes.Int32
	case "long":
		t = arrow.PrimitiveTypes.Int64
	case "float":
		t = arrow.PrimitiveTypes.Float32
	case "double":
		t = arrow.PrimitiveTypes.Float64
	case "boolean":
		t = arrow.FixedWidthTypes.Boolean
	default:
		// string, uuid, date, timestamp, now -> String
		t = arrow.BinaryTypes.String
	}
	return arrow.Field{Name: name, Type: t, Nullable: true}
}

// buildArrowSchema constructs the Arrow schema from the user's declared
// column types in the order given by `colNames`. Caller is expected to
// sort colNames first so the column order is stable across runs (same
// order the JSONL path produced via json.Marshal of a sorted-key map).
func buildArrowSchema(colNames []string, colTypes map[string]string) *arrow.Schema {
	fields := make([]arrow.Field, len(colNames))
	for i, name := range colNames {
		fields[i] = arrowFieldFor(name, colTypes[name])
	}
	return arrow.NewSchema(fields, nil)
}

// appendGeneratedValue appends one generated random value to the column
// builder at position `colIdx` in the record builder, typed by the
// user's declared column type. This is the typed-direct equivalent of
// generator.go's generateValue() — bypasses the map[string]any /
// json.Marshal intermediate that dominated the gateway's CPU in the
// JSONL path (~65% of write-side CPU per Pyroscope).
//
// rng must be non-nil. colType is the user-declared type string.
func appendGeneratedValue(rb *array.RecordBuilder, colIdx int, colType string, rng *rand.Rand, now time.Time) {
	b := rb.Field(colIdx)
	switch colType {
	case "int":
		b.(*array.Int32Builder).Append(rng.Int32())

	case "long":
		b.(*array.Int64Builder).Append(rng.Int64())

	case "float":
		b.(*array.Float32Builder).Append(rng.Float32())

	case "double":
		b.(*array.Float64Builder).Append(rng.Float64())

	case "boolean":
		b.(*array.BooleanBuilder).Append(rng.IntN(2) == 0)

	case "string":
		// 8-20 random bytes -> 16-40 hex chars (matches JSONL path).
		buf := make([]byte, 8+rng.IntN(13))
		for i := range buf {
			buf[i] = byte(rng.IntN(256))
		}
		b.(*array.StringBuilder).Append(hex.EncodeToString(buf))

	case "date":
		daysBack := time.Duration(rng.IntN(365)) * 24 * time.Hour
		d := now.Add(-daysBack)
		b.(*array.StringBuilder).Append(d.Format("2006-01-02"))

	case "timestamp":
		msBack := rng.Int64N(30 * 24 * 60 * 60 * 1000)
		t := now.Add(-time.Duration(msBack) * time.Millisecond)
		b.(*array.StringBuilder).Append(t.Format("2006-01-02T15:04:05.000Z07:00"))

	case "now":
		b.(*array.StringBuilder).Append(now.Format("2006-01-02T15:04:05.000Z07:00"))

	case "uuid":
		id, err := uuid.NewRandom()
		if err != nil {
			// Extremely unlikely; fall back to pseudo-random UUID
			// (same fallback the JSONL path uses).
			var bytes16 [16]byte
			binary.LittleEndian.PutUint64(bytes16[:8], rng.Uint64())
			binary.LittleEndian.PutUint64(bytes16[8:], rng.Uint64())
			bytes16[6] = (bytes16[6] & 0x0f) | 0x40 // version 4
			bytes16[8] = (bytes16[8] & 0x3f) | 0x80 // variant bits
			b.(*array.StringBuilder).Append(
				fmt.Sprintf("%x-%x-%x-%x-%x", bytes16[0:4], bytes16[4:6], bytes16[6:8], bytes16[8:10], bytes16[10:]))
			return
		}
		b.(*array.StringBuilder).Append(id.String())

	default:
		// Unknown declared type — append null so the resulting parquet
		// keeps the schema width but the value is empty (matches the
		// JSONL path's `nil` fallback).
		b.AppendNull()
	}
}

// serializeRecordToIPC turns one arrow.Record into a complete,
// self-contained Arrow IPC stream (schema + record + EOS). Each chunk
// sent to the DG must be a complete IPC stream because the gateway's
// ArrowIPCAdapter.Parse processes one []byte at a time — it can't
// re-use a schema header across chunks.
//
// Schema-repeat overhead is small (~50-500 bytes per batch for typical
// 5-50 column schemas), negligible compared to the per-batch row data
// (tens of KB to MBs). Caller's record is NOT released by this function.
func serializeRecordToIPC(rec arrow.Record, alloc memory.Allocator) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf,
		ipc.WithSchema(rec.Schema()),
		ipc.WithAllocator(alloc),
	)
	if err := w.Write(rec); err != nil {
		w.Close() //nolint:errcheck
		return nil, fmt.Errorf("ipc write record: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("ipc close writer: %w", err)
	}
	return buf.Bytes(), nil
}

// arrowRowSeed re-derives the same row-key bytes used previously by
// generator.go::seedForTable for run-token-stable PRNG seeding. Exposed
// here as a no-op wrapper so callers don't have to import another file
// just to read one symbol.
func arrowRowSeed(runID, tableName string) uint64 {
	h := sha256.New()
	h.Write([]byte(runID))
	h.Write([]byte{0})
	h.Write([]byte(tableName))
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8])
}
