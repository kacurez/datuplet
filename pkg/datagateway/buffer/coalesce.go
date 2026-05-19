package buffer

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Coalescing thresholds applied by BufferManager.Add. They bound the
// per-Record Arrow scaffolding cost when callers write row-at-a-time: a
// single 1-row arrow.Record carries ~4 KiB of scaffolding (schema ref,
// ArrayData headers, minimum buffer allocations) regardless of payload
// size. Retaining millions of such records — as the buffer does at the
// 64 MiB estimator cap — produces multi-GiB heap usage. Coalescing copies
// small records into an in-flight builder until a row or byte threshold
// is reached, then finalizes one larger record per ~thousand rows.
const (
	// coalesceFastPathRows: records with at least this many rows skip
	// the accumulator copy and are appended to batches directly. Above
	// this size the per-Record scaffolding cost is already amortized.
	coalesceFastPathRows = 128

	// coalesceFlushRows: row-count trigger for finalizing the accumulator
	// into a buffered batch. Bench plateau is at K=1000; 1024 keeps us
	// at the plateau with a power-of-two boundary.
	coalesceFlushRows = 1024

	// coalesceFlushBytes: byte-count trigger for finalizing the accumulator.
	// Guards against pathological columns (large strings / binaries) where
	// row count alone underestimates held memory.
	coalesceFlushBytes = 2 * 1024 * 1024 // 2 MiB

	// maxBufferedBatches: hard cap on len(batches). If the per-buffer
	// estimator undercounts so severely that BufferSize is never reached,
	// this still forces a flush before memory balloons.
	maxBufferedBatches = 256
)

// schemasCompatible reports whether two Arrow schemas can be used
// interchangeably for column-by-column row copy. Only structural type
// equality matters; field-id metadata (used by Iceberg / Parquet) is
// ignored — the writer strips it anyway, and tests synthesize records
// with whatever metadata the source happens to carry.
func schemasCompatible(a, b *arrow.Schema) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.NumFields() != b.NumFields() {
		return false
	}
	for i := 0; i < a.NumFields(); i++ {
		if !arrow.TypeEqual(a.Field(i).Type, b.Field(i).Type) {
			return false
		}
	}
	return true
}

// estimateRecordPayloadBytes is the data-only size of a record (sum of
// column buffer bytes). Used to drive the accumulator's byte trigger
// without counting per-Record scaffolding.
func estimateRecordPayloadBytes(rec arrow.Record) int64 {
	var n int64
	for c := 0; c < int(rec.NumCols()); c++ {
		col := rec.Column(c)
		data := col.Data()
		for _, buf := range data.Buffers() {
			if buf != nil {
				n += int64(buf.Len())
			}
		}
	}
	return n
}

// appendRecordToBuilder copies every row of rec into b. The caller has
// already verified schema compatibility via schemasCompatible; this
// function trusts that pre-check and panics on unsupported Arrow types
// (which would be a programming error, not a data error).
func appendRecordToBuilder(b *array.RecordBuilder, rec arrow.Record) error {
	rows := int(rec.NumRows())
	cols := int(rec.NumCols())
	for c := 0; c < cols; c++ {
		src := rec.Column(c)
		dst := b.Field(c)
		for r := 0; r < rows; r++ {
			if src.IsNull(r) {
				dst.AppendNull()
				continue
			}
			if err := copyOneValue(dst, src, r); err != nil {
				return fmt.Errorf("col %d row %d: %w", c, r, err)
			}
		}
	}
	return nil
}

// copyOneValue copies src[row] into dst. Covers every Arrow type the
// gateway's schema layer can produce (mirrors pkg/datagateway/schema/types.go).
// An unhandled type returns an error rather than panicking so the caller
// can surface a clean failure to the writer.
func copyOneValue(dst array.Builder, src arrow.Array, row int) error {
	switch s := src.(type) {
	case *array.Int8:
		dst.(*array.Int8Builder).Append(s.Value(row))
	case *array.Int16:
		dst.(*array.Int16Builder).Append(s.Value(row))
	case *array.Int32:
		dst.(*array.Int32Builder).Append(s.Value(row))
	case *array.Int64:
		dst.(*array.Int64Builder).Append(s.Value(row))
	case *array.Uint8:
		dst.(*array.Uint8Builder).Append(s.Value(row))
	case *array.Uint16:
		dst.(*array.Uint16Builder).Append(s.Value(row))
	case *array.Uint32:
		dst.(*array.Uint32Builder).Append(s.Value(row))
	case *array.Uint64:
		dst.(*array.Uint64Builder).Append(s.Value(row))
	case *array.Float32:
		dst.(*array.Float32Builder).Append(s.Value(row))
	case *array.Float64:
		dst.(*array.Float64Builder).Append(s.Value(row))
	case *array.Boolean:
		dst.(*array.BooleanBuilder).Append(s.Value(row))
	case *array.String:
		dst.(*array.StringBuilder).Append(s.Value(row))
	case *array.Binary:
		dst.(*array.BinaryBuilder).Append(s.Value(row))
	case *array.Timestamp:
		dst.(*array.TimestampBuilder).Append(s.Value(row))
	case *array.Date32:
		dst.(*array.Date32Builder).Append(s.Value(row))
	case *array.Date64:
		dst.(*array.Date64Builder).Append(s.Value(row))
	default:
		return fmt.Errorf("unsupported Arrow type %T for coalesce copy", src)
	}
	return nil
}
