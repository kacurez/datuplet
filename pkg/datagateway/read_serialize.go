package datagateway

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
)

// encodeChunk serializes batches (each with schema sch) as one read chunk:
// FORMAT_ARROW_IPC one IPC stream, FORMAT_PARQUET one self-contained ZSTD
// Parquet file (a zero-row file with sch when there are no batches),
// CSV/JSON/JSONL through adapter's Serialize of the concatenated batches. It
// does not take ownership of the batches.
func encodeChunk(f format.DataFormat, adapter format.FormatAdapter, sch *arrow.Schema, batches []arrow.RecordBatch, mem memory.Allocator) ([]byte, error) {
	switch f {
	case format.FormatArrowIPC:
		return encodeArrowIPC(sch, batches, mem)
	case format.FormatParquet:
		return encodeParquet(sch, batches, mem)
	case format.FormatCSV, format.FormatJSON, format.FormatJSONL:
		if adapter == nil {
			return nil, fmt.Errorf("no adapter for output format %s", f)
		}
		rec, err := concatBatches(sch, batches, mem)
		if err != nil {
			return nil, err
		}
		defer rec.Release()
		return adapter.Serialize(rec)
	default:
		return nil, fmt.Errorf("unsupported output format: %s", f)
	}
}

func encodeArrowIPC(sch *arrow.Schema, batches []arrow.RecordBatch, mem memory.Allocator) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(sch), ipc.WithAllocator(mem))
	for _, rec := range batches {
		if err := w.Write(rec); err != nil {
			w.Close()
			return nil, fmt.Errorf("write Arrow IPC: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close Arrow IPC writer: %w", err)
	}
	return buf.Bytes(), nil
}

// encodeParquet writes every batch into one buffered row group (the plane's
// batches are small; a row group per batch would bloat the footer).
func encodeParquet(sch *arrow.Schema, batches []arrow.RecordBatch, mem memory.Allocator) ([]byte, error) {
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithAllocator(mem),
	)
	w, err := pqarrow.NewFileWriter(sch, &buf, props, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	if err != nil {
		return nil, fmt.Errorf("open Parquet writer: %w", err)
	}
	for _, rec := range batches {
		if err := w.WriteBuffered(rec); err != nil {
			w.Close()
			return nil, fmt.Errorf("write Parquet: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close Parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

// concatBatches returns one record holding every row of batches (a zero-row
// record with sch when there are none). The caller releases it.
func concatBatches(sch *arrow.Schema, batches []arrow.RecordBatch, mem memory.Allocator) (arrow.RecordBatch, error) {
	if len(batches) == 1 {
		batches[0].Retain()
		return batches[0], nil
	}
	cols := make([]arrow.Array, sch.NumFields())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	var rows int64
	for _, rec := range batches {
		rows += rec.NumRows()
	}
	for i := range cols {
		if len(batches) == 0 {
			b := array.NewBuilder(mem, sch.Field(i).Type)
			cols[i] = b.NewArray()
			b.Release()
			continue
		}
		parts := make([]arrow.Array, len(batches))
		for j, rec := range batches {
			parts[j] = rec.Column(i)
		}
		c, err := array.Concatenate(parts, mem)
		if err != nil {
			return nil, fmt.Errorf("concatenate column %q: %w", sch.Field(i).Name, err)
		}
		cols[i] = c
	}
	return array.NewRecordBatch(sch, cols, rows), nil
}

// batchBytes is the uncompressed size of rec's buffers (the chunk-size
// measure).
func batchBytes(rec arrow.RecordBatch) int64 {
	var n int64
	for _, col := range rec.Columns() {
		n += arrayDataBytes(col.Data())
	}
	return n
}

func arrayDataBytes(d arrow.ArrayData) int64 {
	var n int64
	for _, b := range d.Buffers() {
		if b != nil {
			n += int64(b.Len())
		}
	}
	for _, c := range d.Children() {
		n += arrayDataBytes(c)
	}
	return n
}
