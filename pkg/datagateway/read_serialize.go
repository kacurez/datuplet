package datagateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// encodeChunk serializes batches (each with schema sch) as one read chunk:
// FORMAT_ARROW_IPC one IPC stream holding exactly one record batch in the
// SDK wire schema wire (see encodeArrowIPC), FORMAT_PARQUET one
// self-contained ZSTD Parquet file (a zero-row file with sch when there are
// no batches), CSV/JSON/JSONL through adapter's Serialize of the
// concatenated batches. wire is used for FORMAT_ARROW_IPC only. It does not
// take ownership of the batches.
func encodeChunk(f format.DataFormat, adapter format.FormatAdapter, sch, wire *arrow.Schema, batches []arrow.RecordBatch, mem memory.Allocator) ([]byte, error) {
	switch f {
	case format.FormatArrowIPC:
		return encodeArrowIPC(sch, wire, batches, mem)
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

// encodeArrowIPC writes the batches as one IPC stream holding exactly one
// record batch: the batches concatenated (zero rows when there are none) and
// every column cast to its type in wire, the schema the frozen SDK derives
// from OpenReaderResponse.schema (spec Read path 8).
func encodeArrowIPC(sch, wire *arrow.Schema, batches []arrow.RecordBatch, mem memory.Allocator) ([]byte, error) {
	if wire == nil {
		return nil, errors.New("arrow IPC chunk without a wire schema")
	}
	rec, err := concatBatches(sch, batches, mem)
	if err != nil {
		return nil, err
	}
	defer rec.Release()
	out, err := castToWire(rec, wire, mem)
	if err != nil {
		return nil, err
	}
	defer out.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(wire), ipc.WithAllocator(mem))
	if err := w.Write(out); err != nil {
		w.Close()
		return nil, fmt.Errorf("write Arrow IPC: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close Arrow IPC writer: %w", err)
	}
	return buf.Bytes(), nil
}

// wireCastOptions casts a table column to its SDK wire type: a zone-less
// timestamp is reinterpreted as UTC and a finer unit truncates to µs; every
// other cast is safe (an overflow or invalid value fails the chunk).
func wireCastOptions(to arrow.DataType) *compute.CastOptions {
	opts := compute.SafeCastOptions(to)
	opts.AllowTimeTruncate = true
	return opts
}

// castToWire returns rec with every column cast to its type in wire. The
// caller releases the result.
func castToWire(rec arrow.RecordBatch, wire *arrow.Schema, mem memory.Allocator) (arrow.RecordBatch, error) {
	if rec.NumCols() != int64(wire.NumFields()) {
		return nil, fmt.Errorf("record has %d columns, wire schema %d", rec.NumCols(), wire.NumFields())
	}
	ctx := compute.WithAllocator(context.Background(), mem)
	cols := make([]arrow.Array, wire.NumFields())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	for i, f := range wire.Fields() {
		col := rec.Column(i)
		if arrow.TypeEqual(col.DataType(), f.Type) {
			col.Retain()
			cols[i] = col
			continue
		}
		if va, ok := col.(*extensions.VariantArray); ok {
			c, err := variantToJSONText(va, mem)
			if err != nil {
				return nil, fmt.Errorf("cast column %q from %s to %s: %w", f.Name, col.DataType(), f.Type, err)
			}
			cols[i] = c
			continue
		}
		c, err := compute.CastArray(ctx, col, wireCastOptions(f.Type))
		if err != nil {
			return nil, fmt.Errorf("cast column %q from %s to %s: %w", f.Name, col.DataType(), f.Type, err)
		}
		cols[i] = c
	}
	return array.NewRecordBatch(wire, cols, rec.NumRows()), nil
}

// variantToJSONText renders a variant column as utf8 JSON text: SQL NULL
// stays null, a variant JSON null becomes the text null.
func variantToJSONText(va *extensions.VariantArray, mem memory.Allocator) (arrow.Array, error) {
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Reserve(va.Len())
	for i := 0; i < va.Len(); i++ {
		if format.IsSQLNullVariant(va, i) {
			b.AppendNull()
			continue
		}
		text, err := format.VariantJSON(va, i)
		if err != nil {
			return nil, err
		}
		b.Append(string(text))
	}
	return b.NewArray(), nil
}

// checkWireCasts fails when a column of sch has no cast to its type in wire.
func checkWireCasts(sch, wire *arrow.Schema) error {
	if sch.NumFields() != wire.NumFields() {
		return fmt.Errorf("table schema has %d columns, wire schema %d", sch.NumFields(), wire.NumFields())
	}
	for i, f := range sch.Fields() {
		to := wire.Field(i).Type
		if arrow.TypeEqual(f.Type, to) {
			continue
		}
		if _, ok := f.Type.(*extensions.VariantType); ok {
			if to.ID() == arrow.STRING {
				continue
			}
			return fmt.Errorf("column %q: type %s has no cast to the SDK wire type %s", f.Name, f.Type, to)
		}
		if compute.CanCast(f.Type, to) {
			continue
		}
		return fmt.Errorf("column %q: type %s has no cast to the SDK wire type %s", f.Name, f.Type, to)
	}
	return nil
}

// sdkWireSchema is the Arrow schema the frozen Go SDK derives from
// OpenReaderResponse.schema and requires of every IPC chunk. It reproduces
// protoSchemaToArrow in sdk/go/arrow/reader.go exactly (same type strings,
// Arrow types, nullability and error); TestSDKWireSchemaMatchesSDK pins it.
func sdkWireSchema(s *pb.Schema) (*arrow.Schema, error) {
	if s == nil {
		return nil, errors.New("nil proto schema")
	}
	fields := make([]arrow.Field, len(s.Columns))
	for i, c := range s.Columns {
		var t arrow.DataType
		switch c.Type {
		case "int32":
			t = arrow.PrimitiveTypes.Int32
		case "int64":
			t = arrow.PrimitiveTypes.Int64
		case "float32":
			t = arrow.PrimitiveTypes.Float32
		case "float64":
			t = arrow.PrimitiveTypes.Float64
		case "string":
			t = arrow.BinaryTypes.String
		case "boolean", "bool":
			t = arrow.FixedWidthTypes.Boolean
		case "binary":
			t = arrow.BinaryTypes.Binary
		case "timestamp":
			t = arrow.FixedWidthTypes.Timestamp_us
		case "date":
			t = arrow.FixedWidthTypes.Date32
		default:
			return nil, fmt.Errorf("unsupported type %q for column %q", c.Type, c.Name)
		}
		fields[i] = arrow.Field{Name: c.Name, Type: t, Nullable: c.Nullable}
	}
	return arrow.NewSchema(fields, nil), nil
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
