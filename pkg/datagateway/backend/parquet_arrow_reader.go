// Package backend — parquet_arrow_reader.go.
//
// Streams parquet → arrow record batches at row-group granularity. Each
// returned DataChunk is an Arrow IPC stream containing schema + ONE record
// batch. Designed to back DG's OpenReader(FORMAT_ARROW_IPC) path so the
// component never sees a whole-file buffer in memory.

package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// fileSource is the io.ReaderAt-based abstraction for a single parquet file.
// LocalBackend uses os.File (implements both io.ReaderAt and io.Seeker directly).
// MinIOBackend uses minioRangeReaderAt wrapped in readerAtSeeker.
// The closer field is optional — only os.File-backed sources set it.
type fileSource struct {
	name   string
	ra     io.ReaderAt
	size   int64
	closer io.Closer // optional — only set for os.File-backed sources
}

// seekableReaderAt wraps an io.ReaderAt + size into a parquet.ReaderAtSeeker.
// Required because file.NewParquetReader wants Read+ReadAt+Seek, but
// minioRangeReaderAt only implements ReadAt.
type seekableReaderAt struct {
	ra   io.ReaderAt
	size int64
	pos  int64
}

func (r *seekableReaderAt) Read(p []byte) (int, error) {
	n, err := r.ra.ReadAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *seekableReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return r.ra.ReadAt(p, off)
}

func (r *seekableReaderAt) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.pos = offset
	case io.SeekCurrent:
		r.pos += offset
	case io.SeekEnd:
		r.pos = r.size + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	return r.pos, nil
}

// parquetArrowReader emits Arrow-IPC DataChunks at parquet row-group granularity.
type parquetArrowReader struct {
	ctx       context.Context
	sources   []fileSource
	schema    *SchemaInfo
	arrowSch  *arrow.Schema
	allocator memory.Allocator

	fileIdx       int
	currentReader *file.Reader
	currentRR     pqarrow.RecordReader
	totalSize     int64
}

// NewParquetArrowReader opens a streaming arrow reader over a list of parquet
// files. `currentSchema` is the lakekeeper-current iceberg schema — files that
// don't match are projected onto it (missing nullable columns are null-padded;
// type mismatches or missing non-nullable columns are errors).
// This is the file-path-based constructor for LocalBackend.
func NewParquetArrowReader(ctx context.Context, files []string, currentSchema *SchemaInfo) (Reader, error) {
	if len(files) == 0 {
		return nil, errors.New("parquet arrow reader: no files")
	}
	sources := make([]fileSource, 0, len(files))
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			// close any already-opened sources
			for _, s := range sources {
				if s.closer != nil {
					s.closer.Close()
				}
			}
			return nil, fmt.Errorf("stat %s: %w", f, err)
		}
		osf, err := os.Open(f)
		if err != nil {
			for _, s := range sources {
				if s.closer != nil {
					s.closer.Close()
				}
			}
			return nil, fmt.Errorf("open %s: %w", f, err)
		}
		sources = append(sources, fileSource{name: f, ra: osf, size: info.Size(), closer: osf})
	}
	return newParquetArrowReaderFromSources(ctx, sources, currentSchema)
}

// newParquetArrowReaderFromSources constructs a parquetArrowReader from
// pre-built fileSource entries. Used by MinIOBackend.OpenStreamingArrowReader
// to pass in minioRangeReaderAt-backed sources.
func newParquetArrowReaderFromSources(ctx context.Context, sources []fileSource, currentSchema *SchemaInfo) (Reader, error) {
	arrowSch, err := schemaInfoToArrow(currentSchema)
	if err != nil {
		// close any sources we own
		for _, s := range sources {
			if s.closer != nil {
				s.closer.Close()
			}
		}
		return nil, fmt.Errorf("convert schema: %w", err)
	}
	var totalSize int64
	for _, s := range sources {
		totalSize += s.size
	}
	return &parquetArrowReader{
		ctx:       ctx,
		sources:   sources,
		schema:    currentSchema,
		arrowSch:  arrowSch,
		allocator: memory.NewGoAllocator(),
		fileIdx:   -1,
		totalSize: totalSize,
	}, nil
}

func (r *parquetArrowReader) Schema() *SchemaInfo      { return r.schema }
func (r *parquetArrowReader) TotalSizeEstimate() int64 { return r.totalSize }

func (r *parquetArrowReader) Close() error {
	if r.currentRR != nil {
		r.currentRR.Release()
		r.currentRR = nil
	}
	if r.currentReader != nil {
		r.currentReader.Close()
		r.currentReader = nil
	}
	// Close any sources that have a closer (os.File-backed). MinIO-backed
	// sources have no closer (their lifetime is bounded by the context).
	for i := range r.sources {
		if r.sources[i].closer != nil {
			r.sources[i].closer.Close()
			r.sources[i].closer = nil
		}
	}
	return nil
}

func (r *parquetArrowReader) ReadChunk() (*DataChunk, error) {
	for {
		if r.currentRR == nil {
			if err := r.openNextFile(); err == io.EOF {
				return nil, io.EOF
			} else if err != nil {
				return nil, err
			}
		}
		if !r.currentRR.Next() {
			// End of current file's batches — release and roll to next file.
			r.currentRR.Release()
			r.currentRR = nil
			r.currentReader.Close()
			r.currentReader = nil
			continue
		}
		rec := r.currentRR.RecordBatch()

		// Project per-file record onto the lakekeeper-current target schema
		// (null-pad missing nullable columns; fail loud on missing non-nullable
		// or type-mismatched columns). This lets the IPC writer be constructed
		// with r.arrowSch — every emitted record matches the target schema by
		// construction, so the IPC stream has a consistent schema across files.
		projected, err := projectRecord(rec, r.arrowSch, r.allocator)
		if err != nil {
			return nil, fmt.Errorf("file %s: project record: %w", r.sources[r.fileIdx].name, err)
		}
		var buf bytes.Buffer
		w := ipc.NewWriter(&buf, ipc.WithSchema(r.arrowSch))
		if err := w.Write(projected); err != nil {
			projected.Release()
			return nil, fmt.Errorf("ipc write: %w", err)
		}
		if err := w.Close(); err != nil {
			projected.Release()
			return nil, fmt.Errorf("ipc close: %w", err)
		}
		rows := projected.NumRows()
		projected.Release()
		// IsLast is intentionally never set by the streaming reader. The gRPC server
		// stream's natural io.EOF (after the final ReadChunk returns it) is the only
		// terminator. Callers MUST handle io.EOF; setting IsLast on the last chunk
		// would be redundant and would couple chunk emission to file-roll lookahead
		// (which pqarrow.RecordReader doesn't support).
		return &DataChunk{
			Data:        buf.Bytes(),
			Format:      "arrow_ipc",
			IsLast:      false,
			RowsInChunk: rows,
		}, nil
	}
}

// projectRecord reshapes `rec` to match the `target` schema:
//   - For each target field: take the column from `rec` if present (by name match);
//     if missing AND the target field is nullable → null-pad column of `rec.NumRows()`;
//     if missing AND the target field is non-nullable → fail (schema-evolution invariant violated).
//   - Type-widening (e.g., int32 → int64) is NOT performed here; if a file's column type differs
//     from the target, fail with a clear message. iceberg-go's projection layer would handle this
//     longer-term, but for v1 we treat type-mismatch as a hard error rather than silently coerce.
//
// Lifetime contract:
//   - `rec.Column(i)` does NOT retain (verified against arrow-go v18 simpleRecord).
//   - For each `cols[i]` we install, we either own a fresh refcount (null-padding via
//     builder.NewArray) or take one explicitly via Retain (source-column path).
//   - `array.NewRecord` retains each column again, so after construction we Release our
//     owned ref. The returned record holds the only outstanding ref we are responsible for,
//     and the source record's refs are unchanged.
//   - arrow.TypeEqual ignores field metadata by default (only opts in via CheckMetadata),
//     so files written with parquet field-id metadata still match a SchemaInfo-derived target.
func projectRecord(rec arrow.RecordBatch, target *arrow.Schema, pool memory.Allocator) (arrow.RecordBatch, error) {
	cols := make([]arrow.Array, target.NumFields())
	sourceFields := rec.Schema()
	rows := rec.NumRows()
	// On error, release any cols we have already taken refs to (avoid leaks).
	releaseTaken := func(upTo int) {
		for j := 0; j < upTo; j++ {
			if cols[j] != nil {
				cols[j].Release()
			}
		}
	}
	for i, tField := range target.Fields() {
		srcIdx := sourceFields.FieldIndices(tField.Name)
		switch len(srcIdx) {
		case 0:
			if !tField.Nullable {
				releaseTaken(i)
				return nil, fmt.Errorf("missing required column %q (current schema requires it; this file omits it)", tField.Name)
			}
			cols[i] = nullColumn(tField.Type, int(rows), pool)
		case 1:
			srcCol := rec.Column(srcIdx[0])
			if !arrow.TypeEqual(srcCol.DataType(), tField.Type) {
				releaseTaken(i)
				return nil, fmt.Errorf("column %q type mismatch: file has %s, current schema is %s",
					tField.Name, srcCol.DataType(), tField.Type)
			}
			srcCol.Retain()
			cols[i] = srcCol
		default:
			releaseTaken(i)
			return nil, fmt.Errorf("ambiguous column %q (%d matches in source)", tField.Name, len(srcIdx))
		}
	}
	out := array.NewRecord(target, cols, rows)
	// array.NewRecord retains each column; release our owned refs so the only
	// outstanding ref on each cols[i] is held by `out`.
	for _, c := range cols {
		c.Release()
	}
	return out, nil
}

// nullColumn returns an arrow.Array of length `n` containing all nulls of `t`.
// Caller owns one reference (release it after handing off to NewRecord).
func nullColumn(t arrow.DataType, n int, pool memory.Allocator) arrow.Array {
	builder := array.NewBuilder(pool, t)
	defer builder.Release()
	for i := 0; i < n; i++ {
		builder.AppendNull()
	}
	return builder.NewArray()
}

func (r *parquetArrowReader) openNextFile() error {
	r.fileIdx++
	if r.fileIdx >= len(r.sources) {
		return io.EOF
	}
	src := r.sources[r.fileIdx]
	// Wrap the io.ReaderAt in a readerAtSeeker so file.NewParquetReader
	// gets the parquet.ReaderAtSeeker it requires (Read + ReadAt + Seek).
	// os.File satisfies this directly, but the wrapper is harmless for
	// os.File too — and required for MinIO range-read sources.
	ras := &seekableReaderAt{ra: src.ra, size: src.size}
	pr, err := file.NewParquetReader(ras)
	if err != nil {
		return fmt.Errorf("parquet open %s: %w", src.name, err)
	}
	// BatchSize is in ROWS. 4096 keeps per-Record memory bounded across
	// schema widths (wide ~4 KiB/row tables produce ~16 MiB Records that
	// fit comfortably under the SDK's 64 MiB gRPC MaxRecvMsgSize cap).
	arrowReader, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: 4 * 1024,
	}, r.allocator)
	if err != nil {
		pr.Close()
		return fmt.Errorf("pqarrow reader %s: %w", src.name, err)
	}
	rr, err := arrowReader.GetRecordReader(r.ctx, nil /* all columns */, nil /* all row groups */)
	if err != nil {
		pr.Close()
		return fmt.Errorf("get record reader %s: %w", src.name, err)
	}
	r.currentReader = pr
	r.currentRR = rr
	return nil
}

// schemaInfoToArrow converts the gateway's SchemaInfo to an arrow.Schema.
// (Helper — uses a minimal mapping for v1; extend as schema types grow.)
func schemaInfoToArrow(s *SchemaInfo) (*arrow.Schema, error) {
	if s == nil {
		return nil, errors.New("nil schema")
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
			return nil, fmt.Errorf("unsupported schema type %q for column %q", c.Type, c.Name)
		}
		fields[i] = arrow.Field{Name: c.Name, Type: t, Nullable: c.Nullable}
	}
	return arrow.NewSchema(fields, nil), nil
}
