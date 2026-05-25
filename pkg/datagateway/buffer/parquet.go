package buffer

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// Compression represents Parquet compression codec.
type Compression int

const (
	CompressionNone Compression = iota
	CompressionSnappy
	CompressionGzip
	CompressionZstd
	CompressionLz4
)

// String returns the compression name.
func (c Compression) String() string {
	switch c {
	case CompressionSnappy:
		return "snappy"
	case CompressionGzip:
		return "gzip"
	case CompressionZstd:
		return "zstd"
	case CompressionLz4:
		return "lz4"
	default:
		return "none"
	}
}

// toParquetCompression converts to parquet compression codec.
func (c Compression) toParquetCompression() compress.Compression {
	switch c {
	case CompressionSnappy:
		return compress.Codecs.Snappy
	case CompressionGzip:
		return compress.Codecs.Gzip
	case CompressionZstd:
		return compress.Codecs.Zstd
	case CompressionLz4:
		return compress.Codecs.Lz4
	default:
		return compress.Codecs.Uncompressed
	}
}


// ParquetWriterConfig configures the Parquet writer.
type ParquetWriterConfig struct {
	// Compression codec to use. Default: Snappy
	Compression Compression

	// RowGroupSize is the target size for row groups in bytes.
	// Default: 128MB
	RowGroupSize int64

	// DictionaryEnabled enables dictionary encoding.
	// Default: true
	DictionaryEnabled bool

	// WriteStatistics enables writing column statistics.
	// Default: true
	WriteStatistics bool
}

// DefaultParquetWriterConfig returns the default Parquet writer configuration.
func DefaultParquetWriterConfig() *ParquetWriterConfig {
	return &ParquetWriterConfig{
		Compression:       CompressionSnappy,
		RowGroupSize:      128 * 1024 * 1024, // 128MB
		DictionaryEnabled: true,
		WriteStatistics:   true,
	}
}

// StreamingParquetWriter provides streaming Parquet writing capabilities.
// It keeps a file open and allows appending row groups incrementally.
type StreamingParquetWriter struct {
	writer       *pqarrow.FileWriter
	file         io.WriteCloser
	counter      *countingWriter
	schema       *arrow.Schema // caller's schema (carries field-id metadata)
	writeSchema  *arrow.Schema // schema stamped on the parquet (field-ids stripped; required for txn.AddFiles)
	config       *ParquetWriterConfig
	allocator    memory.Allocator

	// Statistics
	rowGroups  int
	totalRows  int64
	totalBytes int64
	closed     bool
}

// NewStreamingParquetWriter creates a streaming Parquet writer.
func NewStreamingParquetWriter(
	path string,
	schema *arrow.Schema,
	config *ParquetWriterConfig,
	allocator memory.Allocator,
	factory WriterFactory,
) (*StreamingParquetWriter, error) {
	if config == nil {
		config = DefaultParquetWriterConfig()
	}
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	if factory == nil {
		factory = &LocalWriterFactory{}
	}

	// Create output file
	file, err := factory.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", path, err)
	}

	// Wrap with counting writer
	counter := newCountingWriter(file)

	// Create Parquet writer properties.
	//
	// WithMaxRowGroupLength = 1M rows is a defensive cap. The primary
	// row-group size control is BufferManager's flushRowGroup() boundary
	// (driven by config.BufferSize, default 16 MiB). pqarrow's open row
	// group is bounded by what we feed before NewBufferedRowGroup(), so
	// memory stays ~BufferSize. The cap here only kicks in if a single
	// record carries >1M rows (e.g., a misbehaving partition router),
	// preventing unbounded pqarrow row-group buffer growth in that case.
	// Matches the convention in pkg/datagateway/backend/parquet_stamp.go.
	// WithBatchSize(8192) raises pqarrow's per-batch internal write
	// granularity from the default 1024 rows. Arrow record batches
	// landing in WriteRowGroup are typically in the 1k-100k row range
	// (driven by config.BufferSize and the incoming Arrow IPC batch
	// shape); the default 1024 forces unnecessary per-1k-row encoder
	// bookkeeping. 8192 cuts that overhead by 8x while staying well
	// under typical batch sizes. Net effect on the 5M-row write
	// benchmark: ~5-10% reduction in pqarrow.Write + s2.encode CPU.
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(config.Compression.toParquetCompression()),
		parquet.WithDictionaryDefault(config.DictionaryEnabled),
		parquet.WithStats(config.WriteStatistics),
		parquet.WithMaxRowGroupLength(1024*1024),
		parquet.WithBatchSize(8192),
	)

	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithAllocator(allocator),
		pqarrow.WithStoreSchema(),
	)

	// Strip Iceberg `PARQUET:field_id` metadata before the parquet writer
	// translates it into real parquet field-ids. iceberg-go's `txn.AddFiles`
	// rejects parquet that already carries field-ids ("add-files only supports
	// the addition of files without field_ids"). With name mappings stamped
	// on the table at first CreateTable, iceberg-go matches columns by name
	// on AddFiles — so the field-ids we'd otherwise embed are redundant AND
	// blocking. Future work: when moving to OverwriteTable / Append's full
	// transactional path (which DOES support field-id'd parquet), this strip
	// can come back. For now, the buffer's inputs always go
	// through `txn.AddFiles`.
	writeSchema := stripFieldIDMetadata(schema)

	// Create the Parquet writer
	writer, err := pqarrow.NewFileWriter(writeSchema, counter, writerProps, arrowProps)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	return &StreamingParquetWriter{
		writer:      writer,
		file:        file,
		counter:     counter,
		schema:      schema,
		writeSchema: writeSchema,
		config:      config,
		allocator:   allocator,
	}, nil
}

// WriteRowGroup writes a batch of records as a single row group.
func (w *StreamingParquetWriter) WriteRowGroup(records []arrow.Record) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	if len(records) == 0 {
		return nil
	}

	// Start a new buffered row group if this isn't the first write
	if w.rowGroups > 0 {
		w.writer.NewBufferedRowGroup()
	}

	// Write all records to the current row group. Each record's schema
	// must match the writer's schema exactly — including metadata. Slice
	// 10b's writer-schema strip removed `PARQUET:field_id`, so we rewrap
	// each record with the stripped schema before passing it to pqarrow.
	// Rewrapping is cheap (no data copy; just a schema swap) and
	// preserves the underlying columns.
	writerSchema := w.writeSchema
	for _, record := range records {
		write := record
		if !record.Schema().Equal(writerSchema) {
			cols := make([]arrow.Array, record.NumCols())
			for i := 0; i < int(record.NumCols()); i++ {
				cols[i] = record.Column(i)
			}
			write = array.NewRecord(writerSchema, cols, record.NumRows())
			defer write.Release()
		}
		if err := w.writer.WriteBuffered(write); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
		w.totalRows += record.NumRows()
	}

	w.rowGroups++
	w.totalBytes = w.counter.BytesWritten()

	return nil
}

// Schema returns the Arrow schema.
func (w *StreamingParquetWriter) Schema() *arrow.Schema {
	return w.schema
}

// RowGroups returns the number of row groups written.
func (w *StreamingParquetWriter) RowGroups() int {
	return w.rowGroups
}

// TotalRows returns the total number of rows written.
func (w *StreamingParquetWriter) TotalRows() int64 {
	return w.totalRows
}

// BytesWritten returns the current bytes written (approximate until closed).
func (w *StreamingParquetWriter) BytesWritten() int64 {
	return w.counter.BytesWritten()
}

// Close closes the writer, writing the Parquet footer.
func (w *StreamingParquetWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Close the Parquet writer (writes footer)
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	w.totalBytes = w.counter.BytesWritten()

	// Close the underlying file
	return w.file.Close()
}

// Stats returns statistics about the written data.
func (w *StreamingParquetWriter) Stats() *FlushStats {
	return &FlushStats{
		RowsWritten:  w.totalRows,
		BytesWritten: w.totalBytes,
		RowGroups:    w.rowGroups,
	}
}

// stripFieldIDMetadata returns a copy of `s` whose fields no longer
// carry the `PARQUET:field_id` metadata key. The rest of the field
// metadata is preserved.
//
// Why: iceberg-go's `Transaction.AddFiles` rejects parquet that already
// carries iceberg field-ids ("add-files only supports the addition of
// files without field_ids", `pkg/lib/iceberg-go@v0.5.0/table/arrow_utils.go:1230`).
// DG's writer-side schema stamps field_id metadata so the buffer
// produces "real" iceberg-compatible parquet directly, but that
// pre-stamping is incompatible with the AddFiles commit path.
// Stripping at parquet-write time (rather than at schema-construction
// time) keeps the field-ids available everywhere else (preview / read
// paths, in-memory schema introspection).
func stripFieldIDMetadata(s *arrow.Schema) *arrow.Schema {
	if s == nil {
		return nil
	}
	fields := s.Fields()
	out := make([]arrow.Field, len(fields))
	for i, f := range fields {
		// Walk the metadata, drop the PARQUET:field_id key. Allocate a
		// fresh map so we don't mutate the caller's schema.
		md := f.Metadata
		keys := md.Keys()
		values := md.Values()
		newKeys := make([]string, 0, len(keys))
		newVals := make([]string, 0, len(values))
		for j, k := range keys {
			if k == "PARQUET:field_id" {
				continue
			}
			newKeys = append(newKeys, k)
			newVals = append(newVals, values[j])
		}
		out[i] = arrow.Field{
			Name:     f.Name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: arrow.NewMetadata(newKeys, newVals),
		}
	}
	return arrow.NewSchema(out, nil)
}

