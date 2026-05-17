package backend

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// readerAtSeeker combines io.ReaderAt and io.ReadSeeker, matching the
// parquet.ReaderAtSeeker interface required by file.NewParquetReader.
// Both *os.File and *bytes.Reader implement this interface.
type readerAtSeeker interface {
	io.ReadSeeker
	io.ReaderAt
}

// StampParquetStream reads a parquet file from srcReader, rewrites its Arrow
// schema to carry per-column "PARQUET:field_id" metadata from fieldIDMap
// (column-name → iceberg field_id), and writes the result to dstWriter.
//
// srcReader must implement both io.ReadSeeker and io.ReaderAt (i.e. it must
// be a readerAtSeeker). *os.File satisfies this.
//
// The caller is responsible for any seeking/reset of srcReader before calling.
// Streams record batches via WriteBuffered so memory is bounded by BatchSize
// (64 Ki rows) × column width. The output is flushed via the writer's Close()
// after all records are written.
func StampParquetStream(ctx context.Context, srcReader readerAtSeeker, dstWriter io.Writer, fieldIDMap map[string]int32) error {
	pf, err := file.NewParquetReader(srcReader)
	if err != nil {
		return fmt.Errorf("open source parquet: %w", err)
	}
	defer pf.Close()

	alloc := memory.DefaultAllocator
	arrReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
		BatchSize: 64 * 1024,
	}, alloc)
	if err != nil {
		return fmt.Errorf("create pqarrow reader: %w", err)
	}

	origSchema, err := arrReader.Schema()
	if err != nil {
		return fmt.Errorf("read source schema: %w", err)
	}
	newSchema := InjectFieldIDsIntoArrowSchema(origSchema, fieldIDMap)

	writerProps := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(1024 * 1024),
	)
	arrowWriterProps := pqarrow.DefaultWriterProps()
	fw, err := pqarrow.NewFileWriter(newSchema, dstWriter, writerProps, arrowWriterProps)
	if err != nil {
		return fmt.Errorf("create pqarrow writer: %w", err)
	}

	rdr, err := arrReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		_ = fw.Close()
		return fmt.Errorf("create record reader: %w", err)
	}
	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.RecordBatch()

		// Re-tag the batch with the field-id-stamped schema; arrow zero-copies
		// the underlying columns so this is cheap.
		cols := rec.Columns()
		for _, col := range cols {
			col.Retain()
		}
		newRec := array.NewRecordBatch(newSchema, cols, rec.NumRows())

		writeErr := fw.WriteBuffered(newRec)
		newRec.Release()
		// Release matches the Retain above; NewRecordBatch retains internally
		// when given live arrays, so the original array is freed only when the
		// reader advances.
		for _, col := range cols {
			col.Release()
		}
		if writeErr != nil {
			_ = fw.Close()
			return fmt.Errorf("write record batch: %w", writeErr)
		}
	}
	if err := rdr.Err(); err != nil {
		_ = fw.Close()
		return fmt.Errorf("read record batches: %w", err)
	}
	if err := fw.Close(); err != nil {
		return fmt.Errorf("close pqarrow writer: %w", err)
	}
	return nil
}

// InjectFieldIDsIntoArrowSchema returns a copy of originalSchema with each
// field carrying a "PARQUET:field_id" metadata entry sourced from fieldIDMap
// (column-name → iceberg field_id). When a column name is missing from the
// map a fallback (1-based index) is used; callers are expected to have
// validated coverage upstream (see validateSchemaMatchesFieldIDs in
// pkg/datagateway/parquet_patcher.go).
//
// All other field metadata (other than a pre-existing PARQUET:field_id) is
// preserved. Schema-level metadata is dropped — Arrow's NewSchema callers
// in the codebase treat the schema as carrying only field-level metadata.
//
// This helper is used by:
//   - pkg/datagateway/parquet_patcher.go (DG buffering's external-file patch)
func InjectFieldIDsIntoArrowSchema(originalSchema *arrow.Schema, fieldIDMap map[string]int32) *arrow.Schema {
	fields := make([]arrow.Field, originalSchema.NumFields())

	for i := 0; i < originalSchema.NumFields(); i++ {
		origField := originalSchema.Field(i)
		fieldID, exists := fieldIDMap[origField.Name]
		if !exists {
			// This shouldn't happen due to validation, but use index as fallback
			fieldID = int32(i + 1)
		}

		// The PARQUET:field_id key is read by Arrow's parquet writer and
		// surfaces as the per-column field_id annotation in the Parquet
		// SchemaElement.
		metadataKeys := []string{"PARQUET:field_id"}
		metadataValues := []string{fmt.Sprintf("%d", fieldID)}

		// Preserve existing metadata if any (skip the field_id key — we own it).
		if origField.Metadata.Len() > 0 {
			for j := 0; j < origField.Metadata.Len(); j++ {
				key := origField.Metadata.Keys()[j]
				if key != "PARQUET:field_id" {
					metadataKeys = append(metadataKeys, key)
					metadataValues = append(metadataValues, origField.Metadata.Values()[j])
				}
			}
		}

		newMetadata := arrow.NewMetadata(metadataKeys, metadataValues)

		fields[i] = arrow.Field{
			Name:     origField.Name,
			Type:     origField.Type,
			Nullable: origField.Nullable,
			Metadata: newMetadata,
		}
	}

	return arrow.NewSchema(fields, nil)
}
