package format

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// ParquetAdapter reads Parquet data to Arrow RecordBatches.
// Note: This adapter only supports Parse (read), not Serialize (write).
// For writing Parquet, use the buffer.ParquetFlusher.
type ParquetAdapter struct {
	allocator memory.Allocator
	parseOpts *ParseOptions
}

// NewParquetAdapter creates a new Parquet adapter.
func NewParquetAdapter(allocator memory.Allocator, parseOpts *ParseOptions) *ParquetAdapter {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	if parseOpts == nil {
		parseOpts = DefaultParseOptions()
	}
	return &ParquetAdapter{
		allocator: allocator,
		parseOpts: parseOpts,
	}
}

// Format returns FormatParquet.
func (a *ParquetAdapter) Format() DataFormat {
	return FormatParquet
}

// Parse reads Parquet bytes and returns an Arrow RecordBatch.
// The schema parameter is ignored - Parquet files contain their own schema.
func (a *ParquetAdapter) Parse(data []byte, _ *schema.Schema) (arrow.Record, *schema.Schema, error) {
	reader := bytes.NewReader(data)

	// Open Parquet file reader
	pqReader, err := file.NewParquetReader(reader, file.WithReadProps(parquet.NewReaderProperties(a.allocator)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open Parquet reader: %w", err)
	}
	defer pqReader.Close()

	// Create Arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, a.allocator)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}

	// Read as table
	ctx := context.Background()
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read Parquet table: %w", err)
	}
	defer table.Release()

	// Convert table to single record batch
	record, err := tableToRecord(table, a.allocator)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert table to record: %w", err)
	}

	// Convert Arrow schema to our schema
	s, err := schema.NewSchemaFromArrow(record.Schema())
	if err != nil {
		record.Release()
		return nil, nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	return record, s, nil
}

// Serialize is not supported for Parquet (use buffer.ParquetFlusher instead).
func (a *ParquetAdapter) Serialize(record arrow.Record) ([]byte, error) {
	return nil, fmt.Errorf("Parquet serialization not supported via adapter; use buffer.ParquetFlusher")
}

// tableToRecord converts an Arrow Table to a single RecordBatch.
func tableToRecord(table arrow.Table, allocator memory.Allocator) (arrow.Record, error) {
	if table.NumRows() == 0 {
		// Create empty record with schema
		return array.NewRecord(table.Schema(), nil, 0), nil
	}

	// Get table reader to iterate through chunks
	reader := array.NewTableReader(table, table.NumRows())
	defer reader.Release()

	// Collect all records
	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}

	if len(records) == 0 {
		return array.NewRecord(table.Schema(), nil, 0), nil
	}

	// If single record, return it directly
	if len(records) == 1 {
		return records[0], nil
	}

	// Concatenate multiple records into one
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	return concatenateRecords(records, allocator)
}

// concatenateRecords combines multiple records into a single record.
func concatenateRecords(records []arrow.Record, allocator memory.Allocator) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to concatenate")
	}

	arrowSchema := records[0].Schema()
	numCols := int(records[0].NumCols())

	// Build arrays for each column
	columns := make([]arrow.Array, numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		// Collect all chunks for this column
		var chunks []arrow.Array
		for _, rec := range records {
			chunks = append(chunks, rec.Column(colIdx))
		}

		// Concatenate column chunks
		concatenated, err := array.Concatenate(chunks, allocator)
		if err != nil {
			// Release any columns we've already created
			for j := 0; j < colIdx; j++ {
				columns[j].Release()
			}
			return nil, fmt.Errorf("failed to concatenate column %d: %w", colIdx, err)
		}
		columns[colIdx] = concatenated
	}

	// Calculate total rows
	var totalRows int64
	for _, rec := range records {
		totalRows += rec.NumRows()
	}

	// Create combined record
	return array.NewRecord(arrowSchema, columns, totalRows), nil
}
