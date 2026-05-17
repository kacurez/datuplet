package format

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// CSVAdapter converts between CSV and Arrow RecordBatches.
type CSVAdapter struct {
	allocator    memory.Allocator
	parseOpts    *ParseOptions
	serializeOpts *SerializeOptions
}

// NewCSVAdapter creates a new CSV adapter.
// If allocator is nil, uses the default Go allocator.
// If options are nil, uses defaults.
func NewCSVAdapter(allocator memory.Allocator, parseOpts *ParseOptions) *CSVAdapter {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	if parseOpts == nil {
		parseOpts = DefaultParseOptions()
	}
	return &CSVAdapter{
		allocator:     allocator,
		parseOpts:     parseOpts,
		serializeOpts: DefaultSerializeOptions(),
	}
}

// Format returns FormatCSV.
func (a *CSVAdapter) Format() DataFormat {
	return FormatCSV
}

// Parse converts CSV bytes to an Arrow RecordBatch.
func (a *CSVAdapter) Parse(data []byte, s *schema.Schema) (arrow.Record, *schema.Schema, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1 // Allow variable fields
	if a.parseOpts.Delimiter != 0 {
		reader.Comma = a.parseOpts.Delimiter
	}

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, nil, fmt.Errorf("CSV has no data")
	}

	var headers []string
	var dataRows [][]string

	if a.parseOpts.HasHeader {
		headers = records[0]
		dataRows = records[1:]
	} else {
		// Generate column names: col0, col1, ...
		if len(records) > 0 {
			headers = make([]string, len(records[0]))
			for i := range headers {
				headers[i] = fmt.Sprintf("col%d", i)
			}
		}
		dataRows = records
	}

	if len(headers) == 0 {
		return nil, nil, fmt.Errorf("CSV has no columns")
	}

	// Infer schema if not provided
	if s == nil {
		inferConfig := a.parseOpts.InferenceConfig
		s, err = schema.InferSchemaWithConfig(headers, dataRows, inferConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to infer schema: %w", err)
		}
	} else {
		// Validate headers match schema column count
		if len(headers) != s.NumColumns() {
			return nil, nil, fmt.Errorf("CSV has %d columns, schema has %d", len(headers), s.NumColumns())
		}
	}

	// Build the RecordBatch
	record, err := a.buildRecord(s, dataRows)
	if err != nil {
		return nil, nil, err
	}

	return record, s, nil
}

// buildRecord creates an Arrow RecordBatch from string rows.
func (a *CSVAdapter) buildRecord(s *schema.Schema, rows [][]string) (arrow.Record, error) {
	builder := array.NewRecordBuilder(a.allocator, s.ArrowSchema())
	defer builder.Release()

	inferrer := schema.NewTypeInferrer(a.parseOpts.InferenceConfig)

	for rowIdx, row := range rows {
		for colIdx := 0; colIdx < s.NumColumns() && colIdx < len(row); colIdx++ {
			value := row[colIdx]
			col := s.Column(colIdx)

			if err := appendCSVValue(builder.Field(colIdx), col.Type, value, inferrer); err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", rowIdx, col.Name, err)
			}
		}
		// Handle missing columns at end of row
		for colIdx := len(row); colIdx < s.NumColumns(); colIdx++ {
			builder.Field(colIdx).AppendNull()
		}
	}

	return builder.NewRecord(), nil
}

// appendCSVValue appends a CSV string value to an Arrow builder.
func appendCSVValue(builder array.Builder, dataType schema.DataType, value string, inferrer *schema.TypeInferrer) error {
	// Check for null
	if inferrer.IsNull(value) {
		builder.AppendNull()
		return nil
	}

	value = strings.TrimSpace(value)

	switch dataType {
	case schema.TypeInt64:
		return appendInt64FromString(builder.(*array.Int64Builder), value)
	case schema.TypeInt32:
		return appendInt32FromString(builder.(*array.Int32Builder), value)
	case schema.TypeFloat64:
		return appendFloat64FromString(builder.(*array.Float64Builder), value)
	case schema.TypeFloat32:
		return appendFloat32FromString(builder.(*array.Float32Builder), value)
	case schema.TypeString:
		builder.(*array.StringBuilder).Append(value)
		return nil
	case schema.TypeBool:
		return appendBoolFromString(builder.(*array.BooleanBuilder), value)
	case schema.TypeTimestamp:
		return appendTimestampFromString(builder.(*array.TimestampBuilder), value)
	case schema.TypeDate:
		return appendDateFromString(builder.(*array.Date32Builder), value)
	case schema.TypeBinary:
		builder.(*array.BinaryBuilder).Append([]byte(value))
		return nil
	default:
		return fmt.Errorf("unsupported type: %v", dataType)
	}
}

// Serialize converts an Arrow RecordBatch to CSV bytes.
func (a *CSVAdapter) Serialize(record arrow.Record) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if a.serializeOpts.Delimiter != 0 {
		writer.Comma = a.serializeOpts.Delimiter
	}

	arrowSchema := record.Schema()
	numCols := int(record.NumCols())
	numRows := int(record.NumRows())

	// Write header
	if a.serializeOpts.IncludeHeader {
		headers := make([]string, numCols)
		for i := 0; i < numCols; i++ {
			headers[i] = arrowSchema.Field(i).Name
		}
		if err := writer.Write(headers); err != nil {
			return nil, fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	// Write data rows
	row := make([]string, numCols)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		for colIdx := 0; colIdx < numCols; colIdx++ {
			col := record.Column(colIdx)
			if col.IsNull(rowIdx) {
				row[colIdx] = a.serializeOpts.NullString
			} else {
				row[colIdx] = formatColumnValue(col, rowIdx)
			}
		}
		if err := writer.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write CSV row %d: %w", rowIdx, err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("CSV write error: %w", err)
	}

	return buf.Bytes(), nil
}

// formatColumnValue formats an Arrow column value as a string.
func formatColumnValue(col arrow.Array, idx int) string {
	switch arr := col.(type) {
	case *array.Int64:
		return fmt.Sprintf("%d", arr.Value(idx))
	case *array.Int32:
		return fmt.Sprintf("%d", arr.Value(idx))
	case *array.Int16:
		return fmt.Sprintf("%d", arr.Value(idx))
	case *array.Int8:
		return fmt.Sprintf("%d", arr.Value(idx))
	case *array.Uint64:
		return fmt.Sprintf("%d", arr.Value(idx))
	case *array.Uint32:
		return fmt.Sprintf("%d", arr.Value(idx))
	case *array.Float64:
		return fmt.Sprintf("%g", arr.Value(idx))
	case *array.Float32:
		return fmt.Sprintf("%g", arr.Value(idx))
	case *array.String:
		return arr.Value(idx)
	case *array.Boolean:
		if arr.Value(idx) {
			return "true"
		}
		return "false"
	case *array.Timestamp:
		ts := arr.Value(idx)
		// Assume microseconds
		return ts.ToTime(arrow.Microsecond).Format("2006-01-02T15:04:05.000000")
	case *array.Date32:
		days := arr.Value(idx)
		return days.ToTime().Format("2006-01-02")
	case *array.Binary:
		return string(arr.Value(idx))
	default:
		return fmt.Sprintf("%v", col)
	}
}
