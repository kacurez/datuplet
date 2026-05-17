package format

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// JSONAdapter converts between JSON arrays and Arrow RecordBatches.
// Expects input in the form: [{"col1": val1, ...}, {"col1": val2, ...}]
type JSONAdapter struct {
	allocator     memory.Allocator
	parseOpts     *ParseOptions
	serializeOpts *SerializeOptions
}

// NewJSONAdapter creates a new JSON adapter.
func NewJSONAdapter(allocator memory.Allocator, parseOpts *ParseOptions) *JSONAdapter {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	if parseOpts == nil {
		parseOpts = DefaultParseOptions()
	}
	serializeOpts := DefaultSerializeOptions()
	serializeOpts.NullString = "null"
	return &JSONAdapter{
		allocator:     allocator,
		parseOpts:     parseOpts,
		serializeOpts: serializeOpts,
	}
}

// Format returns FormatJSON.
func (a *JSONAdapter) Format() DataFormat {
	return FormatJSON
}

// Parse converts JSON array bytes to an Arrow RecordBatch.
func (a *JSONAdapter) Parse(data []byte, s *schema.Schema) (arrow.Record, *schema.Schema, error) {
	// Parse JSON array
	var objects []map[string]any
	if err := json.Unmarshal(data, &objects); err != nil {
		return nil, nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	return a.parseObjects(objects, s)
}

// parseObjects converts a slice of JSON objects to an Arrow RecordBatch.
func (a *JSONAdapter) parseObjects(objects []map[string]any, s *schema.Schema) (arrow.Record, *schema.Schema, error) {
	if len(objects) == 0 {
		return nil, nil, fmt.Errorf("JSON array is empty")
	}

	// Infer schema if not provided
	var err error
	if s == nil {
		s, err = schema.InferSchemaFromJSONWithConfig(objects, a.parseOpts.InferenceConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to infer schema: %w", err)
		}
	}

	// Build the RecordBatch
	record, err := a.buildRecord(s, objects)
	if err != nil {
		return nil, nil, err
	}

	return record, s, nil
}

// buildRecord creates an Arrow RecordBatch from JSON objects.
func (a *JSONAdapter) buildRecord(s *schema.Schema, objects []map[string]any) (arrow.Record, error) {
	builder := array.NewRecordBuilder(a.allocator, s.ArrowSchema())
	defer builder.Release()

	for rowIdx, obj := range objects {
		for colIdx := 0; colIdx < s.NumColumns(); colIdx++ {
			col := s.Column(colIdx)
			value, exists := obj[col.Name]
			if !exists || value == nil {
				builder.Field(colIdx).AppendNull()
				continue
			}

			if err := appendValueFromInterface(builder.Field(colIdx), value); err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", rowIdx, col.Name, err)
			}
		}
	}

	return builder.NewRecord(), nil
}

// Serialize converts an Arrow RecordBatch to JSON array bytes.
func (a *JSONAdapter) Serialize(record arrow.Record) ([]byte, error) {
	arrowSchema := record.Schema()
	numCols := int(record.NumCols())
	numRows := int(record.NumRows())

	objects := make([]map[string]any, numRows)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		obj := make(map[string]any, numCols)
		for colIdx := 0; colIdx < numCols; colIdx++ {
			col := record.Column(colIdx)
			fieldName := arrowSchema.Field(colIdx).Name
			if col.IsNull(rowIdx) {
				obj[fieldName] = nil
			} else {
				obj[fieldName] = extractColumnValue(col, rowIdx)
			}
		}
		objects[rowIdx] = obj
	}

	if a.serializeOpts.Pretty {
		return json.MarshalIndent(objects, "", "  ")
	}
	return json.Marshal(objects)
}

// extractColumnValue extracts a typed value from an Arrow column.
func extractColumnValue(col arrow.Array, idx int) any {
	switch arr := col.(type) {
	case *array.Int64:
		return arr.Value(idx)
	case *array.Int32:
		return arr.Value(idx)
	case *array.Int16:
		return int32(arr.Value(idx))
	case *array.Int8:
		return int32(arr.Value(idx))
	case *array.Uint64:
		return arr.Value(idx)
	case *array.Uint32:
		return arr.Value(idx)
	case *array.Float64:
		return arr.Value(idx)
	case *array.Float32:
		return float64(arr.Value(idx))
	case *array.String:
		return arr.Value(idx)
	case *array.Boolean:
		return arr.Value(idx)
	case *array.Timestamp:
		ts := arr.Value(idx)
		return ts.ToTime(arrow.Microsecond).Format("2006-01-02T15:04:05.000000Z")
	case *array.Date32:
		days := arr.Value(idx)
		return days.ToTime().Format("2006-01-02")
	case *array.Binary:
		return string(arr.Value(idx))
	default:
		return nil
	}
}

// JSONLAdapter converts between JSON Lines and Arrow RecordBatches.
// Expects input in the form: {"col1": val1, ...}\n{"col1": val2, ...}\n
type JSONLAdapter struct {
	allocator     memory.Allocator
	parseOpts     *ParseOptions
	serializeOpts *SerializeOptions
}

// NewJSONLAdapter creates a new JSON Lines adapter.
func NewJSONLAdapter(allocator memory.Allocator, parseOpts *ParseOptions) *JSONLAdapter {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	if parseOpts == nil {
		parseOpts = DefaultParseOptions()
	}
	serializeOpts := DefaultSerializeOptions()
	serializeOpts.NullString = "null"
	return &JSONLAdapter{
		allocator:     allocator,
		parseOpts:     parseOpts,
		serializeOpts: serializeOpts,
	}
}

// Format returns FormatJSONL.
func (a *JSONLAdapter) Format() DataFormat {
	return FormatJSONL
}

// Parse converts JSON Lines bytes to an Arrow RecordBatch.
func (a *JSONLAdapter) Parse(data []byte, s *schema.Schema) (arrow.Record, *schema.Schema, error) {
	var objects []map[string]any

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			return nil, nil, fmt.Errorf("failed to parse JSON at line %d: %w", lineNum, err)
		}
		objects = append(objects, obj)
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to read JSON Lines: %w", err)
	}

	if len(objects) == 0 {
		return nil, nil, fmt.Errorf("JSON Lines data is empty")
	}

	// Infer schema if not provided
	var err error
	if s == nil {
		s, err = schema.InferSchemaFromJSONWithConfig(objects, a.parseOpts.InferenceConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to infer schema: %w", err)
		}
	}

	// Build the RecordBatch
	record, err := a.buildRecord(s, objects)
	if err != nil {
		return nil, nil, err
	}

	return record, s, nil
}

// buildRecord creates an Arrow RecordBatch from JSON objects.
func (a *JSONLAdapter) buildRecord(s *schema.Schema, objects []map[string]any) (arrow.Record, error) {
	builder := array.NewRecordBuilder(a.allocator, s.ArrowSchema())
	defer builder.Release()

	for rowIdx, obj := range objects {
		for colIdx := 0; colIdx < s.NumColumns(); colIdx++ {
			col := s.Column(colIdx)
			value, exists := obj[col.Name]
			if !exists || value == nil {
				builder.Field(colIdx).AppendNull()
				continue
			}

			if err := appendValueFromInterface(builder.Field(colIdx), value); err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", rowIdx, col.Name, err)
			}
		}
	}

	return builder.NewRecord(), nil
}

// Serialize converts an Arrow RecordBatch to JSON Lines bytes.
func (a *JSONLAdapter) Serialize(record arrow.Record) ([]byte, error) {
	arrowSchema := record.Schema()
	numCols := int(record.NumCols())
	numRows := int(record.NumRows())

	var buf bytes.Buffer

	// Get sorted field names for consistent output
	fieldNames := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		fieldNames[i] = arrowSchema.Field(i).Name
	}

	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		obj := make(map[string]any, numCols)
		for colIdx := 0; colIdx < numCols; colIdx++ {
			col := record.Column(colIdx)
			fieldName := fieldNames[colIdx]
			if col.IsNull(rowIdx) {
				obj[fieldName] = nil
			} else {
				obj[fieldName] = extractColumnValue(col, rowIdx)
			}
		}

		line, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize row %d: %w", rowIdx, err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// InferJSONFieldOrder returns field names in a deterministic order from JSON objects.
// Used internally to ensure consistent column ordering.
func InferJSONFieldOrder(objects []map[string]any) []string {
	fieldSet := make(map[string]bool)
	for _, obj := range objects {
		for key := range obj {
			fieldSet[key] = true
		}
	}

	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}
