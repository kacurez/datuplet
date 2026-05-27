package format

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// Compile-time assertion: JSONLAdapter is a streaming-capable adapter.
var _ StreamingAdapter = (*JSONLAdapter)(nil)

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
	// Fast path: schema known. Stream-decode lines directly into the
	// arrow RecordBuilder — no []map[string]any intermediate. Previous
	// run 07a2ddec showed JSONLAdapter.buildRecord holding ~706 MB
	// of [N]map[string]any across the in-flight chunk; with the
	// streaming-decode pattern only ONE map[string]any is alive at any
	// moment (GC'd after each row append).
	//
	// The hot path in production hits this branch: the first write-chunk
	// per (writer, table) infers the schema (slow path below), the
	// gateway caches it on writerState, and every subsequent chunk for
	// that writer passes the cached schema in.
	if s != nil {
		return a.parseWithKnownSchema(bytes.NewReader(data), s)
	}

	// Slow path: schema unknown. Buffer all objects, infer schema (the
	// inference may widen types across rows, e.g. int -> double if any
	// row has a float), then build. Only the FIRST chunk of a writer's
	// lifetime takes this path.
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

	// Infer schema from the buffered objects.
	inferred, err := schema.InferSchemaFromJSONWithConfig(objects, a.parseOpts.InferenceConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to infer schema: %w", err)
	}

	// Build the RecordBatch.
	record, err := a.buildRecord(inferred, objects)
	if err != nil {
		return nil, nil, err
	}

	return record, inferred, nil
}

// ParseReader implements StreamingAdapter for JSON Lines.
//
// Known-schema (the hot path: every chunk after the first for a writer):
// streams line-by-line straight off r via bufio.Scanner into the
// RecordBuilder — no io.ReadAll of the body, one transient map[string]any
// alive at a time.
//
// Unknown-schema (first chunk only): inference needs every row up front, so
// we fall back to buffering the reader and delegating to Parse. This keeps
// the inference semantics identical; the streaming win applies to the 99%
// of chunks that arrive once the schema is cached on the writer.
func (a *JSONLAdapter) ParseReader(r io.Reader, s *schema.Schema) (arrow.Record, *schema.Schema, error) {
	if s != nil {
		return a.parseWithKnownSchema(r, s)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read JSON Lines: %w", err)
	}
	return a.Parse(data, nil)
}

// parseWithKnownSchema is the streaming-direct JSONL decoder used when
// the gateway already has the writer's schema cached. It avoids
// building a []map[string]any of all rows: each line allocates a
// single map[string]any, the values are appended into the
// RecordBuilder, and the map is dropped (eligible for GC) before the
// next line is decoded. Peak live heap is therefore one map per
// in-flight chunk, not (rows-in-chunk) maps.
func (a *JSONLAdapter) parseWithKnownSchema(r io.Reader, s *schema.Schema) (arrow.Record, *schema.Schema, error) {
	builder := array.NewRecordBuilder(a.allocator, s.ArrowSchema())
	defer builder.Release()

	scanner := bufio.NewScanner(r)
	// Default bufio.Scanner line buffer is 64 KiB. Very wide JSONL rows
	// (long strings, many columns) can exceed that; bump the cap to
	// 16 MiB so we don't reject legitimate inputs. The initial buffer
	// stays small — bufio grows on demand within the cap.
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		// One transient map per line. Without buffering across rows the
		// allocator can reuse the same memory arena for each — Go's
		// runtime hits this pattern hard but cheaply.
		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			return nil, nil, fmt.Errorf("failed to parse JSON at line %d: %w", lineNum, err)
		}
		for colIdx := 0; colIdx < s.NumColumns(); colIdx++ {
			col := s.Column(colIdx)
			value, exists := obj[col.Name]
			if !exists || value == nil {
				builder.Field(colIdx).AppendNull()
				continue
			}
			if err := appendValueFromInterface(builder.Field(colIdx), value); err != nil {
				return nil, nil, fmt.Errorf("line %d, column %q: %w", lineNum, col.Name, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to read JSON Lines: %w", err)
	}
	return builder.NewRecord(), s, nil
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
