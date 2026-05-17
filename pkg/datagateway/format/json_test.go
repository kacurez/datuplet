package format

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestJSONAdapterFormat(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)
	if adapter.Format() != FormatJSON {
		t.Errorf("Format() = %v, want FormatJSON", adapter.Format())
	}
}

func TestJSONAdapterParseSimple(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	jsonData := []byte(`[{"id": 1, "name": "Widget", "price": 9.99}, {"id": 2, "name": "Gadget", "price": 19.99}]`)
	record, s, err := adapter.Parse(jsonData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Check schema - column order may vary, so check by name
	if s.NumColumns() != 3 {
		t.Errorf("Schema has %d columns, want 3", s.NumColumns())
	}

	idCol := s.ColumnByName("id")
	if idCol == nil {
		t.Fatal("Schema missing 'id' column")
	}
	if idCol.Type != schema.TypeInt64 {
		t.Errorf("Column 'id' type = %v, want int64", idCol.Type)
	}

	nameCol := s.ColumnByName("name")
	if nameCol == nil {
		t.Fatal("Schema missing 'name' column")
	}
	if nameCol.Type != schema.TypeString {
		t.Errorf("Column 'name' type = %v, want string", nameCol.Type)
	}

	priceCol := s.ColumnByName("price")
	if priceCol == nil {
		t.Fatal("Schema missing 'price' column")
	}
	if priceCol.Type != schema.TypeFloat64 {
		t.Errorf("Column 'price' type = %v, want float64", priceCol.Type)
	}

	// Check row count
	if record.NumRows() != 2 {
		t.Errorf("NumRows() = %d, want 2", record.NumRows())
	}
}

func TestJSONAdapterParseWithNulls(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	jsonData := []byte(`[{"id": 1, "name": "Widget", "price": 9.99}, {"id": 2, "name": null, "price": null}]`)
	record, _, err := adapter.Parse(jsonData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Row 1 should have nulls
	arrowSchema := record.Schema()
	nameIdx := arrowSchema.FieldIndices("name")[0]
	priceIdx := arrowSchema.FieldIndices("price")[0]

	if !record.Column(nameIdx).IsNull(1) {
		t.Error("Row 1, column 'name' should be null")
	}
	if !record.Column(priceIdx).IsNull(1) {
		t.Error("Row 1, column 'price' should be null")
	}
}

func TestJSONAdapterParseMissingFields(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	// Second object missing 'price' field
	jsonData := []byte(`[{"id": 1, "name": "Widget", "price": 9.99}, {"id": 2, "name": "Gadget"}]`)
	record, _, err := adapter.Parse(jsonData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Missing field should be null
	arrowSchema := record.Schema()
	priceIdx := arrowSchema.FieldIndices("price")[0]
	if !record.Column(priceIdx).IsNull(1) {
		t.Error("Row 1, column 'price' should be null (missing field)")
	}
}

func TestJSONAdapterParseWithSchema(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	// Provide explicit schema with all strings
	columns := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeString, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "price", Type: schema.TypeString, Nullable: true},
	}
	s, _ := schema.NewSchema(columns)

	jsonData := []byte(`[{"id": 1, "name": "Widget", "price": 9.99}]`)
	record, returnedSchema, err := adapter.Parse(jsonData, s)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	if returnedSchema != s {
		t.Error("Returned schema should be the provided schema")
	}
}

func TestJSONAdapterParseEmpty(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	_, _, err := adapter.Parse([]byte("[]"), nil)
	if err == nil {
		t.Error("Parse() should return error for empty array")
	}
}

func TestJSONAdapterParseInvalid(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	_, _, err := adapter.Parse([]byte("not valid json"), nil)
	if err == nil {
		t.Error("Parse() should return error for invalid JSON")
	}
}

func TestJSONAdapterSerialize(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	// Parse some data
	jsonData := []byte(`[{"id": 1, "name": "Widget"}, {"id": 2, "name": "Gadget"}]`)
	record, _, err := adapter.Parse(jsonData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Serialize
	output, err := adapter.Serialize(record)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Parse the output to verify it's valid JSON
	var objects []map[string]any
	if err := json.Unmarshal(output, &objects); err != nil {
		t.Fatalf("Output is not valid JSON: %v", err)
	}

	if len(objects) != 2 {
		t.Errorf("Output has %d objects, want 2", len(objects))
	}
}

func TestJSONAdapterRoundTrip(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	original := []byte(`[{"active": true, "id": 1, "name": "Alice", "score": 95.5}, {"active": false, "id": 2, "name": "Bob", "score": 87.3}]`)

	// Parse
	record, _, err := adapter.Parse(original, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Serialize
	output, err := adapter.Serialize(record)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Parse again
	record2, _, err := adapter.Parse(output, nil)
	if err != nil {
		t.Fatalf("Second Parse() error: %v", err)
	}
	defer record2.Release()

	if record.NumRows() != record2.NumRows() {
		t.Errorf("Row counts: %d vs %d", record.NumRows(), record2.NumRows())
	}
	if record.NumCols() != record2.NumCols() {
		t.Errorf("Column counts: %d vs %d", record.NumCols(), record2.NumCols())
	}
}

// JSONL Adapter Tests

func TestJSONLAdapterFormat(t *testing.T) {
	adapter := NewJSONLAdapter(nil, nil)
	if adapter.Format() != FormatJSONL {
		t.Errorf("Format() = %v, want FormatJSONL", adapter.Format())
	}
}

func TestJSONLAdapterParseSimple(t *testing.T) {
	adapter := NewJSONLAdapter(nil, nil)

	jsonlData := []byte(`{"id": 1, "name": "Widget", "price": 9.99}
{"id": 2, "name": "Gadget", "price": 19.99}`)

	record, s, err := adapter.Parse(jsonlData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	if s.NumColumns() != 3 {
		t.Errorf("Schema has %d columns, want 3", s.NumColumns())
	}

	if record.NumRows() != 2 {
		t.Errorf("NumRows() = %d, want 2", record.NumRows())
	}
}

func TestJSONLAdapterParseWithEmptyLines(t *testing.T) {
	adapter := NewJSONLAdapter(nil, nil)

	jsonlData := []byte(`{"id": 1, "name": "Widget"}

{"id": 2, "name": "Gadget"}

{"id": 3, "name": "Gizmo"}
`)

	record, _, err := adapter.Parse(jsonlData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	if record.NumRows() != 3 {
		t.Errorf("NumRows() = %d, want 3", record.NumRows())
	}
}

func TestJSONLAdapterParseWithNulls(t *testing.T) {
	adapter := NewJSONLAdapter(nil, nil)

	jsonlData := []byte(`{"id": 1, "name": "Widget", "price": 9.99}
{"id": 2, "name": null, "price": null}`)

	record, _, err := adapter.Parse(jsonlData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Check nulls
	arrowSchema := record.Schema()
	nameIdx := arrowSchema.FieldIndices("name")[0]
	if !record.Column(nameIdx).IsNull(1) {
		t.Error("Row 1, column 'name' should be null")
	}
}

func TestJSONLAdapterParseEmpty(t *testing.T) {
	adapter := NewJSONLAdapter(nil, nil)

	_, _, err := adapter.Parse([]byte(""), nil)
	if err == nil {
		t.Error("Parse() should return error for empty data")
	}
}

func TestJSONLAdapterParseInvalidLine(t *testing.T) {
	adapter := NewJSONLAdapter(nil, nil)

	jsonlData := []byte(`{"id": 1, "name": "Widget"}
not valid json
{"id": 2, "name": "Gadget"}`)

	_, _, err := adapter.Parse(jsonlData, nil)
	if err == nil {
		t.Error("Parse() should return error for invalid JSON line")
	}
}

func TestJSONLAdapterSerialize(t *testing.T) {
	adapter := NewJSONLAdapter(nil, nil)

	// Parse some data
	jsonlData := []byte(`{"id": 1, "name": "Widget"}
{"id": 2, "name": "Gadget"}`)

	record, _, err := adapter.Parse(jsonlData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Serialize
	output, err := adapter.Serialize(record)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Should have 2 lines
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 2 {
		t.Errorf("Output has %d lines, want 2", len(lines))
	}

	// Each line should be valid JSON
	for i, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Errorf("Line %d is not valid JSON: %v", i, err)
		}
	}
}

func TestJSONLAdapterRoundTrip(t *testing.T) {
	adapter := NewJSONLAdapter(nil, nil)

	original := []byte(`{"active": true, "id": 1, "name": "Alice"}
{"active": false, "id": 2, "name": "Bob"}
{"active": true, "id": 3, "name": "Charlie"}`)

	// Parse
	record, _, err := adapter.Parse(original, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Serialize
	output, err := adapter.Serialize(record)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Parse again
	record2, _, err := adapter.Parse(output, nil)
	if err != nil {
		t.Fatalf("Second Parse() error: %v", err)
	}
	defer record2.Release()

	if record.NumRows() != record2.NumRows() {
		t.Errorf("Row counts: %d vs %d", record.NumRows(), record2.NumRows())
	}
}

func TestInferJSONFieldOrder(t *testing.T) {
	objects := []map[string]any{
		{"name": "Widget", "id": 1},
		{"price": 9.99, "id": 2, "name": "Gadget"},
	}

	fields := InferJSONFieldOrder(objects)

	// Should be sorted alphabetically
	if len(fields) != 3 {
		t.Errorf("Got %d fields, want 3", len(fields))
	}

	// Check sorted order
	expected := []string{"id", "name", "price"}
	for i, f := range fields {
		if f != expected[i] {
			t.Errorf("Field %d = %q, want %q", i, f, expected[i])
		}
	}
}

func TestJSONAdapterBooleans(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	jsonData := []byte(`[{"id": 1, "active": true}, {"id": 2, "active": false}]`)
	record, s, err := adapter.Parse(jsonData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	activeCol := s.ColumnByName("active")
	if activeCol == nil {
		t.Fatal("Schema missing 'active' column")
	}
	if activeCol.Type != schema.TypeBool {
		t.Errorf("Column 'active' type = %v, want bool", activeCol.Type)
	}
}

func TestJSONAdapterNestedIgnored(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	// Nested objects should become strings
	jsonData := []byte(`[{"id": 1, "metadata": {"key": "value"}}]`)
	record, s, err := adapter.Parse(jsonData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// metadata should be string (JSON objects become their string representation)
	metaCol := s.ColumnByName("metadata")
	if metaCol == nil {
		t.Fatal("Schema missing 'metadata' column")
	}
	// Complex types default to string
	if metaCol.Type != schema.TypeString {
		t.Errorf("Column 'metadata' type = %v, want string", metaCol.Type)
	}
}

// TestJSONAdapterNestedObjectSerializedAsJSON verifies that nested objects and
// arrays are stored as valid JSON strings, not Go's fmt.Sprintf %v output (e.g.
// "map[key:value]"). The datalake has no native struct/list type, so nested
// values must be preserved as re-parseable JSON text.
func TestJSONAdapterNestedObjectSerializedAsJSON(t *testing.T) {
	adapter := NewJSONAdapter(nil, nil)

	jsonData := []byte(`[
		{"id": 1, "data": {"configuration_format": "json", "nested": {"a": 1}}, "tags": ["x", "y"]},
		{"id": 2, "data": {"configuration_format": "csv"}, "tags": []}
	]`)
	record, s, err := adapter.Parse(jsonData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	readStringCol := func(colName string, rowIdx int) string {
		colIdx := s.ColumnIndex(colName)
		if colIdx < 0 {
			t.Fatalf("column %q not found", colName)
		}
		strCol, ok := record.Column(colIdx).(*array.String)
		if !ok {
			t.Fatalf("column %q is %T, want *array.String", colName, record.Column(colIdx))
		}
		return strCol.Value(rowIdx)
	}

	// Row 0: nested object
	dataStr := readStringCol("data", 0)
	if strings.HasPrefix(dataStr, "map[") {
		t.Errorf("nested object stored in Go map-printing format: %q", dataStr)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(dataStr), &decoded); err != nil {
		t.Fatalf("stored value is not valid JSON: %q (err=%v)", dataStr, err)
	}
	if decoded["configuration_format"] != "json" {
		t.Errorf("round-trip lost field: got %#v", decoded)
	}
	inner, ok := decoded["nested"].(map[string]any)
	if !ok {
		t.Fatalf("deep-nested object lost: %#v", decoded["nested"])
	}
	if inner["a"].(float64) != 1 {
		t.Errorf("deep-nested field lost: %#v", inner)
	}

	// Row 0: array-of-scalars
	tagsStr := readStringCol("tags", 0)
	if strings.HasPrefix(tagsStr, "[") && strings.Contains(tagsStr, " ") && !strings.Contains(tagsStr, `"`) {
		// Go's default slice printing is "[x y]" without quotes/commas.
		t.Errorf("array stored in Go slice-printing format: %q", tagsStr)
	}
	var tags []string
	if err := json.Unmarshal([]byte(tagsStr), &tags); err != nil {
		t.Fatalf("array stored as non-JSON: %q (err=%v)", tagsStr, err)
	}
	if len(tags) != 2 || tags[0] != "x" || tags[1] != "y" {
		t.Errorf("array round-trip failed: %#v", tags)
	}

	// Row 1: different nested shape (smaller) and empty array — should still be valid JSON
	dataStr1 := readStringCol("data", 1)
	var decoded1 map[string]any
	if err := json.Unmarshal([]byte(dataStr1), &decoded1); err != nil {
		t.Fatalf("row 1 nested object not valid JSON: %q (err=%v)", dataStr1, err)
	}
	tagsStr1 := readStringCol("tags", 1)
	var tags1 []any
	if err := json.Unmarshal([]byte(tagsStr1), &tags1); err != nil {
		t.Fatalf("row 1 empty array not valid JSON: %q (err=%v)", tagsStr1, err)
	}
	if len(tags1) != 0 {
		t.Errorf("empty array round-trip: got %#v", tags1)
	}
}
