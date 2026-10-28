package format

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// The JSON (array) and JSONL (line-delimited) adapters share their behavior
// contract, so the checks below run once per adapter over equivalent inputs.

func TestJSONAdapters_ParseSimple(t *testing.T) {
	tests := []struct {
		name    string
		adapter FormatAdapter
		input   string
	}{
		{"json", NewJSONAdapter(nil, nil), `[{"id": 1, "name": "Widget", "price": 9.99}, {"id": 2, "name": "Gadget", "price": 19.99}]`},
		{"jsonl", NewJSONLAdapter(nil, nil), `{"id": 1, "name": "Widget", "price": 9.99}
{"id": 2, "name": "Gadget", "price": 19.99}`},
	}
	wantTypes := map[string]schema.DataType{"id": schema.TypeInt64, "name": schema.TypeString, "price": schema.TypeFloat64}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record, s, err := tc.adapter.Parse([]byte(tc.input), nil)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			defer record.Release()

			// Column order may vary, so check by name.
			if s.NumColumns() != 3 {
				t.Errorf("Schema has %d columns, want 3", s.NumColumns())
			}
			for name, want := range wantTypes {
				col := s.ColumnByName(name)
				if col == nil {
					t.Fatalf("Schema missing %q column", name)
				}
				if col.Type != want {
					t.Errorf("Column %q type = %v, want %v", name, col.Type, want)
				}
			}
			if record.NumRows() != 2 {
				t.Errorf("NumRows() = %d, want 2", record.NumRows())
			}
		})
	}
}

func TestJSONAdapters_ParseWithNulls(t *testing.T) {
	tests := []struct {
		name    string
		adapter FormatAdapter
		input   string
	}{
		{"json", NewJSONAdapter(nil, nil), `[{"id": 1, "name": "Widget", "price": 9.99}, {"id": 2, "name": null, "price": null}]`},
		{"jsonl", NewJSONLAdapter(nil, nil), `{"id": 1, "name": "Widget", "price": 9.99}
{"id": 2, "name": null, "price": null}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record, _, err := tc.adapter.Parse([]byte(tc.input), nil)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			defer record.Release()

			arrowSchema := record.Schema()
			for _, col := range []string{"name", "price"} {
				if !record.Column(arrowSchema.FieldIndices(col)[0]).IsNull(1) {
					t.Errorf("Row 1, column %q should be null", col)
				}
			}
		})
	}
}

func TestJSONAdapters_ParseRejects(t *testing.T) {
	tests := []struct {
		name    string
		adapter FormatAdapter
		input   string
	}{
		{"json: empty array", NewJSONAdapter(nil, nil), "[]"},
		{"json: invalid JSON", NewJSONAdapter(nil, nil), "not valid json"},
		{"jsonl: empty data", NewJSONLAdapter(nil, nil), ""},
		{"jsonl: invalid line", NewJSONLAdapter(nil, nil), `{"id": 1, "name": "Widget"}
not valid json
{"id": 2, "name": "Gadget"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := tc.adapter.Parse([]byte(tc.input), nil); err == nil {
				t.Error("Parse() should return an error")
			}
		})
	}
}

func TestJSONAdapters_Serialize(t *testing.T) {
	tests := []struct {
		name    string
		adapter FormatAdapter
		input   string
		// objects decodes the serialized output into its row objects.
		objects func(t *testing.T, output []byte) []map[string]any
	}{
		{
			name:    "json: array of objects",
			adapter: NewJSONAdapter(nil, nil),
			input:   `[{"id": 1, "name": "Widget"}, {"id": 2, "name": "Gadget"}]`,
			objects: func(t *testing.T, output []byte) []map[string]any {
				var objects []map[string]any
				if err := json.Unmarshal(output, &objects); err != nil {
					t.Fatalf("Output is not valid JSON: %v", err)
				}
				return objects
			},
		},
		{
			name:    "jsonl: one valid JSON object per line",
			adapter: NewJSONLAdapter(nil, nil),
			input: `{"id": 1, "name": "Widget"}
{"id": 2, "name": "Gadget"}`,
			objects: func(t *testing.T, output []byte) []map[string]any {
				var objects []map[string]any
				for i, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
					var obj map[string]any
					if err := json.Unmarshal([]byte(line), &obj); err != nil {
						t.Errorf("Line %d is not valid JSON: %v", i, err)
					}
					objects = append(objects, obj)
				}
				return objects
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record, _, err := tc.adapter.Parse([]byte(tc.input), nil)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			defer record.Release()

			output, err := tc.adapter.Serialize(record)
			if err != nil {
				t.Fatalf("Serialize() error: %v", err)
			}
			if got := len(tc.objects(t, output)); got != 2 {
				t.Errorf("Output has %d objects, want 2", got)
			}
		})
	}
}

func TestJSONAdapters_RoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		adapter FormatAdapter
		input   string
	}{
		{"json", NewJSONAdapter(nil, nil), `[{"active": true, "id": 1, "name": "Alice", "score": 95.5}, {"active": false, "id": 2, "name": "Bob", "score": 87.3}]`},
		{"jsonl", NewJSONLAdapter(nil, nil), `{"active": true, "id": 1, "name": "Alice"}
{"active": false, "id": 2, "name": "Bob"}
{"active": true, "id": 3, "name": "Charlie"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record, _, err := tc.adapter.Parse([]byte(tc.input), nil)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			defer record.Release()

			output, err := tc.adapter.Serialize(record)
			if err != nil {
				t.Fatalf("Serialize() error: %v", err)
			}

			record2, _, err := tc.adapter.Parse(output, nil)
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
		})
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

// JSONL Adapter Tests

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
