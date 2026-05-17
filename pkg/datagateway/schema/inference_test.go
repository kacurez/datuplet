package schema

import (
	"testing"
)

func TestTypeInferrer_IsNull(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		value    string
		expected bool
	}{
		{"", true},
		{"null", true},
		{"NULL", true},
		{"NA", true},
		{"N/A", true},
		{"\\N", true},
		{"nil", true},
		{"NIL", true},
		{"none", true},
		{"None", true},
		{"NONE", true},
		{"hello", false},
		{"123", false},
		{"true", false},
		{" ", false}, // whitespace is not null
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := ti.IsNull(tt.value); got != tt.expected {
				t.Errorf("IsNull(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestTypeInferrer_InferValueType_Bool(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		value    string
		expected DataType
	}{
		{"true", TypeBool},
		{"TRUE", TypeBool},
		{"True", TypeBool},
		{"false", TypeBool},
		{"FALSE", TypeBool},
		{"False", TypeBool},
		{"yes", TypeBool},
		{"YES", TypeBool},
		{"no", TypeBool},
		{"NO", TypeBool},
		{"y", TypeBool},
		{"Y", TypeBool},
		{"n", TypeBool},
		{"N", TypeBool},
		// "1" and "0" should be detected as int64, not bool
		{"1", TypeInt64},
		{"0", TypeInt64},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := ti.InferValueType(tt.value); got != tt.expected {
				t.Errorf("InferValueType(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestTypeInferrer_InferValueType_Int64(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		value    string
		expected DataType
	}{
		{"0", TypeInt64},
		{"1", TypeInt64},
		{"123", TypeInt64},
		{"-456", TypeInt64},
		{"9223372036854775807", TypeInt64},  // max int64
		{"-9223372036854775808", TypeInt64}, // min int64
		{"999999999999", TypeInt64},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := ti.InferValueType(tt.value); got != tt.expected {
				t.Errorf("InferValueType(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestTypeInferrer_InferValueType_Float64(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		value    string
		expected DataType
	}{
		{"1.5", TypeFloat64},
		{"-3.14", TypeFloat64},
		{"0.0", TypeFloat64},
		{".5", TypeFloat64},
		{"1e10", TypeFloat64},
		{"1.5e-3", TypeFloat64},
		{"-2.5E+10", TypeFloat64},
		{"3.14159265359", TypeFloat64},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := ti.InferValueType(tt.value); got != tt.expected {
				t.Errorf("InferValueType(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestTypeInferrer_InferValueType_Timestamp(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		value    string
		expected DataType
	}{
		{"2024-01-15T10:30:00Z", TypeTimestamp},
		{"2024-01-15T10:30:00+05:00", TypeTimestamp},
		{"2024-01-15T10:30:00", TypeTimestamp},
		{"2024-01-15 10:30:00", TypeTimestamp},
		{"2024-01-15T10:30:00.000", TypeTimestamp},
		{"2024-01-15 10:30:00.000", TypeTimestamp},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := ti.InferValueType(tt.value); got != tt.expected {
				t.Errorf("InferValueType(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestTypeInferrer_InferValueType_Date(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		value    string
		expected DataType
	}{
		{"2024-01-15", TypeDate},
		{"2024-1-5", TypeDate},
		{"01/15/2024", TypeDate},
		{"15/01/2024", TypeDate},
		{"2024/01/15", TypeDate},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := ti.InferValueType(tt.value); got != tt.expected {
				t.Errorf("InferValueType(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestTypeInferrer_InferValueType_String(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		value    string
		expected DataType
	}{
		{"hello", TypeString},
		{"Hello World", TypeString},
		{"foo bar baz", TypeString},
		{"abc123", TypeString},
		{"123abc", TypeString},
		{"user@example.com", TypeString},
		{"https://example.com", TypeString},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := ti.InferValueType(tt.value); got != tt.expected {
				t.Errorf("InferValueType(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestTypeInferrer_InferValueType_Null(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		value    string
		expected DataType
	}{
		{"", TypeUnknown},
		{"null", TypeUnknown},
		{"NULL", TypeUnknown},
		{"NA", TypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := ti.InferValueType(tt.value); got != tt.expected {
				t.Errorf("InferValueType(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestInferSchema_AllSameType(t *testing.T) {
	headers := []string{"id", "count"}
	rows := [][]string{
		{"1", "100"},
		{"2", "200"},
		{"3", "300"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	if schema.NumColumns() != 2 {
		t.Errorf("NumColumns() = %d, want 2", schema.NumColumns())
	}

	col0 := schema.Column(0)
	if col0.Type != TypeInt64 {
		t.Errorf("Column 'id' type = %v, want int64", col0.Type)
	}
	if col0.Nullable {
		t.Error("Column 'id' should not be nullable")
	}

	col1 := schema.Column(1)
	if col1.Type != TypeInt64 {
		t.Errorf("Column 'count' type = %v, want int64", col1.Type)
	}
}

func TestInferSchema_MixedNumericTypes(t *testing.T) {
	headers := []string{"value"}
	rows := [][]string{
		{"1"},
		{"2.5"},
		{"3"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	col := schema.Column(0)
	if col.Type != TypeFloat64 {
		t.Errorf("Mixed int/float column type = %v, want float64", col.Type)
	}
}

func TestInferSchema_NullHandling(t *testing.T) {
	headers := []string{"name", "age"}
	rows := [][]string{
		{"Alice", "30"},
		{"Bob", ""},
		{"", "25"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	col0 := schema.Column(0)
	if !col0.Nullable {
		t.Error("Column 'name' should be nullable (has empty value)")
	}
	if col0.Type != TypeString {
		t.Errorf("Column 'name' type = %v, want string", col0.Type)
	}

	col1 := schema.Column(1)
	if !col1.Nullable {
		t.Error("Column 'age' should be nullable (has empty value)")
	}
	if col1.Type != TypeInt64 {
		t.Errorf("Column 'age' type = %v, want int64", col1.Type)
	}
}

func TestInferSchema_AllNulls(t *testing.T) {
	headers := []string{"empty"}
	rows := [][]string{
		{""},
		{"null"},
		{"NA"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	col := schema.Column(0)
	if col.Type != TypeString {
		t.Errorf("All-null column type = %v, want string (default)", col.Type)
	}
	if !col.Nullable {
		t.Error("All-null column should be nullable")
	}
}

func TestInferSchema_BoolColumn(t *testing.T) {
	headers := []string{"active"}
	rows := [][]string{
		{"true"},
		{"false"},
		{"TRUE"},
		{"FALSE"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	col := schema.Column(0)
	if col.Type != TypeBool {
		t.Errorf("Bool column type = %v, want bool", col.Type)
	}
}

func TestInferSchema_DateColumn(t *testing.T) {
	headers := []string{"date"}
	rows := [][]string{
		{"2024-01-01"},
		{"2024-06-15"},
		{"2024-12-31"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	col := schema.Column(0)
	if col.Type != TypeDate {
		t.Errorf("Date column type = %v, want date", col.Type)
	}
}

func TestInferSchema_TimestampColumn(t *testing.T) {
	headers := []string{"timestamp"}
	rows := [][]string{
		{"2024-01-01T10:00:00Z"},
		{"2024-06-15T12:30:00Z"},
		{"2024-12-31T23:59:59Z"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	col := schema.Column(0)
	if col.Type != TypeTimestamp {
		t.Errorf("Timestamp column type = %v, want timestamp", col.Type)
	}
}

func TestInferSchema_MixedTypesToString(t *testing.T) {
	headers := []string{"mixed"}
	rows := [][]string{
		{"123"},
		{"hello"},
		{"456"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	col := schema.Column(0)
	if col.Type != TypeString {
		t.Errorf("Mixed int/string column type = %v, want string", col.Type)
	}
}

func TestInferSchema_BoolMixedWithNumeric(t *testing.T) {
	// When bool values (true/false) are mixed with numbers, result should be string
	headers := []string{"value"}
	rows := [][]string{
		{"true"},
		{"123"},
		{"false"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	col := schema.Column(0)
	if col.Type != TypeString {
		t.Errorf("Bool+numeric column type = %v, want string", col.Type)
	}
}

func TestInferSchema_EmptyRows(t *testing.T) {
	headers := []string{"col1", "col2"}
	rows := [][]string{}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	// With no data, all columns should default to string and nullable
	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		if col.Type != TypeString {
			t.Errorf("Column %d type = %v, want string", i, col.Type)
		}
		if !col.Nullable {
			t.Errorf("Column %d should be nullable (no data)", i)
		}
	}
}

func TestInferSchema_EmptyHeaders(t *testing.T) {
	headers := []string{}
	rows := [][]string{}

	_, err := InferSchema(headers, rows)
	if err == nil {
		t.Error("InferSchema with empty headers should return error")
	}
}

func TestInferSchema_RealWorldCSV(t *testing.T) {
	headers := []string{"id", "name", "price", "quantity", "active", "created_at"}
	rows := [][]string{
		{"1", "Widget", "9.99", "100", "true", "2024-01-15T10:00:00Z"},
		{"2", "Gadget", "19.99", "50", "false", "2024-01-16T11:30:00Z"},
		{"3", "Gizmo", "29.99", "", "true", "2024-01-17T09:15:00Z"},
	}

	schema, err := InferSchema(headers, rows)
	if err != nil {
		t.Fatalf("InferSchema() error: %v", err)
	}

	expectedTypes := map[string]DataType{
		"id":         TypeInt64,
		"name":       TypeString,
		"price":      TypeFloat64,
		"quantity":   TypeInt64,
		"active":     TypeBool,
		"created_at": TypeTimestamp,
	}

	for name, expectedType := range expectedTypes {
		col := schema.ColumnByName(name)
		if col == nil {
			t.Errorf("Column %q not found", name)
			continue
		}
		if col.Type != expectedType {
			t.Errorf("Column %q type = %v, want %v", name, col.Type, expectedType)
		}
	}

	// quantity should be nullable (has empty value)
	if col := schema.ColumnByName("quantity"); col != nil && !col.Nullable {
		t.Error("Column 'quantity' should be nullable")
	}
}

func TestInferSchemaFromJSON(t *testing.T) {
	objects := []map[string]interface{}{
		{"id": 1, "name": "Alice", "active": true},
		{"id": 2, "name": "Bob", "active": false},
		{"id": 3, "name": "Charlie", "active": true},
	}

	schema, err := InferSchemaFromJSON(objects)
	if err != nil {
		t.Fatalf("InferSchemaFromJSON() error: %v", err)
	}

	if schema.NumColumns() != 3 {
		t.Errorf("NumColumns() = %d, want 3", schema.NumColumns())
	}

	// Check columns exist (order is alphabetical)
	if col := schema.ColumnByName("id"); col == nil {
		t.Error("Column 'id' not found")
	} else if col.Type != TypeInt64 {
		t.Errorf("Column 'id' type = %v, want int64", col.Type)
	}

	if col := schema.ColumnByName("name"); col == nil {
		t.Error("Column 'name' not found")
	} else if col.Type != TypeString {
		t.Errorf("Column 'name' type = %v, want string", col.Type)
	}

	if col := schema.ColumnByName("active"); col == nil {
		t.Error("Column 'active' not found")
	} else if col.Type != TypeBool {
		t.Errorf("Column 'active' type = %v, want bool", col.Type)
	}
}

func TestInferSchemaFromJSON_MissingFields(t *testing.T) {
	objects := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 2},                        // missing name
		{"id": 3, "name": "Charlie", "extra": "value"},
	}

	schema, err := InferSchemaFromJSON(objects)
	if err != nil {
		t.Fatalf("InferSchemaFromJSON() error: %v", err)
	}

	// Should have 3 columns: id, name, extra
	if schema.NumColumns() != 3 {
		t.Errorf("NumColumns() = %d, want 3", schema.NumColumns())
	}

	// name should be nullable (missing in one object)
	if col := schema.ColumnByName("name"); col != nil && !col.Nullable {
		t.Error("Column 'name' should be nullable (missing in some objects)")
	}
}

func TestInferSchemaFromJSON_Empty(t *testing.T) {
	objects := []map[string]interface{}{}

	_, err := InferSchemaFromJSON(objects)
	if err == nil {
		t.Error("InferSchemaFromJSON with empty objects should return error")
	}
}

func TestInferSchemaFromJSON_NilValues(t *testing.T) {
	objects := []map[string]interface{}{
		{"id": 1, "value": nil},
		{"id": 2, "value": "hello"},
	}

	schema, err := InferSchemaFromJSON(objects)
	if err != nil {
		t.Fatalf("InferSchemaFromJSON() error: %v", err)
	}

	if col := schema.ColumnByName("value"); col != nil {
		if !col.Nullable {
			t.Error("Column 'value' should be nullable (has nil)")
		}
		if col.Type != TypeString {
			t.Errorf("Column 'value' type = %v, want string", col.Type)
		}
	}
}

func TestCustomInferenceConfig(t *testing.T) {
	config := &InferenceConfig{
		MinSampleSize:    1,
		NullStrings:      []string{"", "MISSING"},
		BoolTrueStrings:  []string{"Y"},
		BoolFalseStrings: []string{"N"},
		DateFormats:      []string{"02-Jan-2006"},
		TimestampFormats: []string{},
	}

	ti := NewTypeInferrer(config)

	// Test custom null
	if !ti.IsNull("MISSING") {
		t.Error("'MISSING' should be null with custom config")
	}
	if ti.IsNull("null") {
		t.Error("'null' should not be null with custom config")
	}

	// Test custom bool
	if ti.InferValueType("Y") != TypeBool {
		t.Error("'Y' should be bool with custom config")
	}
	if ti.InferValueType("true") == TypeBool {
		t.Error("'true' should not be bool with custom config")
	}
}
