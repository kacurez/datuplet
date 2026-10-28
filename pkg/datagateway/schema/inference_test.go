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

func TestTypeInferrer_InferValueType(t *testing.T) {
	ti := NewTypeInferrer(nil)

	tests := []struct {
		name   string
		values []string
		want   DataType
	}{
		{"bool", []string{"true", "TRUE", "True", "false", "FALSE", "False", "yes", "YES", "no", "NO", "y", "Y", "n", "N"}, TypeBool},
		// "1" and "0" should be detected as int64, not bool
		{"1 and 0 are int64, not bool", []string{"1", "0"}, TypeInt64},
		{"int64", []string{"0", "1", "123", "-456", "9223372036854775807" /* max */, "-9223372036854775808" /* min */, "999999999999"}, TypeInt64},
		{"float64", []string{"1.5", "-3.14", "0.0", ".5", "1e10", "1.5e-3", "-2.5E+10", "3.14159265359"}, TypeFloat64},
		{"timestamp", []string{
			"2024-01-15T10:30:00Z", "2024-01-15T10:30:00+05:00", "2024-01-15T10:30:00",
			"2024-01-15 10:30:00", "2024-01-15T10:30:00.000", "2024-01-15 10:30:00.000",
		}, TypeTimestamp},
		{"date", []string{"2024-01-15", "2024-1-5", "01/15/2024", "15/01/2024", "2024/01/15"}, TypeDate},
		{"string", []string{"hello", "Hello World", "foo bar baz", "abc123", "123abc", "user@example.com", "https://example.com"}, TypeString},
		{"null tokens are unknown", []string{"", "null", "NULL", "NA"}, TypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, v := range tt.values {
				if got := ti.InferValueType(v); got != tt.want {
					t.Errorf("InferValueType(%q) = %v, want %v", v, got, tt.want)
				}
			}
		})
	}
}

func TestInferSchema(t *testing.T) {
	tests := []struct {
		name         string
		headers      []string
		rows         [][]string
		wantTypes    []DataType
		wantNullable []bool // when non-nil, per-column nullability
	}{
		{
			name:         "all same type",
			headers:      []string{"id", "count"},
			rows:         [][]string{{"1", "100"}, {"2", "200"}, {"3", "300"}},
			wantTypes:    []DataType{TypeInt64, TypeInt64},
			wantNullable: []bool{false, false},
		},
		{
			name:      "mixed int/float widens to float64",
			headers:   []string{"value"},
			rows:      [][]string{{"1"}, {"2.5"}, {"3"}},
			wantTypes: []DataType{TypeFloat64},
		},
		{
			name:         "empty values make columns nullable",
			headers:      []string{"name", "age"},
			rows:         [][]string{{"Alice", "30"}, {"Bob", ""}, {"", "25"}},
			wantTypes:    []DataType{TypeString, TypeInt64},
			wantNullable: []bool{true, true},
		},
		{
			name:         "all nulls default to nullable string",
			headers:      []string{"empty"},
			rows:         [][]string{{""}, {"null"}, {"NA"}},
			wantTypes:    []DataType{TypeString},
			wantNullable: []bool{true},
		},
		{
			name:      "bool column",
			headers:   []string{"active"},
			rows:      [][]string{{"true"}, {"false"}, {"TRUE"}, {"FALSE"}},
			wantTypes: []DataType{TypeBool},
		},
		{
			name:      "date column",
			headers:   []string{"date"},
			rows:      [][]string{{"2024-01-01"}, {"2024-06-15"}, {"2024-12-31"}},
			wantTypes: []DataType{TypeDate},
		},
		{
			name:      "timestamp column",
			headers:   []string{"timestamp"},
			rows:      [][]string{{"2024-01-01T10:00:00Z"}, {"2024-06-15T12:30:00Z"}, {"2024-12-31T23:59:59Z"}},
			wantTypes: []DataType{TypeTimestamp},
		},
		{
			name:      "mixed int/string falls back to string",
			headers:   []string{"mixed"},
			rows:      [][]string{{"123"}, {"hello"}, {"456"}},
			wantTypes: []DataType{TypeString},
		},
		{
			// When bool values (true/false) are mixed with numbers, result should be string
			name:      "bool mixed with numeric falls back to string",
			headers:   []string{"value"},
			rows:      [][]string{{"true"}, {"123"}, {"false"}},
			wantTypes: []DataType{TypeString},
		},
		{
			// With no data, all columns default to string and nullable
			name:         "no rows defaults to nullable string",
			headers:      []string{"col1", "col2"},
			rows:         [][]string{},
			wantTypes:    []DataType{TypeString, TypeString},
			wantNullable: []bool{true, true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := InferSchema(tc.headers, tc.rows)
			if err != nil {
				t.Fatalf("InferSchema() error: %v", err)
			}
			if schema.NumColumns() != len(tc.wantTypes) {
				t.Fatalf("NumColumns() = %d, want %d", schema.NumColumns(), len(tc.wantTypes))
			}
			for i, want := range tc.wantTypes {
				if got := schema.Column(i).Type; got != want {
					t.Errorf("Column %d (%s) type = %v, want %v", i, tc.headers[i], got, want)
				}
			}
			for i, want := range tc.wantNullable {
				if got := schema.Column(i).Nullable; got != want {
					t.Errorf("Column %d (%s) nullable = %v, want %v", i, tc.headers[i], got, want)
				}
			}
		})
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
	tests := []struct {
		name         string
		objects      []map[string]interface{}
		wantErr      bool
		wantCols     int
		wantTypes    map[string]DataType
		wantNullable map[string]bool
	}{
		{
			name: "simple objects",
			objects: []map[string]interface{}{
				{"id": 1, "name": "Alice", "active": true},
				{"id": 2, "name": "Bob", "active": false},
				{"id": 3, "name": "Charlie", "active": true},
			},
			wantCols:  3,
			wantTypes: map[string]DataType{"id": TypeInt64, "name": TypeString, "active": TypeBool},
		},
		{
			name: "missing fields are nullable",
			objects: []map[string]interface{}{
				{"id": 1, "name": "Alice"},
				{"id": 2}, // missing name
				{"id": 3, "name": "Charlie", "extra": "value"},
			},
			wantCols:     3, // id, name, extra
			wantNullable: map[string]bool{"name": true},
		},
		{
			name: "nil values are nullable",
			objects: []map[string]interface{}{
				{"id": 1, "value": nil},
				{"id": 2, "value": "hello"},
			},
			wantTypes:    map[string]DataType{"value": TypeString},
			wantNullable: map[string]bool{"value": true},
			wantCols:     2,
		},
		{name: "empty objects", objects: []map[string]interface{}{}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := InferSchemaFromJSON(tc.objects)
			if tc.wantErr {
				if err == nil {
					t.Error("InferSchemaFromJSON should return an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("InferSchemaFromJSON() error: %v", err)
			}
			if schema.NumColumns() != tc.wantCols {
				t.Errorf("NumColumns() = %d, want %d", schema.NumColumns(), tc.wantCols)
			}
			for name, want := range tc.wantTypes {
				col := schema.ColumnByName(name)
				if col == nil {
					t.Errorf("Column %q not found", name)
				} else if col.Type != want {
					t.Errorf("Column %q type = %v, want %v", name, col.Type, want)
				}
			}
			for name, want := range tc.wantNullable {
				col := schema.ColumnByName(name)
				if col == nil {
					t.Errorf("Column %q not found", name)
				} else if col.Nullable != want {
					t.Errorf("Column %q nullable = %v, want %v", name, col.Nullable, want)
				}
			}
		})
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
