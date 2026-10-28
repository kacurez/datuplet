package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		dt       DataType
		expected string
	}{
		{TypeUnknown, "unknown"},
		{TypeInt64, "int64"},
		{TypeInt32, "int32"},
		{TypeFloat64, "float64"},
		{TypeFloat32, "float32"},
		{TypeString, "string"},
		{TypeBool, "bool"},
		{TypeTimestamp, "timestamp"},
		{TypeDate, "date"},
		{TypeBinary, "binary"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.dt.String(); got != tt.expected {
				t.Errorf("DataType.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestParseDataType(t *testing.T) {
	tests := []struct {
		input    string
		expected DataType
	}{
		{"int64", TypeInt64},
		{"int32", TypeInt32},
		{"float64", TypeFloat64},
		{"float32", TypeFloat32},
		{"string", TypeString},
		{"bool", TypeBool},
		{"boolean", TypeBool},
		{"timestamp", TypeTimestamp},
		{"date", TypeDate},
		{"binary", TypeBinary},
		{"unknown", TypeUnknown},
		{"invalid", TypeUnknown},
		{"", TypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseDataType(tt.input); got != tt.expected {
				t.Errorf("ParseDataType(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestToArrowType(t *testing.T) {
	tests := []struct {
		dt          DataType
		expectedID  arrow.Type
		expectError bool
	}{
		{TypeInt64, arrow.INT64, false},
		{TypeInt32, arrow.INT32, false},
		{TypeFloat64, arrow.FLOAT64, false},
		{TypeFloat32, arrow.FLOAT32, false},
		{TypeString, arrow.STRING, false},
		{TypeBool, arrow.BOOL, false},
		{TypeTimestamp, arrow.TIMESTAMP, false},
		{TypeDate, arrow.DATE32, false},
		{TypeBinary, arrow.BINARY, false},
		{TypeUnknown, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.dt.String(), func(t *testing.T) {
			got, err := ToArrowType(tt.dt)
			if tt.expectError {
				if err == nil {
					t.Errorf("ToArrowType(%v) expected error, got nil", tt.dt)
				}
				return
			}
			if err != nil {
				t.Errorf("ToArrowType(%v) unexpected error: %v", tt.dt, err)
				return
			}
			if got.ID() != tt.expectedID {
				t.Errorf("ToArrowType(%v) = %v, want type ID %v", tt.dt, got, tt.expectedID)
			}
		})
	}
}

func TestFromArrowType(t *testing.T) {
	tests := []struct {
		name        string
		arrowType   arrow.DataType
		expected    DataType
		expectError bool
	}{
		{"int64", arrow.PrimitiveTypes.Int64, TypeInt64, false},
		{"int32", arrow.PrimitiveTypes.Int32, TypeInt32, false},
		{"int16", arrow.PrimitiveTypes.Int16, TypeInt32, false}, // promoted
		{"int8", arrow.PrimitiveTypes.Int8, TypeInt32, false},   // promoted
		{"uint64", arrow.PrimitiveTypes.Uint64, TypeInt64, false},
		{"uint32", arrow.PrimitiveTypes.Uint32, TypeInt64, false},
		{"float64", arrow.PrimitiveTypes.Float64, TypeFloat64, false},
		{"float32", arrow.PrimitiveTypes.Float32, TypeFloat32, false},
		{"string", arrow.BinaryTypes.String, TypeString, false},
		{"large_string", arrow.BinaryTypes.LargeString, TypeString, false},
		{"bool", arrow.FixedWidthTypes.Boolean, TypeBool, false},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_us, TypeTimestamp, false},
		{"date32", arrow.FixedWidthTypes.Date32, TypeDate, false},
		{"date64", arrow.FixedWidthTypes.Date64, TypeDate, false},
		{"binary", arrow.BinaryTypes.Binary, TypeBinary, false},
		{"large_binary", arrow.BinaryTypes.LargeBinary, TypeBinary, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FromArrowType(tt.arrowType)
			if tt.expectError {
				if err == nil {
					t.Errorf("FromArrowType(%v) expected error, got nil", tt.arrowType)
				}
				return
			}
			if err != nil {
				t.Errorf("FromArrowType(%v) unexpected error: %v", tt.arrowType, err)
				return
			}
			if got != tt.expected {
				t.Errorf("FromArrowType(%v) = %v, want %v", tt.arrowType, got, tt.expected)
			}
		})
	}
}

func TestNewSchema(t *testing.T) {
	tests := []struct {
		name    string
		columns []ColumnDef
		wantErr bool
	}{
		{
			name: "valid columns",
			columns: []ColumnDef{
				{Name: "id", Type: TypeInt64, Nullable: false},
				{Name: "name", Type: TypeString, Nullable: true},
				{Name: "price", Type: TypeFloat64, Nullable: true},
			},
		},
		{name: "empty columns", columns: []ColumnDef{}, wantErr: true},
		{name: "unknown type", columns: []ColumnDef{{Name: "bad", Type: TypeUnknown, Nullable: true}}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := NewSchema(tc.columns)
			if tc.wantErr {
				if err == nil {
					t.Error("NewSchema should return an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewSchema() error: %v", err)
			}

			if schema.NumColumns() != len(tc.columns) {
				t.Errorf("NumColumns() = %d, want %d", schema.NumColumns(), len(tc.columns))
			}
			for i, want := range tc.columns {
				col := schema.Column(i)
				if col.Name != want.Name || col.Type != want.Type || col.Nullable != want.Nullable {
					t.Errorf("Column(%d) = %+v, want {%s, %v, %v}", i, col, want.Name, want.Type, want.Nullable)
				}
			}

			// Check Arrow schema is created
			arrowSchema := schema.ArrowSchema()
			if arrowSchema == nil {
				t.Fatal("ArrowSchema() returned nil")
			}
			if arrowSchema.NumFields() != len(tc.columns) {
				t.Errorf("ArrowSchema().NumFields() = %d, want %d", arrowSchema.NumFields(), len(tc.columns))
			}
		})
	}
}

func TestNewSchemaFromArrow(t *testing.T) {
	tests := []struct {
		name    string
		in      *arrow.Schema
		wantErr bool
	}{
		{
			name: "converts fields",
			in: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
				{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			}, nil),
		},
		{name: "nil schema", in: nil, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := NewSchemaFromArrow(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Error("NewSchemaFromArrow should return an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewSchemaFromArrow() error: %v", err)
			}

			if schema.NumColumns() != 2 {
				t.Errorf("NumColumns() = %d, want 2", schema.NumColumns())
			}
			col0 := schema.Column(0)
			if col0.Name != "id" || col0.Type != TypeInt64 || col0.Nullable != false {
				t.Errorf("Column(0) = %+v, want {id, int64, false}", col0)
			}
		})
	}
}

func TestSchemaColumnLookup(t *testing.T) {
	schema, err := NewSchema([]ColumnDef{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
		{Name: "price", Type: TypeFloat64, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	tests := []struct {
		name     string
		wantIdx  int
		wantType DataType // ignored when wantIdx is -1 (not found)
	}{
		{"id", 0, TypeInt64},
		{"name", 1, TypeString},
		{"price", 2, TypeFloat64},
		{"nonexistent", -1, TypeUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if idx := schema.ColumnIndex(tc.name); idx != tc.wantIdx {
				t.Errorf("ColumnIndex(%q) = %d, want %d", tc.name, idx, tc.wantIdx)
			}
			col := schema.ColumnByName(tc.name)
			if tc.wantIdx == -1 {
				if col != nil {
					t.Errorf("ColumnByName(%q) should return nil", tc.name)
				}
				return
			}
			if col == nil {
				t.Fatalf("ColumnByName(%q) returned nil", tc.name)
			}
			if col.Type != tc.wantType {
				t.Errorf("ColumnByName(%q).Type = %v, want %v", tc.name, col.Type, tc.wantType)
			}
		})
	}
}

func TestSchemaColumns(t *testing.T) {
	columns := []ColumnDef{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
	}

	schema, err := NewSchema(columns)
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	// Get copy of columns
	cols := schema.Columns()
	if len(cols) != 2 {
		t.Errorf("Columns() returned %d columns, want 2", len(cols))
	}

	// Verify it's a copy (modifying it shouldn't affect original)
	cols[0].Name = "modified"
	if schema.Column(0).Name == "modified" {
		t.Error("Columns() should return a copy, not the original slice")
	}
}

func TestSchemaEqual(t *testing.T) {
	columns1 := []ColumnDef{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
	}

	columns2 := []ColumnDef{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
	}

	columns3 := []ColumnDef{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "title", Type: TypeString, Nullable: true}, // different name
	}

	schema1, _ := NewSchema(columns1)
	schema2, _ := NewSchema(columns2)
	schema3, _ := NewSchema(columns3)

	if !schema1.Equal(schema2) {
		t.Error("schema1 should equal schema2")
	}

	if schema1.Equal(schema3) {
		t.Error("schema1 should not equal schema3")
	}

	if schema1.Equal(nil) {
		t.Error("schema1 should not equal nil")
	}
}
