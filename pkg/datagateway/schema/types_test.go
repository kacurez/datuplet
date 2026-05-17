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

func TestDataTypeRoundTrip(t *testing.T) {
	types := []DataType{
		TypeInt64, TypeInt32, TypeFloat64, TypeFloat32,
		TypeString, TypeBool, TypeTimestamp, TypeDate, TypeBinary,
	}

	for _, dt := range types {
		t.Run(dt.String(), func(t *testing.T) {
			s := dt.String()
			parsed := ParseDataType(s)
			if parsed != dt {
				t.Errorf("Round trip failed: %v -> %q -> %v", dt, s, parsed)
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

func TestArrowTypeRoundTrip(t *testing.T) {
	types := []DataType{
		TypeInt64, TypeInt32, TypeFloat64, TypeFloat32,
		TypeString, TypeBool, TypeTimestamp, TypeDate, TypeBinary,
	}

	for _, dt := range types {
		t.Run(dt.String(), func(t *testing.T) {
			arrowType, err := ToArrowType(dt)
			if err != nil {
				t.Fatalf("ToArrowType(%v) error: %v", dt, err)
			}
			back, err := FromArrowType(arrowType)
			if err != nil {
				t.Fatalf("FromArrowType(%v) error: %v", arrowType, err)
			}
			if back != dt {
				t.Errorf("Round trip failed: %v -> %v -> %v", dt, arrowType, back)
			}
		})
	}
}

func TestNewSchema(t *testing.T) {
	columns := []ColumnDef{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
		{Name: "price", Type: TypeFloat64, Nullable: true},
	}

	schema, err := NewSchema(columns)
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	if schema.NumColumns() != 3 {
		t.Errorf("NumColumns() = %d, want 3", schema.NumColumns())
	}

	// Check each column
	col0 := schema.Column(0)
	if col0.Name != "id" || col0.Type != TypeInt64 || col0.Nullable != false {
		t.Errorf("Column(0) = %+v, want {id, int64, false}", col0)
	}

	col1 := schema.Column(1)
	if col1.Name != "name" || col1.Type != TypeString || col1.Nullable != true {
		t.Errorf("Column(1) = %+v, want {name, string, true}", col1)
	}

	// Check Arrow schema is created
	arrowSchema := schema.ArrowSchema()
	if arrowSchema == nil {
		t.Error("ArrowSchema() returned nil")
	}
	if arrowSchema.NumFields() != 3 {
		t.Errorf("ArrowSchema().NumFields() = %d, want 3", arrowSchema.NumFields())
	}
}

func TestNewSchemaEmpty(t *testing.T) {
	_, err := NewSchema([]ColumnDef{})
	if err == nil {
		t.Error("NewSchema with empty columns should return error")
	}
}

func TestNewSchemaInvalidType(t *testing.T) {
	columns := []ColumnDef{
		{Name: "bad", Type: TypeUnknown, Nullable: true},
	}

	_, err := NewSchema(columns)
	if err == nil {
		t.Error("NewSchema with unknown type should return error")
	}
}

func TestNewSchemaFromArrow(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	schema, err := NewSchemaFromArrow(arrowSchema)
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
}

func TestNewSchemaFromArrowNil(t *testing.T) {
	_, err := NewSchemaFromArrow(nil)
	if err == nil {
		t.Error("NewSchemaFromArrow(nil) should return error")
	}
}

func TestSchemaColumnByName(t *testing.T) {
	columns := []ColumnDef{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
	}

	schema, err := NewSchema(columns)
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	// Found
	col := schema.ColumnByName("name")
	if col == nil {
		t.Error("ColumnByName('name') returned nil")
	} else if col.Type != TypeString {
		t.Errorf("ColumnByName('name').Type = %v, want string", col.Type)
	}

	// Not found
	col = schema.ColumnByName("nonexistent")
	if col != nil {
		t.Error("ColumnByName('nonexistent') should return nil")
	}
}

func TestSchemaColumnIndex(t *testing.T) {
	columns := []ColumnDef{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
		{Name: "price", Type: TypeFloat64, Nullable: true},
	}

	schema, err := NewSchema(columns)
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	if idx := schema.ColumnIndex("id"); idx != 0 {
		t.Errorf("ColumnIndex('id') = %d, want 0", idx)
	}
	if idx := schema.ColumnIndex("name"); idx != 1 {
		t.Errorf("ColumnIndex('name') = %d, want 1", idx)
	}
	if idx := schema.ColumnIndex("price"); idx != 2 {
		t.Errorf("ColumnIndex('price') = %d, want 2", idx)
	}
	if idx := schema.ColumnIndex("nonexistent"); idx != -1 {
		t.Errorf("ColumnIndex('nonexistent') = %d, want -1", idx)
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
