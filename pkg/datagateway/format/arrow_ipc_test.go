package format

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestArrowIPCAdapterFormat(t *testing.T) {
	adapter := NewArrowIPCAdapter(nil)
	if adapter.Format() != FormatArrowIPC {
		t.Errorf("Format() = %v, want FormatArrowIPC", adapter.Format())
	}
}

func makeTestRecord(allocator memory.Allocator) arrow.Record {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()

	// Row 0
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("Widget")
	builder.Field(2).(*array.Float64Builder).Append(9.99)

	// Row 1
	builder.Field(0).(*array.Int64Builder).Append(2)
	builder.Field(1).(*array.StringBuilder).Append("Gadget")
	builder.Field(2).(*array.Float64Builder).Append(19.99)

	// Row 2 with null
	builder.Field(0).(*array.Int64Builder).Append(3)
	builder.Field(1).(*array.StringBuilder).AppendNull()
	builder.Field(2).(*array.Float64Builder).AppendNull()

	return builder.NewRecord()
}

func TestArrowIPCAdapterRoundTrip(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a test record
	original := makeTestRecord(allocator)
	defer original.Release()

	// Serialize to IPC
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	if len(ipcData) == 0 {
		t.Fatal("Serialize() returned empty data")
	}

	// Parse back
	parsed, s, err := adapter.Parse(ipcData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer parsed.Release()

	// Verify schema
	if s.NumColumns() != 3 {
		t.Errorf("Schema has %d columns, want 3", s.NumColumns())
	}

	// Verify column names
	if s.Column(0).Name != "id" {
		t.Errorf("Column 0 name = %q, want id", s.Column(0).Name)
	}
	if s.Column(1).Name != "name" {
		t.Errorf("Column 1 name = %q, want name", s.Column(1).Name)
	}
	if s.Column(2).Name != "price" {
		t.Errorf("Column 2 name = %q, want price", s.Column(2).Name)
	}

	// Verify row count
	if parsed.NumRows() != 3 {
		t.Errorf("NumRows() = %d, want 3", parsed.NumRows())
	}

	// Verify data
	idCol := parsed.Column(0).(*array.Int64)
	if idCol.Value(0) != 1 || idCol.Value(1) != 2 || idCol.Value(2) != 3 {
		t.Errorf("id column values incorrect")
	}

	nameCol := parsed.Column(1).(*array.String)
	if nameCol.Value(0) != "Widget" || nameCol.Value(1) != "Gadget" {
		t.Errorf("name column values incorrect")
	}
	if !nameCol.IsNull(2) {
		t.Errorf("name column row 2 should be null")
	}

	priceCol := parsed.Column(2).(*array.Float64)
	if priceCol.Value(0) != 9.99 || priceCol.Value(1) != 19.99 {
		t.Errorf("price column values incorrect")
	}
	if !priceCol.IsNull(2) {
		t.Errorf("price column row 2 should be null")
	}
}

func TestArrowIPCAdapterParseWithProvidedSchema(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a test record
	original := makeTestRecord(allocator)
	defer original.Release()

	// Serialize to IPC
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Create matching schema
	s, err := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "price", Type: schema.TypeFloat64, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	// Parse with provided schema
	parsed, resultSchema, err := adapter.Parse(ipcData, s)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer parsed.Release()

	// Should use provided schema
	if resultSchema != s {
		t.Error("Parse() should return the provided schema")
	}

	if parsed.NumRows() != 3 {
		t.Errorf("NumRows() = %d, want 3", parsed.NumRows())
	}
}

func TestArrowIPCAdapterParseSchemaMismatch(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a test record
	original := makeTestRecord(allocator)
	defer original.Release()

	// Serialize to IPC
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Create mismatched schema (different column count)
	s, err := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	// Parse with mismatched schema should fail
	_, _, err = adapter.Parse(ipcData, s)
	if err == nil {
		t.Error("Parse() should fail with schema mismatch")
	}
}

func TestArrowIPCAdapterParseTypeMismatch(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a test record
	original := makeTestRecord(allocator)
	defer original.Release()

	// Serialize to IPC
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Create schema with wrong type for 'id' column
	s, err := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeString, Nullable: false}, // Wrong type
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "price", Type: schema.TypeFloat64, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	// Parse with type mismatch should fail
	_, _, err = adapter.Parse(ipcData, s)
	if err == nil {
		t.Error("Parse() should fail with type mismatch")
	}
}

func TestArrowIPCAdapterParseEmpty(t *testing.T) {
	adapter := NewArrowIPCAdapter(nil)

	_, _, err := adapter.Parse([]byte{}, nil)
	if err == nil {
		t.Error("Parse() should fail with empty data")
	}
}

func TestArrowIPCAdapterInRegistry(t *testing.T) {
	registry := DefaultRegistry()

	adapter, err := registry.Get(FormatArrowIPC)
	if err != nil {
		t.Fatalf("Registry.Get(FormatArrowIPC) error: %v", err)
	}

	if adapter.Format() != FormatArrowIPC {
		t.Errorf("Adapter format = %v, want FormatArrowIPC", adapter.Format())
	}
}

func TestDataFormatArrowIPC(t *testing.T) {
	f := FormatArrowIPC

	if f.String() != "arrow" {
		t.Errorf("String() = %q, want arrow", f.String())
	}

	if f.Extension() != ".arrow" {
		t.Errorf("Extension() = %q, want .arrow", f.Extension())
	}

	if f.MimeType() != "application/vnd.apache.arrow.stream" {
		t.Errorf("MimeType() = %q, want application/vnd.apache.arrow.stream", f.MimeType())
	}
}

func TestParseDataFormatArrowIPC(t *testing.T) {
	tests := []struct {
		input  string
		expect DataFormat
	}{
		{"arrow", FormatArrowIPC},
		{"ARROW", FormatArrowIPC},
		{"ipc", FormatArrowIPC},
		{"IPC", FormatArrowIPC},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseDataFormat(tt.input)
			if result != tt.expect {
				t.Errorf("ParseDataFormat(%q) = %v, want %v", tt.input, result, tt.expect)
			}
		})
	}
}

func TestArrowIPCAdapterWithDifferentTypes(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a record with various types
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "int8_col", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "int32_col", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "float32_col", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "date_col", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()

	builder.Field(0).(*array.Int8Builder).Append(42)
	builder.Field(1).(*array.Int32Builder).Append(1000)
	builder.Field(2).(*array.Float32Builder).Append(3.14)
	builder.Field(3).(*array.BooleanBuilder).Append(true)
	builder.Field(4).(*array.Date32Builder).Append(arrow.Date32(19000)) // Some date

	original := builder.NewRecord()
	defer original.Release()

	// Serialize and parse
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	parsed, s, err := adapter.Parse(ipcData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer parsed.Release()

	// Verify column count and types
	if s.NumColumns() != 5 {
		t.Errorf("Schema has %d columns, want 5", s.NumColumns())
	}

	if parsed.NumRows() != 1 {
		t.Errorf("NumRows() = %d, want 1", parsed.NumRows())
	}

	// Verify values
	if parsed.Column(0).(*array.Int8).Value(0) != 42 {
		t.Error("int8_col value incorrect")
	}
	if parsed.Column(1).(*array.Int32).Value(0) != 1000 {
		t.Error("int32_col value incorrect")
	}
	if parsed.Column(3).(*array.Boolean).Value(0) != true {
		t.Error("bool_col value incorrect")
	}
}
