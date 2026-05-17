package format

import (
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestCSVAdapterFormat(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)
	if adapter.Format() != FormatCSV {
		t.Errorf("Format() = %v, want FormatCSV", adapter.Format())
	}
}

func TestCSVAdapterParseSimple(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	csvData := []byte("id,name,price\n1,Widget,9.99\n2,Gadget,19.99")
	record, s, err := adapter.Parse(csvData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Check schema
	if s.NumColumns() != 3 {
		t.Errorf("Schema has %d columns, want 3", s.NumColumns())
	}

	// Check column types
	if s.Column(0).Type != schema.TypeInt64 {
		t.Errorf("Column 0 type = %v, want int64", s.Column(0).Type)
	}
	if s.Column(1).Type != schema.TypeString {
		t.Errorf("Column 1 type = %v, want string", s.Column(1).Type)
	}
	if s.Column(2).Type != schema.TypeFloat64 {
		t.Errorf("Column 2 type = %v, want float64", s.Column(2).Type)
	}

	// Check row count
	if record.NumRows() != 2 {
		t.Errorf("NumRows() = %d, want 2", record.NumRows())
	}
}

func TestCSVAdapterParseNoHeader(t *testing.T) {
	opts := &ParseOptions{
		HasHeader: false,
		Delimiter: ',',
	}
	adapter := NewCSVAdapter(nil, opts)

	csvData := []byte("1,Widget,9.99\n2,Gadget,19.99")
	record, s, err := adapter.Parse(csvData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Check auto-generated column names
	if s.Column(0).Name != "col0" {
		t.Errorf("Column 0 name = %q, want col0", s.Column(0).Name)
	}
	if s.Column(1).Name != "col1" {
		t.Errorf("Column 1 name = %q, want col1", s.Column(1).Name)
	}
	if s.Column(2).Name != "col2" {
		t.Errorf("Column 2 name = %q, want col2", s.Column(2).Name)
	}

	if record.NumRows() != 2 {
		t.Errorf("NumRows() = %d, want 2", record.NumRows())
	}
}

func TestCSVAdapterParseCustomDelimiter(t *testing.T) {
	opts := &ParseOptions{
		HasHeader: true,
		Delimiter: ';',
	}
	adapter := NewCSVAdapter(nil, opts)

	csvData := []byte("id;name;price\n1;Widget;9.99\n2;Gadget;19.99")
	record, s, err := adapter.Parse(csvData, nil)
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

func TestCSVAdapterParseWithNulls(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	csvData := []byte("id,name,price\n1,Widget,9.99\n2,,\n3,Gadget,")
	record, _, err := adapter.Parse(csvData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Check nulls in name column (index 1)
	nameCol := record.Column(1)
	if !nameCol.IsNull(1) {
		t.Error("Row 1, column 'name' should be null")
	}
	if nameCol.IsNull(0) {
		t.Error("Row 0, column 'name' should not be null")
	}

	// Check nulls in price column (index 2)
	priceCol := record.Column(2)
	if !priceCol.IsNull(1) {
		t.Error("Row 1, column 'price' should be null")
	}
	if !priceCol.IsNull(2) {
		t.Error("Row 2, column 'price' should be null")
	}
}

func TestCSVAdapterParseWithSchema(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	// Provide explicit schema
	columns := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeString, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "price", Type: schema.TypeString, Nullable: true},
	}
	s, err := schema.NewSchema(columns)
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	csvData := []byte("id,name,price\n1,Widget,9.99\n2,Gadget,19.99")
	record, returnedSchema, err := adapter.Parse(csvData, s)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Schema should be the one we provided
	if returnedSchema != s {
		t.Error("Returned schema should be the provided schema")
	}

	// All columns should be strings
	for i := 0; i < 3; i++ {
		if s.Column(i).Type != schema.TypeString {
			t.Errorf("Column %d type = %v, want string", i, s.Column(i).Type)
		}
	}
}

func TestCSVAdapterParseEmpty(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	_, _, err := adapter.Parse([]byte(""), nil)
	if err == nil {
		t.Error("Parse() should return error for empty data")
	}
}

func TestCSVAdapterParseHeaderOnly(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	csvData := []byte("id,name,price")
	record, s, err := adapter.Parse(csvData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	if s.NumColumns() != 3 {
		t.Errorf("Schema has %d columns, want 3", s.NumColumns())
	}
	if record.NumRows() != 0 {
		t.Errorf("NumRows() = %d, want 0", record.NumRows())
	}
}

func TestCSVAdapterSerialize(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	// First parse some data
	csvData := []byte("id,name,price\n1,Widget,9.99\n2,Gadget,19.99")
	record, _, err := adapter.Parse(csvData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Serialize back to CSV
	output, err := adapter.Serialize(record)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Check output contains header and data
	outputStr := string(output)
	if !strings.Contains(outputStr, "id,name,price") {
		t.Errorf("Output missing header: %s", outputStr)
	}
	if !strings.Contains(outputStr, "Widget") {
		t.Errorf("Output missing data: %s", outputStr)
	}
}

func TestCSVAdapterRoundTrip(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	original := []byte("id,name,active,score\n1,Alice,true,95.5\n2,Bob,false,87.3\n3,Charlie,true,91.0")

	// Parse
	record, s, err := adapter.Parse(original, nil)
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
	record2, s2, err := adapter.Parse(output, nil)
	if err != nil {
		t.Fatalf("Second Parse() error: %v", err)
	}
	defer record2.Release()

	// Compare
	if s.NumColumns() != s2.NumColumns() {
		t.Errorf("Schema columns: %d vs %d", s.NumColumns(), s2.NumColumns())
	}
	if record.NumRows() != record2.NumRows() {
		t.Errorf("Row counts: %d vs %d", record.NumRows(), record2.NumRows())
	}
}

func TestCSVAdapterParseTimestamps(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	csvData := []byte("id,created_at\n1,2024-01-15T10:30:00Z\n2,2024-02-20T15:45:00Z")
	record, s, err := adapter.Parse(csvData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// created_at should be inferred as timestamp
	if s.Column(1).Type != schema.TypeTimestamp {
		t.Errorf("Column 'created_at' type = %v, want timestamp", s.Column(1).Type)
	}
}

func TestCSVAdapterParseDates(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	csvData := []byte("id,birth_date\n1,2000-01-15\n2,1995-06-20")
	record, s, err := adapter.Parse(csvData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// birth_date should be inferred as date
	if s.Column(1).Type != schema.TypeDate {
		t.Errorf("Column 'birth_date' type = %v, want date", s.Column(1).Type)
	}
}

func TestCSVAdapterParseBooleans(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	csvData := []byte("id,active\n1,true\n2,false\n3,yes\n4,no")
	record, s, err := adapter.Parse(csvData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// active should be inferred as bool
	if s.Column(1).Type != schema.TypeBool {
		t.Errorf("Column 'active' type = %v, want bool", s.Column(1).Type)
	}

	if record.NumRows() != 4 {
		t.Errorf("NumRows() = %d, want 4", record.NumRows())
	}
}

func TestCSVAdapterColumnMismatch(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	// Schema has 2 columns
	columns := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
	}
	s, _ := schema.NewSchema(columns)

	// CSV has 3 columns
	csvData := []byte("id,name,price\n1,Widget,9.99")
	_, _, err := adapter.Parse(csvData, s)
	if err == nil {
		t.Error("Parse() should return error for column count mismatch")
	}
}
