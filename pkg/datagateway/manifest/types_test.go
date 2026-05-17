package manifest

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestWriteAndReadSchemaFile(t *testing.T) {
	// Create a test Iceberg schema
	icebergSchema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "price", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	runID := "test-session-123"
	tablePath := "raw/products"

	// Write schema file to buffer
	var buf bytes.Buffer
	err := WriteSchemaFile(&buf, runID, tablePath, icebergSchema, nil)
	if err != nil {
		t.Fatalf("WriteSchemaFile failed: %v", err)
	}

	// Verify JSON structure
	written := buf.String()
	if !strings.Contains(written, `"run_id": "test-session-123"`) {
		t.Errorf("Expected run_id in output, got: %s", written)
	}
	if !strings.Contains(written, `"table_path": "raw/products"`) {
		t.Errorf("Expected table_path in output, got: %s", written)
	}

	// Read it back
	sf, err := ReadSchemaFile(strings.NewReader(written))
	if err != nil {
		t.Fatalf("ReadSchemaFile failed: %v", err)
	}

	if sf.RunID != runID {
		t.Errorf("RunID mismatch: got %s, want %s", sf.RunID, runID)
	}
	if sf.TablePath != tablePath {
		t.Errorf("TablePath mismatch: got %s, want %s", sf.TablePath, tablePath)
	}

	// Parse the Iceberg schema
	parsedSchema, err := sf.ParseIcebergSchema()
	if err != nil {
		t.Fatalf("ParseIcebergSchema failed: %v", err)
	}

	fields := parsedSchema.Fields()
	if len(fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(fields))
	}
	if fields[0].Name != "id" {
		t.Errorf("Expected first field name 'id', got %s", fields[0].Name)
	}
}

func TestWriteAndReadManifestFile(t *testing.T) {
	runID := "test-session-456"
	tablePath := "staging/orders"

	files := []DataFileEntry{
		{Path: "data/part-00001.parquet", RowCount: 1000, SizeBytes: 50000},
		{Path: "data/part-00002.parquet", RowCount: 1500, SizeBytes: 75000},
		{Path: "data/part-00003.parquet", RowCount: 500, SizeBytes: 25000},
	}

	// Write manifest file
	var buf bytes.Buffer
	err := WriteManifestFile(&buf, runID, tablePath, files)
	if err != nil {
		t.Fatalf("WriteManifestFile failed: %v", err)
	}

	// Read it back
	mf, err := ReadManifestFile(&buf)
	if err != nil {
		t.Fatalf("ReadManifestFile failed: %v", err)
	}

	if mf.RunID != runID {
		t.Errorf("RunID mismatch: got %s, want %s", mf.RunID, runID)
	}
	if mf.TablePath != tablePath {
		t.Errorf("TablePath mismatch: got %s, want %s", mf.TablePath, tablePath)
	}
	if len(mf.Files) != 3 {
		t.Errorf("Expected 3 files, got %d", len(mf.Files))
	}

	// Verify totals were calculated correctly
	expectedRows := int64(1000 + 1500 + 500)
	if mf.TotalRows != expectedRows {
		t.Errorf("TotalRows mismatch: got %d, want %d", mf.TotalRows, expectedRows)
	}

	expectedBytes := int64(50000 + 75000 + 25000)
	if mf.TotalBytes != expectedBytes {
		t.Errorf("TotalBytes mismatch: got %d, want %d", mf.TotalBytes, expectedBytes)
	}

	// Verify individual file entries
	if mf.Files[0].Path != "data/part-00001.parquet" {
		t.Errorf("First file path mismatch: got %s", mf.Files[0].Path)
	}
	if mf.Files[1].RowCount != 1500 {
		t.Errorf("Second file row count mismatch: got %d", mf.Files[1].RowCount)
	}
}

func TestReadSchemaFileInvalidJSON(t *testing.T) {
	_, err := ReadSchemaFile(strings.NewReader("not valid json"))
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestReadManifestFileInvalidJSON(t *testing.T) {
	_, err := ReadManifestFile(strings.NewReader("{invalid}"))
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestSchemaToIceberg(t *testing.T) {
	tests := []struct {
		name     string
		columns  []schema.ColumnDef
		expected []struct {
			name     string
			typ      string
			required bool
		}
	}{
		{
			name: "basic types",
			columns: []schema.ColumnDef{
				{Name: "id", Type: schema.TypeInt64, Nullable: false},
				{Name: "name", Type: schema.TypeString, Nullable: true},
				{Name: "price", Type: schema.TypeFloat64, Nullable: true},
				{Name: "active", Type: schema.TypeBool, Nullable: false},
			},
			expected: []struct {
				name     string
				typ      string
				required bool
			}{
				{"id", "long", true},
				{"name", "string", false},
				{"price", "double", false},
				{"active", "boolean", true},
			},
		},
		{
			name: "all numeric types",
			columns: []schema.ColumnDef{
				{Name: "int32_col", Type: schema.TypeInt32, Nullable: false},
				{Name: "int64_col", Type: schema.TypeInt64, Nullable: false},
				{Name: "float32_col", Type: schema.TypeFloat32, Nullable: false},
				{Name: "float64_col", Type: schema.TypeFloat64, Nullable: false},
			},
			expected: []struct {
				name     string
				typ      string
				required bool
			}{
				{"int32_col", "int", true},
				{"int64_col", "long", true},
				{"float32_col", "float", true},
				{"float64_col", "double", true},
			},
		},
		{
			name: "temporal types",
			columns: []schema.ColumnDef{
				{Name: "created_at", Type: schema.TypeTimestamp, Nullable: true},
				{Name: "birth_date", Type: schema.TypeDate, Nullable: true},
			},
			expected: []struct {
				name     string
				typ      string
				required bool
			}{
				{"created_at", "timestamptz", false},
				{"birth_date", "date", false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create gateway schema
			gatewaySchema, err := schema.NewSchema(tt.columns)
			if err != nil {
				t.Fatalf("NewSchema failed: %v", err)
			}

			// Convert to Iceberg
			icebergSchema := SchemaToIceberg(gatewaySchema)

			if icebergSchema == nil {
				t.Fatal("SchemaToIceberg returned nil")
			}

			fields := icebergSchema.Fields()
			if len(fields) != len(tt.expected) {
				t.Fatalf("Expected %d fields, got %d", len(tt.expected), len(fields))
			}

			for i, exp := range tt.expected {
				field := fields[i]
				if field.Name != exp.name {
					t.Errorf("Field %d name: got %s, want %s", i, field.Name, exp.name)
				}
				if field.Required != exp.required {
					t.Errorf("Field %d required: got %v, want %v", i, field.Required, exp.required)
				}
				// Check type string representation
				if field.Type.String() != exp.typ {
					t.Errorf("Field %d type: got %s, want %s", i, field.Type.String(), exp.typ)
				}
			}
		})
	}
}

func TestSchemaToIcebergNil(t *testing.T) {
	result := SchemaToIceberg(nil)
	if result != nil {
		t.Error("Expected nil for nil input")
	}
}

func TestDataFileEntryJSONRoundTrip(t *testing.T) {
	entry := DataFileEntry{
		Path:      "data/part-00001.parquet",
		RowCount:  12345,
		SizeBytes: 67890,
	}

	// Marshal to JSON
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal back
	var parsed DataFileEntry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if parsed.Path != entry.Path {
		t.Errorf("Path mismatch: got %s, want %s", parsed.Path, entry.Path)
	}
	if parsed.RowCount != entry.RowCount {
		t.Errorf("RowCount mismatch: got %d, want %d", parsed.RowCount, entry.RowCount)
	}
	if parsed.SizeBytes != entry.SizeBytes {
		t.Errorf("SizeBytes mismatch: got %d, want %d", parsed.SizeBytes, entry.SizeBytes)
	}
}

func TestEmptyManifest(t *testing.T) {
	var buf bytes.Buffer
	err := WriteManifestFile(&buf, "session-1", "table/path", []DataFileEntry{})
	if err != nil {
		t.Fatalf("WriteManifestFile failed: %v", err)
	}

	mf, err := ReadManifestFile(&buf)
	if err != nil {
		t.Fatalf("ReadManifestFile failed: %v", err)
	}

	if len(mf.Files) != 0 {
		t.Errorf("Expected empty files, got %d", len(mf.Files))
	}
	if mf.TotalRows != 0 {
		t.Errorf("Expected 0 total rows, got %d", mf.TotalRows)
	}
	if mf.TotalBytes != 0 {
		t.Errorf("Expected 0 total bytes, got %d", mf.TotalBytes)
	}
}
