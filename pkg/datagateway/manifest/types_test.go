package manifest

import (
	"testing"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

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
				{"id", "long", false},
				{"name", "string", false},
				{"price", "double", false},
				{"active", "boolean", false},
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
				{"int32_col", "int", false},
				{"int64_col", "long", false},
				{"float32_col", "float", false},
				{"float64_col", "double", false},
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
