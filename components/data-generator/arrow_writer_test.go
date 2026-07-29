package main

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// TestBuildArrowSchema_TypedNonNullable proves the port to dgarrow.Sink left
// data-generator's typed Arrow schema untouched: every declared column type
// must still map to a concrete Arrow type (never stringified) and every
// field must stay Nullable: false, per arrowFieldFor's doc comment (the
// iceberg AddFiles rejections it documents — runs 99ebb24e, 944823d3 — are
// exactly what a type or nullability regression here would reintroduce).
func TestBuildArrowSchema_TypedNonNullable(t *testing.T) {
	colTypes := map[string]string{
		"a_int":       "int",
		"b_long":      "long",
		"c_float":     "float",
		"d_double":    "double",
		"e_boolean":   "boolean",
		"f_string":    "string",
		"g_uuid":      "uuid",
		"h_date":      "date",
		"i_timestamp": "timestamp",
		"j_now":       "now",
	}
	colNames := []string{
		"a_int", "b_long", "c_float", "d_double", "e_boolean",
		"f_string", "g_uuid", "h_date", "i_timestamp", "j_now",
	}

	want := map[string]arrow.DataType{
		"a_int":       arrow.PrimitiveTypes.Int64,
		"b_long":      arrow.PrimitiveTypes.Int64,
		"c_float":     arrow.PrimitiveTypes.Float64,
		"d_double":    arrow.PrimitiveTypes.Float64,
		"e_boolean":   arrow.FixedWidthTypes.Boolean,
		"f_string":    arrow.BinaryTypes.String,
		"g_uuid":      arrow.BinaryTypes.String,
		"h_date":      arrow.BinaryTypes.String,
		"i_timestamp": arrow.BinaryTypes.String,
		"j_now":       arrow.BinaryTypes.String,
	}

	schema := buildArrowSchema(colNames, colTypes)

	if schema.NumFields() != len(colNames) {
		t.Fatalf("schema has %d fields, want %d", schema.NumFields(), len(colNames))
	}
	for i, name := range colNames {
		f := schema.Field(i)
		if f.Name != name {
			t.Errorf("field[%d].Name = %q, want %q (column order must stay stable)", i, f.Name, name)
		}
		if f.Nullable {
			t.Errorf("field %q: Nullable = true, want false", name)
		}
		if !arrow.TypeEqual(f.Type, want[name]) {
			t.Errorf("field %q: Type = %v, want %v", name, f.Type, want[name])
		}
	}
}
