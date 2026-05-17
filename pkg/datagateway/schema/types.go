// Package schema provides type definitions and schema handling for the Data Gateway.
package schema

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
)

// DataType represents a supported data type in the gateway.
type DataType int

const (
	TypeUnknown DataType = iota
	TypeInt64            // 64-bit signed integer
	TypeInt32            // 32-bit signed integer
	TypeFloat64          // 64-bit IEEE 754 floating point
	TypeFloat32          // 32-bit IEEE 754 floating point
	TypeString           // UTF-8 encoded string
	TypeBool             // Boolean true/false
	TypeTimestamp        // Timestamp with microsecond precision, UTC timezone
	TypeDate             // Date without time component (days since Unix epoch)
	TypeBinary           // Raw binary data
)

// String returns the string representation of the DataType.
func (dt DataType) String() string {
	switch dt {
	case TypeInt64:
		return "int64"
	case TypeInt32:
		return "int32"
	case TypeFloat64:
		return "float64"
	case TypeFloat32:
		return "float32"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	case TypeTimestamp:
		return "timestamp"
	case TypeDate:
		return "date"
	case TypeBinary:
		return "binary"
	default:
		return "unknown"
	}
}

// ParseDataType parses a string into a DataType.
// Returns TypeUnknown if the string is not recognized.
func ParseDataType(s string) DataType {
	switch s {
	case "int64":
		return TypeInt64
	case "int32":
		return TypeInt32
	case "float64":
		return TypeFloat64
	case "float32":
		return TypeFloat32
	case "string":
		return TypeString
	case "bool", "boolean":
		return TypeBool
	case "timestamp":
		return TypeTimestamp
	case "date":
		return TypeDate
	case "binary":
		return TypeBinary
	default:
		return TypeUnknown
	}
}

// ColumnDef defines a single column in a schema.
type ColumnDef struct {
	Name     string
	Type     DataType
	Nullable bool
	FieldID  int32 // Iceberg field ID (0 = auto-assign starting from 1)
}

// Schema wraps an Arrow schema with gateway-specific metadata.
type Schema struct {
	columns     []ColumnDef
	arrowSchema *arrow.Schema
}

// NewSchema creates a new Schema from column definitions.
func NewSchema(columns []ColumnDef) (*Schema, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("schema must have at least one column")
	}

	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		arrowType, err := ToArrowType(col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		// Add Iceberg field ID metadata (1-based, matching Iceberg schema)
		// This enables Iceberg readers to correctly map columns
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: col.Nullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": strconv.Itoa(i + 1),
			}),
		}
	}

	// Make a copy of columns to avoid external mutation
	colsCopy := make([]ColumnDef, len(columns))
	copy(colsCopy, columns)

	return &Schema{
		columns:     colsCopy,
		arrowSchema: arrow.NewSchema(fields, nil),
	}, nil
}

// NewSchemaFromArrow creates a Schema from an Arrow schema.
func NewSchemaFromArrow(arrowSchema *arrow.Schema) (*Schema, error) {
	if arrowSchema == nil {
		return nil, fmt.Errorf("arrow schema cannot be nil")
	}

	columns := make([]ColumnDef, arrowSchema.NumFields())
	for i, field := range arrowSchema.Fields() {
		dt, err := FromArrowType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		columns[i] = ColumnDef{
			Name:     field.Name,
			Type:     dt,
			Nullable: field.Nullable,
		}
	}
	return &Schema{
		columns:     columns,
		arrowSchema: arrowSchema,
	}, nil
}

// Columns returns a copy of the column definitions.
func (s *Schema) Columns() []ColumnDef {
	cols := make([]ColumnDef, len(s.columns))
	copy(cols, s.columns)
	return cols
}

// NumColumns returns the number of columns.
func (s *Schema) NumColumns() int {
	return len(s.columns)
}

// Column returns the column definition at the given index.
// Panics if index is out of bounds.
func (s *Schema) Column(i int) ColumnDef {
	return s.columns[i]
}

// ColumnByName returns the column definition for the given name.
// Returns nil if the column is not found.
func (s *Schema) ColumnByName(name string) *ColumnDef {
	for i := range s.columns {
		if s.columns[i].Name == name {
			col := s.columns[i]
			return &col
		}
	}
	return nil
}

// ColumnIndex returns the index of the column with the given name.
// Returns -1 if the column is not found.
func (s *Schema) ColumnIndex(name string) int {
	for i := range s.columns {
		if s.columns[i].Name == name {
			return i
		}
	}
	return -1
}

// ArrowSchema returns the underlying Arrow schema.
func (s *Schema) ArrowSchema() *arrow.Schema {
	return s.arrowSchema
}

// ToArrowType converts a gateway DataType to an Arrow DataType.
func ToArrowType(dt DataType) (arrow.DataType, error) {
	switch dt {
	case TypeInt64:
		return arrow.PrimitiveTypes.Int64, nil
	case TypeInt32:
		return arrow.PrimitiveTypes.Int32, nil
	case TypeFloat64:
		return arrow.PrimitiveTypes.Float64, nil
	case TypeFloat32:
		return arrow.PrimitiveTypes.Float32, nil
	case TypeString:
		return arrow.BinaryTypes.String, nil
	case TypeBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case TypeTimestamp:
		// Microsecond precision, UTC timezone
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case TypeDate:
		return arrow.FixedWidthTypes.Date32, nil
	case TypeBinary:
		return arrow.BinaryTypes.Binary, nil
	default:
		return nil, fmt.Errorf("unsupported data type: %v", dt)
	}
}

// FromArrowType converts an Arrow DataType to a gateway DataType.
func FromArrowType(at arrow.DataType) (DataType, error) {
	switch at.ID() {
	case arrow.INT64:
		return TypeInt64, nil
	case arrow.INT32:
		return TypeInt32, nil
	case arrow.INT16, arrow.INT8:
		// Promote smaller integers to int32
		return TypeInt32, nil
	case arrow.UINT64, arrow.UINT32, arrow.UINT16, arrow.UINT8:
		// Treat unsigned as int64 for simplicity
		return TypeInt64, nil
	case arrow.FLOAT64:
		return TypeFloat64, nil
	case arrow.FLOAT32:
		return TypeFloat32, nil
	case arrow.STRING, arrow.LARGE_STRING:
		return TypeString, nil
	case arrow.BOOL:
		return TypeBool, nil
	case arrow.TIMESTAMP:
		return TypeTimestamp, nil
	case arrow.DATE32, arrow.DATE64:
		return TypeDate, nil
	case arrow.BINARY, arrow.LARGE_BINARY:
		return TypeBinary, nil
	default:
		return TypeUnknown, fmt.Errorf("unsupported Arrow type: %v", at)
	}
}

// Equal returns true if two schemas have the same columns.
func (s *Schema) Equal(other *Schema) bool {
	if other == nil {
		return false
	}
	if len(s.columns) != len(other.columns) {
		return false
	}
	for i := range s.columns {
		if s.columns[i] != other.columns[i] {
			return false
		}
	}
	return true
}
