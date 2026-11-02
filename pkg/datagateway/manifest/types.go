// Package manifest converts gateway schemas to Iceberg schemas for table
// creation.
package manifest

import (
	"github.com/apache/iceberg-go"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// SchemaToIceberg converts a gateway Schema to an Iceberg Schema. Every field
// is optional: the data plane writes OPTIONAL Parquet columns, and iceberg-go
// rejects an optional column for a required field.
func SchemaToIceberg(s *schema.Schema) *iceberg.Schema {
	if s == nil {
		return nil
	}

	columns := s.Columns()
	fields := make([]iceberg.NestedField, len(columns))

	for i, col := range columns {
		fields[i] = iceberg.NestedField{
			ID:       i + 1, // Iceberg field IDs start at 1
			Name:     col.Name,
			Type:     dataTypeToIceberg(col.Type),
			Required: false,
		}
	}

	return iceberg.NewSchema(0, fields...)
}

// dataTypeToIceberg converts a gateway DataType to an Iceberg Type.
func dataTypeToIceberg(dt schema.DataType) iceberg.Type {
	switch dt {
	case schema.TypeInt64:
		return iceberg.PrimitiveTypes.Int64
	case schema.TypeInt32:
		return iceberg.PrimitiveTypes.Int32
	case schema.TypeFloat64:
		return iceberg.PrimitiveTypes.Float64
	case schema.TypeFloat32:
		return iceberg.PrimitiveTypes.Float32
	case schema.TypeString:
		return iceberg.PrimitiveTypes.String
	case schema.TypeBool:
		return iceberg.PrimitiveTypes.Bool
	case schema.TypeTimestamp:
		return iceberg.PrimitiveTypes.TimestampTz
	case schema.TypeDate:
		return iceberg.PrimitiveTypes.Date
	case schema.TypeBinary:
		return iceberg.PrimitiveTypes.Binary
	default:
		// Default to string for unknown types
		return iceberg.PrimitiveTypes.String
	}
}
