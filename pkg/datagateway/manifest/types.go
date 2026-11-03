// Package manifest converts gateway schemas to Iceberg schemas for table
// creation.
package manifest

import (
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// SchemaToIceberg converts a gateway Schema to an Iceberg Schema. Every field
// is optional: the data plane writes OPTIONAL Parquet columns, and iceberg-go
// rejects an optional column for a required field. A column of an unknown
// type is an error.
func SchemaToIceberg(s *schema.Schema) (*iceberg.Schema, error) {
	if s == nil {
		return nil, nil
	}

	columns := s.Columns()
	fields := make([]iceberg.NestedField, len(columns))

	for i, col := range columns {
		typ, err := dataTypeToIceberg(col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		fields[i] = iceberg.NestedField{
			ID:       i + 1, // Iceberg field IDs start at 1
			Name:     col.Name,
			Type:     typ,
			Required: false,
		}
	}

	return iceberg.NewSchema(0, fields...), nil
}

// dataTypeToIceberg converts a gateway DataType to an Iceberg Type.
func dataTypeToIceberg(dt schema.DataType) (iceberg.Type, error) {
	switch dt {
	case schema.TypeInt64:
		return iceberg.PrimitiveTypes.Int64, nil
	case schema.TypeInt32:
		return iceberg.PrimitiveTypes.Int32, nil
	case schema.TypeFloat64:
		return iceberg.PrimitiveTypes.Float64, nil
	case schema.TypeFloat32:
		return iceberg.PrimitiveTypes.Float32, nil
	case schema.TypeString:
		return iceberg.PrimitiveTypes.String, nil
	case schema.TypeBool:
		return iceberg.PrimitiveTypes.Bool, nil
	case schema.TypeTimestamp:
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case schema.TypeDate:
		return iceberg.PrimitiveTypes.Date, nil
	case schema.TypeBinary:
		return iceberg.PrimitiveTypes.Binary, nil
	case schema.TypeVariant:
		return iceberg.VariantType{}, nil
	default:
		return nil, fmt.Errorf("unsupported data type %s", dt)
	}
}
