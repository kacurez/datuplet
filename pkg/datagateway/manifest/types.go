// Package manifest provides types for data file manifests and schema files
// used by the TableCommit job to create Iceberg snapshots.
package manifest

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/iceberg-go"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// PartitionFieldDef describes a partition field in the stable format.
type PartitionFieldDef struct {
	SourceColumn string `json:"source_column"`
	Transform    string `json:"transform"`
	FieldName    string `json:"field_name"` // Directory name used in partition paths
}

// SchemaFile represents the schema file written by the Data Gateway.
// It contains the inferred schema in Iceberg format.
type SchemaFile struct {
	// RunID is the unique execution identifier for this pipeline run.
	RunID string `json:"run_id"`

	// TablePath is the logical table path (e.g., "raw/products").
	TablePath string `json:"table_path"`

	// IcebergSchema is the schema in Iceberg JSON format.
	IcebergSchema json.RawMessage `json:"iceberg_schema"`

	// PartitionSpec describes the partition fields (omitted for unpartitioned tables).
	PartitionSpec []PartitionFieldDef `json:"partition_spec,omitempty"`
}

// DataFilesManifest lists all data files produced in a write session.
type DataFilesManifest struct {
	// RunID is the unique execution identifier for this pipeline run.
	RunID string `json:"run_id"`

	// TablePath is the logical table path (e.g., "raw/products").
	TablePath string `json:"table_path"`

	// Files lists all data files written.
	Files []DataFileEntry `json:"files"`

	// TotalRows is the sum of all rows across all files.
	TotalRows int64 `json:"total_rows"`

	// TotalBytes is the sum of all file sizes.
	TotalBytes int64 `json:"total_bytes"`
}

// DataFileEntry represents a single data file in the manifest.
type DataFileEntry struct {
	// Path is the relative path to the data file (e.g., "data/part-00001.parquet").
	Path string `json:"path"`

	// RowCount is the number of rows in this file.
	RowCount int64 `json:"row_count"`

	// SizeBytes is the file size in bytes.
	SizeBytes int64 `json:"size_bytes,omitempty"`

	// PartitionValues maps field_name to canonical string value (omitted for unpartitioned).
	PartitionValues map[string]string `json:"partition_values,omitempty"`
}

// WriteSchemaFile writes the schema file to the given writer.
// partitionSpec is optional (nil or empty for unpartitioned tables).
func WriteSchemaFile(w io.Writer, runID, tablePath string, schema *iceberg.Schema, partitionSpec []PartitionFieldDef) error {
	// Serialize Iceberg schema to JSON
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal iceberg schema: %w", err)
	}

	sf := &SchemaFile{
		RunID:         runID,
		TablePath:     tablePath,
		IcebergSchema: schemaJSON,
		PartitionSpec: partitionSpec,
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(sf)
}

// WriteManifestFile writes the data files manifest to the given writer.
func WriteManifestFile(w io.Writer, runID, tablePath string, files []DataFileEntry) error {
	var totalRows, totalBytes int64
	for _, f := range files {
		totalRows += f.RowCount
		totalBytes += f.SizeBytes
	}

	mf := &DataFilesManifest{
		RunID: runID,
		TablePath:      tablePath,
		Files:          files,
		TotalRows:      totalRows,
		TotalBytes:     totalBytes,
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(mf)
}

// ReadSchemaFile reads a schema file from the given reader.
func ReadSchemaFile(r io.Reader) (*SchemaFile, error) {
	var sf SchemaFile
	if err := json.NewDecoder(r).Decode(&sf); err != nil {
		return nil, fmt.Errorf("failed to decode schema file: %w", err)
	}
	return &sf, nil
}

// ReadManifestFile reads a data files manifest from the given reader.
func ReadManifestFile(r io.Reader) (*DataFilesManifest, error) {
	var mf DataFilesManifest
	if err := json.NewDecoder(r).Decode(&mf); err != nil {
		return nil, fmt.Errorf("failed to decode manifest file: %w", err)
	}
	return &mf, nil
}

// ParseIcebergSchema parses the Iceberg schema from a SchemaFile.
func (sf *SchemaFile) ParseIcebergSchema() (*iceberg.Schema, error) {
	var s iceberg.Schema
	if err := json.Unmarshal(sf.IcebergSchema, &s); err != nil {
		return nil, fmt.Errorf("failed to parse iceberg schema: %w", err)
	}
	return &s, nil
}

// SchemaToIceberg converts a gateway Schema to an Iceberg Schema.
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
			Required: !col.Nullable,
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
