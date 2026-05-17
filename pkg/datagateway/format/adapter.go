// Package format provides adapters for converting between data formats and Arrow RecordBatches.
package format

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// DataFormat represents a supported data format.
type DataFormat int

const (
	FormatUnknown DataFormat = iota
	FormatCSV                // Comma-separated values
	FormatJSON               // JSON array: [{...}, {...}]
	FormatJSONL              // JSON Lines: {...}\n{...}\n
	FormatParquet            // Apache Parquet (read-only in adapters)
	FormatArrowIPC           // Arrow IPC format (future)
)

// String returns the string representation of the format.
func (f DataFormat) String() string {
	switch f {
	case FormatCSV:
		return "csv"
	case FormatJSON:
		return "json"
	case FormatJSONL:
		return "jsonl"
	case FormatParquet:
		return "parquet"
	case FormatArrowIPC:
		return "arrow"
	default:
		return "unknown"
	}
}

// ParseDataFormat parses a string into a DataFormat.
func ParseDataFormat(s string) DataFormat {
	switch s {
	case "csv", "CSV":
		return FormatCSV
	case "json", "JSON":
		return FormatJSON
	case "jsonl", "JSONL", "jsonlines", "ndjson", "NDJSON":
		return FormatJSONL
	case "parquet", "PARQUET":
		return FormatParquet
	case "arrow", "ARROW", "ipc", "IPC", "arrow_ipc", "ARROW_IPC":
		return FormatArrowIPC
	default:
		return FormatUnknown
	}
}

// MimeType returns the MIME type for the format.
func (f DataFormat) MimeType() string {
	switch f {
	case FormatCSV:
		return "text/csv"
	case FormatJSON:
		return "application/json"
	case FormatJSONL:
		return "application/x-ndjson"
	case FormatParquet:
		return "application/vnd.apache.parquet"
	case FormatArrowIPC:
		return "application/vnd.apache.arrow.stream"
	default:
		return "application/octet-stream"
	}
}

// Extension returns the file extension for the format (including dot).
func (f DataFormat) Extension() string {
	switch f {
	case FormatCSV:
		return ".csv"
	case FormatJSON:
		return ".json"
	case FormatJSONL:
		return ".jsonl"
	case FormatParquet:
		return ".parquet"
	case FormatArrowIPC:
		return ".arrow"
	default:
		return ""
	}
}

// FormatAdapter converts between a specific data format and Arrow RecordBatches.
type FormatAdapter interface {
	// Parse converts input bytes to an Arrow RecordBatch.
	// If schema is nil, the schema will be inferred from the data.
	// Returns the record, the schema used (may be inferred), and any error.
	// The caller is responsible for calling Release() on the returned record.
	Parse(data []byte, s *schema.Schema) (arrow.Record, *schema.Schema, error)

	// Serialize converts an Arrow RecordBatch to output bytes.
	Serialize(record arrow.Record) ([]byte, error)

	// Format returns the data format this adapter handles.
	Format() DataFormat
}

// ParseOptions configures parsing behavior.
type ParseOptions struct {
	// HasHeader indicates if the first row is a header (CSV only).
	// Default: true
	HasHeader bool

	// Delimiter is the field delimiter (CSV only).
	// Default: ','
	Delimiter rune

	// NullStrings are values treated as null.
	// If nil, uses default null strings from schema.InferenceConfig.
	NullStrings []string

	// InferenceConfig customizes type inference.
	// If nil, uses default configuration.
	InferenceConfig *schema.InferenceConfig
}

// DefaultParseOptions returns the default parse options.
func DefaultParseOptions() *ParseOptions {
	return &ParseOptions{
		HasHeader: true,
		Delimiter: ',',
	}
}

// SerializeOptions configures serialization behavior.
type SerializeOptions struct {
	// IncludeHeader adds a header row (CSV only).
	// Default: true
	IncludeHeader bool

	// Delimiter is the field delimiter (CSV only).
	// Default: ','
	Delimiter rune

	// Pretty enables indented output (JSON only).
	// Default: false
	Pretty bool

	// NullString is the string to use for null values.
	// Default: "" for CSV, "null" for JSON
	NullString string
}

// DefaultSerializeOptions returns the default serialize options.
func DefaultSerializeOptions() *SerializeOptions {
	return &SerializeOptions{
		IncludeHeader: true,
		Delimiter:     ',',
		Pretty:        false,
		NullString:    "",
	}
}

// Registry manages format adapters.
type Registry struct {
	adapters map[DataFormat]FormatAdapter
}

// NewRegistry creates a new adapter registry.
func NewRegistry() *Registry {
	return &Registry{
		adapters: make(map[DataFormat]FormatAdapter),
	}
}

// Register adds an adapter to the registry.
func (r *Registry) Register(adapter FormatAdapter) {
	r.adapters[adapter.Format()] = adapter
}

// Get returns the adapter for the given format.
func (r *Registry) Get(format DataFormat) (FormatAdapter, error) {
	adapter, ok := r.adapters[format]
	if !ok {
		return nil, fmt.Errorf("no adapter registered for format: %s", format)
	}
	return adapter, nil
}

// Formats returns all registered formats.
func (r *Registry) Formats() []DataFormat {
	formats := make([]DataFormat, 0, len(r.adapters))
	for f := range r.adapters {
		formats = append(formats, f)
	}
	return formats
}

// DefaultRegistry returns a registry with all built-in adapters registered.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register(NewCSVAdapter(nil, nil))
	r.Register(NewJSONAdapter(nil, nil))
	r.Register(NewJSONLAdapter(nil, nil))
	r.Register(NewArrowIPCAdapter(nil))
	r.Register(NewParquetAdapter(nil, nil))
	return r
}
