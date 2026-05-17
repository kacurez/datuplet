package format

import (
	"bytes"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// ArrowIPCAdapter converts between Arrow IPC format and Arrow RecordBatches.
// Arrow IPC is the native Arrow serialization format, providing zero-copy
// deserialization when possible.
//
// This adapter is useful for components written in Python (PyArrow) or Rust
// that can produce/consume Arrow data natively, avoiding the overhead of
// text format parsing.
type ArrowIPCAdapter struct {
	allocator memory.Allocator
}

// NewArrowIPCAdapter creates a new Arrow IPC adapter.
// If allocator is nil, uses the default Go allocator.
func NewArrowIPCAdapter(allocator memory.Allocator) *ArrowIPCAdapter {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	return &ArrowIPCAdapter{
		allocator: allocator,
	}
}

// Format returns FormatArrowIPC.
func (a *ArrowIPCAdapter) Format() DataFormat {
	return FormatArrowIPC
}

// Parse converts Arrow IPC bytes to an Arrow RecordBatch.
// The schema parameter is optional - if nil, the schema is read from the IPC stream.
// If schema is provided, it validates that the IPC schema matches.
func (a *ArrowIPCAdapter) Parse(data []byte, s *schema.Schema) (arrow.Record, *schema.Schema, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty Arrow IPC data")
	}

	reader, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(a.allocator))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer reader.Release()

	// Get schema from IPC stream
	ipcSchema := reader.Schema()

	// If schema provided, validate it matches
	if s != nil {
		if err := validateSchemaMatch(s.ArrowSchema(), ipcSchema); err != nil {
			return nil, nil, fmt.Errorf("schema mismatch: %w", err)
		}
	} else {
		// Convert IPC schema to our schema
		var err error
		s, err = schema.NewSchemaFromArrow(ipcSchema)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to convert Arrow schema: %w", err)
		}
	}

	// Read all records and concatenate them
	// Most IPC messages contain a single RecordBatch, but we handle multiple
	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain() // Retain before appending
		records = append(records, rec)
	}

	if err := reader.Err(); err != nil && err != io.EOF {
		for _, rec := range records {
			rec.Release()
		}
		return nil, nil, fmt.Errorf("error reading Arrow IPC: %w", err)
	}

	if len(records) == 0 {
		// Return an empty record with the schema
		return a.emptyRecord(s), s, nil
	}

	if len(records) == 1 {
		return records[0], s, nil
	}

	// Concatenate multiple records into one (reuse function from parquet.go)
	concatenated, err := concatenateRecords(records, a.allocator)
	// Release the original records
	for _, rec := range records {
		rec.Release()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to concatenate records: %w", err)
	}

	return concatenated, s, nil
}

// Serialize converts an Arrow RecordBatch to Arrow IPC bytes.
func (a *ArrowIPCAdapter) Serialize(record arrow.Record) ([]byte, error) {
	var buf bytes.Buffer

	writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()), ipc.WithAllocator(a.allocator))

	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("failed to write Arrow IPC: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close Arrow IPC writer: %w", err)
	}

	return buf.Bytes(), nil
}

// validateSchemaMatch checks if two Arrow schemas are compatible.
func validateSchemaMatch(expected, actual *arrow.Schema) error {
	if expected.NumFields() != actual.NumFields() {
		return fmt.Errorf("field count mismatch: expected %d, got %d",
			expected.NumFields(), actual.NumFields())
	}

	for i := 0; i < expected.NumFields(); i++ {
		expField := expected.Field(i)
		actField := actual.Field(i)

		if expField.Name != actField.Name {
			return fmt.Errorf("field %d name mismatch: expected %q, got %q",
				i, expField.Name, actField.Name)
		}

		if !arrow.TypeEqual(expField.Type, actField.Type) {
			return fmt.Errorf("field %q type mismatch: expected %v, got %v",
				expField.Name, expField.Type, actField.Type)
		}
	}

	return nil
}

// emptyRecord creates an empty record with the given schema.
func (a *ArrowIPCAdapter) emptyRecord(s *schema.Schema) arrow.Record {
	arrowSchema := s.ArrowSchema()
	builder := array.NewRecordBuilder(a.allocator, arrowSchema)
	defer builder.Release()
	return builder.NewRecord()
}
