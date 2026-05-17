package buffer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestCompressionString(t *testing.T) {
	tests := []struct {
		comp     Compression
		expected string
	}{
		{CompressionNone, "none"},
		{CompressionSnappy, "snappy"},
		{CompressionGzip, "gzip"},
		{CompressionZstd, "zstd"},
		{CompressionLz4, "lz4"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.comp.String(); got != tt.expected {
				t.Errorf("Compression.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDefaultParquetWriterConfig(t *testing.T) {
	config := DefaultParquetWriterConfig()

	if config.Compression != CompressionSnappy {
		t.Errorf("Default compression = %v, want Snappy", config.Compression)
	}
	if config.RowGroupSize != 128*1024*1024 {
		t.Errorf("Default RowGroupSize = %d, want 128MB", config.RowGroupSize)
	}
	if !config.DictionaryEnabled {
		t.Error("Default DictionaryEnabled should be true")
	}
	if !config.WriteStatistics {
		t.Error("Default WriteStatistics should be true")
	}
}

func TestStreamingParquetWriter(t *testing.T) {
	tmpDir := t.TempDir()

	// Create schema
	columns := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "value", Type: schema.TypeFloat64, Nullable: true},
	}
	s, _ := schema.NewSchema(columns)
	allocator := memory.NewGoAllocator()

	path := filepath.Join(tmpDir, "streaming.parquet")
	writer, err := NewStreamingParquetWriter(path, s.ArrowSchema(), nil, allocator, nil)
	if err != nil {
		t.Fatalf("NewStreamingParquetWriter() error: %v", err)
	}

	// Write multiple row groups
	for i := 0; i < 3; i++ {
		records := createTestRecords(allocator, s.ArrowSchema(), 50)
		if err := writer.WriteRowGroup(records); err != nil {
			t.Fatalf("WriteRowGroup() error: %v", err)
		}
		for _, rec := range records {
			rec.Release()
		}
	}

	// Check stats before close
	if writer.RowGroups() != 3 {
		t.Errorf("RowGroups() = %d, want 3", writer.RowGroups())
	}
	if writer.TotalRows() != 150 {
		t.Errorf("TotalRows() = %d, want 150", writer.TotalRows())
	}

	// Close writer
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Verify file
	verifyParquetFile(t, path, 150)
}

func TestStreamingParquetWriterClosed(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []schema.ColumnDef{{Name: "id", Type: schema.TypeInt64, Nullable: false}}
	s, _ := schema.NewSchema(columns)

	path := filepath.Join(tmpDir, "closed.parquet")
	writer, _ := NewStreamingParquetWriter(path, s.ArrowSchema(), nil, nil, nil)
	writer.Close()

	// Writing after close should error
	allocator := memory.NewGoAllocator()
	records := createTestRecords(allocator, s.ArrowSchema(), 10)
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	if err := writer.WriteRowGroup(records); err == nil {
		t.Error("WriteRowGroup() should error after close")
	}
}

// Helper functions

func createTestRecords(allocator memory.Allocator, arrowSchema *arrow.Schema, numRows int) []arrow.Record {
	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()

	for i := 0; i < numRows; i++ {
		for fieldIdx := 0; fieldIdx < arrowSchema.NumFields(); fieldIdx++ {
			field := arrowSchema.Field(fieldIdx)
			switch field.Type.ID() {
			case arrow.INT64:
				builder.Field(fieldIdx).(*array.Int64Builder).Append(int64(i))
			case arrow.FLOAT64:
				builder.Field(fieldIdx).(*array.Float64Builder).Append(float64(i) * 1.5)
			case arrow.STRING:
				builder.Field(fieldIdx).(*array.StringBuilder).Append("value_" + string(rune('A'+i%26)))
			}
		}
	}

	return []arrow.Record{builder.NewRecord()}
}

func verifyParquetFile(t *testing.T, path string, expectedRows int64) {
	t.Helper()

	// Check file exists
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("File not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("File is empty")
	}

	// Read the file using format adapter
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	adapter := format.NewParquetAdapter(nil, nil)
	record, _, err := adapter.Parse(data, nil)
	if err != nil {
		t.Fatalf("Failed to parse Parquet: %v", err)
	}
	defer record.Release()

	if record.NumRows() != expectedRows {
		t.Errorf("NumRows() = %d, want %d", record.NumRows(), expectedRows)
	}
}
