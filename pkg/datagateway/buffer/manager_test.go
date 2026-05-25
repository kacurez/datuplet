package buffer

import (
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestDefaultBufferConfig(t *testing.T) {
	config := DefaultBufferConfig()

	// Defaults trace:
	//   - BufferSize / RowGroupSize: 64 MiB → 16 MiB (Phase 1) → 8 MiB
	//     (iter-buf8mib in 53e7d6b) to halve the open-row-group heap.
	//   - Compression: Snappy → Zstd (this commit) for ~30% smaller
	//     parquet files at acceptable encode-CPU cost.
	if config.BufferSize != 8*1024*1024 {
		t.Errorf("Default BufferSize = %d, want 8 MiB", config.BufferSize)
	}
	if config.RowGroupSize != 8*1024*1024 {
		t.Errorf("Default RowGroupSize = %d, want 8 MiB", config.RowGroupSize)
	}
	if config.TargetFileSize != 128*1024*1024 {
		t.Errorf("Default TargetFileSize = %d, want 128 MiB", config.TargetFileSize)
	}
	if config.FilePrefix != "part" {
		t.Errorf("Default FilePrefix = %q, want 'part'", config.FilePrefix)
	}
	if config.Compression != CompressionZstd {
		t.Errorf("Default Compression = %v, want Zstd", config.Compression)
	}
}

func TestNewBufferManagerErrors(t *testing.T) {
	columns := []schema.ColumnDef{{Name: "id", Type: schema.TypeInt64, Nullable: false}}
	s, _ := schema.NewSchema(columns)

	t.Run("NilSchema", func(t *testing.T) {
		_, err := NewBufferManager(nil, &BufferConfig{OutputDir: "/tmp"}, nil, nil)
		if err == nil {
			t.Error("Expected error for nil schema")
		}
	})

	t.Run("EmptyOutputDir", func(t *testing.T) {
		_, err := NewBufferManager(s, &BufferConfig{}, nil, nil)
		if err == nil {
			t.Error("Expected error for empty output directory")
		}
	})
}

func TestBufferManagerBasic(t *testing.T) {
	tmpDir := t.TempDir()

	// Create schema
	columns := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
	}
	s, _ := schema.NewSchema(columns)

	// Create config with small buffer size
	config := &BufferConfig{
		BufferSize:     1024, // 1KB - small for testing
		TargetFileSize: 10 * 1024 * 1024,
		OutputDir:      tmpDir,
		FilePrefix:     "test",
		Compression:    CompressionSnappy,
	}

	mgr, err := NewBufferManager(s, config, nil, nil)
	if err != nil {
		t.Fatalf("NewBufferManager() error: %v", err)
	}
	defer mgr.Close()

	// Add some records
	allocator := memory.NewGoAllocator()
	for i := 0; i < 10; i++ {
		record := createSingleRecord(allocator, s.ArrowSchema(), 100)
		if err := mgr.Add(record); err != nil {
			t.Fatalf("Add() error: %v", err)
		}
	}

	// Close to flush
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Check stats
	stats := mgr.Stats()
	if stats.TotalRowsFlushed != 1000 {
		t.Errorf("TotalRowsFlushed = %d, want 1000", stats.TotalRowsFlushed)
	}
	if stats.TotalFiles == 0 {
		t.Error("TotalFiles should be > 0")
	}

	// Verify files exist
	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.parquet"))
	if len(files) == 0 {
		t.Error("No Parquet files created")
	}
}

func TestBufferManagerFlush(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []schema.ColumnDef{{Name: "id", Type: schema.TypeInt64, Nullable: false}}
	s, _ := schema.NewSchema(columns)

	config := &BufferConfig{
		BufferSize:     1024 * 1024, // 1MB - won't auto-flush
		TargetFileSize: 128 * 1024 * 1024,
		OutputDir:      tmpDir,
		FilePrefix:     "flush",
	}

	mgr, _ := NewBufferManager(s, config, nil, nil)
	defer mgr.Close()

	// Add some records (won't auto-flush)
	allocator := memory.NewGoAllocator()
	record := createSingleRecord(allocator, s.ArrowSchema(), 10)
	mgr.Add(record)

	// Check buffer has data
	stats := mgr.Stats()
	if stats.BufferedRecords == 0 {
		t.Error("BufferedRecords should be > 0")
	}

	// Manual flush
	if err := mgr.Flush(); err != nil {
		t.Fatalf("Flush() error: %v", err)
	}

	// Buffer should be empty after flush
	stats = mgr.Stats()
	if stats.BufferedRecords != 0 {
		t.Errorf("BufferedRecords = %d after flush, want 0", stats.BufferedRecords)
	}
}

func TestBufferManagerFileRotation(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []schema.ColumnDef{{Name: "id", Type: schema.TypeInt64, Nullable: false}}
	s, _ := schema.NewSchema(columns)

	// Use very small sizes to force rotation
	config := &BufferConfig{
		BufferSize:     100,  // 100 bytes
		TargetFileSize: 1000, // 1KB
		OutputDir:      tmpDir,
		FilePrefix:     "rotate",
	}

	mgr, _ := NewBufferManager(s, config, nil, nil)

	// Add many records to trigger rotation
	allocator := memory.NewGoAllocator()
	for i := 0; i < 100; i++ {
		record := createSingleRecord(allocator, s.ArrowSchema(), 100)
		if err := mgr.Add(record); err != nil {
			t.Fatalf("Add() error at iteration %d: %v", i, err)
		}
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Should have multiple files
	files, _ := filepath.Glob(filepath.Join(tmpDir, "rotate-*.parquet"))
	if len(files) < 2 {
		t.Errorf("Expected multiple files, got %d", len(files))
	}
}

func TestBufferManagerClosed(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []schema.ColumnDef{{Name: "id", Type: schema.TypeInt64, Nullable: false}}
	s, _ := schema.NewSchema(columns)

	config := &BufferConfig{
		OutputDir: tmpDir,
	}

	mgr, _ := NewBufferManager(s, config, nil, nil)
	mgr.Close()

	// Adding after close should error
	allocator := memory.NewGoAllocator()
	record := createSingleRecord(allocator, s.ArrowSchema(), 10)
	if err := mgr.Add(record); err == nil {
		t.Error("Add() should error after Close()")
	}
	record.Release()
}

func TestEstimateRecordSize(t *testing.T) {
	allocator := memory.NewGoAllocator()

	columns := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
	}
	s, _ := schema.NewSchema(columns)

	record := createSingleRecord(allocator, s.ArrowSchema(), 100)
	defer record.Release()

	size := estimateRecordSize(record)
	if size <= 0 {
		t.Error("estimateRecordSize() should return > 0")
	}
}

// Helper function
func createSingleRecord(allocator memory.Allocator, arrowSchema *arrow.Schema, numRows int) arrow.Record {
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
				builder.Field(fieldIdx).(*array.StringBuilder).Append("test_value")
			}
		}
	}

	return builder.NewRecord()
}
