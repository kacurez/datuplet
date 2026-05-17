package buffer

import (
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// joinStoragePath joins a base path with a filename in a URL-safe way.
// It handles both URL schemes (s3://, file://) and plain relative paths.
// This avoids filepath.Join corrupting URLs (e.g., s3://bucket → s3:/bucket).
func joinStoragePath(basePath, filename string) string {
	// Ensure basePath ends with / for proper concatenation
	if !strings.HasSuffix(basePath, "/") {
		basePath = basePath + "/"
	}
	return basePath + filename
}

// BufferConfig configures the buffer manager.
type BufferConfig struct {
	// BufferSize is the memory buffer size before flushing to a row group.
	// Default: 10MB
	BufferSize int64

	// RowGroupSize is the target size for Parquet row groups.
	// Default: same as BufferSize
	RowGroupSize int64

	// TargetFileSize is the target Parquet file size before rotation.
	// Default: 128MB
	TargetFileSize int64

	// OutputDir is the base directory for output files.
	OutputDir string

	// FilePrefix is the prefix for output files.
	// Default: "part"
	FilePrefix string

	// Compression is the Parquet compression codec.
	// Default: Snappy
	Compression Compression
}

// DefaultBufferConfig returns the default buffer configuration.
func DefaultBufferConfig() *BufferConfig {
	return &BufferConfig{
		BufferSize:     64 * 1024 * 1024,  // 64MB
		RowGroupSize:   64 * 1024 * 1024,  // 64MB (same as BufferSize)
		TargetFileSize: 128 * 1024 * 1024, // 128MB
		FilePrefix:     "part",
		Compression:    CompressionSnappy,
	}
}

// StreamingWriter is the common interface for streaming output writers.
type StreamingWriter interface {
	// WriteRowGroup writes a batch of records.
	WriteRowGroup(records []arrow.Record) error
	// BytesWritten returns the current bytes written.
	BytesWritten() int64
	// Close closes the writer.
	Close() error
	// Stats returns write statistics.
	Stats() *FlushStats
}

// FileInfo contains information about a written file.
type FileInfo struct {
	// Path is the relative path to the file.
	Path string
	// RowCount is the number of rows in this file.
	RowCount int64
	// SizeBytes is the file size in bytes.
	SizeBytes int64
}

// BufferManager manages in-memory buffering and streaming Parquet output.
// It implements two-level buffering:
//   - Level 1: Memory buffer (BufferSize) - flush to row group when full
//   - Level 2: File size (TargetFileSize) - rotate to new file when reached
type BufferManager struct {
	mu sync.Mutex

	config    *BufferConfig
	schema    *schema.Schema
	allocator memory.Allocator
	factory   WriterFactory

	// Buffered records
	batches []arrow.Record

	// Current state
	currentBufferSize int64
	currentFileSize   int64
	currentFileRows   int64 // Rows in the current file
	fileIndex         int
	currentFilePath   string // Path of current file being written

	// Streaming Parquet writer (kept open)
	writer StreamingWriter

	// Statistics
	totalRowsFlushed  int64
	totalBytesFlushed int64
	totalFiles        int

	// Files written (for manifest generation)
	filesWritten []FileInfo

	// Closed state
	closed bool
}

// NewBufferManager creates a new buffer manager.
func NewBufferManager(
	s *schema.Schema,
	config *BufferConfig,
	allocator memory.Allocator,
	factory WriterFactory,
) (*BufferManager, error) {
	if s == nil {
		return nil, fmt.Errorf("schema is required")
	}
	if config == nil {
		config = DefaultBufferConfig()
	}
	if config.OutputDir == "" {
		return nil, fmt.Errorf("output directory is required")
	}
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	if factory == nil {
		factory = &LocalWriterFactory{}
	}

	return &BufferManager{
		config:       config,
		schema:       s,
		allocator:    allocator,
		factory:      factory,
		batches:      make([]arrow.Record, 0),
		filesWritten: make([]FileInfo, 0),
	}, nil
}

// Add adds a record batch to the buffer.
// May trigger flush to row group and/or file rotation.
func (m *BufferManager) Add(record arrow.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("buffer manager is closed")
	}

	// Retain the record (we're holding onto it)
	record.Retain()
	m.batches = append(m.batches, record)
	m.currentBufferSize += estimateRecordSize(record)

	// Check if we need to flush buffer to row group
	if m.shouldFlushBuffer() {
		if err := m.flushRowGroup(); err != nil {
			return err
		}
	}

	// Check if we need to rotate to a new file
	if m.shouldRotateFile() {
		if err := m.rotateFile(); err != nil {
			return err
		}
	}

	return nil
}

// shouldFlushBuffer returns true if buffer should be flushed to a row group.
func (m *BufferManager) shouldFlushBuffer() bool {
	return m.currentBufferSize >= m.config.BufferSize
}

// shouldRotateFile returns true if we should rotate to a new file.
func (m *BufferManager) shouldRotateFile() bool {
	return m.writer != nil && m.currentFileSize >= m.config.TargetFileSize
}

// flushRowGroup flushes the current buffer to a row group.
func (m *BufferManager) flushRowGroup() error {
	if len(m.batches) == 0 {
		return nil
	}

	// Ensure we have a writer
	if m.writer == nil {
		if err := m.openNewFile(); err != nil {
			return err
		}
	}

	// Count rows in batches before writing
	var rowsInBatch int64
	for _, batch := range m.batches {
		rowsInBatch += batch.NumRows()
	}

	// Write the buffered records as a row group
	if err := m.writer.WriteRowGroup(m.batches); err != nil {
		return fmt.Errorf("failed to write row group: %w", err)
	}

	// Update file size and row count
	m.currentFileSize = m.writer.BytesWritten()
	m.currentFileRows += rowsInBatch

	// Release the records
	for _, batch := range m.batches {
		batch.Release()
	}

	// Reset buffer
	m.batches = m.batches[:0]
	m.currentBufferSize = 0

	return nil
}

// rotateFile closes the current file and opens a new one.
func (m *BufferManager) rotateFile() error {
	if m.writer != nil {
		// Close writer first (this flushes all buffered data for Parquet)
		if err := m.writer.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}

		// Get stats AFTER close to get accurate byte counts
		stats := m.writer.Stats()
		m.totalRowsFlushed += stats.RowsWritten
		m.totalBytesFlushed += stats.BytesWritten
		m.totalFiles++

		// Record file info for manifest
		if m.currentFilePath != "" {
			m.filesWritten = append(m.filesWritten, FileInfo{
				Path:      m.currentFilePath,
				RowCount:  m.currentFileRows,
				SizeBytes: stats.BytesWritten,
			})
		}

		m.writer = nil
	}

	m.currentFileSize = 0
	m.currentFileRows = 0
	m.currentFilePath = ""
	return nil
}

// openNewFile creates a new output file.
func (m *BufferManager) openNewFile() error {
	m.fileIndex++
	path := m.generateFilePath()
	m.currentFilePath = path
	m.currentFileRows = 0

	// Create Parquet writer
	rowGroupSize := m.config.RowGroupSize
	if rowGroupSize == 0 {
		rowGroupSize = m.config.BufferSize // Fall back to BufferSize
	}
	parquetConfig := &ParquetWriterConfig{
		Compression:       m.config.Compression,
		RowGroupSize:      rowGroupSize,
		DictionaryEnabled: true,
		WriteStatistics:   true,
	}

	var err error
	m.writer, err = NewStreamingParquetWriter(
		path,
		m.schema.ArrowSchema(),
		parquetConfig,
		m.allocator,
		m.factory,
	)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	m.currentFileSize = 0
	return nil
}

// generateFilePath generates the path for the next output file.
// Uses URL-safe path joining to support s3:// and other URL schemes.
func (m *BufferManager) generateFilePath() string {
	filename := fmt.Sprintf("%s-%05d.parquet", m.config.FilePrefix, m.fileIndex)
	return joinStoragePath(m.config.OutputDir, filename)
}

// Flush forces a flush of any buffered data.
func (m *BufferManager) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.flushRowGroup()
}

// Close flushes remaining data and closes the writer.
func (m *BufferManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	// Flush any remaining data
	if err := m.flushRowGroup(); err != nil {
		return err
	}

	// Close the current file
	if m.writer != nil {
		// Close writer first (this flushes all buffered data for Parquet)
		if err := m.writer.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}

		// Get stats AFTER close to get accurate byte counts
		stats := m.writer.Stats()
		m.totalRowsFlushed += stats.RowsWritten
		m.totalBytesFlushed += stats.BytesWritten
		m.totalFiles++

		// Record file info for manifest
		if m.currentFilePath != "" {
			m.filesWritten = append(m.filesWritten, FileInfo{
				Path:      m.currentFilePath,
				RowCount:  m.currentFileRows,
				SizeBytes: stats.BytesWritten,
			})
		}

		m.writer = nil
	}

	return nil
}

// Stats returns buffer manager statistics.
func (m *BufferManager) Stats() *BufferStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	return &BufferStats{
		BufferedRecords:   len(m.batches),
		BufferedBytes:     m.currentBufferSize,
		CurrentFileBytes:  m.currentFileSize,
		TotalRowsFlushed:  m.totalRowsFlushed,
		TotalBytesFlushed: m.totalBytesFlushed,
		TotalFiles:        m.totalFiles,
		CurrentFileIndex:  m.fileIndex,
	}
}

// FilesWritten returns the list of files written by the buffer manager.
// Should be called after Close() to get the complete list.
func (m *BufferManager) FilesWritten() []FileInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return a copy to prevent external mutation
	files := make([]FileInfo, len(m.filesWritten))
	copy(files, m.filesWritten)
	return files
}

// BufferStats contains buffer manager statistics.
type BufferStats struct {
	// BufferedRecords is the number of records currently in the buffer.
	BufferedRecords int

	// BufferedBytes is the estimated size of buffered data.
	BufferedBytes int64

	// CurrentFileBytes is the current file size.
	CurrentFileBytes int64

	// TotalRowsFlushed is the total rows written to files.
	TotalRowsFlushed int64

	// TotalBytesFlushed is the total bytes written to files.
	TotalBytesFlushed int64

	// TotalFiles is the number of files created.
	TotalFiles int

	// CurrentFileIndex is the current file index.
	CurrentFileIndex int
}

// estimateRecordSize estimates the memory size of a record batch.
func estimateRecordSize(record arrow.Record) int64 {
	var size int64
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		size += estimateArraySize(col)
	}
	return size
}

// estimateArraySize estimates the memory size of an Arrow array.
func estimateArraySize(arr arrow.Array) int64 {
	if arr == nil {
		return 0
	}

	var size int64

	// Add buffer sizes
	data := arr.Data()
	for _, buf := range data.Buffers() {
		if buf != nil {
			size += int64(buf.Len())
		}
	}

	return size
}

