package buffer

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// asyncCloseQueueDepth controls how many parquet writers can be in
// "pending close" state at once when file rotation hands off the dying
// writer to a background goroutine. Each pending close still holds the
// underlying asyncWriter's bounded byte buffers (≈80 MiB by default at
// queueCap=4 × bufSize=16 MiB), so the cap is also a memory ceiling.
//
// Default 2 → at most 2 × 80 MiB = 160 MiB extra peak per active table
// writer, on top of the active writer's own 80 MiB. Larger queue gives
// the producer more headroom between rotations at proportional memory
// cost. Operators override via DATUPLET_GATEWAY_ASYNC_ROTATE_QUEUE.
//
// Set to 0 (or DATUPLET_GATEWAY_ASYNC_ROTATE=0) to disable async rotation
// entirely and fall back to the pre-iter-2 synchronous close path.
const defaultAsyncCloseQueueDepth = 2

func asyncRotateQueueDepth() int {
	if v := os.Getenv("DATUPLET_GATEWAY_ASYNC_ROTATE"); v == "0" || v == "false" {
		return 0
	}
	if v := os.Getenv("DATUPLET_GATEWAY_ASYNC_ROTATE_QUEUE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 16 {
			return n
		}
	}
	return defaultAsyncCloseQueueDepth
}

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
//
// BufferSize is the in-memory cap for retained Arrow records before a
// row-group flush. Lowered from 16 MiB to 8 MiB to bound peak heap in
// the gateway sidecar: combined with the streaming-upload path
// (StorageBackend.OpenObjectWriter), this caps the row group buffer
// AND the parquet writer's open-row-group memory at ~8 MiB instead
// of ~16 MiB. Operators with looser memory can override via
// Config.BufferSize.
//
// RowGroupSize stays in sync with BufferSize so the on-disk row group
// matches the in-memory flush boundary.
//
// TargetFileSize stays at 128 MiB; the per-file backend writer is now
// streaming so file size no longer dominates heap usage.
func DefaultBufferConfig() *BufferConfig {
	return &BufferConfig{
		BufferSize:     8 * 1024 * 1024,   // 8 MiB (was 16; lowered for memory bound)
		RowGroupSize:   8 * 1024 * 1024,   // 8 MiB (matches BufferSize)
		TargetFileSize: 128 * 1024 * 1024, // 128 MiB
		FilePrefix:     "part",
		// Default codec is Zstd (level inherited from arrow-go's
		// pqarrow default = ZSTD level 1). Snappy was the prior
		// default; Pyroscope on run 156b0c19 showed it eating ~40%
		// of gateway CPU during writes (s2.encodeBlockBetterSnappy).
		//
		// Trade-off ZSTD-1 vs Snappy:
		//   - Compression ratio: ZSTD-1 is ~30-40% better on the
		//     parquet column data we typically write. Smaller files
		//     = lower GCS storage cost AND faster downstream reads
		//     (pqarrow read throughput is bytes-bound through the
		//     decompression pipeline).
		//   - Encode CPU: ZSTD-1 is ~1.5-2x slower than Snappy per
		//     byte. On our most recent benchmark gateway CPU was
		//     only ~4.5 s for a 74 s wallclock — even doubling the
		//     compression CPU adds < 5 s, lost in the I/O
		//     overlap with the streaming upload.
		//   - Decode CPU: ZSTD is comparable to or faster than
		//     Snappy in arrow-go's read path.
		//
		// Pipelines that want the prior Snappy behaviour (or LZ4 for
		// even lower encode CPU at the cost of weaker ratio) override
		// via BufferConfig.Compression at construction time.
		Compression: CompressionZstd,
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

	// Coalescing accumulator. Small (< coalesceFastPathRows) records are
	// copied row-by-row into acc instead of being retained as individual
	// arrow.Records. acc finalizes into batches when accRows or accBytes
	// crosses its threshold, or when Flush/Close runs.
	acc      *array.RecordBuilder
	accRows  int64
	accBytes int64

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

	// Async file-rotation pipeline. When queueDepth > 0, rotateFile()
	// hands the dying writer to closeQueue and immediately returns;
	// a single background worker drains the queue, calls Close()+Stats()
	// on each writer, and appends to filesWritten under m.mu. Producer
	// blocks on a full queue — that's the natural memory back-pressure.
	//
	// closeWG tracks the worker goroutine for BufferManager.Close()
	// to wait on before returning.
	closeQueue   chan pendingClose
	closeWG      sync.WaitGroup
	closeErrOnce sync.Once
	closeErr     error // first error from background close, surfaced via Close()
}

// pendingClose carries the state a background closer needs to finalise
// a parquet file and record it in filesWritten.
type pendingClose struct {
	writer   StreamingWriter
	filePath string
	fileRows int64
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

	m := &BufferManager{
		config:       config,
		schema:       s,
		allocator:    allocator,
		factory:      factory,
		batches:      make([]arrow.Record, 0),
		filesWritten: make([]FileInfo, 0),
	}

	// Spin up the async-rotate close worker iff enabled. queueDepth==0
	// keeps the legacy synchronous close path inside rotateFile().
	//
	// We pass the channel by value to closeWorker so its range loop
	// captures the channel reference at goroutine creation, immune to
	// later field reassignments / nils in Close().
	if depth := asyncRotateQueueDepth(); depth > 0 {
		m.closeQueue = make(chan pendingClose, depth)
		m.closeWG.Add(1)
		go m.closeWorker(m.closeQueue)
	}
	return m, nil
}

// closeWorker drains pendingClose items from queue, calls Close() +
// Stats() on each writer, and updates filesWritten / totalFiles
// counters under m.mu. The worker exits when the channel is closed.
//
// Errors from Close are sticky: the first one is recorded in m.closeErr
// (via closeErrOnce) and surfaced by BufferManager.Close. Subsequent
// closes still run so we don't leak resumable-upload state.
func (m *BufferManager) closeWorker(queue chan pendingClose) {
	defer m.closeWG.Done()
	for pc := range queue {
		if err := pc.writer.Close(); err != nil {
			m.closeErrOnce.Do(func() {
				m.closeErr = fmt.Errorf("failed to close file: %w", err)
			})
			continue
		}
		stats := pc.writer.Stats()
		m.mu.Lock()
		m.totalRowsFlushed += stats.RowsWritten
		m.totalBytesFlushed += stats.BytesWritten
		m.totalFiles++
		if pc.filePath != "" {
			m.filesWritten = append(m.filesWritten, FileInfo{
				Path:      pc.filePath,
				RowCount:  pc.fileRows,
				SizeBytes: stats.BytesWritten,
			})
		}
		m.mu.Unlock()
	}
}

// finalizeWriterSync runs the synchronous close path: blocks on
// writer.Close(), reads Stats(), and appends to filesWritten under the
// caller-held m.mu. Used when async rotation is disabled OR when the
// final writer is being closed in BufferManager.Close() after the queue
// has already been drained.
func (m *BufferManager) finalizeWriterSync(w StreamingWriter, filePath string, fileRows int64) error {
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	stats := w.Stats()
	m.totalRowsFlushed += stats.RowsWritten
	m.totalBytesFlushed += stats.BytesWritten
	m.totalFiles++
	if filePath != "" {
		m.filesWritten = append(m.filesWritten, FileInfo{
			Path:      filePath,
			RowCount:  fileRows,
			SizeBytes: stats.BytesWritten,
		})
	}
	return nil
}

// Add adds a record batch to the buffer.
// May trigger flush to row group and/or file rotation.
//
// Records with at least coalesceFastPathRows rows are appended directly
// (the caller already batched). Smaller records are copied into the
// per-manager accumulator so per-Record Arrow scaffolding doesn't pin
// multiple kilobytes of heap per row.
func (m *BufferManager) Add(record arrow.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("buffer manager is closed")
	}

	// Reject schema-incompatible records cleanly rather than panicking
	// deep in the column copy. Field-id metadata differences are fine —
	// only structural type equality matters.
	if !schemasCompatible(record.Schema(), m.schema.ArrowSchema()) {
		return fmt.Errorf("record schema does not match buffer manager schema")
	}

	if record.NumRows() >= coalesceFastPathRows {
		// Fast path: already-batched record skips the row-by-row copy.
		// Flush the accumulator first to preserve append order.
		if err := m.flushAccumulator(); err != nil {
			return err
		}
		record.Retain()
		m.batches = append(m.batches, record)
		m.currentBufferSize += estimateRecordSize(record)
	} else {
		// Slow path: small record. Copy rows into the in-flight builder.
		if m.acc == nil {
			m.acc = array.NewRecordBuilder(m.allocator, m.schema.ArrowSchema())
		}
		if err := appendRecordToBuilder(m.acc, record); err != nil {
			return fmt.Errorf("coalesce append: %w", err)
		}
		m.accRows += record.NumRows()
		m.accBytes += estimateRecordPayloadBytes(record)
		if m.accRows >= coalesceFlushRows || m.accBytes >= coalesceFlushBytes {
			if err := m.flushAccumulator(); err != nil {
				return err
			}
		}
	}

	if m.shouldFlushBuffer() {
		if err := m.flushRowGroup(); err != nil {
			return err
		}
	}

	if m.shouldRotateFile() {
		if err := m.rotateFile(); err != nil {
			return err
		}
	}

	return nil
}

// flushAccumulator finalizes the in-flight builder into a single
// arrow.Record and appends it to batches. No-op if the accumulator is
// empty. Caller must hold m.mu.
func (m *BufferManager) flushAccumulator() error {
	if m.acc == nil || m.accRows == 0 {
		return nil
	}
	rec := m.acc.NewRecord()
	m.batches = append(m.batches, rec)
	m.currentBufferSize += estimateRecordSize(rec)
	m.accRows = 0
	m.accBytes = 0
	return nil
}

// shouldFlushBuffer returns true if buffer should be flushed to a row group.
// Triggers on the configured byte size OR on len(batches) crossing the
// defensive hard cap — the latter guards against estimator undercounts.
func (m *BufferManager) shouldFlushBuffer() bool {
	return m.currentBufferSize >= m.config.BufferSize || len(m.batches) >= maxBufferedBatches
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

	// Pre-flush debug log: confirms when row groups land and what they
	// cost. Gated by DATUPLET_GATEWAY_DEBUG.
	debugf("flushRowGroup: batches=%d rows=%d est_bytes=%d file=%s",
		len(m.batches), rowsInBatch, m.currentBufferSize, m.currentFilePath)

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
//
// When async rotation is enabled (m.closeQueue != nil), the dying
// writer is handed off to the background closeWorker and the foreground
// returns immediately. The producer briefly drops m.mu while sending
// on the channel — safe because m.closed/m.writer state is already
// reset, and other Add()/Flush()/Close() callers serialize on m.mu.
//
// Caller must hold m.mu on entry. m.mu is held on return (briefly
// released during the channel send when async).
func (m *BufferManager) rotateFile() error {
	if m.writer != nil {
		if m.closeQueue != nil {
			// Async path: hand off, immediately reset state.
			pc := pendingClose{
				writer:   m.writer,
				filePath: m.currentFilePath,
				fileRows: m.currentFileRows,
			}
			m.writer = nil
			// Surface any prior background-close error before queuing
			// more work. Keeps the failure visible quickly.
			if m.closeErr != nil {
				return m.closeErr
			}
			m.mu.Unlock()
			m.closeQueue <- pc
			m.mu.Lock()
		} else {
			// Sync path (legacy): close in foreground.
			if err := m.finalizeWriterSync(m.writer, m.currentFilePath, m.currentFileRows); err != nil {
				return err
			}
			m.writer = nil
		}
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
	debugf("openNewFile: index=%d path=%s", m.fileIndex, path)

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

	if err := m.flushAccumulator(); err != nil {
		return err
	}
	return m.flushRowGroup()
}

// Close flushes remaining data and closes the writer.
//
// With async rotation enabled, the final writer is handed to the
// closeQueue alongside any prior pending closes, then the queue is
// closed and we wait for the worker goroutine to drain everything. The
// returned error is the first close error encountered (foreground or
// background).
func (m *BufferManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	// Drain accumulator first so its rows land in the final flush.
	if err := m.flushAccumulator(); err != nil {
		return err
	}
	if m.acc != nil {
		m.acc.Release()
		m.acc = nil
	}

	// Flush any remaining data into the active writer (creates one if
	// none is open yet and there is buffered data).
	if err := m.flushRowGroup(); err != nil {
		return err
	}

	if m.writer == nil {
		// Nothing to close on the producer side; still drain the queue.
		if m.closeQueue != nil {
			close(m.closeQueue)
			m.closeQueue = nil
			m.mu.Unlock()
			m.closeWG.Wait()
			m.mu.Lock()
			if m.closeErr != nil {
				return m.closeErr
			}
		}
		return nil
	}

	if m.closeQueue != nil {
		// Async path: enqueue the final writer, close the channel so
		// the worker exits after draining, wait. We must release the
		// lock during the send AND during the Wait so the worker can
		// take m.mu inside its loop without deadlocking.
		pc := pendingClose{
			writer:   m.writer,
			filePath: m.currentFilePath,
			fileRows: m.currentFileRows,
		}
		m.writer = nil
		queue := m.closeQueue
		m.closeQueue = nil
		m.mu.Unlock()
		queue <- pc
		close(queue)
		m.closeWG.Wait()
		m.mu.Lock()
		if m.closeErr != nil {
			return m.closeErr
		}
		return nil
	}

	// Sync legacy path.
	if err := m.finalizeWriterSync(m.writer, m.currentFilePath, m.currentFileRows); err != nil {
		return err
	}
	m.writer = nil
	return nil
}

// Stats returns buffer manager statistics.
func (m *BufferManager) Stats() *BufferStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	// BufferedRecords/Bytes include the in-flight accumulator so callers
	// observing buffer state don't see a phantom "zero" between Adds.
	accContribRecords := 0
	if m.accRows > 0 {
		accContribRecords = 1
	}
	return &BufferStats{
		BufferedRecords:   len(m.batches) + accContribRecords,
		BufferedBytes:     m.currentBufferSize + m.accBytes,
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

// estimateRecordSize estimates the heap memory pinned by retaining a
// record batch. For records with many rows the per-column buffer bytes
// dominate. For tiny records (≤ 8 rows) per-Record Arrow scaffolding
// (schema ref, ArrayData headers, minimum buffer allocations) is the
// dominant cost; the membench measured ~4 KiB per single-row record on a
// 5-column schema. Adding that overhead here keeps BufferSize honest as
// a hard ceiling: defense in depth alongside the coalescer in Add.
func estimateRecordSize(record arrow.Record) int64 {
	var bufBytes int64
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		bufBytes += estimateArraySize(col)
	}
	if record.NumRows() <= 8 {
		// Conservative per-Record overhead: 2 KiB fixed + 256 B per column.
		// Tuned against the measured ~4 KiB at 5 columns; the constants
		// over-estimate slightly on purpose so the cap fires defensively.
		bufBytes += 2048 + 256*int64(record.NumCols())
	}
	return bufBytes
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

