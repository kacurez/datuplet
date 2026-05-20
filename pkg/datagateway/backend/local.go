package backend

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// LocalConfig configures the local filesystem backend.
type LocalConfig struct {
	// DataDir is the base directory for all data.
	DataDir string

	// InputFormat specifies input format detection ("auto", "csv", "parquet", "json").
	InputFormat string

	// OutputFormat specifies output format ("csv", "parquet", "json").
	OutputFormat string

	// ChunkSize is the size of chunks to read/write.
	ChunkSize int64
}

// LocalBackend implements StorageBackend for local filesystem.
// This is used for local development without MinIO/S3.
type LocalBackend struct {
	config LocalConfig
}

// toLocalPath converts a storage path (which may be a URL) to a local filesystem path.
// This handles file://, s3://, and plain relative paths.
// For s3:// URLs, it extracts the path component (for testing scenarios where
// storage paths flow through without modification).
func (b *LocalBackend) toLocalPath(storagePath string) string {
	// Handle file:// prefix
	if strings.HasPrefix(storagePath, "file://") {
		return strings.TrimPrefix(storagePath, "file://")
	}
	// Handle s3:// prefix (extract path for local testing)
	if strings.HasPrefix(storagePath, "s3://") {
		// Extract path after bucket: s3://bucket/path → path
		withoutScheme := strings.TrimPrefix(storagePath, "s3://")
		parts := strings.SplitN(withoutScheme, "/", 2)
		if len(parts) == 2 {
			return parts[1]
		}
		return ""
	}
	// Already a local path
	return storagePath
}

// NewLocalBackend creates a new local filesystem backend.
func NewLocalBackend(cfg LocalConfig) *LocalBackend {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 10 * 1024 * 1024 // 10MB default
	}
	if cfg.InputFormat == "" {
		cfg.InputFormat = "auto"
	}
	if cfg.OutputFormat == "" {
		cfg.OutputFormat = "csv" // CSV for simplicity in dev mode
	}
	return &LocalBackend{config: cfg}
}

func (b *LocalBackend) OpenReader(ctx context.Context, tablePath string) (Reader, error) {
	fullPath := filepath.Join(b.config.DataDir, tablePath)

	// Find data files in the directory
	files, err := b.findDataFiles(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to find data files in %s: %w", fullPath, err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no data files found in %s", fullPath)
	}

	// Detect format from first file
	format := b.detectFormat(files[0])

	return &localReader{
		files:     files,
		format:    format,
		chunkSize: b.config.ChunkSize,
	}, nil
}

// OpenStreamingArrowReader opens a row-group-streaming arrow reader over a list
// of parquet files. Used by DG's OpenReader(FORMAT_ARROW_IPC) path. `currentSchema`
// is the lakekeeper-current iceberg schema; per-file projection happens in the reader.
func (b *LocalBackend) OpenStreamingArrowReader(ctx context.Context, filePaths []string, currentSchema *SchemaInfo) (Reader, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no files provided")
	}
	localFiles := make([]string, len(filePaths))
	for i, fp := range filePaths {
		localFiles[i] = b.toLocalPath(fp)
	}
	return NewParquetArrowReader(ctx, localFiles, currentSchema)
}

// OpenReaderForFiles creates a reader for a specific list of data files.
// File paths can be file:// URLs, s3:// URLs (for testing), or relative paths.
func (b *LocalBackend) OpenReaderForFiles(ctx context.Context, filePaths []string) (Reader, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no files provided")
	}

	// Convert file paths to local paths
	localFiles := make([]string, len(filePaths))
	for i, fp := range filePaths {
		localFiles[i] = b.toLocalPath(fp)
	}

	// Detect format from first file
	format := b.detectFormat(localFiles[0])

	return &localReader{
		files:     localFiles,
		format:    format,
		chunkSize: b.config.ChunkSize,
	}, nil
}

func (b *LocalBackend) OpenWriter(ctx context.Context, tablePath string, opts WriteOptions) (Writer, error) {
	fullPath := filepath.Join(b.config.DataDir, tablePath)

	// Create output directory
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory %s: %w", fullPath, err)
	}

	format := opts.Format
	if format == "" {
		format = b.config.OutputFormat
	}

	return &localWriter{
		dir:        fullPath,
		outputName: opts.OutputName,
		tablePath:  tablePath,
		format:     format,
		compress:   opts.Compress,
	}, nil
}

func (b *LocalBackend) Commit(ctx context.Context, writers []Writer) (*CommitResult, error) {
	// Local mode: files are already written, nothing special to commit
	results := make([]TableCommitResult, len(writers))
	for i, w := range writers {
		stats := w.Stats()
		results[i] = TableCommitResult{
			OutputName: w.OutputName(),
			TablePath:  w.TablePath(),
			Status:     CommitStatusCommitted,
			SnapshotID: 0, // No Iceberg
			FilesAdded: stats.PartsWritten,
			RowsAdded:  stats.RowsWritten,
		}
	}
	return &CommitResult{Tables: results}, nil
}

func (b *LocalBackend) Rollback(ctx context.Context, writers []Writer) error {
	for _, w := range writers {
		lw, ok := w.(*localWriter)
		if !ok {
			continue
		}
		// Delete output files
		pattern := filepath.Join(lw.dir, "part-*")
		files, _ := filepath.Glob(pattern)
		for _, f := range files {
			os.Remove(f)
		}
	}
	return nil
}

func (b *LocalBackend) GetSchema(ctx context.Context, tablePath string) (*SchemaInfo, error) {
	fullPath := filepath.Join(b.config.DataDir, tablePath)

	files, err := b.findDataFiles(fullPath)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no data files found in %s", fullPath)
	}

	format := b.detectFormat(files[0])

	switch format {
	case "csv":
		return b.getCSVSchema(files[0])
	case "parquet":
		return b.getParquetSchema(files[0])
	default:
		return nil, fmt.Errorf("schema detection not supported for format: %s", format)
	}
}

// getParquetSchema reads the schema from a parquet file using pqarrow.
func (b *LocalBackend) getParquetSchema(path string) (*SchemaInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	pr, err := file.NewParquetReader(&seekableReaderAt{ra: f, size: info.Size()})
	if err != nil {
		return nil, fmt.Errorf("parquet open %s: %w", path, err)
	}
	defer pr.Close()
	fr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		return nil, fmt.Errorf("pqarrow reader %s: %w", path, err)
	}
	arrowSch, err := fr.Schema()
	if err != nil {
		return nil, fmt.Errorf("read arrow schema %s: %w", path, err)
	}
	cols := make([]ColumnInfo, arrowSch.NumFields())
	for i, f := range arrowSch.Fields() {
		typeName := f.Type.Name()
		// Normalise a few common Arrow type names to the gateway's type strings.
		switch typeName {
		case "int32":
			typeName = "int32"
		case "int64":
			typeName = "int64"
		case "float":
			typeName = "float32"
		case "double":
			typeName = "float64"
		case "utf8":
			typeName = "string"
		case "bool":
			typeName = "boolean"
		case "date32":
			typeName = "date"
		}
		cols[i] = ColumnInfo{Name: f.Name, Type: typeName, Nullable: f.Nullable}
	}
	return &SchemaInfo{Columns: cols}, nil
}

func (b *LocalBackend) GetSample(ctx context.Context, tablePath string, limit int) (*SampleResult, error) {
	fullPath := filepath.Join(b.config.DataDir, tablePath)

	files, err := b.findDataFiles(fullPath)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no data files found in %s", fullPath)
	}

	format := b.detectFormat(files[0])

	switch format {
	case "csv":
		return b.getCSVSample(files[0], limit)
	default:
		return nil, fmt.Errorf("sampling not supported for format: %s", format)
	}
}

func (b *LocalBackend) GetObject(ctx context.Context, storagePath string) ([]byte, error) {
	// Convert storage path (may be URL) to local path
	localPath := b.toLocalPath(storagePath)

	// Create full path
	fullPath := filepath.Join(b.config.DataDir, localPath)

	// Read the file
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", fullPath, err)
	}

	return data, nil
}

func (b *LocalBackend) PutObject(ctx context.Context, storagePath string, data []byte) error {
	// Convert storage path (may be URL) to local path
	localPath := b.toLocalPath(storagePath)

	// Create full path
	fullPath := filepath.Join(b.config.DataDir, localPath)

	// Create directory if it doesn't exist
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write the file
	if err := os.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", fullPath, err)
	}

	return nil
}

// OpenObjectWriter opens a streaming writer for the given storage path.
// The returned writer is an os.File; bytes flow directly to disk via the OS
// page cache — no in-process buffering of the full payload. Detected via
// the optional objectStreamingBackend interface in pkg/datagateway/buffer
// and used by BufferManager to avoid materializing whole parquet files.
func (b *LocalBackend) OpenObjectWriter(ctx context.Context, storagePath string) (io.WriteCloser, error) {
	localPath := b.toLocalPath(storagePath)
	fullPath := filepath.Join(b.config.DataDir, localPath)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	f, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}
	return f, nil
}

func (b *LocalBackend) RemoveAll(ctx context.Context, prefix string) error {
	// Strip any leading slash so filepath.Join doesn't treat it as absolute.
	cleanPrefix := filepath.Clean(strings.TrimPrefix(prefix, "/"))

	// Resolve against the backend root.
	root := filepath.Clean(b.config.DataDir)
	resolved := filepath.Join(root, cleanPrefix)

	// Containment check: resolved path must be inside root.
	// filepath.Clean already removed any ".." components; we just
	// verify the resolved path starts with root + separator to prevent
	// root-escape (e.g. prefix "../../etc").
	if resolved != root && !strings.HasPrefix(resolved, root+string(filepath.Separator)) {
		return fmt.Errorf("RemoveAll: prefix %q resolves outside backend root", prefix)
	}

	if err := os.RemoveAll(resolved); err != nil {
		return fmt.Errorf("RemoveAll %q: %w", prefix, err)
	}
	return nil
}

func (b *LocalBackend) Close() error {
	return nil
}

func (b *LocalBackend) findDataFiles(dir string) ([]string, error) {
	// Check if it's a file directly
	info, err := os.Stat(dir)
	if err == nil && !info.IsDir() {
		return []string{dir}, nil
	}

	// Look for data files in the directory
	var files []string
	extensions := []string{".csv", ".parquet", ".json", ".jsonl"}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		for _, ext := range extensions {
			if strings.HasSuffix(strings.ToLower(name), ext) {
				files = append(files, filepath.Join(dir, name))
				break
			}
		}
	}

	return files, nil
}

func (b *LocalBackend) detectFormat(path string) string {
	if b.config.InputFormat != "auto" {
		return b.config.InputFormat
	}

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv":
		return "csv"
	case ".parquet":
		return "parquet"
	case ".json", ".jsonl":
		return "json"
	default:
		return "csv" // Default to CSV
	}
}

func (b *LocalBackend) getCSVSchema(path string) (*SchemaInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	columns := make([]ColumnInfo, len(headers))
	for i, h := range headers {
		columns[i] = ColumnInfo{
			Name:     h,
			Type:     "string", // CSV doesn't have type info
			Nullable: true,
		}
	}

	return &SchemaInfo{Columns: columns}, nil
}

func (b *LocalBackend) getCSVSample(path string, limit int) (*SampleResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	columns := make([]ColumnInfo, len(headers))
	for i, h := range headers {
		columns[i] = ColumnInfo{
			Name:     h,
			Type:     "string",
			Nullable: true,
		}
	}

	var rows [][]byte
	for i := 0; i < limit; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Convert to map
		row := make(map[string]string)
		for j, h := range headers {
			if j < len(record) {
				row[h] = record[j]
			}
		}

		jsonRow, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		rows = append(rows, jsonRow)
	}

	return &SampleResult{
		Schema:        &SchemaInfo{Columns: columns},
		Rows:          rows,
		TotalEstimate: -1, // Unknown
	}, nil
}

// localReader implements Reader for local files.
type localReader struct {
	files       []string
	format      string
	chunkSize   int64
	currentFile int
	file        *os.File
	csvReader   *csv.Reader
	headers     []string
	schema      *SchemaInfo
	totalSize   int64
	bytesRead   int64
	initialized bool
}

func (r *localReader) ReadChunk() (*DataChunk, error) {
	if !r.initialized {
		if err := r.init(); err != nil {
			return nil, err
		}
	}

	switch r.format {
	case "csv":
		return r.readCSVChunk()
	default:
		return r.readRawChunk()
	}
}

func (r *localReader) init() error {
	// Calculate total size
	for _, f := range r.files {
		info, err := os.Stat(f)
		if err == nil {
			r.totalSize += info.Size()
		}
	}

	// Open first file
	if err := r.openNextFile(); err != nil {
		return err
	}

	r.initialized = true
	return nil
}

func (r *localReader) openNextFile() error {
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}

	if r.currentFile >= len(r.files) {
		return io.EOF
	}

	f, err := os.Open(r.files[r.currentFile])
	if err != nil {
		return err
	}
	r.file = f
	r.currentFile++

	if r.format == "csv" {
		r.csvReader = csv.NewReader(f)
		// Read headers
		headers, err := r.csvReader.Read()
		if err != nil {
			return err
		}
		r.headers = headers

		columns := make([]ColumnInfo, len(headers))
		for i, h := range headers {
			columns[i] = ColumnInfo{Name: h, Type: "string", Nullable: true}
		}
		r.schema = &SchemaInfo{Columns: columns}
	}

	return nil
}

func (r *localReader) readCSVChunk() (*DataChunk, error) {
	var buf strings.Builder
	var rowCount int64

	// Write headers
	writer := csv.NewWriter(&buf)
	writer.Write(r.headers)

	// Read rows until chunk size reached
	for buf.Len() < int(r.chunkSize) {
		record, err := r.csvReader.Read()
		if err == io.EOF {
			// Try next file
			if err := r.openNextFile(); err == io.EOF {
				// No more files
				if rowCount == 0 {
					return nil, io.EOF
				}
				writer.Flush()
				return &DataChunk{
					Data:        []byte(buf.String()),
					Format:      "csv",
					IsLast:      true,
					RowsInChunk: rowCount,
				}, nil
			} else if err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}

		writer.Write(record)
		rowCount++
	}

	writer.Flush()
	return &DataChunk{
		Data:        []byte(buf.String()),
		Format:      "csv",
		IsLast:      false,
		RowsInChunk: rowCount,
	}, nil
}

func (r *localReader) readRawChunk() (*DataChunk, error) {
	buf := make([]byte, r.chunkSize)
	n, err := r.file.Read(buf)

	if err == io.EOF {
		// Try next file
		if err := r.openNextFile(); err == io.EOF {
			return nil, io.EOF
		}
		return r.readRawChunk()
	}
	if err != nil {
		return nil, err
	}

	r.bytesRead += int64(n)
	isLast := r.bytesRead >= r.totalSize

	return &DataChunk{
		Data:   buf[:n],
		Format: r.format,
		IsLast: isLast,
	}, nil
}

func (r *localReader) Schema() *SchemaInfo {
	return r.schema
}

func (r *localReader) TotalSizeEstimate() int64 {
	return r.totalSize
}

func (r *localReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// localWriter implements Writer for local files.
type localWriter struct {
	dir        string
	outputName string
	tablePath  string
	format     string
	compress   bool

	file         *os.File
	csvWriter    *csv.Writer
	partNum      int32
	bytesWritten int64
	rowsWritten  int64
	filePaths    []string
	headerSet    bool
}

func (w *localWriter) WriteChunk(data []byte, rows int64) error {
	// If no file open, create one
	if w.file == nil {
		if err := w.openNewPart(); err != nil {
			return err
		}
	}

	switch w.format {
	case "csv":
		return w.writeCSVChunk(data, rows)
	default:
		return w.writeRawChunk(data, rows)
	}
}

func (w *localWriter) openNewPart() error {
	ext := w.format
	if w.compress {
		ext += ".gz"
	}

	filename := fmt.Sprintf("part-%05d.%s", w.partNum, ext)
	path := filepath.Join(w.dir, filename)

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	w.file = f
	w.filePaths = append(w.filePaths, path)
	w.partNum++

	if w.format == "csv" {
		w.csvWriter = csv.NewWriter(f)
	}

	return nil
}

func (w *localWriter) writeCSVChunk(data []byte, rows int64) error {
	// Parse incoming CSV and write it
	reader := csv.NewReader(strings.NewReader(string(data)))

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	// Handle header
	startIdx := 0
	if !w.headerSet && len(records) > 0 {
		// First row is header, write it
		if err := w.csvWriter.Write(records[0]); err != nil {
			return err
		}
		w.headerSet = true
		startIdx = 1
	}

	// Write data rows
	for i := startIdx; i < len(records); i++ {
		if err := w.csvWriter.Write(records[i]); err != nil {
			return err
		}
		w.rowsWritten++
	}

	w.csvWriter.Flush()
	w.bytesWritten += int64(len(data))

	return nil
}

func (w *localWriter) writeRawChunk(data []byte, rows int64) error {
	n, err := w.file.Write(data)
	if err != nil {
		return err
	}

	w.bytesWritten += int64(n)
	w.rowsWritten += rows

	return nil
}

func (w *localWriter) OutputName() string {
	return w.outputName
}

func (w *localWriter) TablePath() string {
	return w.tablePath
}

func (w *localWriter) Stats() WriterStats {
	return WriterStats{
		BytesWritten: w.bytesWritten,
		RowsWritten:  w.rowsWritten,
		PartsWritten: w.partNum,
		FilePaths:    w.filePaths,
	}
}

func (w *localWriter) Close() error {
	if w.csvWriter != nil {
		w.csvWriter.Flush()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
