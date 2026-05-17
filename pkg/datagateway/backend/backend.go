// Package backend provides storage backend implementations for the data gateway.
package backend

import (
	"context"
)

// StorageBackend is the interface for reading and writing data to various storage systems.
type StorageBackend interface {
	// OpenReader opens a reader for the given table path.
	OpenReader(ctx context.Context, tablePath string) (Reader, error)

	// OpenWriter opens a writer for the given table path.
	OpenWriter(ctx context.Context, tablePath string, opts WriteOptions) (Writer, error)

	// Commit commits all the given writers (creates Iceberg snapshots for Iceberg backend).
	Commit(ctx context.Context, writers []Writer) (*CommitResult, error)

	// Rollback cleans up staged files for the given writers.
	Rollback(ctx context.Context, writers []Writer) error

	// GetSchema returns the schema for the given table path.
	GetSchema(ctx context.Context, tablePath string) (*SchemaInfo, error)

	// GetSample returns sample rows from the given table path.
	GetSample(ctx context.Context, tablePath string, limit int) (*SampleResult, error)

	// GetObject downloads raw bytes from the given path in the backend.
	// Returns the file contents as bytes.
	GetObject(ctx context.Context, path string) ([]byte, error)

	// PutObject uploads raw bytes to the given path in the backend.
	PutObject(ctx context.Context, path string, data []byte) error

	// RemoveAll recursively removes all objects under the given prefix.
	// Used by workspace cleanup. The prefix MUST be inside the backend's
	// namespace (the implementation rejects out-of-scope prefixes).
	// Idempotent: no error if the prefix does not exist or has no objects.
	RemoveAll(ctx context.Context, prefix string) error

	// Close releases any resources held by the backend.
	Close() error
}

// Reader provides chunked reading from a data source.
type Reader interface {
	// ReadChunk reads the next chunk of data.
	// Returns io.EOF when there is no more data.
	ReadChunk() (*DataChunk, error)

	// Schema returns the schema of the data being read.
	Schema() *SchemaInfo

	// TotalSizeEstimate returns an estimate of the total size in bytes.
	TotalSizeEstimate() int64

	// Close releases resources.
	Close() error
}

// Writer provides chunked writing to a data destination.
type Writer interface {
	// WriteChunk writes a chunk of data.
	WriteChunk(data []byte, rows int64) error

	// OutputName returns the name of the output this writer is for.
	OutputName() string

	// TablePath returns the table path this writer is writing to.
	TablePath() string

	// Stats returns current write statistics.
	Stats() WriterStats

	// Close finalizes the writer (but does not commit).
	Close() error
}

// WriteOptions configures how data is written.
type WriteOptions struct {
	// OutputName is the logical name of the output (from component config).
	OutputName string

	// Format specifies the output format ("csv", "parquet").
	// Default is "parquet".
	Format string

	// Compress enables gzip compression.
	Compress bool
}

// DataChunk represents a chunk of data read from a source.
type DataChunk struct {
	// Data is the raw bytes of the chunk.
	Data []byte

	// Format is the format of the data ("csv", "parquet", "json").
	Format string

	// IsLast indicates this is the last chunk.
	IsLast bool

	// RowsInChunk is the number of rows in this chunk (if known).
	RowsInChunk int64

	// Metadata contains additional key-value metadata.
	Metadata map[string]string
}

// WriterStats contains statistics about data written.
type WriterStats struct {
	// BytesWritten is the total bytes written.
	BytesWritten int64

	// RowsWritten is the total rows written.
	RowsWritten int64

	// PartsWritten is the number of file parts written.
	PartsWritten int32

	// FilePaths contains the paths of all files written.
	FilePaths []string
}

// CommitResult contains the results of committing writers.
type CommitResult struct {
	// Tables contains the result for each committed table.
	Tables []TableCommitResult
}

// TableCommitResult contains the result of committing a single table.
type TableCommitResult struct {
	// OutputName is the logical name of the output.
	OutputName string

	// TablePath is the path to the table.
	TablePath string

	// Status indicates the commit status.
	Status CommitStatus

	// SnapshotID is the Iceberg snapshot ID (if applicable).
	SnapshotID int64

	// FilesAdded is the number of files added.
	FilesAdded int32

	// RowsAdded is the number of rows added.
	RowsAdded int64

	// Error contains the error message if status is Failed.
	Error string
}

// CommitStatus represents the status of a table commit.
type CommitStatus int

const (
	_ CommitStatus = iota
	CommitStatusCommitted
)

// SchemaInfo describes the schema of a table.
type SchemaInfo struct {
	Columns []ColumnInfo
}

// ColumnInfo describes a single column.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// SampleResult contains sample data and metadata.
type SampleResult struct {
	Schema        *SchemaInfo
	Rows          [][]byte // JSON-encoded rows
	TotalEstimate int64
}
