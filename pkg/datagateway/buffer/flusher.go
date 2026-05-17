// Package buffer provides buffering and flushing utilities for Arrow data.
package buffer

import (
	"io"
)

// WriterFactory creates io.WriteCloser instances for output paths.
// This abstraction allows writing to local files, S3, GCS, etc.
type WriterFactory interface {
	// Create creates a new writer for the given path.
	Create(path string) (io.WriteCloser, error)
}

// LocalWriterFactory creates local file writers.
type LocalWriterFactory struct{}

// Create implements WriterFactory for local files.
func (f *LocalWriterFactory) Create(path string) (io.WriteCloser, error) {
	return createLocalFile(path)
}

// FlushStats contains statistics about a flush operation.
type FlushStats struct {
	// RowsWritten is the total number of rows written.
	RowsWritten int64

	// BytesWritten is the total bytes written to the output.
	BytesWritten int64

	// RowGroups is the number of row groups written (Parquet only).
	RowGroups int

	// FilePath is the path where data was written.
	FilePath string
}
