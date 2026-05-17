package buffer

import (
	"context"
	"fmt"
	"io"

	"github.com/datuplet/datuplet/pkg/datagateway/backend"
)

// BackendWriterFactory creates writers that write directly to the storage backend (MinIO).
type BackendWriterFactory struct {
	ctx     context.Context
	backend backend.StorageBackend
}

// NewBackendWriterFactory creates a new backend writer factory.
func NewBackendWriterFactory(ctx context.Context, be backend.StorageBackend) *BackendWriterFactory {
	return &BackendWriterFactory{
		ctx:     ctx,
		backend: be,
	}
}

// Create implements WriterFactory for backend storage.
// The path is relative to the backend's base path (e.g., "tablename/data/uuid/file001.parquet").
func (f *BackendWriterFactory) Create(path string) (io.WriteCloser, error) {
	// Use the backend's PutObject method to create a writer for the given path
	// We need to return an io.WriteCloser that buffers the data and uploads on Close

	writer := &backendWriter{
		ctx:     f.ctx,
		backend: f.backend,
		path:    path,
		buf:     make([]byte, 0, 1024*1024), // 1MB initial buffer
	}

	return writer, nil
}

// backendWriter is an io.WriteCloser that buffers data and uploads to backend on Close.
type backendWriter struct {
	ctx     context.Context
	backend backend.StorageBackend
	path    string
	buf     []byte
	closed  bool
}

// Write appends data to the buffer.
func (w *backendWriter) Write(p []byte) (n int, err error) {
	if w.closed {
		return 0, fmt.Errorf("writer is closed")
	}
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// Close uploads the buffered data to the backend.
func (w *backendWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Upload the buffered data to the backend
	if err := w.backend.PutObject(w.ctx, w.path, w.buf); err != nil {
		return fmt.Errorf("failed to upload %s: %w", w.path, err)
	}

	return nil
}
