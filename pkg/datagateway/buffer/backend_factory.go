package buffer

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/datuplet/datuplet/pkg/datagateway/backend"
)

// BackendWriterFactory creates writers that write directly to the storage backend (MinIO).
type BackendWriterFactory struct {
	ctx     context.Context
	backend backend.StorageBackend
}

// objectStreamingBackend is an optional capability: backends that
// implement it expose a streaming WriteCloser so callers (parquet writer)
// do not have to materialize the full object in memory before upload.
// All three production backends (gcs, minio/s3, local) can implement this.
type objectStreamingBackend interface {
	OpenObjectWriter(ctx context.Context, path string) (io.WriteCloser, error)
}

// NewBackendWriterFactory creates a new backend writer factory.
func NewBackendWriterFactory(ctx context.Context, be backend.StorageBackend) *BackendWriterFactory {
	return &BackendWriterFactory{
		ctx:     ctx,
		backend: be,
	}
}

// Create implements WriterFactory for backend storage.
// If the backend supports objectStreamingBackend, Create returns its
// streaming WriteCloser directly (~one chunk peak memory). Otherwise it
// falls back to buffering the full object before PutObject.
func (f *BackendWriterFactory) Create(path string) (io.WriteCloser, error) {
	if sb, ok := f.backend.(objectStreamingBackend); ok {
		debugf("backend.Create: path=%s mode=streaming", path)
		return sb.OpenObjectWriter(f.ctx, path)
	}
	debugf("backend.Create: path=%s mode=buffered (legacy PutObject path)", path)
	writer := &backendWriter{
		ctx:      f.ctx,
		backend:  f.backend,
		path:     path,
		buf:      make([]byte, 0, 1024*1024),
		openedAt: time.Now(),
	}
	return writer, nil
}

// backendWriter is an io.WriteCloser that buffers data and uploads to backend on Close.
type backendWriter struct {
	ctx      context.Context
	backend  backend.StorageBackend
	path     string
	buf      []byte
	closed   bool
	openedAt time.Time // for DATUPLET_GATEWAY_DEBUG upload-duration accounting
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

	// Debug-mode upload timing: useful for diagnosing slow GCS / network
	// uploads on long runs. Captures both the buffered phase (which can
	// hold up to TargetFileSize before this Close) and the upload itself.
	bufBytes := len(w.buf)
	bufferedFor := time.Since(w.openedAt)
	uploadStart := time.Now()
	debugf("backendWriter.Close: path=%s bytes=%d buffered_for=%s upload starting",
		w.path, bufBytes, bufferedFor.Round(time.Millisecond))

	// Upload the buffered data to the backend
	if err := w.backend.PutObject(w.ctx, w.path, w.buf); err != nil {
		debugf("backendWriter.Close: path=%s upload FAILED after %s: %v",
			w.path, time.Since(uploadStart).Round(time.Millisecond), err)
		return fmt.Errorf("failed to upload %s: %w", w.path, err)
	}
	debugf("backendWriter.Close: path=%s bytes=%d upload_took=%s",
		w.path, bufBytes, time.Since(uploadStart).Round(time.Millisecond))

	return nil
}
