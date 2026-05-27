package buffer

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
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

// asyncUploadConfig returns (enabled, queueCap, bufSize) for the
// async-upload wrapper around streaming backends.
//
// Defaults are tuned for the 50M-row gen-pod write profile: queueCap=4
// in-flight buffers × bufSize=16 MiB = 80 MiB extra peak per writer. The
// inner GCS resumable-upload writer's own ChunkSize is matched to this
// bufSize so each handoff aligns with a single HTTP PUT.
//
// Operators can override via env:
//
//	DATUPLET_GATEWAY_ASYNC_UPLOAD=0          # disable wrapper, single-thread upload
//	DATUPLET_GATEWAY_ASYNC_UPLOAD_QUEUE=N    # buffer count (default 4)
//	DATUPLET_GATEWAY_ASYNC_UPLOAD_BUF_MB=N   # per-buffer MiB (default 16)
func asyncUploadConfig() (enabled bool, queueCap, bufSize int) {
	enabled = true
	queueCap = 4
	bufSize = 16 * 1024 * 1024 // 16 MiB

	if v := os.Getenv("DATUPLET_GATEWAY_ASYNC_UPLOAD"); v == "0" || v == "false" {
		enabled = false
	}
	if v := os.Getenv("DATUPLET_GATEWAY_ASYNC_UPLOAD_QUEUE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 32 {
			queueCap = n
		}
	}
	if v := os.Getenv("DATUPLET_GATEWAY_ASYNC_UPLOAD_BUF_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 128 {
			bufSize = n * 1024 * 1024
		}
	}
	return
}

// Create implements WriterFactory for backend storage.
// If the backend supports objectStreamingBackend, Create returns its
// streaming WriteCloser wrapped in an async upload buffer so the parquet
// encoder pipelines with chunked HTTP PUTs to the object store. The
// wrapper is opt-out via DATUPLET_GATEWAY_ASYNC_UPLOAD=0. Otherwise it
// falls back to buffering the full object before PutObject.
func (f *BackendWriterFactory) Create(path string) (io.WriteCloser, error) {
	if sb, ok := f.backend.(objectStreamingBackend); ok {
		inner, err := sb.OpenObjectWriter(f.ctx, path)
		if err != nil {
			return nil, err
		}
		enabled, queueCap, bufSize := asyncUploadConfig()
		if !enabled {
			debugf("backend.Create: path=%s mode=streaming async=off", path)
			return inner, nil
		}
		debugf("backend.Create: path=%s mode=streaming async=on queue=%d buf_mb=%d",
			path, queueCap, bufSize/(1024*1024))
		return newAsyncWriter(inner, queueCap, bufSize), nil
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
