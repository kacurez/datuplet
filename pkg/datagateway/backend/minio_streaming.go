package backend

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"
)

// minioPutObjectAPI is the minimal minio client surface used by the
// streaming upload path. Production code passes *minio.Client; tests
// inject a fake to drive failure scenarios (network errors, context
// cancellation, retry semantics) per the Phase 1 acceptance tests.
type minioPutObjectAPI interface {
	PutObject(
		ctx context.Context,
		bucketName, objectName string,
		reader io.Reader, objectSize int64,
		opts minio.PutObjectOptions,
	) (minio.UploadInfo, error)
}

// streamingPartSize is the multipart part size for streaming uploads.
// Per codex: pass PartSize explicitly to avoid minio-go's unknown-size
// memory warning when size=-1. 16 MiB is AWS S3's documented sweet spot
// and bounds peak memory inside minio-go to roughly one part.
const streamingPartSize = 16 * 1024 * 1024

// OpenObjectWriter opens a streaming writer that uploads bytes to the
// S3-compatible backend as the caller writes. Implements the optional
// objectStreamingBackend interface picked up by
// pkg/datagateway/buffer.BackendWriterFactory via type assertion.
//
// Streaming semantics:
//   - Peak memory inside minio-go is bounded by PartSize (16 MiB)
//     rather than the full object. For a 175 MiB parquet file this
//     saves ~160 MiB heap vs the legacy PutObject(buf) path.
//   - Uses multipart upload (size=-1); each part is uploaded as it
//     fills, with retry per-part on transient errors.
//   - Backpressure: caller's Write blocks until minio-go has consumed
//     the previous part. Prevents producer running ahead of upload.
//
// Lifecycle contract:
//   - Caller MUST call Close. Failure to do so leaks one upload goroutine.
//   - Close blocks until the upload goroutine finishes; it surfaces the
//     final upload error (or nil on success).
//   - Context cancellation propagates through minio-go's ctx parameter
//     into the upload goroutine; subsequent Write calls fail with the
//     pipe's stored error.
func (b *MinIOBackend) OpenObjectWriter(ctx context.Context, storagePath string) (io.WriteCloser, error) {
	objectKey := b.toObjectKey(storagePath)
	return openStreamingMinIOWriter(ctx, b.client, b.bucket, objectKey), nil
}

// openStreamingMinIOWriter is the testable core of OpenObjectWriter. Production
// passes *minio.Client; tests inject a fake minioPutObjectAPI.
func openStreamingMinIOWriter(
	ctx context.Context,
	api minioPutObjectAPI,
	bucket, objectKey string,
) io.WriteCloser {
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	go func() {
		defer close(errCh)
		_, err := api.PutObject(
			ctx,
			bucket,
			objectKey,
			pr,
			-1, // unknown size; triggers multipart upload
			minio.PutObjectOptions{
				ContentType: "application/octet-stream",
				PartSize:    streamingPartSize,
			},
		)
		if err != nil {
			// Surface the upload failure to the writer side: any pending
			// or subsequent Write returns this error immediately. Without
			// CloseWithError the writer would block forever waiting for
			// the reader.
			_ = pr.CloseWithError(fmt.Errorf("s3 multipart upload %s/%s: %w", bucket, objectKey, err))
		} else {
			_ = pr.Close()
		}
		errCh <- err
	}()

	return &minioStreamingWriter{pw: pw, errCh: errCh, bucket: bucket, key: objectKey}
}

// minioStreamingWriter wraps the writer end of an io.Pipe whose reader
// end is being consumed by a multipart upload goroutine.
type minioStreamingWriter struct {
	pw     *io.PipeWriter
	errCh  chan error
	bucket string
	key    string

	closeOnce sync.Once
	closeErr  error
}

// Write forwards to the pipe. Blocks until the upload goroutine has
// consumed the data (one multipart part at a time). Returns the upload
// error (wrapped) if the goroutine failed.
func (w *minioStreamingWriter) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

// Close finalizes the upload. Idempotent. Blocks until the upload
// goroutine returns its definitive result, then returns that error
// (or nil on success). Calling Close before all data is written cuts
// the upload short and finalizes with whatever the goroutine has
// already consumed.
func (w *minioStreamingWriter) Close() error {
	w.closeOnce.Do(func() {
		// EOF signal to the upload goroutine.
		if err := w.pw.Close(); err != nil {
			w.closeErr = err
			// Continue to drain errCh so the goroutine isn't leaked.
		}
		if uploadErr, ok := <-w.errCh; ok && uploadErr != nil {
			// Prefer the upload error over a pipe close error: the
			// upload error is the root cause; pipe close is downstream.
			w.closeErr = uploadErr
		}
	})
	return w.closeErr
}
