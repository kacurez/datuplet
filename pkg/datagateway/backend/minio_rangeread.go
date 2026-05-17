// Package backend — minio_rangeread.go.
//
// io.ReaderAt over MinIO's range-GET API. Required because parquet's
// reader expects io.ReaderAt (footer-first read pattern). Each ReadAt
// issues one ranged GetObject; we do not hold an open object stream.
//
// Stateless across calls — safe for concurrent use as long as the
// underlying objectGetter is.

package backend

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
)

// objectGetter is the minio-client surface we depend on. Defined narrowly
// so tests can fake it without spinning a real S3.
type objectGetter interface {
	GetObject(ctx context.Context, bucket, key string, opts minio.GetObjectOptions) ([]byte, error)
}

// minioClientAdapter wraps a *minio.Client to fit the objectGetter interface.
// The real implementation drains the streaming *minio.Object into a byte
// slice; the slice size is bounded by the Range header on opts.
type minioClientAdapter struct {
	c *minio.Client
}

func (a *minioClientAdapter) GetObject(ctx context.Context, bucket, key string, opts minio.GetObjectOptions) ([]byte, error) {
	obj, err := a.c.GetObject(ctx, bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	// io.ReadAll handles the chunked reads; the response body is bounded
	// by the Range header, so this won't pull more than requested.
	out, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// minioRangeReaderAt issues one Range GET per ReadAt call.
type minioRangeReaderAt struct {
	ctx    context.Context
	getter objectGetter
	bucket string
	key    string
	size   int64
}

// newMinIORangeReaderAt constructs an io.ReaderAt over a MinIO/S3 object.
// size MUST be the object's content length (typically obtained via StatObject).
func newMinIORangeReaderAt(ctx context.Context, getter objectGetter, bucket, key string, size int64) *minioRangeReaderAt {
	return &minioRangeReaderAt{ctx: ctx, getter: getter, bucket: bucket, key: key, size: size}
}

// Size returns the object's content length, as supplied at construction.
func (r *minioRangeReaderAt) Size() int64 { return r.size }

// ReadAt satisfies io.ReaderAt. It issues exactly one ranged GetObject per
// call and returns either:
//   - n == len(p), nil          on a full read that doesn't reach EOF
//   - n <= len(p), io.EOF       when the read reaches or exceeds object end
//   - 0, io.EOF                 when off >= size
//   - n < len(p), non-nil err   on short range read or transport failure
//
// Per io.ReaderAt contract, n < len(p) MUST come with a non-nil err.
func (r *minioRangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	// Zero-length read: per io.ReaderAt the result is undefined, but the
	// safest implementation is a no-op. Return early to avoid issuing a
	// degenerate Range header (off..off-1).
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}
	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, end); err != nil {
		return 0, fmt.Errorf("set range %d-%d: %w", off, end, err)
	}
	data, err := r.getter.GetObject(r.ctx, r.bucket, r.key, opts)
	if err != nil {
		return 0, fmt.Errorf("get object range %d-%d: %w", off, end, err)
	}
	n := copy(p, data)
	// Did we drain the tail? If so, signal EOF (some callers — e.g. the
	// parquet reader — rely on EOF to stop probing past the footer).
	if int64(n)+off >= r.size {
		return n, io.EOF
	}
	// Non-tail short read: contract violation by the server. Surface as an
	// error (io.ReaderAt requires err != nil when n < len(p)).
	if n < len(p) {
		return n, fmt.Errorf("short range read: got %d, asked for %d", n, len(p))
	}
	return n, nil
}
