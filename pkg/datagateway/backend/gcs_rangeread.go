// Package backend — gcs_rangeread.go.
//
// io.ReaderAt over GCS's range-GET API. Required because parquet's
// reader expects io.ReaderAt (footer-first read pattern). Each ReadAt
// issues one ranged NewRangeReader; we do not hold an open object stream.
//
// Stateless across calls — safe for concurrent use as long as the
// underlying gcsRangeGetter is.
//
// Mirrors minio_rangeread.go's contract: size-aware, EOF on tail reads,
// io.ReaderAt-compliant error semantics. The GCS-specific knob is
// *storage.ObjectHandle.NewRangeReader(ctx, offset, length); this file
// adapts it to the same surface MinIO exposes.

package backend

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

// gcsRangeGetter is the GCS surface gcsRangeReaderAt depends on. Defined
// narrowly so tests can fake it without spinning a real GCS (or even
// fake-gcs-server) for unit-level coverage. Production: a thin adapter
// around *storage.ObjectHandle.
type gcsRangeGetter interface {
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
}

// gcsHandleAdapter wraps a *storage.BucketHandle to fit gcsRangeGetter.
// The real implementation drains the streaming *storage.Reader returned
// by NewRangeReader into a byte slice bounded by `length`.
type gcsHandleAdapter struct {
	bkt *storage.BucketHandle
}

func (a *gcsHandleAdapter) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	rd, err := a.bkt.Object(key).NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, err
	}
	defer rd.Close()
	// io.ReadAll handles the chunked reads; the *storage.Reader is bounded
	// by `length`, so this won't pull more than requested.
	out, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// gcsRangeReaderAt issues one ranged NewRangeReader per ReadAt call.
type gcsRangeReaderAt struct {
	ctx    context.Context
	getter gcsRangeGetter
	key    string
	size   int64
}

// newGCSRangeReaderAt constructs an io.ReaderAt over a GCS object.
// size MUST be the object's content length (typically obtained via
// *storage.ObjectHandle.Attrs).
func newGCSRangeReaderAt(ctx context.Context, getter gcsRangeGetter, key string, size int64) *gcsRangeReaderAt {
	return &gcsRangeReaderAt{ctx: ctx, getter: getter, key: key, size: size}
}

// NewRangeReader is the *gcsBackend-bound constructor used by Parquet's
// footer-first reader. It looks up the object size via Attrs once, then
// hands back a stateless gcsRangeReaderAt that issues one Range GET per
// ReadAt call. Not part of StorageBackend — backend-specific extension,
// same as MinIO's analogue.
func (g *gcsBackend) NewRangeReader(ctx context.Context, key string) (*gcsRangeReaderAt, error) {
	objectKey := g.toObjectKey(key)
	attrs, err := g.bkt.Object(objectKey).Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs: attrs %q: %w", objectKey, err)
	}
	return newGCSRangeReaderAt(ctx, &gcsHandleAdapter{bkt: g.bkt}, objectKey, attrs.Size), nil
}

// Size returns the object's content length, as supplied at construction.
func (r *gcsRangeReaderAt) Size() int64 { return r.size }

// ReadAt satisfies io.ReaderAt. It issues exactly one ranged GET per
// call and returns either:
//   - n == len(p), nil          on a full read that doesn't reach EOF
//   - n <= len(p), io.EOF       when the read reaches or exceeds object end
//   - 0, io.EOF                 when off >= size
//   - n < len(p), non-nil err   on short range read or transport failure
//
// Per io.ReaderAt contract, n < len(p) MUST come with a non-nil err.
func (r *gcsRangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	// Zero-length read: per io.ReaderAt the result is undefined, but the
	// safest implementation is a no-op. Return early to avoid issuing a
	// degenerate range request.
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}
	length := int64(len(p))
	if off+length > r.size {
		length = r.size - off
	}
	data, err := r.getter.GetRange(r.ctx, r.key, off, length)
	if err != nil {
		return 0, fmt.Errorf("get object range %d-%d: %w", off, off+length-1, err)
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
