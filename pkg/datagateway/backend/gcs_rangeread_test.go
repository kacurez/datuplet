package backend

// Unit tests for the GCS range-read adapter (fake-based, no containers).
// The fake-gcs-server end-to-end footer-first test lives in
// gcs_rangeread_integration_test.go under `//go:build integration`.

import (
	"context"
	"fmt"
	"io"
	"testing"
)

// fakeGCSRangeGetter is a stand-in for a real GCS bucket handle. It
// serves bytes from an in-memory payload according to the (offset,
// length) pair on each call. Mirrors fakeObjectGetter from
// minio_rangeread_test.go but speaks the GCS-shaped API instead of
// minio's Range header.
type fakeGCSRangeGetter struct {
	data []byte
	// calls records the (offset, length) pairs the adapter requested. The
	// per-test assertions use this to verify the Range-GET pattern.
	calls []gcsRangeCall
}

type gcsRangeCall struct{ offset, length int64 }

func (f *fakeGCSRangeGetter) GetRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("invalid range: offset=%d length=%d", offset, length)
	}
	if offset >= int64(len(f.data)) {
		return nil, fmt.Errorf("range out of bounds: offset=%d (size %d)", offset, len(f.data))
	}
	end := offset + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data)) // clamp, don't error — matches real GCS behaviour
	}
	f.calls = append(f.calls, gcsRangeCall{offset, length})
	return append([]byte(nil), f.data[offset:end]...), nil
}

func TestGCSRangeReaderAt(t *testing.T) {
	testRangeReaderAt(t, func(payload []byte) (io.ReaderAt, func() [][2]int64) {
		g := &fakeGCSRangeGetter{data: payload}
		r := newGCSRangeReaderAt(context.Background(), g, "key", int64(len(payload)))
		return r, func() [][2]int64 {
			var out [][2]int64
			for _, c := range g.calls {
				out = append(out, [2]int64{c.offset, c.length})
			}
			return out
		}
	})
}
