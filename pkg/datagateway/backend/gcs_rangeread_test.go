package backend

// Unit tests for the GCS range-read adapter (fake-based, no containers).
// The fake-gcs-server end-to-end footer-first test lives in
// gcs_rangeread_integration_test.go under `//go:build integration`.

import (
	"bytes"
	"context"
	"errors"
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

func TestGCSRangeReaderAt_ReadsContiguousBytes(t *testing.T) {
	payload := []byte("0123456789abcdef")
	g := &fakeGCSRangeGetter{data: payload}
	r := newGCSRangeReaderAt(context.Background(), g, "key", int64(len(payload)))

	buf := make([]byte, 4)
	n, err := r.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 4 || !bytes.Equal(buf, []byte("6789")) {
		t.Errorf("got %q (n=%d), want %q (n=4)", buf, n, "6789")
	}
	if len(g.calls) != 1 || g.calls[0] != (gcsRangeCall{6, 4}) {
		t.Errorf("expected single Range GET offset=6 length=4, got %+v", g.calls)
	}
}

func TestGCSRangeReaderAt_TailReadReturnsEOF(t *testing.T) {
	payload := []byte("0123456789abcdef") // size 16
	g := &fakeGCSRangeGetter{data: payload}
	r := newGCSRangeReaderAt(context.Background(), g, "key", int64(len(payload)))

	// Read final byte exactly: off=15, len=1 — n+off = 16 = size, expect EOF.
	buf := make([]byte, 1)
	n, err := r.ReadAt(buf, 15)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF on tail read, got %v", err)
	}
	if n != 1 || buf[0] != 'f' {
		t.Errorf("got %q (n=%d), want %q (n=1)", buf, n, "f")
	}
}

func TestGCSRangeReaderAt_ReadPastEndReturnsEOF(t *testing.T) {
	payload := []byte("0123456789abcdef")
	g := &fakeGCSRangeGetter{data: payload}
	r := newGCSRangeReaderAt(context.Background(), g, "key", int64(len(payload)))

	// off >= size: zero bytes, EOF, no GET.
	n, err := r.ReadAt(make([]byte, 4), int64(len(payload)))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected n=0, got %d", n)
	}
	if len(g.calls) != 0 {
		t.Errorf("expected zero GETs past end, got %d", len(g.calls))
	}
}

func TestGCSRangeReaderAt_OverflowingReadClampsAndReturnsEOF(t *testing.T) {
	payload := []byte("0123456789abcdef") // size 16
	g := &fakeGCSRangeGetter{data: payload}
	r := newGCSRangeReaderAt(context.Background(), g, "key", int64(len(payload)))

	// Ask for 32 bytes from offset 0; object is 16. Expect length clamped to
	// 16, 16 bytes copied, EOF returned.
	buf := make([]byte, 32)
	n, err := r.ReadAt(buf, 0)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF on overflowing read, got %v", err)
	}
	if n != 16 {
		t.Errorf("expected n=16, got %d", n)
	}
	if !bytes.Equal(buf[:16], payload) {
		t.Errorf("payload mismatch: got %q want %q", buf[:16], payload)
	}
	if len(g.calls) != 1 || g.calls[0] != (gcsRangeCall{0, 16}) {
		t.Errorf("expected Range offset=0 length=16, got %+v", g.calls)
	}
}

func TestGCSRangeReaderAt_ZeroLengthReadIsNoOp(t *testing.T) {
	payload := []byte("0123456789abcdef")
	g := &fakeGCSRangeGetter{data: payload}
	r := newGCSRangeReaderAt(context.Background(), g, "key", int64(len(payload)))

	// Zero-length p: must return (0, nil) without issuing a degenerate GET.
	n, err := r.ReadAt(nil, 4)
	if err != nil || n != 0 {
		t.Errorf("ReadAt(nil, 4): got (%d, %v), want (0, nil)", n, err)
	}
	if len(g.calls) != 0 {
		t.Errorf("zero-length read should not issue any GETs, got %d", len(g.calls))
	}
}

func TestGCSRangeReaderAt_Size(t *testing.T) {
	g := &fakeGCSRangeGetter{data: []byte("xyz")}
	r := newGCSRangeReaderAt(context.Background(), g, "key", 3)
	if got := r.Size(); got != 3 {
		t.Errorf("Size(): got %d, want 3", got)
	}
}
