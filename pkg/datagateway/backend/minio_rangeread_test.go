package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

// fakeObjectGetter is a stand-in for a real minio client. It serves bytes from
// an in-memory payload according to the Range header on opts. A real-S3
// integration test runs in slice 5's e2e.
//
// NOTE: minio-go v7's GetObjectOptions has unexported RangeStart / RangeEnd
// fields, so we parse the Range header (e.g. "bytes=6-9") instead of reading
// struct fields directly.
type fakeObjectGetter struct {
	data []byte
	// calls records the (start, end) pairs the adapter requested. The
	// per-test assertions use this to verify the Range-GET pattern.
	calls []rangeCall
}

type rangeCall struct{ start, end int64 }

func (f *fakeObjectGetter) GetObject(_ context.Context, _ string, _ string, opts minio.GetObjectOptions) ([]byte, error) {
	rng := opts.Header().Get("Range")
	start, end, err := parseRangeHeader(rng)
	if err != nil {
		return nil, err
	}
	if start < 0 || end >= int64(len(f.data)) || start > end {
		return nil, fmt.Errorf("range out of bounds: %d-%d (size %d)", start, end, len(f.data))
	}
	f.calls = append(f.calls, rangeCall{start, end})
	return append([]byte(nil), f.data[start:end+1]...), nil
}

// parseRangeHeader parses a single-range "bytes=START-END" header.
func parseRangeHeader(h string) (int64, int64, error) {
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, fmt.Errorf("missing bytes= prefix in Range header %q", h)
	}
	body := strings.TrimPrefix(h, "bytes=")
	dash := strings.IndexByte(body, '-')
	if dash < 0 {
		return 0, 0, fmt.Errorf("malformed Range header %q", h)
	}
	var start, end int64
	if _, err := fmt.Sscanf(body[:dash], "%d", &start); err != nil {
		return 0, 0, fmt.Errorf("parse start of %q: %w", h, err)
	}
	if _, err := fmt.Sscanf(body[dash+1:], "%d", &end); err != nil {
		return 0, 0, fmt.Errorf("parse end of %q: %w", h, err)
	}
	return start, end, nil
}

func TestMinIORangeReaderAt_ReadsContiguousBytes(t *testing.T) {
	payload := []byte("0123456789abcdef")
	g := &fakeObjectGetter{data: payload}
	r := newMinIORangeReaderAt(context.Background(), g, "bucket", "key", int64(len(payload)))

	buf := make([]byte, 4)
	n, err := r.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 4 || !bytes.Equal(buf, []byte("6789")) {
		t.Errorf("got %q (n=%d), want %q (n=4)", buf, n, "6789")
	}
	if len(g.calls) != 1 || g.calls[0] != (rangeCall{6, 9}) {
		t.Errorf("expected single Range GET 6-9, got %+v", g.calls)
	}
}

func TestMinIORangeReaderAt_TailReadReturnsEOF(t *testing.T) {
	payload := []byte("0123456789abcdef") // size 16
	g := &fakeObjectGetter{data: payload}
	r := newMinIORangeReaderAt(context.Background(), g, "bucket", "key", int64(len(payload)))

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

func TestMinIORangeReaderAt_ReadPastEndReturnsEOF(t *testing.T) {
	payload := []byte("0123456789abcdef")
	g := &fakeObjectGetter{data: payload}
	r := newMinIORangeReaderAt(context.Background(), g, "bucket", "key", int64(len(payload)))

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

func TestMinIORangeReaderAt_OverflowingReadClampsAndReturnsEOF(t *testing.T) {
	payload := []byte("0123456789abcdef") // size 16
	g := &fakeObjectGetter{data: payload}
	r := newMinIORangeReaderAt(context.Background(), g, "bucket", "key", int64(len(payload)))

	// Ask for 32 bytes from offset 0; object is 16. Expect end clamped to 15,
	// 16 bytes copied, EOF returned.
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
	if len(g.calls) != 1 || g.calls[0] != (rangeCall{0, 15}) {
		t.Errorf("expected Range 0-15, got %+v", g.calls)
	}
}

func TestMinIORangeReaderAt_ZeroLengthReadIsNoOp(t *testing.T) {
	payload := []byte("0123456789abcdef")
	g := &fakeObjectGetter{data: payload}
	r := newMinIORangeReaderAt(context.Background(), g, "bucket", "key", int64(len(payload)))

	// Zero-length p: must return (0, nil) without issuing a degenerate GET.
	n, err := r.ReadAt(nil, 4)
	if err != nil || n != 0 {
		t.Errorf("ReadAt(nil, 4): got (%d, %v), want (0, nil)", n, err)
	}
	if len(g.calls) != 0 {
		t.Errorf("zero-length read should not issue any GETs, got %d", len(g.calls))
	}
}

func TestMinIORangeReaderAt_Size(t *testing.T) {
	g := &fakeObjectGetter{data: []byte("xyz")}
	r := newMinIORangeReaderAt(context.Background(), g, "bucket", "key", 3)
	if got := r.Size(); got != 3 {
		t.Errorf("Size(): got %d, want 3", got)
	}
}
