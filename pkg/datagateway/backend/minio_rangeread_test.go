package backend

import (
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

// testRangeReaderAt is the shared range-reader contract for the MinIO and GCS
// adapters. newReader builds a reader over payload (object size == len(payload))
// and returns a func reporting the (offset, length) of every ranged GET the
// adapter issued, so the Range-GET pattern can be asserted uniformly.
func testRangeReaderAt(t *testing.T, newReader func(payload []byte) (io.ReaderAt, func() [][2]int64)) {
	t.Helper()
	payload := []byte("0123456789abcdef") // size 16

	tests := []struct {
		name      string
		bufLen    int // 0 = nil buffer
		off       int64
		wantN     int
		wantEOF   bool
		wantData  string
		wantCalls [][2]int64
	}{
		{
			name: "reads contiguous bytes", bufLen: 4, off: 6,
			wantN: 4, wantData: "6789", wantCalls: [][2]int64{{6, 4}},
		},
		{
			// Final byte exactly: off=15, len=1 — n+off = size, expect EOF.
			name: "tail read returns EOF", bufLen: 1, off: 15,
			wantN: 1, wantEOF: true, wantData: "f", wantCalls: [][2]int64{{15, 1}},
		},
		{
			// off >= size: zero bytes, EOF, no GET.
			name: "read past end returns EOF without a GET", bufLen: 4, off: 16,
			wantN: 0, wantEOF: true,
		},
		{
			// 32 bytes from offset 0; object is 16: length clamped to 16, EOF.
			name: "overflowing read clamps and returns EOF", bufLen: 32, off: 0,
			wantN: 16, wantEOF: true, wantData: string(payload), wantCalls: [][2]int64{{0, 16}},
		},
		{
			// Zero-length p: (0, nil) without a degenerate GET.
			name: "zero-length read is a no-op", bufLen: 0, off: 4,
			wantN: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, calls := newReader(payload)
			var buf []byte
			if tc.bufLen > 0 {
				buf = make([]byte, tc.bufLen)
			}

			n, err := r.ReadAt(buf, tc.off)
			if tc.wantEOF != errors.Is(err, io.EOF) || (!tc.wantEOF && err != nil) {
				t.Fatalf("ReadAt err = %v, want EOF=%v", err, tc.wantEOF)
			}
			if n != tc.wantN {
				t.Errorf("n = %d, want %d", n, tc.wantN)
			}
			if got := string(buf[:n]); got != tc.wantData {
				t.Errorf("data = %q, want %q", got, tc.wantData)
			}
			got := calls()
			if len(got) != len(tc.wantCalls) {
				t.Fatalf("GETs = %v, want %v", got, tc.wantCalls)
			}
			for i := range got {
				if got[i] != tc.wantCalls[i] {
					t.Errorf("GET %d = %v, want %v", i, got[i], tc.wantCalls[i])
				}
			}
		})
	}
}

func TestMinIORangeReaderAt(t *testing.T) {
	testRangeReaderAt(t, func(payload []byte) (io.ReaderAt, func() [][2]int64) {
		g := &fakeObjectGetter{data: payload}
		r := newMinIORangeReaderAt(context.Background(), g, "bucket", "key", int64(len(payload)))
		return r, func() [][2]int64 {
			var out [][2]int64
			for _, c := range g.calls {
				out = append(out, [2]int64{c.start, c.end - c.start + 1}) // Range header is inclusive
			}
			return out
		}
	})
}
