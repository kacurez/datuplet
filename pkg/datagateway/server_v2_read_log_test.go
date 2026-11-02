package datagateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// captureLog redirects the standard logger for the test and returns the
// "read summary:" lines logged so far.
func captureLog(t *testing.T) func() []string {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	return func() []string {
		var out []string
		for _, l := range strings.Split(buf.String(), "\n") {
			if strings.Contains(l, "read summary:") {
				out = append(out, l)
			}
		}
		return out
	}
}

func wantSummary(t *testing.T, lines []string, wants ...string) {
	t.Helper()
	if len(lines) != 1 {
		t.Fatalf("summary lines = %d, want exactly 1: %q", len(lines), lines)
	}
	for _, w := range wants {
		if !strings.Contains(lines[0], w) {
			t.Errorf("summary %q lacks %q", lines[0], w)
		}
	}
	if !strings.Contains(lines[0], "duration=") {
		t.Errorf("summary %q lacks duration", lines[0])
	}
}

func TestReadSummaryRawOnly(t *testing.T) {
	lines := captureLog(t)
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 3)
	defer a.Release()
	raw1, raw2 := parquetBytes(t, sch, a), parquetBytes(t, sch, a)
	p.setTableParts("raw", "clean", sch, rawFakePart(raw1, 3), rawFakePart(raw2, 3))

	resp := openTestReader(t, s, "clean", pb.DataFormat_FORMAT_PARQUET)
	drainHTTP(t, s, resp.ReaderId)
	if _, err := s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId}); err != nil {
		t.Fatal(err)
	}
	wantSummary(t, lines(), "table=raw.clean", "format=", "raw_files=2", "decoded_files=0", "rows=6", "chunks=2", "outcome=ok",
		"bytes="+strconv.Itoa(len(raw1)+len(raw2)))
}

func TestReadSummaryMixed(t *testing.T) {
	lines := captureLog(t)
	s, p := newWriteTestServer(t, &Config{})
	mem := memory.NewGoAllocator()
	sch, a := idNameBatch(t, mem, 1, 2)
	_, r1 := idNameBatch(t, mem, 10, 4)
	defer a.Release()
	defer r1.Release()
	raw := parquetBytes(t, sch, r1)
	dec := batchFakePart(a)
	dec.files = 2 // two data files decoded into one Records part
	p.setTableParts("raw", "mixed", sch, rawFakePart(raw, 4), dec)

	resp := openTestReader(t, s, "mixed", pb.DataFormat_FORMAT_PARQUET)
	stream := &mockReadChunkStream{ctx: context.Background()}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream); err != nil {
		t.Fatal(err)
	}
	s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId})
	wantSummary(t, lines(), "table=raw.mixed", "raw_files=1", "decoded_files=2", "rows=6", "chunks=2", "outcome=ok")
}

func TestReadSummaryError(t *testing.T) {
	lines := captureLog(t)
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 2)
	defer a.Release()
	p.setTableParts("raw", "bad", sch, batchFakePart(a, a))
	p.failNext(errors.New("storage exploded"), 1)

	resp := openTestReader(t, s, "bad", pb.DataFormat_FORMAT_PARQUET)
	for i := 0; i < 3; i++ { // retries return the sticky error; still one line
		httpReadChunk(t, s, resp.ReaderId)
	}
	s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId})
	wantSummary(t, lines(), "table=raw.bad", "outcome=error")
}

func TestReadSummaryCancelledAndNoSecrets(t *testing.T) {
	lines := captureLog(t)
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 2)
	defer a.Release()
	// The raw body carries a token-like value; it must never reach the log.
	secret := "SECRET-TOKEN-VALUE-12345"
	p.setTableParts("raw", "cancel", sch,
		rawFakePartFunc(int64(len(secret)), 1, func() io.ReadCloser { return io.NopCloser(strings.NewReader(secret)) }),
		batchFakePart(a))

	resp := openTestReader(t, s, "cancel", pb.DataFormat_FORMAT_PARQUET)
	httpReadChunk(t, s, resp.ReaderId) // first chunk only; the read is not finished
	s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId})
	s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId})
	wantSummary(t, lines(), "table=raw.cancel", "outcome=cancelled")
	if strings.Contains(lines()[0], secret) || strings.Contains(lines()[0], "fake://") {
		t.Errorf("summary leaks body or path: %q", lines()[0])
	}
}
