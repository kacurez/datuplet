package datagateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
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

type failingWriter struct{ *httptest.ResponseRecorder }

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("client went away") }

type failingSendStream struct{ mockReadChunkStream }

func (*failingSendStream) Send(*pb.DataChunk) error { return errors.New("transport closed") }

// A decoded HTTP chunk whose write fails finalizes the summary as error and
// fails the reader for good (as a failed raw stream does), so a retry never
// silently skips the undelivered chunk.
func TestReadSummaryHTTPWriteFailure(t *testing.T) {
	lines := captureLog(t)
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 2)
	defer a.Release()
	p.setTableParts("raw", "w", sch, batchFakePart(a))

	resp := openTestReader(t, s, "w", pb.DataFormat_FORMAT_PARQUET)
	s.handleRead(failingWriter{httptest.NewRecorder()}, httptest.NewRequest(http.MethodGet, "/data/read/"+resp.ReaderId, nil))
	if c := httpReadChunk(t, s, resp.ReaderId); c.status != http.StatusInternalServerError {
		t.Errorf("retry after a failed write: HTTP %d, want 500 (reader failed for good)", c.status)
	}
	s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId})
	wantSummary(t, lines(), "table=raw.w", "decoded_files=1", "chunks=1", "outcome=error")
}

func TestReadSummaryGRPCSendFailure(t *testing.T) {
	lines := captureLog(t)
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 2)
	defer a.Release()
	p.setTableParts("raw", "g", sch, batchFakePart(a))

	resp := openTestReader(t, s, "g", pb.DataFormat_FORMAT_PARQUET)
	stream := &failingSendStream{mockReadChunkStream{ctx: context.Background()}}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream); err == nil {
		t.Fatal("ReadChunk succeeded, want the send error")
	}
	// The failed send is sticky: a retry on a working stream gets the send
	// error again, never end-of-data that would skip the undelivered chunk.
	retry := &mockReadChunkStream{ctx: context.Background()}
	err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, retry)
	if err == nil || !strings.Contains(err.Error(), "transport closed") {
		t.Fatalf("retry ReadChunk = %v, want the sticky send error", err)
	}
	if len(retry.chunks) != 0 {
		t.Errorf("retry sent %d chunks, want 0", len(retry.chunks))
	}
	s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId})
	wantSummary(t, lines(), "table=raw.g", "outcome=error")
}

// close() racing chunk production: the raw open is held inside nextChunk, the
// close starts meanwhile, cancels the reader and must then wait for the
// production to finish before it takes the lock, so the summary's counters
// agree with each other.
func TestReadSummaryCloseDuringProduction(t *testing.T) {
	lines := captureLog(t)
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 2)
	defer a.Release()
	entered, release := make(chan struct{}), make(chan struct{})
	body := []byte("0123456789")
	p.setTableParts("raw", "race", sch, rawFakePartFunc(int64(len(body)), 7, func() io.ReadCloser {
		close(entered)
		<-release
		return io.NopCloser(bytes.NewReader(body))
	}))
	resp := openTestReader(t, s, "race", pb.DataFormat_FORMAT_PARQUET)
	rs, _ := s.lookupReader(resp.ReaderId)

	produced := make(chan struct{})
	go func() {
		defer close(produced)
		if c, err := rs.nextChunk(1<<20, memory.NewGoAllocator()); err == nil && c != nil {
			c.raw.Close()
		}
	}()
	<-entered
	closed := make(chan struct{})
	go func() { defer close(closed); rs.close() }()
	<-rs.ctx.Done() // close has cancelled and now waits for the lock
	close(release)
	<-produced
	<-closed
	wantSummary(t, lines(), "raw_files=1", "chunks=1", "rows=7", "bytes=10", "outcome=cancelled")
}

func TestReadSummaryDecodedCounts(t *testing.T) {
	// An empty decoded part of 2 tasks still counts both tasks.
	lines := captureLog(t)
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 2)
	defer a.Release()
	empty := batchFakePart()
	empty.files = 2
	p.setTableParts("raw", "empty", sch, empty)
	resp := openTestReader(t, s, "empty", pb.DataFormat_FORMAT_PARQUET)
	drainHTTP(t, s, resp.ReaderId)
	wantSummary(t, lines(), "table=raw.empty", "decoded_files=2", "rows=0", "outcome=ok")

	// Interruption partway through a 3-task part: only started tasks count.
	lines = captureLog(t)
	s, p = newWriteTestServer(t, &Config{ChunkSize: 1})
	part := batchFakePart(a, a, a)
	part.files = 3
	p.setTableParts("raw", "partial", sch, part)
	resp = openTestReader(t, s, "partial", pb.DataFormat_FORMAT_PARQUET)
	httpReadChunk(t, s, resp.ReaderId) // chunk 1; the lookahead has started task 2
	s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId})
	wantSummary(t, lines(), "table=raw.partial", "decoded_files=2", "chunks=1", "outcome=cancelled")
}
