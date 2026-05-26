package buffer

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// fakeInner is an io.WriteCloser used as the GCS / S3 stand-in in tests.
// Writes are appended to buf; Close marks closed. Inserts a per-Write
// delay if writeDelay > 0 so we can observe async overlap. Writes return
// writeErr after wantErrAfter bytes if writeErr is set.
type fakeInner struct {
	buf           bytes.Buffer
	closed        atomic.Bool
	writes        atomic.Int64
	writeDelay    time.Duration
	writeErr      error
	wantErrAfter  int64
	bytesObserved atomic.Int64
}

func (f *fakeInner) Write(p []byte) (int, error) {
	if f.writeDelay > 0 {
		time.Sleep(f.writeDelay)
	}
	f.writes.Add(1)
	total := f.bytesObserved.Add(int64(len(p)))
	if f.writeErr != nil && total >= f.wantErrAfter {
		return 0, f.writeErr
	}
	return f.buf.Write(p)
}

func (f *fakeInner) Close() error {
	f.closed.Store(true)
	return nil
}

// TestAsyncWriter_HappyPath round-trips a known payload through the
// wrapper and confirms Close drains it fully to the inner writer.
func TestAsyncWriter_HappyPath(t *testing.T) {
	inner := &fakeInner{}
	a := newAsyncWriter(inner, 2, 1024) // 1 KiB buffers, queue 2

	// Write 5 KiB in several chunks.
	payload := bytes.Repeat([]byte("x"), 5000)
	for _, chunk := range [][]byte{payload[:500], payload[500:1500], payload[1500:4096], payload[4096:]} {
		n, err := a.Write(chunk)
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		if n != len(chunk) {
			t.Fatalf("Write: short write %d != %d", n, len(chunk))
		}
	}

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !inner.closed.Load() {
		t.Errorf("inner.Close not called")
	}
	if got := inner.buf.Bytes(); !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestAsyncWriter_Pipelining proves the producer doesn't block on
// per-Write inner latency once the queue has capacity. With writeDelay=10ms
// and 4 buffers of 1 KiB, writing 4 KiB should take ~10ms (one full
// pipeline cycle) instead of ~40ms (one per buffer serially).
func TestAsyncWriter_Pipelining(t *testing.T) {
	inner := &fakeInner{writeDelay: 10 * time.Millisecond}
	a := newAsyncWriter(inner, 4, 1024)

	payload := bytes.Repeat([]byte("y"), 4*1024)
	start := time.Now()
	if _, err := a.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	produceElapsed := time.Since(start)

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Producer should NOT have paid all 4 * 10ms = 40ms of inner-write
	// latency. With queue=4 it should return in < 20ms. Generous bound
	// to avoid flake on busy CI runners.
	if produceElapsed > 30*time.Millisecond {
		t.Errorf("produce phase too slow: %s — async pipelining not engaging", produceElapsed)
	}
	if got := inner.buf.Len(); got != len(payload) {
		t.Errorf("payload bytes: got %d, want %d", got, len(payload))
	}
}

// TestAsyncWriter_PropagatesError ensures an inner-Write error eventually
// surfaces to a Write or Close call on the wrapper.
func TestAsyncWriter_PropagatesError(t *testing.T) {
	want := errors.New("simulated gcs failure")
	inner := &fakeInner{writeErr: want, wantErrAfter: 1024}
	a := newAsyncWriter(inner, 2, 1024)

	// First buffer (1 KiB) triggers the inner error.
	_, _ = a.Write(bytes.Repeat([]byte("z"), 1024))
	// Subsequent writes may or may not see the error depending on timing.
	// Close MUST surface it.
	_, _ = a.Write(bytes.Repeat([]byte("z"), 1024))

	err := a.Close()
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("Close did not surface inner error: got %v, want %v", err, want)
	}
}

// TestAsyncWriter_CloseIdempotent ensures double-Close is a no-op.
func TestAsyncWriter_CloseIdempotent(t *testing.T) {
	a := newAsyncWriter(&fakeInner{}, 2, 1024)
	if err := a.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestAsyncWriter_RejectsWriteAfterClose makes sure callers can't
// resurrect a closed writer (would hang on a closed channel send).
func TestAsyncWriter_RejectsWriteAfterClose(t *testing.T) {
	a := newAsyncWriter(&fakeInner{}, 2, 1024)
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := a.Write([]byte("after close")); err == nil {
		t.Fatal("expected error writing after Close")
	}
}

// Compile-time guard: asyncWriter must satisfy io.WriteCloser.
var _ io.WriteCloser = (*asyncWriter)(nil)
