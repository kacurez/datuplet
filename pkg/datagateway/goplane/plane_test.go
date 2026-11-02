package goplane

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"gocloud.dev/blob"

	"github.com/datuplet/datuplet/pkg/datagateway"
)

// slowAbortSink accepts writes; its Close, when the upload context is
// already cancelled (an abort), blocks until release is closed, as a slow
// AbortMultipartUpload does.
type slowAbortSink struct {
	ctx     context.Context
	release chan struct{}
	aborted atomic.Bool // the abort Close finished
	closes  atomic.Int32
}

func (s *slowAbortSink) Write(p []byte) (int, error) { return len(p), nil }

func (s *slowAbortSink) Close() error {
	s.closes.Add(1)
	if s.ctx.Err() != nil {
		<-s.release
		s.aborted.Store(true)
	}
	return nil
}

// openWriterWithSlowAbort opens a writer on p whose single file's sink is
// returned once the writer has opened it (after one batch is written).
func openWriterWithSlowAbort(t *testing.T, p *Plane) *slowAbortSink {
	t.Helper()
	sink := &slowAbortSink{release: make(chan struct{})}
	opened := make(chan struct{})
	p.openSink = func(ctx context.Context, _ map[string]string, _ string, _ *blob.WriterOptions) (io.WriteCloser, error) {
		sink.ctx = ctx
		close(opened)
		return sink, nil
	}
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: "file:///unused/"})
	rec := batchWith(memory.NewGoAllocator(), testSchema, 0, 10)
	defer rec.Release()
	if err := w.Write(context.Background(), rec); err != nil {
		t.Fatalf("Write: %v", err)
	}
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("writer never opened its sink")
	}
	return sink
}

// TestPlaneCloseWaitsForWriterCleanup: plane Close returns only after an
// open writer's (delayed) abort has completed, and admits no new writer.
func TestPlaneCloseWaitsForWriterCleanup(t *testing.T) {
	p := New(Config{})
	sink := openWriterWithSlowAbort(t, p)

	const delay = 300 * time.Millisecond
	go func() {
		time.Sleep(delay)
		close(sink.release)
	}()
	start := time.Now()
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !sink.aborted.Load() {
		t.Fatal("plane Close returned before the writer's abort completed")
	}
	if sink.closes.Load() != 1 {
		t.Errorf("sink closed %d times, want 1", sink.closes.Load())
	}
	if el := time.Since(start); el < delay/2 {
		t.Errorf("Close returned after %s; it did not wait for the abort", el)
	}

	if _, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: "file:///unused/", FilePrefix: "part-x", Schema: testSchema,
	}); err == nil {
		t.Error("OpenTableWriter after Close succeeded, want an error")
	}
	if err := p.Close(); err != nil { // idempotent, nothing left to wait for
		t.Fatalf("second Close: %v", err)
	}
}

// TestPlaneCloseBoundedWhenAbortHangs: an abort that never returns makes
// Close return after the bound.
func TestPlaneCloseBoundedWhenAbortHangs(t *testing.T) {
	p := New(Config{})
	p.closeWait = 200 * time.Millisecond
	sink := openWriterWithSlowAbort(t, p)
	t.Cleanup(func() { close(sink.release) }) // let the writer goroutine exit

	returned := make(chan time.Duration, 1)
	start := time.Now()
	go func() {
		_ = p.Close()
		returned <- time.Since(start)
	}()
	select {
	case el := <-returned:
		if el < p.closeWait {
			t.Errorf("Close returned after %s, before the %s bound", el, p.closeWait)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after its bound")
	}
	if sink.aborted.Load() {
		t.Error("abort reported done although its Close never returned")
	}
	if sink.closes.Load() != 1 {
		t.Errorf("sink Close calls = %d, want 1 (the abort ran)", sink.closes.Load())
	}
}
