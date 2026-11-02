package goplane

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"gocloud.dev/blob"

	"github.com/datuplet/datuplet/pkg/datagateway"
)

// slowAbortSink accepts writes; its Close, when the upload context is
// already cancelled (an abort), closes abortStarted and blocks until release
// is closed, as a slow AbortMultipartUpload does.
type slowAbortSink struct {
	ctx          context.Context
	release      chan struct{}
	abortStarted chan struct{}
	startOnce    sync.Once
	aborted      atomic.Bool // the abort Close finished
	closes       atomic.Int32
}

func (s *slowAbortSink) Write(p []byte) (int, error) { return len(p), nil }

func (s *slowAbortSink) Close() error {
	s.closes.Add(1)
	if s.ctx.Err() != nil {
		s.startOnce.Do(func() { close(s.abortStarted) })
		<-s.release
		s.aborted.Store(true)
	}
	return nil
}

// openWriterWithSlowAbort opens a writer on p and returns it with its single
// file's sink once the writer has opened that sink (after one batch is
// written).
func openWriterWithSlowAbort(t *testing.T, p *Plane) (*slowAbortSink, *writer) {
	t.Helper()
	sink := &slowAbortSink{release: make(chan struct{}), abortStarted: make(chan struct{})}
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
	return sink, w.(*writer)
}

// TestPlaneCloseWaitsForWriterCleanup: plane Close returns only after an
// open writer's (delayed) abort has completed, and admits no new writer.
func TestPlaneCloseWaitsForWriterCleanup(t *testing.T) {
	p := New(Config{})
	sink, _ := openWriterWithSlowAbort(t, p)

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
	sink, w := openWriterWithSlowAbort(t, p)
	t.Cleanup(func() {
		close(sink.release) // let the writer goroutine exit
		<-w.done            // and join it, so it does not outlive the test
		p.writers.Wait()
	})

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

// waitClosed fails the test unless ch is closed within a generous bound (a
// deadlock guard, not a timing assertion).
func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never happened", what)
	}
}

// isClosed reports whether ch is already closed.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestPlaneCloseRejectsWritersOnceClosing: while Close waits for an admitted
// writer's cleanup, OpenTableWriter is rejected; Close returns only after
// that cleanup.
func TestPlaneCloseRejectsWritersOnceClosing(t *testing.T) {
	p := New(Config{})
	sink, w := openWriterWithSlowAbort(t, p)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_ = p.Close()
	}()
	// The abort is running, so Close has stopped admission and cancelled.
	waitClosed(t, sink.abortStarted, "the writer's abort")
	if _, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: "file:///unused/", FilePrefix: "part-late", Schema: testSchema,
	}); err == nil {
		t.Fatal("OpenTableWriter while Close waits succeeded, want an error")
	}
	if isClosed(closed) {
		t.Fatal("Close returned while the admitted writer's abort was blocked")
	}
	close(sink.release)
	waitClosed(t, closed, "Close returning")
	if !isClosed(w.done) || !sink.aborted.Load() {
		t.Fatal("Close returned before the admitted writer's cleanup finished")
	}
}

// TestPlaneCloseDuringAdmission: Close starts while OpenTableWriter is
// admitting a writer (from the admission hook, under the plane lock). The
// admitted writer's upload abort is held; Close must wait for it, so at the
// moment Close returns the writer has finished and its abort completed.
func TestPlaneCloseDuringAdmission(t *testing.T) {
	p := New(Config{})
	sink := &slowAbortSink{release: make(chan struct{}), abortStarted: make(chan struct{})}
	opened := make(chan struct{})
	p.openSink = func(ctx context.Context, _ map[string]string, _ string, _ *blob.WriterOptions) (io.WriteCloser, error) {
		sink.ctx = ctx
		close(opened)
		return sink, nil
	}
	rec := batchWith(memory.NewGoAllocator(), testSchema, 0, 10)
	defer rec.Release()

	var admitted *writer
	var doneAtReturn, abortedAtReturn bool
	closed := make(chan struct{})
	p.admitHook = func(w *writer) {
		admitted = w
		// Close starts now; it blocks on the plane lock this call holds.
		go func() {
			_ = p.Close()
			doneAtReturn, abortedAtReturn = isClosed(w.done), sink.aborted.Load()
			close(closed)
		}()
		// Give the writer an open upload before admission completes, so its
		// cleanup is an abort Close has to wait for.
		if err := w.Write(context.Background(), rec); err != nil {
			t.Errorf("Write during admission: %v", err)
			return
		}
		waitClosed(t, opened, "the writer opening its sink")
	}
	tw, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: "file:///unused/", FilePrefix: "part-admit", Schema: testSchema,
	})
	if err != nil || tw != admitted {
		t.Fatalf("OpenTableWriter = %v, %v; want the admitted writer", tw, err)
	}

	waitClosed(t, sink.abortStarted, "the writer's abort")
	if isClosed(closed) {
		t.Fatal("Close returned while the admitted writer's abort was blocked")
	}
	if _, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: "file:///unused/", FilePrefix: "part-late", Schema: testSchema,
	}); err == nil {
		t.Fatal("OpenTableWriter while Close waits succeeded, want an error")
	}
	close(sink.release)
	waitClosed(t, closed, "Close returning")
	if !doneAtReturn || !abortedAtReturn {
		t.Fatalf("at Close's return: writer finished %v, abort finished %v; want both", doneAtReturn, abortedAtReturn)
	}
	if n := sink.closes.Load(); n != 1 {
		t.Errorf("sink closed %d times, want 1", n)
	}
}

// TestPlaneCloseRacesOpenTableWriter (supplementary stress): writers opened
// concurrently with Close are either rejected or admitted; every admitted
// one has finished by the time the openers are joined and accepts no more
// batches. Under -race, an admission escaping Close's wait is also a
// WaitGroup misuse. TestPlaneCloseDuringAdmission is the deterministic case.
func TestPlaneCloseRacesOpenTableWriter(t *testing.T) {
	const openers = 8
	rec := batchWith(memory.NewGoAllocator(), testSchema, 0, 1)
	defer rec.Release()
	for i := 0; i < 50; i++ {
		p := New(Config{})
		start := make(chan struct{})
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			admitted []*writer
			rejected int
		)
		for range openers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				tw, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
					DataPrefix: "file:///unused/", FilePrefix: "part-race", Schema: testSchema,
				})
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					rejected++
					return
				}
				admitted = append(admitted, tw.(*writer))
			}()
		}
		close(start)
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// Openers still running past Close can only be rejected.
		wg.Wait()
		if got := len(admitted) + rejected; got != openers {
			t.Fatalf("iteration %d: %d admitted + %d rejected != %d", i, len(admitted), rejected, openers)
		}
		for _, w := range admitted {
			if !isClosed(w.done) {
				t.Fatalf("iteration %d: an admitted writer is still running after Close returned", i)
			}
			if err := w.Write(context.Background(), rec); err == nil {
				t.Fatalf("iteration %d: Write on an admitted writer after Close succeeded", i)
			}
		}
	}
}

// TestWriterAbortRacesPlaneClose: a writer's Abort and plane Close running
// together clean the writer's upload up exactly once, and both return once
// that cleanup is done.
func TestWriterAbortRacesPlaneClose(t *testing.T) {
	for _, order := range []string{"Abort first", "Close first", "together"} {
		t.Run(order, func(t *testing.T) {
			p := New(Config{})
			sink, w := openWriterWithSlowAbort(t, p)
			aborted, closed := make(chan struct{}), make(chan struct{})
			abort := func() {
				defer close(aborted)
				w.Abort()
			}
			planeClose := func() {
				defer close(closed)
				_ = p.Close()
			}
			switch order {
			case "Abort first":
				go abort()
				waitClosed(t, sink.abortStarted, "the writer's abort")
				go planeClose()
			case "Close first":
				go planeClose()
				waitClosed(t, sink.abortStarted, "the writer's abort")
				go abort()
			default:
				start := make(chan struct{})
				go func() { <-start; abort() }()
				go func() { <-start; planeClose() }()
				close(start)
				waitClosed(t, sink.abortStarted, "the writer's abort")
			}
			// The upload abort is blocked, so neither may have returned.
			if isClosed(aborted) || isClosed(closed) {
				t.Fatal("Abort or Close returned while the upload abort was blocked")
			}
			close(sink.release)
			waitClosed(t, aborted, "Abort returning")
			waitClosed(t, closed, "Close returning")
			if !sink.aborted.Load() {
				t.Error("the upload abort did not finish")
			}
			if n := sink.closes.Load(); n != 1 {
				t.Errorf("sink closed %d times, want exactly 1", n)
			}
			rec := batchWith(memory.NewGoAllocator(), testSchema, 0, 1)
			defer rec.Release()
			if err := w.Write(context.Background(), rec); !errors.Is(err, errAborted) && !errors.Is(err, errInterrupted) {
				t.Errorf("Write after Abort and Close = %v, want aborted or interrupted", err)
			}
		})
	}
}
