package buffer

import (
	"fmt"
	"io"
	"sync"
)

// asyncWriter wraps an io.WriteCloser (the streaming GCS / S3 writer
// returned by StorageBackend.OpenObjectWriter) with a producer/consumer
// queue so the parquet encoder (the producer) can keep working while the
// previous chunk's HTTP PUT to the object store completes on a background
// goroutine.
//
// # Why
//
// Profiling run 99bba07f (50M-row write to GCS) showed the gateway pod
// idle ~88% of wall time and on-CPU dominated (61%) by Zstd compression
// inside flushRowGroup. The encoder was sitting blocked waiting for the
// underlying GCS resumable-upload Writer to flush its 4-MiB chunk
// synchronously over HTTP — each chunk costs ~50-200 ms in TLS+HTTP
// round-trip alone, and at 1.4 GB total payload (≈350 chunks) that
// dominates wall time.
//
// This wrapper decouples Write(p) (producer) from inner.Write (consumer)
// via a bounded channel of byte buffers. The producer accumulates into
// a per-instance staging buffer; once that fills, the buffer is handed
// off to the channel and the producer takes a fresh one. A single
// background goroutine drains the channel into the wrapped writer.
//
// # Memory bound
//
// Hard cap = (queueCap + 1) × bufSize. With queueCap=4 and bufSize=16
// MiB that is 80 MiB extra per active writer. There is only ever one
// active per-table writer (BufferManager) at a time, so the per-table
// blast radius is 80 MiB on top of whatever the parquet encoder + GCS
// writer themselves keep resident.
//
// # Back-pressure
//
// Sending on a full channel blocks the producer — this is the natural
// back-pressure path when GCS slows down. There is no separate signal /
// "is the queue full" check; the channel IS the contract.
//
// # Error handling
//
// The worker goroutine reports the first inner-Write error to a
// 1-buffer errs channel and stops draining further buffers. Subsequent
// producer Writes / the final Close observe the error and surface it.
// On Close we always drain the channel + wait for the worker exit
// before calling inner.Close so the upload is properly finalised.
//
// # Lifecycle
//
// NewAsyncWriter starts the worker goroutine. After Close returns, the
// instance is single-use; do not reuse it.
type asyncWriter struct {
	inner io.WriteCloser

	bufSize int
	pool    sync.Pool // *[]byte, capacity == bufSize, length 0

	buffers chan *[]byte
	errs    chan error
	done    chan struct{}

	mu     sync.Mutex
	cur    *[]byte // accumulating buffer (caller-side)
	closed bool
	err    error // first error, observed via Write / Close
}

// newAsyncWriter wraps inner with an async upload queue. queueCap controls
// how many full buffers can be in flight before the producer blocks.
// bufSize controls the per-buffer size (also the units in which the inner
// writer sees data — match this to the inner's chunk size for optimal
// HTTP behaviour).
func newAsyncWriter(inner io.WriteCloser, queueCap, bufSize int) *asyncWriter {
	if queueCap < 1 {
		queueCap = 1
	}
	if bufSize < 1 {
		bufSize = 4 * 1024 * 1024
	}
	a := &asyncWriter{
		inner:   inner,
		bufSize: bufSize,
		buffers: make(chan *[]byte, queueCap),
		errs:    make(chan error, 1),
		done:    make(chan struct{}),
	}
	a.pool.New = func() any {
		b := make([]byte, 0, bufSize)
		return &b
	}
	go a.uploadLoop()
	return a
}

// uploadLoop drains buffers and writes them to the inner writer. Exits
// when buffers channel is closed (drained) OR on first inner-Write error.
// Errors are reported via errs (non-blocking; first wins).
func (a *asyncWriter) uploadLoop() {
	defer close(a.done)
	for b := range a.buffers {
		if _, err := a.inner.Write(*b); err != nil {
			// Report first error, then drain remaining buffers without
			// writing them — we want the producer to observe the error
			// quickly and bail, but we can't deadlock by leaving items
			// in the channel.
			select {
			case a.errs <- err:
			default:
			}
			a.releaseBuffer(b)
			for drain := range a.buffers {
				a.releaseBuffer(drain)
			}
			return
		}
		a.releaseBuffer(b)
	}
}

// releaseBuffer returns a buffer to the pool with a fresh zero length.
func (a *asyncWriter) releaseBuffer(b *[]byte) {
	if b == nil {
		return
	}
	*b = (*b)[:0]
	a.pool.Put(b)
}

// pollError checks for a worker-reported error without blocking. Returns
// (nil, nil) if none, otherwise records and returns the error.
func (a *asyncWriter) pollError() error {
	if a.err != nil {
		return a.err
	}
	select {
	case err := <-a.errs:
		a.err = err
		return err
	default:
		return nil
	}
}

// Write copies p into the staging buffer; once full, hands off to the
// upload queue. Returns len(p), nil unless an earlier worker error
// surfaces here.
func (a *asyncWriter) Write(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return 0, fmt.Errorf("async writer is closed")
	}
	if err := a.pollError(); err != nil {
		return 0, err
	}

	written := 0
	for len(p) > 0 {
		if a.cur == nil {
			a.cur = a.pool.Get().(*[]byte)
			*a.cur = (*a.cur)[:0]
		}
		avail := a.bufSize - len(*a.cur)
		n := len(p)
		if n > avail {
			n = avail
		}
		*a.cur = append(*a.cur, p[:n]...)
		p = p[n:]
		written += n
		if len(*a.cur) >= a.bufSize {
			// Hand off; producer may block here if queue is full —
			// this is the intended back-pressure path.
			b := a.cur
			a.cur = nil
			// Release lock while we send so other goroutines aren't
			// starved on the (unlikely) parallel-Writer path, and so
			// the worker can pull from the channel even if we're held
			// here. The cur field is already nil; safe.
			a.mu.Unlock()
			a.buffers <- b
			a.mu.Lock()
			// Re-check error after releasing/reacquiring lock.
			if err := a.pollError(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// Close flushes the staging buffer, drains the upload queue, then closes
// the inner writer. Surfaces the first error encountered. Idempotent.
func (a *asyncWriter) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return a.err
	}
	a.closed = true

	// Flush staging buffer (may be a short final buffer).
	if a.cur != nil && len(*a.cur) > 0 {
		b := a.cur
		a.cur = nil
		a.mu.Unlock()
		a.buffers <- b
		a.mu.Lock()
	} else if a.cur != nil {
		// Empty cur — just return to pool.
		a.releaseBuffer(a.cur)
		a.cur = nil
	}
	a.mu.Unlock()

	// Signal end of stream + wait for worker to finish (or fail) all
	// in-flight uploads.
	close(a.buffers)
	<-a.done

	// Collect any error the worker reported after our last pollError.
	a.mu.Lock()
	_ = a.pollError()
	firstErr := a.err
	a.mu.Unlock()

	// Always call inner.Close so the underlying resumable upload is
	// finalised (or errored cleanly).
	closeErr := a.inner.Close()
	if firstErr != nil {
		return firstErr
	}
	return closeErr
}
