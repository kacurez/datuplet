//go:build duckdb_arrow

package duckplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/datuplet/datuplet/gateway/duck"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

var errReaderClosed = errors.New("duckplane: reader is closed")

// reader streams SELECT * of one catalog table on its own connection.
//
// Context ownership: ctx is the reader's lifetime (derived from the plane,
// cancelled by Close, Interrupt or plane Close); the ctx of a Next call only
// bounds that call.
type reader struct {
	p      *Plane
	conn   *duck.Conn
	stream *duck.Stream
	ctx    context.Context
	cancel context.CancelCauseFunc

	// op is a 1-slot semaphore serializing Next and Close; waiting for it is
	// bounded by the caller's ctx.
	op     chan struct{}
	closed bool

	live    atomic.Int64 // handed-out batches not yet released
	maxLive atomic.Int64
}

var _ datagateway.RecordStream = (*reader)(nil)

// OpenTableReader ATTACHes the catalog on first use and starts a streaming
// SELECT * of spec's table on a new connection. ctx bounds only this call.
func (p *Plane) OpenTableReader(ctx context.Context, spec datagateway.ReaderSpec) (datagateway.RecordStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if p.cfg.Catalog.URL == "" {
		return nil, errNoCatalog
	}
	if spec.Namespace == "" || spec.Table == "" {
		return nil, errors.New("duckplane: reader needs a namespace and a table")
	}
	if strings.ContainsRune(spec.Namespace, 0) || strings.ContainsRune(spec.Table, 0) {
		return nil, errors.New("duckplane: namespace or table name contains a NUL byte")
	}
	if err := p.attach(ctx); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPlaneClosed
	}
	conn, err := p.db.Connect()
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("duckplane: %w", err)
	}
	rctx, cancel := context.WithCancelCause(p.ctx)
	r := &reader{p: p, conn: conn, ctx: rctx, cancel: cancel, op: make(chan struct{}, 1)}
	r.op <- struct{}{} // held until the query is running: Close waits for it
	p.readers[r] = struct{}{}
	p.mu.Unlock()

	sql := "SELECT * FROM " + duck.Ident(catalogAlias) + "." + duck.Ident(spec.Namespace) + "." + duck.Ident(spec.Table)
	err = r.call(ctx, func(cctx context.Context) error {
		var qerr error
		r.stream, qerr = conn.QueryStream(cctx, sql)
		return qerr
	})
	<-r.op
	if err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// call runs fn under a context cancelled by ctx and by the end of the
// reader's lifetime, and prefers the lifetime's cause in the error.
func (r *reader) call(ctx context.Context, fn func(context.Context) error) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	err := fn(cctx)
	if err == nil || errors.Is(err, io.EOF) {
		return err
	}
	if cause := context.Cause(r.ctx); cause != nil {
		return cause
	}
	return fmt.Errorf("duckplane: read: %w", err)
}

// Schema is the table's Arrow schema as DuckDB returns it.
func (r *reader) Schema() *arrow.Schema { return r.stream.Schema() }

// Next returns the next batch, owned by the caller, or io.EOF after the last.
// A ctx that ends during the fetch fails the reader (DuckDB's query is
// interrupted); a ctx that is already done leaves the reader untouched.
func (r *reader) Next(ctx context.Context) (arrow.RecordBatch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("duckplane: read: %w", err)
	}
	select {
	case r.op <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("duckplane: waiting for the reader: %w", ctx.Err())
	}
	defer func() { <-r.op }()
	if cause := context.Cause(r.ctx); cause != nil {
		return nil, cause // errReaderClosed after Close
	}
	var rec arrow.RecordBatch
	err := r.call(ctx, func(cctx context.Context) error {
		var ferr error
		rec, ferr = r.stream.NextCtx(cctx)
		return ferr
	})
	if err != nil {
		return nil, err
	}
	rec.Retain() // the stream drops its own reference at the next fetch
	n := r.live.Add(1)
	for {
		m := r.maxLive.Load()
		if n <= m || r.maxLive.CompareAndSwap(m, n) {
			break
		}
	}
	b := &batch{RecordBatch: rec, live: &r.live}
	b.refs.Store(1)
	return b, nil
}

// liveHighWater is the most handed-out batches alive at once (tests).
func (r *reader) liveHighWater() int64 { return r.maxLive.Load() }

// interrupt fails the reader: a running fetch is interrupted. Safe from any
// goroutine.
func (r *reader) interrupt(cause error) {
	r.cancel(cause)
	r.conn.Interrupt()
}

// Close interrupts a running Next, releases the stream and closes the
// connection. Batches already handed out stay valid. Idempotent.
func (r *reader) Close() error {
	r.cancel(errReaderClosed)
	r.op <- struct{}{}
	defer func() { <-r.op }()
	if r.closed {
		return nil
	}
	r.closed = true
	if r.stream != nil {
		r.stream.Release()
	}
	r.conn.Close()
	r.p.unregisterReader(r)
	return nil
}

// batch is a handed-out record batch; its last Release updates the reader's
// count of live batches. Its buffers are owned by the batch, not the stream.
type batch struct {
	arrow.RecordBatch
	refs atomic.Int64
	live *atomic.Int64
}

func (b *batch) Retain() { b.refs.Add(1) }

func (b *batch) Release() {
	if b.refs.Add(-1) == 0 {
		b.RecordBatch.Release()
		b.live.Add(-1)
	}
}
