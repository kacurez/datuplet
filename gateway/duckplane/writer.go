//go:build duckdb_arrow

package duckplane

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/datuplet/datuplet/gateway/duck"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

// channelBatches is the number of batches buffered between Write and COPY.
const channelBatches = 4

var (
	errAborted      = errors.New("duckplane: writer aborted")
	errWriterClosed = errors.New("duckplane: writer is closed")
	errCopyEnded    = errors.New("duckplane: COPY ended before its input")
)

// writer is one table writer: Write feeds batches through a bounded channel
// into a registered Arrow view that one COPY … TO '<file>.parquet' reads.
// A file ends (rotation) once rotateAt uncompressed bytes were fed into it;
// the next Write starts the next COPY with freshly checked credentials.
//
// Context ownership: ctx is the writer's lifetime (derived from the plane,
// cancelled by Abort, Interrupt, plane Close or a failed Close); the ctx of a
// Write or Close call only bounds that call's blocking.
type writer struct {
	p          *Plane
	id         uint64
	spec       datagateway.WriterSpec
	sel        string
	remote     bool
	rotateAt   int64
	secretName string
	conn       *duck.Conn
	ctx        context.Context
	cancel     context.CancelCauseFunc

	queued   atomic.Int64
	finished atomic.Bool // done, readable without mu

	mu         sync.Mutex // serializes Write, Close and Abort
	run        *copyRun
	fed        int64 // bytes fed into the current run
	seq        int   // next file number
	files      []datagateway.WrittenFile
	err        error // sticky: the writer failed
	secretMade bool
	done       bool // Close or Abort finished
	aborted    bool
	result     []datagateway.WrittenFile
	resultErr  error
}

// copyRun is one COPY producing one file.
type copyRun struct {
	path    string
	ch      chan item
	done    chan struct{} // closed when the COPY statement returned
	release func()        // releases the Arrow view; after done only
	rows    [][]any       // RETURN_STATS rows
	err     error
}

type item struct {
	rec  arrow.RecordBatch
	size int64
}

var _ datagateway.TableWriter = (*writer)(nil)

// Write feeds rec to the current COPY, starting one if needed. It blocks
// while the channel is full. rec is retained, not taken over.
func (w *writer) Write(ctx context.Context, rec arrow.RecordBatch) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if rec == nil {
		return errors.New("duckplane: nil record")
	}
	if !sameColumns(w.spec.Schema, rec.Schema()) {
		return errors.New("duckplane: record schema does not match the writer schema")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usable(); err != nil {
		return err
	}
	if w.run == nil {
		if err := w.startCopy(ctx); err != nil {
			if ctx.Err() != nil && w.ctx.Err() == nil {
				return err // the call was cancelled; the writer is fine
			}
			w.err = err
			return err
		}
	}
	run := w.run
	select {
	case <-run.done: // the COPY already failed: do not queue behind it
		w.err = w.copyError(run)
		return w.err
	default:
	}
	it := item{rec: rec, size: recordBytes(rec)}
	rec.Retain()
	w.queued.Add(it.size)
	select {
	case run.ch <- it:
	case <-run.done:
		w.dropItem(it)
		w.err = w.copyError(run)
		return w.err
	case <-w.ctx.Done():
		w.dropItem(it)
		w.err = context.Cause(w.ctx)
		return w.err
	case <-ctx.Done():
		w.dropItem(it)
		return fmt.Errorf("duckplane: write: %w", ctx.Err())
	}
	w.fed += it.size
	if w.fed >= w.rotateAt {
		if err := w.finishRun(ctx); err != nil {
			w.err = err
			return err
		}
	}
	return nil
}

func (w *writer) usable() error {
	switch {
	case w.aborted:
		return errAborted
	case w.done:
		return errWriterClosed
	case w.err != nil:
		return w.err
	case w.ctx.Err() != nil:
		w.err = context.Cause(w.ctx)
		return w.err
	}
	return nil
}

func (w *writer) dropItem(it item) {
	it.rec.Release()
	w.queued.Add(-it.size)
}

// QueuedBytes is the size of the batches waiting in the channel.
func (w *writer) QueuedBytes() int64 { return w.queued.Load() }

// startCopy checks the credentials, refreshes the writer's secret, registers
// the next Arrow view and starts its COPY in a goroutine.
func (w *writer) startCopy(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(w.ctx, cancel)
	defer stop()

	path := fmt.Sprintf("%s%s-%05d.parquet", w.spec.DataPrefix, w.spec.FilePrefix, w.seq)
	target, err := duck.LiteralChecked(path)
	if err != nil {
		return fmt.Errorf("duckplane: file path: %w", err)
	}
	if err := w.refreshSecret(cctx); err != nil {
		if cerr := context.Cause(w.ctx); cerr != nil {
			return cerr
		}
		return err
	}

	run := &copyRun{path: path, ch: make(chan item, channelBatches), done: make(chan struct{})}
	r := &chanReader{schema: w.spec.Schema, ch: run.ch, stop: w.ctx, queued: &w.queued}
	r.refs.Store(1)
	view := fmt.Sprintf("dgv_%d_%d", w.id, w.seq)
	release, err := w.conn.RegisterArrowView(view, r)
	r.Release() // the view holds its own reference
	if err != nil {
		return fmt.Errorf("duckplane: register view: %w", err)
	}
	run.release = release
	copySQL := "COPY (SELECT " + w.sel + " FROM " + duck.Ident(view) + ") TO " + target +
		" (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE " + strconv.FormatInt(w.spec.RowGroupRows, 10) +
		", RETURN_STATS true)"
	go func() {
		defer close(run.done)
		run.rows, run.err = w.conn.QueryRows(w.ctx, copySQL)
	}()
	w.run = run
	w.seq++
	w.fed = 0
	return nil
}

// refreshSecret replaces the writer's storage secret with the credentials
// current now. Local prefixes need none (unless a test hook is installed).
func (w *writer) refreshSecret(ctx context.Context) error {
	hook := w.p.secretExec
	if w.spec.Creds == nil || (!w.remote && hook == nil) {
		return nil
	}
	creds, err := currentCreds(ctx, w.spec.Creds)
	if err != nil {
		return err
	}
	// Scoped to this writer's own files: the longest matching scope wins, so
	// a COPY never picks up another writer's secret for the same table.
	sql, err := secretSQL(w.secretName, w.spec.DataPrefix+w.spec.FilePrefix, creds)
	if err != nil {
		return err
	}
	if hook != nil {
		return hook(ctx, sql)
	}
	if err := w.conn.ExecRedacted(ctx, sql, "create secret"); err != nil {
		return errSecret
	}
	w.secretMade = true
	return nil
}

// finishRun ends the current COPY's input, waits for it and records its file.
// ctx bounds the wait; when it expires the writer is failed.
func (w *writer) finishRun(ctx context.Context) error {
	run := w.run
	w.run = nil
	close(run.ch)
	select {
	case <-run.done:
	case <-ctx.Done():
		w.interrupt(fmt.Errorf("duckplane: close: %w", ctx.Err()))
		<-run.done
	}
	for it := range run.ch {
		w.dropItem(it)
	}
	run.release()
	if run.err != nil || w.ctx.Err() != nil {
		return w.copyError(run)
	}
	if len(run.rows) != 1 || len(run.rows[0]) < 3 {
		return fmt.Errorf("duckplane: COPY to %s returned %d stats rows", run.path, len(run.rows))
	}
	rows, ok1 := run.rows[0][1].(uint64)
	size, ok2 := run.rows[0][2].(uint64)
	if !ok1 || !ok2 {
		return fmt.Errorf("duckplane: COPY to %s: unexpected stats %T, %T", run.path, run.rows[0][1], run.rows[0][2])
	}
	w.files = append(w.files, datagateway.WrittenFile{Path: run.path, Rows: int64(rows), Bytes: int64(size)})
	return nil
}

// copyError is the error of a COPY that ended (or was stopped) early.
func (w *writer) copyError(run *copyRun) error {
	if cause := context.Cause(w.ctx); cause != nil {
		return cause
	}
	select {
	case <-run.done:
	default:
		return errCopyEnded
	}
	if run.err != nil {
		return fmt.Errorf("duckplane: COPY to %s: %w", run.path, run.err)
	}
	return errCopyEnded
}

// interrupt fails the writer: its reader stops and its statement is
// interrupted. Safe from any goroutine.
func (w *writer) interrupt(cause error) {
	w.cancel(cause)
	w.conn.Interrupt()
}

// Close ends the current COPY and returns every file written. Zero rows
// written means zero files. A second call returns the same result.
func (w *writer) Close(ctx context.Context) ([]datagateway.WrittenFile, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return w.result, w.resultErr
	}
	err := w.usable()
	if w.run != nil {
		if err != nil {
			w.interrupt(err)
		}
		if ferr := w.finishRun(ctx); err == nil {
			err = ferr
		}
	}
	w.finish()
	if err != nil {
		w.result, w.resultErr = nil, err
	} else {
		w.result = w.files
	}
	return w.result, w.resultErr
}

// Abort stops the current COPY (it fails, so no file is produced by it) and
// releases everything. Files of earlier rotations stay where they are and are
// never reported. Idempotent; a no-op after Close.
func (w *writer) Abort() {
	if w.finished.Load() {
		return
	}
	w.interrupt(errAborted) // unblocks a Write waiting on the channel
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return
	}
	w.aborted = true
	if w.run != nil {
		_ = w.finishRun(context.Background())
	}
	w.finish()
	w.result, w.resultErr = nil, errAborted
}

// finish drops the secret, closes the connection and leaves the plane.
func (w *writer) finish() {
	if w.secretMade {
		// Best effort: the secret is in memory only and dies with the plane.
		_ = w.conn.Exec(context.Background(), "DROP TEMPORARY SECRET IF EXISTS "+duck.Ident(w.secretName))
		w.secretMade = false
	}
	w.conn.Close()
	w.cancel(errWriterClosed)
	w.done = true
	w.finished.Store(true)
	w.p.unregister(w)
}

// chanReader is the array.RecordReader behind a writer's Arrow view: it
// yields the batches Write sends until the channel is closed (end of file) or
// the writer's lifetime ends (the scan, and so the COPY, fails).
type chanReader struct {
	refs   atomic.Int64
	schema *arrow.Schema
	ch     <-chan item
	stop   context.Context
	queued *atomic.Int64
	cur    arrow.RecordBatch
	err    error
}

var _ array.RecordReader = (*chanReader)(nil)

func (r *chanReader) Retain() { r.refs.Add(1) }

func (r *chanReader) Release() {
	if r.refs.Add(-1) == 0 && r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
}

func (r *chanReader) Schema() *arrow.Schema { return r.schema }

func (r *chanReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.err != nil {
		return false
	}
	if r.stop.Err() != nil {
		r.err = context.Cause(r.stop)
		return false
	}
	select {
	case it, ok := <-r.ch:
		if !ok {
			return false
		}
		r.queued.Add(-it.size)
		r.cur = it.rec
		return true
	case <-r.stop.Done():
		r.err = context.Cause(r.stop)
		return false
	}
}

func (r *chanReader) RecordBatch() arrow.RecordBatch { return r.cur }

// Record returns RecordBatch().
//
// Deprecated: use RecordBatch.
func (r *chanReader) Record() arrow.RecordBatch { return r.cur }

func (r *chanReader) Err() error { return r.err }

// recordBytes is the uncompressed size of rec: the bytes of every buffer its
// columns reference (like arrow/util.TotalRecordSize, without its deps).
func recordBytes(rec arrow.RecordBatch) int64 {
	var n int64
	for _, col := range rec.Columns() {
		n += dataBytes(col.Data())
	}
	return n
}

func dataBytes(d arrow.ArrayData) int64 {
	var n int64
	for _, b := range d.Buffers() {
		if b != nil {
			n += int64(b.Len())
		}
	}
	for _, c := range d.Children() {
		n += dataBytes(c)
	}
	return n // dictionary types are rejected (casts.go)
}
