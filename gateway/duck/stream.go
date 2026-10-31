//go:build duckdb_arrow

package duck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	bindings "github.com/duckdb/duckdb-go-bindings"
)

// ErrStreamReleased is returned by NextCtx after Release.
var ErrStreamReleased = errors.New("duck: stream is released")

var _ array.RecordReader = (*Stream)(nil)

// Stream is a DuckDB streaming query result read as Arrow record batches, one
// DuckDB data chunk per batch. A batch returned by NextCtx (or RecordBatch)
// is valid until the next NextCtx or Release; Retain it to keep it longer.
// Retained batches own their buffers and stay valid after Release.
type Stream struct {
	c      *Conn
	ps     bindings.PreparedStatement
	res    *bindings.Result
	opts   bindings.ArrowOptions
	schema *arrow.Schema

	refs atomic.Int64

	// closing is cancelled by Release before it waits for s.mu. Every fetch
	// runs under a context derived from it, so a fetch that is entering when
	// Release starts either sees closing and never enters DuckDB, or is
	// interrupted (and re-interrupted) by the interruptible helper.
	closing     context.Context
	closeCancel context.CancelFunc

	mu       sync.Mutex // guards everything below and the result handle
	cur      arrow.RecordBatch
	err      error // sticky: a failed fetch ends the stream
	done     bool
	released bool
	held     int // batches the stream currently references (0 or 1)
	maxHeld  int
}

func newStream(c *Conn, ps bindings.PreparedStatement, res *bindings.Result) (*Stream, error) {
	opts := bindings.ResultGetArrowOptions(res)
	schema, err := resultArrowSchema(res, opts)
	if err != nil {
		bindings.DestroyArrowOptions(&opts)
		bindings.DestroyResult(res)
		bindings.DestroyPrepare(&ps)
		return nil, err
	}
	s := &Stream{c: c, ps: ps, res: res, opts: opts, schema: schema}
	s.closing, s.closeCancel = context.WithCancel(context.Background())
	s.refs.Store(1)
	return s, nil
}

// resultArrowSchema builds the Arrow schema of a result's columns.
func resultArrowSchema(res *bindings.Result, opts bindings.ArrowOptions) (*arrow.Schema, error) {
	cc := bindings.ColumnCount(res)
	names := make([]string, cc)
	types := make([]bindings.LogicalType, cc)
	for i := range cc {
		names[i] = bindings.ColumnName(res, i)
		types[i] = bindings.ColumnLogicalType(res, i)
	}
	defer func() {
		for i := range types {
			bindings.DestroyLogicalType(&types[i])
		}
	}()
	schema, ed := bindings.NewArrowSchema(opts, types, names)
	if err := errorDataErr(ed); err != nil {
		return nil, err
	}
	return schema, nil
}

// errorDataErr converts and destroys a bindings ErrorData.
func errorDataErr(ed bindings.ErrorData) error {
	defer bindings.DestroyErrorData(&ed)
	if !bindings.ErrorDataHasError(ed) {
		return nil
	}
	return errors.New("duck: arrow conversion: " + bindings.ErrorDataMessage(ed))
}

// IsStreaming reports whether DuckDB produced a streaming (not materialized)
// result.
func (s *Stream) IsStreaming() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return false
	}
	return bindings.ResultIsStreaming(*s.res)
}

// Schema returns the Arrow schema of the result.
func (s *Stream) Schema() *arrow.Schema { return s.schema }

// NextCtx returns the next batch, or io.EOF after the last one. Cancelling
// ctx interrupts the fetch and fails the stream; the connection stays usable
// once the stream is released.
func (s *Stream) NextCtx(ctx context.Context) (arrow.RecordBatch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return nil, ErrStreamReleased
	}
	s.dropCurrent()
	if s.err != nil {
		return nil, s.err
	}
	if s.done {
		return nil, io.EOF
	}
	if testHookBeforeFetch != nil {
		testHookBeforeFetch()
	}
	rec, err := s.fetch(ctx)
	switch {
	case errors.Is(err, io.EOF):
		s.done = true
		return nil, io.EOF
	case err != nil:
		s.err = err
		return nil, err
	}
	s.cur = rec
	s.held++
	s.maxHeld = max(s.maxHeld, s.held)
	return rec, nil
}

// testHookBeforeFetch, when set by a test, runs in NextCtx after s.mu is
// taken and before the fetch starts.
var testHookBeforeFetch func()

func (s *Stream) fetch(callerCtx context.Context) (arrow.RecordBatch, error) {
	if s.closing.Err() != nil {
		return nil, ErrStreamReleased
	}
	ctx, cancel := context.WithCancel(callerCtx)
	defer cancel()
	stop := context.AfterFunc(s.closing, cancel)
	defer stop()
	var rec arrow.RecordBatch
	before := s.c.interrupts.Load()
	err := s.c.interruptible(ctx, func() error {
		ch := bindings.FetchChunk(*s.res)
		if ch.Ptr != nil && bindings.DataChunkGetSize(ch) > 0 {
			r, ed := bindings.DataChunkToArrowArray(s.opts, s.schema, ch)
			bindings.DestroyDataChunk(&ch)
			if err := errorDataErr(ed); err != nil {
				return err
			}
			rec = r
			return nil
		}
		bindings.DestroyDataChunk(&ch)
		// A NULL or empty chunk ends the stream: on success, or (as observed
		// on DuckDB 2.0.0) with an error message. An interrupt must never be
		// mistaken for the end of the data.
		if msg := bindings.ResultError(s.res); msg != "" {
			return duckErr(ctx, msg)
		}
		if err := ctx.Err(); err != nil {
			return duckErr(ctx, "fetch interrupted")
		}
		if s.c.interrupts.Load() != before {
			return errors.New("duck: fetch interrupted")
		}
		return io.EOF
	})
	if err != nil && rec != nil {
		rec.Release()
		rec = nil
	}
	if err != nil && callerCtx.Err() == nil && s.closing.Err() != nil {
		err = fmt.Errorf("%w: %w", ErrStreamReleased, err)
	}
	return rec, err
}

func (s *Stream) dropCurrent() {
	if s.cur != nil {
		s.cur.Release()
		s.cur = nil
		s.held--
	}
}

// maxHeldBatches is the high-water mark of batches the stream referenced at
// once (tests).
func (s *Stream) maxHeldBatches() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxHeld
}

// Retain adds a reference (array.RecordReader).
func (s *Stream) Retain() { s.refs.Add(1) }

// Release drops a reference; the last one frees the result, the current batch
// and the prepared statement and frees the connection for other statements.
// Safe at any point of the stream and idempotent past the last reference. If a
// NextCtx is in flight on another goroutine, Release interrupts it and waits
// for it to return.
func (s *Stream) Release() {
	if s.refs.Add(-1) <= 0 {
		s.close()
	}
}

func (s *Stream) close() {
	// Publish closing before waiting for s.mu: an in-flight or entering fetch
	// is interrupted or refuses to start, so the wait below is short.
	s.closeCancel()
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return
	}
	s.released = true
	s.dropCurrent()
	bindings.DestroyArrowOptions(&s.opts)
	bindings.DestroyResult(s.res)
	bindings.DestroyPrepare(&s.ps)
	s.mu.Unlock()

	s.c.mu.Lock()
	if s.c.stream == s {
		s.c.stream = nil
	}
	s.c.mu.Unlock()
}

// Next advances to the next batch (array.RecordReader).
func (s *Stream) Next() bool {
	_, err := s.NextCtx(context.Background())
	return err == nil
}

// RecordBatch returns the batch Next advanced to (array.RecordReader).
func (s *Stream) RecordBatch() arrow.RecordBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// Record returns RecordBatch().
//
// Deprecated: use RecordBatch.
func (s *Stream) Record() arrow.RecordBatch { return s.RecordBatch() }

// Err returns the error that ended the stream, nil at a clean end
// (array.RecordReader).
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
