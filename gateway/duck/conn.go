//go:build duckdb_arrow

package duck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	bindings "github.com/duckdb/duckdb-go-bindings"
)

var (
	// ErrClosed is returned by every operation on a closed Conn.
	ErrClosed = errors.New("duck: connection is closed")
	// ErrStreamOpen is returned when a statement is issued on a Conn whose
	// Stream has not been released: one QueryStream per connection at a time,
	// and nothing else runs on the connection while it is open.
	ErrStreamOpen = errors.New("duck: connection has an open stream")
)

// interruptInterval is how often a cancelled call re-asserts duckdb_interrupt
// while it is still inside DuckDB (DuckDB may clear the flag internally).
const interruptInterval = 100 * time.Millisecond

// Conn is one DuckDB connection. It is not safe for concurrent use, except
// Interrupt, which may be called from any goroutine.
type Conn struct {
	db   *DB
	conn bindings.Connection

	mu     sync.Mutex // guards closed, stream and the handle against Interrupt
	closed bool
	stream *Stream
	// views holds every registered arrow view's C stream until Close (see
	// arrowview.go for why a DROP never frees it). pendingDrops are released
	// views whose DROP has not run yet, retried before every statement.
	views        []*arrowView
	pendingDrops []string

	interrupts atomic.Uint64 // bumped by every Interrupt call
}

func (c *Conn) ready() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closed:
		return ErrClosed
	case c.db.unusable.Load():
		return ErrUnusable
	case c.stream != nil:
		return ErrStreamOpen
	}
	return nil
}

// duckErr builds the error for a failed DuckDB call. The DuckDB message may
// quote parts of the statement; callers holding secrets use ExecRedacted.
func duckErr(ctx context.Context, msg string) error {
	err := errors.New("duck: " + msg)
	if cerr := ctx.Err(); cerr != nil {
		return fmt.Errorf("%w: %w", cerr, err)
	}
	return err
}

// interruptible runs fn, which calls into DuckDB on c, and turns ctx
// cancellation into duckdb_interrupt for as long as fn runs. The interrupter
// goroutine has exited when interruptible returns, so it can never hit a later
// statement.
func (c *Conn) interruptible(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("duck: %w", err)
	}
	if ctx.Done() == nil {
		return fn()
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		t := time.NewTicker(interruptInterval)
		defer t.Stop()
		for {
			bindings.Interrupt(c.conn)
			select {
			case <-done:
				return
			case <-t.C:
			}
		}
	}()
	defer func() {
		close(done)
		<-exited
	}()
	return fn()
}

// prepare prepares exactly one statement. Multiple statements are rejected
// by DuckDB, so no call can smuggle in a second statement.
func (c *Conn) prepare(ctx context.Context, sql string) (bindings.PreparedStatement, error) {
	var ps bindings.PreparedStatement
	if bindings.Prepare(c.conn, sql, &ps) == bindings.StateError {
		msg := bindings.PrepareError(ps)
		bindings.DestroyPrepare(&ps)
		return ps, duckErr(ctx, msg)
	}
	return ps, nil
}

// execute prepares and executes sql. The caller owns the returned result and
// prepared statement and destroys both.
func (c *Conn) execute(ctx context.Context, sql string, streaming bool) (bindings.PreparedStatement, *bindings.Result, error) {
	var ps bindings.PreparedStatement
	var res *bindings.Result
	err := c.interruptible(ctx, func() error {
		var err error
		if ps, err = c.prepare(ctx, sql); err != nil {
			return err
		}
		var pend bindings.PendingResult
		var state bindings.State
		if streaming {
			state = bindings.PendingPreparedStreaming(ps, &pend)
		} else {
			state = bindings.PendingPrepared(ps, &pend)
		}
		defer bindings.DestroyPending(&pend)
		if state == bindings.StateError {
			return duckErr(ctx, bindings.PendingError(pend))
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("duck: %w", err)
		}
		r := new(bindings.Result)
		if bindings.ExecutePending(pend, r) == bindings.StateError {
			msg := bindings.ResultError(r)
			bindings.DestroyResult(r)
			return duckErr(ctx, msg)
		}
		res = r
		return nil
	})
	if err != nil {
		bindings.DestroyPrepare(&ps)
		return ps, nil, err
	}
	return ps, res, nil
}

func (c *Conn) exec(ctx context.Context, sql string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.ready(); err != nil {
		return err
	}
	c.dropPendingViews()
	ps, res, err := c.execute(ctx, sql, false)
	if err != nil {
		return err
	}
	bindings.DestroyResult(res)
	bindings.DestroyPrepare(&ps)
	return nil
}

// Exec runs one statement and discards its result. Cancelling ctx interrupts
// the statement. The error carries DuckDB's message, which can quote parts of
// sql: use ExecRedacted for statements that contain secrets.
func (c *Conn) Exec(ctx context.Context, sql string) error {
	return c.exec(ctx, sql)
}

// ExecRedacted is Exec for statements that carry secrets (CREATE SECRET,
// ATTACH with a token). On failure the error text is exactly
// "duck: <label> failed": neither the statement nor DuckDB's message (which
// can echo the statement) is included.
func (c *Conn) ExecRedacted(ctx context.Context, sql, label string) error {
	if err := c.exec(ctx, sql); err != nil {
		return errors.New("duck: " + label + " failed")
	}
	return nil
}

// QueryStream runs a query as a DuckDB streaming result and returns it as an
// Arrow record stream. ctx bounds only the prepare and execute calls; each
// NextCtx takes its own context. Nothing else may run on c until the Stream is
// released (ErrStreamOpen).
func (c *Conn) QueryStream(ctx context.Context, sql string) (*Stream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.ready(); err != nil {
		return nil, err
	}
	c.dropPendingViews()
	ps, res, err := c.execute(ctx, sql, true)
	if err != nil {
		return nil, err
	}
	s, err := newStream(c, ps, res)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.stream = s
	c.mu.Unlock()
	return s, nil
}

// QueryRows runs a query with a small result (stats rows, settings) and
// returns it fully materialized. Values are as arrow's GetOneForMarshal
// returns them: int64, float64, bool, string, ..., nil for NULL.
func (c *Conn) QueryRows(ctx context.Context, sql string) ([][]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.ready(); err != nil {
		return nil, err
	}
	c.dropPendingViews()
	ps, res, err := c.execute(ctx, sql, false)
	if err != nil {
		return nil, err
	}
	defer bindings.DestroyPrepare(&ps)
	defer bindings.DestroyResult(res)
	opts := bindings.ResultGetArrowOptions(res)
	defer bindings.DestroyArrowOptions(&opts)
	schema, err := resultArrowSchema(res, opts)
	if err != nil {
		return nil, err
	}
	var rows [][]any
	for {
		ch := bindings.FetchChunk(*res)
		if ch.Ptr == nil {
			break
		}
		if bindings.DataChunkGetSize(ch) == 0 {
			bindings.DestroyDataChunk(&ch)
			break
		}
		rec, ed := bindings.DataChunkToArrowArray(opts, schema, ch)
		bindings.DestroyDataChunk(&ch)
		if err := errorDataErr(ed); err != nil {
			return nil, err
		}
		rows = appendRows(rows, rec)
		rec.Release()
	}
	if msg := bindings.ResultError(res); msg != "" {
		return nil, duckErr(ctx, msg)
	}
	return rows, nil
}

func appendRows(rows [][]any, rec arrow.RecordBatch) [][]any {
	for r := 0; r < int(rec.NumRows()); r++ {
		row := make([]any, rec.NumCols())
		for col := range row {
			a := rec.Column(col)
			if a.IsNull(r) {
				continue
			}
			row[col] = detach(a.GetOneForMarshal(r))
		}
		rows = append(rows, row)
	}
	return rows
}

// detach deep-copies the string and binary values GetOneForMarshal returns:
// they alias the batch's C-allocated buffers, which rec.Release frees.
func detach(v any) any {
	switch v := v.(type) {
	case string:
		return strings.Clone(v)
	case []byte:
		return bytes.Clone(v)
	case json.RawMessage:
		return json.RawMessage(bytes.Clone(v))
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			out[strings.Clone(k)] = detach(e)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = detach(e)
		}
		return out
	}
	return v
}

// RegisterArrowView exposes r as a temporary view named name on this
// connection (duckdb_arrow_scan). The view reads r once: scan it in one
// statement. release releases r and drops the view; call it after the
// statement reading the view has finished, on this goroutine. If the DROP
// cannot run now (open Stream, failed statement, unusable DB) it is retried
// before the connection's next statement. The small exported C stream struct
// stays allocated until Close (see arrowview.go), so a view restored by a
// ROLLBACK fails its scan instead of reading freed memory.
func (c *Conn) RegisterArrowView(name string, r array.RecordReader) (release func(), err error) {
	if err := c.ready(); err != nil {
		return nil, err
	}
	c.dropPendingViews()
	return registerArrowView(c, name, r)
}

// releaseView detaches the caller's reader from the view's stream and drops
// the view now if the connection is free, else before its next statement.
// The C stream itself stays allocated until Close.
func (c *Conn) releaseView(v *arrowView) {
	v.reader.Release() // the reference registerArrowView took: detaches
	v.reader.detach()  // detach even if DuckDB still holds the export's reference
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.pendingDrops = append(c.pendingDrops, v.name)
	c.mu.Unlock()
	if c.ready() == nil {
		c.dropPendingViews()
	}
}

// dropPendingViews drops released views. A failed DROP stays pending and is
// retried before the next statement; memory safety does not depend on it.
// The caller has checked ready().
func (c *Conn) dropPendingViews() {
	c.mu.Lock()
	names := c.pendingDrops
	c.pendingDrops = nil
	c.mu.Unlock()
	if len(names) == 0 {
		return
	}
	var keep []string
	for _, name := range names {
		if err := c.dropView(name); err != nil {
			keep = append(keep, name)
		}
	}
	if len(keep) > 0 {
		c.mu.Lock()
		c.pendingDrops = append(keep, c.pendingDrops...)
		c.mu.Unlock()
	}
}

func (c *Conn) dropView(name string) error {
	if testHookDropView != nil {
		if err := testHookDropView(name); err != nil {
			return err
		}
	}
	ps, res, err := c.execute(context.Background(), "DROP VIEW IF EXISTS "+Ident(name), false)
	if err != nil {
		return err
	}
	bindings.DestroyResult(res)
	bindings.DestroyPrepare(&ps)
	return nil
}

// testHookDropView, when set by a test, can fail a view DROP.
var testHookDropView func(name string) error

// Interrupt asks DuckDB to abort the statement running on c. Safe from any
// goroutine; a no-op on a closed connection.
func (c *Conn) Interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.interrupts.Add(1)
	bindings.Interrupt(c.conn)
}

// Close releases an open Stream and disconnects. Idempotent.
func (c *Conn) Close() error {
	c.mu.Lock()
	s := c.stream
	c.mu.Unlock()
	if s != nil {
		s.close()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	bindings.Disconnect(&c.conn)
	// The connection's temporary views died with it; their streams are
	// unreachable now.
	for _, v := range c.views {
		v.free()
		v.reader.detach() // also for views their caller never released
	}
	c.views = nil
	c.pendingDrops = nil
	return nil
}
