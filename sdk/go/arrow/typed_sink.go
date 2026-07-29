package arrow

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// DefaultBatchRows is the per-batch row count a Sink (or StringSink)
// accumulates before flushing a self-contained Arrow IPC stream. Same
// empirical sweet spot data-generator uses
// (components/data-generator/arrow_writer.go).
const DefaultBatchRows = 8192

// ChunkWriter is the slice of *sdk.Writer a Sink needs; narrowed for
// testability with a fake.
type ChunkWriter interface {
	Write(ctx context.Context, data []byte) error
	Close(ctx context.Context) (*sdk.CloseResult, error)
	Bucket() string
	Table() string
}

// WriteError marks writer-open/Write/Close failures (infrastructure) so
// callers can map them to application-error exits (sdk.ExitAppError) while
// their decode errors stay user errors.
type WriteError struct{ Err error }

func (e *WriteError) Error() string { return e.Err.Error() }
func (e *WriteError) Unwrap() error { return e.Err }

// sinkSettings collects the effect of every SinkOption. NewSink,
// NewStringSink, and NewInferringSink (inferring_sink.go) each start from
// their own DefaultBatchRows-seeded sinkSettings and apply the given opts:
// NewSink only ever reads batchRows back (it has no use for columns or
// typedColumns), NewStringSink reads columns, and NewInferringSink reads
// both columns (WithColumns: explicit names, inferred types) and
// typedColumns (WithTypedColumns: declared names AND types) — the two are
// mutually exclusive there.
type sinkSettings struct {
	batchRows int
	columns   *columnPlan
	// typedColumns holds WithTypedColumns' argument for InferringSink's
	// declared-columns mode (inferring_sink.go). A pointer so a
	// caller-supplied EMPTY slice still counts as "the option was given",
	// distinguishable from "not given" (nil) — the same reason columns
	// above is itself a pointer rather than a plain slice/value.
	typedColumns *[]TypedColumn
}

// SinkOption configures a Sink, StringSink, or InferringSink at
// construction. They share one option type so WithBatchRows behaves
// identically for all three; WithColumns (declared alongside StringSink) is
// meaningful to NewStringSink and NewInferringSink; WithTypedColumns
// (declared alongside InferringSink, inferring_sink.go) is meaningful only
// to NewInferringSink.
type SinkOption func(*sinkSettings)

// WithBatchRows overrides DefaultBatchRows (values <= 0 are ignored).
func WithBatchRows(n int) SinkOption {
	return func(s *sinkSettings) {
		if n > 0 {
			s.batchRows = n
		}
	}
}

// errSinkRowDoneAfterFinish guards a caller that keeps driving the per-row
// loop after Finish has already run: RowDone would otherwise try to flush
// against a builder Finish already released.
var errSinkRowDoneAfterFinish = errors.New("arrow: Sink.RowDone called after Finish")

// Sink is the generic Arrow-IPC batching core. It accumulates rows into a
// RecordBuilder up to batchRows, then flushes exactly one self-contained IPC
// stream (schema + record + EOS) per ChunkWriter.Write call: payloads are
// never concatenated, so every Write call is independently parseable as one
// complete stream on its own.
//
// The schema is fixed up front (unlike StringSink, which may infer its
// schema from the first batch). Callers append field values directly into
// Builder() — one value per field, per row — and then call RowDone() once
// per completed row. There is no per-row callback and no per-row allocation
// beyond what the Arrow builders themselves need, so this stays cheap on a
// hot path streaming millions of rows.
//
// The writer opens lazily on the first flush: if RowDone/Finish never
// produces a non-empty batch, open is never called, letting callers
// preserve a commit-empty path.
//
// Sticky failure: once a flush fails, the batch it had already consumed
// from the builder is unrecoverable (the builder was released as part of
// the failure), so every later RowDone/Finish call returns that same error
// instead of silently emitting a short batch or dropping rows without a
// trace.
type Sink struct {
	ctx    context.Context
	open   func() (ChunkWriter, error)
	writer ChunkWriter

	alloc   memory.Allocator
	builder *array.RecordBuilder

	batchRows int
	inBatch   int
	rows      int64

	finished    bool
	failed      error
	closeResult *sdk.CloseResult

	bytesShipped int64
}

// NewSink builds a Sink for schema, whose writer opens lazily via open on
// the first flush.
func NewSink(ctx context.Context, open func() (ChunkWriter, error), schema *arrow.Schema, opts ...SinkOption) *Sink {
	settings := &sinkSettings{batchRows: DefaultBatchRows}
	for _, opt := range opts {
		opt(settings)
	}
	alloc := memory.NewGoAllocator()
	return &Sink{
		ctx:       ctx,
		open:      open,
		alloc:     alloc,
		builder:   array.NewRecordBuilder(alloc, schema),
		batchRows: settings.batchRows,
	}
}

// Builder exposes the RecordBuilder the caller appends field values into
// directly (one value per schema field, per row) before calling RowDone.
// Returns nil once the sink has released its builder — after a sticky
// failure, or after Finish — so a caller that keeps appending despite a
// returned error fails fast on a nil dereference instead of silently
// touching a released builder.
func (s *Sink) Builder() *array.RecordBuilder { return s.builder }

// RowDone marks one row complete: the caller must already have appended
// exactly one value per schema field into Builder(). It increments the
// row/batch counters and, once the batch reaches batchRows, flushes it as
// one self-contained Arrow IPC stream via a single ChunkWriter.Write call.
// No per-row callback, no per-row allocation — this is the hot path.
//
// Sticky failure: once a flush fails, RowDone (like Finish) keeps returning
// that same error on every later call rather than attempting another flush.
func (s *Sink) RowDone() error {
	if s.failed != nil {
		return s.failed
	}
	if s.finished {
		return errSinkRowDoneAfterFinish
	}
	s.inBatch++
	s.rows++
	if s.inBatch >= s.batchRows {
		return s.flush()
	}
	return nil
}

// releaseBuilder idempotently releases the RecordBuilder (nil-safe).
func (s *Sink) releaseBuilder() {
	if s.builder != nil {
		s.builder.Release()
		s.builder = nil
	}
}

// fail marks the sink as failed (sticky), releases the builder, and returns
// err. Once failed, the sink stays failed: RowDone and Finish keep
// returning this same error rather than attempting further work.
func (s *Sink) fail(err error) error {
	if s.failed == nil {
		s.failed = err
	}
	s.releaseBuilder()
	return err
}

// flush serializes the current batch as a complete IPC stream (schema +
// record + EOS) and ships it with one Write call. The writer opens here,
// before builder.NewRecord(), so that an open failure leaves the batch
// intact in the builder rather than consuming it for nothing.
func (s *Sink) flush() error {
	if s.inBatch == 0 {
		return nil
	}
	if s.writer == nil {
		wr, err := s.open()
		if err != nil {
			return s.fail(&WriteError{Err: fmt.Errorf("failed to open writer: %w", err)})
		}
		s.writer = wr
	}
	rec := s.builder.NewRecord() // builds + resets internal builders
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(s.alloc))
	if err := w.Write(rec); err != nil {
		w.Close() //nolint:errcheck
		return s.fail(&WriteError{Err: fmt.Errorf("ipc write record: %w", err)})
	}
	if err := w.Close(); err != nil {
		return s.fail(&WriteError{Err: fmt.Errorf("ipc close writer: %w", err)})
	}

	payload := buf.Bytes()
	if err := s.writer.Write(s.ctx, payload); err != nil {
		return s.fail(&WriteError{Err: fmt.Errorf("failed to write IPC batch: %w", err)})
	}
	s.bytesShipped += int64(len(payload))
	s.inBatch = 0
	return nil
}

// Finish flushes any partial batch and closes the writer if one was ever
// opened. Zero rows ever added means the writer is never opened, and Finish
// returns (0, nil, nil). Idempotent: a second call returns the same result
// without double-closing the writer.
//
// Sticky failure: the failure is usually raised by a failed
// ChunkWriter.Write inside flush(), which means the writer had already been
// opened successfully. So Finish makes a best-effort attempt to close it
// exactly once — see closeAfterFailure — and then returns the original
// sticky error unchanged: a close error on an already-broken writer is
// noise next to the first, diagnostically useful failure.
func (s *Sink) Finish() (int64, *sdk.CloseResult, error) {
	if s.failed != nil {
		s.closeAfterFailure()
		return s.rows, nil, s.failed
	}
	if s.finished {
		return s.rows, s.closeResult, nil
	}
	defer s.releaseBuilder()

	if err := s.flush(); err != nil {
		return s.rows, nil, err
	}
	s.finished = true
	if s.writer == nil {
		return 0, nil, nil
	}
	cr, err := s.writer.Close(s.ctx)
	if err != nil {
		return s.rows, nil, s.fail(&WriteError{Err: fmt.Errorf("failed to close writer: %w", err)})
	}
	s.closeResult = cr
	return s.rows, cr, nil
}

// closeAfterFailure makes a best-effort attempt to close the writer exactly
// once after a sticky failure, discarding any close error: the original
// sticky failure returned by Finish is the diagnostically useful one, and a
// close error on an already-broken writer would only be noise in its place.
// No-op if no writer was ever opened (the lazy-open guarantee: a sink that
// failed before its first flush must not open or close anything) or if this
// has already run once (guarded by the same finished flag Finish otherwise
// uses to mark its own terminal state — reachable only here once s.failed
// is set, since Finish's normal success path is unreachable afterward).
func (s *Sink) closeAfterFailure() {
	if s.finished || s.writer == nil {
		return
	}
	s.finished = true
	_, _ = s.writer.Close(s.ctx)
}

// Writer exposes the opened writer (nil until the first successful flush
// opens one).
func (s *Sink) Writer() ChunkWriter { return s.writer }

// BytesShipped returns the running total of serialized IPC-stream bytes
// successfully shipped via ChunkWriter.Write across every flush so far
// (including the batch flushed by the most recent Finish). A failed Write
// does not advance this total. Load-bearing for callers enforcing a
// total-bytes budget from inside their per-row loop — e.g. data-generator's
// user-facing sizeInBytes limit, previously computed from len(data) per
// serialized batch — check this total the same way.
func (s *Sink) BytesShipped() int64 { return s.bytesShipped }
