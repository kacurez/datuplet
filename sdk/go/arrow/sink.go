package arrow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// stringifyValue renders one decoded JSON value as an Arrow String cell.
// Returns (cell, isNull). json.Number keeps its exact source text; nested
// objects/arrays become compact JSON.
func stringifyValue(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", true
	case string:
		return x, false
	case json.Number:
		return x.String(), false
	case bool:
		return strconv.FormatBool(x), false
	case float64: // defensive: non-UseNumber callers
		return strconv.FormatFloat(x, 'g', -1, 64), false
	default: // map[string]any, []any
		b, err := json.Marshal(x)
		if err != nil {
			return "", true
		}
		return string(b), false
	}
}

// columnPlan fixes the output column names and how to pull each column's
// value out of a decoded record.
type columnPlan struct {
	names   []string
	extract func(rec map[string]any, i int) any
}

// planFromBatch derives the plan when no explicit columns are given: the
// sorted union of top-level keys across the first batch (matching the JSONL
// path's gateway inference, which collected field names from all objects in
// the first chunk).
func planFromBatch(batch []map[string]any) *columnPlan {
	set := make(map[string]bool)
	for _, rec := range batch {
		for k := range rec {
			set[k] = true
		}
	}
	names := make([]string, 0, len(set))
	for k := range set {
		names = append(names, k)
	}
	sort.Strings(names)
	return &columnPlan{
		names: names,
		extract: func(rec map[string]any, i int) any {
			return rec[names[i]]
		},
	}
}

// DefaultBatchRows is the per-batch row count a StringSink accumulates
// before flushing a self-contained Arrow IPC stream. Same empirical sweet
// spot data-generator uses (components/data-generator/arrow_writer.go).
const DefaultBatchRows = 8192

// ChunkWriter is the slice of *sdk.Writer a StringSink needs; narrowed for
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

// StringSink accumulates records into an all-String Arrow RecordBuilder and
// flushes a self-contained IPC stream per batch via ONE writer.Write call
// each (the SDK sends Arrow-IPC writes immediately — one POST per call — so
// the gateway never sees concatenated IPC streams).
//
// Columns: WithColumns fixes them explicitly; otherwise the sink infers them
// as the sorted union of top-level keys across the first batch, fixed for
// the run (later records project onto that set: missing key → null, unseen
// key → ignored).
//
// The writer is opened lazily on the first flush: zero records = no writer,
// letting callers preserve their commit-empty path.
type StringSink struct {
	ctx       context.Context
	open      func() (ChunkWriter, error)
	writer    ChunkWriter
	plan      *columnPlan
	pending   []map[string]any // buffered records while plan is unknown
	alloc     memory.Allocator
	builder   *array.RecordBuilder
	batchRows int
	inBatch   int
	rows      int64
}

// SinkOption configures a StringSink at construction.
type SinkOption func(*StringSink)

// WithColumns fixes the output columns explicitly: names in declared order,
// each row's value for column i pulled by extract(rec, i). names is copied,
// so later caller mutation cannot desynchronize the plan from the built
// schema (public API defensiveness).
func WithColumns(names []string, extract func(rec map[string]any, i int) any) SinkOption {
	return func(s *StringSink) {
		s.plan = &columnPlan{names: append([]string(nil), names...), extract: extract}
	}
}

// WithBatchRows overrides DefaultBatchRows (values <= 0 are ignored).
func WithBatchRows(n int) SinkOption {
	return func(s *StringSink) {
		if n > 0 {
			s.batchRows = n
		}
	}
}

// NewStringSink builds a sink whose writer is opened lazily via open on the
// first flush.
func NewStringSink(ctx context.Context, open func() (ChunkWriter, error), opts ...SinkOption) *StringSink {
	s := &StringSink{ctx: ctx, open: open, alloc: memory.NewGoAllocator(), batchRows: DefaultBatchRows}
	for _, opt := range opts {
		opt(s)
	}
	if s.plan != nil {
		s.adoptPlan(s.plan)
	}
	return s
}

// adoptPlan fixes the schema and creates the builder.
func (s *StringSink) adoptPlan(p *columnPlan) {
	s.plan = p
	fieldsArr := make([]arrow.Field, len(p.names))
	for i, n := range p.names {
		fieldsArr[i] = arrow.Field{Name: n, Type: arrow.BinaryTypes.String, Nullable: true}
	}
	s.builder = array.NewRecordBuilder(s.alloc, arrow.NewSchema(fieldsArr, nil))
}

// Add appends one record, flushing a batch when full.
func (s *StringSink) Add(rec map[string]any) error {
	if s.plan == nil {
		s.pending = append(s.pending, rec)
		if len(s.pending) < s.batchRows {
			return nil
		}
		s.adoptPlan(planFromBatch(s.pending))
		for _, p := range s.pending {
			s.appendRow(p)
		}
		s.pending = nil
		return s.flush()
	}
	s.appendRow(rec)
	if s.inBatch >= s.batchRows {
		return s.flush()
	}
	return nil
}

func (s *StringSink) appendRow(rec map[string]any) {
	for i := range s.plan.names {
		cell, isNull := stringifyValue(s.plan.extract(rec, i))
		b := s.builder.Field(i).(*array.StringBuilder)
		if isNull {
			b.AppendNull()
		} else {
			b.Append(cell)
		}
	}
	s.inBatch++
	s.rows++
}

// flush serializes the current batch as a complete IPC stream (schema +
// record + EOS) and ships it with one Write call.
func (s *StringSink) flush() error {
	if s.inBatch == 0 {
		return nil
	}
	rec := s.builder.NewRecord() // builds + resets internal builders
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(s.alloc))
	if err := w.Write(rec); err != nil {
		w.Close() //nolint:errcheck
		return &WriteError{Err: fmt.Errorf("ipc write record: %w", err)}
	}
	if err := w.Close(); err != nil {
		return &WriteError{Err: fmt.Errorf("ipc close writer: %w", err)}
	}

	if s.writer == nil {
		wr, err := s.open()
		if err != nil {
			return &WriteError{Err: fmt.Errorf("failed to open writer: %w", err)}
		}
		s.writer = wr
	}
	if err := s.writer.Write(s.ctx, buf.Bytes()); err != nil {
		return &WriteError{Err: fmt.Errorf("failed to write IPC batch: %w", err)}
	}
	s.inBatch = 0
	return nil
}

// Finish flushes the partial batch (deriving the plan first if it is still
// pending) and closes the writer when one was opened. Zero records: no
// writer was opened and (0, nil, nil) is returned.
func (s *StringSink) Finish() (int64, *sdk.CloseResult, error) {
	if s.plan == nil && len(s.pending) > 0 {
		s.adoptPlan(planFromBatch(s.pending))
		for _, p := range s.pending {
			s.appendRow(p)
		}
		s.pending = nil
	}
	if err := s.flush(); err != nil {
		return s.rows, nil, err
	}
	if s.builder != nil {
		s.builder.Release()
		s.builder = nil
	}
	if s.writer == nil {
		return 0, nil, nil
	}
	cr, err := s.writer.Close(s.ctx)
	if err != nil {
		return s.rows, nil, &WriteError{Err: fmt.Errorf("failed to close writer: %w", err)}
	}
	return s.rows, cr, nil
}

// Writer exposes the opened writer (nil when no rows were written).
func (s *StringSink) Writer() ChunkWriter { return s.writer }
