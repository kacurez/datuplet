package arrow

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

// WithColumns fixes the output columns explicitly: names in declared order,
// each row's value for column i pulled by extract(rec, i). names is copied,
// so later caller mutation cannot desynchronize the plan from the built
// schema (public API defensiveness). Meaningful to NewStringSink and to
// NewInferringSink's explicit-names/inferred-types mode
// (inferring_sink.go); NewSink's schema is supplied directly and ignores
// this option.
func WithColumns(names []string, extract func(rec map[string]any, i int) any) SinkOption {
	return func(s *sinkSettings) {
		s.columns = &columnPlan{names: append([]string(nil), names...), extract: extract}
	}
}

// StringSink accumulates records into an all-String Arrow RecordBuilder and
// flushes a self-contained IPC stream per batch via ONE writer.Write call
// each. It is a thin façade over the generic Sink core: StringSink owns the
// column plan, the pending buffer used while that plan is still unknown,
// first-batch schema inference, cell stringification, and unknown-key
// tracking; batching, flush, and writer lifecycle are delegated to Sink.
//
// Columns: WithColumns fixes them explicitly; otherwise the sink infers them
// as the sorted union of top-level keys across the first batch, fixed for
// the run (later records project onto that set: missing key → null, unseen
// key → ignored). Because the schema may not be known until the first batch
// is inferred, the Sink core cannot be constructed until the plan is adopted
// (at WithColumns time, or at first-batch inference).
//
// The writer is opened lazily on the first flush: zero records = no writer,
// letting callers preserve their commit-empty path.
//
// Unknown keys: for an INFERRED plan (schema fixed from the first batch, no
// WithColumns), a later record carrying a key outside that fixed schema has
// that value silently dropped. UnknownKeys reports the (capped) distinct
// names and the exact affected-record count so callers can warn instead of
// losing data silently. Explicit WithColumns plans never track this — an
// extra key there is a deliberate projection choice, not a defect.
type StringSink struct {
	ctx       context.Context
	open      func() (ChunkWriter, error)
	batchRows int
	core      *Sink // constructed lazily once the column plan is adopted

	plan    *columnPlan
	pending []map[string]any // buffered records while plan is unknown

	finished bool

	inferredNames  map[string]struct{} // non-nil only when the plan was inferred (planFromBatch)
	unknownKeys    map[string]struct{} // distinct unknown key names, capped at MaxTrackedUnknownKeys
	unknownRecords int64               // records carrying >=1 unknown key (exact, never capped)
}

// MaxTrackedUnknownKeys bounds memory for StringSink.unknownKeys: a
// pathological feed with many distinct unexpected keys cannot grow the
// tracked-name set without bound. unknownRecords stays exact regardless.
// Exported so callers of UnknownKeys can tell whether the returned name list
// is the complete set or was truncated at the cap (len(keys) == MaxTrackedUnknownKeys).
const MaxTrackedUnknownKeys = 64

// NewStringSink builds a sink whose Sink core (and therefore its writer) is
// constructed lazily: at WithColumns time if given, otherwise once the
// column plan is inferred from the first batch.
func NewStringSink(ctx context.Context, open func() (ChunkWriter, error), opts ...SinkOption) *StringSink {
	settings := &sinkSettings{batchRows: DefaultBatchRows}
	for _, opt := range opts {
		opt(settings)
	}
	s := &StringSink{ctx: ctx, open: open, batchRows: settings.batchRows}
	if settings.columns != nil {
		// An explicit projection, not inferred.
		s.adoptPlan(settings.columns, false)
	}
	return s
}

// adoptPlan fixes the schema and constructs the Sink core. inferred marks a
// plan derived from planFromBatch (as opposed to an explicit WithColumns
// plan); only then does the sink track unknown-key drops (trackUnknownKeys /
// UnknownKeys) — an explicit plan's extra keys are a deliberate projection,
// not a defect worth warning about.
func (s *StringSink) adoptPlan(p *columnPlan, inferred bool) {
	s.plan = p
	fields := make([]arrow.Field, len(p.names))
	for i, n := range p.names {
		fields[i] = arrow.Field{Name: n, Type: arrow.BinaryTypes.String, Nullable: true}
	}
	s.core = NewSink(s.ctx, s.open, arrow.NewSchema(fields, nil), WithBatchRows(s.batchRows))
	if inferred {
		names := make(map[string]struct{}, len(p.names))
		for _, n := range p.names {
			names[n] = struct{}{}
		}
		s.inferredNames = names
	}
}

// Add appends one record, flushing a batch when full.
func (s *StringSink) Add(rec map[string]any) error {
	if s.core != nil && s.core.failed != nil {
		return s.core.failed
	}
	if s.finished {
		return errors.New("arrow: StringSink.Add called after Finish")
	}
	if s.plan == nil {
		s.pending = append(s.pending, rec)
		if len(s.pending) < s.batchRows {
			return nil
		}
		s.adoptPlan(planFromBatch(s.pending), true)
		pending := s.pending
		s.pending = nil
		for _, p := range pending {
			s.appendRow(p)
			if err := s.core.RowDone(); err != nil {
				return err
			}
		}
		return nil
	}
	s.appendRow(rec)
	return s.core.RowDone()
}

// appendRow stringifies rec's plan-selected values directly into the core's
// builder. Row/batch counters and flushing are the core's responsibility
// (via the paired RowDone call at each call site).
func (s *StringSink) appendRow(rec map[string]any) {
	b := s.core.Builder()
	for i := range s.plan.names {
		cell, isNull := stringifyValue(s.plan.extract(rec, i))
		fb := b.Field(i).(*array.StringBuilder)
		if isNull {
			fb.AppendNull()
		} else {
			fb.Append(cell)
		}
	}
	if s.inferredNames != nil {
		s.trackUnknownKeys(rec)
	}
}

// trackUnknownKeys records, for an inferred plan only, any of rec's keys
// that fall outside the fixed schema — those values were just silently
// dropped by appendRow (the schema was fixed from an earlier batch), and
// this is how callers learn about it via UnknownKeys. Allocation-free when
// every key is already known.
func (s *StringSink) trackUnknownKeys(rec map[string]any) {
	sawUnknown := false
	for k := range rec {
		if _, ok := s.inferredNames[k]; ok {
			continue
		}
		sawUnknown = true
		if _, seen := s.unknownKeys[k]; seen {
			continue
		}
		if len(s.unknownKeys) >= MaxTrackedUnknownKeys {
			continue
		}
		if s.unknownKeys == nil {
			s.unknownKeys = make(map[string]struct{})
		}
		s.unknownKeys[k] = struct{}{}
	}
	if sawUnknown {
		s.unknownRecords++
	}
}

// UnknownKeys returns the distinct key names (sorted, capped at
// MaxTrackedUnknownKeys) seen outside an INFERRED schema, plus the exact
// count of records that carried at least one such key. Always (nil, 0) for
// an explicit WithColumns plan: there the caller chose the projection
// deliberately, so extra keys are not a data-loss defect.
func (s *StringSink) UnknownKeys() (keys []string, records int64) {
	if len(s.unknownKeys) == 0 {
		return nil, s.unknownRecords
	}
	keys = make([]string, 0, len(s.unknownKeys))
	for k := range s.unknownKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, s.unknownRecords
}

// Finish flushes the partial batch (deriving the plan first if it is still
// pending) and closes the writer when one was opened. Zero records: no
// writer was opened and (0, nil, nil) is returned. Idempotent: the second
// call returns the same result without double-closing.
func (s *StringSink) Finish() (int64, *sdk.CloseResult, error) {
	if s.core != nil && s.core.failed != nil {
		// Route through the core's own Finish rather than returning its
		// failed/rows fields directly: that is what makes the core's
		// best-effort close-on-sticky-failure (Sink.closeAfterFailure)
		// actually run for this façade, instead of only for direct Sink
		// callers.
		return s.core.Finish()
	}
	if s.finished {
		if s.core == nil {
			return 0, nil, nil
		}
		return s.core.rows, s.core.closeResult, nil
	}
	if s.plan == nil && len(s.pending) > 0 {
		s.adoptPlan(planFromBatch(s.pending), true)
		pending := s.pending
		s.pending = nil
		for _, p := range pending {
			s.appendRow(p)
			if err := s.core.RowDone(); err != nil {
				// Same reasoning as above: go through Finish so a failure
				// discovered while replaying pending rows still gets the
				// best-effort close.
				return s.core.Finish()
			}
		}
	}
	s.finished = true
	if s.core == nil {
		return 0, nil, nil
	}
	return s.core.Finish()
}

// Writer exposes the opened writer (nil when no rows were written).
func (s *StringSink) Writer() ChunkWriter {
	if s.core == nil {
		return nil
	}
	return s.core.Writer()
}
