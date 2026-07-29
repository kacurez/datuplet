package arrow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// InferringSink accumulates records into a per-column-typed Arrow
// RecordBuilder and flushes a self-contained IPC stream per batch via ONE
// writer.Write call each — the same shape as StringSink. Unlike StringSink
// (which fixes an all-utf8 schema and stringifies every cell), InferringSink
// writes one Arrow type per column — Int64, Float64, Boolean, or String.
//
// It supports two mutually exclusive column-selection modes, chosen by
// which NewInferringSink option is supplied (see that function's doc for
// the precise rule and the error returned on conflict):
//
//  1. Typed INFERENCE (default; also the mode used when WithColumns names
//     the columns explicitly). The first batch is buffered, then a
//     per-column Arrow TYPE is inferred by joining the "kind" of every
//     sampled value across that batch (classifyValue / joinKind / the
//     lattice below this doc comment). This costs nothing extra: at the
//     moment a StringSink-style façade would adopt its plan, the buffered
//     records still hold their ORIGINAL decoded values (json.Number, bool,
//     string, map[string]any, []any, nil) because per-cell conversion
//     happens later — so full type information is available with no extra
//     buffering and no second pass. Column names, when not pinned via
//     WithColumns, are the sorted union of top-level keys across the first
//     batch, via the same planFromBatch StringSink uses, so the two façades
//     cannot drift on naming or column order. A later record's value that
//     no longer fits its column's now-fixed type is a *TypeViolationError
//     (see below); a later record's key outside the fixed name set is
//     silently dropped and reported via UnknownKeys.
//
//  2. Declared columns (WithTypedColumns): the caller supplies the full
//     column set up front — name AND type, e.g. sourced from a pipeline
//     config's `outputs.tables[].columns` — so nothing is sampled or
//     inferred. NO other record key is ever written, and unlike inference
//     mode, an out-of-mapping key is never reported via UnknownKeys either:
//     a declared mapping is a deliberate projection, not partial data loss
//     — the same precedent StringSink's own explicit WithColumns already
//     set. Each column's type string is resolved via the exported
//     ArrowTypeFor. The schema being known up front means the Sink core is
//     built immediately (no buffering at all), mirroring StringSink's
//     explicit-WithColumns path. A record value that does not coerce into
//     its column's declared type is also a *TypeViolationError, under
//     stricter rules than inference mode's lattice (see declaredValueFits).
//
// Whichever mode is in effect, every field is Nullable: true,
// unconditionally: the extractor cannot promise a key is present in every
// record, and a non-nullable Arrow column against an iceberg "optional"
// field (or vice versa) causes AddFiles rejections — see arrowFieldFor's
// doc comment in components/data-generator/arrow_writer.go for the concrete
// failure modes this class of mismatch caused in practice.
//
// Late type violations: once a column's type is fixed (by inference or by
// declaration) and the Sink core is built, a later value that does not fit
// it cannot be accommodated — earlier batches may already have shipped
// under that schema, and neither Iceberg nor the dev mock's drift lock
// tolerates a mid-writer schema change. Add returns a *TypeViolationError
// in that case: it is NOT a *WriteError, and the sink becomes stickily
// failed through the exact same mechanism a WriteError failure uses
// (Sink.fail) — every subsequent Add/Finish call keeps returning that same
// *TypeViolationError, and Finish still makes its usual best-effort attempt
// to close an already-opened writer. This sticky-failure choice is
// deliberate, not the only option available: a type violation is a user
// data-shape problem discovered mid-stream, potentially after earlier rows
// have already been unrecoverably shipped under the now-invalid schema, so
// treating the rest of the run as fatal — same as a WriteError — is the
// safer default; a caller wanting to skip only the offending row would need
// a different API shape than Add's single-error return. The expected
// caller (the component) exits immediately on any error from Add/Finish, so
// in practice sticky failure mostly affects predictability for a caller
// that (incorrectly) keeps driving the loop despite a returned error.
//
// The writer opens lazily on the first flush regardless of mode: zero
// records ever added means no writer is ever opened.
type InferringSink struct {
	ctx       context.Context
	open      func() (ChunkWriter, error)
	batchRows int
	core      *Sink // constructed lazily once names+types are adopted; immediately in declared mode

	plan     *columnPlan                        // names; may be fixed early (WithColumns/WithTypedColumns) or only alongside colTypes
	colTypes []arrow.DataType                   // index-aligned with plan.names; nil until known (sampled, or declared up front)
	pending  []map[string]any                   // buffered records while colTypes is still unknown (never populated in declared mode)
	fits     func(v any, t arrow.DataType) bool // mode-specific type-fit check, set once at construction

	finished bool

	inferredNames  map[string]struct{} // non-nil only when plan.names came from planFromBatch (nil for WithColumns and for declared mode)
	unknownKeys    map[string]struct{} // distinct unknown key names, capped at MaxTrackedUnknownKeys
	unknownRecords int64               // records carrying >=1 unknown key (exact, never capped)

	scratch []any // reused per-row buffer for the validate-then-append pass; avoids an allocation per Add
}

// errInferringSinkConflictingColumnOptions guards NewInferringSink against a
// caller supplying both WithTypedColumns and WithColumns: the two select
// different, incompatible column-selection modes (declared vs.
// explicit-names-with-inferred-types), so silently preferring one over the
// other would hide what is almost certainly a caller bug. Returned
// unwrapped (not joined with anything) from NewInferringSink.
var errInferringSinkConflictingColumnOptions = errors.New("arrow: NewInferringSink: WithTypedColumns and WithColumns are mutually exclusive")

// NewInferringSink builds a sink in exactly one of three mutually exclusive
// column-selection modes, chosen by which options are supplied:
//
//   - Declared columns (WithTypedColumns): the schema is fixed immediately
//     from the given columns, in declared order, with each column's Arrow
//     type resolved via ArrowTypeFor — no sampling, no buffering, no type
//     inference. The Sink core is constructed right here, before this
//     function returns.
//   - Explicit names, inferred types (WithColumns): column names are fixed
//     now, exactly as given; each column's Arrow TYPE is still inferred
//     from the first sampled batch of (projected) values, so the Sink core
//     is still deferred, same as the next mode.
//   - Full inference (neither option given): both the column NAMES (the
//     sorted union of top-level keys across the first batch, via the same
//     planFromBatch StringSink uses) and their TYPES are deferred to the
//     first sampled batch.
//
// WithTypedColumns and WithColumns are mutually exclusive — passing both
// returns a non-nil error (errInferringSinkConflictingColumnOptions,
// unexported but reachable via errors.Is if ever needed) and a nil
// *InferringSink, rather than silently preferring one. A WithTypedColumns
// entry whose Type does not resolve via ArrowTypeFor is also reported here,
// at construction time, rather than deferred to the first Add: declared
// columns are configuration data (see the package doc above), and a
// malformed configuration should fail before any row is processed, not
// mid-run. Neither failure mode panics.
//
// Callers must check the returned error before using the sink: on any
// error, the *InferringSink return is nil.
func NewInferringSink(ctx context.Context, open func() (ChunkWriter, error), opts ...SinkOption) (*InferringSink, error) {
	settings := &sinkSettings{batchRows: DefaultBatchRows}
	for _, opt := range opts {
		opt(settings)
	}
	if settings.columns != nil && settings.typedColumns != nil {
		return nil, errInferringSinkConflictingColumnOptions
	}
	s := &InferringSink{ctx: ctx, open: open, batchRows: settings.batchRows, fits: valueFitsArrowType}
	if settings.typedColumns != nil {
		plan, colTypes, err := planFromTypedColumns(*settings.typedColumns)
		if err != nil {
			return nil, err
		}
		s.plan = plan
		s.colTypes = colTypes
		s.fits = declaredValueFits
		s.buildCore()
		return s, nil
	}
	if settings.columns != nil {
		// Names fixed now; types are still deferred to the first batch.
		s.plan = settings.columns
	}
	return s, nil
}

// --- Mode 2: declared columns ---

// TypedColumn declares one output column's name and config-facing type
// string for NewInferringSink's declared-columns mode (WithTypedColumns).
// Type is resolved via the exported ArrowTypeFor; an unrecognized string is
// a construction-time error (see NewInferringSink), never a panic.
type TypedColumn struct {
	Name string
	Type string
}

// ArrowTypeFor maps a pipeline-config column type string to the Arrow type
// InferringSink's declared-columns mode (WithTypedColumns) builds its
// schema from. It is exported as the one canonical vocabulary so a
// component can pass a config's column type strings straight through
// without hand-rolling its own copy of this switch — and so a config typo
// produces one consistent error shape everywhere the vocabulary is used.
//
// Recognized, case-sensitively: "int" and "long" both resolve to Int64;
// "float" and "double" both resolve to Float64; "boolean" resolves to
// Boolean; "string" resolves to String. This is deliberately narrower than
// data-generator's arrowFieldFor vocabulary
// (components/data-generator/arrow_writer.go), which additionally accepts
// "uuid"/"date"/"timestamp"/"now" as String — those are data-generator's
// own random-value-shape hints, not part of the declared-columns config
// vocabulary. Any other string returns a non-nil error naming it; this
// function never panics.
func ArrowTypeFor(t string) (arrow.DataType, error) {
	switch t {
	case "int", "long":
		return arrow.PrimitiveTypes.Int64, nil
	case "float", "double":
		return arrow.PrimitiveTypes.Float64, nil
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "string":
		return arrow.BinaryTypes.String, nil
	default:
		return nil, fmt.Errorf("arrow: unrecognized column type %q (want one of: int, long, float, double, boolean, string)", t)
	}
}

// WithTypedColumns selects InferringSink's declared-columns mode: see
// NewInferringSink's doc comment for the full mode description and its
// (mutually exclusive) interaction with WithColumns. cols is copied, so
// later caller mutation cannot desynchronize the option from the built
// schema — the same public-API defensiveness WithColumns already applies.
func WithTypedColumns(cols []TypedColumn) SinkOption {
	return func(s *sinkSettings) {
		cp := append([]TypedColumn(nil), cols...)
		s.typedColumns = &cp
	}
}

// planFromTypedColumns builds the declared-mode column plan — names, in
// declared order, each extracted by direct map lookup (a missing key
// yields nil, i.e. Arrow null, exactly like planFromBatch's extract) — and
// resolves every column's Arrow type via ArrowTypeFor up front. Returns the
// first unrecognized type string as an error, naming the offending column:
// a declared column set is configuration data, so a bad type name is a
// config error to surface immediately, not a runtime data problem to
// discover mid-run.
func planFromTypedColumns(cols []TypedColumn) (*columnPlan, []arrow.DataType, error) {
	names := make([]string, len(cols))
	types := make([]arrow.DataType, len(cols))
	for i, c := range cols {
		t, err := ArrowTypeFor(c.Type)
		if err != nil {
			return nil, nil, fmt.Errorf("arrow: declared column %q: %w", c.Name, err)
		}
		names[i] = c.Name
		types[i] = t
	}
	plan := &columnPlan{
		names: names,
		extract: func(rec map[string]any, i int) any {
			return rec[names[i]]
		},
	}
	return plan, types, nil
}

// errInferringSinkAddAfterFinish guards a caller that keeps calling Add
// after Finish has already run.
var errInferringSinkAddAfterFinish = errors.New("arrow: InferringSink.Add called after Finish")

// Add appends one record, buffering it while the schema (names and types)
// is still being sampled, then flushing a batch when full — the same
// buffer-then-adopt structure as StringSink.Add. In declared-columns mode
// s.colTypes is already non-nil from construction, so every Add call goes
// straight to the append path below with no buffering at all. Returns the
// sink's sticky error unchanged once one is set: either a *WriteError
// (writer open/Write/Close failure) or a *TypeViolationError (a value that
// does not fit its column's already-fixed type).
func (s *InferringSink) Add(rec map[string]any) error {
	if s.core != nil && s.core.failed != nil {
		return s.core.failed
	}
	if s.finished {
		return errInferringSinkAddAfterFinish
	}
	if s.colTypes == nil {
		s.pending = append(s.pending, rec)
		if len(s.pending) < s.batchRows {
			return nil
		}
		return s.adoptFromPending()
	}
	return s.appendAndRowDone(rec)
}

// adoptFromPending fixes the plan (deriving names via planFromBatch if
// WithColumns was not given — the same first batch in either case), infers
// one Arrow type per column from that batch, builds the Sink core, then
// replays every pending record through the normal append path. Unreachable
// in declared-columns mode: there, colTypes is never nil, so Add never
// buffers into s.pending in the first place.
func (s *InferringSink) adoptFromPending() error {
	if s.plan == nil {
		s.plan = planFromBatch(s.pending)
		names := make(map[string]struct{}, len(s.plan.names))
		for _, n := range s.plan.names {
			names[n] = struct{}{}
		}
		s.inferredNames = names
	}
	s.colTypes = inferTypesFromBatch(s.plan, s.pending)
	s.buildCore()
	pending := s.pending
	s.pending = nil
	for _, rec := range pending {
		if err := s.appendAndRowDone(rec); err != nil {
			return err
		}
	}
	return nil
}

// buildCore constructs the Sink core from the now-fixed plan/colTypes.
// Every field is Nullable: true, unconditionally — see the InferringSink
// doc comment for why that is a forced design decision, not a default.
// Shared by both modes: inference mode calls this from adoptFromPending
// (after sampling), declared mode calls it directly from NewInferringSink
// (immediately, since the schema needs no sampling).
func (s *InferringSink) buildCore() {
	fields := make([]arrow.Field, len(s.plan.names))
	for i, n := range s.plan.names {
		fields[i] = arrow.Field{Name: n, Type: s.colTypes[i], Nullable: true}
	}
	s.core = NewSink(s.ctx, s.open, arrow.NewSchema(fields, nil), WithBatchRows(s.batchRows))
}

// appendAndRowDone appends one row into the core's builder and drives the
// paired RowDone call, mirroring StringSink's appendRow-then-RowDone call
// sites.
func (s *InferringSink) appendAndRowDone(rec map[string]any) error {
	if err := s.appendRow(rec); err != nil {
		return err
	}
	return s.core.RowDone()
}

// appendRow validates every column's value against its fixed type BEFORE
// appending anything — a two-pass approach. A violation discovered midway
// through a row must not leave some of that row's field builders one
// element longer than others, which would desynchronize every later row in
// the batch; validating the whole row first makes that impossible.
//
// The fit check is mode-specific (s.fits: valueFitsArrowType's permissive
// lattice for inference mode, or declaredValueFits' stricter rules for
// declared mode — set once at construction), but the append itself
// (appendValueToBuilder) is shared: by the time a value reaches it, one of
// those two checks has already confirmed it fits.
//
// On a violation, appendRow routes the error through s.core.fail so the
// failure becomes sticky via the exact mechanism a WriteError uses
// (RowDone/Finish keep returning it, and Finish's best-effort
// close-after-failure still runs against an already-opened writer) —
// without ever constructing or wrapping a WriteError.
func (s *InferringSink) appendRow(rec map[string]any) error {
	n := len(s.plan.names)
	if cap(s.scratch) < n {
		s.scratch = make([]any, n)
	}
	values := s.scratch[:n]
	for i := 0; i < n; i++ {
		values[i] = s.plan.extract(rec, i)
	}
	for i, v := range values {
		if !s.fits(v, s.colTypes[i]) {
			return s.core.fail(newTypeViolationError(s.plan.names[i], s.colTypes[i], v))
		}
	}
	b := s.core.Builder()
	for i, v := range values {
		appendValueToBuilder(b.Field(i), s.colTypes[i], v)
	}
	if s.inferredNames != nil {
		s.trackUnknownKeys(rec)
	}
	return nil
}

// trackUnknownKeys records, for an inferred-names plan only, any of rec's
// keys that fall outside the fixed schema — those values were just
// silently dropped by appendRow (the schema was fixed from an earlier
// batch), and this is how callers learn about it via UnknownKeys.
// Allocation-free when every key is already known. Identical logic to
// StringSink's method of the same name; duplicated rather than shared
// because the two sinks otherwise share no per-instance state to hang a
// common helper off without a larger refactor than this task's scope
// allows. Never invoked in declared-columns mode (s.inferredNames stays nil
// there — see the InferringSink doc comment).
func (s *InferringSink) trackUnknownKeys(rec map[string]any) {
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
// MaxTrackedUnknownKeys) seen outside an INFERRED-names schema, plus the
// exact count of records that carried at least one such key. Always (nil,
// 0) for an explicit WithColumns plan or a declared WithTypedColumns plan:
// in both cases the caller chose the projection deliberately, so extra keys
// are not a data-loss defect.
func (s *InferringSink) UnknownKeys() (keys []string, records int64) {
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

// Finish flushes the partial batch (deriving the plan/types first if still
// pending) and closes the writer when one was opened. Zero records: no
// writer was opened and (0, nil, nil) is returned. Idempotent: a second call
// returns the same result without double-closing the writer. In
// declared-columns mode, s.pending is always empty (Add never buffers
// there), so the adoptFromPending branch below is simply skipped.
func (s *InferringSink) Finish() (int64, *sdk.CloseResult, error) {
	if s.core != nil && s.core.failed != nil {
		// Route through the core's own Finish (rather than returning its
		// failed/rows fields directly) so the core's best-effort
		// close-on-sticky-failure actually runs for this façade too —
		// same reasoning as StringSink.Finish.
		return s.core.Finish()
	}
	if s.finished {
		if s.core == nil {
			return 0, nil, nil
		}
		return s.core.rows, s.core.closeResult, nil
	}
	if s.colTypes == nil && len(s.pending) > 0 {
		if err := s.adoptFromPending(); err != nil {
			// A violation surfaced while replaying pending rows; s.core was
			// already built by adoptFromPending, so route through its
			// Finish for the same best-effort close as above.
			return s.core.Finish()
		}
	}
	s.finished = true
	if s.core == nil {
		return 0, nil, nil
	}
	return s.core.Finish()
}

// Writer exposes the opened writer (nil until the first successful flush
// opens one, and nil for a sink that never received a row).
func (s *InferringSink) Writer() ChunkWriter {
	if s.core == nil {
		return nil
	}
	return s.core.Writer()
}

// --- type inference: the lattice (Mode 1) ---
//
// One Arrow type is inferred per column by folding (joinKind) the
// columnKind of every sampled value in the first batch, with String as the
// top type that every mismatch degrades to:
//
//	sampled values for a column                  | inferred
//	----------------------------------------------|------------------------
//	json.Number, every value parses as an integer  | arrow.PrimitiveTypes.Int64
//	json.Number, any value only parses as float    | arrow.PrimitiveTypes.Float64
//	bool only                                      | arrow.FixedWidthTypes.Boolean
//	Int64 seen + Float64 seen                      | Float64
//	anything + string                              | String
//	map[string]any or []any present                | String (compact JSON,
//	                                                |  same as stringifyValue)
//	only nulls, or column never present in sample  | String
//
// Numeric kind is resolved from json.Number's TEXT via strconv.ParseInt
// then strconv.ParseFloat — never by casting to float64 — so a large
// integer like 5938028332 stays exactly that instead of drifting into
// scientific notation, and "1.50" is recognised as a float without being
// rewritten to "1.5". A json.Number that parses as neither (possible in
// principle) degrades that column to String.
//
// A caller that did not use json.UseNumber hands numbers as float64;
// classifyValue treats an integral float64 as Int64 and a non-integral one
// as Float64, mirroring stringifyValue's own defensive float64 case, rather
// than panicking.
//
// This lattice governs INFERENCE mode only. Declared-columns mode
// (WithTypedColumns) never joins kinds across a sample — every column's
// type is exactly what ArrowTypeFor resolved from the caller's declared
// string — and applies its own, stricter per-value fit rules; see
// declaredValueFits below.

// columnKind is the join-lattice element classifyValue maps one sampled
// value to. kindUnset is the fold's identity (bottom); kindString is the
// top type every mismatch (other than Int+Float) degrades to.
type columnKind int

const (
	kindUnset columnKind = iota
	kindInt
	kindFloat
	kindBool
	kindString
)

// classifyValue maps one decoded JSON value — or, defensively, a raw
// float64 from a caller that skipped json.UseNumber — to the columnKind it
// contributes to the join. nil maps to kindUnset, the fold's identity
// element, so a null or absent value never forces a column toward String by
// itself; only a genuinely incompatible sampled value does that.
func classifyValue(v any) columnKind {
	switch x := v.(type) {
	case nil:
		return kindUnset
	case json.Number:
		if _, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
			return kindInt
		}
		if _, err := strconv.ParseFloat(x.String(), 64); err == nil {
			return kindFloat
		}
		return kindString
	case bool:
		return kindBool
	case string:
		return kindString
	case float64: // defensive: non-UseNumber callers
		if math.IsNaN(x) || math.IsInf(x, 0) || x != math.Trunc(x) {
			return kindFloat
		}
		return kindInt
	default: // map[string]any, []any, or any other unlisted shape
		return kindString
	}
}

// joinKind folds one more sampled kind (b) into the column's accumulated
// kind so far (a). kindUnset is the identity element. Once two different
// non-unset kinds meet, Int+Float is the only combination that resolves to
// something other than the String top type (it widens to Float); every
// other mismatch — bool vs. a number, anything vs. string, ... — degrades
// to String.
func joinKind(a, b columnKind) columnKind {
	if a == kindUnset {
		return b
	}
	if b == kindUnset {
		return a
	}
	if a == b {
		return a
	}
	if (a == kindInt && b == kindFloat) || (a == kindFloat && b == kindInt) {
		return kindFloat
	}
	return kindString
}

// arrowTypeForKind maps a fully-joined columnKind to the Arrow type
// InferringSink writes for it. kindUnset (only nulls, or the column never
// appeared in the sample) resolves to String, same as kindString.
func arrowTypeForKind(k columnKind) arrow.DataType {
	switch k {
	case kindInt:
		return arrow.PrimitiveTypes.Int64
	case kindFloat:
		return arrow.PrimitiveTypes.Float64
	case kindBool:
		return arrow.FixedWidthTypes.Boolean
	default: // kindUnset, kindString
		return arrow.BinaryTypes.String
	}
}

// inferTypesFromBatch computes one Arrow type per column in p.names by
// joining classifyValue's kind across every record in batch — the same
// first batch planFromBatch draws its name set from (or, for a WithColumns
// plan, the same batch its projected values are inferred from). This costs
// no second pass: batch still holds the original decoded values at this
// point.
func inferTypesFromBatch(p *columnPlan, batch []map[string]any) []arrow.DataType {
	kinds := make([]columnKind, len(p.names))
	for _, rec := range batch {
		for i := range p.names {
			kinds[i] = joinKind(kinds[i], classifyValue(p.extract(rec, i)))
		}
	}
	types := make([]arrow.DataType, len(kinds))
	for i, k := range kinds {
		types[i] = arrowTypeForKind(k)
	}
	return types
}

// valueFitsArrowType reports whether v — one raw decoded value, of the same
// shapes classifyValue accepts — can be appended into a column whose Arrow
// type is already fixed to t by INFERENCE (Mode 1). nil always fits (every
// field is nullable). A value that fits t's join-lattice kind by
// construction always reports true here too; this function additionally
// has to accept the "upgrade" shapes a fixed type must still tolerate from
// a fresh batch — e.g. a json.Number integer text landing in a Float64
// column. String is the lattice's permissive top type here: every sampled
// value shape fits it. Contrast with declaredValueFits, used instead of
// this function in declared-columns mode, where String is NOT permissive.
func valueFitsArrowType(v any, t arrow.DataType) bool {
	if v == nil {
		return true
	}
	switch t.ID() {
	case arrow.INT64:
		switch x := v.(type) {
		case json.Number:
			_, err := strconv.ParseInt(x.String(), 10, 64)
			return err == nil
		case float64:
			return !math.IsNaN(x) && !math.IsInf(x, 0) && x == math.Trunc(x)
		default:
			return false
		}
	case arrow.FLOAT64:
		switch x := v.(type) {
		case json.Number:
			_, err := strconv.ParseFloat(x.String(), 64)
			return err == nil
		case float64:
			return true
		default:
			return false
		}
	case arrow.BOOL:
		_, ok := v.(bool)
		return ok
	default: // arrow.STRING: the top type — every sampled value shape fits.
		return true
	}
}

// declaredValueFits reports whether v fits the declared Arrow type t under
// declared-columns mode's (WithTypedColumns) coercion rules. Deliberately
// STRICTER than valueFitsArrowType's inference-mode lattice, where String
// is a permissive top type every value shape fits: in declared mode a
// column's type is a projection the caller committed to ahead of time, not
// a fallback, so:
//
//   - nil always fits (declared columns are always nullable; a missing key
//     or an explicit JSON null both resolve to Arrow null).
//   - json.Number is parsed per t: ParseInt for Int64, ParseFloat for
//     Float64. Against a String column it always fits, keeping its exact
//     source text (see appendValueToBuilder / stringifyValue) rather than
//     being reparsed and reformatted — e.g. declared string + json.Number
//     "1.50" stays the 4-byte string "1.50", not "1.5". It never fits
//     Boolean.
//   - a Go bool fits ONLY Boolean. In particular — unlike inference mode's
//     permissive String top type — a bool value does NOT fit a declared
//     String column; that is a *TypeViolationError.
//   - a Go string fits ONLY String.
//   - a nested map[string]any / []any fits ONLY String (rendered as
//     compact JSON, identical to stringifyValue).
//   - a defensive float64 (a non-json.UseNumber caller's decoded number)
//     fits Int64 only when integral, always fits Float64, and always fits
//     String (stringified via strconv.FormatFloat, same as stringifyValue)
//     — mirroring how the inference lattice already treats float64 as
//     polymorphic between Int and Float, extended here to String too so a
//     non-UseNumber caller is not penalized specifically in declared mode.
func declaredValueFits(v any, t arrow.DataType) bool {
	if v == nil {
		return true
	}
	switch t.ID() {
	case arrow.INT64:
		switch x := v.(type) {
		case json.Number:
			_, err := strconv.ParseInt(x.String(), 10, 64)
			return err == nil
		case float64:
			return !math.IsNaN(x) && !math.IsInf(x, 0) && x == math.Trunc(x)
		default:
			return false
		}
	case arrow.FLOAT64:
		switch x := v.(type) {
		case json.Number:
			_, err := strconv.ParseFloat(x.String(), 64)
			return err == nil
		case float64:
			return true
		default:
			return false
		}
	case arrow.BOOL:
		_, ok := v.(bool)
		return ok
	default: // arrow.STRING: strict — only these shapes fit, NOT bool.
		switch v.(type) {
		case string, json.Number, float64, map[string]any, []any:
			return true
		default:
			return false
		}
	}
}

// appendValueToBuilder appends v into the field builder b, dispatching on
// t (the column's fixed Arrow type). Shared by both InferringSink modes:
// callers must have already checked one of the two fit functions above
// (valueFitsArrowType for inference mode, declaredValueFits for declared
// mode) for (v, t); the numeric parses here are treated as infallible
// because that check already ran.
func appendValueToBuilder(b array.Builder, t arrow.DataType, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch t.ID() {
	case arrow.INT64:
		ib := b.(*array.Int64Builder)
		switch x := v.(type) {
		case json.Number:
			n, _ := strconv.ParseInt(x.String(), 10, 64) // pre-validated by the fit check
			ib.Append(n)
		case float64:
			ib.Append(int64(x))
		}
	case arrow.FLOAT64:
		fb := b.(*array.Float64Builder)
		switch x := v.(type) {
		case json.Number:
			f, _ := strconv.ParseFloat(x.String(), 64) // pre-validated
			fb.Append(f)
		case float64:
			fb.Append(x)
		}
	case arrow.BOOL:
		b.(*array.BooleanBuilder).Append(v.(bool))
	default: // arrow.STRING
		sb := b.(*array.StringBuilder)
		cell, isNull := stringifyValue(v)
		if isNull {
			sb.AppendNull()
		} else {
			sb.Append(cell)
		}
	}
}

// --- TypeViolationError ---

// typeViolationPreviewLimit bounds TypeViolationError.ValuePreview so a
// pathological value (a giant nested object, a multi-MB string) never gets
// dumped whole into an error message that a caller might log verbatim.
const typeViolationPreviewLimit = 80

// TypeViolationError reports a value that no longer fits a column's Arrow
// type — whether that type was fixed by first-batch inference (Mode 1) or
// supplied up front via WithTypedColumns (Mode 2). This is a user
// data-shape problem, not an infrastructure failure: in inference mode,
// earlier batches may have already shipped under the fixed schema; in
// declared mode, the schema was the caller's explicit configuration from
// the start. Either way neither Iceberg nor the dev mock's drift lock
// tolerates a mid-writer schema change, so the run must fail. Callers
// should map *TypeViolationError to sdk.ExitUserError (exit 1) — in
// contrast to *WriteError (writer open/Write/Close failures), which maps to
// sdk.ExitAppError (exit >= 20). TypeViolationError is never a WriteError
// and is never wrapped in one anywhere in this package.
//
// No Unwrap method: TypeViolationError never wraps another error (there is
// no underlying cause beyond the value/type mismatch itself), so
// errors.As's direct type match is all a caller ever needs.
type TypeViolationError struct {
	// Column is the name of the offending column.
	Column string
	// Type is the column's Arrow type — as fixed by first-batch inference
	// in Mode 1, or as resolved from the caller's declared type string via
	// ArrowTypeFor in Mode 2.
	Type arrow.DataType
	// ValueType is the offending value's Go type, e.g. "string" or
	// "json.Number" (via fmt.Sprintf("%T", v)).
	ValueType string
	// ValuePreview is a truncated, human-readable rendering of the
	// offending value (capped at typeViolationPreviewLimit runes) — never
	// the full value, so a pathologically large payload cannot inflate an
	// error message (or whatever logs it) without bound.
	ValuePreview string
}

// Error renders the violation with the column name, the value's Go type,
// its truncated preview, and the column's fixed Arrow type.
func (e *TypeViolationError) Error() string {
	return fmt.Sprintf("arrow: column %q: value of Go type %s (%s) does not fit its type %s",
		e.Column, e.ValueType, e.ValuePreview, e.Type)
}

// newTypeViolationError builds a TypeViolationError for column/t/v, reusing
// stringifyValue to render the preview (so it matches exactly what that
// value would render as had the column been a String column), then
// truncates it to typeViolationPreviewLimit.
func newTypeViolationError(column string, t arrow.DataType, v any) *TypeViolationError {
	preview, _ := stringifyValue(v)
	if len(preview) > typeViolationPreviewLimit {
		preview = preview[:typeViolationPreviewLimit] + "...(truncated)"
	}
	return &TypeViolationError{
		Column:       column,
		Type:         t,
		ValueType:    fmt.Sprintf("%T", v),
		ValuePreview: preview,
	}
}
