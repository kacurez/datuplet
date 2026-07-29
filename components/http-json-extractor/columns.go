package main

import (
	"context"
	"fmt"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
	dgarrow "github.com/datuplet/datuplet/sdk/go/arrow"
)

// Recognized values for the component's schema_inference config option
// (Config.SchemaInference). Only meaningful when no declared output-column
// mapping applies to the resolved output table — a mapping always wins
// regardless of this setting (see newExtractorSink).
const (
	schemaInferenceTyped   = "typed"   // default: infer Int64/Float64/Boolean/String per column from the first batch
	schemaInferenceStrings = "strings" // compatibility escape hatch: every column is a string (pre-typed-inference behavior)
)

// extractorSink is the shape both dgarrow.StringSink and dgarrow.InferringSink
// already share (Add/Finish/Writer/UnknownKeys). newExtractorSink returns
// this interface so the mode-selection logic below, finishQuietly, and both
// main.go call sites never need to know which concrete sink backs a given
// run.
type extractorSink interface {
	Add(rec map[string]any) error
	Finish() (int64, *sdk.CloseResult, error)
	Writer() dgarrow.ChunkWriter
	UnknownKeys() (keys []string, records int64)
}

// fieldsProjectingSink wraps an extractorSink, pre-projecting every record
// through a `fields` mapping (dot-path extraction + rename) before handing it
// to the underlying sink. Used only when a declared output-column mapping
// AND a `fields` projection are BOTH configured for the same output table:
// the declared sink's WithTypedColumns plan selects columns by direct map
// lookup on the name it was given, so `fields` must run first to produce a
// record keyed by the OUTPUT column names the mapping expects, not the
// source JSON's paths. Finish/Writer/UnknownKeys are promoted unchanged from
// the embedded extractorSink.
type fieldsProjectingSink struct {
	extractorSink
	fields []FieldMapping
}

// Add projects rec through s.fields (path -> renamed value) and forwards the
// result to the wrapped declared-columns sink. Any key of rec not named by
// s.fields is dropped before the declared sink ever sees it — the same
// "fields selects/renames first" composition documented on newExtractorSink.
func (s *fieldsProjectingSink) Add(rec map[string]any) error {
	projected := make(map[string]any, len(s.fields))
	for _, f := range s.fields {
		projected[f.Name] = getValueRaw(rec, f.Path)
	}
	return s.extractorSink.Add(projected)
}

// fieldColumns adapts the config's `fields` projection to the SDK sink's
// explicit-columns form: names in declared order, values resolved via
// getValueRaw dot-paths.
func fieldColumns(fields []FieldMapping) ([]string, func(rec map[string]any, i int) any) {
	names := make([]string, len(fields))
	paths := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
		paths[i] = f.Path
	}
	return names, func(rec map[string]any, i int) any {
		return getValueRaw(rec, paths[i])
	}
}

// columnMappingFor returns the declared Columns of the cfg.OutputTables
// entry whose (already logicalName-preferred) Name equals outputTable, or
// nil when no entry matches or the matched entry declares no columns. The
// first match wins; a pipeline doc is not expected to declare the same
// output table twice.
func columnMappingFor(outputTables []sdk.OutputTableRef, outputTable string) []sdk.ColumnRef {
	for _, t := range outputTables {
		if t.Name == outputTable {
			return t.Columns
		}
	}
	return nil
}

// newExtractorSink builds the sink both extraction paths (single-request and
// paginated) write to, selecting exactly one of three mutually exclusive
// modes per the maintainer-ruled matrix:
//
//  1. A declared output-column mapping exists for outputTable (found via
//     columnMappingFor against sdkCfg.OutputTables, sourced from the
//     pipeline doc's `outputs.tables[].columns`) -> DECLARED mode,
//     regardless of schemaInference: dgarrow.WithTypedColumns writes exactly
//     the mapped columns with the mapped types, no inference, extra source
//     keys silently excluded. When `fields` is ALSO set, it composes by
//     running first: fields extracts/renames from the raw record, and the
//     RESULTING (projected) record is what the declared sink sees
//     (fieldsProjectingSink). When `fields` is not set, the raw record goes
//     straight to the declared sink (a key not in the mapping is simply not
//     selected — WithTypedColumns' own contract).
//  2. No mapping, schemaInference is "typed" (the default, including the
//     empty/unset value) -> dgarrow.NewInferringSink: WithColumns(the
//     `fields` projection) when `fields` is set (names fixed, types still
//     inferred from the projected values), pure first-batch inference
//     (names AND types) otherwise.
//  3. No mapping, schemaInference is "strings" -> dgarrow.NewStringSink,
//     unchanged from the component's pre-typed-inference behavior: WithColumns
//     when `fields` is set, first-batch name inference (all-String)
//     otherwise.
//
// The only error this can return is a sink-construction error surfaced by
// NewInferringSink's declared-columns mode (an unrecognized column type
// string in the mapping — WithTypedColumns/WithColumns conflicting options
// can never happen here, since this function itself decides which one, if
// either, to pass). That is always a user configuration problem, never an
// infrastructure failure: callers should map a non-nil error to
// sdk.ExitUserError. Once construction succeeds, every later failure surfaces
// through the returned sink's Add/Finish instead (a *dgarrow.WriteError for
// infrastructure failures, a *dgarrow.TypeViolationError for a value that
// does not fit its column's type).
func newExtractorSink(ctx context.Context, client *sdk.Client, outputTable string, fields []FieldMapping, sdkCfg *sdk.Config, schemaInference string) (extractorSink, error) {
	open := func() (dgarrow.ChunkWriter, error) {
		return client.OpenWriter(ctx, outputTable, sdk.WithFormat(pb.DataFormat_FORMAT_ARROW_IPC))
	}

	if mapping := columnMappingFor(sdkCfg.OutputTables, outputTable); len(mapping) > 0 {
		typedCols := make([]dgarrow.TypedColumn, len(mapping))
		for i, c := range mapping {
			typedCols[i] = dgarrow.TypedColumn{Name: c.Name, Type: c.Type}
		}
		sink, err := dgarrow.NewInferringSink(ctx, open, dgarrow.WithTypedColumns(typedCols))
		if err != nil {
			return nil, fmt.Errorf("output table %q: declared columns: %w", outputTable, err)
		}
		if len(fields) > 0 {
			return &fieldsProjectingSink{extractorSink: sink, fields: fields}, nil
		}
		return sink, nil
	}

	var opts []dgarrow.SinkOption
	if len(fields) > 0 {
		names, extract := fieldColumns(fields)
		opts = append(opts, dgarrow.WithColumns(names, extract))
	}

	if schemaInference == schemaInferenceStrings {
		return dgarrow.NewStringSink(ctx, open, opts...), nil
	}

	// Default ("typed", and any unset/empty value that ParseAndValidate has
	// already confirmed is empty rather than an unrecognized string).
	sink, err := dgarrow.NewInferringSink(ctx, open, opts...)
	if err != nil {
		return nil, fmt.Errorf("output table %q: %w", outputTable, err)
	}
	return sink, nil
}

// finishQuietly calls sink.Finish() and discards the result. It exists
// because sdk.ExitUserError/ExitAppError call os.Exit, which skips all of
// main's deferred cleanup — so an already-opened gateway writer (e.g. a
// single-request fetch that streamed past the first batch, flushed once,
// then hit a decode error in the malformed tail) would otherwise be left
// open. Call this immediately before any sdk.Exit* that can run after the
// sink was constructed. Finish is idempotent, so a later real Finish call
// (or another finishQuietly call) is a harmless no-op. A nil sink (possible
// now that newExtractorSink's construction can fail) is a no-op too — every
// call site checks newExtractorSink's error and exits/returns before ever
// reaching finishQuietly, but the guard costs nothing.
func finishQuietly(sink extractorSink) {
	if sink == nil {
		return
	}
	_, _, _ = sink.Finish()
}
