package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	sdk "github.com/datuplet/datuplet/sdk/go"
	dgarrow "github.com/datuplet/datuplet/sdk/go/arrow"
)

func TestFieldColumns(t *testing.T) {
	names, extract := fieldColumns([]FieldMapping{
		{Path: "country.value", Name: "entity"},
		{Path: "iso", Name: "iso3"},
	})
	if len(names) != 2 || names[0] != "entity" || names[1] != "iso3" {
		t.Fatalf("names = %v (declared order required)", names)
	}
	rec := map[string]any{"country": map[string]any{"value": "Africa"}, "iso": "AFE"}
	if v := extract(rec, 0); v != "Africa" {
		t.Fatalf("extract nested = %v", v)
	}
	if v := extract(rec, 1); v != "AFE" {
		t.Fatalf("extract flat = %v", v)
	}
	if v := extract(map[string]any{}, 0); v != nil {
		t.Fatalf("missing path should be nil, got %v", v)
	}
}

// fakeChunkWriter is a minimal dgarrow.ChunkWriter for finishQuietly tests.
// payloads records every Write call's bytes verbatim (a copy, since Write's
// caller may reuse its buffer) for tests that need to decode what was
// actually flushed (e.g. TestFieldsProjectingSink_ComposesDeclaredColumns).
type fakeChunkWriter struct {
	closed   bool
	payloads [][]byte
}

func (f *fakeChunkWriter) Write(_ context.Context, data []byte) error {
	f.payloads = append(f.payloads, append([]byte(nil), data...))
	return nil
}
func (f *fakeChunkWriter) Close(_ context.Context) (*sdk.CloseResult, error) {
	f.closed = true
	return &sdk.CloseResult{TotalRows: 1}, nil
}
func (f *fakeChunkWriter) Bucket() string { return "raw" }
func (f *fakeChunkWriter) Table() string  { return "t" }

// TestFinishQuietly_ClosesAlreadyOpenedWriter covers Finding I1: a sink
// that already flushed a batch (and so opened its writer) must have that
// writer closed by finishQuietly, matching the case where sdk.Exit* runs
// after the sink has written data but before the normal Finish() call.
func TestFinishQuietly_ClosesAlreadyOpenedWriter(t *testing.T) {
	fw := &fakeChunkWriter{}
	sink := dgarrow.NewStringSink(context.Background(), func() (dgarrow.ChunkWriter, error) { return fw, nil },
		dgarrow.WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }))
	if err := sink.Add(map[string]any{"id": "1"}); err != nil {
		t.Fatal(err)
	}
	finishQuietly(sink)
	if !fw.closed {
		t.Fatal("finishQuietly must close a writer the sink already opened")
	}
	// Idempotent: a later call (e.g. main's normal-path Finish, or another
	// finishQuietly from a defer) must not panic or double-close incorrectly.
	finishQuietly(sink)
}

// TestFinishQuietly_UntouchedSinkNoWriter covers the fetchStream-failure
// case: finishQuietly on a sink that never received any records must be a
// harmless no-op (no writer ever opened, per StringSink's lazy-open design).
func TestFinishQuietly_UntouchedSinkNoWriter(t *testing.T) {
	opened := false
	sink := dgarrow.NewStringSink(context.Background(), func() (dgarrow.ChunkWriter, error) {
		opened = true
		return &fakeChunkWriter{}, nil
	}, dgarrow.WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }))
	finishQuietly(sink)
	if opened {
		t.Fatal("finishQuietly on an untouched sink must not open a writer")
	}
}

// TestColumnMappingFor covers the lookup newExtractorSink's mode-selection
// relies on: match by (already logicalName-preferred) Name, nil when the
// matched entry declares no columns, nil when nothing matches.
func TestColumnMappingFor(t *testing.T) {
	tables := []sdk.OutputTableRef{
		{Name: "a", Columns: []sdk.ColumnRef{{Name: "x", Type: "int"}}},
		{Name: "b"}, // matched but no columns declared
	}
	if got := columnMappingFor(tables, "a"); len(got) != 1 || got[0].Name != "x" {
		t.Fatalf("columnMappingFor(a) = %v, want [{x int}]", got)
	}
	if got := columnMappingFor(tables, "b"); got != nil {
		t.Fatalf("columnMappingFor(b) = %v, want nil (matched entry declares no columns)", got)
	}
	if got := columnMappingFor(tables, "missing"); got != nil {
		t.Fatalf("columnMappingFor(missing) = %v, want nil", got)
	}
}

// TestNewExtractorSink_ModeSelection is the mode-selection matrix: for every
// combination of {mapping present, no-mapping+typed, no-mapping+strings} x
// {fields set, fields unset}, asserts which CONCRETE sink type
// newExtractorSink constructs. client is nil throughout: newExtractorSink's
// `open` closure captures it but never calls it during construction (a sink
// opens its writer lazily, on first flush), so a nil client is safe as long
// as the test never drives a flush — exactly what this test needs, since it
// only inspects the returned type.
func TestNewExtractorSink_ModeSelection(t *testing.T) {
	mapping := []sdk.ColumnRef{{Name: "id", Type: "int"}}
	oneField := []FieldMapping{{Path: "a.b", Name: "id"}}

	tests := []struct {
		name            string
		fields          []FieldMapping
		mapping         []sdk.ColumnRef
		schemaInference string
		assertType      func(t *testing.T, sink extractorSink)
	}{
		{
			name: "mapping_present_no_fields_declared_mode",
			assertType: func(t *testing.T, sink extractorSink) {
				if _, ok := sink.(*dgarrow.InferringSink); !ok {
					t.Fatalf("got %T, want *dgarrow.InferringSink (declared mode)", sink)
				}
			},
			mapping: mapping,
		},
		{
			name:   "mapping_present_with_fields_wraps_for_composition",
			fields: oneField,
			assertType: func(t *testing.T, sink extractorSink) {
				wrapped, ok := sink.(*fieldsProjectingSink)
				if !ok {
					t.Fatalf("got %T, want *fieldsProjectingSink", sink)
				}
				if _, ok := wrapped.extractorSink.(*dgarrow.InferringSink); !ok {
					t.Fatalf("wrapped sink is %T, want *dgarrow.InferringSink", wrapped.extractorSink)
				}
			},
			mapping: mapping,
		},
		{
			name:            "mapping_present_overrides_strings_setting",
			schemaInference: schemaInferenceStrings,
			assertType: func(t *testing.T, sink extractorSink) {
				if _, ok := sink.(*dgarrow.InferringSink); !ok {
					t.Fatalf("got %T, want *dgarrow.InferringSink (mapping wins over schema_inference=strings)", sink)
				}
			},
			mapping: mapping,
		},
		{
			name: "no_mapping_default_empty_is_typed_no_fields",
			assertType: func(t *testing.T, sink extractorSink) {
				if _, ok := sink.(*dgarrow.InferringSink); !ok {
					t.Fatalf("got %T, want *dgarrow.InferringSink (default inference)", sink)
				}
			},
		},
		{
			name:            "no_mapping_explicit_typed_with_fields",
			fields:          oneField,
			schemaInference: schemaInferenceTyped,
			assertType: func(t *testing.T, sink extractorSink) {
				if _, ok := sink.(*dgarrow.InferringSink); !ok {
					t.Fatalf("got %T, want *dgarrow.InferringSink (typed + fields)", sink)
				}
			},
		},
		{
			name:            "no_mapping_strings_no_fields",
			schemaInference: schemaInferenceStrings,
			assertType: func(t *testing.T, sink extractorSink) {
				if _, ok := sink.(*dgarrow.StringSink); !ok {
					t.Fatalf("got %T, want *dgarrow.StringSink", sink)
				}
			},
		},
		{
			name:            "no_mapping_strings_with_fields",
			fields:          oneField,
			schemaInference: schemaInferenceStrings,
			assertType: func(t *testing.T, sink extractorSink) {
				if _, ok := sink.(*dgarrow.StringSink); !ok {
					t.Fatalf("got %T, want *dgarrow.StringSink", sink)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sdkCfg := &sdk.Config{}
			if tt.mapping != nil {
				sdkCfg.OutputTables = []sdk.OutputTableRef{{Name: "out", Columns: tt.mapping}}
			}
			sink, err := newExtractorSink(context.Background(), nil, "out", tt.fields, sdkCfg, tt.schemaInference)
			if err != nil {
				t.Fatalf("newExtractorSink: %v", err)
			}
			tt.assertType(t, sink)
		})
	}
}

// TestNewExtractorSink_BadDeclaredType covers the one error newExtractorSink
// can return: an unrecognized type string in a declared mapping is a
// construction-time config problem (the caller must map it to
// sdk.ExitUserError — see main.go), and the error must name the output
// table so the operator can find the offending outputs.tables entry.
func TestNewExtractorSink_BadDeclaredType(t *testing.T) {
	sdkCfg := &sdk.Config{OutputTables: []sdk.OutputTableRef{
		{Name: "out", Columns: []sdk.ColumnRef{{Name: "x", Type: "not_a_real_type"}}},
	}}
	sink, err := newExtractorSink(context.Background(), nil, "out", nil, sdkCfg, "")
	if err == nil {
		t.Fatal("expected error for unrecognized declared column type")
	}
	if sink != nil {
		t.Fatalf("sink must be nil on construction error, got %T", sink)
	}
	if !strings.Contains(err.Error(), "out") {
		t.Fatalf("error should name the output table %q, got: %v", "out", err)
	}
}

// decodeIPCPayload decodes one self-contained Arrow IPC payload (schema +
// one record batch + EOS — the shape every Sink flush produces) into a
// slice of column-name-keyed rows, for tests that need to verify actual
// emitted values/types rather than just the sink's Go type.
func decodeIPCPayload(t *testing.T, payload []byte) []map[string]any {
	t.Helper()
	br := bytes.NewReader(payload)
	rd, err := ipc.NewReader(br)
	if err != nil {
		t.Fatalf("payload is not a valid IPC stream: %v", err)
	}
	defer rd.Release()
	fields := rd.Schema().Fields()

	var rows []map[string]any
	for rd.Next() {
		rec := rd.Record()
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make(map[string]any, len(fields))
			for c, f := range fields {
				col := rec.Column(c)
				if col.IsNull(r) {
					row[f.Name] = nil
					continue
				}
				switch arr := col.(type) {
				case *array.Int32:
					row[f.Name] = arr.Value(r)
				case *array.Int64:
					row[f.Name] = arr.Value(r)
				case *array.Float64:
					row[f.Name] = arr.Value(r)
				case *array.Boolean:
					row[f.Name] = arr.Value(r)
				case *array.String:
					row[f.Name] = arr.Value(r)
				default:
					t.Fatalf("unexpected column array type %T for field %s", col, f.Name)
				}
			}
			rows = append(rows, row)
		}
	}
	if rd.Err() != nil {
		t.Fatalf("ipc read: %v", rd.Err())
	}
	return rows
}

// TestFieldsProjectingSink_ComposesDeclaredColumns covers the
// fields-then-mapping composition newExtractorSink documents: `fields`
// extracts/renames from the raw record FIRST (dot-path via getValueRaw),
// and the declared (WithTypedColumns) sink types the RESULTING record by
// its (already renamed) column names. A source key not named by `fields`
// (here "extra") must never reach the declared sink at all.
func TestFieldsProjectingSink_ComposesDeclaredColumns(t *testing.T) {
	fields := []FieldMapping{
		{Path: "user.id", Name: "user_id"},
		{Path: "amount", Name: "amount"},
	}
	typedCols := []dgarrow.TypedColumn{
		{Name: "user_id", Type: "int"},
		{Name: "amount", Type: "float"},
	}
	fw := &fakeChunkWriter{}
	inner, err := dgarrow.NewInferringSink(context.Background(), func() (dgarrow.ChunkWriter, error) { return fw, nil },
		dgarrow.WithTypedColumns(typedCols))
	if err != nil {
		t.Fatalf("NewInferringSink: %v", err)
	}
	sink := &fieldsProjectingSink{extractorSink: inner, fields: fields}

	rec := map[string]any{
		"user":   map[string]any{"id": json.Number("42")},
		"amount": json.Number("19.99"),
		"extra":  "must not reach the declared sink",
	}
	if err := sink.Add(rec); err != nil {
		t.Fatalf("Add: %v", err)
	}
	rows, _, err := sink.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	if len(fw.payloads) != 1 {
		t.Fatalf("payloads flushed = %d, want 1", len(fw.payloads))
	}

	decoded := decodeIPCPayload(t, fw.payloads[0])
	if len(decoded) != 1 {
		t.Fatalf("decoded rows = %d, want 1", len(decoded))
	}
	row := decoded[0]
	// Declared `int` is 32-bit (Iceberg's int), so this decodes as int32 —
	// declare `long` for values that may exceed int32.
	if v, ok := row["user_id"].(int32); !ok || v != 42 {
		t.Fatalf("user_id = %v (%T), want int32(42)", row["user_id"], row["user_id"])
	}
	if v, ok := row["amount"].(float64); !ok || v != 19.99 {
		t.Fatalf("amount = %v (%T), want float64(19.99)", row["amount"], row["amount"])
	}
	if _, present := row["extra"]; present {
		t.Fatalf("declared sink must not have received the unmapped key %q: %v", "extra", row)
	}
	if len(row) != 2 {
		t.Fatalf("row has %d columns, want exactly 2 (user_id, amount): %v", len(row), row)
	}
}
