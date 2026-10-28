package arrow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// mustNewInferringSink builds an InferringSink and fails the test immediately
// on a construction error — used by every test that is not itself exercising
// a constructor error path (those call NewInferringSink directly).
func mustNewInferringSink(t *testing.T, ctx context.Context, open func() (ChunkWriter, error), opts ...SinkOption) *InferringSink {
	t.Helper()
	s, err := NewInferringSink(ctx, open, opts...)
	if err != nil {
		t.Fatalf("NewInferringSink: %v", err)
	}
	return s
}

// fieldByName finds the field named name in fields, for tests that infer a
// full-batch column set (sorted union) and want to assert on one column
// without hardcoding its position.
func fieldByName(fields []arrow.Field, name string) (arrow.Field, bool) {
	for _, f := range fields {
		if f.Name == name {
			return f, true
		}
	}
	return arrow.Field{}, false
}

// decodeInferredPayload parses one IPC payload against an ARBITRARY mix of
// Int32/Int64/Float64/Boolean/String columns — InferringSink's typed output can mix
// column types within a single schema, unlike typed_sink_test.go's fixed
// 4-column decodeTypedPayload or sink_test.go's all-String ipcCells — and
// returns one map per row keyed by column name, plus the schema's fields. A
// null cell decodes as a nil map entry (Go's zero value for `any`, so a
// present-but-absent-key lookup and an explicit null are indistinguishable —
// exactly the property under test in several cases below).
func decodeInferredPayload(t *testing.T, payload []byte) (rows []map[string]any, fields []arrow.Field) {
	t.Helper()
	br := bytes.NewReader(payload)
	rd, err := ipc.NewReader(br)
	if err != nil {
		t.Fatalf("payload is not a valid IPC stream: %v", err)
	}
	defer rd.Release()
	fields = rd.Schema().Fields()

	for rd.Next() {
		rec := rd.Record()
		numRows := int(rec.NumRows())
		for r := 0; r < numRows; r++ {
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
		rec.Release()
	}
	if rd.Err() != nil {
		t.Fatalf("ipc read: %v", rd.Err())
	}
	// Same self-contained-stream invariant sink_test.go's ipcRows/ipcCells
	// check: no trailing bytes after the stream.
	remaining, err := br.ReadByte()
	if err == nil {
		t.Fatalf("trailing byte after IPC stream: %d", remaining)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("reading after IPC stream: %v", err)
	}
	return rows, fields
}

// =====================================================================
// Mode 1: typed inference
// =====================================================================

// TestInferringSink_LatticeTableDriven exercises every cell of the
// classifyValue/joinKind lattice documented on InferringSink: every base
// kind alone, every join that widens (Int+Float), every join that degrades
// to String, nulls-only, a column absent from every sampled record, and a
// column present in only some records. All cases pin the column name to "v"
// via WithColumns so the test only has to reason about the inferred TYPE.
func TestInferringSink_LatticeTableDriven(t *testing.T) {
	cases := []struct {
		name    string
		records []map[string]any
		wantID  arrow.Type
	}{
		{"all_int", []map[string]any{{"v": json.Number("1")}, {"v": json.Number("2")}, {"v": json.Number("3")}}, arrow.INT64},
		{"all_float", []map[string]any{{"v": json.Number("1.5")}, {"v": json.Number("2.75")}}, arrow.FLOAT64},
		{"int_plus_float_widens_to_float", []map[string]any{{"v": json.Number("1")}, {"v": json.Number("2.5")}}, arrow.FLOAT64},
		{"float_plus_int_widens_to_float_order_independent", []map[string]any{{"v": json.Number("2.5")}, {"v": json.Number("1")}}, arrow.FLOAT64},
		{"bool_only", []map[string]any{{"v": true}, {"v": false}}, arrow.BOOL},
		{"int_plus_string_degrades_to_string", []map[string]any{{"v": json.Number("1")}, {"v": "x"}}, arrow.STRING},
		{"bool_plus_number_degrades_to_string", []map[string]any{{"v": true}, {"v": json.Number("1")}}, arrow.STRING},
		{"null_only", []map[string]any{{"v": nil}, {"v": nil}}, arrow.STRING},
		{"nested_object_present", []map[string]any{{"v": map[string]any{"a": json.Number("1")}}}, arrow.STRING},
		{"nested_array_present", []map[string]any{{"v": []any{json.Number("1"), "x"}}}, arrow.STRING},
		{"key_absent_from_some_records_still_infers_from_present_ones", []map[string]any{
			{"v": json.Number("1")}, {"other": "x"}, {"v": json.Number("2")},
		}, arrow.INT64},
		{"column_never_present_in_sample_defaults_to_string", []map[string]any{{"other": "x"}, {"other": "y"}}, arrow.STRING},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fw := &fakeWriter{}
			sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
				WithColumns([]string{"v"}, func(rec map[string]any, _ int) any { return rec["v"] }),
				WithBatchRows(len(tc.records)))
			for _, r := range tc.records {
				if err := sink.Add(r); err != nil {
					t.Fatalf("Add: %v", err)
				}
			}
			if _, _, err := sink.Finish(); err != nil {
				t.Fatalf("Finish: %v", err)
			}
			if len(fw.payloads) == 0 {
				t.Fatal("no payload flushed")
			}
			_, names, fields := ipcRows(t, fw.payloads[0])
			if len(names) != 1 || names[0] != "v" {
				t.Fatalf("names = %v, want [v]", names)
			}
			if fields[0].Type.ID() != tc.wantID {
				t.Fatalf("inferred type = %v, want %v", fields[0].Type, tc.wantID)
			}
			if !fields[0].Nullable {
				t.Fatal("field must be nullable")
			}
		})
	}
}

// TestInferringSink_KeyAbsentFromSomeRecordsInfersFromPresentValuesAndIsNullable
// drills into one lattice case beyond its inferred TYPE: the row where the
// key was absent must decode as an actual Arrow null, not e.g. a zero value.
func TestInferringSink_KeyAbsentFromSomeRecordsInfersFromPresentValuesAndIsNullable(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil }, WithBatchRows(3))
	recs := []map[string]any{
		{"v": json.Number("1")},
		{"other": "x"}, // "v" absent here
		{"v": json.Number("2")},
	}
	for _, r := range recs {
		if err := sink.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	rows, fields := decodeInferredPayload(t, fw.payloads[0])
	vf, ok := fieldByName(fields, "v")
	if !ok || vf.Type.ID() != arrow.INT64 || !vf.Nullable {
		t.Fatalf("field v = %+v (ok=%v), want nullable Int64", vf, ok)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[0]["v"] != int64(1) {
		t.Fatalf("row0 v = %v, want 1", rows[0]["v"])
	}
	if rows[1]["v"] != nil {
		t.Fatalf("row1 v = %v, want nil (key absent from this record)", rows[1]["v"])
	}
	if rows[2]["v"] != int64(2) {
		t.Fatalf("row2 v = %v, want 2", rows[2]["v"])
	}
}

// TestInferringSink_ExactNumericFidelity proves the load-bearing property
// that a large integer stays an exact Int64, resolved from json.Number's
// TEXT (strconv.ParseInt), never by round-tripping through float64.
func TestInferringSink_ExactNumericFidelity(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"n"}, func(rec map[string]any, _ int) any { return rec["n"] }),
		WithBatchRows(1))
	if err := sink.Add(map[string]any{"n": json.Number("5938028332")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	rows, fields := decodeInferredPayload(t, fw.payloads[0])
	if fields[0].Type.ID() != arrow.INT64 {
		t.Fatalf("type = %v, want Int64", fields[0].Type)
	}
	if rows[0]["n"] != int64(5938028332) {
		t.Fatalf("value = %v, want exactly 5938028332", rows[0]["n"])
	}
}

// TestInferringSink_FloatNotRewrittenFromExactText proves "1.50" is
// recognised as a Float64 without being renormalized in the process of
// deciding its kind (the stored value is the float 1.5 — the *text*
// preservation guarantee applies only to declared/inferred STRING columns).
func TestInferringSink_FloatNotRewrittenFromExactText(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"n"}, func(rec map[string]any, _ int) any { return rec["n"] }),
		WithBatchRows(1))
	if err := sink.Add(map[string]any{"n": json.Number("1.50")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	rows, fields := decodeInferredPayload(t, fw.payloads[0])
	if fields[0].Type.ID() != arrow.FLOAT64 {
		t.Fatalf("type = %v, want Float64", fields[0].Type)
	}
	if rows[0]["n"] != float64(1.5) {
		t.Fatalf("value = %v, want 1.5", rows[0]["n"])
	}
}

// TestInferringSink_AllFieldsNullableRegardlessOfType asserts the emitted
// IPC schema marks Nullable: true unconditionally, across all four inferred
// types at once (Int64, Float64, Boolean, String) in a single schema.
func TestInferringSink_AllFieldsNullableRegardlessOfType(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil }, WithBatchRows(1))
	if err := sink.Add(map[string]any{
		"i": json.Number("1"), "f": json.Number("1.5"), "b": true, "s": "x",
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	_, names, fields := ipcRows(t, fw.payloads[0])
	if len(names) != 4 {
		t.Fatalf("fields = %d, want 4: %v", len(names), names)
	}
	for _, f := range fields {
		if !f.Nullable {
			t.Fatalf("field %s (%v) not nullable", f.Name, f.Type)
		}
	}
}

// TestInferringSink_NestedValueStringifiesIdenticallyToStringifyValue proves
// the nested-shape -> String rendering is byte-identical to stringifyValue
// (StringSink's own cell renderer), not a second, possibly-divergent
// implementation.
func TestInferringSink_NestedValueStringifiesIdenticallyToStringifyValue(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"v"}, func(rec map[string]any, _ int) any { return rec["v"] }),
		WithBatchRows(1))
	nested := map[string]any{"a": json.Number("1"), "b": "x"}
	want, isNull := stringifyValue(nested)
	if isNull {
		t.Fatal("test setup: stringifyValue(nested) unexpectedly null")
	}
	if err := sink.Add(map[string]any{"v": nested}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	rows, fields := decodeInferredPayload(t, fw.payloads[0])
	if fields[0].Type.ID() != arrow.STRING {
		t.Fatalf("type = %v, want String", fields[0].Type)
	}
	if rows[0]["v"] != want {
		t.Fatalf("cell = %v, want %v (identical to stringifyValue)", rows[0]["v"], want)
	}
}

// TestInferringSink_LateTypeViolationIsTypeViolationErrorNotWriteError fixes
// column "n" to Int64 from a first batch of two integers, then feeds a
// plain string in the (already-adopted) third row: Add must return a
// *TypeViolationError naming "n" — never a *WriteError — and the failure
// must be sticky (every later Add/Finish returns that exact same error).
func TestInferringSink_LateTypeViolationIsTypeViolationErrorNotWriteError(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"n"}, func(rec map[string]any, _ int) any { return rec["n"] }),
		WithBatchRows(2))
	if err := sink.Add(map[string]any{"n": json.Number("1")}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Add(map[string]any{"n": json.Number("2")}); err != nil {
		t.Fatal(err)
	}
	err := sink.Add(map[string]any{"n": "not-a-number"})
	if err == nil {
		t.Fatal("want a type violation error, got nil")
	}
	var tv *TypeViolationError
	if !errors.As(err, &tv) {
		t.Fatalf("want *TypeViolationError, got %T: %v", err, err)
	}
	if tv.Column != "n" {
		t.Fatalf("Column = %q, want %q", tv.Column, "n")
	}
	var we *WriteError
	if errors.As(err, &we) {
		t.Fatalf("must NOT be classified as a *WriteError, got %v", we)
	}
	// Sticky: further Add/Finish calls return the exact same error.
	if err2 := sink.Add(map[string]any{"n": json.Number("3")}); err2 != err {
		t.Fatalf("Add after violation should return the same sticky error, got %v", err2)
	}
	if _, _, ferr := sink.Finish(); ferr != err {
		t.Fatalf("Finish after violation should return the same sticky error, got %v", ferr)
	}
}

// TestInferringSink_LateUnknownKeyTrackedAndUnwritten mirrors StringSink's
// unknown-key precedent: a key introduced after the (inferred) schema is
// fixed is dropped from every output batch but reported via UnknownKeys.
func TestInferringSink_LateUnknownKeyTrackedAndUnwritten(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil }, WithBatchRows(2))
	if err := sink.Add(map[string]any{"a": json.Number("1"), "b": "x"}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Add(map[string]any{"a": json.Number("2"), "b": "y"}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Add(map[string]any{"a": json.Number("3"), "c": "unexpected"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	keys, n := sink.UnknownKeys()
	if n != 1 {
		t.Fatalf("unknown records = %d, want 1", n)
	}
	if len(keys) != 1 || keys[0] != "c" {
		t.Fatalf("unknown keys = %v, want [c]", keys)
	}
	for _, p := range fw.payloads {
		_, names, _ := ipcRows(t, p)
		if len(names) != 2 || names[0] != "a" || names[1] != "b" {
			t.Fatalf("schema leaked unknown column: %v", names)
		}
	}
}

// TestInferringSink_WithColumnsFixesNamesTypesStillInferredNoUnknownKeyTracking
// proves the WithColumns+inference composition: names come from the
// caller, in the caller's order (NOT the sorted-union order full inference
// would produce), while each column's TYPE is still inferred from the
// projected values — and, matching StringSink's own WithColumns precedent,
// an explicit projection never triggers unknown-key tracking even though
// records here carry an extra "z" key never referenced by the projection.
func TestInferringSink_WithColumnsFixesNamesTypesStillInferredNoUnknownKeyTracking(t *testing.T) {
	fw := &fakeWriter{}
	names := []string{"y", "x"} // deliberately NOT alphabetical
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns(names, func(rec map[string]any, i int) any { return rec[names[i]] }),
		WithBatchRows(2))
	if err := sink.Add(map[string]any{"x": json.Number("1"), "y": true, "z": "extra"}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Add(map[string]any{"x": json.Number("2"), "y": false, "z": "extra2"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	_, gotNames, fields := ipcRows(t, fw.payloads[0])
	if len(gotNames) != 2 || gotNames[0] != "y" || gotNames[1] != "x" {
		t.Fatalf("names = %v, want [y x] (declared order, not sorted)", gotNames)
	}
	if fields[0].Type.ID() != arrow.BOOL {
		t.Fatalf("y type = %v, want Boolean", fields[0].Type)
	}
	if fields[1].Type.ID() != arrow.INT64 {
		t.Fatalf("x type = %v, want Int64", fields[1].Type)
	}
	keys, n := sink.UnknownKeys()
	if n != 0 || len(keys) != 0 {
		t.Fatalf("WithColumns plan must never report unknown keys even with inferred types, got keys=%v n=%d", keys, n)
	}
}

// =====================================================================
// Mode 2: declared columns
// =====================================================================

// TestArrowTypeFor_VocabularyTableDriven exercises the exported canonical
// vocabulary, including the case-sensitive and unrecognized-string error
// paths.
func TestArrowTypeFor_VocabularyTableDriven(t *testing.T) {
	cases := []struct {
		in      string
		wantID  arrow.Type
		wantErr bool
	}{
		{"int", arrow.INT32, false}, // 32-bit, matching Iceberg's `int`
		{"long", arrow.INT64, false},
		{"float", arrow.FLOAT64, false},
		{"double", arrow.FLOAT64, false},
		{"boolean", arrow.BOOL, false},
		{"string", arrow.STRING, false},
		{"bogus", 0, true},
		{"", 0, true},
		{"Int", 0, true}, // case-sensitive: "Int" must not match "int"
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%q", tc.in), func(t *testing.T) {
			got, err := ArrowTypeFor(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ArrowTypeFor(%q) = %v, want a non-nil error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ArrowTypeFor(%q) unexpected error: %v", tc.in, err)
			}
			if got.ID() != tc.wantID {
				t.Fatalf("ArrowTypeFor(%q) = %v, want ID %v", tc.in, got, tc.wantID)
			}
		})
	}
}

// TestInferringSink_DeclaredUnknownTypeStringErrorsAtConstruction proves the
// vocabulary error surfaces from NewInferringSink itself (constructor-time,
// per this implementation's documented choice), not merely from
// ArrowTypeFor in isolation, and that the sink returned is nil.
func TestInferringSink_DeclaredUnknownTypeStringErrorsAtConstruction(t *testing.T) {
	sink, err := NewInferringSink(context.Background(), func() (ChunkWriter, error) { return &fakeWriter{}, nil },
		WithTypedColumns([]TypedColumn{{Name: "x", Type: "bogus"}}))
	if err == nil {
		t.Fatal("want a config error for an unrecognized declared type, got nil")
	}
	if sink != nil {
		t.Fatalf("want nil sink on construction error, got %v", sink)
	}
}

// TestInferringSink_ConflictingColumnOptionsErrors proves WithTypedColumns
// and WithColumns together are rejected rather than one silently winning.
func TestInferringSink_ConflictingColumnOptionsErrors(t *testing.T) {
	sink, err := NewInferringSink(context.Background(), func() (ChunkWriter, error) { return &fakeWriter{}, nil },
		WithColumns([]string{"a"}, func(rec map[string]any, _ int) any { return rec["a"] }),
		WithTypedColumns([]TypedColumn{{Name: "a", Type: "string"}}))
	if err == nil {
		t.Fatal("want an error for conflicting WithColumns+WithTypedColumns, got nil")
	}
	if sink != nil {
		t.Fatalf("want nil sink on conflicting options, got %v", sink)
	}
}

// TestInferringSink_DeclaredExtraKeysUnwrittenAndUnreported proves a
// declared mapping is a deliberate projection: a record key outside the
// mapping is dropped from the output AND never surfaces via UnknownKeys
// (unlike inference mode's late-unknown-key tracking).
func TestInferringSink_DeclaredExtraKeysUnwrittenAndUnreported(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithTypedColumns([]TypedColumn{{Name: "a", Type: "int"}}),
		WithBatchRows(4))
	if err := sink.Add(map[string]any{"a": json.Number("1"), "extra": "ignored", "other": json.Number("99")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	_, names, _ := ipcRows(t, fw.payloads[0])
	if len(names) != 1 || names[0] != "a" {
		t.Fatalf("schema = %v, want only [a] (declared projection)", names)
	}
	keys, n := sink.UnknownKeys()
	if n != 0 || len(keys) != 0 {
		t.Fatalf("declared mode must never report unknown keys, got keys=%v n=%d", keys, n)
	}
}

// TestInferringSink_DeclaredMissingKeyIsNull proves a record missing a
// declared column's key produces Arrow null for that cell (not a zero
// value), and that declared columns are nullable regardless of type.
func TestInferringSink_DeclaredMissingKeyIsNull(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithTypedColumns([]TypedColumn{{Name: "a", Type: "int"}, {Name: "b", Type: "string"}}),
		WithBatchRows(2))
	if err := sink.Add(map[string]any{"a": json.Number("1"), "b": "x"}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Add(map[string]any{"a": json.Number("2")}); err != nil { // "b" missing
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	rows, fields := decodeInferredPayload(t, fw.payloads[0])
	for _, f := range fields {
		if !f.Nullable {
			t.Fatalf("field %s must be nullable in declared mode", f.Name)
		}
	}
	if rows[0]["b"] != "x" {
		t.Fatalf("row0 b = %v, want x", rows[0]["b"])
	}
	if rows[1]["b"] != nil {
		t.Fatalf("row1 b = %v, want nil (missing key -> Arrow null)", rows[1]["b"])
	}
}

// TestInferringSink_DeclaredValues pins how declared-mode columns resolve a Go
// value: each row declares one column, adds the values, and asserts either the
// resulting Arrow type and cells (exact, never reparsed or widened) or a
// *TypeViolationError on that column (never a *WriteError). Narrowing "int" to
// 32-bit is only defensible because overflow is loud; declared String is not
// the permissive top type inference mode's is.
func TestInferringSink_DeclaredValues(t *testing.T) {
	nested := map[string]any{"a": json.Number("1")}
	nestedWant, isNull := stringifyValue(nested)
	if isNull {
		t.Fatal("test setup: stringifyValue(nested) unexpectedly null")
	}

	cases := []struct {
		name          string
		declared      string
		values        []any
		wantType      arrow.Type // success rows
		wantCells     []any      // success rows: decoded cell per value
		wantViolation bool       // the Add must fail with a *TypeViolationError
	}{
		{
			// Exceeds int32, hence "long"; resolves json.Number via its exact text.
			name: "long from json.Number exact", declared: "long",
			values:   []any{json.Number("5938028332")},
			wantType: arrow.INT64, wantCells: []any{int64(5938028332)},
		},
		{
			// A declared "int" is 32-bit on the wire (Iceberg's int), not widened
			// to Int64; values across the int32 boundary round-trip exactly.
			name: "int is 32-bit", declared: "int",
			values:   []any{json.Number("0"), json.Number("2147483647"), json.Number("-2147483648")},
			wantType: arrow.INT32, wantCells: []any{int32(0), int32(2147483647), int32(-2147483648)},
		},
		{name: "int overflow just above MaxInt32", declared: "int", values: []any{json.Number("2147483648")}, wantViolation: true},
		{name: "int overflow just below MinInt32", declared: "int", values: []any{json.Number("-2147483649")}, wantViolation: true},
		{name: "int overflow far beyond", declared: "int", values: []any{json.Number("5938028332")}, wantViolation: true},
		{
			name: "float from json.Number", declared: "float",
			values:   []any{json.Number("1.50")},
			wantType: arrow.FLOAT64, wantCells: []any{float64(1.5)},
		},
		{
			// Keeps json.Number's exact source text, not a reparsed "1.5".
			name: "string from json.Number keeps exact text", declared: "string",
			values:   []any{json.Number("1.50")},
			wantType: arrow.STRING, wantCells: []any{"1.50"},
		},
		{name: "string value violates int column", declared: "int", values: []any{"abc"}, wantViolation: true},
		{name: "bool value violates string column", declared: "string", values: []any{true}, wantViolation: true},
		{
			// A nested map/slice fits a declared string as compact JSON identical
			// to stringifyValue.
			name: "nested value fits string column as compact JSON", declared: "string",
			values:   []any{nested},
			wantType: arrow.STRING, wantCells: []any{nestedWant},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fw := &fakeWriter{}
			sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
				WithTypedColumns([]TypedColumn{{Name: "n", Type: tc.declared}}), WithBatchRows(4))

			if tc.wantViolation {
				err := sink.Add(map[string]any{"n": tc.values[0]})
				if err == nil {
					t.Fatalf("Add(%v) into a declared %s column: got nil, want a type violation", tc.values[0], tc.declared)
				}
				var tv *TypeViolationError
				if !errors.As(err, &tv) {
					t.Fatalf("want *TypeViolationError, got %T: %v", err, err)
				}
				if tv.Column != "n" {
					t.Errorf("Column = %q, want %q", tv.Column, "n")
				}
				var we *WriteError
				if errors.As(err, &we) {
					t.Errorf("a value that does not fit a declared column is a user error, never a *WriteError: %v", we)
				}
				return
			}

			for _, v := range tc.values {
				if err := sink.Add(map[string]any{"n": v}); err != nil {
					t.Fatalf("Add(%v): %v", v, err)
				}
			}
			if _, _, err := sink.Finish(); err != nil {
				t.Fatal(err)
			}
			rows, fields := decodeInferredPayload(t, fw.payloads[0])
			if fields[0].Type.ID() != tc.wantType {
				t.Fatalf("type = %v, want %v", fields[0].Type, tc.wantType)
			}
			for i, want := range tc.wantCells {
				if rows[i]["n"] != want {
					t.Errorf("row%d n = %v (%T), want %v (%T)", i, rows[i]["n"], rows[i]["n"], want, want)
				}
			}
		})
	}
}
