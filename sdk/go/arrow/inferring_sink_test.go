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
// Int64/Float64/Boolean/String columns — InferringSink's typed output can mix
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

// TestInferringSink_BatchBoundariesAndSelfContainedStreams mirrors the
// StringSink/Sink precedent test of the same shape: a tiny batch size forces
// multiple flushes, each payload independently parseable (the no-concat
// invariant), row totals and schema consistent across every payload.
func TestInferringSink_BatchBoundariesAndSelfContainedStreams(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }),
		WithBatchRows(4))
	for i := 0; i < 10; i++ {
		if err := sink.Add(map[string]any{"id": json.Number(fmt.Sprintf("%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	rows, _, err := sink.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if rows != 10 {
		t.Fatalf("rows = %d, want 10", rows)
	}
	if len(fw.payloads) != 3 { // 4+4+2
		t.Fatalf("payloads = %d, want 3 (4+4+2)", len(fw.payloads))
	}
	var total int64
	for _, p := range fw.payloads {
		n, names, fields := ipcRows(t, p)
		total += n
		if len(names) != 1 || names[0] != "id" || fields[0].Type.ID() != arrow.INT64 {
			t.Fatalf("schema = %v", names)
		}
		if !fields[0].Nullable {
			t.Fatal("field should be nullable")
		}
	}
	if total != 10 {
		t.Fatalf("ipc total rows = %d, want 10", total)
	}
	if !fw.closed {
		t.Fatal("writer not closed")
	}
}

// TestInferringSink_ZeroRecordsNeverOpensWriter mirrors the StringSink/Sink
// zero-rows precedent: the writer's lazy-open guarantee holds even though
// InferringSink defers schema construction internally.
func TestInferringSink_ZeroRecordsNeverOpensWriter(t *testing.T) {
	opened := false
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) {
		opened = true
		return &fakeWriter{}, nil
	}, WithBatchRows(4))
	rows, cr, err := sink.Finish()
	if err != nil || rows != 0 || cr != nil {
		t.Fatalf("rows=%d cr=%v err=%v", rows, cr, err)
	}
	if opened {
		t.Fatal("writer must not be opened for zero records")
	}
	if sink.Writer() != nil {
		t.Fatal("Writer() must be nil for zero records")
	}
}

// TestInferringSink_OpenFailureIsWriteError mirrors the StringSink/Sink
// precedent: a writer-open failure surfaces as *WriteError.
func TestInferringSink_OpenFailureIsWriteError(t *testing.T) {
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) {
		return nil, errors.New("open boom")
	}, WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }), WithBatchRows(1))
	err := sink.Add(map[string]any{"id": "1"})
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("want WriteError, got %v", err)
	}
}

// TestInferringSink_FinishIsIdempotent mirrors the StringSink/Sink precedent.
func TestInferringSink_FinishIsIdempotent(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }),
		WithBatchRows(4))
	if err := sink.Add(map[string]any{"id": json.Number("1")}); err != nil {
		t.Fatal(err)
	}
	rows1, cr1, err1 := sink.Finish()
	rows2, cr2, err2 := sink.Finish()
	if err1 != nil || err2 != nil {
		t.Fatalf("Finish errors: %v, %v", err1, err2)
	}
	if rows1 != 1 || rows2 != 1 {
		t.Fatalf("rows mismatch: %d vs %d", rows1, rows2)
	}
	if cr1 != cr2 {
		t.Fatalf("closeResult mismatch")
	}
	if fw.closeCount != 1 {
		t.Fatalf("closeCount = %d, want 1 (not double-closed)", fw.closeCount)
	}
}

// TestInferringSink_AddAfterFinishErrors mirrors the StringSink precedent.
func TestInferringSink_AddAfterFinishErrors(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }),
		WithBatchRows(4))
	if err := sink.Add(map[string]any{"id": json.Number("1")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	err := sink.Add(map[string]any{"id": json.Number("2")})
	if err == nil {
		t.Fatal("Add after Finish should error")
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
		{"int", arrow.INT64, false},
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

// TestInferringSink_DeclaredIntFromJSONNumberExact proves declared "int"
// resolves json.Number via its exact text (ParseInt), matching inference
// mode's own numeric-fidelity guarantee.
func TestInferringSink_DeclaredIntFromJSONNumberExact(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithTypedColumns([]TypedColumn{{Name: "n", Type: "int"}}), WithBatchRows(1))
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
		t.Fatalf("value = %v, want 5938028332 exactly", rows[0]["n"])
	}
}

// TestInferringSink_DeclaredFloatFromJSONNumber proves declared
// "float"/"double" resolves json.Number via ParseFloat.
func TestInferringSink_DeclaredFloatFromJSONNumber(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithTypedColumns([]TypedColumn{{Name: "n", Type: "float"}}), WithBatchRows(1))
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

// TestInferringSink_DeclaredStringFromJSONNumberKeepsExactText proves a
// declared "string" column keeps json.Number's exact source text ("1.50"),
// not a reparsed-and-reformatted "1.5".
func TestInferringSink_DeclaredStringFromJSONNumberKeepsExactText(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithTypedColumns([]TypedColumn{{Name: "n", Type: "string"}}), WithBatchRows(1))
	if err := sink.Add(map[string]any{"n": json.Number("1.50")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	rows, fields := decodeInferredPayload(t, fw.payloads[0])
	if fields[0].Type.ID() != arrow.STRING {
		t.Fatalf("type = %v, want String", fields[0].Type)
	}
	if rows[0]["n"] != "1.50" {
		t.Fatalf("value = %v, want exact source text %q", rows[0]["n"], "1.50")
	}
}

// TestInferringSink_DeclaredStringValueViolatesIntColumn proves a plain Go
// string does not coerce into a declared "int" column: TypeViolationError,
// not a silent parse-or-null fallback.
func TestInferringSink_DeclaredStringValueViolatesIntColumn(t *testing.T) {
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return &fakeWriter{}, nil },
		WithTypedColumns([]TypedColumn{{Name: "n", Type: "int"}}), WithBatchRows(4))
	err := sink.Add(map[string]any{"n": "abc"})
	var tv *TypeViolationError
	if !errors.As(err, &tv) {
		t.Fatalf("want *TypeViolationError, got %T: %v", err, err)
	}
	if tv.Column != "n" {
		t.Fatalf("Column = %q, want %q", tv.Column, "n")
	}
	var we *WriteError
	if errors.As(err, &we) {
		t.Fatalf("must NOT be a *WriteError, got %v", we)
	}
}

// TestInferringSink_DeclaredBoolValueViolatesStringColumn proves declared
// mode's String is NOT the permissive top type inference mode's is: a bool
// value does not fit a declared "string" column.
func TestInferringSink_DeclaredBoolValueViolatesStringColumn(t *testing.T) {
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return &fakeWriter{}, nil },
		WithTypedColumns([]TypedColumn{{Name: "s", Type: "string"}}), WithBatchRows(4))
	err := sink.Add(map[string]any{"s": true})
	var tv *TypeViolationError
	if !errors.As(err, &tv) {
		t.Fatalf("want *TypeViolationError (bool must not fit a declared string column), got %T: %v", err, err)
	}
}

// TestInferringSink_DeclaredNestedValueFitsStringColumnAsCompactJSON proves
// a nested map/slice DOES fit a declared "string" column, rendered as
// compact JSON identical to stringifyValue — the one shape declared String
// accepts beyond a literal Go string, besides json.Number/float64.
func TestInferringSink_DeclaredNestedValueFitsStringColumnAsCompactJSON(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithTypedColumns([]TypedColumn{{Name: "s", Type: "string"}}), WithBatchRows(1))
	nested := map[string]any{"a": json.Number("1")}
	want, isNull := stringifyValue(nested)
	if isNull {
		t.Fatal("test setup: stringifyValue(nested) unexpectedly null")
	}
	if err := sink.Add(map[string]any{"s": nested}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	rows, _ := decodeInferredPayload(t, fw.payloads[0])
	if rows[0]["s"] != want {
		t.Fatalf("cell = %v, want %v", rows[0]["s"], want)
	}
}

// TestInferringSink_DeclaredZeroRecordsNeverOpensWriter proves the
// lazy-writer-open guarantee also holds in declared mode, even though the
// Sink core itself is built immediately at construction (schema known up
// front) rather than deferred to a sampled batch.
func TestInferringSink_DeclaredZeroRecordsNeverOpensWriter(t *testing.T) {
	opened := false
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) {
		opened = true
		return &fakeWriter{}, nil
	}, WithTypedColumns([]TypedColumn{{Name: "a", Type: "int"}}), WithBatchRows(4))
	rows, cr, err := sink.Finish()
	if err != nil || rows != 0 || cr != nil {
		t.Fatalf("rows=%d cr=%v err=%v", rows, cr, err)
	}
	if opened {
		t.Fatal("writer must not be opened for zero records, even though the core is built immediately in declared mode")
	}
	if sink.Writer() != nil {
		t.Fatal("Writer() must be nil for zero records")
	}
}

// TestInferringSink_DeclaredBatchBoundariesAndSelfContainedStreams proves
// batching/flush/self-contained-stream behavior is identical to inference
// mode's, in declared mode's no-buffering construction path.
func TestInferringSink_DeclaredBatchBoundariesAndSelfContainedStreams(t *testing.T) {
	fw := &fakeWriter{}
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithTypedColumns([]TypedColumn{{Name: "id", Type: "int"}}), WithBatchRows(4))
	for i := 0; i < 10; i++ {
		if err := sink.Add(map[string]any{"id": json.Number(fmt.Sprintf("%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	rows, _, err := sink.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if rows != 10 {
		t.Fatalf("rows = %d, want 10", rows)
	}
	if len(fw.payloads) != 3 { // 4+4+2
		t.Fatalf("payloads = %d, want 3 (4+4+2)", len(fw.payloads))
	}
	var total int64
	for _, p := range fw.payloads {
		n, names, fields := ipcRows(t, p)
		total += n
		if len(names) != 1 || names[0] != "id" || fields[0].Type.ID() != arrow.INT64 {
			t.Fatalf("schema = %v", names)
		}
	}
	if total != 10 {
		t.Fatalf("total = %d, want 10", total)
	}
	if !fw.closed {
		t.Fatal("writer not closed")
	}
}

// TestInferringSink_DeclaredOpenFailureIsWriteError proves *WriteError
// plumbing still works through declared mode's immediately-built core.
func TestInferringSink_DeclaredOpenFailureIsWriteError(t *testing.T) {
	sink := mustNewInferringSink(t, context.Background(), func() (ChunkWriter, error) {
		return nil, errors.New("open boom")
	}, WithTypedColumns([]TypedColumn{{Name: "a", Type: "int"}}), WithBatchRows(1))
	err := sink.Add(map[string]any{"a": json.Number("1")})
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("want *WriteError, got %v", err)
	}
}
