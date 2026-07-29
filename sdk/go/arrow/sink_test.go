package arrow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

func TestStringifyValue(t *testing.T) {
	cases := []struct {
		name     string
		in       any
		want     string
		wantNull bool
	}{
		{"nil", nil, "", true},
		{"string", "hello", "hello", false},
		{"json_number_int", json.Number("5938028332"), "5938028332", false},
		{"json_number_frac", json.Number("1.50"), "1.50", false},
		{"bool", true, "true", false},
		{"float64_defensive", float64(2.5), "2.5", false},
		{"nested_object", map[string]any{"a": json.Number("1")}, `{"a":1}`, false},
		{"nested_array", []any{json.Number("1"), "x"}, `[1,"x"]`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, null := stringifyValue(tc.in)
			if null != tc.wantNull || got != tc.want {
				t.Fatalf("stringifyValue(%#v) = (%q,%v), want (%q,%v)", tc.in, got, null, tc.want, tc.wantNull)
			}
		})
	}
}

func TestPlanFromBatch(t *testing.T) {
	batch := []map[string]any{
		{"b": 1, "a": 2},
		{"c": 3}, // later-only key must still become a column (union)
	}
	p := planFromBatch(batch)
	if len(p.names) != 3 || p.names[0] != "a" || p.names[1] != "b" || p.names[2] != "c" {
		t.Fatalf("names = %v, want sorted union [a b c]", p.names)
	}
	if v := p.extract(batch[1], 2); v != 3 {
		t.Fatalf("extract c = %v", v)
	}
	if v := p.extract(batch[1], 0); v != nil {
		t.Fatalf("absent key should be nil, got %v", v)
	}
}

// --- appended in Task 3 ---

type fakeWriter struct {
	payloads    [][]byte
	closed      bool
	rows        int64
	closeCount  int
	failWriteOn int // fail on the Nth Write call (0 = never fail)
	writeCount  int
}

func (f *fakeWriter) Write(_ context.Context, data []byte) error {
	f.writeCount++
	if f.failWriteOn > 0 && f.writeCount == f.failWriteOn {
		return errors.New("write failed")
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.payloads = append(f.payloads, cp)
	return nil
}
func (f *fakeWriter) Close(context.Context) (*sdk.CloseResult, error) {
	f.closed = true
	f.closeCount++
	return &sdk.CloseResult{TotalRows: f.rows}, nil
}
func (f *fakeWriter) Bucket() string { return "raw" }
func (f *fakeWriter) Table() string  { return "t" }

// ipcRows parses one payload as a complete standalone IPC stream and returns
// (rows, columnNames, fields). Fails the test if the payload is not self-contained
// or if trailing bytes exist after the stream.
func ipcRows(t *testing.T, payload []byte) (int64, []string, []arrow.Field) {
	t.Helper()
	br := bytes.NewReader(payload)
	rd, err := ipc.NewReader(br)
	if err != nil {
		t.Fatalf("payload is not a valid IPC stream: %v", err)
	}
	defer rd.Release()
	var rows int64
	for rd.Next() {
		rows += rd.Record().NumRows()
	}
	if rd.Err() != nil {
		t.Fatalf("ipc read: %v", rd.Err())
	}
	names := make([]string, 0)
	fields := rd.Schema().Fields()
	for _, f := range fields {
		names = append(names, f.Name)
	}
	// Verify no trailing bytes after the stream
	remaining, err := br.ReadByte()
	if err == nil {
		t.Fatalf("trailing byte after IPC stream: %d", remaining)
	}
	if err.Error() != "EOF" {
		t.Fatalf("reading after IPC stream: %v", err)
	}
	return rows, names, fields
}

func TestStringSink_BatchBoundariesAndSelfContainedStreams(t *testing.T) {
	fw := &fakeWriter{}
	sink := NewStringSink(context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }),
		WithBatchRows(4) /* tiny batch */)
	for i := 0; i < 10; i++ {
		if err := sink.Add(map[string]any{"id": json.Number("1")}); err != nil {
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
		n, names, fields := ipcRows(t, p) // each independently parseable = no-concat invariant
		total += n
		if len(names) != 1 || names[0] != "id" {
			t.Fatalf("schema = %v", names)
		}
		// Verify all fields are String and Nullable
		if len(fields) != 1 {
			t.Fatalf("expected 1 field, got %d", len(fields))
		}
		if fields[0].Type.ID() != arrow.STRING {
			t.Fatalf("field type = %v, want String", fields[0].Type)
		}
		if !fields[0].Nullable {
			t.Fatalf("field should be Nullable")
		}
	}
	if total != 10 {
		t.Fatalf("ipc total rows = %d, want 10", total)
	}
	if !fw.closed {
		t.Fatal("writer not closed")
	}
}

func TestStringSink_NoColumnsDerivesPlanFromFirstBatch(t *testing.T) {
	fw := &fakeWriter{}
	sink := NewStringSink(context.Background(), func() (ChunkWriter, error) { return fw, nil }, WithBatchRows(3))
	// key "c" appears only in the 2nd record — union must include it.
	recs := []map[string]any{
		{"b": json.Number("1")},
		{"a": "x", "c": true},
		{"a": "y"},
		{"a": "z"}, // second batch
	}
	for _, r := range recs {
		if err := sink.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	rows, _, err := sink.Finish()
	if err != nil || rows != 4 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	_, names, fields := ipcRows(t, fw.payloads[0])
	want := []string{"a", "b", "c"}
	if len(names) != 3 || names[0] != want[0] || names[1] != want[1] || names[2] != want[2] {
		t.Fatalf("schema = %v, want %v (sorted union of first batch)", names, want)
	}
	// Verify all fields are String and Nullable
	for i, f := range fields {
		if f.Type.ID() != arrow.STRING {
			t.Fatalf("field %d (%s) type = %v, want String", i, f.Name, f.Type)
		}
		if !f.Nullable {
			t.Fatalf("field %d (%s) should be Nullable", i, f.Name)
		}
	}
}

func TestStringSink_ZeroRecordsNeverOpensWriter(t *testing.T) {
	opened := false
	sink := NewStringSink(context.Background(), func() (ChunkWriter, error) {
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

func TestStringSink_OpenFailureIsWriteError(t *testing.T) {
	sink := NewStringSink(context.Background(), func() (ChunkWriter, error) {
		return nil, errors.New("open boom")
	}, WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }), WithBatchRows(1))
	err := sink.Add(map[string]any{"id": "1"})
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("want WriteError, got %v", err)
	}
}

func TestStringSink_FinishIsIdempotent(t *testing.T) {
	fw := &fakeWriter{}
	sink := NewStringSink(context.Background(), func() (ChunkWriter, error) { return fw, nil },
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

func TestStringSink_AddAfterFinishErrors(t *testing.T) {
	fw := &fakeWriter{}
	sink := NewStringSink(context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }),
		WithBatchRows(4))
	if err := sink.Add(map[string]any{"id": json.Number("1")}); err != nil {
		t.Fatal(err)
	}
	_, _, err := sink.Finish()
	if err != nil {
		t.Fatal(err)
	}
	// Add after Finish should error, not panic
	err = sink.Add(map[string]any{"id": json.Number("2")})
	if err == nil {
		t.Fatal("Add after Finish should error")
	}
	if err.Error() != "arrow: StringSink.Add called after Finish" {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestStringSink_WriteFailureIsStickyAndDropsNoRowsSilently(t *testing.T) {
	fw := &fakeWriter{failWriteOn: 2} // fail on second Write
	sink := NewStringSink(context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }),
		WithBatchRows(2))
	// Add 5 records: 2 flush (success), 2 flush (fail), 1 pending
	var failureErr error
	for i := 0; i < 5; i++ {
		err := sink.Add(map[string]any{"id": json.Number(fmt.Sprintf("%d", i))})
		if i < 3 && err != nil {
			t.Fatalf("Add %d should succeed, got: %v", i, err)
		}
		if i == 3 { // second flush fails
			if err == nil {
				t.Fatalf("Add 3 (second flush) should fail")
			}
			var we *WriteError
			if !errors.As(err, &we) {
				t.Fatalf("Add 3 (second flush) want WriteError, got %v", err)
			}
			failureErr = err
		}
		if i == 4 { // after failure, Add should return the sticky error
			if err != failureErr {
				t.Fatalf("Add 4 should return same sticky error, got different error: %v vs %v", err, failureErr)
			}
		}
	}
	// The sticky error is now set; Finish should return it without further writes
	_, _, err := sink.Finish()
	if err != failureErr {
		t.Fatalf("Finish should return sticky error, got: %v", err)
	}
	// Verify: 1 successful write, then the failure; no further writes
	if len(fw.payloads) != 1 {
		t.Fatalf("payloads = %d, want 1 (second batch never sent after write failed)", len(fw.payloads))
	}
	if fw.closeCount != 0 {
		t.Fatalf("closeCount = %d, want 0 (writer never successfully closed after failure)", fw.closeCount)
	}
}

func TestStringSink_CellValuesAndNullability(t *testing.T) {
	fw := &fakeWriter{}
	sink := NewStringSink(context.Background(), func() (ChunkWriter, error) { return fw, nil },
		WithColumns([]string{"a", "b"}, func(rec map[string]any, i int) any {
			if i == 0 {
				return rec["a"]
			}
			return rec["b"]
		}),
		WithBatchRows(2))
	// Add records with a missing key to verify null handling
	recs := []map[string]any{
		{"a": json.Number("1"), "b": "x"},
		{"a": "y"}, // b is missing => null
	}
	for _, r := range recs {
		if err := sink.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err := sink.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(fw.payloads) != 1 {
		t.Fatalf("payloads = %d, want 1", len(fw.payloads))
	}
	n, names, fields := ipcRows(t, fw.payloads[0])
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("schema = %v, want [a b]", names)
	}
	// Verify field types and nullability
	for i, f := range fields {
		if f.Type.ID() != arrow.STRING {
			t.Fatalf("field %d type = %v, want String", i, f.Type)
		}
		if !f.Nullable {
			t.Fatalf("field %d should be Nullable", i)
		}
	}
}
