package arrow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

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
	payloads [][]byte
	closed   bool
	rows     int64
}

func (f *fakeWriter) Write(_ context.Context, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	f.payloads = append(f.payloads, cp)
	return nil
}
func (f *fakeWriter) Close(context.Context) (*sdk.CloseResult, error) {
	f.closed = true
	return &sdk.CloseResult{TotalRows: f.rows}, nil
}
func (f *fakeWriter) Bucket() string { return "raw" }
func (f *fakeWriter) Table() string  { return "t" }

// ipcRows parses one payload as a complete standalone IPC stream and returns
// (rows, columnNames). Fails the test if the payload is not self-contained.
func ipcRows(t *testing.T, payload []byte) (int64, []string) {
	t.Helper()
	rd, err := ipc.NewReader(bytes.NewReader(payload))
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
	for _, f := range rd.Schema().Fields() {
		names = append(names, f.Name)
	}
	return rows, names
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
		n, names := ipcRows(t, p) // each independently parseable = no-concat invariant
		total += n
		if len(names) != 1 || names[0] != "id" {
			t.Fatalf("schema = %v", names)
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
	_, names := ipcRows(t, fw.payloads[0])
	want := []string{"a", "b", "c"}
	if len(names) != 3 || names[0] != want[0] || names[1] != want[1] || names[2] != want[2] {
		t.Fatalf("schema = %v, want %v (sorted union of first batch)", names, want)
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
