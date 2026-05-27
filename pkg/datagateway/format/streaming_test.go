package format

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// TestArrowIPCParseReader_MatchesParse confirms the streaming ParseReader
// path yields the same record as the buffered Parse path (nil schema:
// inferred from the IPC stream).
func TestArrowIPCParseReader_MatchesParse(t *testing.T) {
	alloc := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(alloc)

	original := makeTestRecord(alloc)
	defer original.Release()

	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	// Streaming parse straight off a reader.
	rec, s, err := adapter.ParseReader(bytes.NewReader(ipcData), nil)
	if err != nil {
		t.Fatalf("ParseReader: %v", err)
	}
	defer rec.Release()

	if s.NumColumns() != 3 {
		t.Fatalf("schema cols = %d, want 3", s.NumColumns())
	}
	if rec.NumRows() != 3 {
		t.Fatalf("rows = %d, want 3", rec.NumRows())
	}
	idCol := rec.Column(0).(*array.Int64)
	if idCol.Value(0) != 1 || idCol.Value(1) != 2 || idCol.Value(2) != 3 {
		t.Errorf("id column values incorrect: %v", idCol)
	}
	nameCol := rec.Column(1).(*array.String)
	if nameCol.Value(0) != "Widget" || nameCol.Value(1) != "Gadget" {
		t.Errorf("name column values incorrect")
	}
}

// TestArrowIPCParseReader_KnownSchema confirms ParseReader validates and
// uses a provided schema (the hot-path branch in production).
func TestArrowIPCParseReader_KnownSchema(t *testing.T) {
	alloc := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(alloc)

	original := makeTestRecord(alloc)
	defer original.Release()
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	s, err := schema.NewSchemaFromArrow(original.Schema())
	if err != nil {
		t.Fatalf("NewSchemaFromArrow: %v", err)
	}

	rec, gotSchema, err := adapter.ParseReader(bytes.NewReader(ipcData), s)
	if err != nil {
		t.Fatalf("ParseReader(known schema): %v", err)
	}
	defer rec.Release()

	if gotSchema != s {
		t.Errorf("ParseReader should return the provided schema unchanged")
	}
	if rec.NumRows() != 3 {
		t.Errorf("rows = %d, want 3", rec.NumRows())
	}
}

// TestArrowIPCParseReader_EmptyStreamErrors confirms an empty stream
// surfaces as an error (the streaming analog of Parse's empty-data guard).
func TestArrowIPCParseReader_EmptyStreamErrors(t *testing.T) {
	alloc := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(alloc)

	_, _, err := adapter.ParseReader(bytes.NewReader(nil), nil)
	if err == nil {
		t.Fatal("ParseReader on empty stream should error")
	}
}

// TestJSONLParseReader_KnownSchema confirms the JSONL streaming path
// (known schema) matches the buffered Parse path row-for-row.
func TestJSONLParseReader_KnownSchema(t *testing.T) {
	alloc := memory.NewGoAllocator()
	adapter := NewJSONLAdapter(alloc, nil)

	jsonl := []byte(`{"id":1,"name":"a"}` + "\n" + `{"id":2,"name":"b"}` + "\n")

	s, err := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: true},
		{Name: "name", Type: schema.TypeString, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema: %v", err)
	}

	// Buffered baseline.
	bufRec, _, err := adapter.Parse(jsonl, s)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer bufRec.Release()

	// Streaming.
	strRec, _, err := adapter.ParseReader(bytes.NewReader(jsonl), s)
	if err != nil {
		t.Fatalf("ParseReader: %v", err)
	}
	defer strRec.Release()

	if strRec.NumRows() != bufRec.NumRows() {
		t.Fatalf("row count mismatch: streaming %d vs buffered %d", strRec.NumRows(), bufRec.NumRows())
	}
	if strRec.NumRows() != 2 {
		t.Fatalf("rows = %d, want 2", strRec.NumRows())
	}
	ids := strRec.Column(0).(*array.Int64)
	if ids.Value(0) != 1 || ids.Value(1) != 2 {
		t.Errorf("id values incorrect: %v", ids)
	}
	names := strRec.Column(1).(*array.String)
	if names.Value(0) != "a" || names.Value(1) != "b" {
		t.Errorf("name values incorrect")
	}
}

// TestJSONLParseReader_NilSchemaInfers confirms ParseReader falls back to
// buffering + inference when no schema is supplied (first-chunk path).
func TestJSONLParseReader_NilSchemaInfers(t *testing.T) {
	alloc := memory.NewGoAllocator()
	adapter := NewJSONLAdapter(alloc, nil)

	jsonl := []byte(`{"id":1,"name":"a"}` + "\n" + `{"id":2,"name":"b"}` + "\n")

	rec, s, err := adapter.ParseReader(bytes.NewReader(jsonl), nil)
	if err != nil {
		t.Fatalf("ParseReader(nil schema): %v", err)
	}
	defer rec.Release()

	if s == nil || s.NumColumns() != 2 {
		t.Fatalf("inferred schema wrong: %+v", s)
	}
	if rec.NumRows() != 2 {
		t.Errorf("rows = %d, want 2", rec.NumRows())
	}
}
