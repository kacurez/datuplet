package format

import (
	"bytes"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// serializeIPCStream writes one or more record batches into a SINGLE Arrow
// IPC stream (one schema message, N record-batch messages, one EOS) — the
// legitimate multi-batch case, as opposed to concatenating the output of
// multiple Serialize() calls (which produces N independent streams back to
// back and is the defect this file's guard tests exercise).
func serializeIPCStream(t *testing.T, allocator memory.Allocator, records ...arrow.Record) []byte {
	t.Helper()
	if len(records) == 0 {
		t.Fatal("serializeIPCStream: need at least one record")
	}

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(records[0].Schema()), ipc.WithAllocator(allocator))
	for _, rec := range records {
		if err := writer.Write(rec); err != nil {
			t.Fatalf("ipc writer.Write() error: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("ipc writer.Close() error: %v", err)
	}
	return buf.Bytes()
}

func TestArrowIPCAdapterFormat(t *testing.T) {
	adapter := NewArrowIPCAdapter(nil)
	if adapter.Format() != FormatArrowIPC {
		t.Errorf("Format() = %v, want FormatArrowIPC", adapter.Format())
	}
}

func makeTestRecord(allocator memory.Allocator) arrow.Record {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()

	// Row 0
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("Widget")
	builder.Field(2).(*array.Float64Builder).Append(9.99)

	// Row 1
	builder.Field(0).(*array.Int64Builder).Append(2)
	builder.Field(1).(*array.StringBuilder).Append("Gadget")
	builder.Field(2).(*array.Float64Builder).Append(19.99)

	// Row 2 with null
	builder.Field(0).(*array.Int64Builder).Append(3)
	builder.Field(1).(*array.StringBuilder).AppendNull()
	builder.Field(2).(*array.Float64Builder).AppendNull()

	return builder.NewRecord()
}

func TestArrowIPCAdapterRoundTrip(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a test record
	original := makeTestRecord(allocator)
	defer original.Release()

	// Serialize to IPC
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	if len(ipcData) == 0 {
		t.Fatal("Serialize() returned empty data")
	}

	// Parse back
	parsed, s, err := adapter.Parse(ipcData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer parsed.Release()

	// Verify schema
	if s.NumColumns() != 3 {
		t.Errorf("Schema has %d columns, want 3", s.NumColumns())
	}

	// Verify column names
	if s.Column(0).Name != "id" {
		t.Errorf("Column 0 name = %q, want id", s.Column(0).Name)
	}
	if s.Column(1).Name != "name" {
		t.Errorf("Column 1 name = %q, want name", s.Column(1).Name)
	}
	if s.Column(2).Name != "price" {
		t.Errorf("Column 2 name = %q, want price", s.Column(2).Name)
	}

	// Verify row count
	if parsed.NumRows() != 3 {
		t.Errorf("NumRows() = %d, want 3", parsed.NumRows())
	}

	// Verify data
	idCol := parsed.Column(0).(*array.Int64)
	if idCol.Value(0) != 1 || idCol.Value(1) != 2 || idCol.Value(2) != 3 {
		t.Errorf("id column values incorrect")
	}

	nameCol := parsed.Column(1).(*array.String)
	if nameCol.Value(0) != "Widget" || nameCol.Value(1) != "Gadget" {
		t.Errorf("name column values incorrect")
	}
	if !nameCol.IsNull(2) {
		t.Errorf("name column row 2 should be null")
	}

	priceCol := parsed.Column(2).(*array.Float64)
	if priceCol.Value(0) != 9.99 || priceCol.Value(1) != 19.99 {
		t.Errorf("price column values incorrect")
	}
	if !priceCol.IsNull(2) {
		t.Errorf("price column row 2 should be null")
	}
}

func TestArrowIPCAdapterParseWithProvidedSchema(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a test record
	original := makeTestRecord(allocator)
	defer original.Release()

	// Serialize to IPC
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Create matching schema
	s, err := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "price", Type: schema.TypeFloat64, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	// Parse with provided schema
	parsed, resultSchema, err := adapter.Parse(ipcData, s)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer parsed.Release()

	// Should use provided schema
	if resultSchema != s {
		t.Error("Parse() should return the provided schema")
	}

	if parsed.NumRows() != 3 {
		t.Errorf("NumRows() = %d, want 3", parsed.NumRows())
	}
}

func TestArrowIPCAdapterParseSchemaMismatch(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a test record
	original := makeTestRecord(allocator)
	defer original.Release()

	// Serialize to IPC
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Create mismatched schema (different column count)
	s, err := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	// Parse with mismatched schema should fail
	_, _, err = adapter.Parse(ipcData, s)
	if err == nil {
		t.Error("Parse() should fail with schema mismatch")
	}
}

func TestArrowIPCAdapterParseTypeMismatch(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a test record
	original := makeTestRecord(allocator)
	defer original.Release()

	// Serialize to IPC
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Create schema with wrong type for 'id' column
	s, err := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeString, Nullable: false}, // Wrong type
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "price", Type: schema.TypeFloat64, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema() error: %v", err)
	}

	// Parse with type mismatch should fail
	_, _, err = adapter.Parse(ipcData, s)
	if err == nil {
		t.Error("Parse() should fail with type mismatch")
	}
}

func TestArrowIPCAdapterParseEmpty(t *testing.T) {
	adapter := NewArrowIPCAdapter(nil)

	_, _, err := adapter.Parse([]byte{}, nil)
	if err == nil {
		t.Error("Parse() should fail with empty data")
	}
}

func TestArrowIPCAdapterInRegistry(t *testing.T) {
	registry := DefaultRegistry()

	adapter, err := registry.Get(FormatArrowIPC)
	if err != nil {
		t.Fatalf("Registry.Get(FormatArrowIPC) error: %v", err)
	}

	if adapter.Format() != FormatArrowIPC {
		t.Errorf("Adapter format = %v, want FormatArrowIPC", adapter.Format())
	}
}

func TestDataFormatArrowIPC(t *testing.T) {
	f := FormatArrowIPC

	if f.String() != "arrow" {
		t.Errorf("String() = %q, want arrow", f.String())
	}

	if f.Extension() != ".arrow" {
		t.Errorf("Extension() = %q, want .arrow", f.Extension())
	}

	if f.MimeType() != "application/vnd.apache.arrow.stream" {
		t.Errorf("MimeType() = %q, want application/vnd.apache.arrow.stream", f.MimeType())
	}
}

func TestParseDataFormatArrowIPC(t *testing.T) {
	tests := []struct {
		input  string
		expect DataFormat
	}{
		{"arrow", FormatArrowIPC},
		{"ARROW", FormatArrowIPC},
		{"ipc", FormatArrowIPC},
		{"IPC", FormatArrowIPC},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseDataFormat(tt.input)
			if result != tt.expect {
				t.Errorf("ParseDataFormat(%q) = %v, want %v", tt.input, result, tt.expect)
			}
		})
	}
}

func TestArrowIPCAdapterWithDifferentTypes(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	// Create a record with various types
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "int8_col", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "int32_col", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "float32_col", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "date_col", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()

	builder.Field(0).(*array.Int8Builder).Append(42)
	builder.Field(1).(*array.Int32Builder).Append(1000)
	builder.Field(2).(*array.Float32Builder).Append(3.14)
	builder.Field(3).(*array.BooleanBuilder).Append(true)
	builder.Field(4).(*array.Date32Builder).Append(arrow.Date32(19000)) // Some date

	original := builder.NewRecord()
	defer original.Release()

	// Serialize and parse
	ipcData, err := adapter.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	parsed, s, err := adapter.Parse(ipcData, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer parsed.Release()

	// Verify column count and types
	if s.NumColumns() != 5 {
		t.Errorf("Schema has %d columns, want 5", s.NumColumns())
	}

	if parsed.NumRows() != 1 {
		t.Errorf("NumRows() = %d, want 1", parsed.NumRows())
	}

	// Verify values
	if parsed.Column(0).(*array.Int8).Value(0) != 42 {
		t.Error("int8_col value incorrect")
	}
	if parsed.Column(1).(*array.Int32).Value(0) != 1000 {
		t.Error("int32_col value incorrect")
	}
	if parsed.Column(3).(*array.Boolean).Value(0) != true {
		t.Error("bool_col value incorrect")
	}
}

// wantTrailingByteError asserts err is non-nil and mentions the trailing-byte
// guard specifically, so a test failure can't be masked by some unrelated
// error (e.g. a schema-conversion bug) that also happens to be non-nil.
func wantTrailingByteError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "trailing byte") {
		t.Errorf("error = %q, want it to mention trailing bytes", err.Error())
	}
}

// TestArrowIPCAdapterParseSingleStreamNoGuardFalsePositive is a regression
// guard against the trailing-bytes check misfiring on ordinary, valid
// single-stream input — every Arrow write today is exactly one stream (see
// sdk/go/client.go forcing batchThreshold=0 for FORMAT_ARROW_IPC), so a
// false positive here would break every Arrow producer.
func TestArrowIPCAdapterParseSingleStreamNoGuardFalsePositive(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	rec := makeTestRecord(allocator)
	defer rec.Release()

	stream := serializeIPCStream(t, allocator, rec)

	// Parse (in-memory *bytes.Reader path).
	parsed, _, err := adapter.Parse(stream, nil)
	if err != nil {
		t.Fatalf("Parse() on a single valid IPC stream should succeed: %v", err)
	}
	defer parsed.Release()
	if parsed.NumRows() != rec.NumRows() {
		t.Errorf("NumRows() = %d, want %d", parsed.NumRows(), rec.NumRows())
	}

	// ParseReader over a non-seekable reader (the streaming/HTTP-body path).
	parsed2, _, err := adapter.ParseReader(iotest.OneByteReader(bytes.NewReader(stream)), nil)
	if err != nil {
		t.Fatalf("ParseReader() on a single valid IPC stream (non-seekable) should succeed: %v", err)
	}
	defer parsed2.Release()
	if parsed2.NumRows() != rec.NumRows() {
		t.Errorf("NumRows() = %d, want %d", parsed2.NumRows(), rec.NumRows())
	}
}

// TestArrowIPCAdapterParsesMultiBatchSingleStream proves the guard doesn't
// confuse a single stream carrying multiple record batches (legitimate) with
// concatenated streams (rejected below). Both Read this many bytes past the
// first record batch's end, but only one of them is a second EOS-terminated
// stream.
func TestArrowIPCAdapterParsesMultiBatchSingleStream(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	rec1 := makeTestRecord(allocator)
	defer rec1.Release()
	rec2 := makeTestRecord(allocator)
	defer rec2.Release()

	stream := serializeIPCStream(t, allocator, rec1, rec2)

	parsed, _, err := adapter.Parse(stream, nil)
	if err != nil {
		t.Fatalf("Parse() should accept a single stream with multiple record batches: %v", err)
	}
	defer parsed.Release()

	wantRows := rec1.NumRows() + rec2.NumRows()
	if parsed.NumRows() != wantRows {
		t.Errorf("NumRows() = %d, want %d", parsed.NumRows(), wantRows)
	}
}

// TestArrowIPCAdapterRejectsConcatenatedStreams covers the core defect: a
// payload that is a valid IPC stream followed by a second, independent IPC
// stream. Without the guard, parseFromReader silently stops at the first
// stream's EOS and reports only its row count — this must instead be a hard
// error, via both entry points (Parse and the streaming ParseReader).
func TestArrowIPCAdapterRejectsConcatenatedStreams(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	rec := makeTestRecord(allocator)
	defer rec.Release()

	stream := serializeIPCStream(t, allocator, rec)
	payload := append(append([]byte{}, stream...), stream...)

	t.Run("Parse", func(t *testing.T) {
		_, _, err := adapter.Parse(payload, nil)
		wantTrailingByteError(t, err)
	})

	t.Run("ParseReader", func(t *testing.T) {
		_, _, err := adapter.ParseReader(bytes.NewReader(payload), nil)
		wantTrailingByteError(t, err)
	})
}

// TestArrowIPCAdapterRejectsTrailingGarbage covers a valid stream followed by
// bytes that aren't a second IPC stream at all (e.g. a caller bug that
// appends unrelated data after the write).
func TestArrowIPCAdapterRejectsTrailingGarbage(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	rec := makeTestRecord(allocator)
	defer rec.Release()

	stream := serializeIPCStream(t, allocator, rec)
	payload := append(append([]byte{}, stream...), []byte("garbage-not-arrow-ipc")...)

	_, _, err := adapter.Parse(payload, nil)
	wantTrailingByteError(t, err)
}

// TestArrowIPCAdapterRejectsConcatenatedStreamsOnNonSeekableReader is the
// correctness-critical case: parseFromReader is called on two paths, and the
// real gateway path (ParseReader, fed the HTTP request body) is a streaming,
// non-seekable io.Reader — not a *bytes.Reader. If arrow-go's ipc.Reader
// buffered ahead past the stream's EOS marker internally, a post-EOS read on
// the same reader variable would find nothing (already drained) and the
// guard would silently never fire.
//
// iotest.OneByteReader wraps the payload in a type that exposes nothing but
// Read (no io.ByteReader/io.ReaderAt/io.Seeker to shortcut through, and it is
// not a *bytes.Reader) and forces exactly one byte per underlying Read call,
// which defeats any buffering shortcut a naive implementation might rely on.
func TestArrowIPCAdapterRejectsConcatenatedStreamsOnNonSeekableReader(t *testing.T) {
	allocator := memory.NewGoAllocator()
	adapter := NewArrowIPCAdapter(allocator)

	rec := makeTestRecord(allocator)
	defer rec.Release()

	stream := serializeIPCStream(t, allocator, rec)

	tests := []struct {
		name    string
		payload []byte
	}{
		{"stream||stream", append(append([]byte{}, stream...), stream...)},
		{"stream||garbage", append(append([]byte{}, stream...), []byte("garbage-not-arrow-ipc")...)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := iotest.OneByteReader(bytes.NewReader(tt.payload))
			_, _, err := adapter.ParseReader(r, nil)
			wantTrailingByteError(t, err)
		})
	}
}
