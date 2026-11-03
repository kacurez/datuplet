package datagateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// parquetBytes encodes batches as one Parquet file with sch (a stand-in for
// a raw data file).
func parquetBytes(t *testing.T, sch *arrow.Schema, batches ...arrow.RecordBatch) []byte {
	t.Helper()
	data, err := encodeChunk(format.FormatParquet, nil, sch, nil, batches, memory.NewGoAllocator())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// sameNamesAndTypes compares column names and Arrow types in order (the
// Read path 5 guarantee; nullability and field metadata are not part of it).
func sameNamesAndTypes(a, b *arrow.Schema) bool {
	if a.NumFields() != b.NumFields() {
		return false
	}
	for i := range a.Fields() {
		if a.Field(i).Name != b.Field(i).Name || !arrow.TypeEqual(a.Field(i).Type, b.Field(i).Type) {
			return false
		}
	}
	return true
}

// headerGuardWriter records whether WriteHeader was called before any body
// byte, so a raw body can tell whether it is being read before the headers
// went out (that is, buffered).
type headerGuardWriter struct {
	*httptest.ResponseRecorder
	headerSent atomic.Bool
}

func (w *headerGuardWriter) WriteHeader(code int) {
	w.headerSent.Store(true)
	w.ResponseRecorder.WriteHeader(code)
}

// guardedBody fails a Read made before the response headers were written.
type guardedBody struct {
	r      io.Reader
	w      *headerGuardWriter
	closed atomic.Bool
}

func (b *guardedBody) Read(p []byte) (int, error) {
	if !b.w.headerSent.Load() {
		return 0, errors.New("raw body read before the response headers were written (buffered)")
	}
	return b.r.Read(p)
}

func (b *guardedBody) Close() error { b.closed.Store(true); return nil }

// A raw part is one chunk streamed straight from the plane: the body is the
// file's bytes, Content-Length its size, X-Datuplet-Rows the manifest record
// count, and no byte is read before the headers are written.
func TestReadRawPartStreamsFileBytes(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 1)
	defer a.Release()
	raw := bytes.Repeat([]byte("0123456789abcdef"), 64<<10) // 1 MiB, several io.Copy buffers
	w := &headerGuardWriter{ResponseRecorder: httptest.NewRecorder()}
	body := &guardedBody{r: bytes.NewReader(raw), w: w}
	p.setTableParts("raw", "files", sch, rawFakePartFunc(int64(len(raw)), 4242, func() io.ReadCloser { return body }))

	resp := openTestReader(t, s, "files", pb.DataFormat_FORMAT_PARQUET)
	if specs := p.readerSpecs; len(specs) != 1 || !specs[0].RawParquet {
		t.Errorf("ReaderSpecs = %+v, want RawParquet for FORMAT_PARQUET", specs)
	}
	s.handleRead(w, httptest.NewRequest(http.MethodGet, "/data/read/"+resp.ReaderId, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.Bytes())
	}
	if !bytes.Equal(w.Body.Bytes(), raw) {
		t.Errorf("body: %d bytes, want the raw file's %d bytes", w.Body.Len(), len(raw))
	}
	h := w.Header()
	if h.Get("Content-Length") != strconv.Itoa(len(raw)) || h.Get("X-Datuplet-Rows") != "4242" ||
		h.Get("X-Datuplet-Is-Last") != "true" || h.Get("Content-Type") != "application/vnd.apache.parquet" {
		t.Errorf("headers = %v", h)
	}
	if !body.closed.Load() {
		t.Error("raw body not closed after the chunk")
	}
	if c := httpReadChunk(t, s, resp.ReaderId); c.status != http.StatusOK || !c.last || c.rows != 0 || len(c.data) != 0 {
		t.Errorf("GET after the last chunk: HTTP %d last %v rows %d", c.status, c.last, c.rows)
	}

	// A body shorter than its advertised size cuts the response short and
	// fails the reader for good.
	short := []byte("PAR1-short")
	p.setTableParts("raw", "short", sch, rawFakePartFunc(100, 1, func() io.ReadCloser { return io.NopCloser(bytes.NewReader(short)) }))
	resp = openTestReader(t, s, "short", pb.DataFormat_FORMAT_PARQUET)
	if c := httpReadChunk(t, s, resp.ReaderId); c.status != http.StatusOK || len(c.data) != len(short) {
		t.Errorf("short body: HTTP %d, %d bytes", c.status, len(c.data))
	}
	if c := httpReadChunk(t, s, resp.ReaderId); c.status != http.StatusInternalServerError {
		t.Errorf("GET after a short body: HTTP %d, want 500", c.status)
	}
}

// Raw, decoded and raw parts: each raw part is one chunk, the decoded part
// is cut by chunk_size without spanning into the next part, a trailing
// empty decoded part is skipped, and only the final chunk is last. Every
// chunk is a Parquet file with the table schema.
func TestReadMixedPartsChunking(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{ChunkSize: 1})
	mem := memory.NewGoAllocator()
	sch, a := idNameBatch(t, mem, 1, 2)
	_, b := idNameBatch(t, mem, 3, 3)
	_, r1 := idNameBatch(t, mem, 10, 4)
	_, r2 := idNameBatch(t, mem, 20, 5)
	for _, rec := range []arrow.RecordBatch{a, b, r1, r2} {
		defer rec.Release()
	}
	raw1, raw2 := parquetBytes(t, sch, r1), parquetBytes(t, sch, r2)
	p.setTableParts("raw", "mixed", sch,
		rawFakePart(raw1, 4), batchFakePart(a, b), rawFakePart(raw2, 5), batchFakePart())

	resp := openTestReader(t, s, "mixed", pb.DataFormat_FORMAT_PARQUET)
	chunks := drainHTTP(t, s, resp.ReaderId)
	wantRows := []int64{4, 2, 3, 5}
	if len(chunks) != len(wantRows) {
		t.Fatalf("chunks = %d, want %d", len(chunks), len(wantRows))
	}
	if !bytes.Equal(chunks[0].data, raw1) || !bytes.Equal(chunks[3].data, raw2) {
		t.Error("raw chunks are not the raw files' bytes")
	}
	for i, c := range chunks {
		if c.rows != wantRows[i] || c.last != (i == len(chunks)-1) {
			t.Errorf("chunk %d: rows %d last %v; want rows %d last %v", i, c.rows, c.last, wantRows[i], i == len(chunks)-1)
		}
		tbl := readParquetChunk(t, c.data)
		if !sameNamesAndTypes(tbl.Schema(), sch) || tbl.NumRows() != c.rows {
			t.Errorf("chunk %d: schema %s rows %d; want the table schema and %d rows", i, tbl.Schema(), tbl.NumRows(), c.rows)
		}
		tbl.Release()
	}

	// The same parts over gRPC.
	resp = openTestReader(t, s, "mixed", pb.DataFormat_FORMAT_PARQUET)
	stream := &mockReadChunkStream{ctx: context.Background()}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.chunks) != len(wantRows) {
		t.Fatalf("gRPC chunks = %d, want %d", len(stream.chunks), len(wantRows))
	}
	for i, c := range stream.chunks {
		if c.RowsInChunk != wantRows[i] || c.IsLast != (i == len(wantRows)-1) {
			t.Errorf("gRPC chunk %d: rows %d last %v", i, c.RowsInChunk, c.IsLast)
		}
	}
}

// gRPC ReadChunk carries a raw part whole in one message; a raw body that
// does not match its size fails the read.
func TestReadGRPCRawPart(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{})
	mem := memory.NewGoAllocator()
	sch, a := idNameBatch(t, mem, 1, 3)
	defer a.Release()
	raw := parquetBytes(t, sch, a)
	p.setTableParts("raw", "files", sch, rawFakePart(raw, 3), batchFakePart(a))

	resp := openTestReader(t, s, "files", pb.DataFormat_FORMAT_PARQUET)
	stream := &mockReadChunkStream{ctx: context.Background()}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(stream.chunks))
	}
	c0 := stream.chunks[0]
	if !bytes.Equal(c0.Data, raw) || c0.RowsInChunk != 3 || c0.IsLast || c0.Format != pb.DataFormat_FORMAT_PARQUET {
		t.Errorf("raw chunk: %d bytes rows %d last %v format %v", len(c0.Data), c0.RowsInChunk, c0.IsLast, c0.Format)
	}
	if c1 := stream.chunks[1]; c1.RowsInChunk != 3 || !c1.IsLast {
		t.Errorf("decoded chunk: rows %d last %v", c1.RowsInChunk, c1.IsLast)
	}
	for _, r := range p.readers() {
		for _, body := range r.rawOpen {
			if !body.isClosed() {
				t.Error("raw body left open after ReadChunk")
			}
		}
	}

	p.setTableParts("raw", "bad", sch, rawFakePartFunc(1000, 1, func() io.ReadCloser { return io.NopCloser(bytes.NewReader(raw)) }))
	resp = openTestReader(t, s, "bad", pb.DataFormat_FORMAT_PARQUET)
	stream = &mockReadChunkStream{ctx: context.Background()}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream); err == nil || len(stream.chunks) != 0 {
		t.Errorf("raw body shorter than its size: err %v, %d chunks; want an error and no chunk", err, len(stream.chunks))
	}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream); err == nil {
		t.Error("ReadChunk after a failed raw chunk: want the sticky error")
	}
}

// FORMAT_ARROW_IPC: a chunk built from three plane batches decodes to
// exactly one batch holding all their rows (spec Read path 8), over HTTP and
// gRPC.
func TestReadArrowIPCOneBatchPerChunk(t *testing.T) {
	s, _, _ := newReadTestServer(t, 0, memory.NewGoAllocator()) // three batches, one chunk
	resp := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_ARROW_IPC)
	wire, err := sdkWireSchema(resp.Schema)
	if err != nil {
		t.Fatal(err)
	}
	chunks := drainHTTP(t, s, resp.ReaderId)
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d, want 1", len(chunks))
	}
	got, batches, rows := ipcBatches(t, chunks[0].data)
	if batches != 1 || rows != 6 || !got.Equal(wire) {
		t.Errorf("IPC chunk: %d batches, %d rows, schema %s; want 1 batch, 6 rows, %s", batches, rows, got, wire)
	}

	resp = openTestReader(t, s, "orders", pb.DataFormat_FORMAT_ARROW_IPC)
	stream := &mockReadChunkStream{ctx: context.Background()}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.chunks) != 1 {
		t.Fatalf("gRPC chunks = %d, want 1", len(stream.chunks))
	}
	if _, batches, rows := ipcBatches(t, stream.chunks[0].Data); batches != 1 || rows != 6 {
		t.Errorf("gRPC IPC chunk: %d batches, %d rows; want 1 and 6", batches, rows)
	}
}

// sdkWireSchema must stay identical to the frozen SDK's protoSchemaToArrow
// (sdk/go/arrow/reader.go:132-165, the switch over c.Type at lines 140-161).
// The expected Arrow types below are copied from that switch.
func TestSDKWireSchemaMatchesSDK(t *testing.T) {
	cases := []struct {
		typ  string
		want arrow.DataType
	}{
		{"int32", arrow.PrimitiveTypes.Int32},
		{"int64", arrow.PrimitiveTypes.Int64},
		{"float32", arrow.PrimitiveTypes.Float32},
		{"float64", arrow.PrimitiveTypes.Float64},
		{"string", arrow.BinaryTypes.String},
		{"boolean", arrow.FixedWidthTypes.Boolean},
		{"bool", arrow.FixedWidthTypes.Boolean},
		{"binary", arrow.BinaryTypes.Binary},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_us},
		{"date", arrow.FixedWidthTypes.Date32},
	}
	for _, nullable := range []bool{true, false} {
		ps := &pb.Schema{}
		for _, c := range cases {
			ps.Columns = append(ps.Columns, &pb.ColumnDef{Name: "c_" + c.typ, Type: c.typ, Nullable: nullable})
		}
		got, err := sdkWireSchema(ps)
		if err != nil {
			t.Fatal(err)
		}
		if got.NumFields() != len(cases) || got.HasMetadata() {
			t.Fatalf("wire schema %s: want %d fields and no metadata", got, len(cases))
		}
		for i, c := range cases {
			f := got.Field(i)
			if f.Name != "c_"+c.typ || !arrow.TypeEqual(f.Type, c.want) || f.Nullable != nullable || f.HasMetadata() {
				t.Errorf("%q (nullable=%v) -> %s, want %s nullable=%v", c.typ, nullable, f, c.want, nullable)
			}
		}
	}
	if ts := arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType); ts.Unit != arrow.Microsecond || ts.TimeZone != "UTC" {
		t.Errorf("Timestamp_us = %s, want timestamp[us, tz=UTC]", ts)
	}

	_, err := sdkWireSchema(&pb.Schema{Columns: []*pb.ColumnDef{{Name: "c", Type: "decimal"}}})
	if err == nil || err.Error() != `unsupported type "decimal" for column "c"` {
		t.Errorf("unknown type: err = %v, want the SDK's error", err)
	}
	if _, err := sdkWireSchema(nil); err == nil || err.Error() != "nil proto schema" {
		t.Errorf("nil schema: err = %v", err)
	}

	// Every type string arrowSchemaToProto can emit is one the SDK accepts.
	for _, at := range []arrow.DataType{
		arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint64, arrow.BinaryTypes.LargeString,
		arrow.FixedWidthTypes.Date64, arrow.ListOf(arrow.PrimitiveTypes.Int32), &arrow.Decimal128Type{Precision: 10, Scale: 2}, extensions.NewDefaultVariantType(),
	} {
		ps := arrowSchemaToProto(arrow.NewSchema([]arrow.Field{{Name: "c", Type: at}}, nil))
		if _, err := sdkWireSchema(ps); err != nil {
			t.Errorf("%s -> proto %q: SDK would reject it: %v", at, ps.Columns[0].Type, err)
		}
	}
}

// timestampBatch builds one row per value for columns naive (timestamp[us]),
// utc (timestamp[us, UTC]) and ns (timestamp[ns, UTC]).
func timestampBatch(t *testing.T, mem memory.Allocator, sch *arrow.Schema, us []int64, ns []int64) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(mem, sch)
	defer b.Release()
	for i := range us {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(us[i]))
		b.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(us[i]))
		b.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(ns[i]))
	}
	return b.NewRecordBatch()
}

// Review Focus 2: IPC chunks of a table with zone-less, UTC and nanosecond
// timestamps carry exactly the SDK wire schema of OpenReaderResponse.schema,
// with the same instants (ns truncated to µs).
func TestReadArrowIPCTimestampsMatchSDKWire(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{ChunkSize: 1})
	mem := memory.NewGoAllocator()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "naive", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "utc", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: false},
	}, nil)
	us := [][]int64{{1_700_000_000_123_456, 0}, {-86_400_000_001}}
	ns := [][]int64{{1_700_000_000_123_456_789, 999}, {-86_400_000_001_000}}
	a := timestampBatch(t, mem, sch, us[0], ns[0])
	b := timestampBatch(t, mem, sch, us[1], ns[1])
	defer a.Release()
	defer b.Release()
	p.setTable("raw", "ts", sch, a, b)

	resp := openTestReader(t, s, "ts", pb.DataFormat_FORMAT_ARROW_IPC)
	wire, err := sdkWireSchema(resp.Schema)
	if err != nil {
		t.Fatal(err)
	}
	chunks := drainHTTP(t, s, resp.ReaderId)
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(chunks))
	}
	for i, c := range chunks {
		r, err := ipc.NewReader(bytes.NewReader(c.data))
		if err != nil {
			t.Fatal(err)
		}
		if !r.Schema().Equal(wire) {
			t.Errorf("chunk %d schema %s, want the SDK wire schema %s", i, r.Schema(), wire)
		}
		if !r.Next() {
			t.Fatalf("chunk %d: no batch", i)
		}
		rec := r.RecordBatch()
		for row := 0; row < int(rec.NumRows()); row++ {
			naive := rec.Column(0).(*array.Timestamp).Value(row)
			utc := rec.Column(1).(*array.Timestamp).Value(row)
			nsv := rec.Column(2).(*array.Timestamp).Value(row)
			wantNs := ns[i][row] / 1000 // truncation toward zero, as the cast does
			if int64(naive) != us[i][row] || int64(utc) != us[i][row] || int64(nsv) != wantNs {
				t.Errorf("chunk %d row %d = %d/%d/%d, want %d/%d/%d", i, row, naive, utc, nsv, us[i][row], us[i][row], wantNs)
			}
		}
		if r.Next() {
			t.Errorf("chunk %d holds more than one batch", i)
		}
		r.Release()
	}
}

// Review Focus 2: a column the SDK cannot represent and that has no cast to
// its wire type fails OpenReader with InvalidArgument naming it; no reader
// is published and the plane reader is closed. The same table still reads
// as Parquet.
func TestReadArrowIPCUncastableTypeFailsOpen(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{})
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)
	p.setTable("raw", "nested", sch)

	_, err := s.OpenReader(context.Background(), &pb.OpenReaderRequest{Bucket: "raw", Table: "nested", OutputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), `"tags"`) || !strings.Contains(err.Error(), "list") {
		t.Fatalf("OpenReader = %v, want InvalidArgument naming column tags and its type", err)
	}
	s.mu.RLock()
	n := len(s.readers)
	s.mu.RUnlock()
	if n != 0 {
		t.Errorf("readers after a refused open = %d, want 0", n)
	}
	if rs := p.readers(); len(rs) != 1 || rs[0].closeCount() != 1 {
		t.Errorf("plane reader not closed after the refused open")
	}

	resp := openTestReader(t, s, "nested", pb.DataFormat_FORMAT_PARQUET)
	if c := httpReadChunk(t, s, resp.ReaderId); c.status != http.StatusOK || !c.last {
		t.Errorf("Parquet read of the same table: HTTP %d last %v", c.status, c.last)
	}
}
