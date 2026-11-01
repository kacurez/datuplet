package datagateway

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// mockReadChunkStream implements pb.DataGateway_ReadChunkServer for tests.
type mockReadChunkStream struct {
	chunks []*pb.DataChunk
	ctx    context.Context
}

func (m *mockReadChunkStream) Send(chunk *pb.DataChunk) error {
	m.chunks = append(m.chunks, chunk)
	return nil
}

func (m *mockReadChunkStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockReadChunkStream) SendHeader(metadata.MD) error { return nil }
func (m *mockReadChunkStream) SetTrailer(metadata.MD)       {}
func (m *mockReadChunkStream) Context() context.Context     { return m.ctx }
func (m *mockReadChunkStream) SendMsg(v interface{}) error  { return nil }
func (m *mockReadChunkStream) RecvMsg(v interface{}) error  { return nil }

// readChunkResult is one HTTP read response.
type readChunkResult struct {
	status int
	data   []byte
	rows   int64
	last   bool
	ctype  string
}

func httpReadChunk(t *testing.T, s *ServerV2, readerID string) readChunkResult {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleRead(rec, httptest.NewRequest(http.MethodGet, "/data/read/"+readerID, nil))
	res := readChunkResult{status: rec.Code, data: rec.Body.Bytes(), ctype: rec.Header().Get("Content-Type")}
	res.last = rec.Header().Get("X-Datuplet-Is-Last") == "true"
	if v := rec.Header().Get("X-Datuplet-Rows"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("X-Datuplet-Rows %q: %v", v, err)
		}
		res.rows = n
	}
	return res
}

// drainHTTP GETs chunks until one is last; every response must be 200.
func drainHTTP(t *testing.T, s *ServerV2, readerID string) []readChunkResult {
	t.Helper()
	var out []readChunkResult
	for i := 0; i < 100; i++ {
		res := httpReadChunk(t, s, readerID)
		if res.status != http.StatusOK {
			t.Fatalf("GET chunk %d: HTTP %d: %s", i, res.status, res.data)
		}
		out = append(out, res)
		if res.last {
			return out
		}
	}
	t.Fatal("no last chunk after 100 GETs")
	return nil
}

// newReadTestServer builds a server on a fake plane holding raw.orders with
// three batches: ids 1-2, 3-5 and 6 (names null for even ids).
func newReadTestServer(t *testing.T, chunkSize int64, mem memory.Allocator) (*ServerV2, *fakeDataPlane, *arrow.Schema) {
	t.Helper()
	s, p := newWriteTestServer(t, &Config{ChunkSize: chunkSize})
	sch, a := idNameBatch(t, mem, 1, 2)
	_, b := idNameBatch(t, mem, 3, 3)
	_, c := idNameBatch(t, mem, 6, 1)
	t.Cleanup(func() { a.Release(); b.Release(); c.Release() })
	p.setTable("raw", "orders", sch, a, b, c)
	return s, p, sch
}

func openTestReader(t *testing.T, s *ServerV2, table string, f pb.DataFormat) *pb.OpenReaderResponse {
	t.Helper()
	resp, err := s.OpenReader(context.Background(), &pb.OpenReaderRequest{Bucket: "raw", Table: table, OutputFormat: f})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	return resp
}

// Every FORMAT_PARQUET chunk is a self-contained Parquet file; with a 1-byte
// chunk size each batch is its own chunk, and only the last is marked last.
func TestReadParquetChunksAreSelfContained(t *testing.T) {
	s, _, _ := newReadTestServer(t, 1, memory.NewGoAllocator())
	resp := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_PARQUET)
	if resp.HttpEndpoint == "" || !strings.HasSuffix(resp.HttpEndpoint, "/data/read/"+resp.ReaderId) {
		t.Errorf("HttpEndpoint = %q", resp.HttpEndpoint)
	}
	if resp.Schema == nil || len(resp.Schema.Columns) != 2 || resp.Schema.Columns[0].Name != "id" ||
		resp.Schema.Columns[0].Type != "int64" || resp.Schema.Columns[1].Type != "string" {
		t.Errorf("OpenReader schema = %v, want id int64, name string", resp.Schema)
	}
	for _, c := range resp.Schema.GetColumns() {
		if c.FieldId != 0 {
			t.Errorf("column %q FieldId = %d, want 0 (unknown: not catalog-sourced)", c.Name, c.FieldId)
		}
	}

	chunks := drainHTTP(t, s, resp.ReaderId)
	if len(chunks) != 3 {
		t.Fatalf("chunks = %d, want 3", len(chunks))
	}
	var ids []int64
	for i, c := range chunks {
		if c.last != (i == 2) {
			t.Errorf("chunk %d: is_last = %v", i, c.last)
		}
		if c.ctype != "application/vnd.apache.parquet" {
			t.Errorf("chunk %d: Content-Type = %q", i, c.ctype)
		}
		tbl := readParquetChunk(t, c.data)
		if tbl.NumRows() != c.rows {
			t.Errorf("chunk %d: file rows %d, header rows %d", i, tbl.NumRows(), c.rows)
		}
		ids = append(ids, int64Column(t, tbl, "id")...)
		tbl.Release()
	}
	if len(ids) != 6 || ids[0] != 1 || ids[5] != 6 {
		t.Errorf("ids = %v, want 1..6", ids)
	}
}

// FORMAT_ARROW_IPC: one IPC stream per chunk holding every batch of the
// chunk, over HTTP and over gRPC ReadChunk.
func TestReadArrowIPCChunks(t *testing.T) {
	s, _, sch := newReadTestServer(t, 0, memory.NewGoAllocator()) // default 32 MiB: one chunk
	resp := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_ARROW_IPC)
	chunks := drainHTTP(t, s, resp.ReaderId)
	if len(chunks) != 1 || chunks[0].rows != 6 {
		t.Fatalf("chunks = %d (rows %d), want one chunk of 6 rows", len(chunks), chunks[0].rows)
	}
	r, err := ipc.NewReader(bytes.NewReader(chunks[0].data))
	if err != nil {
		t.Fatalf("not an IPC stream: %v", err)
	}
	var batches, rows int64
	for r.Next() {
		batches++
		rows += r.RecordBatch().NumRows()
	}
	if !r.Schema().Equal(sch) || batches != 3 || rows != 6 {
		t.Errorf("IPC: schema %s, %d batches, %d rows; want the table schema, 3 batches, 6 rows", r.Schema(), batches, rows)
	}
	r.Release()

	s2, _, _ := newReadTestServer(t, 1, memory.NewGoAllocator())
	resp2 := openTestReader(t, s2, "orders", pb.DataFormat_FORMAT_ARROW_IPC)
	stream := &mockReadChunkStream{ctx: context.Background()}
	if err := s2.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp2.ReaderId}, stream); err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if len(stream.chunks) != 3 {
		t.Fatalf("gRPC chunks = %d, want 3", len(stream.chunks))
	}
	var total int64
	for i, c := range stream.chunks {
		if c.Format != pb.DataFormat_FORMAT_ARROW_IPC || c.IsLast != (i == 2) {
			t.Errorf("gRPC chunk %d: format %v is_last %v", i, c.Format, c.IsLast)
		}
		total += c.RowsInChunk
	}
	if total != 6 {
		t.Errorf("gRPC rows = %d, want 6", total)
	}
}

func TestReadCSVAndJSONLChunks(t *testing.T) {
	s, _, _ := newReadTestServer(t, 0, memory.NewGoAllocator())
	csvResp := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_CSV)
	chunks := drainHTTP(t, s, csvResp.ReaderId)
	if len(chunks) != 1 || chunks[0].ctype != "text/csv" || chunks[0].rows != 6 {
		t.Fatalf("CSV: %d chunks, Content-Type %q, rows %d", len(chunks), chunks[0].ctype, chunks[0].rows)
	}
	rows, err := csv.NewReader(bytes.NewReader(chunks[0].data)).ReadAll()
	if err != nil || len(rows) != 7 || rows[0][0] != "id" || rows[1][0] != "1" || rows[1][1] != "n1" || rows[6][0] != "6" {
		t.Errorf("CSV = %q (%v), want a header and ids 1..6", rows, err)
	}

	jsonlResp := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_JSONL)
	chunks = drainHTTP(t, s, jsonlResp.ReaderId)
	lines := strings.Split(strings.TrimSpace(string(chunks[0].data)), "\n")
	if len(chunks) != 1 || chunks[0].ctype != "application/x-ndjson" || len(lines) != 6 ||
		!strings.Contains(lines[0], `"id":1`) || !strings.Contains(lines[5], `"id":6`) {
		t.Errorf("JSONL: %d chunks, Content-Type %q, lines %q", len(chunks), chunks[0].ctype, lines)
	}

	// An unspecified output format reads as CSV, as before.
	def := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_UNSPECIFIED)
	if c := httpReadChunk(t, s, def.ReaderId); c.ctype != "text/csv" || c.rows != 6 {
		t.Errorf("unspecified format: Content-Type %q rows %d, want CSV", c.ctype, c.rows)
	}
}

// Review Focus 5: an empty table reads as one last chunk with zero rows; for
// FORMAT_PARQUET it is a valid zero-row file with the table schema.
func TestReadEmptyTableParquetZeroRowFile(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{})
	sch, a := idNameBatch(t, memory.NewGoAllocator(), 1, 1)
	a.Release()
	p.setTable("raw", "empty", sch)

	resp := openTestReader(t, s, "empty", pb.DataFormat_FORMAT_PARQUET)
	c := httpReadChunk(t, s, resp.ReaderId)
	if c.status != http.StatusOK || !c.last || c.rows != 0 || len(c.data) == 0 {
		t.Fatalf("empty table chunk: HTTP %d last=%v rows=%d bytes=%d", c.status, c.last, c.rows, len(c.data))
	}
	tbl := readParquetChunk(t, c.data)
	defer tbl.Release()
	if tbl.NumRows() != 0 || tbl.Schema().NumFields() != 2 || tbl.Schema().Field(0).Name != "id" || tbl.Schema().Field(1).Name != "name" {
		t.Errorf("zero-row file: rows %d schema %s", tbl.NumRows(), tbl.Schema())
	}

	grpcResp := openTestReader(t, s, "empty", pb.DataFormat_FORMAT_PARQUET)
	stream := &mockReadChunkStream{ctx: context.Background()}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: grpcResp.ReaderId}, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.chunks) != 1 || !stream.chunks[0].IsLast || stream.chunks[0].RowsInChunk != 0 || len(stream.chunks[0].Data) == 0 {
		t.Errorf("gRPC empty table: %d chunks", len(stream.chunks))
	}
}

// X-Datuplet-Rows counts each chunk's rows and X-Datuplet-Is-Last marks the
// final one; a GET after the last chunk is an empty last chunk.
func TestReadHeadersIsLastAndRows(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{ChunkSize: 1})
	mem := memory.NewGoAllocator()
	sch, a := idNameBatch(t, mem, 1, 2)
	_, b := idNameBatch(t, mem, 3, 3)
	defer a.Release()
	defer b.Release()
	p.setTable("raw", "orders", sch, a, b)

	resp := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_JSONL)
	want := []struct {
		rows int64
		last bool
	}{{2, false}, {3, true}, {0, true}}
	for i, w := range want {
		c := httpReadChunk(t, s, resp.ReaderId)
		if c.status != http.StatusOK || c.rows != w.rows || c.last != w.last {
			t.Errorf("GET %d: HTTP %d rows=%d last=%v; want 200 rows=%d last=%v", i, c.status, c.rows, c.last, w.rows, w.last)
		}
		if i == 2 && len(c.data) != 0 {
			t.Errorf("GET after the last chunk: body %q, want empty", c.data)
		}
	}
	if c := httpReadChunk(t, s, "r-404"); c.status != http.StatusNotFound {
		t.Errorf("unknown reader: HTTP %d, want 404", c.status)
	}
}

// Plane failures surface: OpenTableReader's error from OpenReader, a Next
// failure as HTTP 500 and as the gRPC ReadChunk error.
func TestReadPlaneErrorSurfaces(t *testing.T) {
	boom := errors.New("boom")

	s, p, _ := newReadTestServer(t, 1, memory.NewGoAllocator())
	p.failOpenReader(boom)
	if _, err := s.OpenReader(context.Background(), &pb.OpenReaderRequest{Bucket: "raw", Table: "orders"}); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("OpenReader = %v, want the plane error", err)
	}
	p.failOpenReader(nil)

	p.failNext(boom, 1)
	resp := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_PARQUET)
	if c := httpReadChunk(t, s, resp.ReaderId); c.status != http.StatusOK || c.rows != 2 || c.last {
		t.Errorf("first chunk: HTTP %d rows %d last %v; want the batch read before the failure", c.status, c.rows, c.last)
	}
	c := httpReadChunk(t, s, resp.ReaderId)
	if c.status != http.StatusInternalServerError || !strings.Contains(string(c.data), "boom") || !strings.Contains(string(c.data), "READ_ERROR") {
		t.Errorf("failed chunk: HTTP %d %s; want 500 READ_ERROR naming the plane error", c.status, c.data)
	}

	// A failure is sticky: a transient plane error in the middle of a chunk
	// fails that GET and every later one, never a 200 that skips the rows of
	// the failed chunk.
	s2, p2, _ := newReadTestServer(t, 0, memory.NewGoAllocator()) // default chunk size: the failure lands mid-fill
	p2.failNextOnce(boom, 1)
	resp = openTestReader(t, s2, "orders", pb.DataFormat_FORMAT_JSONL)
	for i := 0; i < 2; i++ {
		c := httpReadChunk(t, s2, resp.ReaderId)
		if c.status != http.StatusInternalServerError || !strings.Contains(string(c.data), "boom") {
			t.Errorf("GET %d after a mid-chunk failure: HTTP %d last=%v rows=%d %s; want 500 naming the plane error",
				i, c.status, c.last, c.rows, c.data)
		}
	}
	stream2 := &mockReadChunkStream{ctx: context.Background()}
	if err := s2.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream2); err == nil || !strings.Contains(err.Error(), "boom") || len(stream2.chunks) != 0 {
		t.Errorf("ReadChunk after a mid-chunk failure = %v (%d chunks), want the plane error and no chunk", err, len(stream2.chunks))
	}

	p.failNext(boom, 0)
	resp = openTestReader(t, s, "orders", pb.DataFormat_FORMAT_ARROW_IPC)
	stream := &mockReadChunkStream{ctx: context.Background()}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("ReadChunk = %v, want the plane error", err)
	}
	if err := s.ReadChunk(&pb.ReadChunkRequest{ReaderId: "r-404"}, stream); err == nil {
		t.Error("ReadChunk of an unknown reader: want error")
	}
}

func TestGetSchemaUnimplemented(t *testing.T) {
	s, _ := newWriteTestServer(t, &Config{Inputs: map[string]string{"input": "raw/orders"}})
	_, err := s.GetSchema(context.Background(), &pb.GetSchemaRequest{InputName: "input"})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("GetSchema = %v, want codes.Unimplemented", err)
	}
	_, err = s.GetSample(context.Background(), &pb.GetSampleRequest{InputName: "input", Limit: 5})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("GetSample = %v, want codes.Unimplemented", err)
	}
}

// The reader outlives the OpenReader RPC: once that call's ctx is cancelled,
// later GETs still drain every chunk (Next never runs on the RPC ctx).
func TestReadSurvivesOpenReaderCtxCancel(t *testing.T) {
	s, _, _ := newReadTestServer(t, 1, memory.NewGoAllocator())
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := s.OpenReader(ctx, &pb.OpenReaderRequest{Bucket: "raw", Table: "orders", OutputFormat: pb.DataFormat_FORMAT_PARQUET})
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	var rows int64
	for _, c := range drainHTTP(t, s, resp.ReaderId) {
		rows += c.rows
	}
	if rows != 6 {
		t.Errorf("rows after the OpenReader ctx was cancelled = %d, want 6", rows)
	}
}

// CloseReader closes the plane stream once, forgets the reader and releases
// the batch the server held for the next chunk.
func TestCloseReaderReleasesStream(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	s, p := newWriteTestServer(t, &Config{ChunkSize: 1})
	sch, a := idNameBatch(t, mem, 1, 2)
	_, b := idNameBatch(t, mem, 3, 3)
	_, c := idNameBatch(t, mem, 6, 1)
	p.setTable("raw", "orders", sch, a, b, c)

	resp := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_ARROW_IPC)
	if c := httpReadChunk(t, s, resp.ReaderId); c.status != http.StatusOK || c.last {
		t.Fatalf("first chunk: HTTP %d last %v", c.status, c.last)
	}
	for i := 0; i < 2; i++ { // idempotent
		if _, err := s.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: resp.ReaderId}); err != nil {
			t.Fatalf("CloseReader: %v", err)
		}
	}
	streams := p.readers()
	if len(streams) != 1 || streams[0].closeCount() != 1 {
		t.Fatalf("plane streams %d, closes %d; want one stream closed once", len(streams), streams[0].closeCount())
	}
	s.mu.RLock()
	n := len(s.readers)
	s.mu.RUnlock()
	if n != 0 {
		t.Errorf("readers after CloseReader = %d, want 0", n)
	}
	if c := httpReadChunk(t, s, resp.ReaderId); c.status != http.StatusNotFound {
		t.Errorf("GET after CloseReader: HTTP %d, want 404", c.status)
	}

	a.Release()
	b.Release()
	c.Release()
	mem.AssertSize(t, 0)
}

// The cancel annotation interrupts the plane and closes every open reader; a
// reader whose open raced the cancel is closed too, and no reader opens
// afterwards (the DuckDB plane accepts opens after Interrupt, which the fake
// mimics here).
func TestCancelInterruptsReaders(t *testing.T) {
	annotations := filepath.Join(t.TempDir(), "annotations")
	if err := os.WriteFile(annotations, []byte("other=\"x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, p := newWriteTestServer(t, &Config{ChunkSize: 1, PodAnnotationsPath: annotations, CancelPollInterval: 5 * time.Millisecond})
	p.openAfterInterrupt = true
	mem := memory.NewGoAllocator()
	sch, a := idNameBatch(t, mem, 1, 2)
	_, b := idNameBatch(t, mem, 3, 3)
	defer a.Release()
	defer b.Release()
	p.setTable("raw", "orders", sch, a, b)

	open := openTestReader(t, s, "orders", pb.DataFormat_FORMAT_PARQUET)
	if c := httpReadChunk(t, s, open.ReaderId); c.status != http.StatusOK || c.last {
		t.Fatalf("first chunk: HTTP %d last %v", c.status, c.last)
	}

	entered, release := make(chan struct{}), make(chan struct{})
	p.beforeOpenReader = func() {
		close(entered)
		<-release
	}
	racing := make(chan error, 1)
	go func() {
		_, err := s.OpenReader(context.Background(), &pb.OpenReaderRequest{Bucket: "raw", Table: "orders"})
		racing <- err
	}()
	<-entered

	if err := os.WriteFile(annotations, []byte(CancelAnnotationKey+"=\"true\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.cancelCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("cancel not observed")
	}
	if !p.interrupted() {
		t.Fatal("plane not interrupted by the cancel")
	}
	close(release)
	if err := <-racing; err == nil {
		t.Error("OpenReader that raced the cancel succeeded")
	}
	p.beforeOpenReader = nil

	streams := p.readers()
	if len(streams) != 2 {
		t.Fatalf("plane streams = %d, want 2 (the open reader and the racing one)", len(streams))
	}
	for i, rs := range streams {
		if rs.closeCount() == 0 {
			t.Errorf("plane stream %d still open after the cancel", i)
		}
	}
	if c := httpReadChunk(t, s, open.ReaderId); c.status == http.StatusOK {
		t.Errorf("GET on a reader after the cancel: HTTP 200, want a failure")
	}
	if _, err := s.OpenReader(context.Background(), &pb.OpenReaderRequest{Bucket: "raw", Table: "orders"}); err == nil {
		t.Error("OpenReader after the cancel succeeded")
	}
	if got := len(p.readers()); got != 2 {
		t.Errorf("plane streams after a post-cancel OpenReader = %d, want 2 (none opened)", got)
	}
}
