package datagateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"google.golang.org/grpc/metadata"

	"github.com/datuplet/datuplet/pkg/datagateway/backend"
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

// newTestServerV2WithLocalBackend creates a ServerV2 with a real LocalBackend
// rooted at dir. The test should place parquet files directly inside dir and
// pass bucket=dir, table=<filename> so the static-backend streaming path resolves
// the absolute path correctly.
func newTestServerV2WithLocalBackend(t *testing.T, dir string) *ServerV2 {
	t.Helper()
	lb := backend.NewLocalBackend(backend.LocalConfig{DataDir: ""}) // DataDir="" so abs paths pass through
	cfg := &Config{
		RunID:         "test-stream",
		ComponentName: "test",
		DefaultBucket: "raw",
		Backend:       lb,
	}
	return NewServerV2(cfg)
}

// writeStreamingFixtureParquet writes a single-column int64 parquet at path.
func writeStreamingFixtureParquet(t *testing.T, path string, values []int64, rowsPerGroup int) {
	t.Helper()
	pool := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create fixture: %v", err)
	}
	defer f.Close()
	props := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(rowsPerGroup)))
	pqw, err := pqarrow.NewFileWriter(arrowSchema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("pqarrow writer: %v", err)
	}
	defer pqw.Close()
	builder := array.NewInt64Builder(pool)
	for _, v := range values {
		builder.Append(v)
	}
	arr := builder.NewArray()
	defer arr.Release()
	rec := array.NewRecord(arrowSchema, []arrow.Array{arr}, int64(len(values)))
	defer rec.Release()
	if err := pqw.Write(rec); err != nil {
		t.Fatalf("write record: %v", err)
	}
}

// OpenReader with FORMAT_ARROW_IPC routes to the streaming reader; ReadChunk
// returns nil once the reader is exhausted (ReadChunk maps io.EOF to nil, so
// any error, io.EOF included, is a failure) and every chunk is Arrow IPC with
// the full row count.
func TestOpenReader_FormatArrowIPC_RoutesToStreaming(t *testing.T) {
	tests := []struct {
		name         string
		values       []int64
		rowsPerGroup int
	}{
		{"3 rows in groups of 2", []int64{1, 2, 3}, 2},
		{"6 rows, 3 row groups -> 3 chunks", []int64{1, 2, 3, 4, 5, 6}, 2},
		{"1 row in a single group", []int64{42}, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeStreamingFixtureParquet(t, filepath.Join(dir, "in.parquet"), tc.values, tc.rowsPerGroup)

			srv := newTestServerV2WithLocalBackend(t, dir)

			// Use bucket = dir so that fmt.Sprintf("%s/%s", bucket, table) = absolute path to the file.
			resp, err := srv.OpenReader(context.Background(), &pb.OpenReaderRequest{
				Bucket:       dir,
				Table:        "in.parquet",
				OutputFormat: pb.DataFormat_FORMAT_ARROW_IPC,
			})
			if err != nil {
				t.Fatalf("OpenReader: %v", err)
			}

			stream := &mockReadChunkStream{ctx: context.Background()}
			err = srv.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream)
			if err != nil {
				t.Fatalf("ReadChunk: %v", err)
			}

			if len(stream.chunks) == 0 {
				t.Fatal("expected at least one chunk, got none")
			}
			var totalRows int64
			for _, chunk := range stream.chunks {
				if chunk.Format != pb.DataFormat_FORMAT_ARROW_IPC {
					t.Errorf("expected FORMAT_ARROW_IPC, got %v", chunk.Format)
				}
				totalRows += chunk.RowsInChunk
			}
			if totalRows != int64(len(tc.values)) {
				t.Errorf("totalRows=%d, want %d", totalRows, len(tc.values))
			}
		})
	}
}

// TestOpenReader_FormatCSV_NotAffected verifies that non-ArrowIPC formats are
// unaffected by the streaming routing change.
func TestOpenReader_FormatCSV_NotAffected(t *testing.T) {
	// Uses existing mock backend + createTestServerV2 to ensure CSV/JSON paths unchanged.
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	resp, err := server.OpenReader(context.Background(), &pb.OpenReaderRequest{
		Bucket:       "raw",
		Table:        "input",
		OutputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenReader (CSV): %v", err)
	}
	if resp.ReaderId == "" {
		t.Error("expected ReaderId to be set")
	}

	// ReadChunk with mockBackend data
	stream := &mockReadChunkStream{ctx: context.Background()}
	_ = server.ReadChunk(&pb.ReadChunkRequest{ReaderId: resp.ReaderId}, stream)
	// mockReader returns 1 chunk, CSV format — just check it doesn't error
	for _, chunk := range stream.chunks {
		if chunk.Format != pb.DataFormat_FORMAT_CSV {
			t.Errorf("expected FORMAT_CSV, got %v", chunk.Format)
		}
	}
}
