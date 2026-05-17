package arrow

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// TestArrowReader_FromGRPCStream verifies that NewReader correctly drains
// a gRPC server-streaming ReadChunk channel and surfaces each IPC-encoded
// chunk as a single arrow.RecordBatch.
func TestArrowReader_FromGRPCStream(t *testing.T) {
	pool := memory.NewGoAllocator()
	arrowSch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	chunks := buildFakeIPCChunks(t, pool, arrowSch, [][]int64{
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		{11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		{21, 22, 23, 24, 25, 26, 27, 28, 29, 30},
	})

	lis := bufconn.Listen(1024 * 1024)
	grpcSrv := grpc.NewServer()
	pb.RegisterDataGatewayServer(grpcSrv, &fakeGW{schema: arrowSch, chunks: chunks})
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	sdkClient := sdk.NewWithDialer(t, func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	defer sdkClient.Close()

	sdkReader, err := sdkClient.OpenReader(context.Background(), "b", "t",
		sdk.WithOutputFormat(pb.DataFormat_FORMAT_ARROW_IPC))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}

	rr, err := NewReader(context.Background(), sdkReader)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer rr.Release()

	var totalRows int64
	for rr.Next() {
		totalRows += rr.RecordBatch().NumRows()
	}
	if rr.Err() != nil && !errors.Is(rr.Err(), io.EOF) {
		t.Fatalf("rr.Err: %v", rr.Err())
	}
	if totalRows != 30 {
		t.Errorf("totalRows = %d, want 30", totalRows)
	}
}

type fakeGW struct {
	pb.UnimplementedDataGatewayServer
	schema *arrow.Schema
	chunks [][]byte
}

func (g *fakeGW) GetConfig(ctx context.Context, req *pb.GetConfigRequest) (*pb.ComponentConfig, error) {
	return &pb.ComponentConfig{}, nil
}

func (g *fakeGW) OpenReader(ctx context.Context, req *pb.OpenReaderRequest) (*pb.OpenReaderResponse, error) {
	return &pb.OpenReaderResponse{
		ReaderId: "r1",
		Schema:   arrowSchemaToProto(g.schema),
		// Empty HttpEndpoint forces gRPC-only mode in OpenReader.
	}, nil
}

func (g *fakeGW) ReadChunk(req *pb.ReadChunkRequest, stream pb.DataGateway_ReadChunkServer) error {
	for i, b := range g.chunks {
		if err := stream.Send(&pb.DataChunk{
			Data:        b,
			Format:      pb.DataFormat_FORMAT_ARROW_IPC,
			IsLast:      i == len(g.chunks)-1,
			RowsInChunk: 10,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (g *fakeGW) CloseReader(ctx context.Context, req *pb.CloseReaderRequest) (*pb.CloseReaderResponse, error) {
	return &pb.CloseReaderResponse{}, nil
}

func buildFakeIPCChunks(t *testing.T, pool memory.Allocator, sch *arrow.Schema, rows [][]int64) [][]byte {
	t.Helper()
	out := make([][]byte, len(rows))
	for i, r := range rows {
		b := array.NewInt64Builder(pool)
		for _, v := range r {
			b.Append(v)
		}
		arr := b.NewArray()
		rec := array.NewRecord(sch, []arrow.Array{arr}, int64(len(r)))
		var buf bytes.Buffer
		w := ipc.NewWriter(&buf, ipc.WithSchema(sch))
		if err := w.Write(rec); err != nil {
			t.Fatalf("ipc write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("ipc close: %v", err)
		}
		// Release builder/array/record after writing — IPC writer copied them.
		arr.Release()
		rec.Release()
		out[i] = buf.Bytes()
	}
	return out
}

func arrowSchemaToProto(sch *arrow.Schema) *pb.Schema {
	cols := make([]*pb.ColumnDef, sch.NumFields())
	for i, f := range sch.Fields() {
		cols[i] = &pb.ColumnDef{Name: f.Name, Type: arrowTypeName(f.Type), Nullable: f.Nullable}
	}
	return &pb.Schema{Columns: cols}
}

func arrowTypeName(t arrow.DataType) string {
	switch t.ID() {
	case arrow.INT64:
		return "int64"
	case arrow.STRING:
		return "string"
	default:
		return "unknown"
	}
}
