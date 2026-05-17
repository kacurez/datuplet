// Package arrow provides a streaming Arrow RecordReader wrapper around the
// Datuplet SDK's gRPC ReadChunk stream. It is opt-in: only components that
// need arrow streaming (sql-transform) import this package. The base sdk/go
// stays arrow-free.
package arrow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// NewReader wraps an sdk.Reader's gRPC chunk stream as an array.RecordReader.
// The underlying sdk.Reader MUST have been opened with WithOutputFormat(FORMAT_ARROW_IPC).
// Closing the returned reader (via Release()) closes the underlying sdk.Reader.
func NewReader(ctx context.Context, r *sdk.Reader) (array.RecordReader, error) {
	if r == nil {
		return nil, errors.New("nil sdk.Reader")
	}
	grpcStream, err := r.OpenGRPCReadChunk(ctx)
	if err != nil {
		return nil, fmt.Errorf("open grpc read chunk: %w", err)
	}
	targetSchema, err := protoSchemaToArrow(r.Schema())
	if err != nil {
		return nil, fmt.Errorf("convert schema: %w", err)
	}
	return &grpcRecordReader{
		ctx:          ctx,
		stream:       grpcStream,
		targetSchema: targetSchema,
		sdkReader:    r,
		refCount:     1,
	}, nil
}

type grpcRecordReader struct {
	ctx          context.Context
	stream       pb.DataGateway_ReadChunkClient
	targetSchema *arrow.Schema
	sdkReader    *sdk.Reader
	current      arrow.RecordBatch
	err          error
	closed       bool
	refCount     int
}

func (g *grpcRecordReader) Schema() *arrow.Schema { return g.targetSchema }

func (g *grpcRecordReader) Next() bool {
	if g.current != nil {
		g.current.Release()
		g.current = nil
	}
	if g.err != nil || g.closed {
		return false
	}
	for {
		chunk, err := g.stream.Recv()
		if errors.Is(err, io.EOF) {
			return false
		}
		if err != nil {
			g.err = err
			return false
		}
		rdr, err := ipc.NewReader(bytes.NewReader(chunk.Data))
		if err != nil {
			g.err = fmt.Errorf("ipc read: %w", err)
			return false
		}
		if !rdr.Next() {
			// Empty chunk (no record batches in IPC stream) — try next.
			rdr.Release()
			continue
		}
		if !rdr.Schema().Equal(g.targetSchema) {
			rdr.Release()
			g.err = fmt.Errorf("ipc schema mismatch: stream %s, expected %s", rdr.Schema(), g.targetSchema)
			return false
		}
		rec := rdr.RecordBatch()
		rec.Retain()
		rdr.Release()
		g.current = rec
		return true
	}
}

func (g *grpcRecordReader) RecordBatch() arrow.RecordBatch { return g.current }

// Record is the deprecated alias kept on array.RecordReader; arrow-go v18 still
// requires it on the interface even though RecordBatch is preferred.
func (g *grpcRecordReader) Record() arrow.RecordBatch { return g.current }

func (g *grpcRecordReader) Err() error { return g.err }

func (g *grpcRecordReader) Retain() { g.refCount++ }

func (g *grpcRecordReader) Release() {
	g.refCount--
	if g.refCount > 0 {
		return
	}
	if g.current != nil {
		g.current.Release()
		g.current = nil
	}
	// Use Background here — g.ctx may already be cancelled by the caller
	// (e.g. defer rr.Release() after the ctx deadline fired) and we still
	// want CloseReader to land at the gateway. Errors are intentionally
	// ignored: Release has no error channel, and the gRPC reader-id is
	// reaped server-side anyway.
	if !g.closed {
		_ = g.sdkReader.Close(context.Background())
	}
	g.closed = true
}

// protoSchemaToArrow converts the proto Schema returned by OpenReaderResponse to *arrow.Schema.
// The mapping mirrors backend.schemaInfoToArrow on the DataGateway side so that
// IPC-stream schemas (built from the same SchemaInfo) compare equal here.
func protoSchemaToArrow(s *pb.Schema) (*arrow.Schema, error) {
	if s == nil {
		return nil, errors.New("nil proto schema")
	}
	fields := make([]arrow.Field, len(s.Columns))
	for i, c := range s.Columns {
		var t arrow.DataType
		switch c.Type {
		case "int32":
			t = arrow.PrimitiveTypes.Int32
		case "int64":
			t = arrow.PrimitiveTypes.Int64
		case "float32":
			t = arrow.PrimitiveTypes.Float32
		case "float64":
			t = arrow.PrimitiveTypes.Float64
		case "string":
			t = arrow.BinaryTypes.String
		case "boolean", "bool":
			t = arrow.FixedWidthTypes.Boolean
		case "binary":
			t = arrow.BinaryTypes.Binary
		case "timestamp":
			t = arrow.FixedWidthTypes.Timestamp_us
		case "date":
			t = arrow.FixedWidthTypes.Date32
		default:
			return nil, fmt.Errorf("unsupported type %q for column %q", c.Type, c.Name)
		}
		fields[i] = arrow.Field{Name: c.Name, Type: t, Nullable: c.Nullable}
	}
	return arrow.NewSchema(fields, nil), nil
}
