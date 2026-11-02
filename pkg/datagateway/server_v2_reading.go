package datagateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// defaultReadChunkBytes is the chunk boundary (uncompressed Arrow bytes) when
// the config sets no chunk_size.
const defaultReadChunkBytes = 32 << 20

var errReaderClosed = errors.New("reader is closed")

// readerState is one open table reader: the parts of a plane TableReader
// served chunk by chunk over HTTP GET /data/read/{id} and gRPC ReadChunk. A
// raw part is exactly one chunk; a decoded (Records) part is cut into chunks
// of about chunk_size uncompressed bytes. A chunk never spans two parts.
type readerState struct {
	inputName string
	reader    TableReader
	schema    *arrow.Schema // the table's current schema
	wire      *arrow.Schema // SDK wire schema (FORMAT_ARROW_IPC only)
	format    format.DataFormat
	adapter   format.FormatAdapter // CSV/JSON/JSONL only

	// ctx is the reader's lifetime, owned by the server and cancelled only
	// by close: NextPart, Next and raw Open always run on it, never on an
	// RPC or HTTP request ctx.
	ctx    context.Context
	cancel context.CancelFunc

	// serveMu serializes whole chunk deliveries (including streaming a raw
	// body after nextChunk returned), so chunks reach the client in order.
	// close does not take it.
	serveMu sync.Mutex

	// mu guards the fields below.
	mu      sync.Mutex
	started bool // the first lookahead ran
	// Lookahead (what follows the chunk served last), filled by lookahead:
	// at most one of pending (the next batch of cur), nextRaw (the next
	// part, raw) and pendingErr (a NextPart/Next failure; the next chunk
	// returns it) is set; partsEOF means the plane has no more parts.
	cur        RecordStream
	pending    arrow.RecordBatch
	nextRaw    *RawFile
	pendingErr error
	partsEOF   bool
	// err is the reader's first failure (a Next while filling a chunk, a raw
	// open or stream, an encode, or pendingErr once returned); every later
	// chunk returns it, so a retried read never skips the failed chunk's rows.
	err    error
	done   bool // the last chunk was served
	closed bool

	closeOnce sync.Once
}

// readChunk is one chunk: encoded bytes, or a raw Parquet file body.
type readChunk struct {
	data    []byte
	raw     io.ReadCloser // raw part body; the receiver closes it
	rawSize int64         // raw body length in bytes
	rows    int64
	last    bool
}

func (s *ServerV2) OpenReader(ctx context.Context, req *pb.OpenReaderRequest) (*pb.OpenReaderResponse, error) {
	bucket := req.Bucket
	table := req.Table
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required for reads")
	}
	if table == "" {
		return nil, fmt.Errorf("table is required for reads")
	}

	// Incremental reads are not supported: a CRD `sinceDays` setting
	// degrades to a full-snapshot read with a warning instead of failing.
	if req.Incremental == nil {
		req.Incremental = s.getIncrementalSpecForTable(bucket, table)
	}
	if req.Incremental != nil {
		log.Printf("warning: incremental reads not supported (table %s.%s); falling back to full snapshot", bucket, table)
	}

	outputFormat := protoToDataFormat(req.OutputFormat)
	if outputFormat == format.FormatUnknown {
		outputFormat = format.FormatCSV
	}
	var adapter format.FormatAdapter
	switch outputFormat {
	case format.FormatArrowIPC, format.FormatParquet:
	default:
		var err error
		if adapter, err = s.registry.Get(outputFormat); err != nil {
			return nil, fmt.Errorf("unsupported output format: %s", outputFormat)
		}
	}

	if s.isCancelled() {
		return nil, errRunCancelled
	}
	tr, err := s.plane.OpenTableReader(ctx, ReaderSpec{
		Namespace:  bucket,
		Table:      table,
		RawParquet: outputFormat == format.FormatParquet,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open reader for %s.%s: %w", bucket, table, err)
	}
	sch := tr.Schema()
	protoSchema := arrowSchemaToProto(sch)

	var wire *arrow.Schema
	if outputFormat == format.FormatArrowIPC {
		wire, err = sdkWireSchema(protoSchema)
		if err == nil {
			err = checkWireCasts(sch, wire)
		}
		if err != nil {
			if cerr := tr.Close(); cerr != nil {
				log.Printf("Warning: close reader %s.%s: %v", bucket, table, cerr)
			}
			return nil, status.Errorf(codes.InvalidArgument, "table %s.%s cannot be read as Arrow IPC: %v", bucket, table, err)
		}
	}

	rctx, cancel := context.WithCancel(context.Background())
	rs := &readerState{
		inputName: bucket + "." + table,
		reader:    tr,
		schema:    sch,
		wire:      wire,
		format:    outputFormat,
		adapter:   adapter,
		ctx:       rctx,
		cancel:    cancel,
	}

	// Publish under s.mu, the lock the cancel watcher sets s.cancelled and
	// snapshots open readers under: either the watcher sees this reader and
	// closes it, or this sees the cancel and closes it here.
	s.mu.Lock()
	if s.cancelled {
		s.mu.Unlock()
		rs.close()
		return nil, errRunCancelled
	}
	s.readerCounter++
	readerID := fmt.Sprintf("r-%d", s.readerCounter)
	s.readers[readerID] = rs
	s.mu.Unlock()

	return &pb.OpenReaderResponse{
		ReaderId:     readerID,
		Schema:       protoSchema,
		HttpEndpoint: s.getHTTPEndpoint(fmt.Sprintf("/data/read/%s", readerID)),
		Bucket:       bucket,
		Table:        table,
	}, nil
}

// arrowSchemaToProto describes a plane reader's schema; a column whose Arrow
// type has no gateway type is reported as string. FieldId and Nullable are
// not catalog-sourced on this path: FieldId is 0 (unknown; the Iceberg field
// ids are not read) and Nullable is the plane's Arrow flag, not the Iceberg
// required flag.
func arrowSchemaToProto(sch *arrow.Schema) *pb.Schema {
	out := &pb.Schema{Columns: make([]*pb.ColumnDef, sch.NumFields())}
	for i, f := range sch.Fields() {
		dt, err := schema.FromArrowType(f.Type)
		if err != nil {
			dt = schema.TypeString
		}
		out.Columns[i] = &pb.ColumnDef{Name: f.Name, Type: dt.String(), Nullable: f.Nullable}
	}
	return out
}

// getIncrementalSpecForTable checks the gateway config for incremental read settings
// on the given bucket.table and returns an IncrementalReadSpec if found, or nil.
func (s *ServerV2) getIncrementalSpecForTable(bucket, table string) *pb.IncrementalReadSpec {
	for _, t := range s.config.InputTables {
		if t.Bucket == bucket && t.Table == table {
			if t.SinceTimestampMs > 0 {
				return &pb.IncrementalReadSpec{
					BaseSelector: &pb.IncrementalReadSpec_FromTimestampMs{
						FromTimestampMs: t.SinceTimestampMs,
					},
				}
			}
			if t.SinceSnapshot > 0 {
				return &pb.IncrementalReadSpec{
					BaseSelector: &pb.IncrementalReadSpec_FromSnapshotId{
						FromSnapshotId: t.SinceSnapshot,
					},
				}
			}
		}
	}
	return nil
}

// readChunkSize is the chunk boundary in uncompressed Arrow bytes.
func (s *ServerV2) readChunkSize() int64 {
	if s.config.ChunkSize > 0 {
		return s.config.ChunkSize
	}
	return defaultReadChunkBytes
}

// nextChunk produces the next chunk. A raw part is one chunk whose body is
// opened here (the receiver streams and closes it); a decoded part yields
// batches until the chunk holds at least limit uncompressed bytes or the
// part ends. The lookahead after each chunk decides whether it was the last;
// an empty table is one last chunk with zero rows. nil (no error) means the
// last chunk was already served.
func (rs *readerState) nextChunk(limit int64, mem memory.Allocator) (*readChunk, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.closed {
		return nil, errReaderClosed
	}
	if rs.err != nil {
		return nil, rs.err
	}
	if rs.done {
		return nil, nil
	}
	if !rs.started {
		rs.started = true
		rs.lookahead()
	}

	switch {
	case rs.nextRaw != nil:
		return rs.rawChunk()
	case rs.pending != nil:
		return rs.recordsChunk(limit, mem)
	case rs.pendingErr != nil:
		return nil, rs.fail(rs.pendingErr)
	default: // no parts at all: an empty table
		data, err := encodeChunk(rs.format, rs.adapter, rs.schema, rs.wire, nil, mem)
		if err != nil {
			return nil, rs.fail(fmt.Errorf("serialize %s chunk: %w", rs.format, err))
		}
		rs.done = true
		return &readChunk{data: data, last: true}, nil
	}
}

// rawChunk opens the raw part in nextRaw as one chunk; the caller holds rs.mu.
func (rs *readerState) rawChunk() (*readChunk, error) {
	rf := rs.nextRaw
	rs.nextRaw = nil
	if rs.format != format.FormatParquet {
		return nil, rs.fail(fmt.Errorf("data plane returned a raw part for a %s read", rs.format))
	}
	body, err := rf.Open(rs.ctx)
	if err != nil {
		return nil, rs.fail(fmt.Errorf("open data file %s: %w", rf.Path, err))
	}
	rs.lookahead()
	last := rs.exhausted()
	rs.done = last
	return &readChunk{raw: body, rawSize: rf.Size, rows: rf.Rows, last: last}, nil
}

// recordsChunk encodes pending plus the following batches of cur, up to
// limit bytes or the end of the part; the caller holds rs.mu.
func (rs *readerState) recordsChunk(limit int64, mem memory.Allocator) (*readChunk, error) {
	var batches []arrow.RecordBatch
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	var size, rows int64
	add := func(b arrow.RecordBatch) {
		batches = append(batches, b)
		size += batchBytes(b)
		rows += b.NumRows()
	}
	add(rs.pending)
	rs.pending = nil
	for size < limit && rs.cur != nil {
		b, err := rs.cur.Next(rs.ctx)
		switch {
		case errors.Is(err, io.EOF):
			rs.closeCur()
		case err != nil:
			return nil, rs.fail(err)
		default:
			add(b)
		}
	}
	rs.lookahead()

	data, err := encodeChunk(rs.format, rs.adapter, rs.schema, rs.wire, batches, mem)
	if err != nil {
		return nil, rs.fail(fmt.Errorf("serialize %s chunk: %w", rs.format, err))
	}
	last := rs.exhausted()
	rs.done = last
	return &readChunk{data: data, rows: rows, last: last}, nil
}

// lookahead finds what follows the current position: the next batch of the
// current part, else the next part (a decoded part is peeked to its first
// batch, so an empty decoded part is skipped). A failure is kept in
// pendingErr for the next chunk; the caller holds rs.mu.
func (rs *readerState) lookahead() {
	for {
		if rs.cur != nil {
			b, err := rs.cur.Next(rs.ctx)
			switch {
			case errors.Is(err, io.EOF):
				rs.closeCur()
				continue
			case err != nil:
				rs.pendingErr = err
			default:
				rs.pending = b
			}
			return
		}
		if rs.partsEOF {
			return
		}
		part, err := rs.reader.NextPart(rs.ctx)
		switch {
		case errors.Is(err, io.EOF):
			rs.partsEOF = true
			return
		case err != nil:
			rs.pendingErr = err
			return
		case part.Raw != nil:
			rs.nextRaw = part.Raw
			return
		case part.Records != nil:
			rs.cur = part.Records
		default:
			rs.pendingErr = errors.New("data plane returned an empty read part")
			return
		}
	}
}

// exhausted reports that nothing follows the chunk just produced; the caller
// holds rs.mu.
func (rs *readerState) exhausted() bool {
	return rs.partsEOF && rs.cur == nil && rs.pending == nil && rs.nextRaw == nil && rs.pendingErr == nil
}

// closeCur closes the current decoded part; the caller holds rs.mu.
func (rs *readerState) closeCur() {
	if rs.cur == nil {
		return
	}
	if err := rs.cur.Close(); err != nil {
		log.Printf("Warning: close read part of %s: %v", rs.inputName, err)
	}
	rs.cur = nil
}

// fail records err as the reader's sticky failure and drops the lookahead;
// the caller holds rs.mu.
func (rs *readerState) fail(err error) error {
	rs.err = err
	if rs.pending != nil {
		rs.pending.Release()
		rs.pending = nil
	}
	rs.nextRaw = nil
	rs.closeCur()
	return err
}

// failServed records a failure met while delivering an already produced
// chunk (a raw body that could not be streamed whole).
func (rs *readerState) failServed(err error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.err == nil && !rs.closed {
		rs.fail(err)
	}
}

// readRawFull reads a raw chunk body whole (gRPC carries a chunk in one
// message) and closes it; a failure is the reader's sticky failure.
func (rs *readerState) readRawFull(c *readChunk) ([]byte, error) {
	defer c.raw.Close()
	data, err := io.ReadAll(io.LimitReader(c.raw, c.rawSize+1))
	if err == nil && int64(len(data)) != c.rawSize {
		err = fmt.Errorf("data file is %d bytes, manifest says %d", len(data), c.rawSize)
	}
	if err != nil {
		err = fmt.Errorf("read data file: %w", err)
		rs.failServed(err)
		return nil, err
	}
	return data, nil
}

// close ends the reader: it cancels the lifetime ctx (interrupting a running
// NextPart, Next or raw read), drops the lookahead and closes the plane
// reader, which closes its open part streams and raw bodies. Idempotent.
func (rs *readerState) close() {
	rs.closeOnce.Do(func() {
		rs.cancel()
		rs.mu.Lock()
		rs.closed = true
		if rs.pending != nil {
			rs.pending.Release()
			rs.pending = nil
		}
		rs.nextRaw = nil
		rs.cur = nil // closed by the reader's Close
		rs.mu.Unlock()
		if err := rs.reader.Close(); err != nil {
			log.Printf("Warning: close reader %s: %v", rs.inputName, err)
		}
	})
}

func (s *ServerV2) lookupReader(id string) (*readerState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rs, ok := s.readers[id]
	return rs, ok
}

func (s *ServerV2) ReadChunk(req *pb.ReadChunkRequest, stream pb.DataGateway_ReadChunkServer) error {
	rs, ok := s.lookupReader(req.ReaderId)
	if !ok {
		return fmt.Errorf("unknown reader: %s", req.ReaderId)
	}
	for {
		last, err := s.sendNextChunk(rs, stream)
		if err != nil || last {
			return err
		}
	}
}

// sendNextChunk sends one chunk over gRPC; last is true when there is
// nothing more to send.
func (s *ServerV2) sendNextChunk(rs *readerState, stream pb.DataGateway_ReadChunkServer) (bool, error) {
	rs.serveMu.Lock()
	defer rs.serveMu.Unlock()
	chunk, err := rs.nextChunk(s.readChunkSize(), s.allocator)
	if err != nil {
		return true, fmt.Errorf("read error: %w", err)
	}
	if chunk == nil {
		return true, nil
	}
	data := chunk.data
	if chunk.raw != nil {
		if data, err = rs.readRawFull(chunk); err != nil {
			return true, fmt.Errorf("read error: %w", err)
		}
	}
	if err := stream.Send(&pb.DataChunk{
		Data:        data,
		Format:      dataFormatToProto(rs.format),
		RowsInChunk: chunk.rows,
		IsLast:      chunk.last,
	}); err != nil {
		return true, err
	}
	return chunk.last, nil
}

func (s *ServerV2) CloseReader(ctx context.Context, req *pb.CloseReaderRequest) (*pb.CloseReaderResponse, error) {
	s.mu.Lock()
	rs, ok := s.readers[req.ReaderId]
	if ok {
		delete(s.readers, req.ReaderId)
	}
	s.mu.Unlock()

	if ok {
		rs.close()
	}
	return &pb.CloseReaderResponse{}, nil
}

// GetSchema is not supported: OpenReader returns the table schema.
func (s *ServerV2) GetSchema(ctx context.Context, req *pb.GetSchemaRequest) (*pb.SchemaResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetSchema is not supported; OpenReader returns the table schema")
}

// GetSample is not supported: read the table with OpenReader.
func (s *ServerV2) GetSample(ctx context.Context, req *pb.GetSampleRequest) (*pb.SampleResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetSample is not supported; read the table with OpenReader")
}
