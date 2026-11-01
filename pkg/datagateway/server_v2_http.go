package datagateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// httpWriteResponse is the JSON body returned by the HTTP write endpoint.
type httpWriteResponse struct {
	RowsAccepted    int64      `json:"rows_accepted"`
	BufferSizeBytes int64      `json:"buffer_size_bytes"`
	InferredSchema  *pb.Schema `json:"inferred_schema,omitempty"`
}

// httpErrorResponse is the JSON error envelope returned by the HTTP data plane.
type httpErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

func (s *ServerV2) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.httpError(w, http.StatusMethodNotAllowed, "method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	// Extract writerID from path: /data/write/{writerID}
	path := strings.TrimPrefix(r.URL.Path, "/data/write/")
	writerID := strings.TrimSuffix(path, "/")
	if writerID == "" {
		s.httpError(w, http.StatusBadRequest, "missing writer ID", "MISSING_WRITER_ID")
		return
	}

	// Look up writer state
	s.mu.RLock()
	ws, ok := s.writers[writerID]
	s.mu.RUnlock()

	if !ok {
		s.httpError(w, http.StatusNotFound, fmt.Sprintf("unknown writer: %s", writerID), "WRITER_NOT_FOUND")
		return
	}

	// Process the chunk. Pass the request's context through so a
	// cancellation at the HTTP layer short-circuits the deferred
	// lakekeeper round-trip.
	//
	// Fast path: if the writer's adapter can stream (Arrow IPC, JSONL),
	// parse directly off r.Body — no io.ReadAll copy of the whole chunk.
	// Otherwise (CSV/Parquet, or any non-streaming adapter) fall back to
	// buffering the body and calling the []byte path.
	var (
		resp *pb.WriteChunkResponse
		err  error
	)
	if sa, ok := ws.adapter.(format.StreamingAdapter); ok {
		resp, err = s.processWriteChunkReader(r.Context(), ws, r.Body, sa)
	} else {
		var data []byte
		data, err = io.ReadAll(r.Body)
		if err != nil {
			s.httpError(w, http.StatusBadRequest, fmt.Sprintf("failed to read body: %v", err), "READ_ERROR")
			return
		}
		resp, err = s.processWriteChunk(r.Context(), ws, data)
	}
	if err != nil {
		s.httpError(w, http.StatusInternalServerError, err.Error(), "PROCESS_ERROR")
		return
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(httpWriteResponse{
		RowsAccepted:    resp.RowsAccepted,
		BufferSizeBytes: resp.BufferSizeBytes,
		InferredSchema:  resp.InferredSchema,
	})
}

// processWriteChunk processes a data chunk (shared between gRPC and HTTP).
//
// `ctx` carries the originating request's deadline / cancellation. It bounds
// the deferred lakekeeper.LoadOrCreateForWrite call and the blocking of the
// plane writer's Write, never the writer itself.
func (s *ServerV2) processWriteChunk(ctx context.Context, ws *writerState, data []byte) (*pb.WriteChunkResponse, error) {
	// Per-chunk debug log: the SINGLE most useful signal for diagnosing
	// whether SDK batching is in effect. Batched calls deliver ~1 MiB;
	// row-at-a-time calls deliver tens of bytes. Gated by DATUPLET_GATEWAY_DEBUG.
	Debugf("processWriteChunk: writer=%s bytes=%d format=%s", ws.writerID, len(data), ws.inputFormat)

	// Parse input data to Arrow
	record, inferredSchema, err := ws.adapter.Parse(data, ws.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}
	defer record.Release()

	return s.processParsedRecord(ctx, ws, record, inferredSchema)
}

// processWriteChunkReader is the streaming-ingest sibling of
// processWriteChunk. It parses directly off r (the HTTP request body) via a
// StreamingAdapter, avoiding the io.ReadAll copy of the whole chunk — the
// gateway's #1 ingest allocation site (run 40556560 profile).
func (s *ServerV2) processWriteChunkReader(ctx context.Context, ws *writerState, r io.Reader, sa format.StreamingAdapter) (*pb.WriteChunkResponse, error) {
	Debugf("processWriteChunkReader: writer=%s format=%s mode=streaming", ws.writerID, ws.inputFormat)

	record, inferredSchema, err := sa.ParseReader(r, ws.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}
	defer record.Release()

	return s.processParsedRecord(ctx, ws, record, inferredSchema)
}

// processParsedRecord holds the post-parse write path shared by the
// buffered (processWriteChunk) and streaming (processWriteChunkReader)
// entry points: the drop pipeline, today's record-type acceptance check on
// the post-drop record, first-chunk setup (deferred create, plane writer
// open) and the plane Write. The type check runs before any create or open,
// so a rejected first chunk fails with the "buffer error" class.
func (s *ServerV2) processParsedRecord(ctx context.Context, ws *writerState, record arrow.Record, inferredSchema *schema.Schema) (*pb.WriteChunkResponse, error) {
	// Apply transforms if configured
	outputRecord := record
	if ws.pipeline != nil {
		var err error
		outputRecord, err = ws.pipeline.Apply(record, s.allocator)
		if err != nil {
			return nil, fmt.Errorf("transform failed: %w", err)
		}
		if outputRecord != record {
			defer outputRecord.Release()
		}
	}

	tw, responseSchema, err := s.ensureTableWriter(ctx, ws, inferredSchema, outputRecord.Schema())
	if err != nil {
		return nil, err
	}
	if err := tw.Write(ctx, outputRecord); err != nil {
		return nil, fmt.Errorf("write error: %w", err)
	}

	return &pb.WriteChunkResponse{
		RowsAccepted:    outputRecord.NumRows(),
		BufferSizeBytes: tw.QueuedBytes(),
		InferredSchema:  responseSchema,
	}, nil
}

// ensureTableWriter checks the post-drop record schema recSchema against the
// writer's output schema (the plane only ever receives the types today's
// write path accepted; a non-canonical type in a dropped column never gets
// here) and returns ws's plane writer, opening it on the first chunk: the
// input schema is the component's or the first chunk's inferred one, the
// table (when not created at OpenWriter) is created with its post-drop form,
// and the writer is opened with that post-drop schema. Serialized under
// ws.initMu so parallel first chunks open one writer. Fails once the run is
// cancelled; a writer opened across the cancel is aborted. The inferred
// schema is returned (for the response) only by the call that adopted it.
func (s *ServerV2) ensureTableWriter(ctx context.Context, ws *writerState, inferred *schema.Schema, recSchema *arrow.Schema) (TableWriter, *pb.Schema, error) {
	ws.initMu.Lock()
	defer ws.initMu.Unlock()
	if ws.tw != nil {
		if err := acceptRecordTypes(recSchema, ws.outputSchema.ArrowSchema()); err != nil {
			return nil, nil, fmt.Errorf("buffer error: %w", err)
		}
		return ws.tw, nil, nil
	}

	inSchema, fromInference := ws.schema, false
	if inSchema == nil {
		inSchema, fromInference = inferred, true
	}
	if inSchema == nil {
		return nil, nil, fmt.Errorf("cannot create table %s.%s: no schema (request didn't supply one and inference failed)", ws.bucket, ws.table)
	}
	outSchema, err := outputSchemaFor(inSchema, ws.pipeline)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compute output schema: %w", err)
	}
	if err := acceptRecordTypes(recSchema, outSchema.ArrowSchema()); err != nil {
		return nil, nil, fmt.Errorf("buffer error: %w", err)
	}
	if s.isCancelled() {
		return nil, nil, errRunCancelled
	}

	if ws.basePath == "" {
		if s.lakekeeperResolver == nil {
			return nil, nil, fmt.Errorf("no write target for %s.%s", ws.bucket, ws.table)
		}
		sp := func(context.Context) (*schema.Schema, error) { return outSchema, nil }
		target, lkErr := s.lakekeeperResolver.LoadOrCreateForWrite(ctx, ws.bucket, ws.table, sp)
		if lkErr != nil {
			return nil, nil, fmt.Errorf("lakekeeper deferred create %s.%s: %w", ws.bucket, ws.table, lkErr)
		}
		ws.basePath = target.BasePath
		ws.creds = credsSource(target.VendedCreds)
	}

	tw, err := s.plane.OpenTableWriter(ctx, WriterSpec{
		DataPrefix:      ws.basePath,
		FilePrefix:      filePrefix(s.config.GetRunID(), ws.writerID),
		Schema:          outSchema.ArrowSchema(),
		Creds:           ws.creds,
		TargetFileBytes: s.config.TargetFileSize,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open table writer %s.%s: %w", ws.bucket, ws.table, err)
	}

	// Publish under s.mu, the lock the cancel watcher sets s.cancelled and
	// snapshots open writers under: either the watcher sees this writer and
	// aborts it, or this sees the cancel and aborts it here.
	s.mu.Lock()
	if s.cancelled {
		s.mu.Unlock()
		tw.Abort()
		return nil, nil, errRunCancelled
	}
	ws.schema = inSchema
	ws.outputSchema = outSchema
	ws.tw = tw
	s.mu.Unlock()

	var responseSchema *pb.Schema
	if fromInference {
		responseSchema = schemaToProto(inSchema)
		logWriterSchema(ws, "gateway-inferred")
	}
	return tw, responseSchema, nil
}

// handleRead serves the next chunk of a reader. The request ctx is not used
// for the read: a client that goes away mid-chunk does not end the reader.
func (s *ServerV2) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.httpError(w, http.StatusMethodNotAllowed, "method not allowed", "METHOD_NOT_ALLOWED")
		return
	}

	// Extract readerID from path: /data/read/{readerID}
	path := strings.TrimPrefix(r.URL.Path, "/data/read/")
	readerID := strings.TrimSuffix(path, "/")
	if readerID == "" {
		s.httpError(w, http.StatusBadRequest, "missing reader ID", "MISSING_READER_ID")
		return
	}

	rs, ok := s.lookupReader(readerID)
	if !ok {
		s.httpError(w, http.StatusNotFound, fmt.Sprintf("unknown reader: %s", readerID), "READER_NOT_FOUND")
		return
	}

	chunk, err := rs.nextChunk(s.readChunkSize(), s.allocator)
	if err != nil {
		s.httpError(w, http.StatusInternalServerError, fmt.Sprintf("read error: %v", err), "READ_ERROR")
		return
	}
	if chunk == nil {
		// The last chunk was already served.
		w.Header().Set("X-Datuplet-Is-Last", "true")
		w.Header().Set("X-Datuplet-Rows", "0")
		w.WriteHeader(http.StatusOK)
		return
	}

	w.Header().Set("Content-Type", dataFormatToContentType(rs.format))
	w.Header().Set("Content-Length", strconv.Itoa(len(chunk.data)))
	w.Header().Set("X-Datuplet-Rows", strconv.FormatInt(chunk.rows, 10))
	w.Header().Set("X-Datuplet-Is-Last", strconv.FormatBool(chunk.last))
	w.WriteHeader(http.StatusOK)
	w.Write(chunk.data)
}

// httpError sends an HTTP error response.
func (s *ServerV2) httpError(w http.ResponseWriter, status int, message, code string) {
	writeHTTPError(w, status, message, code)
}

// writeHTTPError writes the httpErrorResponse envelope with status.
func writeHTTPError(w http.ResponseWriter, status int, message, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(httpErrorResponse{
		Error: message,
		Code:  code,
	})
}

// dataFormatToContentType converts DataFormat to HTTP Content-Type.
func dataFormatToContentType(f format.DataFormat) string {
	switch f {
	case format.FormatCSV:
		return "text/csv"
	case format.FormatJSON:
		return "application/json"
	case format.FormatJSONL:
		return "application/x-ndjson"
	case format.FormatParquet:
		return "application/vnd.apache.parquet"
	case format.FormatArrowIPC:
		return "application/vnd.apache.arrow.stream"
	default:
		return "application/octet-stream"
	}
}

// getHTTPEndpoint returns the HTTP endpoint URL for a given path.
func (s *ServerV2) getHTTPEndpoint(path string) string {
	// Extract host part from httpAddr (remove leading colon if present)
	host := s.httpAddr
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	}
	return fmt.Sprintf("http://%s%s", host, path)
}
