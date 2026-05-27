package datagateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	"github.com/datuplet/datuplet/pkg/datagateway/buffer"
	"github.com/datuplet/datuplet/pkg/datagateway/format"
	"github.com/datuplet/datuplet/pkg/datagateway/partition"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// countingReader wraps an io.Reader and tallies bytes read. Used by the
// streaming-ingest path to keep the per-chunk byte count (ws.totalBytes)
// accurate without buffering the payload.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

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
// `ctx` carries the originating request's deadline / cancellation. It is
// used both for the deferred lakekeeper.LoadOrCreateForWrite call and as
// the parent of the buffer-manager backend writer, so a cancelled
// request short-circuits the in-flight S3 PutObject path.
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

	return s.processParsedRecord(ctx, ws, record, inferredSchema, int64(len(data)))
}

// processWriteChunkReader is the streaming-ingest sibling of
// processWriteChunk. It parses directly off r (the HTTP request body) via a
// StreamingAdapter, avoiding the io.ReadAll copy of the whole chunk — the
// gateway's #1 ingest allocation site (run 40556560 profile). A
// byte-counting reader keeps ws.totalBytes accurate without materialising
// the payload.
func (s *ServerV2) processWriteChunkReader(ctx context.Context, ws *writerState, r io.Reader, sa format.StreamingAdapter) (*pb.WriteChunkResponse, error) {
	Debugf("processWriteChunkReader: writer=%s format=%s mode=streaming", ws.writerID, ws.inputFormat)

	cr := &countingReader{r: r}
	record, inferredSchema, err := sa.ParseReader(cr, ws.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}
	defer record.Release()

	return s.processParsedRecord(ctx, ws, record, inferredSchema, cr.n)
}

// processParsedRecord holds the post-parse write path shared by the
// buffered (processWriteChunk) and streaming (processWriteChunkReader)
// entry points: deferred lakekeeper create, buffer-manager init, transform,
// and the Add into the buffer/partition router. inputBytes is the raw
// chunk size used only for the ws.totalBytes counter.
func (s *ServerV2) processParsedRecord(ctx context.Context, ws *writerState, record arrow.Record, inferredSchema *schema.Schema, inputBytes int64) (*pb.WriteChunkResponse, error) {
	var err error

	// Deferred-create + buffer-manager init: serialize under ws.initMu so
	// two parallel chunk requests for the same writer don't race on
	// building duplicate BufferManager / partitionRouter / per-writer
	// backend instances. The lock guards the whole "first-chunk wins"
	// path: lakekeeper LoadOrCreate, schema fan-out, buffer construction.
	var responseSchema *pb.Schema
	ws.initMu.Lock()
	if ws.basePath == "" && s.lakekeeperResolver != nil {
		schemaToUse := ws.schema
		if schemaToUse == nil {
			schemaToUse = inferredSchema
		}
		if schemaToUse == nil {
			ws.initMu.Unlock()
			return nil, fmt.Errorf("cannot create table %s.%s: no schema (request didn't supply one and inference failed)", ws.bucket, ws.table)
		}
		// Capture the schema in a closure so the resolver hits the
		// schema we already have (no second inference round-trip).
		sch := schemaToUse
		sp := func(context.Context) (*schema.Schema, error) { return sch, nil }
		target, lkErr := s.lakekeeperResolver.LoadOrCreateForWrite(ctx, ws.bucket, ws.table, sp)
		if lkErr != nil {
			ws.initMu.Unlock()
			return nil, fmt.Errorf("lakekeeper deferred create %s.%s: %w", ws.bucket, ws.table, lkErr)
		}
		ws.basePath = target.BasePath
		ws.writerBackend = target.Backend
		ws.tableExists = true
	}

	// If schema was inferred, store it and create buffer manager
	if ws.schema == nil && inferredSchema != nil {
		ws.schema = inferredSchema
		responseSchema = schemaToProto(inferredSchema)

		// Build common buffer config
		bufferConfig := buffer.DefaultBufferConfig()
		bufferConfig.OutputDir = ws.basePath
		// Run-scoped file prefix: part-{runID8}-{writerID} to avoid collisions in shared data/ dir
		runID := s.config.GetRunID()
		runID8 := runID
		if len(runID8) > 8 {
			runID8 = runID8[:8]
		}
		bufferConfig.FilePrefix = fmt.Sprintf("part-%s-%s", runID8, ws.writerID)
		if s.config.BufferSize > 0 {
			bufferConfig.BufferSize = s.config.BufferSize
		}
		if s.config.RowGroupSize > 0 {
			bufferConfig.RowGroupSize = s.config.RowGroupSize
		}
		if s.config.TargetFileSize > 0 {
			bufferConfig.TargetFileSize = s.config.TargetFileSize
		}

		// Apply transform to get output schema
		outputSchema := ws.schema
		if ws.pipeline != nil {
			outputSchema, err = ws.pipeline.TransformSchema(ws.schema)
			if err != nil {
				ws.initMu.Unlock()
				return nil, fmt.Errorf("failed to compute output schema: %w", err)
			}
		}
		ws.outputSchema = outputSchema

		// Use per-writer backend if available (lakekeeper / static backend)
		backendToUse := s.backend
		if ws.writerBackend != nil {
			backendToUse = ws.writerBackend
		}
		factory := buffer.NewBackendWriterFactory(s.ctx, backendToUse)

		if len(ws.partitionFields) > 0 {
			// Partitioned table: create Router that manages per-partition BufferManagers
			fields := make([]partition.FieldSpec, len(ws.partitionFieldDefs))
			for i, pf := range ws.partitionFieldDefs {
				fields[i] = partition.FieldSpec{
					SourceColumn: pf.SourceColumn,
					Transform:    pf.Transform,
					FieldName:    pf.FieldName,
				}
			}
			ws.partitionRouter = partition.NewRouter(
				fields, ws.basePath, bufferConfig, s.allocator, factory,
			)
		} else {
			// Unpartitioned table: single BufferManager
			ws.bufferMgr, err = buffer.NewBufferManager(
				outputSchema,
				bufferConfig,
				s.allocator,
				factory,
			)
			if err != nil {
				ws.initMu.Unlock()
				return nil, fmt.Errorf("failed to create buffer manager: %w", err)
			}
		}
	}
	ws.initMu.Unlock()

	// Apply transforms if configured
	outputRecord := record
	if ws.pipeline != nil {
		outputRecord, err = ws.pipeline.Apply(record, s.allocator)
		if err != nil {
			return nil, fmt.Errorf("transform failed: %w", err)
		}
		if outputRecord != record {
			defer outputRecord.Release()
		}
	}

	// Add to buffer (partitioned or unpartitioned)
	if ws.partitionRouter != nil {
		if err := ws.partitionRouter.Add(outputRecord, ws.outputSchema); err != nil {
			return nil, fmt.Errorf("partition routing error: %w", err)
		}
	} else if ws.bufferMgr != nil {
		if err := ws.bufferMgr.Add(outputRecord); err != nil {
			return nil, fmt.Errorf("buffer error: %w", err)
		}
	}

	// Update statistics
	ws.totalRows += outputRecord.NumRows()
	ws.totalBytes += inputBytes

	// Get buffer size for monitoring
	var bufferSize int64
	if ws.bufferMgr != nil {
		bufferSize = ws.bufferMgr.Stats().BufferedBytes
	}

	return &pb.WriteChunkResponse{
		RowsAccepted:    outputRecord.NumRows(),
		BufferSizeBytes: bufferSize,
		InferredSchema:  responseSchema,
	}, nil
}

// handleRead handles HTTP GET requests for reading data.
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

	// Look up reader state
	s.mu.RLock()
	rs, ok := s.readers[readerID]
	s.mu.RUnlock()

	if !ok {
		s.httpError(w, http.StatusNotFound, fmt.Sprintf("unknown reader: %s", readerID), "READER_NOT_FOUND")
		return
	}

	// Read a chunk from backend
	chunk, err := rs.backendReader.ReadChunk()
	if err == io.EOF {
		// No more data
		w.Header().Set("X-Datuplet-Is-Last", "true")
		w.Header().Set("X-Datuplet-Rows", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		s.httpError(w, http.StatusInternalServerError, fmt.Sprintf("read error: %v", err), "READ_ERROR")
		return
	}

	// Process chunk (convert format if needed)
	outputData, rowsInChunk, err := s.processReadChunk(rs, chunk)
	if err != nil {
		s.httpError(w, http.StatusInternalServerError, err.Error(), "PROCESS_ERROR")
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", dataFormatToContentType(rs.adapter.Format()))
	w.Header().Set("X-Datuplet-Rows", fmt.Sprintf("%d", rowsInChunk))
	w.Header().Set("X-Datuplet-Is-Last", fmt.Sprintf("%t", chunk.IsLast))
	w.WriteHeader(http.StatusOK)
	w.Write(outputData)
}

// processReadChunk processes a read chunk (shared between gRPC and HTTP).
func (s *ServerV2) processReadChunk(rs *readerState, chunk *backend.DataChunk) ([]byte, int64, error) {
	// Determine input format from chunk
	inputFormat := format.ParseDataFormat(chunk.Format)

	// If same format and no transforms, pass through
	if inputFormat == rs.adapter.Format() && rs.pipeline == nil {
		return chunk.Data, chunk.RowsInChunk, nil
	}

	// Get input adapter
	inputAdapter, err := s.registry.Get(inputFormat)
	if err != nil {
		return nil, 0, fmt.Errorf("unsupported input format: %s", inputFormat)
	}

	// Parse to Arrow
	record, _, err := inputAdapter.Parse(chunk.Data, rs.schema)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse chunk: %w", err)
	}
	defer record.Release()

	// Apply transforms if configured
	outputRecord := record
	if rs.pipeline != nil {
		outputRecord, err = rs.pipeline.Apply(record, s.allocator)
		if err != nil {
			return nil, 0, fmt.Errorf("transform failed: %w", err)
		}
		if outputRecord != record {
			defer outputRecord.Release()
		}
	}

	// Serialize to output format
	outputData, err := rs.adapter.Serialize(outputRecord)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to serialize: %w", err)
	}

	return outputData, outputRecord.NumRows(), nil
}

// httpError sends an HTTP error response.
func (s *ServerV2) httpError(w http.ResponseWriter, status int, message, code string) {
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
