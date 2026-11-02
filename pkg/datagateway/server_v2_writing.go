package datagateway

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway/format"
	dglakekeeper "github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	"github.com/datuplet/datuplet/pkg/datagateway/processor"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// isSchemaDeferred reports whether a lakekeeper resolve failure is the
// EXPECTED "table doesn't exist yet and we have no schema for it" case, which
// is not an error at all: the table is created on first write once the first
// chunk's schema is known. Matches the message raised by
// pkg/datagateway/lakekeeper.
func isSchemaDeferred(err error) bool {
	return err != nil && strings.Contains(err.Error(), "missing and no schema available")
}

// maxLoggedSchemaColumns caps how many columns logWriterSchema prints. Wide
// tables exist (a live GBIF occurrence table has 100 columns); the point of
// the line is to make a schema mismatch legible, and the leading columns plus
// an exact total achieve that without emitting a 4 KB log line.
const maxLoggedSchemaColumns = 30

// logWriterSchema records the schema a writer will actually emit, once per
// writer. Without this, a commit rejected on an Iceberg schema mismatch left
// NO record anywhere of what schema was sent — diagnosing one meant reading
// table metadata straight out of object storage. `source` distinguishes a
// component-supplied schema from one the gateway inferred from the first chunk.
func logWriterSchema(ws *writerState, source string) {
	if ws.schema == nil {
		return
	}
	cols := ws.schema.Columns()
	shown := cols
	if len(shown) > maxLoggedSchemaColumns {
		shown = shown[:maxLoggedSchemaColumns]
	}
	parts := make([]string, 0, len(shown))
	for _, c := range shown {
		null := ""
		if c.Nullable {
			null = "?"
		}
		parts = append(parts, fmt.Sprintf("%s:%s%s", c.Name, c.Type, null))
	}
	truncated := ""
	if len(cols) > len(shown) {
		truncated = fmt.Sprintf(", +%d more", len(cols)-len(shown))
	}
	log.Printf("writer schema: writer=%s table=%s.%s source=%s columns=%d [%s%s]",
		ws.writerID, ws.bucket, ws.table, source, len(cols), strings.Join(parts, ", "), truncated)
}

// partitionStatus returns a partitioning failure as gRPC InvalidArgument
// (spec decision 2); other errors pass through.
func partitionStatus(err error) error {
	if err != nil && errors.Is(err, ErrInvalidPartition) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return err
}

// partitionFieldsFor returns the configured partition_fields of (bucket,
// table), matching both exactly as writeModeForTable does.
func (s *ServerV2) partitionFieldsFor(bucket, table string) []dglakekeeper.PartitionField {
	for _, t := range s.config.OutputTables {
		if t.Name == table && t.Bucket == bucket {
			out := make([]dglakekeeper.PartitionField, len(t.PartitionFields))
			for i, f := range t.PartitionFields {
				out[i] = dglakekeeper.PartitionField{SourceColumn: f.SourceColumn, Transform: f.Transform}
			}
			return out
		}
	}
	return nil
}

// adoptTarget records a resolved write target on ws and logs, once per
// writer, a configured partitioning the existing table does not have.
func adoptTarget(ws *writerState, target *dglakekeeper.WriterTarget) {
	ws.basePath = target.BasePath
	ws.creds = credsSource(target.VendedCreds)
	spec := target.Spec
	ws.partSpec = &spec
	ws.tableSchema = target.Schema
	if target.SpecWarning != "" {
		log.Printf("warning: writer=%s table=%s.%s: %s", ws.writerID, ws.bucket, ws.table, target.SpecWarning)
	}
}

func (s *ServerV2) OpenWriter(ctx context.Context, req *pb.OpenWriterRequest) (*pb.OpenWriterResponse, error) {
	resp, err := s.openWriter(ctx, req)
	return resp, partitionStatus(err)
}

func (s *ServerV2) openWriter(ctx context.Context, req *pb.OpenWriterRequest) (*pb.OpenWriterResponse, error) {
	// Determine bucket and table from request
	bucket := req.Bucket
	table := req.Table
	if table == "" {
		return nil, fmt.Errorf("table is required")
	}

	// If bucket is not specified in request, try to find it from output_tables config
	if bucket == "" {
		// Look up table in output_tables configuration
		for _, t := range s.config.OutputTables {
			if t.Name == table {
				bucket = t.Bucket
				break
			}
		}
		// If still no bucket, use defaultBucket from config
		if bucket == "" {
			bucket = s.config.DefaultBucket
		}
	}
	if bucket == "" {
		// Distinguish the two ways to land here. The common one by far is a
		// component writing a table name that isn't declared in
		// outputs.tables — typically because the pipeline's component
		// config and its outputs.tables list drifted apart. Naming the
		// declared tables turns a confusing "bucket is required" into an
		// obvious diff.
		if len(s.config.OutputTables) > 0 {
			declared := make([]string, 0, len(s.config.OutputTables))
			for _, t := range s.config.OutputTables {
				declared = append(declared, t.Name)
			}
			return nil, fmt.Errorf("table '%s' is not declared in outputs.tables (declared: %s) and no defaultBucket is configured; "+
				"either add '%s' to outputs.tables, set outputs.defaultBucket, or make the component write one of the declared tables",
				table, strings.Join(declared, ", "), table)
		}
		return nil, fmt.Errorf("cannot determine a bucket for table '%s': no bucket in the request, no outputs.tables entries, and no outputs.defaultBucket configured", table)
	}

	// Get format adapter
	inputFormat := protoToDataFormat(req.InputFormat)
	if inputFormat == format.FormatUnknown {
		inputFormat = format.FormatCSV // Default to CSV
	}

	adapter, err := s.registry.Get(inputFormat)
	if err != nil {
		return nil, fmt.Errorf("unsupported input format: %s", inputFormat)
	}

	// Build transform pipeline from config processors (if any configured globally)
	pipeline := buildPipelineFromProcessors(s.config.Processors)

	// Convert proto schema if provided. Records are parsed with it; the
	// table is created with its post-drop form.
	var sch *schema.Schema
	if req.Schema != nil && len(req.Schema.Columns) > 0 {
		sch, err = protoToSchema(req.Schema)
		if err != nil {
			return nil, fmt.Errorf("invalid schema: %w", err)
		}
	}

	// Resolve the per-table write target. Two paths only:
	//
	//   1. Lakekeeper resolver (production): ask lakekeeper for the
	//      writable target. The resolver creates the table now when the
	//      component supplied a schema (with the drop processors applied),
	//      otherwise on the first chunk from the inferred schema.
	//   2. LocalDataDir (tests): <LocalDataDir>/<bucket>/<table>/data/.
	var target *dglakekeeper.WriterTarget
	var localBase string
	switch {
	case s.lakekeeperResolver != nil:
		var sp dglakekeeper.SchemaProvider
		if sch != nil {
			createSchema, schErr := outputSchemaFor(sch, pipeline)
			if schErr != nil {
				return nil, fmt.Errorf("failed to compute output schema: %w", schErr)
			}
			sp = func(context.Context) (*schema.Schema, error) { return createSchema, nil }
		}
		var lkErr error
		target, lkErr = s.lakekeeperResolver.LoadOrCreateForWrite(ctx, bucket, table, sp, s.partitionFieldsFor(bucket, table))
		if lkErr != nil {
			if sp != nil || !isSchemaDeferred(lkErr) {
				return nil, fmt.Errorf("lakekeeper open writer: %w", lkErr)
			}
			// Schema-deferred: processParsedRecord creates the table once
			// the first chunk's inferred schema is in hand.
			target = nil
		}
	case s.config.LocalDataDir != "":
		localBase = joinStoragePath(s.config.LocalDataDir, bucket+"/"+table+"/data/")
	default:
		return nil, fmt.Errorf("no storage backend configured (need lakekeeper resolver or local data dir)")
	}

	// Generate writer ID and store state
	s.mu.Lock()
	s.writerCounter++
	writerID := fmt.Sprintf("w%d", s.writerCounter)
	s.writers[writerID] = &writerState{
		writerID:       writerID,
		outputName:     table, // Use table name as output name
		bucket:         bucket,
		table:          table,
		basePath:       localBase,
		inputFormat:    inputFormat,
		adapter:        adapter,
		pipeline:       pipeline,
		schema:         sch,
		schemaInferred: sch == nil,
	}
	ws := s.writers[writerID]
	if target != nil {
		adoptTarget(ws, target)
	}
	s.mu.Unlock()

	// A component-supplied schema is known now; an inferred one is logged by
	// processParsedRecord once the first chunk resolves it.
	logWriterSchema(ws, "component")

	// Build HTTP endpoint URL
	httpEndpoint := s.getHTTPEndpoint(fmt.Sprintf("/data/write/%s", writerID))

	return &pb.OpenWriterResponse{
		WriterId:     writerID,
		HttpEndpoint: httpEndpoint,
		Bucket:       bucket,
		Table:        table,
	}, nil
}

func (s *ServerV2) WriteChunk(ctx context.Context, req *pb.WriteChunkRequest) (*pb.WriteChunkResponse, error) {
	s.mu.RLock()
	ws, ok := s.writers[req.WriterId]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown writer: %s", req.WriterId)
	}

	// Use shared processing logic
	resp, err := s.processWriteChunk(ctx, ws, req.Data)
	return resp, partitionStatus(err)
}

func (s *ServerV2) CloseWriter(ctx context.Context, req *pb.CloseWriterRequest) (*pb.CloseWriterResponse, error) {
	s.mu.Lock()
	ws, ok := s.writers[req.WriterId]
	// Don't delete writer from map here - Commit needs to process it later.
	s.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("unknown writer: %s", req.WriterId)
	}

	if len(req.GetExternalFiles()) > 0 {
		return nil, fmt.Errorf("external data files are not supported (writer %s, table %s.%s)", req.WriterId, ws.bucket, ws.table)
	}

	// Close the plane writer (the writer stays in the map for Commit).
	files, err := s.closeTableWriter(ctx, ws)
	if err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	// Dispatch the inline commit for the files just written.
	if err := s.finalizeAndDispatch(ctx, ws, s.config.GetRunID()); err != nil {
		return nil, err
	}

	var totalRows, totalBytes int64
	for _, f := range files {
		totalRows += f.Rows
		totalBytes += f.Bytes
	}
	return &pb.CloseWriterResponse{
		TotalRows:    totalRows,
		TotalBytes:   totalBytes,
		FilesWritten: int32(len(files)),
	}, nil
}

// closeTableWriter closes ws's plane writer and records its files. A writer
// that never received a chunk has no plane writer and no files. Safe to call
// again (TableWriter.Close is idempotent): the Commit sweep relies on that.
func (s *ServerV2) closeTableWriter(ctx context.Context, ws *writerState) ([]WrittenFile, error) {
	ws.initMu.Lock()
	defer ws.initMu.Unlock()
	if ws.tw == nil {
		return nil, nil
	}
	files, err := ws.tw.Close(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	ws.files = files
	s.mu.Unlock()
	return files, nil
}

// finalizeAndDispatch dispatches a closed writer's parquet files to the
// commit pool. Zero files: nothing is dispatched (the table result is
// SKIPPED).
//
// Claims the writer under s.mu (sets ws.committed) then releases the lock for
// the dispatch. MUST be called WITHOUT s.mu held. No-op when the writer is
// already claimed. When s.commitPool is nil (tests) the writer is still
// claimed but no commit is dispatched.
func (s *ServerV2) finalizeAndDispatch(ctx context.Context, ws *writerState, runID string) error {
	s.mu.Lock()
	if ws.committed {
		s.mu.Unlock()
		return nil
	}
	ws.committed = true
	files := ws.files
	s.mu.Unlock()

	if s.commitPool == nil || len(files) == 0 {
		return nil
	}
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.Path
	}
	var part *icebergjob.PartitionedFiles
	if files[0].Partition != nil {
		part = &icebergjob.PartitionedFiles{SpecID: files[0].SpecID, Tuples: make([]map[int]any, len(files))}
		for i, f := range files {
			part.Tuples[i] = f.Partition
		}
	}
	mode := icebergjob.WriteModeAppend
	if m := s.writeModeForTable(ws.bucket, ws.table); m != "" {
		mode = m
	}
	if err := s.commitPool.Dispatch(ctx, CommitJob{
		WriterID: ws.writerID, Namespace: ws.bucket, Table: ws.table,
		DataPaths: paths, Partition: part, Mode: mode, RunID: runID,
	}); err != nil {
		return fmt.Errorf("dispatch commit %s.%s: %w", ws.bucket, ws.table, err)
	}
	log.Printf("dispatched inline commit: writer=%s table=%s.%s files=%d mode=%s", ws.writerID, ws.bucket, ws.table, len(paths), mode)
	return nil
}

// outputSchemaFor applies the drop pipeline (if any) to an input schema.
func outputSchemaFor(in *schema.Schema, pipeline *processor.Pipeline) (*schema.Schema, error) {
	if pipeline == nil {
		return in, nil
	}
	return pipeline.TransformSchema(in)
}

// credsSource keeps a nil *VendedCreds (file:// prefixes) from becoming a
// non-nil CredsSource interface.
func credsSource(vc *catalogwriter.VendedCreds) CredsSource {
	if vc == nil {
		return nil
	}
	return vc
}

// filePrefix is the run-scoped file name prefix part-<run8>-<writerID>, so
// writers of different runs never collide in a shared data/ directory.
func filePrefix(runID, writerID string) string {
	if len(runID) > 8 {
		runID = runID[:8]
	}
	return fmt.Sprintf("part-%s-%s", runID, writerID)
}

// acceptRecordTypes is today's record-type acceptance check (formerly in
// BufferManager.Add: structural schema equality, then the column types its
// copy path supported), applied to the post-drop record before the plane
// sees it. want is the writer's output schema.
func acceptRecordTypes(got, want *arrow.Schema) error {
	if !schemasCompatible(got, want) {
		return errors.New("record schema does not match buffer manager schema")
	}
	for i, f := range got.Fields() {
		if !writableArrowType(f.Type) {
			return fmt.Errorf("col %d (%q): unsupported Arrow type %s", i, f.Name, f.Type)
		}
	}
	return nil
}

// schemasCompatible reports whether two Arrow schemas have the same field
// count and structurally equal field types (names, nullability and metadata
// do not matter).
func schemasCompatible(a, b *arrow.Schema) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.NumFields() != b.NumFields() {
		return false
	}
	for i := 0; i < a.NumFields(); i++ {
		if !arrow.TypeEqual(a.Field(i).Type, b.Field(i).Type) {
			return false
		}
	}
	return true
}

// writableArrowType reports whether today's write path accepted a column of
// this type (large_string / large_binary and nested types were rejected).
func writableArrowType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.BOOL, arrow.STRING, arrow.BINARY,
		arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64:
		return true
	}
	return false
}

// writeModeForTable returns the configured WriteMode for the given (bucket,
// table) pair, or "" if no explicit mode is set (caller defaults to APPEND).
// Matches on BOTH bucket AND table — never on table name alone — to prevent
// accidentally applying FULL_LOAD to a same-named table in a different bucket.
//
// Resolution order:
//  1. Explicit OutputTables entry matching (bucket, table) — highest priority.
//  2. DefaultBucket + DefaultWriteMode: applies to every table written to the
//     default bucket (the common "defaultBucket: FULL_LOAD" pipeline form).
//  3. No match → "" (caller defaults to APPEND).
//
// OutputBuckets entries are plain strings with no per-bucket write mode, so
// they are intentionally not consulted here.
func (s *ServerV2) writeModeForTable(bucket, table string) icebergjob.WriteMode {
	// 1. Explicit per-table config wins.
	for _, t := range s.config.OutputTables {
		if t.Bucket == bucket && t.Name == table {
			switch strings.ToUpper(t.WriteMode) {
			case "FULL_LOAD":
				return icebergjob.WriteModeFullLoad
			default:
				return icebergjob.WriteModeAppend
			}
		}
	}
	// 2. DefaultBucket + DefaultWriteMode fallback: covers the common pipeline
	// form where all outputs go to a single default bucket and the operator
	// sets default_write_mode (defaults to "FULL_LOAD" when not specified).
	if s.config.DefaultBucket != "" && s.config.DefaultBucket == bucket {
		if strings.ToUpper(s.config.DefaultWriteMode) == "FULL_LOAD" {
			return icebergjob.WriteModeFullLoad
		}
		// DefaultBucket set but mode is APPEND (or empty → operator defaults
		// FULL_LOAD, so empty here means something unusual — default to APPEND).
		return icebergjob.WriteModeAppend
	}
	return ""
}
