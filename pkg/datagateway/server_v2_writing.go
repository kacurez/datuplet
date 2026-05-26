package datagateway

import (
	"context"
	"fmt"
	"log"
	"strings"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	"github.com/datuplet/datuplet/pkg/datagateway/buffer"
	"github.com/datuplet/datuplet/pkg/datagateway/format"
	dglakekeeper "github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	"github.com/datuplet/datuplet/pkg/datagateway/manifest"
	"github.com/datuplet/datuplet/pkg/datagateway/processor"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
	"github.com/datuplet/datuplet/pkg/icebergjob"
	"github.com/datuplet/datuplet/pkg/lib/controlfile"
)

func (s *ServerV2) OpenWriter(ctx context.Context, req *pb.OpenWriterRequest) (*pb.OpenWriterResponse, error) {
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
		return nil, fmt.Errorf("bucket is required (either in request, via output_tables config for table '%s', or via defaultBucket config)", table)
	}

	// Resolve the per-table write target. Two paths only:
	//
	//   1. Static `s.backend` (test fixtures): reuse it verbatim and
	//      synthesize the basePath from <bucket>/<table>. tableExists
	//      stays false; control-file lookup is a real-deployment
	//      concern, tests inject schema via the pipeline config or
	//      rely on inference.
	//   2. Lakekeeper resolver (production): ask lakekeeper for the
	//      writable target. The resolver creates the table on first
	//      write when a schema is available (request-supplied or
	//      first-chunk inferred). Vended STS creds flow into a per-
	//      writer minio backend.
	var basePath string
	var writerBackend backend.StorageBackend
	var tableExists bool
	if s.backend != nil {
		writerBackend = s.backend
		basePath = bucket + "/" + table + "/data/"
	} else if s.lakekeeperResolver != nil {
		var sp dglakekeeper.SchemaProvider
		if req.Schema != nil && len(req.Schema.Columns) > 0 {
			sch, schErr := protoToSchema(req.Schema)
			if schErr != nil {
				return nil, fmt.Errorf("invalid schema: %w", schErr)
			}
			sp = func(context.Context) (*schema.Schema, error) { return sch, nil }
		}
		target, lkErr := s.lakekeeperResolver.LoadOrCreateForWrite(ctx, bucket, table, sp)
		if lkErr != nil {
			if sp == nil && strings.Contains(lkErr.Error(), "missing and no schema available") {
				// Schema-deferred: nothing to do at OpenWriter time;
				// processWriteChunk re-runs LoadOrCreate once the first
				// chunk's inferred schema is in hand.
				basePath = ""
				writerBackend = nil
				tableExists = false
			} else {
				return nil, fmt.Errorf("lakekeeper open writer: %w", lkErr)
			}
		} else {
			basePath = target.BasePath
			writerBackend = target.Backend
			tableExists = true
		}
	} else {
		return nil, fmt.Errorf("no storage backend configured (need lakekeeper resolver or static backend)")
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
	var pipeline *processor.Pipeline
	if len(s.config.Processors) > 0 {
		pipeline = buildPipelineFromProcessors(s.config.Processors)
	}

	// Convert proto schema if provided
	var sch *schema.Schema
	if req.Schema != nil && len(req.Schema.Columns) > 0 {
		sch, err = protoToSchema(req.Schema)
		if err != nil {
			return nil, fmt.Errorf("invalid schema: %w", err)
		}
	}

	// Resolve partition spec: from config, control file, or unpartitioned.
	// `tableExists` was set by the lakekeeper resolver (or stays false
	// for test backends / schema-deferred opens).
	var partFields []PartitionFieldConfig
	var partFieldDefs []manifest.PartitionFieldDef

	// Check if output_tables config has partition_fields for this table
	for _, t := range s.config.OutputTables {
		if t.Name == table && len(t.PartitionFields) > 0 {
			partFields = t.PartitionFields
			break
		}
	}

	if len(partFields) > 0 {
		// Pipeline config provides partition spec
		partFieldDefs = make([]manifest.PartitionFieldDef, len(partFields))
		for i, pf := range partFields {
			partFieldDefs[i] = manifest.PartitionFieldDef{
				SourceColumn: pf.SourceColumn,
				Transform:    pf.Transform,
				FieldName:    controlfile.DeriveFieldName(pf.SourceColumn, pf.Transform),
			}
		}
	} else if tableExists {
		// No pipeline spec — try to read the legacy `_datuplet_tableinfo.json`
		// control file. Older tables carry partition spec there; lakekeeper-
		// managed tables don't (partition spec lives in iceberg metadata
		// read on LoadTable). A missing file is expected for the new path;
		// log and proceed unpartitioned. (TODO: once legacy tables are gone,
		// delete this branch and source partition spec from iceberg metadata.)
		tableBasePath := strings.TrimSuffix(basePath, "data/")
		info, err := readControlFile(ctx, writerBackend, tableBasePath)
		if err != nil {
			log.Printf("server_v2: no control file at %s (lakekeeper-managed table; proceeding unpartitioned): %v", tableBasePath, err)
		} else if len(info.PartitionSpec) > 0 {
			partFields = make([]PartitionFieldConfig, len(info.PartitionSpec))
			partFieldDefs = make([]manifest.PartitionFieldDef, len(info.PartitionSpec))
			for i, pf := range info.PartitionSpec {
				partFields[i] = PartitionFieldConfig{
					SourceColumn: pf.SourceColumn,
					Transform:    pf.Transform,
				}
				partFieldDefs[i] = manifest.PartitionFieldDef{
					SourceColumn: pf.SourceColumn,
					Transform:    pf.Transform,
					FieldName:    pf.FieldName,
				}
			}
		}
		// else: control file has empty partition_spec → unpartitioned (proceed normally)
	}
	// else: no pipeline spec + table doesn't exist → unpartitioned

	// Generate writer ID and store state
	s.mu.Lock()
	s.writerCounter++
	writerID := fmt.Sprintf("w%d", s.writerCounter)
	s.writers[writerID] = &writerState{
		writerID:           writerID,
		outputName:         table, // Use table name as output name
		bucket:             bucket,
		table:              table,
		basePath:           basePath,
		inputFormat:        inputFormat,
		adapter:            adapter,
		pipeline:           pipeline,
		schema:             sch,
		schemaInferred:     sch == nil,
		writerBackend:      writerBackend, // Per-writer backend (vended-creds-backed minio or static)
		partitionFields:    partFields,
		partitionFieldDefs: partFieldDefs,
		tableExists:        tableExists,
	}
	s.mu.Unlock()

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
	return s.processWriteChunk(ctx, ws, req.Data)
}

func (s *ServerV2) CloseWriter(ctx context.Context, req *pb.CloseWriterRequest) (*pb.CloseWriterResponse, error) {
	s.mu.Lock()
	ws, ok := s.writers[req.WriterId]
	// Don't delete writer from map here - Commit needs to process it later
	// The writer will be removed during Commit after schema/manifest are written
	s.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("unknown writer: %s", req.WriterId)
	}

	extFilesCount := len(req.GetExternalFiles())

	// Check if component provided external files (e.g., DuckDB writing directly to S3)
	if extFilesCount > 0 {
		log.Printf("CloseWriter: writerID=%s external_files=%d bucket=%s table=%s", req.WriterId, extFilesCount, ws.bucket, ws.table)
		// Reject external files for partitioned tables
		if len(ws.partitionFields) > 0 {
			return nil, fmt.Errorf("external files not supported for partitioned tables")
		}

		// Convert external files to internal format
		ws.externalFiles = make([]buffer.FileInfo, len(req.ExternalFiles))
		var totalRows int64
		var totalBytes int64
		for i, ef := range req.ExternalFiles {
			// If the caller already supplies an absolute URL (contains "://"),
			// use it verbatim — the file lives at a different location than the
			// production table prefix (e.g. a workspace prefix for sql-transform).
			// For relative paths, join with basePath as before.
			var fullPath string
			if strings.Contains(ef.Path, "://") {
				fullPath = ef.Path
			} else {
				fullPath = joinStoragePath(ws.basePath, ef.Path)
			}
			ws.externalFiles[i] = buffer.FileInfo{
				Path:      fullPath,
				RowCount:  ef.RowCount,
				SizeBytes: ef.SizeBytes,
			}
			totalRows += ef.RowCount
			totalBytes += ef.SizeBytes
		}

		ws.totalRows = totalRows
		ws.totalBytes = totalBytes

		// Dispatch inline commit for external-files writers. The closed
		// flag is intentionally NOT set here — external-files writers have
		// no buffer/router to close, so the Commit sweep's needClose check
		// is already a no-op for them.
		if err := s.finalizeAndDispatch(ctx, ws, s.config.GetRunID()); err != nil {
			return nil, err
		}

		return &pb.CloseWriterResponse{
			TotalRows:    totalRows,
			TotalBytes:   totalBytes,
			FilesWritten: int32(len(req.ExternalFiles)),
		}, nil
	}

	// Standard flow: close buffer/router (but keep writer in map for Commit to process)
	var filesWritten int32
	if ws.partitionRouter != nil {
		if err := ws.partitionRouter.Close(); err != nil {
			return nil, fmt.Errorf("failed to close partition router: %w", err)
		}
		stats := ws.partitionRouter.Stats()
		filesWritten = int32(stats.TotalFiles)
		ws.totalBytes = stats.TotalBytesFlushed
	} else if ws.bufferMgr != nil {
		if err := ws.bufferMgr.Close(); err != nil {
			return nil, fmt.Errorf("failed to close buffer: %w", err)
		}
		stats := ws.bufferMgr.Stats()
		filesWritten = int32(stats.TotalFiles)
		ws.totalBytes = stats.TotalBytesFlushed
	}

	// Mark buffer/router as closed so the Commit sweep doesn't attempt a
	// second Close (which is not guaranteed idempotent).
	s.mu.Lock()
	ws.closed = true
	s.mu.Unlock()

	// Dispatch inline commit for the now-closed buffer/router.
	if err := s.finalizeAndDispatch(ctx, ws, s.config.GetRunID()); err != nil {
		return nil, err
	}

	return &pb.CloseWriterResponse{
		TotalRows:    ws.totalRows,
		TotalBytes:   ws.totalBytes,
		FilesWritten: filesWritten,
	}, nil
}

// finalizeAndDispatch collects a closed writer's parquet paths, writes the
// schema/manifest + files.json breadcrumb, then dispatches the iceberg commit
// to the pool. The writer's buffer/router must already be closed before calling
// this for standard writers; for external-files writers the buffer/router is
// never opened, so there is nothing to close first.
//
// Claims the writer under s.mu (sets ws.committed) then releases the lock for
// all I/O. MUST be called WITHOUT s.mu held. No-op when the writer is already
// claimed. When s.commitPool is nil (test/static-backend mode) the writer is
// still claimed and metadata is written, but no commit is dispatched.
func (s *ServerV2) finalizeAndDispatch(ctx context.Context, ws *writerState, runID string) error {
	s.mu.Lock()
	if ws.committed {
		s.mu.Unlock()
		return nil
	}
	ws.committed = true
	s.mu.Unlock()

	var paths []string
	switch {
	case ws.partitionRouter != nil:
		for _, pf := range ws.partitionRouter.FilesWritten() {
			paths = append(paths, pf.FileInfo.Path)
		}
	case ws.bufferMgr != nil:
		for _, f := range ws.bufferMgr.FilesWritten() {
			paths = append(paths, f.Path)
		}
	case len(ws.externalFiles) > 0:
		for _, f := range ws.externalFiles {
			paths = append(paths, f.Path)
		}
	}

	if len(ws.externalFiles) > 0 {
		if ws.schema == nil {
			return fmt.Errorf("external files but no schema for %s.%s", ws.bucket, ws.table)
		}
		if err := s.patchParquetFieldIDs(ctx, ws); err != nil {
			return fmt.Errorf("patch parquet field IDs %s.%s: %w", ws.bucket, ws.table, err)
		}
	}

	if ws.schema != nil && len(paths) > 0 {
		if err := s.writeSchemaAndManifest(ctx, ws, runID); err != nil {
			if len(ws.externalFiles) > 0 {
				return fmt.Errorf("write manifest %s.%s: %w", ws.bucket, ws.table, err)
			}
			log.Printf("Warning: schema/manifest write failed for %s.%s: %v", ws.bucket, ws.table, err)
		}
	}
	if ws.writerBackend != nil {
		for _, p := range paths {
			s.filesManifest.Append(ws.bucket, ws.table, p)
		}
		if err := s.persistTableManifest(ctx, ws.writerBackend, ws.basePath, ws.bucket, ws.table, runID); err != nil {
			log.Printf("Warning: files.json breadcrumb write failed for %s.%s: %v", ws.bucket, ws.table, err)
		}
	}

	if s.commitPool == nil {
		return nil
	}
	if len(paths) == 0 {
		return nil
	}
	mode := icebergjob.WriteModeAppend
	if m := s.writeModeForTable(ws.bucket, ws.table); m != "" {
		mode = m
	}
	if err := s.commitPool.Dispatch(ctx, CommitJob{
		WriterID: ws.writerID, Namespace: ws.bucket, Table: ws.table,
		DataPaths: paths, Mode: mode, RunID: runID,
	}); err != nil {
		return fmt.Errorf("dispatch commit %s.%s: %w", ws.bucket, ws.table, err)
	}
	log.Printf("dispatched inline commit: writer=%s table=%s.%s files=%d mode=%s", ws.writerID, ws.bucket, ws.table, len(paths), mode)
	return nil
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
