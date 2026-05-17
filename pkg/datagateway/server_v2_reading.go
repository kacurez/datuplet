package datagateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	"github.com/datuplet/datuplet/pkg/datagateway/format"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func (s *ServerV2) OpenReader(ctx context.Context, req *pb.OpenReaderRequest) (*pb.OpenReaderResponse, error) {
	// Bucket and table are required for reads
	bucket := req.Bucket
	table := req.Table
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required for reads")
	}
	if table == "" {
		return nil, fmt.Errorf("table is required for reads")
	}

	var backendReader backend.Reader
	var readerBackend backend.StorageBackend
	var sch *schema.Schema
	var totalRows int64
	// lakekeeperDataFiles is the resolved parquet path list when the
	// lakekeeper path was taken; nil for the static-backend path. We
	// stash it here so the FORMAT_ARROW_IPC streaming branch below can
	// reuse it without calling LoadTableForRead a second time (cuts
	// catalog/lock pressure in half on multi-input concurrent OpenReader calls).
	var lakekeeperDataFiles []string
	var deltaInfo *pb.DeltaInfo
	var err error

	// Auto-apply incremental config from gateway config if SDK didn't set it
	if req.Incremental == nil {
		req.Incremental = s.getIncrementalSpecForTable(bucket, table)
	}

	switch {
	case s.lakekeeperResolver != nil:
		// Incremental reads are not yet supported via the lakekeeper-only
		// path. Soft-skip rather than failing so a CRD `sinceDays` setting
		// degrades to a full-snapshot read with a warning instead of
		// bricking the run.
		if req.Incremental != nil {
			log.Printf("warning: incremental reads not yet supported in lakekeeper mode (table %s.%s); falling back to full snapshot", bucket, table)
			req.Incremental = nil
		}
		readerBackend, backendReader, sch, totalRows, lakekeeperDataFiles, err = s.openReaderViaLakekeeper(ctx, bucket, table)
		if err != nil {
			return nil, fmt.Errorf("failed to read via lakekeeper: %w", err)
		}
	default:
		// Static-backend / direct mode (tests).
		if s.backend == nil {
			return nil, fmt.Errorf("no backend available for reading")
		}
		tablePath := fmt.Sprintf("%s/%s", bucket, table)
		backendReader, err = s.backend.OpenReader(ctx, tablePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open reader for %s.%s: %w", bucket, table, err)
		}
		if backendSchema := backendReader.Schema(); backendSchema != nil {
			sch = backendSchemaToGatewaySchema(backendSchema)
		}
	}

	// Get output format adapter
	outputFormat := protoToDataFormat(req.OutputFormat)
	if outputFormat == format.FormatUnknown {
		outputFormat = format.FormatCSV // Default to CSV
	}

	// FORMAT_ARROW_IPC special path: row-group streaming reader, not a generic
	// adapter conversion. Replaces the "io.Copy whole parquet file" reader for
	// arrow consumers (sql-transform). CSV/JSON/parquet pass-through unaffected.
	if outputFormat == format.FormatArrowIPC {
		type streamingBackend interface {
			OpenStreamingArrowReader(ctx context.Context, filePaths []string, currentSchema *backend.SchemaInfo) (backend.Reader, error)
		}
		if readerBackend != nil {
			// Lakekeeper path: reuse the file list resolved earlier
			// (lakekeeperDataFiles) instead of calling LoadTableForRead
			// a second time. Halves catalog round-trips and lock
			// pressure on multi-input concurrent OpenReader calls.
			if sb, ok := readerBackend.(streamingBackend); ok {
				sInfo := gatewaySchemaToBackendSchema(sch)
				if sInfo != nil {
					backendReader.Close()
					// Detach from the OpenReader request ctx: the streaming reader
					// stores this ctx and reuses it later for ranged GETs and
					// arrow record-reader iteration. Once OpenReader returns,
					// the gRPC request ctx is Done — by the time DuckDB's
					// arrow_scan triggers reads the ctx is dead. Reader lifetime
					// is governed by CloseReader / Close(); ctx cancellation
					// is not the right shutdown signal here.
					readerCtx := context.WithoutCancel(ctx)
					var streamErr error
					backendReader, streamErr = sb.OpenStreamingArrowReader(readerCtx, lakekeeperDataFiles, sInfo)
					if streamErr != nil {
						return nil, fmt.Errorf("open streaming reader: %w", streamErr)
					}
				}
			}
		} else if s.backend != nil {
			// Static-backend path (local mode / tests).
			if sb, ok := s.backend.(streamingBackend); ok {
				sInfo := gatewaySchemaToBackendSchema(sch)
				if sInfo == nil {
					// Schema not available from OpenReader; try GetSchema.
					tablePath := fmt.Sprintf("%s/%s", bucket, table)
					if bSch, bErr := s.backend.GetSchema(ctx, tablePath); bErr == nil {
						sInfo = bSch
						// Keep sch in sync so OpenReaderResponse.Schema is non-nil
						// and the SDK's protoSchemaToArrow doesn't error.
						sch = backendSchemaToGatewaySchema(sInfo)
					}
				}
				if sInfo != nil {
					backendReader.Close()
					tablePath := fmt.Sprintf("%s/%s", bucket, table)
					// See note above: detach from the OpenReader request ctx
					// so subsequent reads (DuckDB arrow_scan) don't fail with
					// context canceled.
					readerCtx := context.WithoutCancel(ctx)
					var streamErr error
					backendReader, streamErr = sb.OpenStreamingArrowReader(readerCtx, []string{tablePath}, sInfo)
					if streamErr != nil {
						return nil, fmt.Errorf("open streaming reader (static): %w", streamErr)
					}
				}
			}
		}
	}

	adapter, err := s.registry.Get(outputFormat)
	if err != nil {
		backendReader.Close()
		return nil, fmt.Errorf("unsupported output format: %s", outputFormat)
	}

	// Note: Read-time transforms not yet implemented
	outputSchema := sch

	// Generate reader ID and store state
	s.mu.Lock()
	s.readerCounter++
	readerID := fmt.Sprintf("r-%d", s.readerCounter)
	s.readers[readerID] = &readerState{
		inputName:     fmt.Sprintf("%s.%s", bucket, table),
		tablePath:     fmt.Sprintf("%s/%s", bucket, table),
		backendReader: backendReader,
		adapter:       adapter,
		pipeline:      nil, // Read-time transforms not yet implemented
		schema:        outputSchema,
		readerBackend: readerBackend, // Per-reader backend (nil in static-backend mode)
	}
	s.mu.Unlock()

	// Build HTTP endpoint URL
	httpEndpoint := s.getHTTPEndpoint(fmt.Sprintf("/data/read/%s", readerID))

	return &pb.OpenReaderResponse{
		ReaderId:          readerID,
		Schema:            schemaToProto(outputSchema),
		TotalRowsEstimate: totalRows,
		TotalSizeEstimate: backendReader.TotalSizeEstimate(),
		HttpEndpoint:      httpEndpoint,
		Bucket:            bucket,
		Table:             table,
		DeltaInfo:         deltaInfo,
	}, nil
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

// openReaderViaLakekeeper uses the lakekeeper resolver to get the
// current snapshot's parquet path list + a vended-creds-backed
// MinIO/Local backend, then opens a reader across all files. The
// resolved DataFiles slice is also returned so the FORMAT_ARROW_IPC
// streaming branch in OpenReader can reuse it without a second
// LoadTableForRead round-trip.
func (s *ServerV2) openReaderViaLakekeeper(ctx context.Context, bucket, table string) (backend.StorageBackend, backend.Reader, *schema.Schema, int64, []string, error) {
	rt, err := s.lakekeeperResolver.LoadTableForRead(ctx, bucket, table)
	if err != nil {
		return nil, nil, nil, 0, nil, fmt.Errorf("lakekeeper load %s.%s: %w", bucket, table, err)
	}
	if len(rt.DataFiles) == 0 {
		return nil, nil, nil, 0, nil, fmt.Errorf("no data files in snapshot for table %s.%s", bucket, table)
	}

	log.Printf("lakekeeper returned %d files for table %s.%s", len(rt.DataFiles), bucket, table)

	// OpenReaderForFiles is on both MinIOBackend and LocalBackend but
	// not on the StorageBackend interface. The resolver returns one or
	// the other depending on dataPrefix scheme.
	type fileReader interface {
		OpenReaderForFiles(ctx context.Context, filePaths []string) (backend.Reader, error)
	}
	fr, ok := rt.Backend.(fileReader)
	if !ok {
		return nil, nil, nil, 0, nil, fmt.Errorf("reader backend does not support OpenReaderForFiles")
	}
	reader, err := fr.OpenReaderForFiles(ctx, rt.DataFiles)
	if err != nil {
		return nil, nil, nil, 0, nil, fmt.Errorf("failed to open reader for resolved files: %w", err)
	}

	var sch *schema.Schema
	if len(rt.SchemaJSON) > 0 {
		sch, err = parseIcebergSchemaJSON(rt.SchemaJSON)
		if err != nil {
			log.Printf("Warning: failed to parse schema from lakekeeper: %v", err)
		}
	}
	return rt.Backend, reader, sch, rt.TotalRows, rt.DataFiles, nil
}

// parseIcebergSchemaJSON parses an Iceberg schema JSON to our schema type.
func parseIcebergSchemaJSON(schemaJSON []byte) (*schema.Schema, error) {
	// Iceberg schema JSON format:
	// {"type": "struct", "fields": [{"id": 1, "name": "col", "required": true, "type": "long"}, ...]}
	var icebergSchema struct {
		Fields []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Required bool   `json:"required"`
		} `json:"fields"`
	}

	if err := json.Unmarshal(schemaJSON, &icebergSchema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Iceberg schema: %w", err)
	}

	columns := make([]schema.ColumnDef, len(icebergSchema.Fields))
	for i, field := range icebergSchema.Fields {
		columns[i] = schema.ColumnDef{
			Name:     field.Name,
			Type:     icebergTypeToSchemaType(field.Type),
			Nullable: !field.Required,
		}
	}

	return schema.NewSchema(columns)
}

// icebergTypeToSchemaType converts Iceberg type strings to schema.DataType.
func icebergTypeToSchemaType(icebergType string) schema.DataType {
	switch icebergType {
	case "boolean":
		return schema.TypeBool
	case "int":
		return schema.TypeInt32
	case "long":
		return schema.TypeInt64
	case "float":
		return schema.TypeFloat32
	case "double":
		return schema.TypeFloat64
	case "string":
		return schema.TypeString
	case "date":
		return schema.TypeDate
	case "timestamp", "timestamptz":
		return schema.TypeTimestamp
	case "binary":
		return schema.TypeBinary
	default:
		return schema.TypeString // Default to string for unknown types
	}
}

func (s *ServerV2) ReadChunk(req *pb.ReadChunkRequest, stream pb.DataGateway_ReadChunkServer) error {
	s.mu.RLock()
	rs, ok := s.readers[req.ReaderId]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown reader: %s", req.ReaderId)
	}

	for {
		// Read chunk from backend
		chunk, err := rs.backendReader.ReadChunk()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}

		// Parse backend chunk to Arrow (if needed for transforms)
		// For now, pass through if no transforms
		var outputData []byte
		var rowsInChunk int64

		if rs.pipeline != nil {
			// Parse input, apply transforms, serialize output
			inputFormat := format.ParseDataFormat(chunk.Format)
			inputAdapter, err := s.registry.Get(inputFormat)
			if err != nil {
				return fmt.Errorf("unsupported backend format: %s", chunk.Format)
			}

			record, _, err := inputAdapter.Parse(chunk.Data, rs.schema)
			if err != nil {
				return fmt.Errorf("failed to parse backend data: %w", err)
			}

			// Apply transforms
			transformedRecord, err := rs.pipeline.Apply(record, s.allocator)
			if err != nil {
				record.Release()
				return fmt.Errorf("transform failed: %w", err)
			}
			if transformedRecord != record {
				record.Release()
			}

			// Serialize to output format
			outputData, err = rs.adapter.Serialize(transformedRecord)
			if err != nil {
				transformedRecord.Release()
				return fmt.Errorf("failed to serialize output: %w", err)
			}
			rowsInChunk = transformedRecord.NumRows()
			transformedRecord.Release()
		} else {
			// No transforms - convert format if needed
			inputFormat := format.ParseDataFormat(chunk.Format)
			if inputFormat == rs.adapter.Format() {
				// Same format, pass through
				outputData = chunk.Data
				rowsInChunk = chunk.RowsInChunk
			} else {
				// Convert format
				inputAdapter, err := s.registry.Get(inputFormat)
				if err != nil {
					return fmt.Errorf("unsupported backend format: %s", chunk.Format)
				}

				record, _, err := inputAdapter.Parse(chunk.Data, nil)
				if err != nil {
					return fmt.Errorf("failed to parse backend data: %w", err)
				}

				outputData, err = rs.adapter.Serialize(record)
				if err != nil {
					record.Release()
					return fmt.Errorf("failed to serialize output: %w", err)
				}
				rowsInChunk = record.NumRows()
				record.Release()
			}
		}

		pbChunk := &pb.DataChunk{
			Data:        outputData,
			Format:      dataFormatToProto(rs.adapter.Format()),
			RowsInChunk: rowsInChunk,
			IsLast:      chunk.IsLast,
			Metadata:    chunk.Metadata,
		}

		if err := stream.Send(pbChunk); err != nil {
			return err
		}

		if chunk.IsLast {
			return nil
		}
	}
}

func (s *ServerV2) CloseReader(ctx context.Context, req *pb.CloseReaderRequest) (*pb.CloseReaderResponse, error) {
	s.mu.Lock()
	rs, ok := s.readers[req.ReaderId]
	if ok {
		delete(s.readers, req.ReaderId)
	}
	s.mu.Unlock()

	if ok && rs.backendReader != nil {
		rs.backendReader.Close()
	}

	return &pb.CloseReaderResponse{}, nil
}
