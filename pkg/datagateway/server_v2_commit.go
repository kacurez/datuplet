package datagateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	"github.com/datuplet/datuplet/pkg/datagateway/manifest"
	"github.com/datuplet/datuplet/pkg/lib/controlfile"
)

func (s *ServerV2) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runID := s.config.GetRunID()

	// Group writers by bucket for the response
	bucketTables := make(map[string][]*pb.TableCommitResult)
	allSuccess := true

	// Each per-table writer carries its own backend + basePath.
	// Snapshot them into perTableManifest so we can write one files.json
	// per (ns, tbl) AFTER the writer-close loop has finished. Writing
	// per-table inside the loop would couple manifest emission to
	// writer-close ordering; deferring it keeps the close path
	// uncluttered and makes the per-table writes a single well-defined phase.
	type tableManifestTarget struct {
		backend  backend.StorageBackend
		basePath string
		ns       string
		table    string
	}
	var perTableManifest []tableManifestTarget

	for writerID, ws := range s.writers {
		result := &pb.TableCommitResult{
			Table:  ws.table,
			Bucket: ws.bucket,
		}

		if ws.writerBackend != nil {
			perTableManifest = append(perTableManifest, tableManifestTarget{
				backend:  ws.writerBackend,
				basePath: ws.basePath,
				ns:       ws.bucket,
				table:    ws.table,
			})
		}

		// Close buffer/router and write schema/manifest
		if ws.partitionRouter != nil {
			// Partitioned writer flow
			if err := ws.partitionRouter.Close(); err != nil {
				result.Status = pb.TableCommitResult_STATUS_FAILED
				result.Error = err.Error()
				allSuccess = false
			} else {
				stats := ws.partitionRouter.Stats()
				result.Status = pb.TableCommitResult_STATUS_COMMITTED
				result.FilesAdded = int32(stats.TotalFiles)
				result.RowsAdded = stats.TotalRowsFlushed
				result.BytesAdded = stats.TotalBytesFlushed

				// Record paths into the run's files.json manifest.
				// Append before writeSchemaAndManifest so we capture
				// them even if the schema writer fails.
				for _, pf := range ws.partitionRouter.FilesWritten() {
					s.filesManifest.Append(ws.bucket, ws.table, pf.FileInfo.Path)
				}

				if ws.schema != nil {
					if err := s.writeSchemaAndManifest(ctx, ws, runID); err != nil {
						log.Printf("Warning: failed to write schema/manifest for %s.%s: %v", ws.bucket, ws.table, err)
					}
				}
			}
		} else if ws.bufferMgr != nil {
			// Standard unpartitioned flow
			if err := ws.bufferMgr.Close(); err != nil {
				result.Status = pb.TableCommitResult_STATUS_FAILED
				result.Error = err.Error()
				allSuccess = false
			} else {
				stats := ws.bufferMgr.Stats()
				result.Status = pb.TableCommitResult_STATUS_COMMITTED
				result.FilesAdded = int32(stats.TotalFiles)
				result.RowsAdded = stats.TotalRowsFlushed
				result.BytesAdded = stats.TotalBytesFlushed

				// Record paths into the run's files.json manifest.
				for _, f := range ws.bufferMgr.FilesWritten() {
					s.filesManifest.Append(ws.bucket, ws.table, f.Path)
				}

				// Write schema and manifest files
				if ws.schema != nil {
					if err := s.writeSchemaAndManifest(ctx, ws, runID); err != nil {
						log.Printf("Warning: failed to write schema/manifest for %s.%s: %v", ws.bucket, ws.table, err)
						// Don't fail the commit for this - files are already written
					}
				}
			}
		} else if len(ws.externalFiles) > 0 {
			// External files flow (component wrote files directly, e.g., DuckDB)
			result.Status = pb.TableCommitResult_STATUS_COMMITTED
			result.FilesAdded = int32(len(ws.externalFiles))
			result.RowsAdded = ws.totalRows
			result.BytesAdded = ws.totalBytes

			// Record paths into the run's files.json manifest.
			// External files flow through the catalog the same way
			// buffered files do.
			for _, f := range ws.externalFiles {
				s.filesManifest.Append(ws.bucket, ws.table, f.Path)
			}

			// Require schema for manifest generation
			if ws.schema == nil {
				err := fmt.Errorf("external files provided but no schema available for %s.%s", ws.bucket, ws.table)
				result.Status = pb.TableCommitResult_STATUS_FAILED
				result.Error = err.Error()
				allSuccess = false
			} else {
				// Patch Parquet files with Iceberg field IDs (if needed)
				if err := s.patchParquetFieldIDs(ctx, ws); err != nil {
					result.Status = pb.TableCommitResult_STATUS_FAILED
					result.Error = fmt.Sprintf("failed to patch Parquet field IDs: %v", err)
					allSuccess = false
				} else {
					// Only write manifest if patching succeeded
					if err := s.writeSchemaAndManifest(ctx, ws, runID); err != nil {
						result.Status = pb.TableCommitResult_STATUS_FAILED
						result.Error = fmt.Sprintf("failed to write manifest: %v", err)
						allSuccess = false
					}
				}
			}
		} else {
			// No data written
			result.Status = pb.TableCommitResult_STATUS_SKIPPED
		}

		bucketTables[ws.bucket] = append(bucketTables[ws.bucket], result)
		delete(s.writers, writerID)
	}

	// Emit per-table files.json manifests. Each (namespace, table) the
	// run touched gets its own manifest at
	// `<table-base>/.run-state/<run-id>/files.json`, written through
	// the per-writer backend (the same lakekeeper-vended STS creds DG
	// already used for parquet uploads to that table). TableCommit reads
	// each manifest in turn and replays the paths via iceberg-go's
	// `txn.AddFiles(paths, nil, false)`.
	//
	// Failure here is fatal for the run: a successful pipeline that
	// produced parquet but no manifest is unrecoverable downstream
	// (TableCommit cannot find the files). We surface the first error
	// so the SDK propagates it and the component exits non-zero
	// (FailedApplication territory).
	for _, t := range perTableManifest {
		if err := s.persistTableManifest(ctx, t.backend, t.basePath, t.ns, t.table, runID); err != nil {
			log.Printf("ERROR: failed to write files.json manifest for %s.%s: %v", t.ns, t.table, err)
			return nil, fmt.Errorf("write files.json manifest %s.%s: %w", t.ns, t.table, err)
		}
	}

	// Build bucket results
	buckets := make([]*pb.BucketCommitResult, 0, len(bucketTables))
	for bucket, tables := range bucketTables {
		// Determine bucket status from table statuses
		bucketStatus := pb.BucketCommitResult_STATUS_COMMITTED
		for _, t := range tables {
			if t.Status == pb.TableCommitResult_STATUS_FAILED {
				bucketStatus = pb.BucketCommitResult_STATUS_FAILED
				break
			}
		}
		buckets = append(buckets, &pb.BucketCommitResult{
			Bucket: bucket,
			Status: bucketStatus,
			Tables: tables,
		})
	}

	return &pb.CommitResponse{
		Success: allSuccess,
		Buckets: buckets,
	}, nil
}

// writeSchemaAndManifest writes the _schema.json and _manifest.json files for a writer.
// Uses the resolved basePath from lakekeeper (no local path construction).
func (s *ServerV2) writeSchemaAndManifest(ctx context.Context, ws *writerState, runID string) error {
	// Use the resolved base path from lakekeeper (stored in writerState)
	basePath := ws.basePath

	// Get output schema (after transforms if any)
	outputSchema := ws.outputSchema
	if outputSchema == nil {
		outputSchema = ws.schema // Fallback for external files (no processWriteChunk)
	}

	// Convert to Iceberg schema and write _schema.json
	icebergSchema := manifest.SchemaToIceberg(outputSchema)
	// Use per-writer backend if available (lakekeeper / static backend)
	backendForMetadata := s.backend
	if ws.writerBackend != nil {
		backendForMetadata = ws.writerBackend
	}

	tablePath := fmt.Sprintf("%s.%s", ws.bucket, ws.table)
	var schemaBuf bytes.Buffer
	if err := manifest.WriteSchemaFile(&schemaBuf, runID, tablePath, icebergSchema, ws.partitionFieldDefs); err != nil {
		return fmt.Errorf("failed to serialize schema: %w", err)
	}
	schemaPath := joinStoragePath(basePath, fmt.Sprintf("_schema-%s.json", runID))
	if err := backendForMetadata.PutObject(ctx, schemaPath, schemaBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	// Get files written and build manifest entries
	var manifestEntries []manifest.DataFileEntry
	basePathNormalized := strings.TrimSuffix(basePath, "/") + "/"

	if ws.partitionRouter != nil {
		// Partitioned table: collect files with partition values from router
		partFiles := ws.partitionRouter.FilesWritten()
		manifestEntries = make([]manifest.DataFileEntry, len(partFiles))
		for i, pf := range partFiles {
			relativePath := strings.TrimPrefix(pf.FileInfo.Path, basePathNormalized)
			manifestEntries[i] = manifest.DataFileEntry{
				Path:            relativePath,
				RowCount:        pf.FileInfo.RowCount,
				SizeBytes:       pf.FileInfo.SizeBytes,
				PartitionValues: pf.PartitionValues,
			}
		}
	} else if len(ws.externalFiles) > 0 {
		// Component wrote files directly to storage (e.g., DuckDB)
		manifestEntries = make([]manifest.DataFileEntry, len(ws.externalFiles))
		for i, f := range ws.externalFiles {
			relativePath := strings.TrimPrefix(f.Path, basePathNormalized)
			manifestEntries[i] = manifest.DataFileEntry{
				Path:      relativePath,
				RowCount:  f.RowCount,
				SizeBytes: f.SizeBytes,
			}
		}
	} else if ws.bufferMgr != nil {
		// Unpartitioned: files from BufferManager
		filesWritten := ws.bufferMgr.FilesWritten()
		manifestEntries = make([]manifest.DataFileEntry, len(filesWritten))
		for i, f := range filesWritten {
			relativePath := strings.TrimPrefix(f.Path, basePathNormalized)
			manifestEntries[i] = manifest.DataFileEntry{
				Path:      relativePath,
				RowCount:  f.RowCount,
				SizeBytes: f.SizeBytes,
			}
		}
	} else {
		return fmt.Errorf("no files to write (neither external files, partition router, nor buffer manager)")
	}

	if len(manifestEntries) == 0 {
		return fmt.Errorf("no manifest entries generated")
	}

	// Write _manifest.json
	var manifestBuf bytes.Buffer
	if err := manifest.WriteManifestFile(&manifestBuf, runID, tablePath, manifestEntries); err != nil {
		return fmt.Errorf("failed to serialize manifest: %w", err)
	}
	manifestPath := joinStoragePath(basePath, fmt.Sprintf("_manifest-%s.json", runID))

	if err := backendForMetadata.PutObject(ctx, manifestPath, manifestBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write manifest file: %w", err)
	}

	return nil
}


// persistTableManifest writes the per-table files.json under the
// table's own base path. Skip cases (return nil without writing):
//   - manifest is nil (test fixture constructed ServerV2 by hand);
//   - no Append for this (ns, tbl) — the writer produced zero parquet
//     files (extractor with no rows). TableCommit's "missing manifest
//     entry → nothing to commit" branch handles that downstream.
//   - basePath empty / no per-writer backend (test mode).
//
// Returns a hard error only when the JSON upload itself fails — that
// case bubbles up through Commit() so the run exits non-zero, since a
// successful pipeline that produced parquet but no manifest is
// unrecoverable downstream (TableCommit cannot find the files).
//
// The manifest path is derived from the table's basePath via
// ResolveTableManifestPath, which strips the trailing `/data/` to
// recover the table's iceberg-managed prefix.
func (s *ServerV2) persistTableManifest(ctx context.Context, b backend.StorageBackend, basePath, namespace, table, runID string) error {
	if s.filesManifest == nil {
		// Test fixtures sometimes construct ServerV2 by hand without
		// going through NewServerV2 — keep the production path
		// resilient rather than panic.
		return nil
	}
	if b == nil {
		// Test mode: ServerV2 is wired with a mock client and no
		// per-writer backend exists. Log loudly so a real deploy
		// never silently lands here, but don't fail the test.
		log.Printf("Warning: skipping files.json manifest for %s.%s (no per-writer backend available — test mode?)", namespace, table)
		return nil
	}
	manifestPath, ok := ResolveTableManifestPath(basePath, runID)
	if !ok {
		// Empty basePath or runID. In production this can't happen —
		// the lakekeeper resolver always emits a non-empty s3:// or
		// file:// URL. In tests with static backend fixtures it can.
		// Log loudly and skip rather than fail the commit.
		log.Printf("Warning: skipping files.json manifest for %s.%s — cannot derive path from basePath=%q runID=%q", namespace, table, basePath, runID)
		return nil
	}
	wrote, err := s.filesManifest.WriteJSONForTable(ctx, b, namespace, table, manifestPath)
	if err != nil {
		return err
	}
	if !wrote {
		// No Append calls for this writer — extractor produced zero
		// rows. Skip the write; TableCommit's "manifest entry missing
		// or empty Paths → nothing to commit" path handles it.
		log.Printf("skipping files.json manifest for %s.%s (no parquet files produced)", namespace, table)
		return nil
	}
	log.Printf("wrote files.json manifest: path=%s table=%s.%s", manifestPath, namespace, table)
	return nil
}

// readControlFile reads the _datuplet_tableinfo.json control file via the storage backend.
func readControlFile(ctx context.Context, b backend.StorageBackend, tableBasePath string) (*controlfile.TableInfo, error) {
	path := controlfile.ControlFilePath(tableBasePath)
	data, err := b.GetObject(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}
	var info controlfile.TableInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to parse control file: %w", err)
	}
	return &info, nil
}
