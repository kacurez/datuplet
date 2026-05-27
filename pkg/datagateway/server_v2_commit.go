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

// Commit is the session-level barrier. CloseWriter already finalizes and
// dispatches each writer's iceberg commit to the pool; Commit drains the
// pool and reconciles results. Writers that were not yet closed at Commit
// time (e.g. the session was aborted mid-stream) are swept here as a
// defensive fallback.
func (s *ServerV2) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	runID := s.config.GetRunID()

	// Snapshot the writer map under lock; release before any I/O so we
	// don't hold s.mu across storage calls.
	s.mu.Lock()
	expected := make(map[string]*writerState, len(s.writers))
	sweepList := make([]*writerState, 0, len(s.writers))
	for id, ws := range s.writers {
		expected[id] = ws
		if !ws.committed {
			sweepList = append(sweepList, ws)
		}
	}
	s.mu.Unlock()

	// Defensive sweep: close + finalize any writers that CloseWriter
	// didn't process (e.g. abandoned stream).
	var sweepErr error
	for _, ws := range sweepList {
		s.mu.Lock()
		needClose := !ws.closed && (ws.partitionRouter != nil || ws.bufferMgr != nil)
		if needClose {
			ws.closed = true
		}
		s.mu.Unlock()

		if needClose {
			var cerr error
			if ws.partitionRouter != nil {
				cerr = ws.partitionRouter.Close()
			} else if ws.bufferMgr != nil {
				cerr = ws.bufferMgr.Close()
			}
			if cerr != nil {
				if sweepErr == nil {
					sweepErr = fmt.Errorf("sweep close %s.%s: %w", ws.bucket, ws.table, cerr)
				}
				continue
			}
		}
		if err := s.finalizeAndDispatch(ctx, ws, runID); err != nil && sweepErr == nil {
			sweepErr = err
		}
	}

	// Block until all dispatched commits finish.
	var poolResults []CommitResult
	if s.commitPool != nil {
		poolResults = s.commitPool.Wait(ctx)
	}

	// Build response table results from pool outcomes.
	bucketTables := make(map[string][]*pb.TableCommitResult)
	seen := make(map[string]bool)
	allSuccess := sweepErr == nil

	for _, r := range poolResults {
		seen[r.WriterID] = true
		tcr := &pb.TableCommitResult{Table: r.Table, Bucket: r.Namespace}
		switch {
		case r.Err != nil:
			tcr.Status = pb.TableCommitResult_STATUS_FAILED
			tcr.Error = r.Err.Error()
			allSuccess = false
		case r.DataFilesAdded == 0 && r.SnapshotIDAfter == "":
			tcr.Status = pb.TableCommitResult_STATUS_SKIPPED
		default:
			tcr.Status = pb.TableCommitResult_STATUS_COMMITTED
			tcr.FilesAdded = int32(r.DataFilesAdded)
		}
		bucketTables[r.Namespace] = append(bucketTables[r.Namespace], tcr)
	}

	// Reconciliation: handle writers not in the pool results.
	// In nil-pool (test/static-backend) mode no commit is dispatched, so
	// writers that produced files legitimately have no pool result →
	// report COMMITTED. Writers that produced no files → SKIPPED.
	// In pool mode, an absent result is a bug → FAILED.
	for id, ws := range expected {
		if seen[id] {
			continue
		}
		switch {
		case !writerProducedFiles(ws):
			bucketTables[ws.bucket] = append(bucketTables[ws.bucket], &pb.TableCommitResult{
				Table: ws.table, Bucket: ws.bucket,
				Status: pb.TableCommitResult_STATUS_SKIPPED,
			})
		case s.commitPool == nil:
			// Nil-pool: metadata was written by finalizeAndDispatch; no
			// iceberg commit happens, which is expected in test mode.
			bucketTables[ws.bucket] = append(bucketTables[ws.bucket], &pb.TableCommitResult{
				Table: ws.table, Bucket: ws.bucket,
				Status: pb.TableCommitResult_STATUS_COMMITTED,
			})
		default:
			// Pool is live but this writer has no result — should not happen.
			bucketTables[ws.bucket] = append(bucketTables[ws.bucket], &pb.TableCommitResult{
				Table: ws.table, Bucket: ws.bucket,
				Status: pb.TableCommitResult_STATUS_FAILED,
				Error:  "commit not dispatched (reconciliation failure)",
			})
			allSuccess = false
		}
	}

	// N5 (documented assumption): one session per sidecar, no concurrent
	// second session between barrier and clear. Clear writers map so a
	// future defensive sweep in a next Commit finds nothing.
	s.mu.Lock()
	s.writers = make(map[string]*writerState)
	s.mu.Unlock()

	// Build bucket-level results.
	buckets := make([]*pb.BucketCommitResult, 0, len(bucketTables))
	for bucket, tables := range bucketTables {
		st := pb.BucketCommitResult_STATUS_COMMITTED
		for _, t := range tables {
			if t.Status == pb.TableCommitResult_STATUS_FAILED {
				st = pb.BucketCommitResult_STATUS_FAILED
				break
			}
		}
		buckets = append(buckets, &pb.BucketCommitResult{Bucket: bucket, Status: st, Tables: tables})
	}

	if sweepErr != nil {
		log.Printf("ERROR: commit sweep error: %v", sweepErr)
	}
	return &pb.CommitResponse{Success: allSuccess, Buckets: buckets}, nil
}

// writerProducedFiles reports whether the writer wrote any parquet files.
func writerProducedFiles(ws *writerState) bool {
	switch {
	case ws.partitionRouter != nil:
		return len(ws.partitionRouter.FilesWritten()) > 0
	case ws.bufferMgr != nil:
		return len(ws.bufferMgr.FilesWritten()) > 0
	default:
		return len(ws.externalFiles) > 0
	}
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
