package datagateway

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
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
		if _, cerr := s.closeTableWriter(ctx, ws); cerr != nil {
			if sweepErr == nil {
				sweepErr = fmt.Errorf("sweep close %s.%s: %w", ws.bucket, ws.table, cerr)
			}
			continue
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
			// Log gateway-side as well as returning it. The RPC response
			// is the component's only copy, and a component that drops it
			// (or dies before printing it) would otherwise leave no trace
			// of WHY the commit failed anywhere in the pod's logs.
			log.Printf("ERROR: commit failed for %s.%s (writer=%s): %v",
				r.Namespace, r.Table, r.WriterID, r.Err)
		case r.DataFilesAdded == 0 && r.SnapshotIDAfter == "":
			tcr.Status = pb.TableCommitResult_STATUS_SKIPPED
			// A skipped table is indistinguishable from a committed one in an
			// unlogged run, which reads as "data was written" when none was.
			log.Printf("commit skipped for %s.%s (writer=%s): no data files to commit",
				r.Namespace, r.Table, r.WriterID)
		default:
			tcr.Status = pb.TableCommitResult_STATUS_COMMITTED
			tcr.FilesAdded = int32(r.DataFilesAdded)
			if ws, ok := expected[r.WriterID]; ok {
				tcr.RowsAdded = writerRowsWritten(ws)
			}
			// Close the lifecycle that "dispatched inline commit" opens.
			// Without this, committed / skipped / still-in-flight are
			// indistinguishable in the gateway log.
			idem := ""
			if r.IdempotencyHit {
				idem = " (idempotency hit — already committed)"
			}
			log.Printf("commit ok for %s.%s (writer=%s): files=%d rows=%d snapshot=%s→%s duration=%s%s",
				r.Namespace, r.Table, r.WriterID, r.DataFilesAdded, tcr.RowsAdded,
				snapshotOrNone(r.SnapshotIDBefore), snapshotOrNone(r.SnapshotIDAfter),
				r.Duration.Round(time.Millisecond), idem)
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
			// Nil-pool: no iceberg commit happens, which is expected in
			// test mode.
			bucketTables[ws.bucket] = append(bucketTables[ws.bucket], &pb.TableCommitResult{
				Table: ws.table, Bucket: ws.bucket,
				Status:    pb.TableCommitResult_STATUS_COMMITTED,
				RowsAdded: writerRowsWritten(ws),
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

	// Surface sweepErr to the caller as well as logging it. It sets
	// allSuccess=false, so without it in the response the component sees a
	// failed commit carrying no per-table error to explain it.
	resp := &pb.CommitResponse{Success: allSuccess, Buckets: buckets}
	if sweepErr != nil {
		log.Printf("ERROR: commit sweep error: %v", sweepErr)
		resp.Error = sweepErr.Error()
	}
	return resp, nil
}

// snapshotOrNone renders an empty snapshot id as "none" so the
// before→after transition in the commit log reads unambiguously on a
// first-ever commit (where "before" is legitimately absent).
func snapshotOrNone(id string) string {
	if id == "" {
		return "none"
	}
	return id
}

// writerProducedFiles reports whether the writer wrote any parquet files.
func writerProducedFiles(ws *writerState) bool {
	return len(ws.files) > 0
}

// writerRowsWritten sums the row counts across every parquet file the writer
// produced — the number of rows the commit actually persisted. Reporting-only:
// surfaced as TableCommitResult.RowsAdded so the gateway log and component
// status message read honestly. The iceberg commit itself works by file path
// and never sees a row count.
func writerRowsWritten(ws *writerState) int64 {
	var rows int64
	for _, f := range ws.files {
		rows += f.Rows
	}
	return rows
}
