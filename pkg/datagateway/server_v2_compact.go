package datagateway

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

type compactFunc func(ctx context.Context, ident icebergtable.Identifier, opts icebergjob.CompactOpts, operationKey string) (*icebergjob.CompactResult, error)

// CompactTable compacts one table the component declared explicitly in
// output_tables (spec 2026-11-08-table-compaction §5). A bucket-level grant
// (default_bucket, output_buckets) does not authorize it. The work runs on
// the server's lifetime context, not the RPC's, so a client disconnect does
// not abort a commit in flight; the cancel annotation does.
func (s *ServerV2) CompactTable(ctx context.Context, req *pb.CompactTableRequest) (*pb.CompactTableResponse, error) {
	bucket, table := req.GetBucket(), req.GetTable()
	if bucket == "" || table == "" {
		return nil, status.Error(codes.InvalidArgument, "compact: bucket and table are required")
	}
	if req.GetTargetFileSizeBytes() < 0 || req.GetMinInputFiles() < 0 || req.GetMaxInputFiles() < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "compact %s.%s: target_file_size_bytes, min_input_files and max_input_files must not be negative", bucket, table)
	}
	if !s.isDeclaredOutputTable(bucket, table) {
		return nil, status.Errorf(codes.PermissionDenied, "compact %s.%s: not an explicitly declared output table of this component", bucket, table)
	}
	component := s.config.ComponentName
	if component == "" {
		return nil, status.Errorf(codes.Internal, "compact %s.%s: gateway has no component instance name; refusing to derive a shared operation key", bucket, table)
	}
	if s.isCancelled() {
		return nil, errRunCancelled
	}
	if s.compactFn == nil {
		return nil, status.Errorf(codes.Internal, "compact %s.%s: no catalog configured", bucket, table)
	}

	opts := icebergjob.CompactOpts{
		TargetFileSizeBytes: req.GetTargetFileSizeBytes(),
		MinInputFiles:       uint(req.GetMinInputFiles()),
		MaxInputFiles:       int(req.GetMaxInputFiles()),
		DryRun:              req.GetDryRun(),
	}
	if opts.TargetFileSizeBytes == 0 {
		opts.TargetFileSizeBytes = s.config.TargetFileSize
	}
	key := icebergjob.CompactOperationKey(s.config.GetRunID(), component, bucket, table)

	lifetime := s.cancelCtx
	if lifetime == nil {
		lifetime = context.Background()
	}
	start := time.Now()
	res, err := s.compactFn(lifetime, icebergtable.Identifier{bucket, table}, opts, key)
	elapsed := time.Since(start)
	if err != nil {
		compactionRuns.WithLabelValues("FAILED").Inc()
		compactionDuration.WithLabelValues("FAILED").Observe(elapsed.Seconds())
		compactionErrors.WithLabelValues(classifyCommitError(err)).Inc()
		log.Printf("ERROR: compaction %s.%s (component=%s) failed after %s: %v", bucket, table, component, elapsed.Round(time.Millisecond), err)
		if s.isCancelled() {
			return nil, fmt.Errorf("%w: %v", errRunCancelled, err)
		}
		return nil, compactStatusError(bucket, table, err)
	}

	st := string(res.Status)
	compactionRuns.WithLabelValues(st).Inc()
	compactionDuration.WithLabelValues(st).Observe(elapsed.Seconds())
	if res.Status == icebergjob.CompactCompacted {
		compactionFiles.WithLabelValues("removed").Add(float64(res.FilesRemoved))
		compactionFiles.WithLabelValues("added").Add(float64(res.FilesAdded))
	}
	log.Printf("compaction %s.%s (component=%s): status=%s files=%d->%d removed=%d added=%d skipped=%d deferred=%d groups=%d bytes=%d->%d snapshot=%d->%d duration=%s",
		bucket, table, component, st, res.FilesBefore, res.FilesAfter, res.FilesRemoved, res.FilesAdded,
		res.FilesSkipped, res.FilesDeferred, res.Groups, res.BytesBefore, res.BytesAfter,
		res.SnapshotBefore, res.SnapshotAfter, res.Duration.Round(time.Millisecond))

	return &pb.CompactTableResponse{
		Status:         st,
		FilesBefore:    int32(res.FilesBefore),
		FilesRemoved:   int32(res.FilesRemoved),
		FilesAdded:     int32(res.FilesAdded),
		FilesAfter:     int32(res.FilesAfter),
		FilesSkipped:   int32(res.FilesSkipped),
		FilesDeferred:  int32(res.FilesDeferred),
		Groups:         int32(res.Groups),
		BytesBefore:    res.BytesBefore,
		BytesAfter:     res.BytesAfter,
		SnapshotBefore: res.SnapshotBefore,
		SnapshotAfter:  res.SnapshotAfter,
		DurationMs:     res.Duration.Milliseconds(),
	}, nil
}

func (s *ServerV2) isDeclaredOutputTable(bucket, table string) bool {
	for _, t := range s.config.OutputTables {
		if t.Bucket == bucket && t.Name == table {
			return true
		}
	}
	return false
}

func compactStatusError(bucket, table string, err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, catalog.ErrNoSuchTable), errors.Is(err, catalog.ErrNoSuchNamespace):
		code = codes.NotFound
	case errors.Is(err, icebergjob.ErrHistoryUnverifiable), errors.Is(err, icebergjob.ErrCommitOutcomeUnknown):
	case catalogwriter.IsCommitConflict(err):
		code = codes.Aborted
	}
	return status.Errorf(code, "compact %s.%s: %v", bucket, table, err)
}
