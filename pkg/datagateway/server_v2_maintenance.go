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

type (
	rewriteManifestsFunc func(ctx context.Context, ident icebergtable.Identifier, opts icebergjob.RewriteManifestsOpts) (*icebergjob.RewriteManifestsResult, error)
	expireFunc           func(ctx context.Context, ident icebergtable.Identifier, opts icebergjob.ExpireOpts) (*icebergjob.ExpireResult, error)
	orphansFunc          func(ctx context.Context, ident icebergtable.Identifier, opts icebergjob.OrphanOpts) (*icebergjob.OrphanResult, error)
)

const (
	defaultManifestTargetBytes = 8 << 20
	defaultExpireOlderThanH    = 168
	defaultExpireRetainLast    = 10
	minExpireOlderThanH        = 24
	defaultOrphanOlderThanH    = 72
	defaultOrphanMaxFiles      = 10000
	minOrphanOlderThanH        = 25
)

// maintenancePreflight gates the maintenance RPCs: they are served only to
// iceberg-maintenance stages (table_maintenance, set by the controller) and
// only for explicitly declared output tables.
func (s *ServerV2) maintenancePreflight(bucket, table string) error {
	if bucket == "" || table == "" {
		return status.Error(codes.InvalidArgument, "maintenance: bucket and table are required")
	}
	if !s.config.TableMaintenance {
		return status.Error(codes.PermissionDenied, "maintenance RPCs are only served to iceberg-maintenance stages")
	}
	if !s.isDeclaredOutputTable(bucket, table) {
		return status.Errorf(codes.PermissionDenied, "maintenance %s.%s: not an explicitly declared output table of this component", bucket, table)
	}
	if s.isCancelled() {
		return cancelledStatus(errRunCancelled)
	}
	return nil
}

// maintenanceLifetime is the context maintenance work runs on: the server's
// lifetime context, so a client disconnect does not abort a commit in flight.
func (s *ServerV2) maintenanceLifetime() context.Context {
	if s.cancelCtx != nil {
		return s.cancelCtx
	}
	return context.Background()
}

// maintenanceFailed records the failure metrics and maps the error.
func (s *ServerV2) maintenanceFailed(task, bucket, table string, elapsed time.Duration, err error) error {
	maintenanceRuns.WithLabelValues(task, "FAILED").Inc()
	maintenanceDuration.WithLabelValues(task, "FAILED").Observe(elapsed.Seconds())
	log.Printf("ERROR: maintenance %s %s.%s (component=%s) failed after %s: %v", task, bucket, table, s.config.ComponentName, elapsed.Round(time.Millisecond), err)
	if s.isCancelled() {
		return cancelledStatus(fmt.Errorf("%w: %v", errRunCancelled, err))
	}
	return maintenanceStatusError(task, bucket, table, err)
}

func maintenanceOK(task, bucket, table, st string, elapsed time.Duration, files int) {
	maintenanceRuns.WithLabelValues(task, st).Inc()
	maintenanceDuration.WithLabelValues(task, st).Observe(elapsed.Seconds())
	if files > 0 {
		maintenanceFiles.WithLabelValues(task, st).Add(float64(files))
	}
}

func (s *ServerV2) RewriteManifests(_ context.Context, req *pb.RewriteManifestsRequest) (*pb.RewriteManifestsResponse, error) {
	const task = "rewrite_manifests"
	bucket, table := req.GetBucket(), req.GetTable()
	if err := s.maintenancePreflight(bucket, table); err != nil {
		return nil, err
	}
	if req.GetTargetManifestSizeBytes() < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "%s %s.%s: target_manifest_size_bytes must not be negative", task, bucket, table)
	}
	if s.rewriteManifestsFn == nil {
		return nil, status.Errorf(codes.Unimplemented, "%s %s.%s: not implemented", task, bucket, table)
	}
	opts := icebergjob.RewriteManifestsOpts{TargetManifestSizeBytes: req.GetTargetManifestSizeBytes(), DryRun: req.GetDryRun()}
	if opts.TargetManifestSizeBytes == 0 {
		opts.TargetManifestSizeBytes = defaultManifestTargetBytes
	}
	start := time.Now()
	res, err := s.rewriteManifestsFn(s.maintenanceLifetime(), icebergtable.Identifier{bucket, table}, opts)
	elapsed := time.Since(start)
	if err != nil {
		return nil, s.maintenanceFailed(task, bucket, table, elapsed, err)
	}
	maintenanceOK(task, bucket, table, res.Status, elapsed, 0)
	log.Printf("maintenance %s %s.%s (component=%s): status=%s manifests=%d->%d snapshot=%d->%d duration=%s",
		task, bucket, table, s.config.ComponentName, res.Status, res.ManifestsBefore, res.ManifestsAfter,
		res.SnapshotBefore, res.SnapshotAfter, res.Duration.Round(time.Millisecond))
	return &pb.RewriteManifestsResponse{
		Status:          res.Status,
		ManifestsBefore: int32(res.ManifestsBefore),
		ManifestsAfter:  int32(res.ManifestsAfter),
		SnapshotBefore:  res.SnapshotBefore,
		SnapshotAfter:   res.SnapshotAfter,
		DurationMs:      res.Duration.Milliseconds(),
	}, nil
}

func (s *ServerV2) ExpireSnapshots(_ context.Context, req *pb.ExpireSnapshotsRequest) (*pb.ExpireSnapshotsResponse, error) {
	const task = "expire_snapshots"
	bucket, table := req.GetBucket(), req.GetTable()
	if err := s.maintenancePreflight(bucket, table); err != nil {
		return nil, err
	}
	hours, retain := req.GetOlderThanHours(), req.GetRetainLast()
	if hours == 0 {
		hours = defaultExpireOlderThanH
	}
	if retain == 0 {
		retain = defaultExpireRetainLast
	}
	if hours < minExpireOlderThanH || retain < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "%s %s.%s: older_than_hours must be >= %d and retain_last >= 1", task, bucket, table, minExpireOlderThanH)
	}
	if s.expireFn == nil {
		return nil, status.Errorf(codes.Unimplemented, "%s %s.%s: not implemented", task, bucket, table)
	}
	opts := icebergjob.ExpireOpts{OlderThan: time.Duration(hours) * time.Hour, RetainLast: int(retain), DryRun: req.GetDryRun()}
	start := time.Now()
	res, err := s.expireFn(s.maintenanceLifetime(), icebergtable.Identifier{bucket, table}, opts)
	elapsed := time.Since(start)
	if err != nil {
		return nil, s.maintenanceFailed(task, bucket, table, elapsed, err)
	}
	maintenanceOK(task, bucket, table, res.Status, elapsed, res.FilesDeleted)
	log.Printf("maintenance %s %s.%s (component=%s): status=%s expired=%d deleted=%d bytes=%d left=%d snapshot=%d duration=%s",
		task, bucket, table, s.config.ComponentName, res.Status, res.SnapshotsExpired, res.FilesDeleted,
		res.BytesDeleted, res.FilesLeft, res.CurrentSnapshot, res.Duration.Round(time.Millisecond))
	return &pb.ExpireSnapshotsResponse{
		Status:           res.Status,
		SnapshotsExpired: int32(res.SnapshotsExpired),
		FilesDeleted:     int32(res.FilesDeleted),
		BytesDeleted:     res.BytesDeleted,
		FilesLeft:        int32(res.FilesLeft),
		CurrentSnapshot:  res.CurrentSnapshot,
		DurationMs:       res.Duration.Milliseconds(),
	}, nil
}

func (s *ServerV2) DeleteOrphanFiles(_ context.Context, req *pb.DeleteOrphanFilesRequest) (*pb.DeleteOrphanFilesResponse, error) {
	const task = "delete_orphan_files"
	bucket, table := req.GetBucket(), req.GetTable()
	if err := s.maintenancePreflight(bucket, table); err != nil {
		return nil, err
	}
	hours, maxFiles := req.GetOlderThanHours(), req.GetMaxFiles()
	if hours == 0 {
		hours = defaultOrphanOlderThanH
	}
	if maxFiles == 0 {
		maxFiles = defaultOrphanMaxFiles
	}
	if hours < minOrphanOlderThanH || maxFiles < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "%s %s.%s: older_than_hours must be >= %d and max_files must not be negative", task, bucket, table, minOrphanOlderThanH)
	}
	if s.orphansFn == nil {
		return nil, status.Errorf(codes.Unimplemented, "%s %s.%s: not implemented", task, bucket, table)
	}
	opts := icebergjob.OrphanOpts{OlderThan: time.Duration(hours) * time.Hour, MaxFiles: int(maxFiles), DryRun: req.GetDryRun()}
	start := time.Now()
	res, err := s.orphansFn(s.maintenanceLifetime(), icebergtable.Identifier{bucket, table}, opts)
	elapsed := time.Since(start)
	if err != nil {
		return nil, s.maintenanceFailed(task, bucket, table, elapsed, err)
	}
	maintenanceOK(task, bucket, table, res.Status, elapsed, res.FilesDeleted)
	log.Printf("maintenance %s %s.%s (component=%s): status=%s orphans=%d bytes=%d deleted=%d duration=%s",
		task, bucket, table, s.config.ComponentName, res.Status, res.OrphanFiles, res.OrphanBytes,
		res.FilesDeleted, res.Duration.Round(time.Millisecond))
	return &pb.DeleteOrphanFilesResponse{
		Status:       res.Status,
		OrphanFiles:  int32(res.OrphanFiles),
		OrphanBytes:  res.OrphanBytes,
		FilesDeleted: int32(res.FilesDeleted),
		DurationMs:   res.Duration.Milliseconds(),
	}, nil
}

// maintenanceStatusError maps a maintenance failure to a gRPC status. Safety
// refusals (icebergjob.ErrRefused) are FailedPrecondition so the component
// exits 1 (user-visible) rather than 20.
func maintenanceStatusError(op, bucket, table string, err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, icebergjob.ErrHistoryUnverifiable), errors.Is(err, icebergjob.ErrCommitOutcomeUnknown):
	case errors.Is(err, catalog.ErrNoSuchTable), errors.Is(err, catalog.ErrNoSuchNamespace):
		code = codes.NotFound
	case errors.Is(err, icebergjob.ErrUnsupportedFile), errors.Is(err, icebergjob.ErrRefused):
		code = codes.FailedPrecondition
	case catalogwriter.IsCommitConflict(err):
		code = codes.Aborted
	}
	return status.Errorf(code, "%s %s.%s: %v", op, bucket, table, err)
}
