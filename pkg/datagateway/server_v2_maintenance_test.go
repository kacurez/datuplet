package datagateway

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

func maintenanceServer(maintenance bool, cfg Config) *ServerV2 {
	cfg.RunID = "run-1"
	cfg.ComponentName = "maint-a"
	cfg.TableMaintenance = maintenance
	return &ServerV2{config: &cfg}
}

// maintenanceCalls invokes every maintenance RPC on the declared raw.events
// table (or the given bucket/table) and returns the three errors.
func maintenanceCalls(s *ServerV2, bucket, table string) map[string]error {
	ctx := context.Background()
	out := map[string]error{}
	_, out["rewrite"] = s.RewriteManifests(ctx, &pb.RewriteManifestsRequest{Bucket: bucket, Table: table})
	_, out["expire"] = s.ExpireSnapshots(ctx, &pb.ExpireSnapshotsRequest{Bucket: bucket, Table: table})
	_, out["orphans"] = s.DeleteOrphanFiles(ctx, &pb.DeleteOrphanFilesRequest{Bucket: bucket, Table: table})
	return out
}

func TestMaintenanceRPCs_RequireTableMaintenance(t *testing.T) {
	s := maintenanceServer(false, declaredRawEvents())
	for name, err := range maintenanceCalls(s, "raw", "events") {
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: code = %v (%v), want PermissionDenied", name, status.Code(err), err)
		}
	}
}

func TestMaintenanceRPCs_RequireDeclaredTable(t *testing.T) {
	for name, cfg := range map[string]Config{
		"otherTable":    declaredRawEvents(),
		"bucketOnly":    {OutputBuckets: []string{"raw"}},
		"defaultBucket": {DefaultBucket: "raw", OutputBuckets: []string{"raw"}},
	} {
		s := maintenanceServer(true, cfg)
		table := "events"
		if name == "otherTable" {
			table = "other"
		}
		for rpc, err := range maintenanceCalls(s, "raw", table) {
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("%s/%s: code = %v (%v), want PermissionDenied", name, rpc, status.Code(err), err)
			}
		}
	}
}

func TestMaintenanceRPCs_EmptyTargetIsInvalidArgument(t *testing.T) {
	s := maintenanceServer(true, declaredRawEvents())
	for rpc, err := range maintenanceCalls(s, "", "") {
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: code = %v (%v), want InvalidArgument", rpc, status.Code(err), err)
		}
	}
}

func TestMaintenanceRPCs_UnwiredIsUnimplemented(t *testing.T) {
	s := maintenanceServer(true, declaredRawEvents())
	for rpc, err := range maintenanceCalls(s, "raw", "events") {
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("%s: code = %v (%v), want Unimplemented", rpc, status.Code(err), err)
		}
	}
}

func TestMaintenanceStatusError_RefusedIsFailedPrecondition(t *testing.T) {
	cases := map[string]struct {
		err  error
		want codes.Code
	}{
		"refused":      {fmt.Errorf("keep-set guard: %w", icebergjob.ErrRefused), codes.FailedPrecondition},
		"noSuchTable":  {catalog.ErrNoSuchTable, codes.NotFound},
		"unverifiable": {icebergjob.ErrHistoryUnverifiable, codes.Internal},
		"other":        {errors.New("boom"), codes.Internal},
	}
	for name, tc := range cases {
		if got := status.Code(maintenanceStatusError("expire", "raw", "events", tc.err)); got != tc.want {
			t.Errorf("%s: code = %v, want %v", name, got, tc.want)
		}
	}
}

func TestMaintenanceRPCs_SeamFailureMapsRefused(t *testing.T) {
	s := maintenanceServer(true, declaredRawEvents())
	s.rewriteManifestsFn = func(context.Context, icebergtable.Identifier, icebergjob.RewriteManifestsOpts) (*icebergjob.RewriteManifestsResult, error) {
		return nil, fmt.Errorf("gc off: %w", icebergjob.ErrRefused)
	}
	_, err := s.RewriteManifests(context.Background(), &pb.RewriteManifestsRequest{Bucket: "raw", Table: "events"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v (%v), want FailedPrecondition", status.Code(err), err)
	}
}

func TestMaintenanceRPCs_DefaultsAndFloors(t *testing.T) {
	ctx := context.Background()
	s := maintenanceServer(true, declaredRawEvents())
	var rw icebergjob.RewriteManifestsOpts
	var ex icebergjob.ExpireOpts
	var or icebergjob.OrphanOpts
	s.rewriteManifestsFn = func(_ context.Context, _ icebergtable.Identifier, o icebergjob.RewriteManifestsOpts) (*icebergjob.RewriteManifestsResult, error) {
		rw = o
		return &icebergjob.RewriteManifestsResult{Status: "REWRITTEN"}, nil
	}
	s.expireFn = func(_ context.Context, _ icebergtable.Identifier, o icebergjob.ExpireOpts) (*icebergjob.ExpireResult, error) {
		ex = o
		return &icebergjob.ExpireResult{Status: "EXPIRED"}, nil
	}
	s.orphansFn = func(_ context.Context, _ icebergtable.Identifier, o icebergjob.OrphanOpts) (*icebergjob.OrphanResult, error) {
		or = o
		return &icebergjob.OrphanResult{Status: "DELETED"}, nil
	}
	if _, err := s.RewriteManifests(ctx, &pb.RewriteManifestsRequest{Bucket: "raw", Table: "events"}); err != nil || rw.TargetManifestSizeBytes != 8<<20 {
		t.Errorf("rewrite default: err=%v opts=%+v", err, rw)
	}
	if _, err := s.ExpireSnapshots(ctx, &pb.ExpireSnapshotsRequest{Bucket: "raw", Table: "events"}); err != nil || ex.OlderThan != 168*time.Hour || ex.RetainLast != 10 {
		t.Errorf("expire defaults: err=%v opts=%+v", err, ex)
	}
	if _, err := s.DeleteOrphanFiles(ctx, &pb.DeleteOrphanFilesRequest{Bucket: "raw", Table: "events"}); err != nil || or.OlderThan != 72*time.Hour || or.MaxFiles != 10000 {
		t.Errorf("orphan defaults: err=%v opts=%+v", err, or)
	}

	for name, req := range map[string]*pb.ExpireSnapshotsRequest{
		"olderThan23":    {Bucket: "raw", Table: "events", OlderThanHours: 23},
		"negativeRetain": {Bucket: "raw", Table: "events", RetainLast: -1},
	} {
		if _, err := s.ExpireSnapshots(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("expire %s: code = %v, want InvalidArgument", name, status.Code(err))
		}
	}
	if _, err := s.ExpireSnapshots(ctx, &pb.ExpireSnapshotsRequest{Bucket: "raw", Table: "events", OlderThanHours: 24, RetainLast: 1}); err != nil {
		t.Errorf("expire at floor: %v", err)
	}
	if _, err := s.DeleteOrphanFiles(ctx, &pb.DeleteOrphanFilesRequest{Bucket: "raw", Table: "events", OlderThanHours: 24}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("orphans 24h: code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := s.DeleteOrphanFiles(ctx, &pb.DeleteOrphanFilesRequest{Bucket: "raw", Table: "events", OlderThanHours: 25}); err != nil {
		t.Errorf("orphans at floor: %v", err)
	}
}

func TestMaintenanceRPCs_UpperBoundAndNegatives(t *testing.T) {
	ctx := context.Background()
	s := maintenanceServer(true, declaredRawEvents())
	s.rewriteManifestsFn = func(context.Context, icebergtable.Identifier, icebergjob.RewriteManifestsOpts) (*icebergjob.RewriteManifestsResult, error) {
		return &icebergjob.RewriteManifestsResult{Status: "REWRITTEN"}, nil
	}
	s.expireFn = func(context.Context, icebergtable.Identifier, icebergjob.ExpireOpts) (*icebergjob.ExpireResult, error) {
		return &icebergjob.ExpireResult{Status: "EXPIRED"}, nil
	}
	s.orphansFn = func(context.Context, icebergtable.Identifier, icebergjob.OrphanOpts) (*icebergjob.OrphanResult, error) {
		return &icebergjob.OrphanResult{Status: "DELETED"}, nil
	}
	for _, tc := range []struct {
		hours int32
		want  codes.Code
	}{{87600, codes.OK}, {87601, codes.InvalidArgument}, {1 << 30, codes.InvalidArgument}} {
		_, err := s.ExpireSnapshots(ctx, &pb.ExpireSnapshotsRequest{Bucket: "raw", Table: "events", OlderThanHours: tc.hours})
		if status.Code(err) != tc.want {
			t.Errorf("expire hours=%d: code = %v, want %v", tc.hours, status.Code(err), tc.want)
		}
		_, err = s.DeleteOrphanFiles(ctx, &pb.DeleteOrphanFilesRequest{Bucket: "raw", Table: "events", OlderThanHours: tc.hours})
		if status.Code(err) != tc.want {
			t.Errorf("orphans hours=%d: code = %v, want %v", tc.hours, status.Code(err), tc.want)
		}
	}
	if _, err := s.DeleteOrphanFiles(ctx, &pb.DeleteOrphanFilesRequest{Bucket: "raw", Table: "events", MaxFiles: -1}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("negative max_files: code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := s.RewriteManifests(ctx, &pb.RewriteManifestsRequest{Bucket: "raw", Table: "events", TargetManifestSizeBytes: -1}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("negative target size: code = %v, want InvalidArgument", status.Code(err))
	}
}
