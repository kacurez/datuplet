package datagateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

type compactCall struct {
	ident icebergtable.Identifier
	opts  icebergjob.CompactOpts
	key   string
}

func compactServer(component string, cfg Config) (*ServerV2, *[]compactCall) {
	var calls []compactCall
	cfg.RunID = "run-1"
	cfg.ComponentName = component
	if cfg.TargetFileSize == 0 {
		cfg.TargetFileSize = 64 << 20
	}
	s := &ServerV2{config: &cfg}
	s.compactFn = func(_ context.Context, ident icebergtable.Identifier, opts icebergjob.CompactOpts, key string) (*icebergjob.CompactResult, error) {
		calls = append(calls, compactCall{ident: ident, opts: opts, key: key})
		return &icebergjob.CompactResult{Status: icebergjob.CompactCompacted, FilesBefore: 10, FilesRemoved: 8, FilesAdded: 1, FilesAfter: 3}, nil
	}
	return s, &calls
}

func declaredRawEvents() Config {
	return Config{OutputTables: []OutputTableConfig{{Name: "events", Bucket: "raw", WriteMode: "APPEND"}}}
}

func TestCompactTable_RejectsOtherTableInDeclaredBucket(t *testing.T) {
	s, calls := compactServer("compact-a", declaredRawEvents())
	_, err := s.CompactTable(context.Background(), &pb.CompactTableRequest{Bucket: "raw", Table: "other"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v (%v), want PermissionDenied", status.Code(err), err)
	}
	if len(*calls) != 0 {
		t.Fatalf("seam called %d times, want 0", len(*calls))
	}
}

func TestCompactTable_RejectsBucketOnlyDeclaration(t *testing.T) {
	for name, cfg := range map[string]Config{
		"defaultBucket": {DefaultBucket: "raw", OutputBuckets: []string{"raw"}},
		"buckets":       {OutputBuckets: []string{"raw"}},
	} {
		t.Run(name, func(t *testing.T) {
			s, calls := compactServer("compact-a", cfg)
			_, err := s.CompactTable(context.Background(), &pb.CompactTableRequest{Bucket: "raw", Table: "events"})
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("code = %v (%v), want PermissionDenied", status.Code(err), err)
			}
			if len(*calls) != 0 {
				t.Fatalf("seam called %d times, want 0", len(*calls))
			}
		})
	}
}

func TestCompactTable_ExactTargetCallsSeam(t *testing.T) {
	s, calls := compactServer("compact-a", declaredRawEvents())
	resp, err := s.CompactTable(context.Background(), &pb.CompactTableRequest{Bucket: "raw", Table: "events", MinInputFiles: 3, MaxInputFiles: 50, DryRun: true})
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("seam called %d times, want 1", len(*calls))
	}
	c := (*calls)[0]
	if len(c.ident) != 2 || c.ident[0] != "raw" || c.ident[1] != "events" {
		t.Errorf("ident = %v, want [raw events]", c.ident)
	}
	want := icebergjob.CompactOpts{TargetFileSizeBytes: 64 << 20, MinInputFiles: 3, MaxInputFiles: 50, DryRun: true}
	if c.opts != want {
		t.Errorf("opts = %+v, want %+v", c.opts, want)
	}
	if wantKey := icebergjob.CompactOperationKey("run-1", "compact-a", "raw", "events"); c.key != wantKey {
		t.Errorf("key = %q, want %q", c.key, wantKey)
	}
	if resp.Status != "COMPACTED" || resp.FilesBefore != 10 || resp.FilesRemoved != 8 || resp.FilesAdded != 1 || resp.FilesAfter != 3 {
		t.Errorf("response = %+v", resp)
	}

	if _, err := s.CompactTable(context.Background(), &pb.CompactTableRequest{Bucket: "raw", Table: "events", TargetFileSizeBytes: 1 << 20}); err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if got := (*calls)[1].opts.TargetFileSizeBytes; got != 1<<20 {
		t.Errorf("request target size: got %d, want %d", got, 1<<20)
	}
}

func TestCompactTable_DistinctKeysPerComponent(t *testing.T) {
	a, callsA := compactServer("compact-a", declaredRawEvents())
	b, callsB := compactServer("compact-b", declaredRawEvents())
	req := &pb.CompactTableRequest{Bucket: "raw", Table: "events"}
	if _, err := a.CompactTable(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CompactTable(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if (*callsA)[0].key == (*callsB)[0].key {
		t.Fatalf("compact-a and compact-b share operation key %q", (*callsA)[0].key)
	}
}

func TestCompactTable_EmptyComponentName(t *testing.T) {
	s, calls := compactServer("", declaredRawEvents())
	_, err := s.CompactTable(context.Background(), &pb.CompactTableRequest{Bucket: "raw", Table: "events"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v (%v), want Internal", status.Code(err), err)
	}
	if len(*calls) != 0 {
		t.Fatalf("seam called %d times, want 0", len(*calls))
	}
}

func TestCompactTable_InvalidOptions(t *testing.T) {
	for name, req := range map[string]*pb.CompactTableRequest{
		"negative target": {Bucket: "raw", Table: "events", TargetFileSizeBytes: -1},
		"negative min":    {Bucket: "raw", Table: "events", MinInputFiles: -1},
		"negative max":    {Bucket: "raw", Table: "events", MaxInputFiles: -1},
		"empty table":     {Bucket: "raw"},
	} {
		t.Run(name, func(t *testing.T) {
			s, calls := compactServer("compact-a", declaredRawEvents())
			_, err := s.CompactTable(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v (%v), want InvalidArgument", status.Code(err), err)
			}
			if len(*calls) != 0 {
				t.Fatalf("seam called %d times, want 0", len(*calls))
			}
		})
	}
}

func TestCompactTable_ErrorCodes(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want codes.Code
	}{
		"not found":                 {err: fmt.Errorf("CompactTable: load table: %w", catalog.ErrNoSuchTable), want: codes.NotFound},
		"unverifiable":              {err: icebergjob.ErrHistoryUnverifiable, want: codes.Internal},
		"outcome unknown":           {err: icebergjob.ErrCommitOutcomeUnknown, want: codes.Internal},
		"unsupported file":          {err: fmt.Errorf("CompactTable: %w: %w", icebergjob.ErrUnsupportedFile, errors.New("orc: not implemented")), want: codes.FailedPrecondition},
		"unverifiable not found":    {err: fmt.Errorf("CompactTable: %w: reload table: %w", icebergjob.ErrHistoryUnverifiable, catalog.ErrNoSuchTable), want: codes.Internal},
		"outcome unknown not found": {err: fmt.Errorf("CompactTable: %w: %w", icebergjob.ErrCommitOutcomeUnknown, catalog.ErrNoSuchTable), want: codes.Internal},
		"conflict exhausted":        {err: fmt.Errorf("CompactTable: %w", rest.ErrCommitFailed), want: codes.Aborted},
		"other":                     {err: errors.New("boom"), want: codes.Internal},
	} {
		t.Run(name, func(t *testing.T) {
			s, _ := compactServer("compact-a", declaredRawEvents())
			s.compactFn = func(context.Context, icebergtable.Identifier, icebergjob.CompactOpts, string) (*icebergjob.CompactResult, error) {
				return nil, tc.err
			}
			_, err := s.CompactTable(context.Background(), &pb.CompactTableRequest{Bucket: "raw", Table: "events"})
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tc.want)
			}
		})
	}
}

func TestCompactTable_Cancelled(t *testing.T) {
	annotations := filepath.Join(t.TempDir(), "annotations")
	if err := os.WriteFile(annotations, []byte("datuplet.io/cancel=\"true\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := declaredRawEvents()
	cfg.RunID = "run-1"
	cfg.ComponentName = "compact-a"
	cfg.PodAnnotationsPath = annotations
	cfg.CancelPollInterval = 10 * time.Millisecond
	s := mustNewServerV2(t, &cfg)
	defer s.Close()
	called := false
	s.compactFn = func(context.Context, icebergtable.Identifier, icebergjob.CompactOpts, string) (*icebergjob.CompactResult, error) {
		called = true
		return &icebergjob.CompactResult{Status: icebergjob.CompactCompacted}, nil
	}
	deadline := time.Now().Add(5 * time.Second)
	for !s.isCancelled() {
		if time.Now().After(deadline) {
			t.Fatal("cancel annotation not observed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, err := s.CompactTable(context.Background(), &pb.CompactTableRequest{Bucket: "raw", Table: "events"})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("code = %v (%v), want Canceled", status.Code(err), err)
	}
	if called {
		t.Fatal("seam called after cancel")
	}
}
