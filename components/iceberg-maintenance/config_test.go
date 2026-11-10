package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	sdk "github.com/datuplet/datuplet/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type call struct {
	bucket, table string
	opts          sdk.CompactOptions
}

type fakeMaintainer struct {
	calls   []call
	order   []string
	rwErrs  map[string]error
	exOpts  []sdk.ExpireOptions
	exRes   *sdk.ExpireResult
	orOpts  []sdk.OrphanOptions
	orErr   error
	results map[string]*sdk.CompactResult
	errs    map[string]error
}

func (f *fakeMaintainer) Compact(_ context.Context, bucket, table string, opts sdk.CompactOptions) (*sdk.CompactResult, error) {
	f.calls = append(f.calls, call{bucket, table, opts})
	f.order = append(f.order, "compact "+bucket+"."+table)
	key := bucket + "." + table
	if err := f.errs[key]; err != nil {
		return nil, fmt.Errorf("compact %s failed: %w", key, err)
	}
	if r := f.results[key]; r != nil {
		return r, nil
	}
	return &sdk.CompactResult{Status: "NOTHING_TO_DO"}, nil
}

func (f *fakeMaintainer) RewriteManifests(_ context.Context, bucket, table string, _ sdk.RewriteManifestsOptions) (*sdk.RewriteManifestsResult, error) {
	key := bucket + "." + table
	f.order = append(f.order, "rewrite "+key)
	if err := f.rwErrs[key]; err != nil {
		return nil, fmt.Errorf("rewrite %s failed: %w", key, err)
	}
	return &sdk.RewriteManifestsResult{Status: "REWRITTEN", ManifestsBefore: 12, ManifestsAfter: 1}, nil
}

func (f *fakeMaintainer) ExpireSnapshots(_ context.Context, bucket, table string, opts sdk.ExpireOptions) (*sdk.ExpireResult, error) {
	f.order = append(f.order, "expire "+bucket+"."+table)
	f.exOpts = append(f.exOpts, opts)
	if f.exRes != nil {
		return f.exRes, nil
	}
	return &sdk.ExpireResult{Status: "EXPIRED", SnapshotsExpired: 4, FilesDeleted: 9, BytesDeleted: 1234}, nil
}

func (f *fakeMaintainer) DeleteOrphanFiles(_ context.Context, bucket, table string, opts sdk.OrphanOptions) (*sdk.OrphanResult, error) {
	f.order = append(f.order, "orphans "+bucket+"."+table)
	f.orOpts = append(f.orOpts, opts)
	if f.orErr != nil {
		return nil, f.orErr
	}
	return &sdk.OrphanResult{Status: "DELETED", OrphanFiles: 3, OrphanBytes: 456, FilesDeleted: 3}, nil
}

func tbl(bucket, name string) sdk.OutputTableRef {
	return sdk.OutputTableRef{Bucket: bucket, Name: name, Table: name}
}

func TestRun_ExitCodes(t *testing.T) {
	events := []sdk.OutputTableRef{tbl("raw", "events")}
	tests := []struct {
		name          string
		defaultBucket string
		tables        []sdk.OutputTableRef
		res           *sdk.CompactResult
		err           error
		want          int
	}{
		{name: "compacted", tables: events, res: &sdk.CompactResult{Status: "COMPACTED", FilesBefore: 12, FilesAfter: 2}, want: 0},
		{name: "nothing to do", tables: events, res: &sdk.CompactResult{Status: "NOTHING_TO_DO"}, want: 0},
		{name: "superseded", tables: events, res: &sdk.CompactResult{Status: "SUPERSEDED"}, want: 0},
		{name: "already committed", tables: events, res: &sdk.CompactResult{Status: "ALREADY_COMMITTED"}, want: 0},
		{name: "dry run", tables: events, res: &sdk.CompactResult{Status: "DRY_RUN"}, want: 0},
		{name: "default bucket only", defaultBucket: "raw", want: 1},
		{name: "no outputs", want: 1},
		{name: "NotFound", tables: events, err: status.Error(codes.NotFound, "x"), want: 1},
		{name: "PermissionDenied", tables: events, err: status.Error(codes.PermissionDenied, "x"), want: 1},
		{name: "InvalidArgument", tables: events, err: status.Error(codes.InvalidArgument, "x"), want: 1},
		{name: "FailedPrecondition", tables: events, err: status.Error(codes.FailedPrecondition, "x"), want: 1},
		{name: "Canceled", tables: events, err: status.Error(codes.Canceled, "x"), want: 20},
		{name: "Internal", tables: events, err: status.Error(codes.Internal, "x"), want: 20},
		{name: "Aborted", tables: events, err: status.Error(codes.Aborted, "x"), want: 20},
		{name: "Unavailable", tables: events, err: status.Error(codes.Unavailable, "x"), want: 20},
		{name: "Unknown", tables: events, err: status.Error(codes.Unknown, "x"), want: 20},
		{name: "DeadlineExceeded", tables: events, err: status.Error(codes.DeadlineExceeded, "x"), want: 20},
		{name: "non-grpc error", tables: events, err: errors.New("boom"), want: 20},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeMaintainer{results: map[string]*sdk.CompactResult{"raw.events": tc.res}, errs: map[string]error{"raw.events": tc.err}}
			code, msg := run(context.Background(), f, Config{}, tc.defaultBucket, tc.tables)
			if code != tc.want {
				t.Fatalf("exit = %d, want %d (msg %q)", code, tc.want, msg)
			}
			if strings.Contains(msg, "\n") || msg == "" {
				t.Fatalf("status message must be one non-empty line: %q", msg)
			}
			if tc.tables == nil && len(f.calls) != 0 {
				t.Fatalf("gateway called without explicit tables")
			}
		})
	}
}

func TestRun_StatusMessage(t *testing.T) {
	f := &fakeMaintainer{results: map[string]*sdk.CompactResult{
		"raw.events": {Status: "COMPACTED", FilesBefore: 12, FilesAfter: 2},
	}}
	code, msg := run(context.Background(), f, Config{}, "", []sdk.OutputTableRef{tbl("raw", "events")})
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	for _, want := range []string{"raw.events", "COMPACTED", "12 -> 2", "old files stay until snapshots are expired"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
}

func TestRun_MultiTableWorstWins(t *testing.T) {
	tables := []sdk.OutputTableRef{tbl("raw", "a"), tbl("raw", "b"), tbl("raw", "c")}
	f := &fakeMaintainer{
		results: map[string]*sdk.CompactResult{"raw.a": {Status: "COMPACTED", FilesBefore: 9, FilesAfter: 1}},
		errs: map[string]error{
			"raw.b": status.Error(codes.NotFound, "gone"),
			"raw.c": status.Error(codes.Internal, "boom"),
		},
	}
	code, msg := run(context.Background(), f, Config{}, "", tables)
	if code != 20 {
		t.Fatalf("exit = %d, want 20", code)
	}
	if len(f.calls) != 3 {
		t.Fatalf("calls = %d, want 3 (every table processed)", len(f.calls))
	}
	for _, want := range []string{"raw.a", "raw.b", "raw.c", "9 -> 1"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}

	f = &fakeMaintainer{errs: map[string]error{"raw.b": status.Error(codes.NotFound, "gone")}}
	if code, _ := run(context.Background(), f, Config{}, "", tables); code != 1 {
		t.Fatalf("user-only failure exit = %d, want 1", code)
	}
}

func TestRun_Options(t *testing.T) {
	f := &fakeMaintainer{}
	run(context.Background(), f, Config{}, "", []sdk.OutputTableRef{tbl("raw", "events")})
	if got := f.calls[0].opts; got != (sdk.CompactOptions{}) {
		t.Errorf("unset config must send zero options (gateway defaults), got %+v", got)
	}

	f = &fakeMaintainer{}
	cfg := Config{DryRun: true, Compact: CompactTask{TargetFileSizeMB: 64, MinInputFiles: 3, MaxInputFiles: 10}}
	run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "events")})
	want := sdk.CompactOptions{TargetFileSizeBytes: 64 << 20, MinInputFiles: 3, MaxInputFiles: 10, DryRun: true}
	if got := f.calls[0].opts; got != want {
		t.Errorf("opts = %+v, want %+v", got, want)
	}
}

func TestRun_MultiLineErrorIsSingleLine(t *testing.T) {
	f := &fakeMaintainer{errs: map[string]error{
		"raw.events": status.Error(codes.Internal, "boom\r\nsecond line\nthird"),
	}}
	code, msg := run(context.Background(), f, Config{}, "", []sdk.OutputTableRef{tbl("raw", "events")})
	if code != exitApp {
		t.Fatalf("exit = %d", code)
	}
	if strings.ContainsAny(msg, "\r\n") {
		t.Errorf("status message must be a single line, got %q", msg)
	}
	if !strings.Contains(msg, "raw.events: compact Internal:") || !strings.Contains(msg, "second line") {
		t.Errorf("message %q lost detail", msg)
	}
}

func TestRun_EmptyConfig_CompactsOnly(t *testing.T) {
	f := &fakeMaintainer{}
	tables := []sdk.OutputTableRef{tbl("raw", "a"), tbl("raw", "b")}
	code, msg := run(context.Background(), f, Config{}, "", tables)
	if code != 0 {
		t.Fatalf("exit = %d (msg %q)", code, msg)
	}
	if len(f.calls) != 2 {
		t.Fatalf("calls = %d, want one Compact per table", len(f.calls))
	}
}

func TestRun_AllTasksDisabled_ExitUser(t *testing.T) {
	f := &fakeMaintainer{}
	off := false
	cfg := Config{Compact: CompactTask{Enabled: &off}}
	code, msg := run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if code != exitUser {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(msg, "no maintenance task enabled") {
		t.Errorf("message %q", msg)
	}
	if len(f.calls) != 0 {
		t.Errorf("gateway called with every task disabled")
	}
}

func TestRun_StatusLinePrefix(t *testing.T) {
	f := &fakeMaintainer{}
	_, msg := run(context.Background(), f, Config{}, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if !strings.HasPrefix(msg, "maintenance: ") {
		t.Errorf("message %q lacks prefix", msg)
	}
}

func TestRun_OrderAndSkipAfterFailure(t *testing.T) {
	tables := []sdk.OutputTableRef{tbl("raw", "a"), tbl("raw", "b")}
	f := &fakeMaintainer{
		results: map[string]*sdk.CompactResult{"raw.b": {Status: "COMPACTED", FilesBefore: 12, FilesAfter: 2}},
		errs:    map[string]error{"raw.a": status.Error(codes.Unavailable, "down")},
	}
	cfg := Config{RewriteManifests: RewriteManifestsTask{Enabled: true}}
	code, msg := run(context.Background(), f, cfg, "", tables)
	if code != 20 {
		t.Fatalf("exit = %d, want 20 (msg %q)", code, msg)
	}
	want := []string{"compact raw.a", "compact raw.b", "rewrite raw.b"}
	if strings.Join(f.order, ",") != strings.Join(want, ",") {
		t.Fatalf("calls = %v, want %v", f.order, want)
	}
	for _, w := range []string{"compact COMPACTED files 12 -> 2; rewrite_manifests REWRITTEN 12 -> 1", "rewrite_manifests skipped"} {
		if !strings.Contains(msg, w) {
			t.Errorf("message %q missing %q", msg, w)
		}
	}
}

func TestRun_RewriteOnly(t *testing.T) {
	off := false
	f := &fakeMaintainer{}
	cfg := Config{Compact: CompactTask{Enabled: &off}, RewriteManifests: RewriteManifestsTask{Enabled: true}}
	code, _ := run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if code != 0 || len(f.calls) != 0 || len(f.order) != 1 {
		t.Fatalf("exit %d, compact calls %d, order %v", code, len(f.calls), f.order)
	}
}

func TestRun_ExpireAfterRewrite(t *testing.T) {
	f := &fakeMaintainer{results: map[string]*sdk.CompactResult{"raw.a": {Status: "COMPACTED", FilesBefore: 12, FilesAfter: 2}}}
	cfg := Config{DryRun: true, RewriteManifests: RewriteManifestsTask{Enabled: true},
		ExpireSnapshots: ExpireSnapshotsTask{Enabled: true, OlderThanHours: 48, RetainLast: 3}}
	code, msg := run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if code != 0 {
		t.Fatalf("exit = %d (msg %q)", code, msg)
	}
	if want := "compact raw.a,rewrite raw.a,expire raw.a"; strings.Join(f.order, ",") != want {
		t.Fatalf("calls = %v, want %s", f.order, want)
	}
	if got, want := f.exOpts[0], (sdk.ExpireOptions{OlderThanHours: 48, RetainLast: 3, DryRun: true}); got != want {
		t.Errorf("expire opts = %+v, want %+v", got, want)
	}
	if !strings.Contains(msg, "expire_snapshots EXPIRED snapshots 4, files 9 (1234 bytes)") {
		t.Errorf("message %q missing expire summary", msg)
	}
	if strings.Contains(msg, "old files stay") {
		t.Errorf("message %q claims old files stay although expiry ran", msg)
	}
}

func TestRun_ExpirePartialExitsZero(t *testing.T) {
	off := false
	f := &fakeMaintainer{exRes: &sdk.ExpireResult{Status: "EXPIRED_PARTIAL", SnapshotsExpired: 2, FilesDeleted: 5, FilesLeft: 3}}
	cfg := Config{Compact: CompactTask{Enabled: &off}, ExpireSnapshots: ExpireSnapshotsTask{Enabled: true}}
	code, msg := run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (msg %q)", code, msg)
	}
	if !strings.Contains(msg, "EXPIRED_PARTIAL") || !strings.Contains(msg, "files_left 3") {
		t.Errorf("message %q missing partial status / files_left", msg)
	}
	if f.exOpts[0] != (sdk.ExpireOptions{}) {
		t.Errorf("unset expire config must send zero options, got %+v", f.exOpts[0])
	}
}

func TestRun_ExpireFilesLeftUnknown(t *testing.T) {
	off := false
	f := &fakeMaintainer{exRes: &sdk.ExpireResult{Status: "EXPIRED_PARTIAL", SnapshotsExpired: 2, FilesLeft: -1}}
	cfg := Config{Compact: CompactTask{Enabled: &off}, ExpireSnapshots: ExpireSnapshotsTask{Enabled: true}}
	code, msg := run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if code != 0 || !strings.Contains(msg, "EXPIRED_PARTIAL snapshots 2, files 0 (0 bytes), files_left unknown") {
		t.Fatalf("exit %d, message %q", code, msg)
	}
}

func TestRun_AllFourTasksInOrder(t *testing.T) {
	f := &fakeMaintainer{}
	cfg := Config{DryRun: true, RewriteManifests: RewriteManifestsTask{Enabled: true},
		ExpireSnapshots:   ExpireSnapshotsTask{Enabled: true},
		DeleteOrphanFiles: DeleteOrphanFilesTask{Enabled: true, OlderThanHours: 96, MaxFiles: 50}}
	code, msg := run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if code != 0 {
		t.Fatalf("exit = %d (msg %q)", code, msg)
	}
	if want := "compact raw.a,rewrite raw.a,expire raw.a,orphans raw.a"; strings.Join(f.order, ",") != want {
		t.Fatalf("calls = %v, want %s", f.order, want)
	}
	if got, want := f.orOpts[0], (sdk.OrphanOptions{OlderThanHours: 96, MaxFiles: 50, DryRun: true}); got != want {
		t.Errorf("orphan opts = %+v, want %+v", got, want)
	}
	if !strings.Contains(msg, "delete_orphan_files DELETED orphans 3 (456 bytes), deleted 3") {
		t.Errorf("message %q missing orphan summary", msg)
	}
}

func TestRun_OrphansOnly_Enabled(t *testing.T) {
	off := false
	f := &fakeMaintainer{}
	cfg := Config{Compact: CompactTask{Enabled: &off}, DeleteOrphanFiles: DeleteOrphanFilesTask{Enabled: true}}
	code, msg := run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if code != 0 || strings.Join(f.order, ",") != "orphans raw.a" {
		t.Fatalf("exit %d, calls %v, msg %q", code, f.order, msg)
	}
	if f.orOpts[0] != (sdk.OrphanOptions{}) {
		t.Errorf("unset orphan config must send zero options, got %+v", f.orOpts[0])
	}
}

func TestRun_OrphansRefusedExitsUser(t *testing.T) {
	off := false
	f := &fakeMaintainer{orErr: status.Error(codes.FailedPrecondition, "maintenance refused: 3 orphan files exceed max_files 2")}
	cfg := Config{Compact: CompactTask{Enabled: &off}, DeleteOrphanFiles: DeleteOrphanFilesTask{Enabled: true}}
	code, msg := run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "a")})
	if code != exitUser || !strings.Contains(msg, "delete_orphan_files FailedPrecondition") {
		t.Fatalf("exit %d, msg %q", code, msg)
	}
}
