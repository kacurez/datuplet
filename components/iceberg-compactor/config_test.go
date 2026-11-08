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

type fakeCompactor struct {
	calls   []call
	results map[string]*sdk.CompactResult
	errs    map[string]error
}

func (f *fakeCompactor) Compact(_ context.Context, bucket, table string, opts sdk.CompactOptions) (*sdk.CompactResult, error) {
	f.calls = append(f.calls, call{bucket, table, opts})
	key := bucket + "." + table
	if err := f.errs[key]; err != nil {
		return nil, fmt.Errorf("compact %s failed: %w", key, err)
	}
	if r := f.results[key]; r != nil {
		return r, nil
	}
	return &sdk.CompactResult{Status: "NOTHING_TO_DO"}, nil
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
			f := &fakeCompactor{results: map[string]*sdk.CompactResult{"raw.events": tc.res}, errs: map[string]error{"raw.events": tc.err}}
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
	f := &fakeCompactor{results: map[string]*sdk.CompactResult{
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
	f := &fakeCompactor{
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

	f = &fakeCompactor{errs: map[string]error{"raw.b": status.Error(codes.NotFound, "gone")}}
	if code, _ := run(context.Background(), f, Config{}, "", tables); code != 1 {
		t.Fatalf("user-only failure exit = %d, want 1", code)
	}
}

func TestRun_Options(t *testing.T) {
	f := &fakeCompactor{}
	run(context.Background(), f, Config{}, "", []sdk.OutputTableRef{tbl("raw", "events")})
	if got := f.calls[0].opts; got != (sdk.CompactOptions{}) {
		t.Errorf("unset config must send zero options (gateway defaults), got %+v", got)
	}

	f = &fakeCompactor{}
	cfg := Config{TargetFileSizeMB: 64, MinInputFiles: 3, MaxInputFiles: 10, DryRun: true}
	run(context.Background(), f, cfg, "", []sdk.OutputTableRef{tbl("raw", "events")})
	want := sdk.CompactOptions{TargetFileSizeBytes: 64 << 20, MinInputFiles: 3, MaxInputFiles: 10, DryRun: true}
	if got := f.calls[0].opts; got != want {
		t.Errorf("opts = %+v, want %+v", got, want)
	}
}
