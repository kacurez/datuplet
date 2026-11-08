package main

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/datuplet/datuplet/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	exitUser = 1
	exitApp  = 20
)

// Config is the iceberg-compactor component config. Zero values defer to the
// gateway (target size) or library (file-count bounds) defaults.
type Config struct {
	TargetFileSizeMB int  `json:"target_file_size_mb"`
	MinInputFiles    int  `json:"min_input_files"`
	MaxInputFiles    int  `json:"max_input_files"`
	DryRun           bool `json:"dry_run"`
}

type compactor interface {
	Compact(ctx context.Context, bucket, table string, opts sdk.CompactOptions) (*sdk.CompactResult, error)
}

func (c Config) options() sdk.CompactOptions {
	return sdk.CompactOptions{
		TargetFileSizeBytes: int64(c.TargetFileSizeMB) << 20,
		MinInputFiles:       c.MinInputFiles,
		MaxInputFiles:       c.MaxInputFiles,
		DryRun:              c.DryRun,
	}
}

// exitCodeFor maps a Compact error to the exit-code contract.
func exitCodeFor(err error) int {
	st, ok := status.FromError(err)
	if !ok {
		return exitApp
	}
	switch st.Code() {
	case codes.NotFound, codes.PermissionDenied, codes.InvalidArgument, codes.FailedPrecondition:
		return exitUser
	default:
		return exitApp
	}
}

// run compacts every explicitly declared output table and returns the worst
// exit code (>=20 beats 1 beats 0) plus the single status message.
func run(ctx context.Context, c compactor, cfg Config, defaultBucket string, tables []sdk.OutputTableRef) (int, string) {
	if len(tables) == 0 {
		hint := "declare outputs.tables explicitly"
		if defaultBucket != "" {
			hint = "defaultBucket outputs are not supported; " + hint
		}
		return exitUser, "iceberg-compactor needs explicit output tables: " + hint
	}

	exit := 0
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		name := t.Table
		if name == "" {
			name = t.Name
		}
		res, err := c.Compact(ctx, t.Bucket, name, cfg.options())
		if err != nil {
			st := status.Convert(err)
			msg := strings.NewReplacer("\r", " ", "\n", " ").Replace(st.Message())
			parts = append(parts, fmt.Sprintf("%s.%s: %s: %s", t.Bucket, name, st.Code(), msg))
			exit = worst(exit, exitCodeFor(err))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s.%s: %s, files %d -> %d", t.Bucket, name, res.Status, res.FilesBefore, res.FilesAfter))
	}

	msg := "compaction: " + strings.Join(parts, "; ") + " (old files stay until snapshots are expired)"
	return exit, msg
}

func worst(a, b int) int {
	if b > a {
		return b
	}
	return a
}
