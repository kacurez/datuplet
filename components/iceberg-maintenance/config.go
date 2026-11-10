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

// Config is the iceberg-maintenance component config. Zero values defer to the
// gateway (target size) or library (file-count bounds) defaults.
type Config struct {
	DryRun            bool                  `json:"dry_run"`
	Compact           CompactTask           `json:"compact"`
	RewriteManifests  RewriteManifestsTask  `json:"rewrite_manifests"`
	ExpireSnapshots   ExpireSnapshotsTask   `json:"expire_snapshots"`
	DeleteOrphanFiles DeleteOrphanFilesTask `json:"delete_orphan_files"`
}

// CompactTask is on unless Enabled is explicitly false.
type CompactTask struct {
	Enabled          *bool `json:"enabled"`
	TargetFileSizeMB int   `json:"target_file_size_mb"`
	MinInputFiles    int   `json:"min_input_files"`
	MaxInputFiles    int   `json:"max_input_files"`
}

type RewriteManifestsTask struct {
	Enabled              bool `json:"enabled"`
	TargetManifestSizeMB int  `json:"target_manifest_size_mb"`
}

type ExpireSnapshotsTask struct {
	Enabled        bool `json:"enabled"`
	OlderThanHours int  `json:"older_than_hours"`
	RetainLast     int  `json:"retain_last"`
}

type DeleteOrphanFilesTask struct {
	Enabled        bool `json:"enabled"`
	OlderThanHours int  `json:"older_than_hours"`
	MaxFiles       int  `json:"max_files"`
}

// maintainer is the gateway surface the component needs; it grows with each
// maintenance task.
type maintainer interface {
	Compact(ctx context.Context, bucket, table string, opts sdk.CompactOptions) (*sdk.CompactResult, error)
}

func (c Config) compactEnabled() bool {
	return c.Compact.Enabled == nil || *c.Compact.Enabled
}

func (c Config) compactOptions() sdk.CompactOptions {
	return sdk.CompactOptions{
		TargetFileSizeBytes: int64(c.Compact.TargetFileSizeMB) << 20,
		MinInputFiles:       c.Compact.MinInputFiles,
		MaxInputFiles:       c.Compact.MaxInputFiles,
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

// run maintains every explicitly declared output table and returns the worst
// exit code (>=20 beats 1 beats 0) plus the single status message.
func run(ctx context.Context, m maintainer, cfg Config, defaultBucket string, tables []sdk.OutputTableRef) (int, string) {
	if len(tables) == 0 {
		hint := "declare outputs.tables explicitly"
		if defaultBucket != "" {
			hint = "defaultBucket outputs are not supported; " + hint
		}
		return exitUser, "iceberg-maintenance needs explicit output tables: " + hint
	}

	if !cfg.compactEnabled() {
		return exitUser, "no maintenance task enabled"
	}

	exit := 0
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		name := t.Table
		if name == "" {
			name = t.Name
		}
		res, err := m.Compact(ctx, t.Bucket, name, cfg.compactOptions())
		if err != nil {
			st := status.Convert(err)
			msg := strings.NewReplacer("\r", " ", "\n", " ").Replace(st.Message())
			parts = append(parts, fmt.Sprintf("%s.%s: %s: %s", t.Bucket, name, st.Code(), msg))
			exit = worst(exit, exitCodeFor(err))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s.%s: %s, files %d -> %d", t.Bucket, name, res.Status, res.FilesBefore, res.FilesAfter))
	}

	msg := "maintenance: " + strings.Join(parts, "; ") + " (old files stay until snapshots are expired)"
	return exit, msg
}

func worst(a, b int) int {
	if b > a {
		return b
	}
	return a
}
