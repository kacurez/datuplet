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
	RewriteManifests(ctx context.Context, bucket, table string, opts sdk.RewriteManifestsOptions) (*sdk.RewriteManifestsResult, error)
	ExpireSnapshots(ctx context.Context, bucket, table string, opts sdk.ExpireOptions) (*sdk.ExpireResult, error)
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

func (c Config) anyEnabled() bool {
	return c.compactEnabled() || c.RewriteManifests.Enabled || c.ExpireSnapshots.Enabled
}

func (c Config) rewriteManifestsOptions() sdk.RewriteManifestsOptions {
	return sdk.RewriteManifestsOptions{
		TargetManifestSizeBytes: int64(c.RewriteManifests.TargetManifestSizeMB) << 20,
		DryRun:                  c.DryRun,
	}
}

func (c Config) expireOptions() sdk.ExpireOptions {
	return sdk.ExpireOptions{
		OlderThanHours: c.ExpireSnapshots.OlderThanHours,
		RetainLast:     c.ExpireSnapshots.RetainLast,
		DryRun:         c.DryRun,
	}
}

// task is one maintenance step on one table. do returns the summary text that
// follows the task name.
type task struct {
	name string
	do   func(ctx context.Context, m maintainer, bucket, table string) (string, error)
}

// tasks returns the enabled tasks in the fixed order: compact,
// rewrite_manifests, expire_snapshots. Later maintenance tasks append here,
// keeping that order.
func (c Config) tasks() []task {
	var ts []task
	if c.compactEnabled() {
		ts = append(ts, task{"compact", func(ctx context.Context, m maintainer, bucket, table string) (string, error) {
			res, err := m.Compact(ctx, bucket, table, c.compactOptions())
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s files %d -> %d", res.Status, res.FilesBefore, res.FilesAfter), nil
		}})
	}
	if c.RewriteManifests.Enabled {
		ts = append(ts, task{"rewrite_manifests", func(ctx context.Context, m maintainer, bucket, table string) (string, error) {
			res, err := m.RewriteManifests(ctx, bucket, table, c.rewriteManifestsOptions())
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s %d -> %d", res.Status, res.ManifestsBefore, res.ManifestsAfter), nil
		}})
	}
	if c.ExpireSnapshots.Enabled {
		ts = append(ts, task{"expire_snapshots", func(ctx context.Context, m maintainer, bucket, table string) (string, error) {
			res, err := m.ExpireSnapshots(ctx, bucket, table, c.expireOptions())
			if err != nil {
				return "", err
			}
			out := fmt.Sprintf("%s snapshots %d, files %d (%d bytes)", res.Status, res.SnapshotsExpired, res.FilesDeleted, res.BytesDeleted)
			switch {
			case res.FilesLeft < 0:
				out += ", files_left unknown"
			case res.FilesLeft > 0:
				out += fmt.Sprintf(", files_left %d", res.FilesLeft)
			}
			return out, nil
		}})
	}
	return ts
}

// exitCodeFor maps a maintenance error to the exit-code contract.
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

	if !cfg.anyEnabled() {
		return exitUser, "no maintenance task enabled"
	}

	tasks := cfg.tasks()
	exit := 0
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		name := t.Table
		if name == "" {
			name = t.Name
		}
		steps := make([]string, 0, len(tasks))
		for i, tk := range tasks {
			out, err := tk.do(ctx, m, t.Bucket, name)
			if err != nil {
				st := status.Convert(err)
				msg := strings.NewReplacer("\r", " ", "\n", " ").Replace(st.Message())
				steps = append(steps, fmt.Sprintf("%s %s: %s", tk.name, st.Code(), msg))
				// A failed task skips the rest for this table only.
				for _, skipped := range tasks[i+1:] {
					steps = append(steps, skipped.name+" skipped")
				}
				exit = worst(exit, exitCodeFor(err))
				break
			}
			steps = append(steps, tk.name+" "+out)
		}
		parts = append(parts, fmt.Sprintf("%s.%s: %s", t.Bucket, name, strings.Join(steps, "; ")))
	}

	msg := "maintenance: " + strings.Join(parts, "; ")
	if !cfg.ExpireSnapshots.Enabled {
		msg += " (old files stay until snapshots are expired)"
	}
	return exit, msg
}

func worst(a, b int) int {
	if b > a {
		return b
	}
	return a
}
