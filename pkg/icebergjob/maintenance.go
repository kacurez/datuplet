package icebergjob

import (
	"errors"
	"time"
)

// ErrRefused marks a maintenance operation the library declined to run for a
// safety reason (gc disabled, keep-set guard, live-file guard, size cap). The
// gateway maps it to FailedPrecondition.
var ErrRefused = errors.New("maintenance refused")

// RewriteManifestsOpts tunes RewriteTableManifests. Zero target size takes
// the default (8 MiB).
type RewriteManifestsOpts struct {
	TargetManifestSizeBytes int64
	DryRun                  bool
}

// RewriteManifestsResult reports one manifest rewrite. Status is REWRITTEN,
// NOTHING_TO_DO or DRY_RUN.
type RewriteManifestsResult struct {
	Status                          string
	ManifestsBefore, ManifestsAfter int
	SnapshotBefore, SnapshotAfter   int64
	Duration                        time.Duration
}

// ExpireOpts tunes ExpireTableSnapshots. Now nil means time.Now.
type ExpireOpts struct {
	OlderThan  time.Duration
	RetainLast int
	DryRun     bool
	Now        func() time.Time
}

// ExpireResult reports one snapshot expiry. Status is EXPIRED,
// EXPIRED_PARTIAL, NOTHING_TO_DO or DRY_RUN. In a dry run FilesDeleted and
// BytesDeleted are the planned deletions.
type ExpireResult struct {
	Status           string
	SnapshotsExpired int
	FilesDeleted     int
	// FilesLeft counts files the expiry freed but could not delete (now
	// orphans). -1 means unknown: the commit landed but the deletion set
	// could not be read.
	FilesLeft       int
	BytesDeleted    int64
	CurrentSnapshot int64
	Duration        time.Duration
}

// OrphanOpts tunes DeleteTableOrphanFiles.
type OrphanOpts struct {
	OlderThan time.Duration
	MaxFiles  int
	DryRun    bool
}

// OrphanResult reports one orphan cleanup. Status is DELETED, NOTHING_TO_DO
// or DRY_RUN.
type OrphanResult struct {
	Status                    string
	OrphanFiles, FilesDeleted int
	OrphanBytes               int64
	Duration                  time.Duration
}
