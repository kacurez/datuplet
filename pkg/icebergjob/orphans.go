package icebergjob

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	"gocloud.dev/gcerrors"
)

const (
	OrphanDeleted     = "DELETED"
	OrphanNothingToDo = "NOTHING_TO_DO"
	OrphanDryRun      = "DRY_RUN"

	orphanDeleteConcurrency = 8
	maxReportedDeleteErrors = 10
)

// DeleteTableOrphanFiles loads ident and deletes its orphan files; see
// DeleteOrphansOnTable.
func DeleteTableOrphanFiles(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, opts OrphanOpts) (*OrphanResult, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("DeleteTableOrphanFiles %v: load table: %w", ident, err)
	}
	res, err := DeleteOrphansOnTable(ctx, tbl, opts)
	if err != nil {
		return nil, fmt.Errorf("DeleteTableOrphanFiles %v: %w", ident, err)
	}
	return res, nil
}

// DeleteOrphansOnTable deletes files under the table's own location that no
// metadata references and that are older than opts.OlderThan.
//
// Nothing is deleted when: gc.enabled is false (ErrRefused); the plan holds
// more than opts.MaxFiles files (ErrRefused with the count); a planned path is
// not literally under the table location (prefix check, ErrRefused); a planned
// path is referenced by table metadata, compared by object key (live-file
// guard, ErrRefused); opts.DryRun is set (DRY_RUN); or ctx is done before the
// delete. iceberg-go plans with PrefixMismatchError, and the delete runs
// exactly that plan. A partial delete returns an error carrying the counts;
// re-running is safe.
func DeleteOrphansOnTable(ctx context.Context, tbl *icebergtable.Table, opts OrphanOpts) (*OrphanResult, error) {
	start := time.Now()
	if opts.OlderThan <= 0 || opts.MaxFiles < 1 {
		return nil, fmt.Errorf("orphan cleanup: older-than must be > 0 and max-files >= 1 (got %s, %d)", opts.OlderThan, opts.MaxFiles)
	}
	if v, ok := tbl.Properties()[icebergtable.GCEnabledKey]; ok && !strings.EqualFold(v, "true") {
		return nil, fmt.Errorf("%w: table has gc.enabled=%s", ErrRefused, v)
	}

	plan, err := tbl.PlanOrphanFiles(ctx, icebergtable.WithFilesOlderThan(opts.OlderThan))
	if err != nil {
		return nil, fmt.Errorf("plan orphan files: %w", err)
	}
	planned := plan.Files()
	res := &OrphanResult{OrphanFiles: len(planned), OrphanBytes: plan.TotalSizeBytes()}
	if len(planned) == 0 {
		res.Status = OrphanNothingToDo
		res.Duration = time.Since(start)
		return res, nil
	}
	if len(planned) > opts.MaxFiles {
		return nil, fmt.Errorf("%w: %d orphan files exceed max_files %d; nothing deleted", ErrRefused, len(planned), opts.MaxFiles)
	}

	if err := prefixCheck(planned, tbl.Location()); err != nil {
		return nil, err
	}
	live, err := liveFiles(ctx, tbl)
	if err != nil {
		return nil, fmt.Errorf("live files: %w", err)
	}
	if err := liveFileGuard(planned, live); err != nil {
		return nil, err
	}

	if opts.DryRun {
		res.Status = OrphanDryRun
		res.Duration = time.Since(start)
		return res, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("cancelled before deleting %d orphan files: %w", len(planned), err)
	}

	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("table io: %w", err)
	}
	var deleted atomic.Int64
	_, err = tbl.ExecuteOrphanCleanup(ctx, plan,
		icebergtable.WithCleanupMaxConcurrency(orphanDeleteConcurrency),
		icebergtable.WithDeleteFunc(func(p string) error {
			if err := fsys.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) && gcerrors.Code(err) != gcerrors.NotFound {
				return err
			}
			deleted.Add(1)
			return nil
		}))
	res.FilesDeleted = int(deleted.Load())
	if err != nil {
		return nil, fmt.Errorf("deleted %d of %d orphan files: %w", res.FilesDeleted, len(planned), capErrors(err, maxReportedDeleteErrors))
	}
	res.Status = OrphanDeleted
	res.Duration = time.Since(start)
	return res, nil
}

// capErrors keeps the first n errors of the joined delete error inside err
// and summarises the rest, so a mass delete failure stays a short status
// message.
func capErrors(err error, n int) error {
	for e := err; e != nil; e = errors.Unwrap(e) {
		j, ok := e.(interface{ Unwrap() []error })
		if !ok {
			continue
		}
		errs := j.Unwrap()
		if len(errs) <= n {
			return err
		}
		return fmt.Errorf("%w; and %d more errors", errors.Join(errs[:n]...), len(errs)-n)
	}
	return err
}

// objectKey reduces a storage path to its object key: it strips
// "scheme://authority/" and any leading "/", so the same object compares
// equal however a listing spells it (s3://, s3a://, gs://, bare key).
func objectKey(p string) string {
	if _, rest, ok := strings.Cut(p, "://"); ok {
		if _, key, ok := strings.Cut(rest, "/"); ok {
			p = key
		} else {
			p = ""
		}
	}
	return strings.TrimLeft(p, "/")
}

// liveFiles reads, independently of the orphan planner, the object keys of
// every file the table metadata references: the current metadata file, every
// metadata-log entry, statistics and partition-statistics files, and for
// every retained snapshot its manifest list, its manifests and every
// non-DELETED entry (data and delete files).
func liveFiles(ctx context.Context, tbl *icebergtable.Table) (map[string]struct{}, error) {
	md := tbl.Metadata()
	live := map[string]struct{}{objectKey(tbl.MetadataLocation()): {}}
	for e := range md.PreviousFiles() {
		live[objectKey(e.MetadataFile)] = struct{}{}
	}
	for sf := range md.Statistics() {
		if sf.StatisticsPath != "" {
			live[objectKey(sf.StatisticsPath)] = struct{}{}
		}
	}
	for sf := range md.PartitionStatistics() {
		if sf.StatisticsPath != "" {
			live[objectKey(sf.StatisticsPath)] = struct{}{}
		}
	}
	snaps := md.Snapshots()
	if len(snaps) == 0 {
		return live, nil
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("table io: %w", err)
	}
	seen := map[string]struct{}{}
	for _, snap := range snaps {
		if snap.ManifestList != "" {
			live[objectKey(snap.ManifestList)] = struct{}{}
		}
		mfs, err := snap.Manifests(fsys)
		if err != nil {
			return nil, fmt.Errorf("snapshot %d manifests: %w", snap.SnapshotID, err)
		}
		for _, m := range mfs {
			if _, ok := seen[m.FilePath()]; ok {
				continue
			}
			seen[m.FilePath()] = struct{}{}
			live[objectKey(m.FilePath())] = struct{}{}
			for e, err := range m.Entries(fsys, false) {
				if err != nil {
					return nil, fmt.Errorf("read manifest %s: %w", m.FilePath(), err)
				}
				if e.Status() != iceberg.EntryStatusDELETED {
					live[objectKey(e.DataFile().FilePath())] = struct{}{}
				}
			}
		}
	}
	return live, nil
}

// prefixCheck refuses a plan with any path that does not literally start
// with the table location plus "/". Listings that respell paths (bare keys,
// bucket/key, another scheme, percent-encoding, interior "//") or reach a
// sibling table whose location shares the prefix string all fail here.
func prefixCheck(planned []string, location string) error {
	prefix := strings.TrimSuffix(location, "/") + "/"
	for _, p := range planned {
		if !strings.HasPrefix(p, prefix) {
			return fmt.Errorf("%w: prefix check: planned orphan %s is not under table location %s; nothing deleted", ErrRefused, p, prefix)
		}
	}
	return nil
}

// liveFileGuard refuses a plan that names any live file, compared by object
// key.
func liveFileGuard(planned []string, live map[string]struct{}) error {
	for _, p := range planned {
		if _, ok := live[objectKey(p)]; ok {
			return fmt.Errorf("%w: live-file guard: planned orphan %s is referenced by table metadata; nothing deleted", ErrRefused, p)
		}
	}
	return nil
}
