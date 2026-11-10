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
// more than opts.MaxFiles files (ErrRefused with the count); the live-file
// guard finds a planned path among the current snapshot's files (ErrRefused);
// opts.DryRun is set (DRY_RUN); or ctx is done before the delete. iceberg-go
// plans with PrefixMismatchError, and the delete runs exactly that plan. A
// partial delete returns an error carrying the counts; re-running is safe.
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
		return nil, fmt.Errorf("deleted %d of %d orphan files: %w", res.FilesDeleted, len(planned), err)
	}
	res.Status = OrphanDeleted
	res.Duration = time.Since(start)
	return res, nil
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
// the current metadata file and of the current snapshot's manifest list,
// manifests and every non-DELETED entry (data and delete files).
func liveFiles(ctx context.Context, tbl *icebergtable.Table) (map[string]struct{}, error) {
	live := map[string]struct{}{objectKey(tbl.MetadataLocation()): {}}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return live, nil
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("table io: %w", err)
	}
	if snap.ManifestList != "" {
		live[objectKey(snap.ManifestList)] = struct{}{}
	}
	mfs, err := snap.Manifests(fsys)
	if err != nil {
		return nil, fmt.Errorf("snapshot %d manifests: %w", snap.SnapshotID, err)
	}
	for _, m := range mfs {
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
	return live, nil
}

// liveFileGuard refuses a plan that names any live file, compared by object
// key.
func liveFileGuard(planned []string, live map[string]struct{}) error {
	for _, p := range planned {
		if _, ok := live[objectKey(p)]; ok {
			return fmt.Errorf("%w: live-file guard: planned orphan %s is a live file of the current snapshot; nothing deleted", ErrRefused, p)
		}
	}
	return nil
}
