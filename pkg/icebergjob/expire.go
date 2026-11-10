package icebergjob

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

const (
	ExpireExpired        = "EXPIRED"
	ExpireExpiredPartial = "EXPIRED_PARTIAL"
	ExpireNothingToDo    = "NOTHING_TO_DO"
	ExpireDryRun         = "DRY_RUN"

	expireDeleteConcurrency = 8
)

// ExpireTableSnapshots expires ident's snapshots older than opts.OlderThan
// beyond the last opts.RetainLast of main, then deletes the files only the
// expired snapshots referenced.
//
// Reader protection: a snapshot superseded less than OlderThan ago is kept,
// whatever its own age. Keep-set guard: if iceberg-go would remove a snapshot
// this function must keep (per-ref retention written by another engine
// overrides the call options), nothing is committed and ErrRefused is
// returned. The commit runs without iceberg-go's replay; a 409 reloads the
// table and recomputes everything. Deletion happens after the commit, from
// the committed table; failed deletes leave orphans and yield
// EXPIRED_PARTIAL. A dry run stages the expiry, reports the files it would
// delete in FilesDeleted/BytesDeleted, and commits and deletes nothing.
func ExpireTableSnapshots(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, opts ExpireOpts) (*ExpireResult, error) {
	start := time.Now()
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	if opts.OlderThan < 0 || opts.RetainLast < 1 {
		return nil, fmt.Errorf("ExpireTableSnapshots: older-than must be >= 0 and retain-last >= 1 (got %s, %d)", opts.OlderThan, opts.RetainLast)
	}

	var (
		res      *ExpireResult
		paths    []string
		sizes    map[string]int64
		fsys     iceio.IO
		finished bool
	)
	err := catalogwriter.RetryOnConflict(ctx, catalogwriter.RetryOpts{MaxAttempts: catalogwriter.DefaultMaxRetries}, func(ctx context.Context) error {
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			return fmt.Errorf("load table %v: %w", ident, err)
		}
		if v, ok := tbl.Properties()[icebergtable.GCEnabledKey]; ok && !strings.EqualFold(v, "true") {
			return fmt.Errorf("%w: table %v has gc.enabled=%s", ErrRefused, ident, v)
		}
		md := tbl.Metadata()
		cutoffMs := now().UnixMilli() - opts.OlderThan.Milliseconds()
		eff := effectiveRetainLast(md, cutoffMs, opts.RetainLast)
		ks := keepSet(md, cutoffMs, eff)

		txn := noReplay(tbl, cat).NewTransaction()
		if err := txn.ExpireSnapshots(
			icebergtable.WithOlderThan(olderThanFromCutoff(cutoffMs)),
			icebergtable.WithRetainLast(eff),
			icebergtable.WithPostCommit(false),
		); err != nil {
			return fmt.Errorf("stage expiry: %w", err)
		}
		staged, err := txn.StagedTable()
		if err != nil {
			return fmt.Errorf("stage expiry: %w", err)
		}
		removed := removedSnapshotIDs(md, staged.Metadata())
		if err := guardKeepSet(removed, ks); err != nil {
			return err
		}

		res = &ExpireResult{SnapshotsExpired: len(removed), CurrentSnapshot: snapshotIDOf(tbl)}
		if len(removed) == 0 {
			res.Status = ExpireNothingToDo
			finished = true
			return nil
		}
		expired := snapshotsByID(md, removed)
		if fsys, err = tbl.FS(ctx); err != nil {
			return fmt.Errorf("table io: %w", err)
		}
		if opts.DryRun {
			paths, sizes, err = expireDeletionSet(ctx, fsys, staged.Metadata().Snapshots(), expired)
			if err != nil {
				return fmt.Errorf("dry run deletion set: %w", err)
			}
			res.Status = ExpireDryRun
			res.FilesDeleted = len(paths)
			for _, p := range paths {
				res.BytesDeleted += sizes[p]
			}
			finished = true
			return nil
		}

		committed, err := txn.Commit(ctx)
		if err != nil {
			return fmt.Errorf("commit expiry: %w", err)
		}
		// Only snapshots the committed table really lacks count as expired.
		expired = expired[:0]
		for _, s := range snapshotsByID(md, removed) {
			if committed.Metadata().SnapshotByID(s.SnapshotID) == nil {
				expired = append(expired, s)
			}
		}
		res.SnapshotsExpired = len(expired)
		res.CurrentSnapshot = snapshotIDOf(committed)
		if fsys, err = committed.FS(ctx); err != nil {
			return fmt.Errorf("%w: committed table io: %w", errExpireCommitted, err)
		}
		paths, sizes, err = expireDeletionSet(ctx, fsys, committed.Metadata().Snapshots(), expired)
		if err != nil {
			return fmt.Errorf("%w: deletion set: %w", errExpireCommitted, err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errExpireCommitted) {
			// The expiry landed but the files to delete are unknown: they
			// are orphans now, left to the orphan pass.
			slog.Warn("expire: committed, deletion set unavailable", "table", ident, "err", err)
			res.Status = ExpireExpiredPartial
			res.Duration = time.Since(start)
			return res, nil
		}
		return nil, fmt.Errorf("ExpireTableSnapshots %v: %w", ident, err)
	}
	if finished {
		res.Duration = time.Since(start)
		return res, nil
	}

	deleted, left, bytes := deleteFiles(ctx, fsys, paths, sizes)
	res.FilesDeleted, res.FilesLeft, res.BytesDeleted = deleted, left, bytes
	res.Status = ExpireExpired
	if left > 0 {
		res.Status = ExpireExpiredPartial
	}
	res.Duration = time.Since(start)
	return res, nil
}

// errExpireCommitted marks a failure after the expiry commit landed.
var errExpireCommitted = errors.New("expiry committed")

// olderThanFromCutoff converts the cutoff into the age iceberg-go measures
// against its own clock, so an injected Now and iceberg-go agree on which
// snapshots are old. A cutoff in the future clamps to 0 (every existing
// snapshot is old).
func olderThanFromCutoff(cutoffMs int64) time.Duration {
	age := time.Now().UnixMilli() - cutoffMs
	if age < 0 {
		age = 0
	}
	return time.Duration(age) * time.Millisecond
}

// mainChain returns main's head and its ancestors, newest first.
func mainChain(md icebergtable.Metadata) []icebergtable.Snapshot {
	head := md.SnapshotByName(icebergtable.MainBranch)
	if head == nil {
		return nil
	}
	return icebergtable.AncestorsOf(head.SnapshotID, md.SnapshotByID)
}

// effectiveRetainLast raises retainLast so that every snapshot of main
// superseded after cutoffMs (its child committed after the cutoff) is kept.
func effectiveRetainLast(md icebergtable.Metadata, cutoffMs int64, retainLast int) int {
	chain := mainChain(md)
	count := 0
	for i := 1; i < len(chain); i++ {
		if chain[i-1].TimestampMs > cutoffMs {
			count++
		}
	}
	return max(retainLast, count+1)
}

// keepSet is every snapshot this expiry must not remove: main's head, the
// first effective snapshots of main, and every snapshot another ref points to.
func keepSet(md icebergtable.Metadata, cutoffMs int64, effective int) map[int64]struct{} {
	ks := map[int64]struct{}{}
	for i, s := range mainChain(md) {
		if i < effective {
			ks[s.SnapshotID] = struct{}{}
		}
	}
	for name, ref := range md.Refs() {
		if name != icebergtable.MainBranch {
			ks[ref.SnapshotID] = struct{}{}
		}
	}
	return ks
}

// removedSnapshotIDs lists the snapshots of base that staged no longer has.
func removedSnapshotIDs(base, staged icebergtable.Metadata) []int64 {
	var out []int64
	for _, s := range base.Snapshots() {
		if staged.SnapshotByID(s.SnapshotID) == nil {
			out = append(out, s.SnapshotID)
		}
	}
	return out
}

// guardKeepSet refuses an expiry that would remove a kept snapshot.
func guardKeepSet(removed []int64, ks map[int64]struct{}) error {
	for _, id := range removed {
		if _, ok := ks[id]; ok {
			return fmt.Errorf("%w: keep-set guard: snapshot %d would be expired (per-ref retention overrides the options)", ErrRefused, id)
		}
	}
	return nil
}

func snapshotsByID(md icebergtable.Metadata, ids []int64) []icebergtable.Snapshot {
	out := make([]icebergtable.Snapshot, 0, len(ids))
	for _, id := range ids {
		if s := md.SnapshotByID(id); s != nil {
			out = append(out, *s)
		}
	}
	return out
}

// expireDeletionSet returns the files only the expired snapshots reference:
// their manifest lists, their manifests and every entry of those manifests
// (DELETED entries included), minus anything a retained snapshot references
// through its manifest list, a manifest, or a non-DELETED entry. bytes holds
// the known size per path (manifest lists have none).
func expireDeletionSet(_ context.Context, fsys iceio.IO, retained, expired []icebergtable.Snapshot) (paths []string, bytes map[string]int64, err error) {
	candidates := map[string]int64{}
	keep := map[string]struct{}{}

	retainedManifests := map[string]iceberg.ManifestFile{}
	for _, s := range retained {
		if s.ManifestList != "" {
			keep[s.ManifestList] = struct{}{}
		}
		mfs, err := s.Manifests(fsys)
		if err != nil {
			return nil, nil, fmt.Errorf("retained snapshot %d manifests: %w", s.SnapshotID, err)
		}
		for _, m := range mfs {
			keep[m.FilePath()] = struct{}{}
			retainedManifests[m.FilePath()] = m
		}
	}

	visited := map[string]struct{}{}
	for _, s := range expired {
		if s.ManifestList != "" {
			candidates[s.ManifestList] = 0
		}
		mfs, err := s.Manifests(fsys)
		if err != nil {
			return nil, nil, fmt.Errorf("expired snapshot %d manifests: %w", s.SnapshotID, err)
		}
		for _, m := range mfs {
			if _, ok := visited[m.FilePath()]; ok {
				continue
			}
			visited[m.FilePath()] = struct{}{}
			candidates[m.FilePath()] = m.Length()
			for e, err := range m.Entries(fsys, false) {
				if err != nil {
					return nil, nil, fmt.Errorf("read manifest %s: %w", m.FilePath(), err)
				}
				candidates[e.DataFile().FilePath()] = e.DataFile().FileSizeBytes()
			}
		}
	}
	if len(candidates) == 0 {
		return nil, candidates, nil
	}

	for path, m := range retainedManifests {
		for e, err := range m.Entries(fsys, false) {
			if err != nil {
				return nil, nil, fmt.Errorf("read manifest %s: %w", path, err)
			}
			if e.Status() != iceberg.EntryStatusDELETED {
				keep[e.DataFile().FilePath()] = struct{}{}
			}
		}
	}

	bytes = map[string]int64{}
	for p, size := range candidates {
		if _, ok := keep[p]; ok {
			continue
		}
		paths = append(paths, p)
		bytes[p] = size
	}
	return paths, bytes, nil
}

// deleteFiles removes paths with bounded concurrency. A file already gone
// counts as deleted; once ctx is done the remaining files are left.
func deleteFiles(ctx context.Context, fsys iceio.IO, paths []string, sizes map[string]int64) (deleted, left int, bytes int64) {
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, expireDeleteConcurrency)
	)
	for _, p := range paths {
		if ctx.Err() != nil {
			mu.Lock()
			left++
			mu.Unlock()
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(p string) {
			defer func() { <-sem; wg.Done() }()
			err := fsys.Remove(p)
			if err != nil && errors.Is(err, fs.ErrNotExist) {
				err = nil
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				left++
				slog.Warn("expire: delete file", "path", p, "err", err)
				return
			}
			deleted++
			bytes += sizes[p]
		}(p)
	}
	wg.Wait()
	return deleted, left, bytes
}
