package icebergjob

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
)

const (
	RewriteRewritten   = "REWRITTEN"
	RewriteNothingToDo = "NOTHING_TO_DO"
	RewriteDryRun      = "DRY_RUN"

	defaultManifestTargetSize = 8 << 20
)

// RewriteTableManifests merges the small manifests of ident's current
// snapshot into target-sized ones, committed as a metadata-only REPLACE
// snapshot. No data file is touched. iceberg-go replays the commit on a 409
// itself and that is safe here (the plan is re-derived from the fresh parent),
// so there is no gateway-side retry. A dry run plans the merge but never
// commits, and removes the manifests the planning wrote.
func RewriteTableManifests(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, opts RewriteManifestsOpts) (*RewriteManifestsResult, error) {
	start := time.Now()
	target := opts.TargetManifestSizeBytes
	if target <= 0 {
		target = defaultManifestTargetSize
	}

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("RewriteTableManifests: load table %v: %w", ident, err)
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("RewriteTableManifests: table io: %w", err)
	}
	res := &RewriteManifestsResult{SnapshotBefore: snapshotIDOf(tbl)}
	res.SnapshotAfter = res.SnapshotBefore
	if snap := tbl.CurrentSnapshot(); snap != nil {
		mfs, err := snap.Manifests(fsys)
		if err != nil {
			return nil, fmt.Errorf("RewriteTableManifests: read manifests: %w", err)
		}
		res.ManifestsBefore = len(mfs)
	}
	res.ManifestsAfter = res.ManifestsBefore

	txn := tbl.NewTransaction()
	rw, err := txn.RewriteManifests(ctx, icebergtable.WithManifestTargetSize(target))
	if err != nil {
		return nil, fmt.Errorf("RewriteTableManifests: plan %v: %w", ident, err)
	}
	if rw.IsNoOp() {
		res.Status = RewriteNothingToDo
		res.Duration = time.Since(start)
		return res, nil
	}

	if opts.DryRun {
		res.Status = RewriteDryRun
		res.ManifestsAfter = res.ManifestsBefore - len(rw.RewrittenManifests) + len(rw.AddedManifests)
		for _, m := range rw.AddedManifests {
			if err := fsys.Remove(m.FilePath()); err != nil {
				log.Printf("WARN: RewriteTableManifests dry run: remove planned manifest %s: %v", m.FilePath(), err)
			}
		}
		res.Duration = time.Since(start)
		return res, nil
	}

	committed, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("RewriteTableManifests: commit %v: %w", ident, err)
	}
	res.Status = RewriteRewritten
	res.SnapshotAfter = snapshotIDOf(committed)
	// The result aliases the winning attempt, so after a 409 replay these are
	// the manifests actually committed.
	res.ManifestsAfter = res.ManifestsBefore - len(rw.RewrittenManifests) + len(rw.AddedManifests)
	if snap := committed.CurrentSnapshot(); snap != nil {
		if mfs, err := snap.Manifests(fsys); err == nil {
			res.ManifestsAfter = len(mfs)
		}
	}
	res.Duration = time.Since(start)
	return res, nil
}
