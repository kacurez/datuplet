//go:build integration

package goplane

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// manifestCount counts the current snapshot's manifests.
func manifestCount(t *testing.T, cat catalog.Catalog, ns, tbl string) (int, int64) {
	t.Helper()
	ctx := context.Background()
	tb, err := cat.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatal(err)
	}
	fsys, err := tb.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snap := tb.CurrentSnapshot()
	if snap == nil {
		return 0, 0
	}
	mfs, err := snap.Manifests(fsys)
	if err != nil {
		t.Fatal(err)
	}
	return len(mfs), snap.SnapshotID
}

func TestRewriteManifests_MergesAndKeepsRows(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch, inputs := allTypesSchema(t)
	const ns, tbl = "gw_it", "rewrite_merge"
	target := createTable(t, r, ns, tbl, sch)
	for i := 1; i <= 10; i++ {
		files := writeRun(t, p, target, fmt.Sprintf("part-run%05d-w1", i), sch, inputs, 1, target.VendedCreds, 0)
		commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, fmt.Sprintf("key-run%d", i))
	}
	rowsBefore, _ := scanRows(t, r, ns, tbl)

	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before, snapBefore := manifestCount(t, cat, ns, tbl)
	res, err := icebergjob.RewriteTableManifests(ctx, cat, catalog.ToIdentifier(ns, tbl), icebergjob.RewriteManifestsOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != icebergjob.RewriteRewritten || res.ManifestsBefore != before || res.ManifestsAfter >= res.ManifestsBefore {
		t.Fatalf("result = %+v (manifests before %d)", res, before)
	}
	after, snapAfter := manifestCount(t, cat, ns, tbl)
	if after != res.ManifestsAfter || res.SnapshotBefore != snapBefore || res.SnapshotAfter != snapAfter || snapAfter == snapBefore {
		t.Fatalf("result %+v disagrees with table: manifests %d, snapshot %d -> %d", res, after, snapBefore, snapAfter)
	}
	if n, _ := scanRows(t, r, ns, tbl); n != rowsBefore {
		t.Fatalf("rows %d, want %d", n, rowsBefore)
	}

	// Already merged: a second run has nothing to do.
	res, err = icebergjob.RewriteTableManifests(ctx, cat, catalog.ToIdentifier(ns, tbl), icebergjob.RewriteManifestsOpts{})
	if err != nil || res.Status != icebergjob.RewriteNothingToDo {
		t.Fatalf("second run = %+v, %v", res, err)
	}
}

func TestRewriteManifests_DryRunCommitsNothing(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch, inputs := allTypesSchema(t)
	const ns, tbl = "gw_it", "rewrite_dry"
	target := createTable(t, r, ns, tbl, sch)
	for i := 1; i <= 5; i++ {
		files := writeRun(t, p, target, fmt.Sprintf("part-run%05d-w1", i), sch, inputs, 1, target.VendedCreds, 0)
		commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, fmt.Sprintf("key-run%d", i))
	}
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before, snapBefore := manifestCount(t, cat, ns, tbl)
	res, err := icebergjob.RewriteTableManifests(ctx, cat, catalog.ToIdentifier(ns, tbl), icebergjob.RewriteManifestsOpts{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != icebergjob.RewriteDryRun || res.SnapshotAfter != snapBefore || res.ManifestsAfter >= before {
		t.Fatalf("result = %+v", res)
	}
	if after, snapAfter := manifestCount(t, cat, ns, tbl); after != before || snapAfter != snapBefore {
		t.Fatalf("dry run changed the table: manifests %d -> %d, snapshot %d -> %d", before, after, snapBefore, snapAfter)
	}
}

func TestRewriteManifests_EmptyTable_NothingToDo(t *testing.T) {
	fx := sharedFixture(t)
	r := resolver(t, fx)
	sch, _ := allTypesSchema(t)
	const ns, tbl = "gw_it", "rewrite_empty"
	createTable(t, r, ns, tbl, sch)
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := icebergjob.RewriteTableManifests(ctx, cat, catalog.ToIdentifier(ns, tbl), icebergjob.RewriteManifestsOpts{})
	if err != nil || res.Status != icebergjob.RewriteNothingToDo || res.SnapshotBefore != 0 || res.SnapshotAfter != 0 {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

// fileExists reports whether path is readable through the table's IO.
func fileExists(t *testing.T, cat catalog.Catalog, ns, tbl, path string) bool {
	t.Helper()
	ctx := context.Background()
	tb, err := cat.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatal(err)
	}
	fsys, err := tb.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, err := fsys.Open(path)
	if err != nil {
		return false
	}
	_ = f.Close()
	return true
}

func snapshotIDs(t *testing.T, cat catalog.Catalog, ns, tbl string) []int64 {
	t.Helper()
	tb, err := cat.LoadTable(context.Background(), catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatal(err)
	}
	var out []int64
	for _, s := range tb.Metadata().Snapshots() {
		out = append(out, s.SnapshotID)
	}
	return out
}

// later simulates a clock 200 h ahead, so every fixture snapshot is older
// than the 168 h default.
func later() time.Time { return time.Now().Add(200 * time.Hour) }

// seedAppends writes n single-file APPEND runs and returns the data files.
func seedAppends(t *testing.T, fx *fixture, ns, tbl string, n int) (*lakekeeper.Resolver, []string) {
	t.Helper()
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch, inputs := allTypesSchema(t)
	target := createTable(t, r, ns, tbl, sch)
	var written []string
	for i := 1; i <= n; i++ {
		files := writeRun(t, p, target, fmt.Sprintf("part-run%05d-w1", i), sch, inputs, 1, target.VendedCreds, 0)
		commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, fmt.Sprintf("key-run%d", i))
		written = append(written, paths(files)...)
	}
	return r, written
}

func TestExpire_CompactedFilesFreed(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_it", "expire_compacted"
	r, written := seedAppends(t, fx, ns, tbl, 6)
	rowsBefore, _ := scanRows(t, r, ns, tbl)
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ident := catalog.ToIdentifier(ns, tbl)
	cres, err := icebergjob.CompactTable(ctx, cat, ident, icebergjob.CompactOpts{}, nil, icebergjob.CompactOperationKey("run-1", "compact", ns, tbl))
	if err != nil || cres.Status != icebergjob.CompactCompacted {
		t.Fatalf("CompactTable = %+v, %v", cres, err)
	}
	for _, p := range written {
		if !fileExists(t, cat, ns, tbl, p) {
			t.Fatalf("%s gone before expiry", p)
		}
	}

	res, err := icebergjob.ExpireTableSnapshots(ctx, cat, ident, icebergjob.ExpireOpts{OlderThan: 168 * time.Hour, RetainLast: 1, Now: later})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != icebergjob.ExpireExpired || res.SnapshotsExpired != 6 || res.FilesLeft != 0 || res.FilesDeleted < len(written) || res.BytesDeleted <= 0 {
		t.Fatalf("result = %+v", res)
	}
	if ids := snapshotIDs(t, cat, ns, tbl); len(ids) != 1 || ids[0] != res.CurrentSnapshot || res.CurrentSnapshot != cres.SnapshotAfter {
		t.Fatalf("snapshots after expiry %v, result %+v, compaction %+v", ids, res, cres)
	}
	for _, p := range written {
		if fileExists(t, cat, ns, tbl, p) {
			t.Fatalf("pre-compaction file %s still in storage", p)
		}
	}
	if n, _ := scanRows(t, r, ns, tbl); n != rowsBefore {
		t.Fatalf("rows %d, want %d", n, rowsBefore)
	}

	// Re-run: only the current snapshot is left.
	res, err = icebergjob.ExpireTableSnapshots(ctx, cat, ident, icebergjob.ExpireOpts{OlderThan: 168 * time.Hour, RetainLast: 1, Now: later})
	if err != nil || res.Status != icebergjob.ExpireNothingToDo {
		t.Fatalf("re-run = %+v, %v", res, err)
	}
}

func TestExpire_ReaderProtection_KeepsRecentlySuperseded(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_it", "expire_reader"
	r, _ := seedAppends(t, fx, ns, tbl, 2)
	time.Sleep(2 * time.Second)
	seedMore := func() {
		p := New(Config{})
		defer p.Close()
		sch, inputs := allTypesSchema(t)
		target := createTable(t, r, ns, tbl, sch)
		files := writeRun(t, p, target, "part-run00003-w1", sch, inputs, 1, target.VendedCreds, 0)
		commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, "key-run3")
	}
	seedMore()
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ident := catalog.ToIdentifier(ns, tbl)
	tb, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	head := tb.CurrentSnapshot()
	parent := tb.Metadata().SnapshotByID(*head.ParentSnapshotID)
	// Cutoff 1 s before the head's commit: snapshots 1 and 2 are older than
	// the cutoff by their own commit time, but 2 was superseded after it, so a
	// reader may still be on it. retain_last 1 alone would expire it.
	cutoff := time.UnixMilli(head.TimestampMs - 1000)
	if parent.TimestampMs >= cutoff.UnixMilli() {
		t.Fatalf("fixture timing: parent %d not before cutoff %d", parent.TimestampMs, cutoff.UnixMilli())
	}
	now := func() time.Time { return cutoff.Add(168 * time.Hour) }
	res, err := icebergjob.ExpireTableSnapshots(ctx, cat, ident, icebergjob.ExpireOpts{OlderThan: 168 * time.Hour, RetainLast: 1, Now: now})
	if err != nil || res.Status != icebergjob.ExpireExpired || res.SnapshotsExpired != 1 {
		t.Fatalf("result = %+v, %v; want only the grandparent expired", res, err)
	}
	ids := snapshotIDs(t, cat, ns, tbl)
	if len(ids) != 2 || !slices.Contains(ids, parent.SnapshotID) || !slices.Contains(ids, head.SnapshotID) {
		t.Fatalf("snapshots %v, want head %d and parent %d", ids, head.SnapshotID, parent.SnapshotID)
	}
	if !fileExists(t, cat, ns, tbl, parent.ManifestList) {
		t.Fatalf("parent manifest list %s deleted", parent.ManifestList)
	}
}

func TestExpire_RefRetentionOverride_Refused(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_it", "expire_refoverride"
	r, _ := seedAppends(t, fx, ns, tbl, 3)
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ident := catalog.ToIdentifier(ns, tbl)
	tb, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	head := tb.CurrentSnapshot().SnapshotID
	// A foreign engine's per-ref retention on main: keep 1 snapshot, max age
	// 1 ms. Both override the call options inside iceberg-go
	// (table/transaction.go:817-818); max-snapshot-age-ms alone would not,
	// because the call's retain_last still keeps every recent snapshot.
	if _, _, err := cat.CommitTable(ctx, ident,
		[]table.Requirement{table.AssertRefSnapshotID(table.MainBranch, &head)},
		[]table.Update{table.NewSetSnapshotRefUpdate(table.MainBranch, head, table.BranchRef, 0, 1, 1)}); err != nil {
		t.Fatalf("set main retention: %v", err)
	}
	before := snapshotIDs(t, cat, ns, tbl)

	res, err := icebergjob.ExpireTableSnapshots(ctx, cat, ident, icebergjob.ExpireOpts{OlderThan: 168 * time.Hour, RetainLast: 10})
	if !errors.Is(err, icebergjob.ErrRefused) || !strings.Contains(err.Error(), "keep-set guard") {
		t.Fatalf("ExpireTableSnapshots = %+v, %v; want keep-set ErrRefused", res, err)
	}
	if after := snapshotIDs(t, cat, ns, tbl); len(after) != len(before) {
		t.Fatalf("snapshots %v -> %v", before, after)
	}
}

// appendBeforeFirstCommit commits one APPEND through the wrapped catalog
// right before the first CommitTable it forwards.
type appendBeforeFirstCommit struct {
	catalog.Catalog
	once    sync.Once
	append  func()
	commits int
}

func (c *appendBeforeFirstCommit) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.once.Do(c.append)
	c.commits++
	return c.Catalog.CommitTable(ctx, ident, reqs, updates)
}

func TestExpire_ConcurrentAppend_Recomputes(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_it", "expire_concurrent"
	r, _ := seedAppends(t, fx, ns, tbl, 3)
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := New(Config{})
	defer p.Close()
	sch, inputs := allTypesSchema(t)
	target := createTable(t, r, ns, tbl, sch)
	var late []string
	wrapped := &appendBeforeFirstCommit{Catalog: cat, append: func() {
		files := writeRun(t, p, target, "part-late00001-w1", sch, inputs, 1, target.VendedCreds, 0)
		commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, "key-late")
		late = paths(files)
	}}
	res, err := icebergjob.ExpireTableSnapshots(ctx, wrapped, catalog.ToIdentifier(ns, tbl), icebergjob.ExpireOpts{OlderThan: 168 * time.Hour, RetainLast: 1, Now: later})
	if err != nil {
		t.Fatal(err)
	}
	if wrapped.commits != 2 {
		t.Fatalf("expire commits = %d, want 2 (409 then recompute)", wrapped.commits)
	}
	// The recompute sees the late append as head and expires the other 3.
	if res.Status != icebergjob.ExpireExpired || res.SnapshotsExpired != 3 {
		t.Fatalf("result = %+v", res)
	}
	ids := snapshotIDs(t, cat, ns, tbl)
	if len(ids) != 1 || ids[0] != res.CurrentSnapshot {
		t.Fatalf("snapshots %v, result %+v", ids, res)
	}
	for _, p := range late {
		if !fileExists(t, cat, ns, tbl, p) {
			t.Fatalf("late appended file %s deleted", p)
		}
	}
	if n, _ := scanRows(t, r, ns, tbl); n != 4*6 {
		t.Fatalf("rows %d, want %d", n, 4*6)
	}
}

func TestExpire_SingleSnapshot_NothingToDo(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_it", "expire_single"
	r, written := seedAppends(t, fx, ns, tbl, 1)
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := icebergjob.ExpireTableSnapshots(ctx, cat, catalog.ToIdentifier(ns, tbl), icebergjob.ExpireOpts{OlderThan: 168 * time.Hour, RetainLast: 1, Now: later})
	if err != nil || res.Status != icebergjob.ExpireNothingToDo || res.CurrentSnapshot == 0 {
		t.Fatalf("result = %+v, %v", res, err)
	}
	if !fileExists(t, cat, ns, tbl, written[0]) {
		t.Fatal("data file deleted")
	}
}

func TestExpire_DryRun_DeletesNothing(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_it", "expire_dry"
	r, written := seedAppends(t, fx, ns, tbl, 4)
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ident := catalog.ToIdentifier(ns, tbl)
	if res, err := icebergjob.CompactTable(ctx, cat, ident, icebergjob.CompactOpts{MinInputFiles: 2}, nil, icebergjob.CompactOperationKey("run-1", "compact", ns, tbl)); err != nil || res.Status != icebergjob.CompactCompacted {
		t.Fatalf("CompactTable = %+v, %v", res, err)
	}
	before := snapshotIDs(t, cat, ns, tbl)
	res, err := icebergjob.ExpireTableSnapshots(ctx, cat, ident, icebergjob.ExpireOpts{OlderThan: 168 * time.Hour, RetainLast: 1, DryRun: true, Now: later})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != icebergjob.ExpireDryRun || res.SnapshotsExpired != len(before)-1 || res.FilesDeleted < len(written) {
		t.Fatalf("result = %+v (snapshots %d)", res, len(before))
	}
	if after := snapshotIDs(t, cat, ns, tbl); len(after) != len(before) {
		t.Fatalf("dry run expired snapshots: %v -> %v", before, after)
	}
	for _, p := range written {
		if !fileExists(t, cat, ns, tbl, p) {
			t.Fatalf("dry run deleted %s", p)
		}
	}
}

func TestExpire_GCDisabled_Refused(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_it", "expire_nogc"
	r, _ := seedAppends(t, fx, ns, tbl, 2)
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ident := catalog.ToIdentifier(ns, tbl)
	tb, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	txn := tb.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{"gc.enabled": "false"}); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatalf("set gc.enabled: %v", err)
	}
	res, err := icebergjob.ExpireTableSnapshots(ctx, cat, ident, icebergjob.ExpireOpts{OlderThan: 168 * time.Hour, RetainLast: 1, Now: later})
	if !errors.Is(err, icebergjob.ErrRefused) {
		t.Fatalf("ExpireTableSnapshots = %+v, %v; want ErrRefused", res, err)
	}
	if ids := snapshotIDs(t, cat, ns, tbl); len(ids) != 2 {
		t.Fatalf("snapshots %v, want 2", ids)
	}
}
