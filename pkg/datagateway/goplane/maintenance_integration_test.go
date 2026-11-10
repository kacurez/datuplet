//go:build integration

package goplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/catalog"

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
