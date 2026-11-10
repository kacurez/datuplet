package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// TestCompaction_PipelineStage proves iceberg-maintenance works as a pipeline
// stage: a bucket[8]-partitioned table gets 16 small files from two APPEND
// runs, then a compactor stage rewrites them into at most one file per
// partition in a single `replace` snapshot, leaving the rows unchanged.
func TestCompaction_PipelineStage(t *testing.T) {
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "K8s e2e requires E2E_K8S=1 and SetupFGABootstrap to have run in TestMain — see framework/bootstrap.go")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())
	target := framework.NewK8sQueryTarget(ctx, t, h)
	ns := runPrefix + "-compact"

	run := func(fixture string) {
		t.Helper()
		rendered, err := framework.RenderPipeline(filepath.Join("pipelines", "k8s", fixture), framework.TemplateVars{RunPrefix: runPrefix})
		if err != nil {
			t.Fatalf("render %s: %v", fixture, err)
		}
		defer os.Remove(rendered)
		res, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3", Timeout: 5 * time.Minute})
		if err != nil {
			t.Fatalf("run %s: %v", fixture, err)
		}
		if !res.Success {
			t.Fatalf("run %s failed (exit %d, %s): %s\nlogs:\n%s", fixture, res.ExitCode, res.FailureType, res.StatusMessage, res.Logs)
		}
	}

	run("compaction-seed.yaml")
	run("compaction-seed.yaml")

	const rows = 800
	framework.AssertRowCount(t, target, ns, "events", rows)
	// Guard the premise: 2 runs x up to 8 partitions, so compaction has work.
	if n := framework.DataFileCount(t, target, ns, "events"); n < 16 {
		t.Fatalf("seed produced %d data files, want >= 16", n)
	}

	run("compaction.yaml")

	framework.AssertDataFileCount(t, target, ns, "events", 8)
	framework.AssertRowCount(t, target, ns, "events", rows)
	framework.AssertLatestSnapshotReplaceWithCommitKey(t, target, ns, "events")
}

// TestCompaction_OnDemand proves the on-demand route: after 5 seed runs each of
// the 8 partitions holds 5 small files (the route uses the compactor's default
// min_input_files of 5), POST .../compact answers 202, the hidden system
// pipeline's run Succeeds, the file count drops to at most one per partition
// and the row count is unchanged. A second call right after the 202 must be
// refused with 409 compaction_running.
func TestCompaction_OnDemand(t *testing.T) {
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "K8s e2e requires E2E_K8S=1 and SetupFGABootstrap to have run in TestMain — see framework/bootstrap.go")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// RunAsUser defaults to Alice (the seeded admin, project_admin => data_admin),
	// which the compact route requires.
	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())
	target := framework.NewK8sQueryTarget(ctx, t, h)
	// Distinct namespace from TestCompaction_PipelineStage so they don't interfere.
	ns := runPrefix + "-compact-od"

	const seedRuns, rows = 5, 400 * 5
	for i := 0; i < seedRuns; i++ {
		rendered, err := framework.RenderPipeline(filepath.Join("pipelines", "k8s", "compaction-od-seed.yaml"), framework.TemplateVars{RunPrefix: runPrefix})
		if err != nil {
			t.Fatalf("render seed: %v", err)
		}
		res, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3", Timeout: 5 * time.Minute})
		os.Remove(rendered)
		if err != nil {
			t.Fatalf("seed run %d: %v", i, err)
		}
		if !res.Success {
			t.Fatalf("seed run %d failed (exit %d, %s): %s\nlogs:\n%s", i, res.ExitCode, res.FailureType, res.StatusMessage, res.Logs)
		}
	}

	framework.AssertRowCount(t, target, ns, "events", rows)
	before := framework.DataFileCount(t, target, ns, "events")
	if before < 5*seedRuns {
		t.Fatalf("seed produced %d data files, want >= %d", before, 5*seedRuns)
	}

	first, err := kb.CompactTable(ctx, ns, "events")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if first.Status != 202 || first.RunID == "" {
		t.Fatalf("compact: status=%d want 202 with run_id; body=%s", first.Status, first.Body)
	}

	// Immediately re-issue: the first run needs a pod start plus a rewrite, so
	// it is still Pending/Running. If it somehow finished already the route
	// legitimately accepts a second compaction (202); that is a lost race, not
	// a bug, so we accept it, wait for that run too, and only log. 409 is the
	// expected, asserted-when-seen outcome.
	second, err := kb.CompactTable(ctx, ns, "events")
	if err != nil {
		t.Fatalf("second compact: %v", err)
	}
	switch second.Status {
	case 409:
		if second.Code != "compaction_running" {
			t.Fatalf("second compact: 409 code=%q want compaction_running; body=%s", second.Code, second.Body)
		}
	case 202:
		t.Logf("second compact got 202: first run finished before the retry (race lost); waiting for run %s too", second.RunID)
		if err := kb.WaitRunSucceeded(ctx, second.RunID); err != nil {
			t.Fatalf("second compaction run: %v", err)
		}
	default:
		t.Fatalf("second compact: status=%d want 409 (or 202 if the first run already finished); body=%s", second.Status, second.Body)
	}

	if err := kb.WaitRunSucceeded(ctx, first.RunID); err != nil {
		t.Fatalf("compaction run: %v", err)
	}

	if after := framework.DataFileCount(t, target, ns, "events"); after >= before {
		t.Fatalf("data files %d -> %d, want a drop", before, after)
	}
	framework.AssertDataFileCount(t, target, ns, "events", 8)
	framework.AssertRowCount(t, target, ns, "events", rows)
	framework.AssertLatestSnapshotReplaceWithCommitKey(t, target, ns, "events")
}

// TestMaintenance_PipelineStage runs all four iceberg-maintenance tasks as one
// pipeline stage on a freshly seeded table. At default retention nothing is
// old enough to expire or delete, so expire_snapshots and delete_orphan_files
// must report NOTHING_TO_DO; compact bin-packs the 16 seed files, and
// rewrite_manifests may or may not find work. The rows must be unchanged.
func TestMaintenance_PipelineStage(t *testing.T) {
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "K8s e2e requires E2E_K8S=1 and SetupFGABootstrap to have run in TestMain — see framework/bootstrap.go")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())
	target := framework.NewK8sQueryTarget(ctx, t, h)
	ns := runPrefix + "-maint"

	run := func(fixture string) *framework.RunResult {
		t.Helper()
		rendered, err := framework.RenderPipeline(filepath.Join("pipelines", "k8s", fixture), framework.TemplateVars{RunPrefix: runPrefix})
		if err != nil {
			t.Fatalf("render %s: %v", fixture, err)
		}
		defer os.Remove(rendered)
		res, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3", Timeout: 5 * time.Minute})
		if err != nil {
			t.Fatalf("run %s: %v", fixture, err)
		}
		if !res.Success {
			t.Fatalf("run %s failed (exit %d, %s): %s\nlogs:\n%s", fixture, res.ExitCode, res.FailureType, res.StatusMessage, res.Logs)
		}
		return res
	}

	run("maintenance-seed.yaml")
	run("maintenance-seed.yaml")

	const rows = 800
	framework.AssertRowCount(t, target, ns, "events", rows)
	if n := framework.DataFileCount(t, target, ns, "events"); n < 16 {
		t.Fatalf("seed produced %d data files, want >= 16", n)
	}

	res := run("maintenance.yaml")
	// A succeeded run carries no StatusMessage; the component's summary is the
	// DUPLET_STATUS_MESSAGE line in its logs.
	msg := res.Logs
	if !strings.Contains(msg, "DUPLET_STATUS_MESSAGE:maintenance: ") && !strings.Contains(msg, "DUPLET_STATUS_MESSAGE: maintenance: ") {
		t.Fatalf("no maintenance status line in logs:\n%s", msg)
	}
	for _, want := range []string{
		"compact COMPACTED",
		"expire_snapshots NOTHING_TO_DO",
		"delete_orphan_files NOTHING_TO_DO",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("status line missing %q; logs:\n%s", want, msg)
		}
	}
	// rewrite_manifests depends on how many manifests the seed left.
	if !strings.Contains(msg, "rewrite_manifests REWRITTEN") && !strings.Contains(msg, "rewrite_manifests NOTHING_TO_DO") {
		t.Errorf("status line has no rewrite_manifests success status; logs:\n%s", msg)
	}

	framework.AssertDataFileCount(t, target, ns, "events", 8)
	framework.AssertRowCount(t, target, ns, "events", rows)
}
