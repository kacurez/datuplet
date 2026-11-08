package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// TestCompaction_PipelineStage proves iceberg-compactor works as a pipeline
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
