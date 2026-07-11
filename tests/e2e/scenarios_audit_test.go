// Package e2e — audit-trail e2e test.
//
// TestAuditTrail_ClusterRun triggers a K8s pipeline run, waits for it
// to succeed, loads the resulting iceberg table via the lakekeeper REST
// catalog, and asserts that the current snapshot's Summary.Properties
// contains the four datuplet.* audit keys written by icebergjob's
// BuildSnapshotSummary helper.
//
// Skips cleanly when:
//   - E2E_K8S != "1"
//   - framework.SharedHarness() == nil (bootstrap not run)
package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// TestAuditTrail_ClusterRun triggers the explicit-tables pipeline and
// asserts that the committed snapshot's Summary.Properties contains:
//
//   - datuplet.run-mode    == "cluster"
//   - datuplet.pipeline-api == "datuplet-api"
//   - datuplet.actor       non-empty (triggering user UUID)
//   - datuplet.run-id      == run UUID minted for this run
func TestAuditTrail_ClusterRun(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "K8s tier requires SetupFGABootstrap to have run in TestMain — see framework/bootstrap.go")
	}

	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Build K8s backend.
	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())

	// Render the explicit-tables-k8s pipeline template.
	pipelinesDir, _ := filepath.Abs("pipelines")
	templatePath := filepath.Join(pipelinesDir, "k8s/explicit-tables.yaml")
	vars := framework.TemplateVars{RunPrefix: runPrefix}
	rendered, err := framework.RenderPipeline(templatePath, vars)
	if err != nil {
		t.Fatalf("render pipeline template: %v", err)
	}
	defer os.Remove(rendered)

	// Run the pipeline and capture the run-id.
	result, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{
		StorageType: "s3",
		Timeout:     3 * time.Minute,
	})
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
	if !result.Success {
		t.Fatalf("pipeline failed (exit %d, type %s):\n  logs: %s",
			result.ExitCode, result.FailureType, result.Logs)
	}

	runID := result.RunID
	if runID.String() == "00000000-0000-0000-0000-000000000000" {
		t.Fatal("RunID is zero — K8sBackend did not populate result.RunID")
	}
	t.Logf("pipeline succeeded, run-id=%s", runID)

	// Build lakekeeper catalog client using alice's impersonation token.
	tp := func(ctx context.Context) (string, error) {
		tok, err := framework.MintTestUserImpersonation(ctx, h.Signer, framework.AliceID)
		if err != nil {
			return "", err
		}
		return tok.Reveal(), nil
	}
	cli, err := catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:          "datuplet-e2e-audit",
		URI:           h.CatalogURI(),
		Warehouse:     h.WarehouseName,
		ProjectID:     h.LakekeeperProjectID,
		TokenProvider: catalogwriter.TokenProvider(tp),
	})
	if err != nil {
		t.Fatalf("open lakekeeper catalog: %v", err)
	}

	// Load the table written by the pipeline.
	namespace := runPrefix + "-staging"
	tableName := "posts"

	tbl, err := cli.Catalog.LoadTable(ctx, catalog.ToIdentifier(namespace, tableName))
	if err != nil {
		t.Fatalf("LoadTable %s.%s: %v", namespace, tableName, err)
	}

	snap := tbl.CurrentSnapshot()
	if snap == nil {
		t.Fatalf("CurrentSnapshot() returned nil for %s.%s — no commits found", namespace, tableName)
	}

	if snap.Summary == nil || snap.Summary.Properties == nil {
		t.Fatalf("snapshot %d has nil Summary or nil Properties — BuildSnapshotSummary not wired", snap.SnapshotID)
	}

	props := snap.Summary.Properties
	t.Logf("snapshot %d properties: %v", snap.SnapshotID, props)

	// Assert datuplet.run-mode == "cluster"
	if got := props["datuplet.run-mode"]; got != "cluster" {
		t.Errorf("datuplet.run-mode: got %q, want %q", got, "cluster")
	}

	// Assert datuplet.pipeline-api == "datuplet-api" (JWT issuer)
	if got := props["datuplet.pipeline-api"]; got != "datuplet-api" {
		t.Errorf("datuplet.pipeline-api: got %q, want %q", got, "datuplet-api")
	}

	// Assert datuplet.actor is non-empty (triggering user UUID)
	if got := props["datuplet.actor"]; got == "" {
		t.Errorf("datuplet.actor: empty; expected the triggering user UUID")
	} else {
		t.Logf("datuplet.actor: %s", got)
	}

	// Assert datuplet.run-id matches the run-id from the trigger.
	// Compare case-insensitively to guard against UUID canonicalization drift.
	gotRunID := props["datuplet.run-id"]
	if !strings.EqualFold(gotRunID, runID.String()) {
		t.Errorf("datuplet.run-id: got %q, want %q (case-insensitive)", gotRunID, runID.String())
	} else {
		t.Logf("datuplet.run-id matches: %s", gotRunID)
	}
}
