// Package e2e — audit-trail e2e test.
//
// TestAuditTrail_ClusterRun reuses the query seed run (ensureQueryTable,
// scenarios_query_test.go), loads the seeded iceberg table via the lakekeeper
// REST catalog, and asserts that the current snapshot's Summary.Properties
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
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// TestAuditTrail_ClusterRun asserts that the seed run's committed snapshot's
// Summary.Properties contains:
//
//   - datuplet.run-mode    == "cluster"
//   - datuplet.pipeline-api == "datuplet-api"
//   - datuplet.actor       non-empty (triggering user UUID)
//   - datuplet.run-id      == run UUID minted for this run
//
// The seed table is written only by the seed run (its own "<runPrefix>-seed"
// prefix), so the current snapshot is the seed run's commit regardless of
// test order.
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

	namespace, tableName, runID := ensureQueryTable(t)
	if runID == uuid.Nil {
		t.Fatal("RunID is zero — K8sBackend did not populate result.RunID")
	}
	t.Logf("seed run succeeded, run-id=%s", runID)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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

	// Load the table written by the seed run.
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
