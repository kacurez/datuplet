package integration

import (
	"os"
	"testing"
)

// TestTableCommitAndCatalog is parked. The legacy assertion shape (read
// DG's _schema-{runId}.json, _manifest-{runId}.json, then verify the
// bespoke v*.metadata.json pointer + manifest list TableCommit produced)
// is obsolete: TableCommit now commits via lakekeeper's REST catalog
// and owns no Iceberg metadata directly. The DG↔TableCommit signal uses
// per-table files.json manifests, so this test would need to stand up a
// real lakekeeper. A replacement e2e smoke test covers the full live
// pipeline; external DuckDB-iceberg query. Until then this exists only to
// tell operators who set INTEGRATION_TEST=1 that the legacy shape is gone.
func TestTableCommitAndCatalog(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Skipping integration test. Set INTEGRATION_TEST=1 to run.")
	}
	t.Skip("Legacy bespoke-metadata assertions removed. " +
		"Replacement e2e smoke covers this via a live lakekeeper.")
}
