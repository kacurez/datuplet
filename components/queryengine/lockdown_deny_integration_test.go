//go:build duckdb_arrow && integration

// RFC 022 Task 1.4 (D) — integration-tagged deny additions.
//
// These probes require the LIVE fixture from integration_test.go (lakekeeper +
// MinIO + seeded table) and the iceberg/httpfs extensions loaded by
// attachCatalog. They run post-ATTACH, post-lock(), on the SAME engine as
// TestAttachIntegration, so the fixture is provisioned exactly once per run.
//
// RE-RUN ON EVERY duckdb-go VERSION BUMP — see the header note in
// lockdown_deny_test.go. New iceberg-extension table functions are new
// capability surface that must be re-proven against the posture.
//
// Run:
//
//	go test -tags 'duckdb_arrow integration' -timeout 600s -run TestAttachIntegration ./...
package queryengine

import (
	"context"
	"os"
	"strings"
	"testing"
)

// runLockdownDenyIntegration exercises the post-attach, post-lock deny matrix
// that needs the live catalog + loaded iceberg extension. e is already attached
// and locked; engineTempDir is the engine's scratch dir (asserted file-free at
// the end). It never hard-fails the INSERT write-path probe — see below.
func runLockdownDenyIntegration(ctx context.Context, t *testing.T, e *engine, engineTempDir string) {
	t.Helper()

	// --- iceberg-extension functions against a LOCAL path must error ---------
	// disabled_filesystems='LocalFileSystem' must block the iceberg table
	// functions just like the core read_* functions. A local '/tmp/x' metadata
	// path has no catalog-side credential vending, so the only thing it can
	// touch is the local FS — which is disabled. We assert the FS-layer marker
	// (LocalFileSystem / "disabled"), NOT just error presence: that proves the
	// deny fired at the disabled-FS layer, not at file-not-found / wrong-format /
	// parse. Verbatim 1.5.3: "Permission Error: File system LocalFileSystem has
	// been disabled by configuration".
	//
	// NOTE (fix 13, DuckDB 1.5.3): iceberg_schema_properties EXISTS in this
	// extension build but does NOT take a metadata path — against a local '/tmp/x'
	// it denies at the CATALOG layer ("Schema with name /tmp/x does not exist"),
	// never reaching the FS. It is therefore NOT a meaningful FS-layer deny row
	// and is deliberately omitted rather than asserted at the wrong layer. The
	// path-taking functions iceberg_scan / iceberg_metadata / iceberg_snapshots
	// are the ones that route to the FS. (iceberg_schema / iceberg_properties /
	// iceberg_table_information do not exist in 1.5.3.)
	icebergLocal := []struct {
		name string
		stmt string
	}{
		{"iceberg_scan-local", `SELECT * FROM iceberg_scan('/tmp/x')`},
		{"iceberg_metadata-local", `SELECT * FROM iceberg_metadata('/tmp/x')`},
		{"iceberg_snapshots-local", `SELECT * FROM iceberg_snapshots('/tmp/x')`},
	}
	for _, p := range icebergLocal {
		t.Run("iceberg-local/"+p.name, func(t *testing.T) {
			_, err := e.conn.ExecContext(ctx, p.stmt)
			if err == nil {
				t.Fatalf("SECURITY HOLE: %q SUCCEEDED post-lock against a local path; want error.\n  stmt: %s",
					p.name, p.stmt)
			}
			t.Logf("denied (%s): %v", p.name, err)
			if msg := err.Error(); !strings.Contains(msg, "LocalFileSystem") && !strings.Contains(msg, "disabled") {
				t.Fatalf("%q must deny at the FS layer (error must name LocalFileSystem / \"disabled\"), got: %v",
					p.name, err)
			}
		})
	}

	// --- iceberg WRITE path probe (RECORD, do not hard-fail) -----------------
	// Attempt a catalog write post-lock. We RECORD the outcome rather than
	// assert a direction:
	//   - The fixture's lakekeeper runs authorization=allowall, so it cannot
	//     prove catalog-side write-authz denial. Production read-only
	//     enforcement is FGA's job (RFC 022 Phase 2), not this test's.
	//   - SECURITY REVIEW (read-only enforcement locus): production vended-cred
	//     WRITE capability is governed by the CALLER's FGA grants via lakekeeper —
	//     a viewer principal is vended read-only STS, so an INSERT cannot stage
	//     objects or commit metadata regardless of what DuckDB attempts. This
	//     DuckDB-posture layer deliberately does NOT enforce read-only (it has no
	//     identity context); enforcing it here would duplicate authz in the wrong
	//     place. Phase 2 e2e MUST assert INSERT fails for a read-only principal
	//     (viewer ⇒ read-only STS) against a real FGA-backed lakekeeper; until
	//     then this fixture (allowall) cannot and does not prove write denial.
	//   - What this probe DOES guarantee, regardless of the write outcome, is
	//     that no local file is staged in the engine's scratch dir. An iceberg
	//     write stages parquet to OBJECT storage via vended creds, never to the
	//     local FS; if a future code path tried to stage locally,
	//     disabled_filesystems would block it and the empty-dir assertion below
	//     would still hold.
	insertStmt := `INSERT INTO lk."` + seedNamespace + `"."` + seedTable + `" VALUES (9001, 'lockdown-write-probe')`
	if _, err := e.conn.ExecContext(ctx, insertStmt); err != nil {
		t.Logf("iceberg INSERT write-probe DENIED post-lock: %v", err)
		// Categorize for the record: a LocalFileSystem-disabled error proves the
		// posture blocked a local-staging attempt; any other error is a
		// catalog/extension-side refusal (acceptable — FGA owns real authz).
		if strings.Contains(err.Error(), "LocalFileSystem") {
			t.Logf("  (write blocked at the disabled-local-FS layer — no local staging possible)")
		}
	} else {
		t.Logf("iceberg INSERT write-probe SUCCEEDED post-lock " +
			"(expected under allowall fixture authz; production read-only is FGA's job, Phase 2)")
	}

	// --- INVARIANT: no local files staged by ANY of the above ----------------
	// The load-bearing guarantee of this whole block: whatever happened above
	// (denied reads, allowed-or-denied write), the engine's local scratch dir
	// must remain empty. A staged local file would be an exfil channel.
	entries, err := os.ReadDir(engineTempDir)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", engineTempDir, err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, en := range entries {
			names = append(names, en.Name())
		}
		t.Fatalf("SECURITY: %d local file(s) appeared in the engine temp dir from iceberg probes: %v",
			len(entries), names)
	}
	t.Logf("invariant holds: engine temp dir %q is file-free after all iceberg probes", engineTempDir)
}
