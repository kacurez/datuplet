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
// capability surface that must be re-proven against the posture. The 2.0.0
// catalog-mutating functions (iceberg_rollback_to_snapshot, the
// set/remove_iceberg_*_properties family) and dbgen/dsdgen are proven refused by
// the READ_ONLY attach in lockdown_deny_attached_test.go (fake REST catalog, no
// Docker); this file adds the writes that need real vended credentials to get
// past the scan bind (UPDATE / DELETE / iceberg_rewrite_data_files).
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
// the end).
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
	// NOTE (fix 13, DuckDB 2.0.0): iceberg_schema_properties EXISTS in this
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

	// --- iceberg WRITE paths must be refused by the READ_ONLY attach --------
	// The ad-hoc query surface is read-only by product definition, so
	// attachCatalog attaches lk READ_ONLY and DuckDB refuses these writes before
	// any catalog write request (binding the scan still reads catalog metadata
	// and storage). These assertions await a live run (they need the fixture
	// images); until then they are compile-checked only. Defence in depth:
	// lakekeeper's FGA grant stays the per-principal authorization of record (a
	// viewer is vended read-only STS and lakekeeper rejects its commits). The
	// fixture runs authorization=allowall and vends read-WRITE credentials, so a
	// success here could only mean the engine flag is gone. UPDATE, DELETE and
	// iceberg_rewrite_data_files first bind a scan of the table, which needs the
	// real vended credentials — that is why they are asserted here and not only
	// in the fake-catalog matrix.
	writes := []struct{ name, stmt string }{
		{"insert", `INSERT INTO lk."` + seedNamespace + `"."` + seedTable + `" VALUES (9001, 'lockdown-write-probe')`},
		{"update", `UPDATE lk."qe"."posts" SET title = 'lockdown-write-probe'`},
		{"delete", `DELETE FROM lk."qe"."posts"`},
		{"iceberg_rewrite_data_files", `SELECT * FROM iceberg_rewrite_data_files('lk.qe.posts', rewrite_all := true)`},
	}
	for _, p := range writes {
		t.Run("iceberg-write/"+p.name, func(t *testing.T) {
			_, err := e.conn.ExecContext(ctx, p.stmt)
			if err == nil {
				t.Fatalf("SECURITY HOLE: %q SUCCEEDED on the read-only attached catalog.\n  stmt: %s", p.name, p.stmt)
			}
			if !readOnlyDeny.MatchString(err.Error()) {
				t.Fatalf("%q must be refused by the READ_ONLY attach, got a different error: %v", p.name, err)
			}
		})
	}

	// --- INVARIANT: no local files staged by ANY of the above ----------------
	// The load-bearing guarantee of this whole block: whatever happened above
	// (denied reads, refused writes), the engine's local scratch dir
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
