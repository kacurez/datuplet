//go:build duckdb_arrow

// RFC 022 Task 1.4 — the SECURITY BACKBONE of the "no SQL gate" claim.
//
// This is a table-driven deny matrix proving that each dangerous DuckDB
// capability errors AFTER lock() applies the security posture (see lock() in
// engine_open.go: autoinstall/autoload/community-ext off,
// allow_unredacted_secrets=false, disabled_filesystems='LocalFileSystem',
// lock_configuration=true LAST). The design ships untrusted user SQL straight
// to DuckDB with no statement-level allow/deny parsing; this file is the proof
// that the posture — not a SQL parser — is what contains the engine.
//
// RE-RUN THIS MATRIX ON EVERY duckdb-go VERSION BUMP. New DuckDB versions add
// capability surface (new table functions, new filesystem readers, new
// extensions). A capability that lands between releases and is NOT covered here
// is an untested escalation path. Proven posture baseline: DuckDB 1.5.3
// (duckdb-go/v2 v2.10503.0), graduated from the RFC 022 Phase 0 lockdown spike.
//
// NOTE on duckdb_extensions(): calling duckdb_extensions() post-lock ERRORS,
// because it reads the on-disk extension directory through the (now disabled)
// LocalFileSystem. Production code must NEVER call duckdb_extensions() after
// lock(). It is intentionally NOT in the allowed matrix below — it is a deny.
//
// Each probe runs on its OWN fresh in-memory engine: the lockdown settings are
// database-GLOBAL, so a poisoned/half-mutated engine from one probe must never
// mask another. In-memory engines open in ~10ms, so per-probe isolation is
// cheap and correct. The parquet/json readers are statically linked into
// duckdb-go, so read_parquet/read_json/parquet_metadata/parquet_schema exist
// without any INSTALL — the deny is the FS layer, not a missing extension.
package queryengine

import (
	"context"
	"os"
	"strings"
	"testing"
)

// lockedEngine opens a fresh in-memory engine with a per-test TempDir and
// applies the lockdown posture via lock(). It deliberately does NOT attach a
// catalog: the core matrix proves the posture in isolation with no network and
// no iceberg/httpfs extensions, so it runs in plain CI. Returns the engine and
// its temp dir; the caller asserts no files were created in that dir.
func lockedEngine(t *testing.T) (*engine, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	e, err := openEngine(ctx, Request{TempDir: dir})
	if err != nil {
		t.Fatalf("openEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.lock(ctx); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if !e.locked {
		t.Fatal("engine must be locked")
	}
	return e, dir
}

// lockedEngineViaAttach is the high-fidelity warm-up helper: it replays the
// PRODUCTION pre-lock sequence by calling attachCatalog against a dead loopback
// endpoint, IGNORING the (expected) ATTACH failure, then lock(). This is the
// single faithful way to reach lock() in the exact state production does:
//
//   - iceberg + httpfs LOADED (attachCatalog INSTALL+LOADs both before ATTACH);
//   - the secret manager WARMED by a real CREATE SECRET of the production
//     ICEBERG-type bearer secret (lk_tok), which initializes the manager via the
//     local stored-secrets dir while the FS is still enabled;
//   - HTTPFileSystem TOUCHED (the /v1/config handshake does a real HTTP GET),
//     so its cert/config lookup-on-first-use already happened pre-lock;
//   - the engine still UNLOCKED (attachCatalog fails at ATTACH, never reaching
//     lock()), so lock() can then apply the posture.
//
// Both the egress error-class test and the secret-manager post-lock probes need
// this exact state, so they share this one helper. A cold engine (no attach)
// would prove an artifact — cold manager + disabled FS, or unloaded httpfs —
// not the real allowed/denied behaviour production exercises. Skips (not fails)
// when iceberg/httpfs are unavailable (offline plain-CI without the extensions
// pre-baked): the warm-up replay is meaningless without them.
//
// Returns the locked engine and its temp dir (callers may assert it stays
// file-free).
func lockedEngineViaAttach(t *testing.T) (*engine, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	e, err := openEngine(ctx, Request{TempDir: dir})
	if err != nil {
		t.Fatalf("openEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })

	// Pre-flight the extensions explicitly so an offline environment SKIPS here
	// (rather than failing inside attachCatalog). On a warm laptop / CI image the
	// extensions are already resident, so these are no-ops; attachCatalog repeats
	// the INSTALL/LOAD harmlessly.
	for _, s := range []string{"INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs"} {
		if _, err := e.conn.ExecContext(ctx, s); err != nil {
			t.Skipf("iceberg/httpfs unavailable (%s: %v); attach-warmed probe needs them", s, err)
		}
	}

	// Production pre-lock replay. attachCatalog LOADs iceberg+httpfs, creates the
	// ICEBERG-type lk_tok secret (warming the manager), does the /v1/config HTTP
	// handshake (warming HTTPFileSystem), then FAILS at ATTACH against the dead
	// endpoint. We require that the failure is at the ATTACH stage — anything
	// earlier (extension setup, CREATE SECRET) means the warm-up did not reach
	// the state we are replaying, so the test would prove the wrong thing.
	attachErr := attachCatalog(ctx, e, Request{
		LakekeeperURL: "http://127.0.0.1:1/catalog",
		Warehouse:     "x/y",
		CatalogJWT:    dummyAttachJWT,
	})
	if attachErr == nil {
		t.Fatal("attachCatalog against a dead endpoint must fail at ATTACH")
	}
	if !strings.Contains(attachErr.Error(), "ATTACH catalog") {
		t.Fatalf("attach-warm reached the wrong pre-lock state (want failure at the ATTACH stage): %v", attachErr)
	}
	if e.locked {
		t.Fatal("engine must be unlocked after a failed ATTACH")
	}
	if err := e.lock(ctx); err != nil {
		t.Fatalf("lock after attach-warm: %v", err)
	}
	if !e.locked {
		t.Fatal("engine must be locked")
	}
	return e, dir
}

// dummyAttachJWT is a structurally valid (unsigned) JWT shape — three base64url
// segments. attachCatalog's shape guard rejects malformed tokens before any SQL
// is built, so the warm-up must use a well-formed shape. It is never validated:
// ATTACH fails at the dead endpoint long before any token check.
const dummyAttachJWT = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJxZSJ9.c2ln"

// assertNoFilesCreated proves a denied write/export probe did not leak a file
// into the engine's scratch dir before erroring. A capability that errors at
// the END of its work (after staging a file) would still be an exfil channel,
// so the empty-dir check is part of the deny assertion, not a nicety.
func assertNoFilesCreated(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", dir, err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("SECURITY: probe created %d file(s) in the engine temp dir despite erroring: %v",
			len(entries), names)
	}
}

// fsLayerMarker reports whether err is a LocalFileSystem-disabled error — the
// proof that the deny fired at the FILESYSTEM layer (disabled_filesystems), not
// at file-not-found / wrong-format / parse. DuckDB 1.5.3's verbatim text is
// "Permission Error: File system LocalFileSystem has been disabled by
// configuration"; we match on either the type name or the word "disabled" so a
// future-version reword of the sentence still counts as long as it names the FS
// deny.
func fsLayerMarker(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "LocalFileSystem") || strings.Contains(msg, "disabled")
}

// TestLockdownDeny is the core deny matrix. Every row MUST error post-lock. A
// row that wrongly SUCCEEDS is a security hole — do not weaken the assertion to
// make it pass; reconcile against lock() in engine_open.go instead.
func TestLockdownDeny(t *testing.T) {
	// category groups rows only for readability of -run output; every row is an
	// independent fresh-engine assertion.
	type probe struct {
		// name is the subtest label.
		name string
		// stmts are the SQL statements run post-lock, in order, on ONE engine.
		// Most rows have a single statement; multi-statement rows (CREATE MACRO
		// then call, PREPARE then EXECUTE) prove enforcement fires at the right
		// stage. "<dir>" in any statement is replaced with the engine's per-test
		// temp dir at run time. The row DENIES iff AT LEAST ONE statement errors;
		// denyStmt records which one is expected to fire (for the layer assertion).
		stmts []string
		// denyStmt is the 0-based index of the statement that must error. Earlier
		// statements (e.g. a CREATE MACRO that is merely a definition) may legally
		// succeed; the deny is asserted on stmts[denyStmt].
		denyStmt int
		// wantFSLayer asserts the deny at stmts[denyStmt] is a LocalFileSystem /
		// "disabled" error — proving it fired at the FILESYSTEM layer, not at
		// file-not-found / wrong-format / parse / catalog. Rows that genuinely
		// deny at a DIFFERENT layer set this false and document the layer in note.
		wantFSLayer bool
		// note documents WHY this row is in the matrix (the threat it pins) and,
		// for non-FS-layer rows, WHICH layer denies and why that is acceptable.
		note string
	}

	// one is a convenience constructor for the common single-statement row.
	one := func(name, stmt string, wantFS bool, note string) probe {
		return probe{name: name, stmts: []string{stmt}, denyStmt: 0, wantFSLayer: wantFS, note: note}
	}

	rows := []probe{
		// --- lock-integrity probes ------------------------------------------
		// These pin the SEMANTICS of the configuration lock. They are
		// restriction-DIRECTION SETs (or config writes): a SET that *succeeded*
		// here would loosen the posture (re-enable a filesystem, raise the memory
		// limit, turn a guard back on) or mutate a frozen knob. A wrongly-
		// succeeding row here is a self-DoS or a loosening, NOT a privilege
		// escalation — but it still means lock_configuration is not pinning what
		// we claim, so it must DENY. These deny at the CONFIG-LOCK layer (an
		// Invalid Input "configuration has been locked" error), not the FS layer,
		// so wantFSLayer is false.
		one("lock-integrity/unlock", `SET lock_configuration=false`, false,
			"un-setting the lock itself must be refused while locked (config-lock layer)"),
		one("lock-integrity/reset-disabled-fs", `RESET disabled_filesystems`, false,
			"RESET of disabled_filesystems would re-enable LocalFileSystem (config-lock layer)"),
		one("lock-integrity/pragma-clear-disabled-fs", `PRAGMA disabled_filesystems=''`, false,
			"PRAGMA form must be locked too, not just SET (config-lock layer)"),
		one("lock-integrity/set-clear-disabled-fs", `SET disabled_filesystems=''`, false,
			"clearing disabled_filesystems would re-enable LocalFileSystem (config-lock layer)"),
		one("lock-integrity/disable-external-access", `SET enable_external_access=false`, false,
			"toggling external-access knobs post-lock must be refused (config-lock layer)"),
		one("lock-integrity/raise-memory-limit", `SET memory_limit='99GB'`, false,
			"resource limits are frozen by the lock — DoS containment (config-lock layer)"),
		one("lock-integrity/enable-autoinstall", `SET autoinstall_known_extensions=true`, false,
			"re-enabling autoinstall would open the extension-download surface (config-lock layer)"),
		one("lock-integrity/enable-unredacted-secrets", `SET allow_unredacted_secrets=true`, false,
			"re-enabling unredacted secrets would expose vended tokens (config-lock layer)"),
		one("lock-integrity/move-temp-dir", `SET temp_directory='/elsewhere'`, false,
			"relocating temp_directory post-lock must be refused (config-lock layer)"),
		// http_proxy is a real DuckDB setting (verified present in 1.5.3); a
		// post-lock write to it must hit the config lock. If a future build drops
		// the setting it would instead report an unknown-setting error — still a
		// deny, but at a different layer; re-check the note then.
		one("lock-integrity/set-http-proxy", `SET http_proxy='http://127.0.0.1:9'`, false,
			"mutating http_proxy post-lock must be refused (config-lock layer; setting exists in 1.5.3)"),

		// --- local-FS capability probes -------------------------------------
		// disabled_filesystems='LocalFileSystem' must block every path that
		// touches the local FS — read, write, export, attach, glob, sniff, and
		// the implicit replacement scan. parquet/json readers are statically
		// linked, so these functions EXIST; the deny is the FS layer. Every row
		// here sets wantFSLayer=true: the error MUST name LocalFileSystem /
		// "disabled", proving the FS layer denied it (not file-not-found / format
		// / parse). Verbatim 1.5.3: "Permission Error: File system LocalFileSystem
		// has been disabled by configuration".
		one("local-fs/copy-to-csv", `COPY (SELECT 1) TO '<dir>/x.csv'`, true,
			"writing a CSV to local disk must be blocked"),
		one("local-fs/export-database", `EXPORT DATABASE '<dir>/exp'`, true,
			"EXPORT DATABASE writes a dir of files to local disk"),
		one("local-fs/attach-db-file", `ATTACH '<dir>/x.db'`, true,
			"attaching a local .db file must be blocked"),
		one("local-fs/read-text-passwd", `SELECT * FROM read_text('/etc/passwd')`, true,
			"reading an arbitrary local file must be blocked"),
		one("local-fs/read-csv-passwd", `SELECT * FROM read_csv('/etc/passwd')`, true,
			"read_csv against a local path must be blocked"),
		one("local-fs/read-blob-passwd", `SELECT * FROM read_blob('/etc/passwd')`, true,
			"read_blob against a local path must be blocked at the FS layer (not file-not-found)"),
		one("local-fs/read-json-osrelease", `SELECT * FROM read_json('/etc/os-release')`, true,
			"read_json (statically linked) against a local path must be blocked"),
		one("local-fs/read-ndjson-osrelease", `SELECT * FROM read_ndjson('/etc/os-release')`, true,
			"read_ndjson (read_json alias family) against a local path must be blocked"),
		// DuckDB 1.5.0+ ships a local .db reader (read_duckdb). It reads the file
		// through LocalFileSystem, so the FS deny fires.
		one("local-fs/read-duckdb", `SELECT * FROM read_duckdb('<dir>/x.db')`, true,
			"read_duckdb (DuckDB 1.5.0 local .db reader) against a local path must be blocked"),
		one("local-fs/read-parquet-missing", `SELECT * FROM read_parquet('<dir>/nope.parquet')`, true,
			"read_parquet must fail at the FS layer, not at file-not-found"),
		one("local-fs/glob-etc", `SELECT * FROM glob('/etc/*')`, true,
			"directory enumeration via glob() must be blocked"),
		// CVE-2024-41672 regression guard: sniff_csv previously bypassed
		// disabled_filesystems and read arbitrary local files. It MUST error.
		one("local-fs/sniff-csv-passwd", `SELECT * FROM sniff_csv('/etc/passwd')`, true,
			"CVE-2024-41672 regression guard: sniff_csv must NOT bypass disabled_filesystems"),
		one("local-fs/parquet-metadata", `SELECT * FROM parquet_metadata('/etc/os-release')`, true,
			"parquet_metadata reads the local file header — must be blocked at the FS layer"),
		one("local-fs/parquet-schema", `SELECT * FROM parquet_schema('/etc/os-release')`, true,
			"parquet_schema reads the local file footer — must be blocked at the FS layer"),
		// The engine's own scratch dir is still LocalFileSystem — reading it
		// back via SQL must be blocked even though the process owns it.
		one("local-fs/read-own-tempdir", `SELECT * FROM read_text('<dir>/anything')`, true,
			"the engine's own temp dir is LocalFileSystem — reading it via SQL is blocked"),

		// --- replacement-scan probes (layer depends on the path's extension) --
		// The implicit replacement scan resolves a quoted path to a table. Its
		// deny LAYER depends on whether DuckDB recognizes the file extension:
		//   - A path with NO data-file extension (/etc/passwd) is not recognized
		//     as a scannable file, so it falls through to name resolution and
		//     denies at the CATALOG layer ("Table with name ... does not exist").
		//     The local FS is never touched — still a deny, but not an FS deny.
		//   - A path WITH a recognized extension (.duckdb) IS routed to the file
		//     reader, which hits the disabled LocalFileSystem → FS-layer deny.
		// Both forms are pinned so a future version that changes the routing (and
		// could open a read path) is caught.
		one("replacement-scan/no-extension-etc-passwd", `SELECT * FROM '/etc/passwd'`, false,
			"replacement scan of an extensionless local path denies at the CATALOG layer "+
				"(unrecognized as a file → never reaches the FS); pinned so a routing change surfaces"),
		one("replacement-scan/duckdb-extension", `SELECT * FROM '<dir>/x.duckdb'`, true,
			"replacement scan of a recognized .duckdb path IS routed to the file reader → FS-layer deny"),

		// --- macro / prepared-statement enforcement-at-execution probes ------
		// A denied call wrapped in a MACRO or a PREPARE must STILL be enforced —
		// the deny fires when the wrapped FS read is actually attempted, not at
		// definition time. Two-statement probes on one engine.
		{
			name:        "indirect/create-macro-then-call",
			stmts:       []string{`CREATE MACRO m1(p) AS TABLE SELECT * FROM read_text(p)`, `SELECT * FROM m1('/etc/passwd')`},
			denyStmt:    1, // the CREATE is just a definition and SUCCEEDS; the call denies.
			wantFSLayer: true,
			note:        "a macro wrapping read_text is enforced at EXECUTION: the call must deny at the FS layer",
		},
		{
			name: "indirect/prepare-binds-eagerly",
			// FINDING (DuckDB 1.5.3): PREPARE binds the plan EAGERLY, so the FS
			// deny fires at PREPARE itself — the wrapped read_text is resolved and
			// denied at PREPARE time, before any EXECUTE. (Empirically, a following
			// `EXECUTE p1` then reports "Prepared statement p1 does not exist"
			// because the PREPARE failed, so it adds nothing and is not run.) The
			// reviewer expected "at least one of PREPARE/EXECUTE" to deny at the FS
			// layer — reality: it is the PREPARE.
			stmts:       []string{`PREPARE p1 AS SELECT * FROM read_text('/etc/passwd')`},
			denyStmt:    0,
			wantFSLayer: true,
			note:        "PREPARE binds eagerly so the wrapped FS read denies at PREPARE time",
		},

		// --- extension probes -----------------------------------------------
		// autoinstall/autoload off + lock_configuration => no new extension can
		// be installed or loaded post-lock. spatial is not bundled; the evil
		// path is an arbitrary local .duckdb_extension file. These deny at the
		// extension/config layer, not the FS layer (wantFSLayer=false), except the
		// local-extension-file LOAD which reads the file via the disabled FS.
		one("extensions/install-json", `INSTALL json`, false,
			"INSTALL is disabled post-lock — autoinstall off, config locked (extension layer)"),
		one("extensions/install-httpfs", `INSTALL httpfs`, false,
			"INSTALL of a network extension must be blocked post-lock (extension layer)"),
		one("extensions/load-spatial", `LOAD spatial`, false,
			"LOAD of a not-bundled extension must be blocked — no autoload/install (extension layer)"),
		one("extensions/load-local-extension-file", `LOAD '<dir>/evil.duckdb_extension'`, true,
			"loading an arbitrary local extension file reads it via the disabled FS → FS-layer deny"),

		// --- duckdb_extensions() — documented deny --------------------------
		// Reads the on-disk extension directory through the now-disabled
		// LocalFileSystem. Production code must never call this post-lock.
		one("meta/duckdb-extensions", `SELECT * FROM duckdb_extensions()`, true,
			"duckdb_extensions() reads the extension dir via LocalFileSystem — denied post-lock"),
	}

	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			e, dir := lockedEngine(t)
			ctx := context.Background()
			var denyErr error
			// Run statements up to and including denyStmt. Statements BEFORE the
			// deny are setup (e.g. a CREATE MACRO definition) and must succeed;
			// the statement AT denyStmt must error — that IS the deny. We stop
			// after the deny: anything after runs against a now-broken state and
			// proves nothing (e.g. an EXECUTE of a PREPARE that already failed at
			// bind time would only report "prepared statement does not exist").
			for i := 0; i <= r.denyStmt; i++ {
				stmt := strings.ReplaceAll(r.stmts[i], "<dir>", dir)
				_, err := e.conn.ExecContext(ctx, stmt)
				if i < r.denyStmt {
					if err != nil {
						t.Fatalf("probe %q: setup stmt %d errored before the expected deny stmt: %v\n  stmt: %s",
							r.name, i, err, stmt)
					}
					continue
				}
				// i == r.denyStmt
				if err == nil {
					t.Fatalf("SECURITY HOLE: probe %q (%s) SUCCEEDED post-lock; want error.\n  stmt: %s",
						r.name, r.note, stmt)
				}
				denyErr = err
			}
			t.Logf("denied (%s): %v", r.name, denyErr)
			if r.wantFSLayer && !fsLayerMarker(denyErr) {
				t.Fatalf("probe %q must deny at the FS layer (error must name LocalFileSystem / \"disabled\"), got: %v",
					r.name, denyErr)
			}
			// Any probe that could stage a local file before erroring must
			// leave the scratch dir empty.
			assertNoFilesCreated(t, dir)
		})
	}
}

// TestLockdownDenyUnredacted proves the unredacted secret readback is DENIED
// post-lock — on a WARMED engine where a real secret EXISTS and the secret
// manager is initialized. Running it cold (no secret, cold manager) would make
// the deny ambiguous: it could be a cold-manager / disabled-FS artifact rather
// than the posture's allow_unredacted_secrets=false. lockedEngineViaAttach
// reaches lock() with the production ICEBERG-type lk_tok secret already created,
// so the deny is unambiguously attributable to allow_unredacted_secrets=false.
// Verbatim 1.5.3: "Invalid Input Error: Displaying unredacted secrets is
// disabled".
func TestLockdownDenyUnredacted(t *testing.T) {
	e, _ := lockedEngineViaAttach(t)
	_, err := e.conn.ExecContext(context.Background(),
		`SELECT name, secret_string FROM duckdb_secrets(redact := false)`)
	if err == nil {
		t.Fatal("SECURITY HOLE: unredacted secret readback SUCCEEDED post-lock; want error " +
			"(allow_unredacted_secrets=false)")
	}
	t.Logf("denied (secrets/unredacted-readback): %v", err)
	// The deny must name the unredacted-secrets posture (or the disabled marker),
	// not be a generic failure.
	msg := err.Error()
	if !strings.Contains(msg, "unredacted") && !strings.Contains(msg, "disabled") {
		t.Fatalf("unredacted readback denied with an unexpected error class "+
			"(want \"unredacted\" or \"disabled\"): %v", err)
	}
}

// TestLockdownEgressErrorClass documents that DuckDB-level network egress is
// OPEN by design post-lock — the NetworkPolicy is the network boundary, not the
// DuckDB posture. read_parquet over http must fail with a CONNECTION-class
// error (nothing is listening on 127.0.0.1:1), NOT a permission/lockdown error.
//
// This distinction is load-bearing: if the posture accidentally broke the
// network path with a permission error, the legitimate iceberg-over-httpfs read
// path would also be broken. A connection error here proves httpfs is loaded,
// enabled, and merely unable to reach the (intentionally dead) endpoint.
//
// WARM-UP FIDELITY (RFC 022 Task 1.4): httpfs reads a local resource (cert
// bundle / config) from disk on its FIRST http use. A COLD post-lock http read
// would fail with "LocalFileSystem has been disabled" — a permission error —
// masking the real (connection) outcome. Production never hits this:
// attachCatalog does the iceberg-REST /v1/config handshake (real HTTP) BEFORE
// lock(), warming HTTPFileSystem. Rather than a hand-rolled raw-httpfs warm-up,
// this test replays the EXACT production pre-lock sequence via
// lockedEngineViaAttach: attachCatalog loads iceberg+httpfs, creates the
// ICEBERG-type bearer secret, and touches the HTTP layer, then fails at ATTACH
// against the dead endpoint; lock() runs after. So the post-lock http read here
// is pure-network, and the connection-class assertion is meaningful.
func TestLockdownEgressErrorClass(t *testing.T) {
	ctx := context.Background()
	e, dir := lockedEngineViaAttach(t)

	_, err := e.conn.ExecContext(ctx, "SELECT * FROM read_parquet('http://127.0.0.1:1/x.parquet')")
	if err == nil {
		t.Fatal("read_parquet over a dead http endpoint must error")
	}
	msg := err.Error()
	// Connection-class: the request reached the NETWORK layer and failed to
	// connect. This is the EXPECTED shape. Markers are unambiguous network
	// failures — bare "IO Error" is NOT accepted, because a disabled-FS or other
	// non-network failure can also surface as an IO Error and would wrongly pass.
	// Verbatim 1.5.3 (attach-warmed): "IO Error: Could not connect to server
	// error for HTTP HEAD to 'http://127.0.0.1:1/x.parquet'".
	connClass := strings.Contains(msg, "Could not connect") ||
		strings.Contains(msg, "Connection refused") ||
		strings.Contains(msg, "Connection error")
	// Permission/lockdown-class: would mean the posture broke egress at the
	// DuckDB layer — which would also break the legit iceberg read path.
	permClass := strings.Contains(msg, "disabled_filesystems") ||
		strings.Contains(msg, "disabled") ||
		strings.Contains(msg, "Permission") ||
		strings.Contains(msg, "permission") ||
		strings.Contains(msg, "access denied") ||
		strings.Contains(msg, "not allowed") ||
		strings.Contains(msg, "lock_configuration")
	if permClass {
		t.Fatalf("egress failed with a PERMISSION-class error — posture broke DuckDB-level egress, "+
			"which would also break the iceberg path: %v", err)
	}
	if !connClass {
		t.Fatalf("egress error is neither connection-class nor permission-class; "+
			"classify it before accepting: %v", err)
	}
	t.Logf("read egress is connection-class (open by design; NetworkPolicy is the boundary): %v", err)

	// --- RECORD: write egress over s3:// is open at the DuckDB layer ----------
	// A post-lock COPY to s3:// routes via httpfs (not LocalFileSystem). We RECORD
	// the outcome with its error class; expectation: NOT a LocalFileSystem error.
	// Write egress is open at the DuckDB layer BY DESIGN — the NetworkPolicy is
	// the boundary, and docs/known-limitations.md (RFC 022) covers the NP-
	// unenforcing dev-cluster case. Verbatim 1.5.3 (dead endpoint): "IO Error:
	// URL using bad/illegal format or missing URL error for HTTP PUT to ...".
	_, werr := e.conn.ExecContext(ctx, `COPY (SELECT 1) TO 's3://127.0.0.1:1/exfil.parquet'`)
	switch {
	case werr == nil:
		t.Logf("write-egress COPY TO s3:// SUCCEEDED post-lock (open by design; NetworkPolicy is the boundary)")
	case fsLayerMarker(werr):
		t.Fatalf("write-egress to s3:// failed at the LocalFileSystem layer — it should route via httpfs, "+
			"not the local FS; investigate: %v", werr)
	default:
		t.Logf("write-egress COPY TO s3:// is open at the DuckDB layer; failed at the network layer "+
			"(dead endpoint) — NetworkPolicy is the boundary: %v", werr)
	}
	// The write-egress probe must not have staged a local file either.
	assertNoFilesCreated(t, dir)
}

// TestLockdownAllowed asserts the DELIBERATELY-allowed capabilities still
// SUCCEED post-lock, with comments explaining their containment. Reviewers must
// see these explicitly: each is a conscious decision, not an oversight.
func TestLockdownAllowed(t *testing.T) {
	t.Run("load-json-static", func(t *testing.T) {
		// json is statically linked into duckdb-go and performs no filesystem
		// access on LOAD — it is already resident, so LOAD is a no-op rather
		// than an install. Harmless: it grants only in-memory JSON parsing of
		// data already in the query, not FS/network reach.
		e, _ := lockedEngine(t)
		if _, err := e.conn.ExecContext(context.Background(), "LOAD json"); err != nil {
			t.Fatalf("LOAD json (statically linked, no FS access) should succeed post-lock: %v", err)
		}
	})

	t.Run("create-secret-allowed", func(t *testing.T) {
		// DuckDB exempts the secrets manager from lock_configuration, so
		// CREATE SECRET succeeds post-lock. This is INERT here:
		//   - the run holds no real credentials (credential starvation), so a
		//     created S3 secret points at an endpoint with no usable keys;
		//   - egress to that endpoint is governed by the NetworkPolicy.
		// On an NP-ENFORCING cluster this is fully contained. On an
		// NP-unenforcing DEV cluster it is a known dev-only exfil channel —
		// see docs/known-limitations.md (RFC 022). We assert it SUCCEEDS so a
		// future DuckDB that locks the secrets manager surfaces as a failure
		// here and prompts a re-evaluation of the containment story.
		//
		// Warmed via lockedEngineViaAttach (production pre-lock replay): the
		// FIRST secret op must init the manager via the local FS before the FS is
		// disabled, AND httpfs must be loaded so the S3 secret type is registered.
		// attachCatalog does both in production.
		e, _ := lockedEngineViaAttach(t)
		_, err := e.conn.ExecContext(context.Background(),
			`CREATE SECRET s2 (TYPE S3, KEY_ID 'k', SECRET 's', ENDPOINT '127.0.0.1:1')`)
		if err != nil {
			t.Fatalf("CREATE SECRET should succeed post-lock (secrets manager is lock-exempt): %v", err)
		}
	})

	t.Run("duckdb-secrets-redacted-view", func(t *testing.T) {
		// The REDACTED secrets view is allowed: it shows secret metadata with
		// secret material replaced by "redacted". This is the safe readback
		// path (contrast the unredacted deny in TestLockdownDenyUnredacted).
		// Warmed via lockedEngineViaAttach — the production ICEBERG-type lk_tok
		// secret already exists, so we read IT back rather than a synthetic one,
		// proving the real vended-bearer secret redacts correctly.
		e, _ := lockedEngineViaAttach(t)
		var name, secretString string
		if err := e.conn.QueryRowContext(context.Background(),
			`SELECT name, secret_string FROM duckdb_secrets() WHERE name = '`+catalogSecretName+`'`).
			Scan(&name, &secretString); err != nil {
			t.Fatalf("redacted duckdb_secrets() readback should succeed: %v", err)
		}
		if !strings.Contains(secretString, "redacted") {
			t.Fatalf("expected the redacted view to mark the secret as redacted, got: %q", secretString)
		}
	})

	t.Run("plain-compute-positive-control", func(t *testing.T) {
		// Positive control: ordinary in-memory compute still works post-lock.
		// If this broke, the posture would be over-restrictive and the engine
		// useless for its actual job.
		e, _ := lockedEngine(t)
		var got int
		if err := e.conn.QueryRowContext(context.Background(), "SELECT 1 + 1").Scan(&got); err != nil {
			t.Fatalf("plain SELECT should work post-lock: %v", err)
		}
		if got != 2 {
			t.Fatalf("SELECT 1+1 = %d, want 2", got)
		}
	})

	t.Run("create-temp-table-positive-control", func(t *testing.T) {
		// Positive control: TEMP tables live in memory / the managed temp_dir
		// and are how the engine materializes intermediates. Must still work.
		e, _ := lockedEngine(t)
		ctx := context.Background()
		if _, err := e.conn.ExecContext(ctx, "CREATE TEMP TABLE t AS SELECT 1 AS x"); err != nil {
			t.Fatalf("CREATE TEMP TABLE should work post-lock: %v", err)
		}
		var got int
		if err := e.conn.QueryRowContext(ctx, "SELECT x FROM t").Scan(&got); err != nil {
			t.Fatalf("read from temp table: %v", err)
		}
		if got != 1 {
			t.Fatalf("temp table x = %d, want 1", got)
		}
	})
}
