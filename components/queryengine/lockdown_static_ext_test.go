//go:build duckdb_arrow

package queryengine

import (
	"context"
	"database/sql"
	"slices"
	"testing"
)

// TestStaticallyLinkedExtensions pins what the duckdb-go bindings link in. A
// statically linked extension is resident with no file read and has no runtime
// off-switch, so the lockdown posture cannot keep it out: this set is part of
// the post-lock capability surface (spec D5). Runs PRE-lock on purpose:
// duckdb_extensions() is a documented post-lock deny.
func TestStaticallyLinkedExtensions(t *testing.T) {
	ctx := context.Background()
	e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("openEngine: %v", err)
	}
	defer e.Close()

	// Full inventory, logged so a bindings bump shows the diff. install_mode
	// separates linked-in extensions from ones merely installed under the
	// developer's extension directory (REPOSITORY).
	all, err := e.conn.QueryContext(ctx,
		`SELECT extension_name, loaded, installed, install_mode FROM duckdb_extensions() ORDER BY 1`)
	if err != nil {
		t.Fatalf("duckdb_extensions() inventory: %v", err)
	}
	for all.Next() {
		var name string
		var loaded, installed sql.NullBool
		var mode sql.NullString
		if err := all.Scan(&name, &loaded, &installed, &mode); err != nil {
			all.Close()
			t.Fatalf("scan inventory row: %v", err)
		}
		t.Logf("%-18s loaded=%-5v installed=%-5v install_mode=%s", name, loaded.Bool, installed.Bool, mode.String)
	}
	if err := all.Err(); err != nil {
		t.Fatalf("inventory rows: %v", err)
	}
	all.Close() // the pinned conn serves one result set at a time

	rows, err := e.conn.QueryContext(ctx,
		`SELECT extension_name FROM duckdb_extensions() WHERE install_mode = 'STATICALLY_LINKED' ORDER BY 1`)
	if err != nil {
		t.Fatalf("duckdb_extensions() static set: %v", err)
	}
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			t.Fatalf("scan static row: %v", err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("static rows: %v", err)
	}
	rows.Close()

	// Exact set the spike measured on 2.0.0 (1.5.3 lacked tpcds/tpch). dbgen/dsdgen write into
	// the CURRENT catalog — the attached lk — which is READ_ONLY, so they fail there before any
	// request (TestLockdownDenyAttachedReadOnly); aimed at `memory` they generate in RAM, bounded
	// by memory_limit / max_temp_directory_size / the request timeout (spec D5). Any growth or
	// shrink fails here so a bindings bump reconciles it on purpose.
	wantStatic := []string{"autocomplete", "core_functions", "icu", "json", "parquet", "tpcds", "tpch"}
	// Canonical duckdb_extensions() names: there is no row called "postgres" or "mysql".
	mustNotHave := []string{"quack", "httpfs", "iceberg", "postgres_scanner", "mysql_scanner"}

	if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(wantStatic))) {
		t.Errorf("statically linked set changed: got %v, want %v; reconcile against the lockdown posture (spec D5) before accepting",
			got, wantStatic)
	}
	for _, name := range mustNotHave {
		if slices.Contains(got, name) {
			t.Errorf("SECURITY: %q is statically linked, so the posture cannot keep it out; escalate before any posture change", name)
		}
	}
}
