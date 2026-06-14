//go:build duckdb_arrow

package queryengine

import (
	"context"
	"strings"
	"testing"
)

func TestOpenEnginePinsConnAndSetsLimits(t *testing.T) {
	e, err := openEngine(context.Background(), Request{
		MemoryLimit: "512MiB", TempDir: t.TempDir(), MaxTempSize: "1GiB", Threads: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var mem string
	if err := e.conn.QueryRowContext(context.Background(), "SELECT current_setting('memory_limit')").Scan(&mem); err != nil {
		t.Fatal(err)
	}
	// DuckDB normalizes the unit (e.g. "512.0 MiB"); assert it reflects the
	// configured 512MiB rather than just being non-empty. Substring match
	// stays robust across DuckDB's formatting.
	if !strings.Contains(mem, "512") {
		t.Fatalf("memory_limit = %q, want it to reflect configured 512MiB", mem)
	}
	var lock bool
	if err := e.conn.QueryRowContext(context.Background(), "SELECT current_setting('lock_configuration')").Scan(&lock); err != nil {
		t.Fatalf("scan lock_configuration: %v", err)
	}
	if lock {
		t.Fatal("must NOT be locked before attach")
	}
}

func TestEngineEnforcesSingleConnection(t *testing.T) {
	e, err := openEngine(context.Background(), Request{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := e.db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections = %d, want 1 (load-bearing security invariant)", got)
	}
}

func TestLockAppliesPostureAndPins(t *testing.T) {
	ctx := context.Background()
	e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.lock(ctx); err != nil {
		t.Fatal(err)
	}
	if !e.locked {
		t.Fatal("e.locked must be true after a successful lock()")
	}

	// Declarative posture: assert the five settings DuckDB reports through
	// current_setting. All four extension/secret knobs come back as BOOLEAN
	// (Go bool); lock_configuration likewise. disabled_filesystems is a
	// write-only knob that current_setting/duckdb_settings() report as empty
	// once set, so it can't be asserted declaratively — the read_text probe
	// below is its enforcement proof instead.
	for _, name := range []string{
		"autoinstall_known_extensions",
		"autoload_known_extensions",
		"allow_community_extensions",
		"allow_unredacted_secrets",
	} {
		var v bool
		if err := e.conn.QueryRowContext(ctx, "SELECT current_setting('"+name+"')").Scan(&v); err != nil {
			t.Fatalf("scan current_setting(%q): %v", name, err)
		}
		if v {
			t.Fatalf("current_setting(%q) = true, want false", name)
		}
	}
	var locked bool
	if err := e.conn.QueryRowContext(ctx, "SELECT current_setting('lock_configuration')").Scan(&locked); err != nil {
		t.Fatalf("scan current_setting('lock_configuration'): %v", err)
	}
	if !locked {
		t.Fatal("current_setting('lock_configuration') = false, want true")
	}

	// Behavioral probe 1: any further SET must fail (configuration locked).
	if _, err := e.conn.ExecContext(ctx, "SET memory_limit='99GB'"); err == nil {
		t.Fatal("SET succeeded after lock")
	}
	// Behavioral probe 2: local FS must be disabled (disabled_filesystems).
	if _, err := e.conn.ExecContext(ctx, "SELECT * FROM read_text('/etc/passwd')"); err == nil {
		t.Fatal("local FS read succeeded after lock")
	}
	// Behavioral probe 3: unredacted secret display must be disabled
	// (allow_unredacted_secrets=false).
	if _, err := e.conn.ExecContext(ctx, "SELECT * FROM duckdb_secrets(redact := false)"); err == nil {
		t.Fatal("duckdb_secrets(redact := false) succeeded after lock")
	}
}
