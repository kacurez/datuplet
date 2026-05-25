//go:build duckdb_arrow

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	// Side-effect import: registers the "duckdb" database/sql driver.
	_ "github.com/duckdb/duckdb-go/v2"
)

// openDuckDB opens an in-memory DuckDB instance and applies the component's
// runtime knobs (threads + spill directory).
func openDuckDB(ctx context.Context, cfg *ComponentConfig) (*sql.DB, error) {
	// Ensure the spill directory exists.
	if err := os.MkdirAll(cfg.TempDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("create temp directory %q: %w", cfg.TempDirectory, err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("sql.Open duckdb: %w", err)
	}

	// Single connection: DuckDB's shared in-memory database doesn't
	// guarantee that views/tables created on one connection are visible
	// on another. Pin to one to avoid races.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Pinned conservative defaults — the bookkeeping is in config.go but
	// the rationale lives here next to the SET statements.
	//
	//   - `memory_limit` is set explicitly because DuckDB's cgroup
	//     auto-detect is unreliable inside containers (duckdb/duckdb#15080)
	//     and go-duckdb has its own per-process bloat on parquet reads
	//     (marcboeker/go-duckdb#255) that can multiply this budget.
	//
	//   - `max_temp_directory_size` is the HARD synchronous cap on spill
	//     disk usage. Without it, a fast-spilling query can outrun the
	//     kubelet's ~10-second ephemeral-storage eviction sample window,
	//     fill the node's boot disk, and lock the host filesystem
	//     read-only (we hit this on transform-big2 against 5M rows of
	//     products on a single-node 6 GiB GKE cluster — kubelet went
	//     NotReady, GKE auto-repair triggered). The internal cap fails
	//     the query cleanly instead.
	//
	//   - `preserve_insertion_order = false` allows sort/aggregate
	//     operators to discard input ordering for streaming spill.
	//     See https://duckdb.org/docs/current/guides/performance/oom.
	//
	//   - `threads` is set low (default 2) because each thread carries
	//     its own per-pipeline hash-aggregate + decode buffers, so
	//     threads multiply both in-memory state and concurrent spill
	//     writer pressure. On 2-4 vCPU pods the throughput cost is
	//     dwarfed by the safety win.
	stmts := []string{
		fmt.Sprintf("SET threads = %d", cfg.Threads),
		fmt.Sprintf("SET temp_directory = '%s'", escapeSQL(cfg.TempDirectory)),
		fmt.Sprintf("SET memory_limit = '%s'", escapeSQL(cfg.Memory)),
		fmt.Sprintf("SET max_temp_directory_size = '%s'", escapeSQL(cfg.MaxTempSize)),
		"SET preserve_insertion_order = false",
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			db.Close()
			return nil, fmt.Errorf("duckdb pragma %q: %w", s, err)
		}
	}
	return db, nil
}

// escapeSQL escapes a value for embedding into a DuckDB single-quoted string
// literal. DuckDB uses standard SQL: a single quote is doubled.
func escapeSQL(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'', '\'')
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}

// sanitizeIdent validates that `name` matches DuckDB's permissive identifier
// regex (`^[A-Za-z_][A-Za-z0-9_]*$`) and returns it unquoted. The pattern
// matches what InputTableSpec.LogicalName + InputTableSpec.Table already
// enforce in the CRD, so this is a defensive belt-and-braces check rather
// than a parser. On a non-matching name we return an error so the user gets
// "invalid identifier %q" rather than a SQL injection.
func sanitizeIdent(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("empty identifier")
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return "", fmt.Errorf("invalid identifier %q: only [a-zA-Z0-9_] allowed (must start with letter or underscore)", name)
		}
	}
	return name, nil
}
