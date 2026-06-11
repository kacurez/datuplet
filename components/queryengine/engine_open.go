//go:build duckdb_arrow

package queryengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	// Side-effect import: registers the "duckdb" database/sql driver.
	_ "github.com/duckdb/duckdb-go/v2"
)

// engine wraps an embedded DuckDB instance. The whole lifecycle
// (open → attach → lock → run) executes on a single pinned connection: every
// lockdown setting applied by lock() is database-GLOBAL, so the lock is only
// race-free with one connection in flight.
type engine struct {
	// db is the pool. MaxOpenConns(1) + the single held pinned conn is a
	// LOAD-BEARING security invariant: the lockdown posture (lock()) is
	// database-global, so a second connection acquired from the pool would
	// observe (or race) that global state. Never acquire a second conn from
	// db, never raise the pool limit above 1, and never export db.
	db   *sql.DB
	conn *sql.Conn // pinned: all statements run on this one connection
	// locked is set true only after all six lockdown statements in lock()
	// succeed. A future Run() MUST require locked==true as a precondition:
	// running untrusted user SQL against a half-applied or unlocked posture
	// is forbidden.
	locked bool
}

// applyDefaults fills in conservative fallbacks for unset resource knobs. The
// worker injects a real scratch dir; the "/tmp" TempDir fallback is for tests
// and BYO-local invocations that don't supply one.
func applyDefaults(r *Request) {
	if r.Threads <= 0 {
		r.Threads = 2
	}
	if r.MemoryLimit == "" {
		r.MemoryLimit = "1GiB"
	}
	if r.MaxTempSize == "" {
		r.MaxTempSize = "4GiB"
	}
	if r.TempDir == "" {
		r.TempDir = "/tmp"
	}
}

// openEngine opens an in-memory DuckDB instance, pins a single connection, and
// applies the resource limits. It does NOT apply the lockdown posture or
// lock_configuration: locking before catalog attach would self-block, because
// ATTACH (and DuckDB's secret manager) needs mutable config. Call lock() only
// after attachCatalog.
func openEngine(ctx context.Context, r Request) (*engine, error) {
	applyDefaults(&r)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("sql.Open duckdb: %w", err)
	}

	// Single connection: every lockdown setting is database-global, so the
	// posture is race-free only with one pinned connection. Also, DuckDB's
	// shared in-memory database doesn't guarantee that views/tables created
	// on one connection are visible on another.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("pin connection: %w", err)
	}

	// SECURITY: duckdb-go/v2 treats a semicolon in ANY Exec/Query string as a
	// statement separator — the driver runs statements 0..n-1 eagerly, not just
	// the first. So every interpolated string value below MUST go through
	// escapeSQL and sit inside single quotes; never interpolate an unquoted
	// string. A bare interpolation (e.g. SET x = userValue) lets a value like
	// `1; ATTACH ...` smuggle in extra statements.
	for _, s := range []string{
		fmt.Sprintf("SET threads = %d", r.Threads),
		fmt.Sprintf("SET temp_directory = '%s'", escapeSQL(r.TempDir)),
		fmt.Sprintf("SET memory_limit = '%s'", escapeSQL(r.MemoryLimit)),
		fmt.Sprintf("SET max_temp_directory_size = '%s'", escapeSQL(r.MaxTempSize)),
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			conn.Close()
			db.Close()
			return nil, fmt.Errorf("resource pragma %q: %w", s, err)
		}
	}

	return &engine{db: db, conn: conn}, nil
}

// lock applies the RFC 022 §6.2 lockdown posture (proven by Spike 0.2) and
// pins it. MUST run after attachCatalog: the first secret op initializes
// DuckDB's secret manager via the local stored-secrets dir, which
// disabled_filesystems would block; and ATTACH itself needs mutable config.
// Order is load-bearing; lock_configuration LAST.
func (e *engine) lock(ctx context.Context) error {
	for _, s := range []string{
		`SET autoinstall_known_extensions=false`,
		`SET autoload_known_extensions=false`,
		`SET allow_community_extensions=false`,
		`SET allow_unredacted_secrets=false`,
		`SET disabled_filesystems='LocalFileSystem'`,
		`SET lock_configuration=true`,
	} {
		if _, err := e.conn.ExecContext(ctx, s); err != nil {
			// SECURITY: a half-applied posture (some settings on, some off)
			// must never remain usable — self-destruct so no caller can run
			// user SQL against it. locked stays false.
			e.conn.Close()
			e.db.Close()
			return fmt.Errorf("lockdown %q: %w", s, err)
		}
	}
	// Set only after ALL six statements succeed: the posture is fully applied.
	e.locked = true
	return nil
}

// Close tears down the pinned connection then the pool, synchronously. Later
// tasks rely on deterministic teardown (e.g. scratch-dir cleanup ordering).
// Both close errors are collected via errors.Join. After a lock()
// self-destruct the handles are already closed; sql.Conn.Close then returns
// sql.ErrConnDone, which is surfaced (not a panic) — harmless to the caller.
func (e *engine) Close() error {
	var errs []error
	if e.conn != nil {
		errs = append(errs, e.conn.Close())
	}
	if e.db != nil {
		errs = append(errs, e.db.Close())
	}
	return errors.Join(errs...)
}
