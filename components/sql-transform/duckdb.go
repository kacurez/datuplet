//go:build duckdb_arrow

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"

	duckdb "github.com/duckdb/duckdb-go/v2"
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

	stmts := []string{
		fmt.Sprintf("SET threads = %d", cfg.Threads),
		fmt.Sprintf("SET temp_directory = '%s'", escapeSQL(cfg.TempDirectory)),
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			db.Close()
			return nil, fmt.Errorf("duckdb pragma %q: %w", s, err)
		}
	}
	return db, nil
}

// arrowFromDB returns a DuckDB Arrow handle bound to a *sql.Conn.
//
// The returned conn pins the in-memory DuckDB instance so subsequent SQL
// (RegisterView, the user's SELECT, COPY TO outputs) all run on the same
// underlying driver connection where the arrow_scan-backed views are
// visible. The Arrow handle holds a *duckdb.Conn pointer at the C level
// (see duckdb-go/v2/arrow.go), so the handle stays valid for as long as
// the *sql.Conn is held — closing the *sql.Conn returns the driver conn
// to the pool, which (with MaxOpenConns=1) is then unavailable for
// further use of the handle.
//
// The caller MUST:
//   - run all subsequent DuckDB SQL through the returned *sql.Conn
//     (NOT db.ExecContext, which would block waiting for the conn that
//     we are holding under MaxOpenConns=1).
//   - close the *sql.Conn AFTER releasing the registered views (via the
//     release funcs returned by RegisterView) so any in-flight scans
//     drain cleanly before the underlying driver conn goes away.
//
// Requires the binary built with -tags=duckdb_arrow.
func arrowFromDB(db *sql.DB) (*duckdb.Arrow, *sql.Conn, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("db.Conn: %w", err)
	}
	var a *duckdb.Arrow
	err = conn.Raw(func(driverConn any) error {
		var rerr error
		a, rerr = duckdb.NewArrowFromConn(driverConn.(driver.Conn))
		return rerr
	})
	if err != nil {
		conn.Close() //nolint:errcheck
		return nil, nil, fmt.Errorf("NewArrowFromConn: %w", err)
	}
	return a, conn, nil
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
