package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies any pending migrations from migrations/ in lexical order.
// Uses the schema_migrations table to track applied versions and
// pg_advisory_lock to serialize concurrent runners (multi-replica safety).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	// Advisory lock — ensures only one migration runner at a time.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(20260419)`); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer func() {
		// Release with a detached context so a canceled parent ctx can't
		// skip the unlock and leave the lock held on the pooled connection,
		// which would stall future Migrate() calls.
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(20260419)`)
	}()

	// Step 1: ensure schema_migrations exists. We bootstrap it here because
	// we need to query it before applying 001_init (which also creates it
	// with CREATE TABLE IF NOT EXISTS as a safety net).
	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     TEXT PRIMARY KEY,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("bootstrap schema_migrations: %w", err)
	}

	// Step 2: list applied versions.
	applied := map[string]bool{}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("select schema_migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("scan: %w", err)
		}
		applied[v] = true
	}
	rows.Close()

	// Step 3: enumerate on-disk migrations in lexical order.
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		files = append(files, e.Name())
	}
	sort.Strings(files)

	// Step 4: apply each pending migration inside a transaction.
	for _, name := range files {
		version := strings.TrimSuffix(name, ".sql")
		if applied[version] {
			continue
		}
		body, err := fs.ReadFile(migrationsFS, "migrations/"+name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", name, err)
		}
		// Use simple protocol so multi-statement SQL files apply in one Exec.
		// pgx's default extended protocol uses prepared statements, which
		// reject queries containing more than one SQL command.
		if _, err := tx.Exec(ctx, string(body), pgx.QueryExecModeSimpleProtocol); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1) ON CONFLICT DO NOTHING`, version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s: %w", name, err)
		}
	}
	return nil
}
