package db

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RequireSchemaVersion fails when the DB's highest applied migration
// doesn't match the binary's latest embedded migration. Called by the
// reap-once subcommand at startup to prevent a lagging reaper binary
// from operating on a newer (or older) schema than it understands.
//
// pipeline-api itself doesn't call this — runServe runs Migrate under
// an advisory lock, which is a strict superset of the check.
func RequireSchemaVersion(ctx context.Context, pool *pgxpool.Pool) error {
	want, err := latestEmbeddedVersion()
	if err != nil {
		return fmt.Errorf("list embedded migrations: %w", err)
	}
	var got string
	err = pool.QueryRow(ctx, `SELECT COALESCE(MAX(version), '') FROM schema_migrations`).Scan(&got)
	if err != nil {
		return fmt.Errorf("query schema_migrations: %w", err)
	}
	if got != want {
		return fmt.Errorf("schema mismatch: binary expects %q, DB has %q — roll pipeline-api before the reaper", want, got)
	}
	return nil
}

func latestEmbeddedVersion() (string, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return "", err
	}
	var versions []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		versions = append(versions, strings.TrimSuffix(e.Name(), ".sql"))
	}
	if len(versions) == 0 {
		return "", fmt.Errorf("no migrations embedded in binary")
	}
	sort.Strings(versions)
	return versions[len(versions)-1], nil
}
