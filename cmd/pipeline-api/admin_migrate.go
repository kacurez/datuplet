package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
)

// adminMigrate runs the same SQL migrations the in-process startup hook used
// to run, but as a one-shot Job suitable for helm pre-install. Idempotent via
// pg_advisory_lock(20260419) — re-run on every helm upgrade.
func adminMigrate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	dsn := fs.String("database-url", "", "Postgres DSN (default from DATABASE_URL)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dsn == "" {
		*dsn = os.Getenv("DATABASE_URL")
	}
	if *dsn == "" {
		return fmt.Errorf("--database-url is required (or set DATABASE_URL)")
	}

	pool, err := pipelineapidb.Open(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	if err := pipelineapidb.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	fmt.Println("Migrations applied (or already current).")
	return nil
}
