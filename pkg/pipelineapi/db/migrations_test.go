package db_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
)

// testDB opens a pool from TEST_DATABASE_URL, skipping the test when unset.
// Caller must defer the returned cleanup func.
func testDB(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB-backed test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgx pool: %v", err)
	}
	// Reset schema for isolation between tests: every test starts fresh.
	if _, err := pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
		pool.Close()
		t.Fatalf("reset schema: %v", err)
	}
	return pool, func() { pool.Close() }
}

func TestMigrate_AppliesAll(t *testing.T) {
	pool, cleanup := testDB(t)
	defer cleanup()

	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// schema_migrations exists with one row for 001_init.
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM schema_migrations WHERE version = '001_init'`,
	).Scan(&count); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	if count != 1 {
		t.Errorf("schema_migrations count = %d, want 1", count)
	}

	// users table exists (canary check).
	var tblCount int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'users'`,
	).Scan(&tblCount); err != nil {
		t.Fatalf("query tables: %v", err)
	}
	if tblCount != 1 {
		t.Errorf("users table missing")
	}
}

func TestMigrate_Idempotent(t *testing.T) {
	pool, cleanup := testDB(t)
	defer cleanup()

	// Run once, snapshot the row count, then run again: the count must
	// not change. Hard-coding an expected count here would rot every
	// time we add a migration; we just verify the second call doesn't
	// insert anything extra.
	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	var before int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM schema_migrations`,
	).Scan(&before); err != nil {
		t.Fatalf("query before: %v", err)
	}
	if before == 0 {
		t.Fatal("first Migrate left schema_migrations empty")
	}

	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("second Migrate (should be a no-op): %v", err)
	}
	var after int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM schema_migrations`,
	).Scan(&after); err != nil {
		t.Fatalf("query after: %v", err)
	}
	if after != before {
		t.Errorf("schema_migrations count after second Migrate = %d, want %d (migrations double-applied)", after, before)
	}
}
