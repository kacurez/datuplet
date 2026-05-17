package db_test

import (
	"context"
	"testing"

	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
)

func TestRequireSchemaVersion_OKAfterMigrate(t *testing.T) {
	pool, cleanup := testDB(t)
	defer cleanup()
	ctx := context.Background()
	if err := pipelineapidb.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := pipelineapidb.RequireSchemaVersion(ctx, pool); err != nil {
		t.Errorf("RequireSchemaVersion after fresh migrate: %v", err)
	}
}

func TestRequireSchemaVersion_MismatchOnMissingLatest(t *testing.T) {
	pool, cleanup := testDB(t)
	defer cleanup()
	ctx := context.Background()
	if err := pipelineapidb.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Simulate a binary that expects a later migration by deleting the
	// latest applied record — now the DB's MAX(version) is lower than
	// the binary's embedded latest.
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = (SELECT MAX(version) FROM schema_migrations)`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := pipelineapidb.RequireSchemaVersion(ctx, pool); err == nil {
		t.Error("expected mismatch error, got nil")
	}
}
