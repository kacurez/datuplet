package store_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func testStore(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB-backed test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgx pool: %v", err)
	}
	if _, err := pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
		pool.Close()
		t.Fatalf("reset: %v", err)
	}
	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	return pool, func() { pool.Close() }
}

func TestCreateUser_Duplicate(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()

	if _, err := store.CreateUser(context.Background(), pool, "alice@example.com", "hash1"); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	_, err := store.CreateUser(context.Background(), pool, "alice@example.com", "hash2")
	if !errors.Is(err, store.ErrUserAlreadyExists) {
		t.Errorf("expected ErrUserAlreadyExists, got: %v", err)
	}
}

func TestGetUserByEmail(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()

	created, err := store.CreateUser(context.Background(), pool, "bob@example.com", "hash")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	found, err := store.GetUserByEmail(context.Background(), pool, "bob@example.com")
	if err != nil {
		t.Fatalf("GetUserByEmail: %v", err)
	}
	if found.ID != created.ID || found.Email != "bob@example.com" {
		t.Errorf("got %+v, want id=%s email=bob@example.com", found, created.ID)
	}
	if found.PasswordHash != "hash" {
		t.Errorf("PasswordHash not returned")
	}
}

func TestGetUserByEmail_Missing(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()

	_, err := store.GetUserByEmail(context.Background(), pool, "nobody@example.com")
	if !errors.Is(err, store.ErrUserNotFound) {
		t.Errorf("expected ErrUserNotFound, got: %v", err)
	}
}
