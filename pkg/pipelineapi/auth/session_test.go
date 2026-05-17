package auth_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
)

func sessionTestDB(t *testing.T) (*pgxpool.Pool, uuid.UUID, func()) {
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
		t.Fatalf("reset schema: %v", err)
	}
	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	// Seed a user.
	var userID uuid.UUID
	err = pool.QueryRow(context.Background(),
		`INSERT INTO users(email, password_hash) VALUES ($1, $2) RETURNING id`,
		"alice@example.com", "irrelevant-for-this-test",
	).Scan(&userID)
	if err != nil {
		pool.Close()
		t.Fatalf("seed user: %v", err)
	}
	return pool, userID, func() { pool.Close() }
}

func TestCreateAndLookupSession(t *testing.T) {
	pool, userID, cleanup := sessionTestDB(t)
	defer cleanup()

	sid, err := auth.CreateSession(context.Background(), pool, userID)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	sess, err := auth.LookupSession(context.Background(), pool, sid)
	if err != nil {
		t.Fatalf("LookupSession: %v", err)
	}
	if sess.UserID != userID {
		t.Errorf("UserID = %s, want %s", sess.UserID, userID)
	}
	if !sess.ExpiresAt.After(time.Now().Add(23 * time.Hour)) {
		t.Errorf("ExpiresAt %v is not ~24h in the future", sess.ExpiresAt)
	}
}

func TestLookupSession_Expired(t *testing.T) {
	pool, userID, cleanup := sessionTestDB(t)
	defer cleanup()

	var sid uuid.UUID
	err := pool.QueryRow(context.Background(),
		`INSERT INTO sessions(user_id, expires_at) VALUES ($1, now() - interval '1 minute') RETURNING id`,
		userID,
	).Scan(&sid)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	if _, err := auth.LookupSession(context.Background(), pool, sid); err == nil {
		t.Error("expected LookupSession to reject an expired session")
	}
}

func TestLookupSession_Missing(t *testing.T) {
	pool, _, cleanup := sessionTestDB(t)
	defer cleanup()

	_, err := auth.LookupSession(context.Background(), pool, uuid.New())
	if err == nil {
		t.Error("expected LookupSession to reject a nonexistent session")
	}
}

func TestDeleteSession(t *testing.T) {
	pool, userID, cleanup := sessionTestDB(t)
	defer cleanup()

	sid, err := auth.CreateSession(context.Background(), pool, userID)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := auth.DeleteSession(context.Background(), pool, sid); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if _, err := auth.LookupSession(context.Background(), pool, sid); err == nil {
		t.Error("deleted session should not lookup")
	}
}

func TestTouchSession_ExtendsExpiry(t *testing.T) {
	pool, userID, cleanup := sessionTestDB(t)
	defer cleanup()

	sid, err := auth.CreateSession(context.Background(), pool, userID)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := pool.Exec(context.Background(),
		`UPDATE sessions SET expires_at = now() + interval '1 minute' WHERE id = $1`, sid,
	); err != nil {
		t.Fatalf("rewind: %v", err)
	}

	if err := auth.TouchSession(context.Background(), pool, sid); err != nil {
		t.Fatalf("TouchSession: %v", err)
	}

	var expiresAt time.Time
	if err := pool.QueryRow(context.Background(),
		`SELECT expires_at FROM sessions WHERE id = $1`, sid,
	).Scan(&expiresAt); err != nil {
		t.Fatalf("query: %v", err)
	}
	if !expiresAt.After(time.Now().Add(23 * time.Hour)) {
		t.Errorf("expiry not extended; got %v", expiresAt)
	}
}
