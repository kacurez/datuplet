package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SessionLifetime is how long a new or touched session is valid for.
// 24h sliding expiry, matching the run-token lifetime.
const SessionLifetime = 24 * time.Hour

// Session is the decoded row from the sessions table.
type Session struct {
	ID         uuid.UUID
	UserID     uuid.UUID
	CreatedAt  time.Time
	ExpiresAt  time.Time
	LastSeenAt time.Time
}

// ErrSessionNotFound is returned when LookupSession cannot find a matching
// unexpired row.
var ErrSessionNotFound = errors.New("session not found or expired")

// CreateSession inserts a new session for userID and returns its ID.
// The caller sets this value as the opaque cookie on the HTTP response.
func CreateSession(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID) (uuid.UUID, error) {
	var id uuid.UUID
	err := pool.QueryRow(ctx,
		`INSERT INTO sessions (user_id, expires_at) VALUES ($1, now() + $2::interval) RETURNING id`,
		userID, fmt.Sprintf("%d seconds", int(SessionLifetime.Seconds())),
	).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert session: %w", err)
	}
	return id, nil
}

// LookupSession returns the session if it exists and is not expired.
// Returns ErrSessionNotFound otherwise.
func LookupSession(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (*Session, error) {
	s := &Session{}
	err := pool.QueryRow(ctx,
		`SELECT id, user_id, created_at, expires_at, last_seen_at
		   FROM sessions
		  WHERE id = $1 AND expires_at > now()`,
		id,
	).Scan(&s.ID, &s.UserID, &s.CreatedAt, &s.ExpiresAt, &s.LastSeenAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSessionNotFound
		}
		return nil, fmt.Errorf("lookup: %w", err)
	}
	return s, nil
}

// TouchSession extends the session's expires_at by SessionLifetime from now
// and updates last_seen_at. Called from the auth middleware on successful
// cookie validation. Errors are returned but callers typically only log them —
// a failure to extend should not fail the request.
func TouchSession(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) error {
	// Only extend sessions that haven't already expired. Without this guard
	// a race between LookupSession and TouchSession (or a direct caller with
	// a stale ID) could revive an already-expired row.
	_, err := pool.Exec(ctx,
		`UPDATE sessions
		    SET expires_at = now() + $2::interval,
		        last_seen_at = now()
		  WHERE id = $1 AND expires_at > now()`,
		id, fmt.Sprintf("%d seconds", int(SessionLifetime.Seconds())),
	)
	return err
}

// DeleteSession removes the session row. Safe to call with a nonexistent ID.
func DeleteSession(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) error {
	_, err := pool.Exec(ctx, `DELETE FROM sessions WHERE id = $1`, id)
	return err
}
