// Package store is the data-access layer for pipeline-api. Handlers call
// these functions instead of embedding SQL directly.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// User is the in-memory view of a users row.
type User struct {
	ID           uuid.UUID
	Email        string
	PasswordHash string
	CreatedAt    time.Time
	DisabledAt   *time.Time
}

// ErrUserAlreadyExists is returned by CreateUser on a unique-constraint
// violation on email.
var ErrUserAlreadyExists = errors.New("user already exists")

// ErrUserNotFound is returned when no user matches the lookup.
var ErrUserNotFound = errors.New("user not found")

// CreateUser inserts a new user row. passwordHash must already be argon2id.
func CreateUser(ctx context.Context, pool *pgxpool.Pool, email, passwordHash string) (*User, error) {
	u := &User{Email: email, PasswordHash: passwordHash}
	err := pool.QueryRow(ctx,
		`INSERT INTO users(email, password_hash) VALUES ($1, $2) RETURNING id, created_at`,
		email, passwordHash,
	).Scan(&u.ID, &u.CreatedAt)
	if err != nil {
		if strings.Contains(err.Error(), "users_email_key") {
			return nil, ErrUserAlreadyExists
		}
		return nil, fmt.Errorf("insert user: %w", err)
	}
	return u, nil
}

// GetUserByEmail returns the user with the given email, or ErrUserNotFound.
// Disabled users are returned — callers may reject them based on DisabledAt.
func GetUserByEmail(ctx context.Context, pool *pgxpool.Pool, email string) (*User, error) {
	u := &User{}
	err := pool.QueryRow(ctx,
		`SELECT id, email, password_hash, created_at, disabled_at
		   FROM users WHERE email = $1`,
		email,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt, &u.DisabledAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("select user: %w", err)
	}
	return u, nil
}

// GetUserByID is the counterpart used by the auth middleware after looking
// up a session.
func GetUserByID(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (*User, error) {
	u := &User{}
	err := pool.QueryRow(ctx,
		`SELECT id, email, password_hash, created_at, disabled_at
		   FROM users WHERE id = $1`,
		id,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt, &u.DisabledAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("select user: %w", err)
	}
	return u, nil
}
