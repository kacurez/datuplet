package store

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ErrBadCursor wraps every decode failure so the HTTP handler can map a
// tampered/garbage cursor to 400 (vs 500 for genuine DB errors) via errors.Is.
var ErrBadCursor = errors.New("invalid cursor")

// encodeCursor packs a (created_at, id) pair into an opaque base64url token.
func encodeCursor(t time.Time, id uuid.UUID) string {
	raw := t.UTC().Format(time.RFC3339Nano) + "|" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor reverses encodeCursor. Any malformed input is wrapped in
// ErrBadCursor so the handler can reject a tampered cursor with 400 rather than
// scanning from a bogus position.
func decodeCursor(s string) (time.Time, uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: base64: %v", ErrBadCursor, err)
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: malformed", ErrBadCursor)
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: time: %v", ErrBadCursor, err)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: id: %v", ErrBadCursor, err)
	}
	return t, id, nil
}
