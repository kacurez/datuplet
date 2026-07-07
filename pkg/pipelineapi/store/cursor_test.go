package store

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 6, 16, 14, 2, 11, 123456789, time.UTC)
	id := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	enc := encodeCursor(ts, id)
	gotTS, gotID, err := decodeCursor(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !gotTS.Equal(ts) {
		t.Errorf("ts = %v, want %v", gotTS, ts)
	}
	if gotID != id {
		t.Errorf("id = %v, want %v", gotID, id)
	}
}

func TestDecodeCursor_Garbage(t *testing.T) {
	_, _, err := decodeCursor("!!!not-base64!!!")
	if err == nil {
		t.Fatal("expected error for non-base64 cursor")
	}
	if !errors.Is(err, ErrBadCursor) {
		t.Errorf("err = %v, want wrapped ErrBadCursor", err)
	}
}
