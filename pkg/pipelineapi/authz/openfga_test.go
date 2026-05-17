package authz

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestOpenFGAAuthorizer_Check_NetworkUnreachable verifies that a connection-
// refused error is wrapped as ErrAuthzUnavailable, so handlers emit HTTP 503
// instead of 500.
//
// This test does NOT require the "integration" build tag — it hits a closed
// local port and tests purely client-side error mapping.
func TestOpenFGAAuthorizer_Check_NetworkUnreachable(t *testing.T) {
	// 127.0.0.1:1 is guaranteed connection-refused on any OS.
	az, err := NewOpenFGAAuthorizer(
		"http://127.0.0.1:1",
		"01AAAAAAAAAAAAAAAAAAAAAAA1", // fake but non-empty ULID
		"01AAAAAAAAAAAAAAAAAAAAAAA2", // fake but non-empty ULID
		"",                          // no apiKey — existing behaviour
		5*time.Second,
	)
	if err != nil {
		t.Fatalf("NewOpenFGAAuthorizer: %v", err)
	}

	_, err = az.Check(context.Background(), UserObject("alice").String(), "viewer", ProjectObject("proj-1"))
	if err == nil {
		t.Fatal("expected error from unreachable host, got nil")
	}
	if !errors.Is(err, ErrAuthzUnavailable) {
		t.Errorf("expected errors.Is(err, ErrAuthzUnavailable) == true, got err = %v", err)
	}
}

// TestOpenFGABearerHeader verifies that a non-empty apiKey causes the SDK to
// attach "Authorization: Bearer <key>" on every outbound request.
func TestOpenFGABearerHeader(t *testing.T) {
	var seenAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		// Minimal valid OpenFGA Check response.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"allowed":false}`))
	}))
	defer srv.Close()

	a, err := NewOpenFGAAuthorizer(srv.URL, "01JXXXXXXXXXXXXXXXXXXXXXXX", "01JXXXXXXXXXXXXXXXXXXXXXXY", "secret-key-123", 5*time.Second)
	if err != nil {
		t.Fatalf("NewOpenFGAAuthorizer: %v", err)
	}
	// Check a non-existent object — server returns {"allowed":false}, so no error.
	_, _ = a.Check(context.Background(), "user:test", "view", ProjectObject("proj-1"))
	if seenAuth != "Bearer secret-key-123" {
		t.Fatalf("expected Authorization=%q, got %q", "Bearer secret-key-123", seenAuth)
	}
}

// TestResolveModelFromEnv verifies that ResolveStoreAndModel fetches the store_id
// by name and model_id via the version-pin tuple.
func TestResolveModelFromEnv(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/stores":
			fmt.Fprintln(w, `{"stores":[{"id":"store-uuid","name":"datuplet"}]}`)
		case strings.HasSuffix(r.URL.Path, "/read"):
			fmt.Fprintln(w, `{"tuples":[{"key":{"user":"auth_model_id:model-uuid"}}]}`)
		}
	}))
	defer srv.Close()

	storeID, modelID, err := ResolveStoreAndModel(context.Background(), srv.URL, "secret", "datuplet", "4.4")
	if err != nil {
		t.Fatal(err)
	}
	if storeID != "store-uuid" {
		t.Fatalf("storeID = %q", storeID)
	}
	if modelID != "model-uuid" {
		t.Fatalf("modelID = %q", modelID)
	}
}

// TestIsTupleAlreadyExistsErr pins the error-detection contract that
// makes WriteTuples idempotent for single-tuple writes. Match strings
// are taken verbatim from observed OpenFGA error output
// (write_failed_due_to_invalid_input).
func TestIsTupleAlreadyExistsErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			name: "canonical 'tuple to be written already existed'",
			err:  errors.New("POST validation error: tuple to be written already existed or the tuple to be deleted did not exist"),
			want: true,
		},
		{
			name: "alternate 'cannot write a tuple which already exists'",
			err:  errors.New("write_failed_due_to_invalid_input: cannot write a tuple which already exists: user: 'user:oidc~abc'"),
			want: true,
		},
		{
			name: "unrelated error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "model not found",
			err:  errors.New("authorization model not found"),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTupleAlreadyExistsErr(tc.err); got != tc.want {
				t.Errorf("isTupleAlreadyExistsErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
