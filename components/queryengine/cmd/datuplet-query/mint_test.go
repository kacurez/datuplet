package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMintQueryToken_Success(t *testing.T) {
	var gotPath, gotAuth, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotMethod = r.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"fresh-query-jwt","expires_at":"2099-01-01T00:05:00Z"}`))
	}))
	defer srv.Close()

	tok, err := mintQueryToken(context.Background(), srv.URL, "my-api-token")
	if err != nil {
		t.Fatalf("mintQueryToken: %v", err)
	}
	if tok != "fresh-query-jwt" {
		t.Errorf("token = %q, want fresh-query-jwt", tok)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/api/v1/query/token" {
		t.Errorf("path = %q, want /api/v1/query/token", gotPath)
	}
	if gotAuth != "Bearer my-api-token" {
		t.Errorf("auth = %q, want Bearer my-api-token", gotAuth)
	}
}

func TestMintQueryToken_Errors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		// wantSubstr, when set, must appear in the error.
		wantSubstr string
	}{
		{
			// The clear refusal message must surface (not a generic crash).
			name:       "policy off 403 surfaces message",
			status:     http.StatusForbidden,
			body:       `{"error":"client-side query disabled; use the server query service","kind":"forbidden"}`,
			wantSubstr: "client-side query disabled",
		},
		{
			name:       "server error carries status",
			status:     http.StatusInternalServerError,
			body:       `{"error":"boom","kind":"internal"}`,
			wantSubstr: "500",
		},
		{
			name:   "empty token in 200 body",
			status: http.StatusOK,
			body:   `{"token":"","expires_at":"2099-01-01T00:05:00Z"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			_, err := mintQueryToken(context.Background(), srv.URL, "tok")
			if err == nil {
				t.Fatal("expected error")
			}
			if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("error should contain %q, got: %v", tc.wantSubstr, err)
			}
		})
	}
}
