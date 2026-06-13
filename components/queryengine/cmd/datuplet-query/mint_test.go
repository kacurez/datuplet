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

func TestMintQueryToken_PolicyOff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"client-side query disabled; use the server query service","kind":"forbidden"}`))
	}))
	defer srv.Close()

	_, err := mintQueryToken(context.Background(), srv.URL, "tok")
	if err == nil {
		t.Fatalf("expected error on 403 policy-off")
	}
	// The clear refusal message must surface (not a generic crash).
	if !strings.Contains(err.Error(), "client-side query disabled") {
		t.Errorf("403 error must surface the policy-off message, got: %v", err)
	}
}

func TestMintQueryToken_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom","kind":"internal"}`))
	}))
	defer srv.Close()

	_, err := mintQueryToken(context.Background(), srv.URL, "tok")
	if err == nil {
		t.Fatalf("expected error on 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should carry the status, got: %v", err)
	}
}

func TestMintQueryToken_EmptyToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"token":"","expires_at":"2099-01-01T00:05:00Z"}`))
	}))
	defer srv.Close()

	_, err := mintQueryToken(context.Background(), srv.URL, "tok")
	if err == nil {
		t.Fatalf("expected error on empty token in 200 body")
	}
}
