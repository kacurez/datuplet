package authz

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestDiscoverServerObject_TransportUnreachable verifies that a connection-
// refused transport error maps to ErrAuthzUnavailable, so the first
// superadmin check emits HTTP 503 rather than a misclassified 500.
func TestDiscoverServerObject_TransportUnreachable(t *testing.T) {
	// 127.0.0.1:1 is guaranteed connection-refused on any OS; c.Do returns a
	// *url.Error wrapping *net.OpError immediately.
	_, err := DiscoverServerObject(context.Background(), "http://127.0.0.1:1", "", "store-uuid")
	if err == nil {
		t.Fatal("expected error from unreachable FGA endpoint, got nil")
	}
	if !errors.Is(err, ErrAuthzUnavailable) {
		t.Fatalf("want errors.Is(err, ErrAuthzUnavailable), got %v", err)
	}
}

// TestDiscoverServerObject_Non2xx verifies that a degraded-FGA HTTP 5xx maps
// to ErrAuthzUnavailable instead of decoding an empty page and returning the
// misleading "no server:<uuid> tuple found".
func TestDiscoverServerObject_Non2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"internal_error"}`))
	}))
	defer srv.Close()

	_, err := DiscoverServerObject(context.Background(), srv.URL, "", "store-uuid")
	if err == nil {
		t.Fatal("expected error from 5xx FGA response, got nil")
	}
	if !errors.Is(err, ErrAuthzUnavailable) {
		t.Fatalf("want errors.Is(err, ErrAuthzUnavailable), got %v", err)
	}
}
