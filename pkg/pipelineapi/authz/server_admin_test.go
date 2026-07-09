package authz

import (
	"context"
	"errors"
	"testing"
)

const (
	testServerUUID = "11111111-1111-1111-1111-111111111111"
	testServerWire = "server:" + testServerUUID
)

// stubAuthorizer is a minimal in-package Authorizer for serverAdmin tests.
// authztest.Fake would work, but it lives in a subpackage that imports
// authz, so importing it from this internal (package authz) test creates
// an import cycle — hence a local exact-match fake.
type stubAuthorizer struct {
	allowed  map[string]bool
	checkErr error
}

func newStubAuthorizer() *stubAuthorizer {
	return &stubAuthorizer{allowed: map[string]bool{}}
}

func (s *stubAuthorizer) allow(user, relation string, obj Object) {
	s.allowed[user+"|"+relation+"|"+obj.String()] = true
}

func (s *stubAuthorizer) Check(_ context.Context, user, relation string, obj Object) (bool, error) {
	if s.checkErr != nil {
		return false, s.checkErr
	}
	return s.allowed[user+"|"+relation+"|"+obj.String()], nil
}

func (s *stubAuthorizer) BatchCheck(_ context.Context, queries []CheckQuery) ([]bool, []error) {
	return make([]bool, len(queries)), make([]error, len(queries))
}

func (s *stubAuthorizer) ListObjects(context.Context, string, string, ObjectType) ([]Object, error) {
	return nil, nil
}

func (s *stubAuthorizer) WriteTuples(context.Context, []Tuple) error { return nil }
func (s *stubAuthorizer) DeleteTuples(context.Context, []Tuple) error { return nil }

var _ Authorizer = (*stubAuthorizer)(nil)

// newTestServerAdmin builds a serverAdmin whose discovery is stubbed to
// return testServerWire and bumps *count each time it runs — so tests can
// assert memoization without live HTTP.
func newTestServerAdmin(authzr Authorizer, count *int) *serverAdmin {
	return &serverAdmin{
		authzr: authzr,
		discover: func(context.Context) (string, error) {
			*count++
			return testServerWire, nil
		},
	}
}

func TestIsServerAdmin_Allowed(t *testing.T) {
	fake := newStubAuthorizer()
	fake.allow(UserObject("u1").String(), "admin", ServerObject(testServerUUID))

	var n int
	s := newTestServerAdmin(fake, &n)

	ok, err := s.IsServerAdmin(context.Background(), "u1")
	if err != nil {
		t.Fatalf("IsServerAdmin: unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("IsServerAdmin: want true for seeded superadmin, got false")
	}
}

func TestIsServerAdmin_Denied(t *testing.T) {
	fake := newStubAuthorizer()

	var n int
	s := newTestServerAdmin(fake, &n)

	ok, err := s.IsServerAdmin(context.Background(), "u2")
	if err != nil {
		t.Fatalf("IsServerAdmin: unexpected error: %v", err)
	}
	if ok {
		t.Fatal("IsServerAdmin: want false for unseeded user, got true")
	}
}

func TestIsServerAdmin_Unavailable(t *testing.T) {
	fake := newStubAuthorizer()
	fake.checkErr = ErrAuthzUnavailable

	var n int
	s := newTestServerAdmin(fake, &n)

	ok, err := s.IsServerAdmin(context.Background(), "u1")
	if ok {
		t.Fatal("IsServerAdmin: want false when authz unavailable, got true")
	}
	if !errors.Is(err, ErrAuthzUnavailable) {
		t.Fatalf("IsServerAdmin: want ErrAuthzUnavailable, got %v", err)
	}
}

func TestIsServerAdmin_DiscoveryUnavailable(t *testing.T) {
	fake := newStubAuthorizer()

	s := &serverAdmin{
		authzr: fake,
		discover: func(context.Context) (string, error) {
			return "", ErrAuthzUnavailable
		},
	}

	ok, err := s.IsServerAdmin(context.Background(), "u1")
	if ok {
		t.Fatal("IsServerAdmin: want false when discovery unavailable, got true")
	}
	if !errors.Is(err, ErrAuthzUnavailable) {
		t.Fatalf("IsServerAdmin: want ErrAuthzUnavailable propagated from discovery, got %v", err)
	}
}

func TestIsServerAdmin_DiscoveryMemoized(t *testing.T) {
	fake := newStubAuthorizer()
	fake.allow(UserObject("u1").String(), "admin", ServerObject(testServerUUID))

	var n int
	s := newTestServerAdmin(fake, &n)

	if _, err := s.IsServerAdmin(context.Background(), "u1"); err != nil {
		t.Fatalf("first IsServerAdmin: %v", err)
	}
	if _, err := s.IsServerAdmin(context.Background(), "u1"); err != nil {
		t.Fatalf("second IsServerAdmin: %v", err)
	}
	if n != 1 {
		t.Fatalf("discovery ran %d times across two IsServerAdmin calls, want 1", n)
	}
}
