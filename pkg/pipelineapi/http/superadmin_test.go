package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// stubServerAdmin is a test double for authz.ServerAdminChecker.
type stubServerAdmin struct {
	result bool
	err    error
}

func (s stubServerAdmin) ServerObject(context.Context) (string, error) { return "server:x", s.err }
func (s stubServerAdmin) IsServerAdmin(context.Context, string) (bool, error) {
	return s.result, s.err
}

// meStubResolver satisfies auth.UserResolver so handleMe can call Mode().
type meStubResolver struct{}

func (meStubResolver) UserFor(http.ResponseWriter, *http.Request) (*store.User, bool, error) {
	return nil, false, nil
}
func (meStubResolver) Mode() string        { return "cluster" }
func (meStubResolver) SupportsLogin() bool { return false }

var testUser = &store.User{
	ID:    uuid.MustParse("00000000-0000-0000-0000-000000000001"),
	Email: "admin@example.com",
}

// authedRequest builds a request whose context carries testUser, exactly as
// auth.WithUser would after a successful resolve.
func authedRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPut, "/api/v1/admin/components/x", nil)
	return r.WithContext(auth.WithCtxUser(r.Context(), testUser))
}

func TestMustBeSuperadmin_Allows(t *testing.T) {
	s := &Server{serverAdmin: stubServerAdmin{result: true}}
	rec := httptest.NewRecorder()

	user, ok := s.mustBeSuperadmin(rec, authedRequest())
	if !ok {
		t.Fatalf("ok = false (status %d), want true", rec.Code)
	}
	if user == nil || user.ID != testUser.ID {
		t.Fatalf("user = %+v, want testUser", user)
	}
}

func TestMustBeSuperadmin_NonAdmin403(t *testing.T) {
	s := &Server{serverAdmin: stubServerAdmin{result: false}}
	rec := httptest.NewRecorder()

	if _, ok := s.mustBeSuperadmin(rec, authedRequest()); ok {
		t.Fatal("ok = true, want false for a non-superadmin")
	}
	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}
}

func TestMustBeSuperadmin_Unauthenticated401(t *testing.T) {
	s := &Server{serverAdmin: stubServerAdmin{result: true}}
	rec := httptest.NewRecorder()
	// No user in context.
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/components/x", nil)

	if _, ok := s.mustBeSuperadmin(rec, req); ok {
		t.Fatal("ok = true, want false when unauthenticated")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestMustBeSuperadmin_Unavailable503(t *testing.T) {
	s := &Server{serverAdmin: stubServerAdmin{err: authz.ErrAuthzUnavailable}}
	rec := httptest.NewRecorder()

	if _, ok := s.mustBeSuperadmin(rec, authedRequest()); ok {
		t.Fatal("ok = true, want false when authz backend unavailable")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

func TestMustBeSuperadmin_Unwired503(t *testing.T) {
	s := &Server{} // serverAdmin nil
	rec := httptest.NewRecorder()

	if _, ok := s.mustBeSuperadmin(rec, authedRequest()); ok {
		t.Fatal("ok = true, want false when the superadmin checker is unwired")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

func TestMustBeSuperadmin_UnexpectedError500(t *testing.T) {
	s := &Server{serverAdmin: stubServerAdmin{err: errors.New("boom")}}
	rec := httptest.NewRecorder()

	if _, ok := s.mustBeSuperadmin(rec, authedRequest()); ok {
		t.Fatal("ok = true, want false on an unexpected authz error")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func decodeMe(t *testing.T, s *Server) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	req = req.WithContext(auth.WithCtxUser(req.Context(), testUser))
	s.handleMe(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleMe status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}

func TestHandleMe_IsSuperadmin_True(t *testing.T) {
	s := &Server{resolver: meStubResolver{}, serverAdmin: stubServerAdmin{result: true}}
	body := decodeMe(t, s)
	if body["is_superadmin"] != true {
		t.Errorf("is_superadmin = %v, want true", body["is_superadmin"])
	}
}

func TestHandleMe_IsSuperadmin_FalseWhenDenied(t *testing.T) {
	s := &Server{resolver: meStubResolver{}, serverAdmin: stubServerAdmin{result: false}}
	body := decodeMe(t, s)
	if body["is_superadmin"] != false {
		t.Errorf("is_superadmin = %v, want false", body["is_superadmin"])
	}
}

func TestHandleMe_IsSuperadmin_FalseOnError(t *testing.T) {
	// The whoami call must never 5xx over authz availability: an error from
	// the checker degrades is_superadmin to false, still 200.
	s := &Server{resolver: meStubResolver{}, serverAdmin: stubServerAdmin{err: authz.ErrAuthzUnavailable}}
	body := decodeMe(t, s)
	if _, ok := body["is_superadmin"]; !ok {
		t.Fatal("is_superadmin key missing from /me response")
	}
	if body["is_superadmin"] != false {
		t.Errorf("is_superadmin = %v, want false (error degraded)", body["is_superadmin"])
	}
}

func TestHandleMe_IsSuperadmin_FalseWhenUnwired(t *testing.T) {
	s := &Server{resolver: meStubResolver{}} // serverAdmin nil
	body := decodeMe(t, s)
	if _, ok := body["is_superadmin"]; !ok {
		t.Fatal("is_superadmin key missing from /me response")
	}
	if body["is_superadmin"] != false {
		t.Errorf("is_superadmin = %v, want false (checker unwired)", body["is_superadmin"])
	}
}
