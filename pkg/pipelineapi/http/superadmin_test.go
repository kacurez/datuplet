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

func TestMustBeSuperadmin(t *testing.T) {
	tests := []struct {
		name     string
		s        *Server
		unauthed bool // no user in the request context
		wantOK   bool
		wantCode int // checked when !wantOK
	}{
		{name: "superadmin allowed", s: &Server{serverAdmin: stubServerAdmin{result: true}}, wantOK: true},
		{name: "non-admin 403", s: &Server{serverAdmin: stubServerAdmin{result: false}}, wantCode: http.StatusForbidden},
		{name: "unauthenticated 401", s: &Server{serverAdmin: stubServerAdmin{result: true}}, unauthed: true, wantCode: http.StatusUnauthorized},
		{name: "authz unavailable 503", s: &Server{serverAdmin: stubServerAdmin{err: authz.ErrAuthzUnavailable}}, wantCode: http.StatusServiceUnavailable},
		// serverAdmin nil: the superadmin checker is unwired.
		{name: "unwired 503", s: &Server{}, wantCode: http.StatusServiceUnavailable},
		{name: "unexpected error 500", s: &Server{serverAdmin: stubServerAdmin{err: errors.New("boom")}}, wantCode: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := authedRequest()
			if tc.unauthed {
				req = httptest.NewRequest(http.MethodPut, "/api/v1/admin/components/x", nil)
			}

			user, ok := tc.s.mustBeSuperadmin(rec, req)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v (status %d), want %v", ok, rec.Code, tc.wantOK)
			}
			if tc.wantOK {
				if user == nil || user.ID != testUser.ID {
					t.Fatalf("user = %+v, want testUser", user)
				}
				return
			}
			if rec.Code != tc.wantCode {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantCode)
			}
		})
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

// TestHandleMe_IsSuperadmin: the whoami call must never 5xx over authz
// availability — an error from the checker, or an unwired checker, degrades
// is_superadmin to false (key still present, status still 200).
func TestHandleMe_IsSuperadmin(t *testing.T) {
	tests := []struct {
		name string
		s    *Server
		want bool
	}{
		{"granted", &Server{resolver: meStubResolver{}, serverAdmin: stubServerAdmin{result: true}}, true},
		{"denied", &Server{resolver: meStubResolver{}, serverAdmin: stubServerAdmin{result: false}}, false},
		{"checker error degrades to false", &Server{resolver: meStubResolver{}, serverAdmin: stubServerAdmin{err: authz.ErrAuthzUnavailable}}, false},
		{"unwired checker", &Server{resolver: meStubResolver{}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := decodeMe(t, tc.s)
			got, ok := body["is_superadmin"]
			if !ok {
				t.Fatal("is_superadmin key missing from /me response")
			}
			if got != tc.want {
				t.Errorf("is_superadmin = %v, want %v", got, tc.want)
			}
		})
	}
}
