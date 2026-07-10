package http

import (
	"errors"
	"net/http"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// mustBeSuperadmin is the platform-admin guard, sibling to mustHaveRelation.
// It resolves the authenticated user and runs a single FGA Check of
// (user:oidc~<user-uuid>, admin, server:<uuid>) via the memoized
// ServerAdminChecker.
//
// Error mapping (mirrors mustHaveRelation):
//   - 401: no authenticated user.
//   - 503: superadmin checker not wired, or authz backend unavailable
//     (ErrAuthzUnavailable — includes server-object discovery failure).
//   - 500: unexpected authz error.
//   - 403: authenticated but not a superadmin.
//
// On success returns (user, true); callers must return immediately on false.
func (s *Server) mustBeSuperadmin(w http.ResponseWriter, r *http.Request) (*store.User, bool) {
	user, authed := auth.UserFromContext(r.Context())
	if !authed {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return nil, false
	}
	if s.serverAdmin == nil {
		writeError(w, http.StatusServiceUnavailable, "superadmin checks not configured")
		return nil, false
	}
	ok, err := s.serverAdmin.IsServerAdmin(r.Context(), user.ID.String())
	if errors.Is(err, authz.ErrAuthzUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "authz backend unavailable")
		return nil, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "check superadmin")
		return nil, false
	}
	if !ok {
		writeError(w, http.StatusForbidden, "forbidden: superadmin required")
		return nil, false
	}
	return user, true
}
