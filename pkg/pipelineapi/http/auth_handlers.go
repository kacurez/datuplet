package http

import (
	"crypto/rand"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

type loginReq struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// dummyPasswordHash is used by handleLogin when no user matches the requested
// email, so VerifyPassword does the same argon2 work as for a real miss —
// preventing a timing side channel that would leak which emails are registered.
// Computed once at package init from random bytes.
var dummyPasswordHash = mustDummyHash()

func mustDummyHash() string {
	// 32 bytes of randomness — the actual value is irrelevant; we just need a
	// valid PHC argon2id string so VerifyPassword exercises its full path.
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("dummy hash: " + err.Error())
	}
	h, err := auth.HashPassword(string(b))
	if err != nil {
		panic("dummy hash: " + err.Error())
	}
	return h
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "email and password required")
		return
	}

	user, err := store.GetUserByEmail(r.Context(), s.db, req.Email)
	if err != nil && !errors.Is(err, store.ErrUserNotFound) {
		// Real DB/backend failure — do not mask as a credential error,
		// operators need to see infrastructure breakage.
		writeError(w, http.StatusInternalServerError, "authentication temporarily unavailable")
		return
	}

	// Verify the password in constant-ish time regardless of whether the
	// user exists — protects against timing-based enumeration of registered
	// emails. dummyPasswordHash is a PHC-argon2id hash of random bytes so
	// VerifyPassword runs the same expensive path as a real miss.
	stored := dummyPasswordHash
	if user != nil {
		stored = user.PasswordHash
	}
	ok, verr := auth.VerifyPassword(req.Password, stored)
	userInvalid := user == nil || user.DisabledAt != nil || verr != nil || !ok
	if userInvalid {
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	sid, err := auth.CreateSession(r.Context(), s.db, user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create session")
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     auth.SessionCookieName,
		Value:    sid.String(),
		Path:     "/",
		HttpOnly: true,
		Secure:   s.cookieSecure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(auth.SessionLifetime.Seconds()),
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	// If a session cookie is present and parses as a UUID, delete the
	// server-side row. A DB failure must NOT silently succeed — otherwise a
	// stolen cookie would remain usable until expiry even though the API
	// claimed the session was gone.
	if c, err := r.Cookie(auth.SessionCookieName); err == nil {
		if sid, err := uuid.Parse(c.Value); err == nil {
			if err := auth.DeleteSession(r.Context(), s.db, sid); err != nil {
				writeError(w, http.StatusInternalServerError, "logout failed")
				return
			}
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name:     auth.SessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   s.cookieSecure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	// The UI reads "mode" to decide which nav to render.
	// "local" = single-user pipeline-api without Postgres; "cluster" =
	// the standard multi-tenant deploy.
	writeJSON(w, http.StatusOK, map[string]any{
		"id":    user.ID.String(),
		"email": user.Email,
		"mode":  s.resolver.Mode(),
	})
}
