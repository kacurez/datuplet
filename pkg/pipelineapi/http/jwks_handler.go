package http

import (
	"net/http"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// handleJWKS serves the single-key JWKS document at /api/v1/auth/jwks.json.
// Public — no auth, no DB dependency. Computed once per request from the
// signer's public key; an in-memory cache is unnecessary given how tiny the
// output is.
func (s *Server) handleJWKS(w http.ResponseWriter, _ *http.Request) {
	if s.signer == nil {
		writeError(w, http.StatusNotFound, "signing key not configured")
		return
	}
	jwks := tokens.JWKSFromPublicKey(s.signer.Public(), s.signer.KeyID)
	writeJSON(w, http.StatusOK, jwks)
}
