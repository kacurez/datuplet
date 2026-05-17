package http

import (
	"net/http"
	"strings"
)

// oidcDiscoveryDoc is the minimal OIDC Provider Configuration response
// served at /.well-known/openid-configuration.
//
// Lakekeeper only consumes `issuer` + `jwks_uri` to validate JWTs
// locally. We deliberately omit the OAuth flow fields
// (authorization_endpoint, token_endpoint) rather than advertising
// placeholder values: pipeline-api does not implement an interactive
// OAuth flow, and a placeholder URL would mislead any consumer that
// tries it. The remaining fields satisfy lakekeeper's discovery
// validator without lying about capabilities we don't have.
type oidcDiscoveryDoc struct {
	Issuer                           string   `json:"issuer"`
	JWKSURI                          string   `json:"jwks_uri"`
	ResponseTypesSupported           []string `json:"response_types_supported"`
	SubjectTypesSupported            []string `json:"subject_types_supported"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
}

// handleOIDCDiscovery serves the OIDC Provider Configuration document at
// /.well-known/openid-configuration. Public — no auth required.
//
// URLs in the discovery doc (issuer, jwks_uri) are derived from the
// request's scheme + Host so the doc is "self-relative" and works
// regardless of which URL the consumer used to fetch it:
//
//   - Browser via NodePort (localhost:30081/.well-known/...) → URLs
//     point at localhost:30081 (browser can reach JWKS).
//   - Lakekeeper via cluster DNS
//     (datuplet-pipeline-api.<ns>.svc.cluster.local:8081/.well-known/...)
//     → URLs point at the cluster DNS (lakekeeper pod can reach JWKS).
//
// publicURL is used as a fallback only when the request lacks a Host
// header (synthetic admin call, smoke probe). The doc's `issuer` field
// is informational — pipeline-api's run-tokens carry iss="datuplet-api"
// (the constant in pkg/pipelineapi/tokens) which lakekeeper accepts via
// LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS, so the issuer in this doc does
// not need to match the token claim.
func (s *Server) handleOIDCDiscovery(w http.ResponseWriter, r *http.Request) {
	if s.signer == nil {
		writeError(w, http.StatusNotFound, "OIDC discovery not configured")
		return
	}
	base := requestBaseURL(r)
	if base == "" {
		base = strings.TrimRight(s.publicURL, "/")
	}
	if base == "" {
		writeError(w, http.StatusNotFound, "OIDC discovery not configured")
		return
	}
	doc := oidcDiscoveryDoc{
		Issuer:                           base,
		JWKSURI:                          base + "/api/v1/auth/jwks.json",
		ResponseTypesSupported:           []string{"id_token"},
		SubjectTypesSupported:            []string{"public"},
		IDTokenSigningAlgValuesSupported: []string{"RS256"},
	}
	writeJSON(w, http.StatusOK, doc)
}

// requestBaseURL returns "<scheme>://<host>" derived from the inbound
// request. Returns "" if Host is empty. Honours X-Forwarded-Proto when
// behind a proxy/ingress that terminates TLS.
func requestBaseURL(r *http.Request) string {
	if r == nil || r.Host == "" {
		return ""
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if v := r.Header.Get("X-Forwarded-Proto"); v != "" {
		// Trust only the first value; ingress controllers occasionally
		// chain "https,http" through this header.
		if i := strings.IndexByte(v, ','); i >= 0 {
			v = v[:i]
		}
		v = strings.TrimSpace(v)
		if v == "http" || v == "https" {
			scheme = v
		}
	}
	return scheme + "://" + r.Host
}
