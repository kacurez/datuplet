package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// localQueryMintHandler implements POST /api/v1/query/token (RFC 022 §5.3):
// the per-invocation mint endpoint the laptop-side `datuplet-query` CLI calls
// to obtain a fresh short-lived query JWT for BYO-local (mode c) execution.
//
// # Security posture (read before changing)
//
//  1. Gated by the allowClientSideQuery policy, default FALSE. When off, NO
//     token is minted — the refusal is server-enforced, so an old/rogue CLI
//     cannot bypass it. The endpoint is ALWAYS registered (whenever a signer
//     exists) and returns 403 when off, never 404: the client expects a clear
//     refusal, not a missing route.
//  2. Runs behind auth.WithUser (aud=datuplet-api api-token bearer). The
//     handler still defends with a 401 if no user is in ctx (defence in depth).
//  3. The minted query JWT (aud=datuplet-catalog, token_kind=query) has its
//     `sub` derived from the ctx user by MintQueryToken — never caller-supplied
//     — so it cannot mint on behalf of someone else.
//  4. Audit is mint-level (RFC §4.1): exactly one structured log line per
//     authorized mint, carrying the principal + timestamp. There is no SQL at
//     mint time. NEVER log the minted token or the api-token.
type localQueryMintHandler struct {
	signer               *tokens.Signer
	allowClientSideQuery bool
	logger               *slog.Logger
}

// NewLocalQueryMintHandler builds the POST /api/v1/query/token handler. The
// signer is the same *tokens.Signer wired onto the rest of pipeline-api;
// allowClientSideQuery is the operator opt-in policy (default false at the
// call site). The handler is constructed unconditionally so the policy-off
// 403 path works even when client-side query is disabled.
func NewLocalQueryMintHandler(signer *tokens.Signer, allowClientSideQuery bool) http.Handler {
	return &localQueryMintHandler{
		signer:               signer,
		allowClientSideQuery: allowClientSideQuery,
		logger:               slog.Default(),
	}
}

// queryTokenResponse is the 200 body. Token is a plain string set from
// QueryToken.Reveal() — the QueryToken type's MarshalJSON redacts, so it must
// NOT be embedded directly. Mirrors the FIXED client contract
// (components/queryengine/cmd/datuplet-query/mint.go::mintTokenResponse).
type queryTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

func (h *localQueryMintHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Authenticated user from ctx (auth.WithUser puts it there). The
	//    middleware normally guarantees this; the 401 is defence in depth.
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		writeQueryTokenError(w, http.StatusUnauthorized, "not authenticated", "unauthorized")
		return
	}

	// 2. Policy gate. Default OFF → 403 with the EXACT envelope the client
	//    surfaces verbatim and special-cases on (kind="forbidden"). No token
	//    is minted: the refusal is server-enforced.
	if !h.allowClientSideQuery {
		writeQueryTokenError(w, http.StatusForbidden,
			"client-side query disabled; use the server query service", "forbidden")
		return
	}

	// 3. Mint the query-scoped catalog JWT. sub is derived from the ctx user
	//    inside MintQueryToken; ttl is clamped to MaxQueryTokenLifetime.
	//    Capture mintedAt just BEFORE the mint: MintQueryToken stamps exp from
	//    its own (slightly later) time.Now(), so mintedAt <= that instant and the
	//    expires_at we report is <= the JWT's real exp — the safe direction (a
	//    client never believes the token outlives its actual exp).
	ttl := tokens.MaxQueryTokenLifetime
	mintedAt := time.Now()
	tok, err := tokens.MintQueryToken(r.Context(), h.signer, ttl)
	if err != nil {
		h.logger.Error("client-side query mint failed", "principal", user.ID.String(), "err", err)
		writeQueryTokenError(w, http.StatusInternalServerError,
			"failed to mint query credentials", "internal")
		return
	}

	// 4. Mint-level audit (RFC §4.1): exactly one line per authorized mint,
	//    principal + timestamp only. No SQL exists at mint time; the minted
	//    token and the api-token are NEVER logged.
	h.logger.Info("client-side query authorized",
		"principal", user.ID.String(),
		"timestamp", mintedAt.UTC().Format(time.RFC3339),
	)

	writeJSON(w, http.StatusOK, queryTokenResponse{
		Token:     tok.Reveal(),
		ExpiresAt: mintedAt.Add(tokens.MaxQueryTokenLifetime).Format(time.RFC3339),
	})
}

// writeQueryTokenError writes the {"error","kind"} envelope the client expects
// (mirrors queryproxy.writeError). The http package's own writeError emits
// {"error"} only, so this endpoint writes the two-field shape explicitly.
func writeQueryTokenError(w http.ResponseWriter, status int, msg, kind string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg, "kind": kind})
}
