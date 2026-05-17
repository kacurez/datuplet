package tokens

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// MintLocalCLIToken produces an RS256 JWT for the `datuplet run --remote`
// flow. Caller-bound to the authenticated user via ctx —
// same audit-forgery prevention as MintRunToken / MintImpersonation: the
// `actor` claim is derived from `subjectFromCtx(ctx)`, never accepted as
// an argument. The handler at `pkg/pipelineapi/http/cli_token_handler.go`
// MUST install the verified user via `auth.WithUser` before calling this.
//
// Distinct from MintRunToken: token_kind=local-cli, no jti denylist
// expectation, lifetime ceiling enforced by the caller (1h is the
// canonical value defined in the handler — long enough for a working-hour
// session, short enough that a leaked token's blast radius is bounded).
//
// Returns both the signed token string and the exact exp time embedded in
// the JWT claims. The caller MUST use the returned exp for the response's
// ExpiresAt field — deriving it from time.Now().Add(lifetime) separately
// would introduce a sub-ms drift that confuses CLI clients comparing both.
//
// Claims:
//
//	iss=datuplet-api
//	aud=datuplet-catalog
//	sub=<user-uuid>           (raw UUID; lakekeeper composes user:oidc~<sub>)
//	actor=<user-uuid>         (same — the user is acting as themselves)
//	token_kind="local-cli"
//	token_use="local-cli"     (legacy duplicate field; matches service-token shape)
//	jti="cli-tok-<user-uuid>-<unix-nano>"
//	exp=now+lifetime
//
// jti is deterministic-per-call (not denylisted): cancellation for the
// CLI flow goes through FGA tuple deletion (matching the run-token
// posture), not a revocation list.
func MintLocalCLIToken(ctx context.Context, signer *Signer, lifetime time.Duration) (string, time.Time, error) {
	if signer == nil {
		return "", time.Time{}, errors.New("signer is required")
	}
	if lifetime <= 0 {
		return "", time.Time{}, errors.New("lifetime must be positive (tokens without exp are rejected)")
	}
	sub, err := subjectFromCtx(ctx)
	if err != nil {
		return "", time.Time{}, err
	}
	now := time.Now()
	exp := now.Add(lifetime).UTC()
	claims := jwt.MapClaims{
		"iss":        tokenIssuer,
		"aud":        TableTokenAudience,
		"sub":        sub,
		"actor":      sub,
		"token_kind": TokenKindLocalCLI,
		"token_use":  TokenKindLocalCLI,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        exp.Unix(),
		"jti":        fmt.Sprintf("cli-tok-%s-%d", sub, now.UnixNano()),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID
	signed, err := tok.SignedString(signer.Private())
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign: %w", err)
	}
	return signed, exp, nil
}
