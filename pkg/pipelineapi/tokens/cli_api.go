package tokens

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// MintCLIAPIToken mints an RS256 JWT for CLI calls AGAINST pipeline-api
// itself (e.g. `datuplet trigger`, `datuplet storage`).
//
// Distinct from MintLocalCLIToken:
//   - aud=datuplet-api          (pipeline-api validates its own tokens)
//   - token_kind=cli-api        (validator pinpoints accepted shape)
//
// Same audit-forgery posture as the rest of this package — the actor/sub
// claim is derived from auth.UserFromContext(ctx), never an argument.
//
// Returns (signed, exp). Caller must use the returned exp verbatim in
// any response field — derived-from-now drift confuses CLI clients.
//
// Claims:
//
//	iss=datuplet-api
//	aud=datuplet-api
//	sub=<user-uuid>
//	actor=<user-uuid>
//	token_kind="cli-api"
//	jti="cli-api-tok-<user-uuid>-<unix-nano>"
//	exp=now+lifetime
//	nbf=now, iat=now
func MintCLIAPIToken(ctx context.Context, signer *Signer, lifetime time.Duration) (string, time.Time, error) {
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
		"aud":        APITokenAudience,
		"sub":        sub,
		"actor":      sub,
		"token_kind": TokenKindCLIAPI,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        exp.Unix(),
		"jti":        fmt.Sprintf("cli-api-tok-%s-%d", sub, now.UnixNano()),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID
	signed, err := tok.SignedString(signer.Private())
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign: %w", err)
	}
	return signed, exp, nil
}
