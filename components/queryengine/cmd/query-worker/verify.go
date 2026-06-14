package main

// verify.go implements the HTTP-boundary verifier for the internal-query JWTs
// that authenticate pipeline-api→query-worker hops (RFC 022 Task 2.2).
//
// It deliberately mirrors pkg/datagateway/runtoken's discipline: RS256-only
// (pointer-equality on the singleton + jwt.WithValidMethods), the same ±60s
// clock skew, kid-keyed key lookup, and sanitized errors that never embed the
// token text. Audience and token_kind are pinned to the RFC 022 internal-query
// shape via locally-declared constants (see queryWorkerAudience below).
//
// No engine import lives here, so the file carries no `duckdb_arrow` build tag:
// the verifier is pure JWT logic and its tests run without CGO.

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// expectedIssuer mirrors tokens.tokenIssuer in pkg/pipelineapi/tokens.
	expectedIssuer = "datuplet-api"

	// queryWorkerAudience and tokenKindInternalQuery are intentionally
	// re-declared here rather than imported from pkg/pipelineapi/tokens:
	// tokens/mint.go's auth→store→pgx/openfga transitive chain must not link
	// into the worker binary. These values are pinned to
	// tokens.QueryWorkerAudience / tokens.TokenKindInternalQuery by
	// TestConstantsParity in verify_test.go (test files don't link into the
	// binary, so the test may import tokens).
	queryWorkerAudience    = "datuplet-query-worker"
	tokenKindInternalQuery = "internal-query"

	// clockSkewSeconds is the permitted clock-skew tolerance applied to
	// exp / nbf. 60s matches pkg/datagateway/runtoken's ClockSkewSeconds.
	clockSkewSeconds = 60

	// errPrefix is the stable, sanitized prefix on every rejection. Errors
	// describe which check failed, never the raw token bytes.
	errPrefix = "internal token rejected"
)

// KeyProvider resolves an RSA public key by kid. pkg/datagateway/jwks.Client
// satisfies this interface (its KeyFor signature matches exactly), so Task 2.3
// can wire the real JWKS client in with no adapter. Tests use a stub.
type KeyProvider interface {
	KeyFor(ctx context.Context, kid string) (*rsa.PublicKey, error)
}

// tokenVerifier verifies internal-query JWTs at the worker's HTTP boundary.
type tokenVerifier struct {
	keys KeyProvider
}

// newTokenVerifier constructs a verifier over the given KeyProvider.
func newTokenVerifier(keys KeyProvider) *tokenVerifier {
	return &tokenVerifier{keys: keys}
}

// Verify validates an internal-query JWT and returns its sub and jti for the
// audit layer (Task 2.4b). Checks, in order:
//
//  1. Parse + signature against the kid-matched key; RS256 only
//     (alg=none / HS256 / RS384 / RS512 all rejected). exp is required.
//  2. iss == datuplet-api.
//  3. aud is exactly one element and equals queryWorkerAudience
//     (datuplet-query-worker) — rejects cross-token replay of
//     catalog-audience query/run tokens and multi-audience replay.
//  4. exp / nbf valid within ±clockSkewSeconds (done by ParseWithClaims).
//  5. token_kind == tokenKindInternalQuery (internal-query).
//  6. sub present and non-empty.
//
// On ANY failure it returns a single sanitized error wrapped with errPrefix;
// the token text is never included.
func (v *tokenVerifier) Verify(ctx context.Context, tokenString string) (subject, jti string, err error) {
	// Check 1: parse + signature. The keyfunc pins RS256 by pointer-equality
	// with the package singleton (rejecting RS384 / RS512 / PSxxx / HMAC /
	// none, which share the *SigningMethodRSA embedded type); WithValidMethods
	// is a second layer at the parser level.
	var claims jwt.MapClaims
	_, perr := jwt.ParseWithClaims(tokenString, &claims, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodRS256 {
			return nil, fmt.Errorf("unexpected signing method %q (want RS256 only)", t.Header["alg"])
		}
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("kid header missing from token")
		}
		return v.keys.KeyFor(ctx, kid)
	}, jwt.WithLeeway(clockSkewSeconds*time.Second), jwt.WithValidMethods([]string{"RS256"}), jwt.WithExpirationRequired())
	if perr != nil {
		return "", "", fmt.Errorf("%s: parse/signature: %w", errPrefix, perr)
	}

	// Check 2: issuer. Static message — never embed the attacker-controlled
	// iss claim (log-injection hygiene).
	if iss, _ := claims["iss"].(string); iss != expectedIssuer {
		return "", "", fmt.Errorf("%s: iss mismatch", errPrefix)
	}

	// Check 3: audience, runtoken-style strict discipline. GetAudience handles
	// the string / []string / []interface{} shapes and errors on non-string
	// elements. The minter only ever sets a single string aud, so we require
	// EXACTLY ONE element equal to queryWorkerAudience — multi-audience tokens
	// are rejected outright, closing the dual-audience replay wrinkle.
	aud, aerr := claims.GetAudience()
	if aerr != nil {
		return "", "", fmt.Errorf("%s: aud: %w", errPrefix, aerr)
	}
	if len(aud) != 1 || aud[0] != queryWorkerAudience {
		return "", "", fmt.Errorf("%s: aud is not exactly [%q]", errPrefix, queryWorkerAudience)
	}

	// Check 4: exp / nbf validated by ParseWithClaims with the leeway above;
	// exp presence enforced by WithExpirationRequired.

	// Check 5: token_kind. Static message — never embed the attacker-controlled
	// token_kind claim (log-injection hygiene).
	if kind, _ := claims["token_kind"].(string); kind != tokenKindInternalQuery {
		return "", "", fmt.Errorf("%s: token_kind mismatch", errPrefix)
	}

	// Check 6: sub present and non-empty.
	subject, _ = claims["sub"].(string)
	if subject == "" {
		return "", "", fmt.Errorf("%s: required claim sub is missing or empty", errPrefix)
	}

	// jti is returned for the audit layer; a missing/empty jti is not fatal
	// here (the mint side always sets one), but it MUST never be the token.
	jti, _ = claims["jti"].(string)

	return subject, jti, nil
}
