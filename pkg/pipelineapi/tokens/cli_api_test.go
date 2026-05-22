package tokens_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// TestMintCLIAPIToken_ClaimsShape pins the wire shape of the JWT minted for
// pipeline-api bearer auth. Any drift in claim names will cause the
// BearerJWTResolver to reject CLI requests.
func TestMintCLIAPIToken_ClaimsShape(t *testing.T) {
	signer := sharedSigner(t)
	user := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	ctx := userCtx(t, user)

	tok, expTime, err := tokens.MintCLIAPIToken(ctx, signer, time.Hour)
	if err != nil {
		t.Fatalf("MintCLIAPIToken: %v", err)
	}

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	checks := map[string]string{
		"iss":        "datuplet-api",
		"aud":        tokens.APITokenAudience,
		"sub":        user.String(),
		"actor":      user.String(),
		"token_kind": tokens.TokenKindCLIAPI,
	}
	for k, want := range checks {
		if got, _ := claims[k].(string); got != want {
			t.Errorf("claim %q = %v, want %v", k, claims[k], want)
		}
	}

	// jti shape: cli-api-tok-<uuid>-<unix-nano>
	jti, _ := claims["jti"].(string)
	if jti == "" {
		t.Errorf("jti must be set, got empty")
	}
	const wantPrefix = "cli-api-tok-33333333-3333-3333-3333-333333333333-"
	if len(jti) < len(wantPrefix) || jti[:len(wantPrefix)] != wantPrefix {
		t.Errorf("jti = %q, want prefix %q", jti, wantPrefix)
	}

	// exp window check: now ≤ exp ≤ now+1h+slack
	exp, _ := claims["exp"].(float64)
	now := time.Now().Unix()
	if int64(exp) < now {
		t.Errorf("exp %d in the past (now=%d)", int64(exp), now)
	}
	if int64(exp) > now+int64((time.Hour+time.Minute).Seconds()) {
		t.Errorf("exp %d too far in the future (now=%d)", int64(exp), now)
	}

	// expTime returned must match JWT exp claim exactly.
	if int64(exp) != expTime.Unix() {
		t.Errorf("returned expTime.Unix() = %d, JWT exp = %d (must match exactly)",
			expTime.Unix(), int64(exp))
	}
}

// TestMintCLIAPIToken_RejectsAnonymous ensures the actor comes from ctx,
// not from a caller-supplied argument.
func TestMintCLIAPIToken_RejectsAnonymous(t *testing.T) {
	signer := sharedSigner(t)
	if _, _, err := tokens.MintCLIAPIToken(context.Background(), signer, time.Hour); err == nil {
		t.Error("expected error: minting without an authenticated user must fail")
	}
}

// TestMintCLIAPIToken_RequiresPositiveLifetime — tokens without exp are
// rejected by the JWT verifier, so the helper rejects zero/negative lifetimes.
func TestMintCLIAPIToken_RequiresPositiveLifetime(t *testing.T) {
	signer := sharedSigner(t)
	ctx := userCtx(t, uuid.New())
	cases := []time.Duration{0, -time.Second, -time.Hour}
	for _, lt := range cases {
		if _, _, err := tokens.MintCLIAPIToken(ctx, signer, lt); err == nil {
			t.Errorf("expected error for lifetime=%v", lt)
		}
	}
}

// TestMintCLIAPIToken_RequiresSigner — nil signer is a programmer error.
func TestMintCLIAPIToken_RequiresSigner(t *testing.T) {
	ctx := userCtx(t, uuid.New())
	if _, _, err := tokens.MintCLIAPIToken(ctx, nil, time.Hour); err == nil {
		t.Error("expected error for nil signer")
	}
}

// TestMintCLIAPIToken_AudienceDistinctFromCatalog pins that api tokens use
// datuplet-api, not datuplet-catalog, so they cannot be replayed to lakekeeper.
func TestMintCLIAPIToken_AudienceDistinctFromCatalog(t *testing.T) {
	signer := sharedSigner(t)
	ctx := userCtx(t, uuid.New())

	tok, _, err := tokens.MintCLIAPIToken(ctx, signer, time.Hour)
	if err != nil {
		t.Fatalf("MintCLIAPIToken: %v", err)
	}

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	aud, _ := claims["aud"].(string)
	if aud == tokens.TableTokenAudience {
		t.Errorf("api token must NOT use TableTokenAudience (%q); got aud=%q", tokens.TableTokenAudience, aud)
	}
	if aud != tokens.APITokenAudience {
		t.Errorf("aud = %q, want %q", aud, tokens.APITokenAudience)
	}
}
