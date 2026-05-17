package tokens_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// TestMintLocalCLIToken_ClaimsShape pins the wire shape of the JWT minted
// by `POST /api/v1/auth/token`. Lakekeeper consumes this
// token via OIDC + JWKS exactly like impersonation/run tokens, so any
// drift in claim names will cause it to soft-deny at the FGA layer.
func TestMintLocalCLIToken_ClaimsShape(t *testing.T) {
	signer := sharedSigner(t)
	user := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ctx := userCtx(t, user)

	tok, expTime, err := tokens.MintLocalCLIToken(ctx, signer, time.Hour)
	if err != nil {
		t.Fatalf("MintLocalCLIToken: %v", err)
	}

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	checks := map[string]string{
		"iss":        "datuplet-api",
		"aud":        tokens.TableTokenAudience,
		"sub":        user.String(),
		"actor":      user.String(),
		"token_kind": tokens.TokenKindLocalCLI,
		"token_use":  tokens.TokenKindLocalCLI,
	}
	for k, want := range checks {
		if got, _ := claims[k].(string); got != want {
			t.Errorf("claim %q = %v, want %v", k, claims[k], want)
		}
	}

	// jti shape: cli-tok-<uuid>-<unix-nano>
	jti, _ := claims["jti"].(string)
	if got := jti; got == "" {
		t.Errorf("jti must be set, got empty")
	}
	const wantPrefix = "cli-tok-11111111-1111-1111-1111-111111111111-"
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

	// F3: expTime returned by MintLocalCLIToken must match JWT exp claim
	// exactly (same Unix second) — no drift from a second time.Now() call.
	if int64(exp) != expTime.Unix() {
		t.Errorf("returned expTime.Unix() = %d, JWT exp = %d (must match exactly)",
			expTime.Unix(), int64(exp))
	}
}

// TestMintLocalCLIToken_RejectsAnonymous mirrors MintRunToken's audit-
// forgery argument: actor must come from ctx, never from the caller. An
// unauthenticated context must error out — the type-level seam is what
// makes B.1 safe.
func TestMintLocalCLIToken_RejectsAnonymous(t *testing.T) {
	signer := sharedSigner(t)
	if _, _, err := tokens.MintLocalCLIToken(context.Background(), signer, time.Hour); err == nil {
		t.Error("expected error: minting without an authenticated user must fail")
	}
}

// TestMintLocalCLIToken_RequiresPositiveLifetime — tokens without exp are
// rejected by the JWT verifier, so the helper rejects zero/negative
// lifetimes at the source.
func TestMintLocalCLIToken_RequiresPositiveLifetime(t *testing.T) {
	signer := sharedSigner(t)
	ctx := userCtx(t, uuid.New())
	cases := []time.Duration{0, -time.Second, -time.Hour}
	for _, lt := range cases {
		if _, _, err := tokens.MintLocalCLIToken(ctx, signer, lt); err == nil {
			t.Errorf("expected error for lifetime=%v", lt)
		}
	}
}

// TestMintLocalCLIToken_RequiresSigner — nil signer is a programmer error;
// fail loudly at mint time so the call site fails its first request, not
// silently produces unverifiable tokens.
func TestMintLocalCLIToken_RequiresSigner(t *testing.T) {
	ctx := userCtx(t, uuid.New())
	if _, _, err := tokens.MintLocalCLIToken(ctx, nil, time.Hour); err == nil {
		t.Error("expected error for nil signer")
	}
}

// TestLocalCLITokenAcceptedForCatalogAudience pins the token-kind / aud
// pairing the doc-comment in mint.go advertises:
//
//	aud=datuplet-catalog requires token_kind ∈ {run, impersonation, local-cli}
//
// The actual cross-check happens at lakekeeper (its OIDC validator
// enforces aud-and-kind together). This test stands in as a contract
// pin: a JWT minted by MintLocalCLIToken lands with both `aud` and
// `token_kind` exactly as the verifier expects, so we can't ship a token
// that lakekeeper will reject without first failing the build here.
func TestLocalCLITokenAcceptedForCatalogAudience(t *testing.T) {
	signer := sharedSigner(t)
	user := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	ctx := userCtx(t, user)

	tok, _, err := tokens.MintLocalCLIToken(ctx, signer, time.Hour)
	if err != nil {
		t.Fatalf("MintLocalCLIToken: %v", err)
	}

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	// Cross-check: aud=datuplet-catalog AND token_kind ∈ {run, impersonation, local-cli}.
	aud, _ := claims["aud"].(string)
	kind, _ := claims["token_kind"].(string)
	if aud != tokens.TableTokenAudience {
		t.Fatalf("aud = %q, want %q", aud, tokens.TableTokenAudience)
	}
	allowed := map[string]bool{
		tokens.TokenKindRun:           true,
		tokens.TokenKindImpersonation: true,
		tokens.TokenKindLocalCLI:      true,
	}
	if !allowed[kind] {
		t.Errorf("token_kind = %q not in {run, impersonation, local-cli}", kind)
	}
	// Specifically: a local-cli token MUST be the kind a CLI handler emitted.
	if kind != tokens.TokenKindLocalCLI {
		t.Errorf("token_kind = %q, want %q", kind, tokens.TokenKindLocalCLI)
	}
}
