package tokens_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// sharedSigner builds a pipeline-api Signer from a fresh keypair. Tests
// that need to verify signatures decode tokens via jwt.ParseUnverified.
func sharedSigner(t *testing.T) *tokens.Signer {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	dir := t.TempDir()
	privDER, _ := x509.MarshalPKCS8PrivateKey(priv)
	privPath := filepath.Join(dir, "priv.pem")
	_ = os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o400)
	signer, err := tokens.LoadPrivateKeyFromPEMFile(privPath, "key-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return signer
}

// userCtx builds a context with a *store.User attached the same way
// auth.WithUser does at runtime. MintRunToken / MintImpersonation read
// the actor/sub from the user via auth.UserFromContext.
func userCtx(t *testing.T, userID uuid.UUID) context.Context {
	t.Helper()
	u := &store.User{ID: userID}
	// auth.contextWithUser is unexported (sole writer is auth.WithUser);
	// the public read path is auth.UserFromContext. We reach the same
	// effect by going through a fake test handler: build a request,
	// install the user via WithUser, capture the resulting ctx.
	// Cheaper: directly use the package's only read seam — here we
	// use auth.WithUser indirectly via NewRequest.
	return auth.WithCtxUser(context.Background(), u)
}

// --- MintRunToken ---

func TestMintRunToken_ClaimsShape(t *testing.T) {
	signer := sharedSigner(t)
	creator := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	runID := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	ctx := userCtx(t, creator)
	tok, err := tokens.MintRunToken(ctx, signer, tokens.RunSpec{
		RunID:        runID.String(),
		ProjectID:    "proj-abc",
		PipelineName: "etl-daily",
		Warehouse:    "wh-test",
		Lifetime:     1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("MintRunToken: %v", err)
	}

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	// `sub` and `actor` are RAW UUIDs, NOT the `user:oidc~<uuid>` FGA form.
	// Lakekeeper composes the full FGA user object from `<idp_id>~<sub>`
	// during JWT normalisation; pre-composing here would cause
	// double-prefixing (`user:oidc~user:oidc~<uuid>`) and never match
	// Datuplet's tuples.
	checks := map[string]string{
		"aud":           tokens.TableTokenAudience,
		"sub":           runID.String(),
		"actor":         creator.String(),
		"token_kind":    tokens.TokenKindRun,
		"jti":           tokens.JTIForRunID(runID.String()),
		"project_id":    "proj-abc",
		"warehouse":     "wh-test",
		"run_id":        runID.String(),
		"pipeline_name": "etl-daily",
	}
	for k, want := range checks {
		if got, _ := claims[k].(string); got != want {
			t.Errorf("claim %q = %v, want %v", k, claims[k], want)
		}
	}
}

func TestMintRunToken_RejectsAnonymous(t *testing.T) {
	signer := sharedSigner(t)
	if _, err := tokens.MintRunToken(context.Background(), signer, tokens.RunSpec{
		RunID:    "r",
		Lifetime: time.Hour,
	}); err == nil {
		t.Error("expected error: minting without an authenticated user must fail")
	}
}

func TestMintRunToken_RequiresFields(t *testing.T) {
	signer := sharedSigner(t)
	ctx := userCtx(t, uuid.New())
	cases := []struct {
		name string
		spec tokens.RunSpec
	}{
		{"missing run", tokens.RunSpec{Lifetime: time.Hour}},
		{"zero lifetime", tokens.RunSpec{RunID: "r"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tokens.MintRunToken(ctx, signer, tc.spec); err == nil {
				t.Error("expected error")
			}
		})
	}
}

// --- MintImpersonation ---

func TestMintImpersonation_ClaimsShape(t *testing.T) {
	signer := sharedSigner(t)
	creator := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ctx := userCtx(t, creator)
	tok, err := tokens.MintImpersonation(ctx, signer)
	if err != nil {
		t.Fatalf("MintImpersonation: %v", err)
	}
	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok.Reveal(), jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)
	// Raw UUID, NOT `user:oidc~<uuid>` — see MintRunToken claim-shape test
	// for the lakekeeper-normalisation rationale.
	want := creator.String()
	if got, _ := claims["sub"].(string); got != want {
		t.Errorf("sub = %q, want %q", got, want)
	}
	if got, _ := claims["actor"].(string); got != want {
		t.Errorf("actor = %q, want %q", got, want)
	}
	if got, _ := claims["aud"].(string); got != tokens.TableTokenAudience {
		t.Errorf("aud = %q, want %q", got, tokens.TableTokenAudience)
	}
	if got, _ := claims["token_kind"].(string); got != tokens.TokenKindImpersonation {
		t.Errorf("token_kind = %q, want %q", got, tokens.TokenKindImpersonation)
	}
}

func TestMintImpersonation_RejectsAnonymous(t *testing.T) {
	signer := sharedSigner(t)
	if _, err := tokens.MintImpersonation(context.Background(), signer); err == nil {
		t.Error("expected error: minting without an authenticated user must fail")
	}
}

// TestImpersonationToken_RedactsInFmt ensures %s / %v / %#v don't leak
// the raw JWT — the audit point. A stray %v in an error chain must NOT
// surface the impersonation grant to logs.
func TestImpersonationToken_RedactsInFmt(t *testing.T) {
	tok := tokens.ImpersonationToken("eyJ-secret-jwt")
	const redactedLiteral = "[redacted impersonation token]"
	if got := tok.String(); got != redactedLiteral {
		t.Errorf("%%s: got=%q want=%q", got, redactedLiteral)
	}
	if got := tok.GoString(); got != redactedLiteral {
		t.Errorf("%%#v: got=%q want=%q", got, redactedLiteral)
	}
	if got := tok.Reveal(); got != "eyJ-secret-jwt" {
		t.Errorf("Reveal(): got=%q want=eyJ-secret-jwt", got)
	}
}

// --- Service token ---

func TestMintServiceToken_ClaimsShape(t *testing.T) {
	signer := sharedSigner(t)
	spec := tokens.ServiceTokenSpec{
		Subject:  "pipeline-api-bootstrap",
		Lifetime: 5 * time.Minute,
	}
	tok, err := tokens.MintServiceToken(signer, spec)
	if err != nil {
		t.Fatalf("MintServiceToken: %v", err)
	}
	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	if got, _ := claims["aud"].(string); got != "datuplet-catalog" {
		t.Errorf("aud = %v, want datuplet-catalog", claims["aud"])
	}
	checks := map[string]string{
		"sub":        "pipeline-api-bootstrap",
		"jti":        "svc-tok-pipeline-api-bootstrap",
		"token_type": "service",
		"token_use":  "service",
	}
	for k, want := range checks {
		if got, _ := claims[k].(string); got != want {
			t.Errorf("claim %q = %v, want %v", k, claims[k], want)
		}
	}
}

func TestMintServiceToken_RequiresFields(t *testing.T) {
	signer := sharedSigner(t)
	if _, err := tokens.MintServiceToken(signer, tokens.ServiceTokenSpec{Lifetime: time.Hour}); err == nil {
		t.Error("expected error for missing Subject")
	}
	if _, err := tokens.MintServiceToken(signer, tokens.ServiceTokenSpec{Subject: "s"}); err == nil {
		t.Error("expected error for zero Lifetime")
	}
}

func TestJTIForRunID_Deterministic(t *testing.T) {
	a := tokens.JTIForRunID("run-1")
	b := tokens.JTIForRunID("run-1")
	if a != b {
		t.Errorf("not deterministic: %q vs %q", a, b)
	}
	if a != "run-tok-run-1" {
		t.Errorf("unexpected jti shape: %q", a)
	}
}
