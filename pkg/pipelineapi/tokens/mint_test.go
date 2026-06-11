package tokens_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
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

// --- MintQueryToken / MintInternalQueryToken (RFC 022 Task 2.1) ---

func TestMintQueryToken_ClaimsShape(t *testing.T) {
	signer := sharedSigner(t)
	caller := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	ctx := userCtx(t, caller)

	before := time.Now()
	tok, err := tokens.MintQueryToken(ctx, signer, 120*time.Second)
	if err != nil {
		t.Fatalf("MintQueryToken: %v", err)
	}
	after := time.Now()

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok.Reveal(), jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	// sub = ctx user (raw UUID, same derivation as MintImpersonation).
	want := caller.String()
	if got, _ := claims["sub"].(string); got != want {
		t.Errorf("sub = %q, want %q", got, want)
	}
	if got, _ := claims["actor"].(string); got != want {
		t.Errorf("actor = %q, want %q", got, want)
	}
	if got, _ := claims["iss"].(string); got != "datuplet-api" {
		t.Errorf("iss = %q, want datuplet-api", got)
	}
	if got, _ := claims["aud"].(string); got != tokens.TableTokenAudience {
		t.Errorf("aud = %q, want %q", got, tokens.TableTokenAudience)
	}
	if got, _ := claims["token_kind"].(string); got != tokens.TokenKindQuery {
		t.Errorf("token_kind = %q, want %q", got, tokens.TokenKindQuery)
	}

	// exp = now+ttl (ttl < clamp, so honoured exactly within wall-clock skew).
	exp, ok := claims["exp"].(float64)
	if !ok {
		t.Fatalf("exp missing or not numeric: %v", claims["exp"])
	}
	wantMin := before.Add(120 * time.Second).Unix()
	wantMax := after.Add(120 * time.Second).Unix()
	if int64(exp) < wantMin || int64(exp) > wantMax {
		t.Errorf("exp = %d, want in [%d,%d]", int64(exp), wantMin, wantMax)
	}

	// nbf/iat sane: both present, <= exp, within the mint window.
	nbf, ok := claims["nbf"].(float64)
	if !ok {
		t.Fatalf("nbf missing or not numeric: %v", claims["nbf"])
	}
	iat, ok := claims["iat"].(float64)
	if !ok {
		t.Fatalf("iat missing or not numeric: %v", claims["iat"])
	}
	if int64(nbf) < before.Unix() || int64(nbf) > after.Unix() {
		t.Errorf("nbf = %d, want in [%d,%d]", int64(nbf), before.Unix(), after.Unix())
	}
	if int64(iat) < before.Unix() || int64(iat) > after.Unix() {
		t.Errorf("iat = %d, want in [%d,%d]", int64(iat), before.Unix(), after.Unix())
	}
	if int64(nbf) > int64(exp) {
		t.Errorf("nbf %d after exp %d", int64(nbf), int64(exp))
	}

	// jti present and unique across two mints.
	jti1, _ := claims["jti"].(string)
	if jti1 == "" {
		t.Error("jti missing")
	}
	tok2, err := tokens.MintQueryToken(ctx, signer, 120*time.Second)
	if err != nil {
		t.Fatalf("MintQueryToken #2: %v", err)
	}
	parsed2, _, err := parser.ParseUnverified(tok2.Reveal(), jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse #2: %v", err)
	}
	jti2, _ := parsed2.Claims.(jwt.MapClaims)["jti"].(string)
	if jti1 == jti2 {
		t.Errorf("jti not unique across mints: %q == %q", jti1, jti2)
	}
}

func TestMintQueryToken_TTLClamp(t *testing.T) {
	signer := sharedSigner(t)
	ctx := userCtx(t, uuid.New())

	// Requesting a TTL > 330s clamps exp to now+330s.
	before := time.Now()
	tok, err := tokens.MintQueryToken(ctx, signer, 10*time.Minute)
	if err != nil {
		t.Fatalf("MintQueryToken: %v", err)
	}
	after := time.Now()

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok.Reveal(), jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	exp, _ := parsed.Claims.(jwt.MapClaims)["exp"].(float64)
	wantMin := before.Add(330 * time.Second).Unix()
	wantMax := after.Add(330 * time.Second).Unix()
	if int64(exp) < wantMin || int64(exp) > wantMax {
		t.Errorf("clamped exp = %d, want in [%d,%d] (now+330s)", int64(exp), wantMin, wantMax)
	}

	// A TTL <= 0 must error — tokens without a sane exp are rejected,
	// mirroring the sibling minters' Lifetime<=0 guard.
	if _, err := tokens.MintQueryToken(ctx, signer, 0); err == nil {
		t.Error("expected error for zero ttl")
	}
	if _, err := tokens.MintQueryToken(ctx, signer, -1*time.Second); err == nil {
		t.Error("expected error for negative ttl")
	}
}

func TestMintQueryToken_RejectsAnonymous(t *testing.T) {
	signer := sharedSigner(t)
	if _, err := tokens.MintQueryToken(context.Background(), signer, 60*time.Second); err == nil {
		t.Error("expected error: minting without an authenticated user must fail")
	}
}

func TestMintInternalQueryToken_ClaimsShape(t *testing.T) {
	signer := sharedSigner(t)
	caller := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	ctx := userCtx(t, caller)

	tok, err := tokens.MintInternalQueryToken(ctx, signer, 120*time.Second)
	if err != nil {
		t.Fatalf("MintInternalQueryToken: %v", err)
	}

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok.Reveal(), jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	want := caller.String()
	if got, _ := claims["sub"].(string); got != want {
		t.Errorf("sub = %q, want %q", got, want)
	}
	if got, _ := claims["actor"].(string); got != want {
		t.Errorf("actor = %q, want %q", got, want)
	}
	if got, _ := claims["iss"].(string); got != "datuplet-api" {
		t.Errorf("iss = %q, want datuplet-api", got)
	}
	if got, _ := claims["aud"].(string); got != tokens.QueryWorkerAudience {
		t.Errorf("aud = %q, want %q", got, tokens.QueryWorkerAudience)
	}
	if got, _ := claims["token_kind"].(string); got != tokens.TokenKindInternalQuery {
		t.Errorf("token_kind = %q, want %q", got, tokens.TokenKindInternalQuery)
	}

	// jti present and unique across two mints (same discipline as query token).
	jti1, _ := claims["jti"].(string)
	if jti1 == "" {
		t.Error("jti missing")
	}
	tok2, err := tokens.MintInternalQueryToken(ctx, signer, 120*time.Second)
	if err != nil {
		t.Fatalf("MintInternalQueryToken #2: %v", err)
	}
	parsed2, _, err := parser.ParseUnverified(tok2.Reveal(), jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse #2: %v", err)
	}
	jti2, _ := parsed2.Claims.(jwt.MapClaims)["jti"].(string)
	if jti1 == jti2 {
		t.Errorf("jti not unique across mints: %q == %q", jti1, jti2)
	}
}

func TestMintInternalQueryToken_TTLClamp(t *testing.T) {
	signer := sharedSigner(t)
	ctx := userCtx(t, uuid.New())

	before := time.Now()
	tok, err := tokens.MintInternalQueryToken(ctx, signer, 10*time.Minute)
	if err != nil {
		t.Fatalf("MintInternalQueryToken: %v", err)
	}
	after := time.Now()

	parser := jwt.NewParser()
	parsed, _, err := parser.ParseUnverified(tok.Reveal(), jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	exp, _ := parsed.Claims.(jwt.MapClaims)["exp"].(float64)
	wantMin := before.Add(330 * time.Second).Unix()
	wantMax := after.Add(330 * time.Second).Unix()
	if int64(exp) < wantMin || int64(exp) > wantMax {
		t.Errorf("clamped exp = %d, want in [%d,%d] (now+330s)", int64(exp), wantMin, wantMax)
	}

	if _, err := tokens.MintInternalQueryToken(ctx, signer, 0); err == nil {
		t.Error("expected error for zero ttl")
	}
	if _, err := tokens.MintInternalQueryToken(ctx, signer, -1*time.Second); err == nil {
		t.Error("expected error for negative ttl")
	}
}

func TestMintInternalQueryToken_RejectsAnonymous(t *testing.T) {
	signer := sharedSigner(t)
	if _, err := tokens.MintInternalQueryToken(context.Background(), signer, 60*time.Second); err == nil {
		t.Error("expected error: minting without an authenticated user must fail")
	}
}

// TestQueryToken_RedactsInFmt mirrors TestImpersonationToken_RedactsInFmt:
// %s / %v / %#v and json.Marshal must never leak the raw JWT.
func TestQueryToken_RedactsInFmt(t *testing.T) {
	tok := tokens.QueryToken("eyJ-secret-jwt")
	const redactedLiteral = "[redacted query token]"
	if got := tok.String(); got != redactedLiteral {
		t.Errorf("%%s: got=%q want=%q", got, redactedLiteral)
	}
	if got := tok.GoString(); got != redactedLiteral {
		t.Errorf("%%#v: got=%q want=%q", got, redactedLiteral)
	}
	b, err := json.Marshal(struct{ T tokens.QueryToken }{tok})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if strings.Contains(string(b), "eyJ-secret-jwt") {
		t.Errorf("json.Marshal leaked the raw token: %s", b)
	}
	if got := tok.Reveal(); got != "eyJ-secret-jwt" {
		t.Errorf("Reveal(): got=%q want=eyJ-secret-jwt", got)
	}
}

// TestImpersonationToken_RedactsInJSON pins the MarshalJSON guard added
// alongside QueryToken's (encoding/json bypasses Stringer).
func TestImpersonationToken_RedactsInJSON(t *testing.T) {
	tok := tokens.ImpersonationToken("eyJ-secret-jwt")
	b, err := json.Marshal(struct{ T tokens.ImpersonationToken }{tok})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if strings.Contains(string(b), "eyJ-secret-jwt") {
		t.Errorf("json.Marshal leaked the raw token: %s", b)
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
