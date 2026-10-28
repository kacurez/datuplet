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
	"sync"
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

// --- MintQueryToken / MintInternalQueryToken (RFC 022 Task 2.1) ---

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

// --- MintAppToken (RFC 028 §5.4) ---

// TestMintAppToken_ClaimsShape pins the app-token claim set. The `sub` is the
// BARE "app-<uuid>" form — no "oidc~"/"user:" prefix — for exactly the reason
// MintRunToken's raw-UUID sub exists: lakekeeper composes `user:oidc~<sub>`
// itself, so pre-composing here would yield `user:oidc~user:oidc~app-<uuid>`
// on the FGA side and never match the `viewer` tuple pipeline-api writes.
func TestMintAppToken_ClaimsShape(t *testing.T) {
	signer := sharedSigner(t)
	appUUID := "3f9c1b7a-1111-4a2b-9c3d-abcdefabcdef"
	tok, jti, err := tokens.MintAppToken(signer, appUUID, "lk-project-7")
	if err != nil {
		t.Fatalf("MintAppToken: %v", err)
	}
	parsed, _, err := jwt.NewParser().ParseUnverified(tok.Reveal(), jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)

	wantSub := tokens.AppSubjectPrefix + appUUID
	if got, _ := claims["sub"].(string); got != wantSub {
		t.Errorf("sub = %q, want %q", got, wantSub)
	}
	if got, _ := claims["actor"].(string); got != wantSub {
		t.Errorf("actor = %q, want %q (the app acts as itself)", got, wantSub)
	}
	if got, _ := claims["iss"].(string); got != "datuplet-api" {
		t.Errorf("iss = %q, want datuplet-api", got)
	}
	if got, _ := claims["aud"].(string); got != tokens.TableTokenAudience {
		t.Errorf("aud = %q, want %q", got, tokens.TableTokenAudience)
	}
	if got, _ := claims["token_kind"].(string); got != tokens.TokenKindApp {
		t.Errorf("token_kind = %q, want %q", got, tokens.TokenKindApp)
	}
	if got, _ := claims["project_id"].(string); got != "lk-project-7" {
		t.Errorf("project_id = %q, want lk-project-7", got)
	}
	if got, _ := claims["app_id"].(string); got != appUUID {
		t.Errorf("app_id = %q, want %q", got, appUUID)
	}
	if got, _ := claims["jti"].(string); got != jti || jti == "" {
		t.Errorf("jti claim = %q, returned jti = %q; want equal and non-empty", got, jti)
	}
	// 60 s TTL (spec §5.4) — asserted off the token's own iat/exp so clock
	// skew cannot flake it, plus the constant itself.
	iat, _ := claims["iat"].(float64)
	exp, _ := claims["exp"].(float64)
	if int64(exp-iat) != int64(tokens.AppTokenLifetime.Seconds()) {
		t.Errorf("exp-iat = %ds, want %ds", int64(exp-iat), int64(tokens.AppTokenLifetime.Seconds()))
	}
	if tokens.AppTokenLifetime != 60*time.Second {
		t.Errorf("AppTokenLifetime = %v, want 60s (spec §5.4)", tokens.AppTokenLifetime)
	}
	// The bare sub must carry neither FGA prefix.
	if strings.Contains(wantSub, "oidc~") || strings.Contains(wantSub, "user:") {
		t.Errorf("sub %q must not carry the FGA prefixes", wantSub)
	}
}

// TestMintAppToken_ConcurrentJTIsAreUnique is the collision-proof assertion.
// app-worker mints ONE token per render with a per-app in-flight limit of 2,
// so same-app concurrent mints are the normal operating case — and every claim
// except the jti is identical for them. If the jti's uniqueness came from the
// clock, two mints inside one tick would produce the same jti and a
// byte-identical signed JWT, and query_audit could no longer tell two
// concurrent renders apart (the contract has it carry the app subject AND the
// jti for exactly that reason). time.Now()'s resolution is platform-dependent,
// so "nanoseconds are fine on my laptop" is not a guarantee.
func TestMintAppToken_ConcurrentJTIsAreUnique(t *testing.T) {
	signer := sharedSigner(t)
	const appUUID = "3f9c1b7a-1111-4a2b-9c3d-abcdefabcdef"
	const n = 64

	jtis := make([]string, n)
	toks := make([]string, n)
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			tok, jti, err := tokens.MintAppToken(signer, appUUID, "lk-1")
			jtis[i], toks[i], errs[i] = jti, tok.Reveal(), err
		}(i)
	}
	close(start)
	wg.Wait()

	seenJTI := make(map[string]int, n)
	seenTok := make(map[string]int, n)
	for i := range jtis {
		if errs[i] != nil {
			t.Fatalf("mint %d: %v", i, errs[i])
		}
		if prev, dup := seenJTI[jtis[i]]; dup {
			t.Fatalf("duplicate jti across concurrent mints %d and %d: %q", prev, i, jtis[i])
		}
		seenJTI[jtis[i]] = i
		if prev, dup := seenTok[toks[i]]; dup {
			t.Fatalf("byte-identical token across concurrent mints %d and %d", prev, i)
		}
		seenTok[toks[i]] = i
	}
	if len(seenJTI) != n {
		t.Fatalf("got %d distinct jtis, want %d", len(seenJTI), n)
	}
}

func TestMintAppToken_RequiresFields(t *testing.T) {
	signer := sharedSigner(t)
	if _, _, err := tokens.MintAppToken(nil, "app-uuid", "lk-1"); err == nil {
		t.Error("expected error for nil signer")
	}
	if _, _, err := tokens.MintAppToken(signer, "", "lk-1"); err == nil {
		t.Error("expected error for empty appUUID")
	}
	if _, _, err := tokens.MintAppToken(signer, "app-uuid", ""); err == nil {
		t.Error("expected error for empty projectID (fail closed)")
	}
}

// --- Minter input rejection ---

// TestMinters_RejectInvalidInput: every minter must fail closed. The actor
// comes from ctx, never from the caller (audit-forgery argument), so an
// unauthenticated context must error; tokens without a sane exp are rejected
// by the JWT verifier, so zero/negative lifetimes error at the source; and a
// nil signer is a programmer error that must fail loudly at mint time.
func TestMinters_RejectInvalidInput(t *testing.T) {
	type mintFn func(ctx context.Context, s *tokens.Signer) error
	run := func(spec tokens.RunSpec) mintFn {
		return func(ctx context.Context, s *tokens.Signer) error {
			_, err := tokens.MintRunToken(ctx, s, spec)
			return err
		}
	}
	impersonation := func(ctx context.Context, s *tokens.Signer) error {
		_, err := tokens.MintImpersonation(ctx, s)
		return err
	}
	query := func(d time.Duration) mintFn {
		return func(ctx context.Context, s *tokens.Signer) error {
			_, err := tokens.MintQueryToken(ctx, s, d)
			return err
		}
	}
	internalQuery := func(d time.Duration) mintFn {
		return func(ctx context.Context, s *tokens.Signer) error {
			_, err := tokens.MintInternalQueryToken(ctx, s, d)
			return err
		}
	}
	localCLI := func(d time.Duration) mintFn {
		return func(ctx context.Context, s *tokens.Signer) error {
			_, _, err := tokens.MintLocalCLIToken(ctx, s, d)
			return err
		}
	}
	cliAPI := func(d time.Duration) mintFn {
		return func(ctx context.Context, s *tokens.Signer) error {
			_, _, err := tokens.MintCLIAPIToken(ctx, s, d)
			return err
		}
	}
	validRun := tokens.RunSpec{RunID: "r", Lifetime: time.Hour}

	cases := []struct {
		name      string
		anon      bool
		nilSigner bool
		mint      mintFn
	}{
		{"run_anonymous", true, false, run(validRun)},
		{"run_missing_run_id", false, false, run(tokens.RunSpec{Lifetime: time.Hour})},
		{"run_zero_lifetime", false, false, run(tokens.RunSpec{RunID: "r"})},
		{"impersonation_anonymous", true, false, impersonation},
		{"query_anonymous", true, false, query(60 * time.Second)},
		{"query_zero_ttl", false, false, query(0)},
		{"query_negative_ttl", false, false, query(-time.Second)},
		{"internal_query_anonymous", true, false, internalQuery(60 * time.Second)},
		{"internal_query_zero_ttl", false, false, internalQuery(0)},
		{"internal_query_negative_ttl", false, false, internalQuery(-time.Second)},
		{"local_cli_anonymous", true, false, localCLI(time.Hour)},
		{"local_cli_zero_lifetime", false, false, localCLI(0)},
		{"local_cli_negative_second", false, false, localCLI(-time.Second)},
		{"local_cli_negative_hour", false, false, localCLI(-time.Hour)},
		{"local_cli_nil_signer", false, true, localCLI(time.Hour)},
		{"cli_api_anonymous", true, false, cliAPI(time.Hour)},
		{"cli_api_zero_lifetime", false, false, cliAPI(0)},
		{"cli_api_negative_second", false, false, cliAPI(-time.Second)},
		{"cli_api_negative_hour", false, false, cliAPI(-time.Hour)},
		{"cli_api_nil_signer", false, true, cliAPI(time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := userCtx(t, uuid.New())
			if tc.anon {
				ctx = context.Background()
			}
			signer := sharedSigner(t)
			if tc.nilSigner {
				signer = nil
			}
			if err := tc.mint(ctx, signer); err == nil {
				t.Error("expected error")
			}
		})
	}
}

// --- MintQueryToken / MintInternalQueryToken claim shape ---

// TestMintQueryTokens_ClaimsShape: the two query minters differ only in aud
// and token_kind, so they share one table (RFC 022 Task 2.1).
func TestMintQueryTokens_ClaimsShape(t *testing.T) {
	cases := []struct {
		name     string
		caller   uuid.UUID
		mint     func(ctx context.Context, s *tokens.Signer, ttl time.Duration) (string, error)
		wantAud  string
		wantKind string
	}{
		{
			"query", uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			func(ctx context.Context, s *tokens.Signer, ttl time.Duration) (string, error) {
				tok, err := tokens.MintQueryToken(ctx, s, ttl)
				return tok.Reveal(), err
			},
			tokens.TableTokenAudience, tokens.TokenKindQuery,
		},
		{
			"internal_query", uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			func(ctx context.Context, s *tokens.Signer, ttl time.Duration) (string, error) {
				tok, err := tokens.MintInternalQueryToken(ctx, s, ttl)
				return tok.Reveal(), err
			},
			tokens.QueryWorkerAudience, tokens.TokenKindInternalQuery,
		},
	}
	parser := jwt.NewParser()
	claimsOf := func(t *testing.T, tok string) jwt.MapClaims {
		t.Helper()
		parsed, _, err := parser.ParseUnverified(tok, jwt.MapClaims{})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		return parsed.Claims.(jwt.MapClaims)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			signer := sharedSigner(t)
			ctx := userCtx(t, tc.caller)

			before := time.Now()
			tok, err := tc.mint(ctx, signer, 120*time.Second)
			if err != nil {
				t.Fatalf("mint: %v", err)
			}
			after := time.Now()
			claims := claimsOf(t, tok)

			// sub = ctx user (raw UUID, same derivation as MintImpersonation).
			want := tc.caller.String()
			if got, _ := claims["sub"].(string); got != want {
				t.Errorf("sub = %q, want %q", got, want)
			}
			if got, _ := claims["actor"].(string); got != want {
				t.Errorf("actor = %q, want %q", got, want)
			}
			if got, _ := claims["iss"].(string); got != "datuplet-api" {
				t.Errorf("iss = %q, want datuplet-api", got)
			}
			if got, _ := claims["aud"].(string); got != tc.wantAud {
				t.Errorf("aud = %q, want %q", got, tc.wantAud)
			}
			if got, _ := claims["token_kind"].(string); got != tc.wantKind {
				t.Errorf("token_kind = %q, want %q", got, tc.wantKind)
			}

			// exp = now+ttl (ttl < clamp, so honoured exactly within
			// wall-clock skew).
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
			tok2, err := tc.mint(ctx, signer, 120*time.Second)
			if err != nil {
				t.Fatalf("mint #2: %v", err)
			}
			if jti2, _ := claimsOf(t, tok2)["jti"].(string); jti1 == jti2 {
				t.Errorf("jti not unique across mints: %q == %q", jti1, jti2)
			}

			// Requesting a TTL > 330s clamps exp to now+330s.
			before = time.Now()
			clamped, err := tc.mint(ctx, signer, 10*time.Minute)
			if err != nil {
				t.Fatalf("mint clamped: %v", err)
			}
			after = time.Now()
			cexp, _ := claimsOf(t, clamped)["exp"].(float64)
			cMin := before.Add(330 * time.Second).Unix()
			cMax := after.Add(330 * time.Second).Unix()
			if int64(cexp) < cMin || int64(cexp) > cMax {
				t.Errorf("clamped exp = %d, want in [%d,%d] (now+330s)", int64(cexp), cMin, cMax)
			}
		})
	}
}

// --- Token redaction ---

// TestTokenTypes_Redaction: %s / %v / %#v and json.Marshal must never leak
// the raw JWT — the audit point. A stray %v in an error chain must NOT surface
// the grant to logs. MintAppToken returns ImpersonationToken, so its
// redaction is covered by that row. encoding/json bypasses Stringer, hence
// the MarshalJSON guard is asserted separately.
func TestTokenTypes_Redaction(t *testing.T) {
	const raw = "eyJ-secret-jwt"
	cases := []struct {
		name string
		tok  interface {
			String() string
			GoString() string
			Reveal() string
		}
		literal string
	}{
		{"impersonation", tokens.ImpersonationToken(raw), "[redacted impersonation token]"},
		{"query", tokens.QueryToken(raw), "[redacted query token]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.tok.String(); got != tc.literal {
				t.Errorf("%%s: got=%q want=%q", got, tc.literal)
			}
			if got := tc.tok.GoString(); got != tc.literal {
				t.Errorf("%%#v: got=%q want=%q", got, tc.literal)
			}
			b, err := json.Marshal(struct{ T any }{tc.tok})
			if err != nil {
				t.Fatalf("json.Marshal: %v", err)
			}
			if strings.Contains(string(b), raw) {
				t.Errorf("json.Marshal leaked the raw token: %s", b)
			}
			if got := tc.tok.Reveal(); got != raw {
				t.Errorf("Reveal(): got=%q want=%s", got, raw)
			}
		})
	}
}
