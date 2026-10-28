package tokens_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// TestMintCLITokens_ClaimsShape pins the wire shape of the CLI JWTs: the
// local-cli token minted by `POST /api/v1/auth/token` (consumed by lakekeeper
// via OIDC + JWKS exactly like impersonation/run tokens, so any drift in claim
// names makes lakekeeper soft-deny at the FGA layer) and the cli-api token
// used for pipeline-api bearer auth (drift makes BearerJWTResolver reject CLI
// requests).
func TestMintCLITokens_ClaimsShape(t *testing.T) {
	// The api token must use datuplet-api, never datuplet-catalog, so it
	// cannot be replayed to lakekeeper. ClaimsShape below compares against
	// the constants, so pin the constants themselves apart here.
	if tokens.APITokenAudience == tokens.TableTokenAudience {
		t.Fatalf("APITokenAudience must differ from TableTokenAudience (%q)", tokens.TableTokenAudience)
	}

	cases := []struct {
		name      string
		user      uuid.UUID
		mint      func(ctx context.Context, s *tokens.Signer, d time.Duration) (string, time.Time, error)
		wantAud   string
		notAud    string
		extra     map[string]string
		jtiPrefix string
	}{
		{
			name:    "local_cli",
			user:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			mint:    tokens.MintLocalCLIToken,
			wantAud: tokens.TableTokenAudience,
			extra: map[string]string{
				"token_kind": tokens.TokenKindLocalCLI,
				"token_use":  tokens.TokenKindLocalCLI,
			},
			jtiPrefix: "cli-tok-11111111-1111-1111-1111-111111111111-",
		},
		{
			name:      "cli_api",
			user:      uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			mint:      tokens.MintCLIAPIToken,
			wantAud:   tokens.APITokenAudience,
			notAud:    tokens.TableTokenAudience,
			extra:     map[string]string{"token_kind": tokens.TokenKindCLIAPI},
			jtiPrefix: "cli-api-tok-33333333-3333-3333-3333-333333333333-",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tok, expTime, err := tc.mint(userCtx(t, tc.user), sharedSigner(t), time.Hour)
			if err != nil {
				t.Fatalf("mint: %v", err)
			}
			parsed, _, err := jwt.NewParser().ParseUnverified(tok, jwt.MapClaims{})
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			claims := parsed.Claims.(jwt.MapClaims)

			checks := map[string]string{
				"iss":   "datuplet-api",
				"aud":   tc.wantAud,
				"sub":   tc.user.String(),
				"actor": tc.user.String(),
			}
			for k, v := range tc.extra {
				checks[k] = v
			}
			for k, want := range checks {
				if got, _ := claims[k].(string); got != want {
					t.Errorf("claim %q = %v, want %v", k, claims[k], want)
				}
			}
			if aud, _ := claims["aud"].(string); tc.notAud != "" && aud == tc.notAud {
				t.Errorf("aud must NOT be %q", tc.notAud)
			}

			// jti shape: <prefix><uuid>-<unix-nano>
			jti, _ := claims["jti"].(string)
			if jti == "" {
				t.Errorf("jti must be set, got empty")
			}
			if len(jti) < len(tc.jtiPrefix) || jti[:len(tc.jtiPrefix)] != tc.jtiPrefix {
				t.Errorf("jti = %q, want prefix %q", jti, tc.jtiPrefix)
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

			// expTime returned must match JWT exp exactly (same Unix
			// second) — no drift from a second time.Now() call.
			if int64(exp) != expTime.Unix() {
				t.Errorf("returned expTime.Unix() = %d, JWT exp = %d (must match exactly)",
					expTime.Unix(), int64(exp))
			}
		})
	}
}
