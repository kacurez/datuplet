package http_test

import (
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// authedQueryTokenRequest builds a POST /api/v1/query/token request whose
// context carries an authenticated user, exactly the way auth.WithUser does
// at runtime. MintQueryToken reads the subject from this ctx, so without it
// the mint would refuse.
func authedQueryTokenRequest(sub uuid.UUID) *stdhttp.Request {
	r := httptest.NewRequest(stdhttp.MethodPost, "/api/v1/query/token", strings.NewReader(`{}`))
	u := &store.User{ID: sub, Email: "test@example.com"}
	return r.WithContext(auth.WithCtxUser(r.Context(), u))
}

// --- policy OFF (the default) → 403 with the EXACT refusal envelope ---

func TestQueryTokenHandler_PolicyOff_Forbidden(t *testing.T) {
	signer := mustNewSigner(t)
	h := apihttp.NewLocalQueryMintHandler(signer, false) // policy OFF

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedQueryTokenRequest(uuid.New()))

	if rec.Code != stdhttp.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Error string `json:"error"`
		Kind  string `json:"kind"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rec.Body.String())
	}
	// The client (datuplet-query/mint.go) surfaces `error` verbatim and
	// special-cases 403 on `kind`. Both must match EXACTLY.
	const wantErr = "client-side query disabled; use the server query service"
	if body.Error != wantErr {
		t.Errorf("error = %q, want %q", body.Error, wantErr)
	}
	if body.Kind != "forbidden" {
		t.Errorf("kind = %q, want %q", body.Kind, "forbidden")
	}
}

// --- policy ON → 200 with a fresh query JWT ---

func TestQueryTokenHandler_PolicyOn_MintsQueryToken(t *testing.T) {
	signer := mustNewSigner(t)
	h := apihttp.NewLocalQueryMintHandler(signer, true) // policy ON

	sub := uuid.New()
	before := time.Now()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedQueryTokenRequest(sub))

	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rec.Body.String())
	}
	if body.Token == "" {
		t.Fatal("token is empty")
	}
	if body.ExpiresAt == "" {
		t.Fatal("expires_at is empty")
	}

	// Parse + verify the token against the signer's public key — assert the
	// query-token claim shape the laptop-side engine relies on.
	parsed, err := jwt.Parse(body.Token, func(*jwt.Token) (any, error) {
		return signer.Public(), nil
	})
	if err != nil {
		t.Fatalf("parse/verify JWT: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)
	// Use GetAudience() rather than a bare string cast: jwt/v5 may surface `aud`
	// as a single string or a []string depending on encoding, and GetAudience()
	// normalizes both to jwt.ClaimStrings.
	aud, _ := claims.GetAudience()
	if len(aud) != 1 || aud[0] != tokens.TableTokenAudience {
		t.Errorf("aud = %v, want [%q]", aud, tokens.TableTokenAudience)
	}
	if got, _ := claims["token_kind"].(string); got != tokens.TokenKindQuery {
		t.Errorf("token_kind = %q, want %q", got, tokens.TokenKindQuery)
	}
	if got, _ := claims["sub"].(string); got != sub.String() {
		t.Errorf("sub = %q, want %q", got, sub.String())
	}
	// exp present + within the MaxQueryTokenLifetime ceiling.
	expF, ok := claims["exp"].(float64)
	if !ok {
		t.Fatalf("exp claim missing or not numeric: %v", claims["exp"])
	}
	exp := time.Unix(int64(expF), 0)
	maxExp := before.Add(tokens.MaxQueryTokenLifetime + 2*time.Second) // +2s scheduling slack
	if exp.After(maxExp) {
		t.Errorf("exp = %v, want ≤ %v (now+%s)", exp, maxExp, tokens.MaxQueryTokenLifetime)
	}
	if !exp.After(before) {
		t.Errorf("exp = %v, want after %v", exp, before)
	}

	// The response expires_at must never claim the token outlives its real JWT
	// exp — a client may pre-flight on it, and reporting a later expiry would
	// let it use a token lakekeeper has already rejected. The handler anchors
	// expires_at to a mintedAt captured just before the mint, so it must be ≤
	// exp (allow small slack for RFC3339 second-truncation).
	respExp, perr := time.Parse(time.RFC3339, body.ExpiresAt)
	if perr != nil {
		t.Fatalf("parse expires_at %q: %v", body.ExpiresAt, perr)
	}
	if respExp.After(exp.Add(2 * time.Second)) {
		t.Errorf("expires_at %v claims later than JWT exp %v", respExp, exp)
	}
}

// --- no authenticated user in ctx → 401 (defense-in-depth) ---

func TestQueryTokenHandler_NoUser_Unauthorized(t *testing.T) {
	signer := mustNewSigner(t)
	h := apihttp.NewLocalQueryMintHandler(signer, true) // policy ON, but no user

	// Request WITHOUT a ctx user (auth.WithUser would normally reject before
	// reaching the handler; the handler still defends).
	r := httptest.NewRequest(stdhttp.MethodPost, "/api/v1/query/token", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	if rec.Code != stdhttp.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}
}
