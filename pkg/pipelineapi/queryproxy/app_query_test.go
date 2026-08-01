package queryproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate/projectgatetest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// newAppCore builds a *Core pointed at the given worker URL, mirroring
// newHandler in handler_test.go but returning the Core (so tests can reach
// AppHTTPHandler()).
func newAppCore(t *testing.T, workerURL string, signer *tokens.Signer) *Core {
	t.Helper()
	core, err := NewCore(Config{WorkerURL: workerURL, Gate: projectgatetest.AllowAll("lk-proj", "wh")}, signer)
	if err != nil {
		t.Fatalf("NewCore: %v", err)
	}
	return core
}

// appQueryRequest builds a POST .../query request carrying tok as the
// Authorization bearer credential — the shape app-worker presents to
// AppHTTPHandler (in place of a platform session).
func appQueryRequest(pid, tok, jsonBody string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/internal/v1/projects/"+pid+"/query", strings.NewReader(jsonBody))
	r.SetPathValue("pid", pid)
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	return r
}

// echoWorker returns an httptest.Server that decodes the workerRequest body,
// stores it in *got, and replies with a fixed passthrough Result.
func echoWorker(t *testing.T, got *workerRequest) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(got)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`))
	}))
}

func TestAppQuery_ValidAppTokenPassesGate(t *testing.T) {
	signer := testSigner(t)
	appID := uuid.NewString()
	tok, _, err := tokens.MintAppToken(signer, appID, "lk-proj")
	if err != nil {
		t.Fatalf("MintAppToken: %v", err)
	}

	var got workerRequest
	worker := echoWorker(t, &got)
	defer worker.Close()

	core := newAppCore(t, worker.URL, signer)
	rec := httptest.NewRecorder()
	core.AppHTTPHandler().ServeHTTP(rec, appQueryRequest(uuid.NewString(), tok.Reveal(), `{"sql":"SELECT 1"}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}

func TestAppQuery_ForwardsPresentedTokenAsCatalogCredential(t *testing.T) {
	// No re-mint: the catalog JWT the worker receives must be byte-identical
	// to the app JWT app-worker presented (RFC 028 P5's core requirement —
	// there is no ctx-bound *store.User subject to mint a fresh one from).
	signer := testSigner(t)
	appID := uuid.NewString()
	tok, _, err := tokens.MintAppToken(signer, appID, "lk-proj")
	if err != nil {
		t.Fatalf("MintAppToken: %v", err)
	}
	presented := tok.Reveal()

	var got workerRequest
	worker := echoWorker(t, &got)
	defer worker.Close()

	core := newAppCore(t, worker.URL, signer)
	rec := httptest.NewRecorder()
	core.AppHTTPHandler().ServeHTTP(rec, appQueryRequest(uuid.NewString(), presented, `{"sql":"SELECT 1"}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got.CatalogJWT != presented {
		t.Fatalf("worker catalog_jwt = %q, want the exact presented app token %q (no re-mint)", got.CatalogJWT, presented)
	}
}

func TestAppQuery_AuditRecordsAppSubjectAndJTI(t *testing.T) {
	signer := testSigner(t)
	appID := uuid.NewString()
	tok, jti, err := tokens.MintAppToken(signer, appID, "lk-proj")
	if err != nil {
		t.Fatalf("MintAppToken: %v", err)
	}
	wantSub := "app-" + appID

	var got workerRequest
	worker := echoWorker(t, &got)
	defer worker.Close()

	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	defer slog.SetDefault(prev)

	core := newAppCore(t, worker.URL, signer)
	rec := httptest.NewRecorder()
	core.AppHTTPHandler().ServeHTTP(rec, appQueryRequest(uuid.NewString(), tok.Reveal(), `{"sql":"SELECT 1"}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	line := findAuditLine(t, logBuf.String())
	if line["principal"] != wantSub {
		t.Fatalf("audit principal = %v, want %q", line["principal"], wantSub)
	}
	if line["jti"] != jti {
		t.Fatalf("audit jti = %v, want %q (the app token's own jti, not a re-minted one)", line["jti"], jti)
	}
	if line["outcome"] != "ok" {
		t.Fatalf("audit outcome = %v, want ok", line["outcome"])
	}
}

// findAuditLine locates the single "query_audit" JSON log line and decodes
// it into a generic map for field assertions.
func findAuditLine(t *testing.T, logs string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue
		}
		if m["msg"] == "query_audit" {
			return m
		}
	}
	t.Fatalf("no query_audit line found in captured logs: %s", logs)
	return nil
}

// otherKindToken mints a token of a kind OTHER than "app" at the same
// aud=datuplet-catalog audience, for the regression-guard test below. Not
// every kind can be minted without a ctx-bound *store.User (MintImpersonation
// derives sub from it) — userCtx supplies one where needed, exactly like
// authedRequest does for the browser-facing tests in this package.
func userCtx(t *testing.T) context.Context {
	t.Helper()
	return auth.WithCtxUser(context.Background(), &store.User{ID: uuid.New(), Email: "t@e.c"})
}

func TestAppQuery_RejectsNonAppTokenKinds(t *testing.T) {
	// Regression guard (task-P0-report.md §A6): the app route must accept
	// ONLY token_kind=app. Every other kind minted at aud=datuplet-catalog
	// today must still be refused — in particular token_kind=impersonation,
	// which predates RFC 028 and is easy to confuse with the new app kind
	// (task-P4-report.md's central warning).
	signer := testSigner(t)
	ctx := userCtx(t)

	imp, err := tokens.MintImpersonation(ctx, signer)
	if err != nil {
		t.Fatalf("MintImpersonation: %v", err)
	}
	qry, err := tokens.MintQueryToken(ctx, signer, 30_000_000_000)
	if err != nil {
		t.Fatalf("MintQueryToken: %v", err)
	}
	localCLI, _, err := tokens.MintLocalCLIToken(ctx, signer, 30_000_000_000)
	if err != nil {
		t.Fatalf("MintLocalCLIToken: %v", err)
	}

	cases := map[string]string{
		"impersonation": imp.Reveal(),
		"query":         qry.Reveal(),
		"local-cli":     localCLI,
	}

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker must not be called when the presented token is not an app token")
	}))
	defer worker.Close()
	core := newAppCore(t, worker.URL, signer)

	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			core.AppHTTPHandler().ServeHTTP(rec, appQueryRequest(uuid.NewString(), tok, `{"sql":"SELECT 1"}`))
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("token_kind=%s: status = %d, want 401", name, rec.Code)
			}
		})
	}
}

// signCustomClaims mints a JWT with an arbitrary claim set, signed by
// signer. Unlike every tokens.Mint* function, this lets a test decouple
// `sub` from `token_kind` — the combination the real minters never produce
// — to isolate exactly which check in parseAppBearer is doing the work.
func signCustomClaims(t *testing.T, signer *tokens.Signer, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID
	s, err := tok.SignedString(signer.Private())
	if err != nil {
		t.Fatalf("sign custom claims: %v", err)
	}
	return s
}

func TestAppQuery_RejectsAppSubjectWithWrongTokenKind(t *testing.T) {
	// Isolates the token_kind check from the sub-prefix check: a forged (or
	// buggy-minter) token carrying an app-shaped `sub` but a NON-app
	// token_kind must still be refused. Without this, a test that only
	// tries real tokens.Mint* outputs (which always pair sub and kind
	// correctly) cannot tell whether token_kind is doing any independent
	// work, since those tokens' sub never has the "app-" prefix either.
	signer := testSigner(t)
	appID := uuid.NewString()
	now := time.Now()
	forged := signCustomClaims(t, signer, jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        tokens.TableTokenAudience,
		"sub":        tokens.AppSubjectPrefix + appID,
		"actor":      tokens.AppSubjectPrefix + appID,
		"token_kind": tokens.TokenKindRun, // NOT "app"
		"project_id": "lk-proj",
		"app_id":     appID,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(60 * time.Second).Unix(),
		"jti":        "forged-jti",
	})

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker must not be called for a non-app token_kind, even with an app-shaped sub")
	}))
	defer worker.Close()
	core := newAppCore(t, worker.URL, signer)

	rec := httptest.NewRecorder()
	core.AppHTTPHandler().ServeHTTP(rec, appQueryRequest(uuid.NewString(), forged, `{"sql":"SELECT 1"}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestAppQuery_RejectsMissingAuthorizationHeader(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker must not be called for an unauthenticated request")
	}))
	defer worker.Close()
	core := newAppCore(t, worker.URL, testSigner(t))

	rec := httptest.NewRecorder()
	core.AppHTTPHandler().ServeHTTP(rec, appQueryRequest(uuid.NewString(), "", `{"sql":"SELECT 1"}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestAppQuery_RejectsBadSignature(t *testing.T) {
	// A token signed by a DIFFERENT key must never authenticate — this is
	// the forgery case the whole verification exists to stop.
	signer := testSigner(t)
	other := testSigner(t)
	appID := uuid.NewString()
	tok, _, err := tokens.MintAppToken(other, appID, "lk-proj")
	if err != nil {
		t.Fatalf("MintAppToken: %v", err)
	}

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker must not be called for a bad-signature token")
	}))
	defer worker.Close()
	core := newAppCore(t, worker.URL, signer)

	rec := httptest.NewRecorder()
	core.AppHTTPHandler().ServeHTTP(rec, appQueryRequest(uuid.NewString(), tok.Reveal(), `{"sql":"SELECT 1"}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestAppQuery_GateStillEnforced(t *testing.T) {
	// The security invariant: an app token must go through the SAME FGA
	// check the browser route runs (projectgate.Gate), not around it. A
	// valid, well-formed app token must still be refused when the gate
	// denies — proving the app path is not a bypass.
	signer := testSigner(t)
	appID := uuid.NewString()
	tok, _, err := tokens.MintAppToken(signer, appID, "lk-proj")
	if err != nil {
		t.Fatalf("MintAppToken: %v", err)
	}

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("worker must not be called when the gate denies")
	}))
	defer worker.Close()

	g := projectgatetest.AllowAll("lk-proj", "wh")
	g.Authorizer = projectgatetest.FakeAuthorizer{Allow: false}
	core, err := NewCore(Config{WorkerURL: worker.URL, Gate: g}, signer)
	if err != nil {
		t.Fatalf("NewCore: %v", err)
	}

	rec := httptest.NewRecorder()
	core.AppHTTPHandler().ServeHTTP(rec, appQueryRequest(uuid.NewString(), tok.Reveal(), `{"sql":"SELECT 1"}`))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
}
