package apps_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/apps"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// ---------------------------------------------------------------------------
// Fakes specific to the internal API
// ---------------------------------------------------------------------------

// recordingResolver is an auth.UserResolver that records the credential
// headers it observed on the request it was given. sessions/verify's whole
// job is forwarding the caller's Cookie/Authorization headers into the
// existing resolver chain, so "did the resolver actually see them" is the
// assertion that matters.
type recordingResolver struct {
	user       *store.User
	authed     bool
	gotCookie  string
	gotBearer  string
	calls      int
	setsCookie bool
}

func (f *recordingResolver) UserFor(w http.ResponseWriter, r *http.Request) (*store.User, bool, error) {
	f.calls++
	f.gotCookie = r.Header.Get("Cookie")
	f.gotBearer = r.Header.Get("Authorization")
	if f.setsCookie {
		// PostgresResolver refreshes the session cookie on the writer it is
		// handed. sessions/verify must not let that ride out on the internal
		// response — this models it.
		http.SetCookie(w, &http.Cookie{Name: "pipeline_api_session", Value: "refreshed"})
	}
	if !f.authed {
		return nil, false, nil
	}
	return f.user, true, nil
}
func (f *recordingResolver) Mode() string        { return "test" }
func (f *recordingResolver) SupportsLogin() bool { return false }

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const testServiceToken = "s3rvice-cred-abcdefghijklmnop"

type internalHarness struct {
	t         *testing.T
	pool      *pgxpool.Pool
	store     *apps.Store
	ident     *apps.RecorderIdentity
	authz     *fakeAuthz
	projects  *fakeProjects
	resolver  *recordingResolver
	mux       http.Handler
	projectID uuid.UUID
}

func newInternalHarness(t *testing.T) *internalHarness {
	t.Helper()
	pool, cleanup := testStore(t)
	t.Cleanup(cleanup)

	h := &internalHarness{
		t:        t,
		pool:     pool,
		store:    apps.NewStore(pool),
		ident:    &apps.RecorderIdentity{},
		authz:    &fakeAuthz{allow: true},
		projects: &fakeProjects{lakekeeperID: "lk-project-1"},
		resolver: &recordingResolver{user: &store.User{ID: uuid.New(), Email: "viewer@b.c"}, authed: true},
	}
	h.projectID = testProject(t, pool, "proj-apps-internal")

	ih := &apps.InternalHandlers{
		Store:    h.store,
		Identity: h.ident,
		Authz:    h.authz,
		Projects: h.projects,
		Resolver: h.resolver,
		Token:    mustServiceToken(t, testServiceToken),
	}
	mux := http.NewServeMux()
	ih.RegisterInternal(mux)
	h.mux = mux
	return h
}

func mustServiceToken(t *testing.T, secret string) *apps.ServiceToken {
	t.Helper()
	tok, err := apps.NewServiceToken(secret)
	if err != nil {
		t.Fatalf("NewServiceToken: %v", err)
	}
	return tok
}

// do issues an authenticated (service-credential) internal request.
func (h *internalHarness) do(method, path, body string) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.doWithHeaders(method, path, body, map[string]string{
		"Authorization": "Bearer " + testServiceToken,
	})
}

func (h *internalHarness) doWithHeaders(method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	h.t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
		r.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.mux.ServeHTTP(w, r)
	return w
}

// seedApp creates an app with one draft version and returns (appID, hash).
func (h *internalHarness) seedApp(name string, bundle []byte) (string, string) {
	h.t.Helper()
	ctx := context.Background()
	app, err := h.store.Create(ctx, h.projectID, name)
	if err != nil {
		h.t.Fatalf("Create: %v", err)
	}
	v, err := h.store.PutVersion(ctx, app.ID, bundle)
	if err != nil {
		h.t.Fatalf("PutVersion: %v", err)
	}
	return app.ID, v.Hash
}

// assertEnvelope pins the internal error envelope: EXACTLY the two keys
// {error, kind}, both non-empty, and the expected kind.
func assertEnvelope(t *testing.T, w *httptest.ResponseRecorder, wantStatus int, wantKind string) {
	t.Helper()
	if w.Code != wantStatus {
		t.Fatalf("status = %d, want %d (body %s)", w.Code, wantStatus, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode envelope: %v (body %s)", err, w.Body.String())
	}
	if len(got) != 2 {
		t.Errorf("envelope keys = %v, want exactly {error, kind}", got)
	}
	if msg, _ := got["error"].(string); msg == "" {
		t.Errorf(`envelope "error" = %v, want a non-empty string`, got["error"])
	}
	if kind, _ := got["kind"].(string); kind != wantKind {
		t.Errorf(`envelope "kind" = %v, want %q`, got["kind"], wantKind)
	}
}

// ---------------------------------------------------------------------------
// Service-credential auth (401 on missing/wrong token, every endpoint)
// ---------------------------------------------------------------------------

// internalRequests is every internal endpoint, in a form usable without any
// seeded state — the auth gate must answer before the handler body runs.
func internalRequests(h *internalHarness) []struct {
	name, method, path, body string
} {
	appID := uuid.New().String()
	return []struct {
		name, method, path, body string
	}{
		{"resolve", http.MethodGet, "/internal/v1/apps/" + h.projectID.String() + "/dash/resolve?channel=draft", ""},
		{"bundles", http.MethodGet, "/internal/v1/bundles/" + strings.Repeat("a", 64), ""},
		{"viewer-tokens/verify", http.MethodPost, "/internal/v1/viewer-tokens/verify",
			fmt.Sprintf(`{"app_id":%q,"token_id":%q,"secret":"x"}`, appID, uuid.New().String())},
		{"viewer-tokens/active", http.MethodPost, "/internal/v1/viewer-tokens/active",
			fmt.Sprintf(`{"app_id":%q,"token_id":%q}`, appID, uuid.New().String())},
		{"sessions/verify", http.MethodPost, "/internal/v1/sessions/verify",
			fmt.Sprintf(`{"pid":%q}`, h.projectID.String())},
		{"impersonate", http.MethodPost, "/internal/v1/impersonate", fmt.Sprintf(`{"app_id":%q}`, appID)},
		{"logs", http.MethodPost, "/internal/v1/apps/" + appID + "/logs", `{"request_id":"` + uuid.New().String() + `"}`},
	}
}

func TestInternal_401WithoutServiceToken(t *testing.T) {
	h := newInternalHarness(t)
	for _, tc := range internalRequests(h) {
		for _, hdr := range []struct {
			name  string
			value map[string]string
		}{
			{"missing", nil},
			{"wrong secret", map[string]string{"Authorization": "Bearer wrong-secret-wrong-secret"}},
			{"right secret wrong length", map[string]string{"Authorization": "Bearer " + testServiceToken + "x"}},
			{"prefix of secret", map[string]string{"Authorization": "Bearer " + testServiceToken[:5]}},
			{"no bearer scheme", map[string]string{"Authorization": testServiceToken}},
			{"basic scheme", map[string]string{"Authorization": "Basic " + testServiceToken}},
			{"empty bearer", map[string]string{"Authorization": "Bearer "}},
		} {
			t.Run(tc.name+"/"+hdr.name, func(t *testing.T) {
				w := h.doWithHeaders(tc.method, tc.path, tc.body, hdr.value)
				assertEnvelope(t, w, http.StatusUnauthorized, "unauthorized")
			})
		}
	}
}

// The resolver must never be consulted for a request that failed the
// service-credential gate — the gate is the outermost layer.
func TestInternal_401DoesNotReachHandler(t *testing.T) {
	h := newInternalHarness(t)
	w := h.doWithHeaders(http.MethodPost, "/internal/v1/sessions/verify",
		fmt.Sprintf(`{"pid":%q}`, h.projectID.String()), nil)
	assertEnvelope(t, w, http.StatusUnauthorized, "unauthorized")
	if h.resolver.calls != 0 {
		t.Fatalf("resolver called %d times on a 401 request, want 0", h.resolver.calls)
	}
}

func TestServiceToken_LoadFromFileAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	// Trailing newline is what a `kubectl create secret --from-literal`
	// round-trip or an editor commonly leaves behind; it must not change the
	// credential.
	if err := os.WriteFile(path, []byte(testServiceToken+"\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	tok, err := apps.LoadServiceToken(path)
	if err != nil {
		t.Fatalf("LoadServiceToken: %v", err)
	}
	if !tok.Matches(testServiceToken) {
		t.Errorf("Matches(secret) = false, want true (trailing newline must be trimmed)")
	}
	if tok.Matches(testServiceToken + "\n") {
		t.Errorf("Matches(secret+newline) = true, want false")
	}
	if s := fmt.Sprintf("%v %s", tok, tok); strings.Contains(s, testServiceToken) {
		t.Errorf("ServiceToken leaked its secret when formatted: %q", s)
	}

	// Env-var indirection: unset -> (nil, nil) so the routes stay unregistered.
	t.Setenv(apps.ServiceTokenFileEnv, "")
	got, err := apps.ServiceTokenFromEnv()
	if err != nil || got != nil {
		t.Fatalf("ServiceTokenFromEnv() with unset env = (%v, %v), want (nil, nil)", got, err)
	}
	t.Setenv(apps.ServiceTokenFileEnv, path)
	got, err = apps.ServiceTokenFromEnv()
	if err != nil {
		t.Fatalf("ServiceTokenFromEnv: %v", err)
	}
	if !got.Matches(testServiceToken) {
		t.Errorf("token loaded from %s does not match", apps.ServiceTokenFileEnv)
	}

	// An empty file is a misconfiguration, not an "any token works" credential.
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("  \n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := apps.LoadServiceToken(empty); err == nil {
		t.Errorf("LoadServiceToken(empty file) = nil error, want an error")
	}
}

// ---------------------------------------------------------------------------
// GET /internal/v1/apps/{pid}/{name}/resolve
// ---------------------------------------------------------------------------

func TestInternalResolve_HappyPath(t *testing.T) {
	h := newInternalHarness(t)
	appID, hash := h.seedApp("dash1", []byte("bundle-one"))

	w := h.do(http.MethodGet, "/internal/v1/apps/"+h.projectID.String()+"/dash1/resolve?channel=draft", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if len(got) != 2 {
		t.Errorf("response keys = %v, want exactly {app_id, version_hash}", got)
	}
	if got["app_id"] != appID {
		t.Errorf("app_id = %v, want %s", got["app_id"], appID)
	}
	if got["version_hash"] != hash {
		t.Errorf("version_hash = %v, want %s", got["version_hash"], hash)
	}

	// production is unset until a promote -> 404, then resolves.
	w = h.do(http.MethodGet, "/internal/v1/apps/"+h.projectID.String()+"/dash1/resolve?channel=production", "")
	assertEnvelope(t, w, http.StatusNotFound, "app_not_found")

	if err := h.store.Promote(context.Background(), appID, hash, ""); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	w = h.do(http.MethodGet, "/internal/v1/apps/"+h.projectID.String()+"/dash1/resolve?channel=production", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	got = nil
	decodeBody(t, w, &got)
	if got["version_hash"] != hash {
		t.Errorf("production version_hash = %v, want %s", got["version_hash"], hash)
	}
}

func TestInternalResolve_DefaultsToProduction(t *testing.T) {
	h := newInternalHarness(t)
	appID, hash := h.seedApp("dash1", []byte("bundle-one"))
	if err := h.store.Promote(context.Background(), appID, hash, ""); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	w := h.do(http.MethodGet, "/internal/v1/apps/"+h.projectID.String()+"/dash1/resolve", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if got["version_hash"] != hash {
		t.Errorf("version_hash = %v, want %s", got["version_hash"], hash)
	}
}

func TestInternalResolve_UnknownApp404(t *testing.T) {
	h := newInternalHarness(t)
	h.seedApp("dash1", []byte("bundle-one"))

	w := h.do(http.MethodGet, "/internal/v1/apps/"+h.projectID.String()+"/nope/resolve?channel=draft", "")
	assertEnvelope(t, w, http.StatusNotFound, "app_not_found")

	// Right name, wrong project.
	w = h.do(http.MethodGet, "/internal/v1/apps/"+uuid.New().String()+"/dash1/resolve?channel=draft", "")
	assertEnvelope(t, w, http.StatusNotFound, "app_not_found")
}

func TestInternalResolve_BadRequest(t *testing.T) {
	h := newInternalHarness(t)
	h.seedApp("dash1", []byte("bundle-one"))

	w := h.do(http.MethodGet, "/internal/v1/apps/not-a-uuid/dash1/resolve?channel=draft", "")
	assertEnvelope(t, w, http.StatusBadRequest, "bad_request")

	w = h.do(http.MethodGet, "/internal/v1/apps/"+h.projectID.String()+"/dash1/resolve?channel=staging", "")
	assertEnvelope(t, w, http.StatusBadRequest, "bad_request")
}

// ---------------------------------------------------------------------------
// GET /internal/v1/bundles/{hash}
// ---------------------------------------------------------------------------

func TestInternalBundle_ServesBytesWithImmutableCacheControl(t *testing.T) {
	h := newInternalHarness(t)
	bundle := []byte("export async function render(ctx) { return {outputDoc:1}; }")
	_, hash := h.seedApp("dash1", bundle)

	w := h.do(http.MethodGet, "/internal/v1/bundles/"+hash, "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !bytes.Equal(w.Body.Bytes(), bundle) {
		t.Errorf("body = %q, want the raw bundle %q", w.Body.String(), bundle)
	}
	if got, want := w.Header().Get("Cache-Control"), "public, max-age=31536000, immutable"; got != want {
		t.Errorf("Cache-Control = %q, want %q", got, want)
	}
	if got, want := w.Header().Get("Content-Type"), "application/octet-stream"; got != want {
		t.Errorf("Content-Type = %q, want %q", got, want)
	}
}

func TestInternalBundle_Unknown404(t *testing.T) {
	h := newInternalHarness(t)
	h.seedApp("dash1", []byte("bundle-one"))

	w := h.do(http.MethodGet, "/internal/v1/bundles/"+strings.Repeat("b", 64), "")
	assertEnvelope(t, w, http.StatusNotFound, "app_not_found")
	if cc := w.Header().Get("Cache-Control"); strings.Contains(cc, "immutable") {
		t.Errorf("Cache-Control = %q on a 404; a miss must never be cached as immutable", cc)
	}

	// Not a content hash at all.
	w = h.do(http.MethodGet, "/internal/v1/bundles/not-a-hash", "")
	assertEnvelope(t, w, http.StatusBadRequest, "bad_request")
}

// ---------------------------------------------------------------------------
// POST /internal/v1/viewer-tokens/verify
// ---------------------------------------------------------------------------

func TestInternalViewerTokenVerify(t *testing.T) {
	h := newInternalHarness(t)
	appID, _ := h.seedApp("dash1", []byte("bundle-one"))
	ctx := context.Background()
	tokenID, secret, err := h.store.MintToken(ctx, appID)
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}

	verify := func(appID, tokenID, secret string) *httptest.ResponseRecorder {
		return h.do(http.MethodPost, "/internal/v1/viewer-tokens/verify",
			fmt.Sprintf(`{"app_id":%q,"token_id":%q,"secret":%q}`, appID, tokenID, secret))
	}
	okOf := func(w *httptest.ResponseRecorder) bool {
		t.Helper()
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
		}
		var got map[string]any
		decodeBody(t, w, &got)
		if len(got) != 1 {
			t.Errorf("response keys = %v, want exactly {ok}", got)
		}
		ok, isBool := got["ok"].(bool)
		if !isBool {
			t.Fatalf(`"ok" = %v, want a bool`, got["ok"])
		}
		return ok
	}

	if !okOf(verify(appID, tokenID, secret)) {
		t.Errorf("ok = false for the correct secret, want true")
	}
	if okOf(verify(appID, tokenID, secret+"x")) {
		t.Errorf("ok = true for a wrong secret, want false")
	}
	// Right secret, wrong app: viewer tokens bind to the app.
	otherApp, _ := h.seedApp("dash2", []byte("bundle-two"))
	if okOf(verify(otherApp, tokenID, secret)) {
		t.Errorf("ok = true for another app's token, want false")
	}
	// Unknown token id.
	if okOf(verify(appID, uuid.New().String(), secret)) {
		t.Errorf("ok = true for an unknown token_id, want false")
	}

	// Revoked -> ok:false with the correct secret.
	if err := h.store.RevokeToken(ctx, appID, tokenID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if okOf(verify(appID, tokenID, secret)) {
		t.Errorf("ok = true for a revoked token, want false")
	}
}

func TestInternalViewerTokenVerify_BadRequest(t *testing.T) {
	h := newInternalHarness(t)
	for _, body := range []string{
		`{"app_id":"nope","token_id":"` + uuid.New().String() + `","secret":"s"}`,
		`{"app_id":"` + uuid.New().String() + `","token_id":"nope","secret":"s"}`,
		`{"app_id":"` + uuid.New().String() + `","token_id":"` + uuid.New().String() + `"}`,
		`not json`,
	} {
		w := h.do(http.MethodPost, "/internal/v1/viewer-tokens/verify", body)
		assertEnvelope(t, w, http.StatusBadRequest, "bad_request")
	}
}

// ---------------------------------------------------------------------------
// POST /internal/v1/viewer-tokens/active
// ---------------------------------------------------------------------------

// TestInternalTokenActive is the W3-fix Blocker's server-side coverage
// (spec §5.3's cookie-only revocation recheck has no secret to present —
// this endpoint is the secret-less counterpart to
// TestInternalViewerTokenVerify).
func TestInternalTokenActive(t *testing.T) {
	h := newInternalHarness(t)
	appID, _ := h.seedApp("dash1", []byte("bundle-one"))
	ctx := context.Background()
	tokenID, _, err := h.store.MintToken(ctx, appID)
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}

	active := func(appID, tokenID string) bool {
		t.Helper()
		w := h.do(http.MethodPost, "/internal/v1/viewer-tokens/active",
			fmt.Sprintf(`{"app_id":%q,"token_id":%q}`, appID, tokenID))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
		}
		var got map[string]any
		decodeBody(t, w, &got)
		if len(got) != 1 {
			t.Errorf("response keys = %v, want exactly {active}", got)
		}
		val, isBool := got["active"].(bool)
		if !isBool {
			t.Fatalf(`"active" = %v, want a bool`, got["active"])
		}
		return val
	}

	// Happy path: a freshly-minted, non-revoked token is active — no
	// secret presented anywhere in this request.
	if !active(appID, tokenID) {
		t.Errorf("active = false for a fresh token, want true")
	}

	// App mismatch: right token_id, wrong app.
	otherApp, _ := h.seedApp("dash2", []byte("bundle-two"))
	if active(otherApp, tokenID) {
		t.Errorf("active = true for another app's token, want false")
	}

	// Unknown token_id.
	if active(appID, uuid.New().String()) {
		t.Errorf("active = true for an unknown token_id, want false")
	}

	// Revoked -> false, indistinguishably from unknown/mismatched.
	if err := h.store.RevokeToken(ctx, appID, tokenID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if active(appID, tokenID) {
		t.Errorf("active = true for a revoked token, want false")
	}
}

func TestInternalTokenActive_BadRequest(t *testing.T) {
	h := newInternalHarness(t)
	for _, body := range []string{
		`{"app_id":"nope","token_id":"` + uuid.New().String() + `"}`,
		`{"app_id":"` + uuid.New().String() + `","token_id":"nope"}`,
		`not json`,
	} {
		w := h.do(http.MethodPost, "/internal/v1/viewer-tokens/active", body)
		assertEnvelope(t, w, http.StatusBadRequest, "bad_request")
	}
}

// ---------------------------------------------------------------------------
// POST /internal/v1/sessions/verify
// ---------------------------------------------------------------------------

func TestInternalSessionsVerify_ForwardsCredentialHeaders(t *testing.T) {
	h := newInternalHarness(t)

	w := h.doWithHeaders(http.MethodPost, "/internal/v1/sessions/verify",
		fmt.Sprintf(`{"pid":%q}`, h.projectID.String()),
		map[string]string{
			"Authorization": "Bearer " + testServiceToken,
			"Cookie":        "pipeline_api_session=abc123",
		})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if h.resolver.gotCookie != "pipeline_api_session=abc123" {
		t.Errorf("resolver saw Cookie = %q, want the forwarded viewer cookie", h.resolver.gotCookie)
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if len(got) != 2 {
		t.Errorf("response keys = %v, want exactly {user_id, project_member}", got)
	}
	if got["user_id"] != h.resolver.user.ID.String() {
		t.Errorf("user_id = %v, want %s", got["user_id"], h.resolver.user.ID)
	}
	if got["project_member"] != true {
		t.Errorf("project_member = %v, want true", got["project_member"])
	}
	// The membership answer must come from a project-scoped FGA check on the
	// LAKEKEEPER project id, for the session user.
	wantCheck := "user:oidc~" + h.resolver.user.ID.String() + "|describe|project:lk-project-1"
	if len(h.authz.checked) != 1 || h.authz.checked[0] != wantCheck {
		t.Errorf("authz checks = %v, want [%s]", h.authz.checked, wantCheck)
	}
}

func TestInternalSessionsVerify_NonMember(t *testing.T) {
	h := newInternalHarness(t)
	h.authz.allow = false
	w := h.do(http.MethodPost, "/internal/v1/sessions/verify", fmt.Sprintf(`{"pid":%q}`, h.projectID.String()))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if got["user_id"] != h.resolver.user.ID.String() {
		t.Errorf("user_id = %v, want the resolved user", got["user_id"])
	}
	if got["project_member"] != false {
		t.Errorf("project_member = %v, want false", got["project_member"])
	}
}

// No session at all is a 200 "nobody, not a member" — NOT a 401. On the
// internal surface 401 means exactly one thing: the service credential is
// bad. app-worker must be able to tell those two apart.
func TestInternalSessionsVerify_NoSessionIsNotA401(t *testing.T) {
	h := newInternalHarness(t)
	h.resolver.authed = false
	w := h.do(http.MethodPost, "/internal/v1/sessions/verify", fmt.Sprintf(`{"pid":%q}`, h.projectID.String()))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if got["user_id"] != "" {
		t.Errorf("user_id = %v, want \"\"", got["user_id"])
	}
	if got["project_member"] != false {
		t.Errorf("project_member = %v, want false", got["project_member"])
	}
	if len(h.authz.checked) != 0 {
		t.Errorf("authz checks = %v, want none for an unauthenticated session", h.authz.checked)
	}
}

// The session resolver may refresh the platform session cookie on the writer
// it is handed. That Set-Cookie belongs to the viewer's browser, not to
// app-worker's internal response — it must be swallowed.
func TestInternalSessionsVerify_DoesNotLeakSetCookie(t *testing.T) {
	h := newInternalHarness(t)
	h.resolver.setsCookie = true
	w := h.do(http.MethodPost, "/internal/v1/sessions/verify", fmt.Sprintf(`{"pid":%q}`, h.projectID.String()))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if sc := w.Header().Values("Set-Cookie"); len(sc) != 0 {
		t.Errorf("Set-Cookie = %v on an internal response, want none", sc)
	}
}

// ---- Forwarded caller credential (X-Datuplet-Forwarded-Authorization) ----

// bearerResolver authenticates only when it sees one exact bearer value in
// the request's Authorization header — the shape a real BearerJWTResolver
// has. It also records whether an Authorization header was present at all,
// so the negative case can assert the service credential never reaches it.
type bearerResolver struct {
	want       string
	user       *store.User
	sawAuth    string
	sawAuthSet bool
	calls      int
}

func (b *bearerResolver) UserFor(_ http.ResponseWriter, r *http.Request) (*store.User, bool, error) {
	b.calls++
	b.sawAuth = r.Header.Get("Authorization")
	_, b.sawAuthSet = r.Header["Authorization"]
	if b.sawAuth != b.want {
		return nil, false, nil
	}
	return b.user, true, nil
}
func (b *bearerResolver) Mode() string        { return "test" }
func (b *bearerResolver) SupportsLogin() bool { return false }

// withBearerResolver rebuilds the harness's mux around a bearerResolver.
func (h *internalHarness) withBearerResolver(want string) *bearerResolver {
	h.t.Helper()
	br := &bearerResolver{want: want, user: &store.User{ID: uuid.New(), Email: "cli@b.c"}}
	ih := &apps.InternalHandlers{
		Store: h.store, Identity: h.ident, Authz: h.authz,
		Projects: h.projects, Resolver: br,
		Token: mustServiceToken(h.t, testServiceToken),
	}
	mux := http.NewServeMux()
	ih.RegisterInternal(mux)
	h.mux = mux
	return br
}

// A bearer-authenticated caller (Part 5's `datuplet apps render`) cannot put
// its credential in Authorization — app-worker's service credential owns that
// header. It forwards it in X-Datuplet-Forwarded-Authorization instead, and
// the handler must move it into Authorization for the resolver.
func TestInternalSessionsVerify_ForwardsBearerCredential(t *testing.T) {
	h := newInternalHarness(t)
	const userBearer = "Bearer user-cli-jwt-value"
	br := h.withBearerResolver(userBearer)

	w := h.doWithHeaders(http.MethodPost, "/internal/v1/sessions/verify",
		fmt.Sprintf(`{"pid":%q}`, h.projectID.String()),
		map[string]string{
			"Authorization":                      "Bearer " + testServiceToken,
			"X-Datuplet-Forwarded-Authorization": userBearer,
		})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if br.sawAuth != userBearer {
		t.Errorf("resolver saw Authorization = %q, want the forwarded caller credential %q",
			br.sawAuth, userBearer)
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if got["user_id"] != br.user.ID.String() {
		t.Errorf("user_id = %v, want %s", got["user_id"], br.user.ID)
	}
	if got["project_member"] != true {
		t.Errorf("project_member = %v, want true", got["project_member"])
	}
}

// With no forwarding header, the resolver must see NO Authorization at all —
// app-worker's service credential must never be mistakable for a user
// credential (a resolver that accepted it would authenticate the worker as
// whichever user that token maps to).
func TestInternalSessionsVerify_ServiceTokenNeverReachesResolver(t *testing.T) {
	h := newInternalHarness(t)
	br := h.withBearerResolver("Bearer " + testServiceToken)

	w := h.do(http.MethodPost, "/internal/v1/sessions/verify", fmt.Sprintf(`{"pid":%q}`, h.projectID.String()))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if br.sawAuthSet {
		t.Errorf("resolver saw Authorization = %q, want the header absent entirely", br.sawAuth)
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if got["user_id"] != "" || got["project_member"] != false {
		t.Errorf("response = %v, want the unauthenticated {user_id:\"\", project_member:false}", got)
	}
}

// The forwarding header itself must not leak through to the resolver: only
// the Authorization it was translated into.
func TestInternalSessionsVerify_StripsForwardingHeader(t *testing.T) {
	h := newInternalHarness(t)
	const userBearer = "Bearer user-cli-jwt-value"
	var sawForward string
	h.resolver = &recordingResolver{user: &store.User{ID: uuid.New()}, authed: true}
	probe := &headerProbeResolver{inner: h.resolver, onCall: func(r *http.Request) {
		sawForward = r.Header.Get(apps.ForwardedAuthorizationHeader)
	}}
	ih := &apps.InternalHandlers{
		Store: h.store, Identity: h.ident, Authz: h.authz,
		Projects: h.projects, Resolver: probe,
		Token: mustServiceToken(t, testServiceToken),
	}
	mux := http.NewServeMux()
	ih.RegisterInternal(mux)
	h.mux = mux

	w := h.doWithHeaders(http.MethodPost, "/internal/v1/sessions/verify",
		fmt.Sprintf(`{"pid":%q}`, h.projectID.String()),
		map[string]string{
			"Authorization":                   "Bearer " + testServiceToken,
			apps.ForwardedAuthorizationHeader: userBearer,
		})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if sawForward != "" {
		t.Errorf("resolver saw %s = %q, want it stripped", apps.ForwardedAuthorizationHeader, sawForward)
	}
}

type headerProbeResolver struct {
	inner  *recordingResolver
	onCall func(*http.Request)
}

func (p *headerProbeResolver) UserFor(w http.ResponseWriter, r *http.Request) (*store.User, bool, error) {
	p.onCall(r)
	return p.inner.UserFor(w, r)
}
func (p *headerProbeResolver) Mode() string        { return "test" }
func (p *headerProbeResolver) SupportsLogin() bool { return false }

// Rewriting the credential must not mutate the caller's request header map —
// http.Header is shared by reference, and the service-credential gate (plus
// any middleware or logging after the handler) still reads Authorization.
func TestInternalSessionsVerify_DoesNotMutateRequestHeaders(t *testing.T) {
	h := newInternalHarness(t)
	const userBearer = "Bearer user-cli-jwt-value"
	h.withBearerResolver(userBearer)

	r := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/verify",
		bytes.NewReader([]byte(fmt.Sprintf(`{"pid":%q}`, h.projectID.String()))))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+testServiceToken)
	r.Header.Set(apps.ForwardedAuthorizationHeader, userBearer)
	w := httptest.NewRecorder()
	h.mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if got := r.Header.Get("Authorization"); got != "Bearer "+testServiceToken {
		t.Errorf("original request Authorization = %q, want the service credential untouched", got)
	}
	if got := r.Header.Get(apps.ForwardedAuthorizationHeader); got != userBearer {
		t.Errorf("original request %s = %q, want it untouched", apps.ForwardedAuthorizationHeader, got)
	}
}

func TestInternalSessionsVerify_BadRequest(t *testing.T) {
	h := newInternalHarness(t)
	for _, body := range []string{`{"pid":"nope"}`, `{}`, `garbage`} {
		w := h.do(http.MethodPost, "/internal/v1/sessions/verify", body)
		assertEnvelope(t, w, http.StatusBadRequest, "bad_request")
	}
}

// An unknown project is "not a member", not an error: the honest answer to
// "is this user a member of pid" for a pid that does not exist is no.
func TestInternalSessionsVerify_UnknownProject(t *testing.T) {
	h := newInternalHarness(t)
	h.projects.err = apps.ErrNotFound
	w := h.do(http.MethodPost, "/internal/v1/sessions/verify", fmt.Sprintf(`{"pid":%q}`, uuid.New().String()))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if got["project_member"] != false {
		t.Errorf("project_member = %v, want false", got["project_member"])
	}
}

// A project whose lakekeeper counterpart is not provisioned yet cannot be
// authorized against — fail closed with 503, never "member: true".
func TestInternalSessionsVerify_UnprovisionedProject(t *testing.T) {
	h := newInternalHarness(t)
	h.projects.lakekeeperID = ""
	w := h.do(http.MethodPost, "/internal/v1/sessions/verify", fmt.Sprintf(`{"pid":%q}`, h.projectID.String()))
	assertEnvelope(t, w, http.StatusServiceUnavailable, "unavailable")
}

// ---------------------------------------------------------------------------
// POST /internal/v1/impersonate
// ---------------------------------------------------------------------------

func TestInternalImpersonate_MintsViaIdentityManager(t *testing.T) {
	h := newInternalHarness(t)
	appID, _ := h.seedApp("dash1", []byte("bundle-one"))

	w := h.do(http.MethodPost, "/internal/v1/impersonate", fmt.Sprintf(`{"app_id":%q}`, appID))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var got map[string]any
	decodeBody(t, w, &got)
	if len(got) != 1 {
		t.Errorf("response keys = %v, want exactly {token}", got)
	}
	if got["token"] != "fake-token-"+appID {
		t.Errorf("token = %v, want the IdentityManager.Mint result", got["token"])
	}
	// Mint is called with the app row's OWN project id — the worker never
	// names the project (or any identity) itself.
	want := "Mint:" + appID + ":" + h.projectID.String()
	if len(h.ident.Calls) != 1 || h.ident.Calls[0] != want {
		t.Errorf("identity calls = %v, want [%s]", h.ident.Calls, want)
	}
}

func TestInternalImpersonate_RefusesUnknownApp(t *testing.T) {
	h := newInternalHarness(t)
	h.seedApp("dash1", []byte("bundle-one"))

	w := h.do(http.MethodPost, "/internal/v1/impersonate", fmt.Sprintf(`{"app_id":%q}`, uuid.New().String()))
	assertEnvelope(t, w, http.StatusNotFound, "app_not_found")
	if len(h.ident.Calls) != 0 {
		t.Errorf("identity calls = %v, want none for an unknown app_id", h.ident.Calls)
	}

	w = h.do(http.MethodPost, "/internal/v1/impersonate", `{"app_id":"not-a-uuid"}`)
	assertEnvelope(t, w, http.StatusBadRequest, "bad_request")
	w = h.do(http.MethodPost, "/internal/v1/impersonate", `{}`)
	assertEnvelope(t, w, http.StatusBadRequest, "bad_request")
	if len(h.ident.Calls) != 0 {
		t.Errorf("identity calls = %v, want none for a malformed app_id", h.ident.Calls)
	}
}

func TestInternalImpersonate_MintFailureIsUnavailable(t *testing.T) {
	h := newInternalHarness(t)
	appID, _ := h.seedApp("dash1", []byte("bundle-one"))
	failing := &failingMintIdentity{}
	ih := &apps.InternalHandlers{
		Store: h.store, Identity: failing, Authz: h.authz,
		Projects: h.projects, Resolver: h.resolver,
		Token: mustServiceToken(t, testServiceToken),
	}
	mux := http.NewServeMux()
	ih.RegisterInternal(mux)
	h.mux = mux

	w := h.do(http.MethodPost, "/internal/v1/impersonate", fmt.Sprintf(`{"app_id":%q}`, appID))
	assertEnvelope(t, w, http.StatusServiceUnavailable, "unavailable")
}

type failingMintIdentity struct{ apps.RecorderIdentity }

func (f *failingMintIdentity) Mint(context.Context, string, string) (string, error) {
	return "", fmt.Errorf("fga down")
}

// ---------------------------------------------------------------------------
// POST /internal/v1/apps/{app_id}/logs
// ---------------------------------------------------------------------------

func logBody(hash string, kv map[string]any) string {
	body := map[string]any{
		"request_id":     uuid.New().String(),
		"version_hash":   hash,
		"channel":        "production",
		"principal_kind": "viewer_token",
		"principal_id":   uuid.New().String(),
		"started_at":     time.Now().UTC().Format(time.RFC3339Nano),
		"duration_ms":    123,
		"outcome":        "ok",
		"log_text":       "hello from the guest",
	}
	for k, v := range kv {
		if v == nil {
			delete(body, k)
			continue
		}
		body[k] = v
	}
	b, _ := json.Marshal(body)
	return string(b)
}

func TestInternalLogs_AppendsRecord(t *testing.T) {
	h := newInternalHarness(t)
	appID, hash := h.seedApp("dash1", []byte("bundle-one"))
	requestID := uuid.New().String()

	w := h.do(http.MethodPost, "/internal/v1/apps/"+appID+"/logs",
		logBody(hash, map[string]any{"request_id": requestID, "outcome": "render_error", "error": "boom"}))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	recs, err := h.store.GetRenderLogs(context.Background(), appID, requestID, 0)
	if err != nil {
		t.Fatalf("GetRenderLogs: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	rec := recs[0]
	if rec.AppID != appID || rec.VersionHash != hash || rec.Channel != "production" {
		t.Errorf("record = %+v, want app/version/channel from the request", rec)
	}
	if rec.PrincipalKind != "viewer_token" || rec.Outcome != "render_error" || rec.Error != "boom" {
		t.Errorf("record = %+v, want the posted principal/outcome/error", rec)
	}
	if rec.DurationMS != 123 || rec.LogText != "hello from the guest" {
		t.Errorf("record = %+v, want the posted duration/log_text", rec)
	}
}

func TestInternalLogs_UnknownApp404(t *testing.T) {
	h := newInternalHarness(t)
	_, hash := h.seedApp("dash1", []byte("bundle-one"))
	w := h.do(http.MethodPost, "/internal/v1/apps/"+uuid.New().String()+"/logs", logBody(hash, nil))
	assertEnvelope(t, w, http.StatusNotFound, "app_not_found")
}

func TestInternalLogs_BadRequest(t *testing.T) {
	h := newInternalHarness(t)
	appID, hash := h.seedApp("dash1", []byte("bundle-one"))
	path := "/internal/v1/apps/" + appID + "/logs"

	for name, body := range map[string]string{
		"missing request_id":   logBody(hash, map[string]any{"request_id": nil}),
		"bad request_id":       logBody(hash, map[string]any{"request_id": "nope"}),
		"short version_hash":   logBody(hash, map[string]any{"version_hash": "abc"}),
		"missing outcome":      logBody(hash, map[string]any{"outcome": nil}),
		"bad principal_kind":   logBody(hash, map[string]any{"principal_kind": "root"}),
		"bad started_at":       logBody(hash, map[string]any{"started_at": "yesterday"}),
		"negative duration":    logBody(hash, map[string]any{"duration_ms": -1}),
		"app_id/path mismatch": logBody(hash, map[string]any{"app_id": uuid.New().String()}),
		"not json":             `nope`,
	} {
		t.Run(name, func(t *testing.T) {
			assertEnvelope(t, h.do(http.MethodPost, path, body), http.StatusBadRequest, "bad_request")
		})
	}

	// A malformed app_id in the PATH is a worker bug, not a missing app.
	assertEnvelope(t, h.do(http.MethodPost, "/internal/v1/apps/not-a-uuid/logs", logBody(hash, nil)),
		http.StatusBadRequest, "bad_request")
}

// A body echoing the path's app_id (the symmetric shape the author-route
// render-log JSON uses) is accepted.
func TestInternalLogs_AcceptsMatchingAppIDInBody(t *testing.T) {
	h := newInternalHarness(t)
	appID, hash := h.seedApp("dash1", []byte("bundle-one"))
	w := h.do(http.MethodPost, "/internal/v1/apps/"+appID+"/logs",
		logBody(hash, map[string]any{"app_id": appID}))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
}

// log_text is capped (spec §7: ≤64 KiB per render) by truncation, so a
// misbehaving worker can never write an unbounded row.
func TestInternalLogs_TruncatesOversizeLogText(t *testing.T) {
	h := newInternalHarness(t)
	appID, hash := h.seedApp("dash1", []byte("bundle-one"))
	requestID := uuid.New().String()
	huge := strings.Repeat("x", apps.MaxRenderLogBytes+5000)

	w := h.do(http.MethodPost, "/internal/v1/apps/"+appID+"/logs",
		logBody(hash, map[string]any{"request_id": requestID, "log_text": huge}))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	recs, err := h.store.GetRenderLogs(context.Background(), appID, requestID, 0)
	if err != nil {
		t.Fatalf("GetRenderLogs: %v", err)
	}
	if got := len(recs[0].LogText); got > apps.MaxRenderLogBytes {
		t.Errorf("stored log_text = %d bytes, want <= %d", got, apps.MaxRenderLogBytes)
	}
}
