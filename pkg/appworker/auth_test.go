package appworker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newAuthTestServer builds a *Server wired for auth tests only: no engine, no
// render path, an injected authAPI double, cookie key, and fake clock.
func newAuthTestServer(cfg Config, api authAPI, cookieKey string, now func() time.Time) *Server {
	return NewServer(cfg, nil,
		WithAuthAPI(api),
		WithCookieKey(cookieKey),
		WithServerClock(now),
	)
}

// trustLoopbackConfig is the Config for tests that need X-Forwarded-For to be
// honored: httptest connects from loopback, so trusting loopback as a proxy
// makes the harness stand in for "behind the cluster ingress".
func trustLoopbackConfig(hops int) Config {
	return Config{TrustedProxies: ProxyTrust{
		CIDRs: parseTrustedProxies("127.0.0.0/8,::1/128"),
		Hops:  hops,
	}}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const (
	testCookieKey  = "test-cookie-hmac-key-do-not-use-in-production"
	testProjectID  = "11111111-1111-1111-1111-111111111111"
	testAppName    = "sales-overview"
	testAppID      = "22222222-2222-2222-2222-222222222222"
	testOtherAppID = "33333333-3333-3333-3333-333333333333"
	testTokenID    = "44444444-4444-4444-4444-444444444444"
	testSecret     = "s3cret-viewer-token-secret-value-32b"
	testUserID     = "55555555-5555-5555-5555-555555555555"
)

// testClockBase is a fixed, realistic wall-clock instant. Every TTL and rate
// assertion in this file advances this value explicitly — no test ever sleeps.
var testClockBase = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

// fakeAuthAPI is the authAPI seam: a hand-written double that records calls
// and returns scripted answers. Every field is guarded because httptest
// serves each request on its own goroutine (-race must stay clean).
type fakeAuthAPI struct {
	mu sync.Mutex

	// verify script: if correctSecret != "", VerifyToken compares against it;
	// otherwise verifyOK is returned verbatim.
	correctSecret string
	verifyOK      bool
	verifyErr     error

	tokenActive    bool
	tokenActiveErr error

	// verifyHook runs inside VerifyToken, outside the mutex. Used by the
	// concurrency test as a barrier.
	verifyHook func()

	session    SessionInfo
	sessionErr error

	verifyCalls  int
	activeCalls  int
	sessionCalls int

	lastVerifyAppID, lastVerifyTokenID, lastVerifySecret string
	lastActiveAppID, lastActiveTokenID                   string
	lastSessionPID, lastSessionCookies, lastSessionAuthz string
}

// VerifyToken records the call, then runs verifyHook (if any) OUTSIDE the
// mutex — the concurrency test needs real overlap inside the fake, which a
// hook held under f.mu would serialize away.
func (f *fakeAuthAPI) VerifyToken(_ context.Context, appID, tokenID, secret string) (bool, error) {
	f.mu.Lock()
	f.verifyCalls++
	f.lastVerifyAppID, f.lastVerifyTokenID, f.lastVerifySecret = appID, tokenID, secret
	hook, err := f.verifyHook, f.verifyErr
	result := f.verifyOK
	if f.correctSecret != "" {
		result = secret == f.correctSecret
	}
	f.mu.Unlock()

	if hook != nil {
		hook()
	}
	if err != nil {
		return false, err
	}
	return result, nil
}

func (f *fakeAuthAPI) CheckTokenActive(_ context.Context, appID, tokenID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeCalls++
	f.lastActiveAppID, f.lastActiveTokenID = appID, tokenID
	if f.tokenActiveErr != nil {
		return false, f.tokenActiveErr
	}
	return f.tokenActive, nil
}

func (f *fakeAuthAPI) VerifySession(_ context.Context, pid, cookies, authz string) (SessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessionCalls++
	f.lastSessionPID, f.lastSessionCookies, f.lastSessionAuthz = pid, cookies, authz
	if f.sessionErr != nil {
		return SessionInfo{}, f.sessionErr
	}
	return f.session, nil
}

func (f *fakeAuthAPI) counts() (verify, active, session int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.verifyCalls, f.activeCalls, f.sessionCalls
}

// authHarness wires a real *Server (so ServeHTTP's worker-wide
// Referrer-Policy and the real error envelope are exercised) behind an
// httptest server, with a fake clock and a fake authAPI.
type authHarness struct {
	t   *testing.T
	srv *Server
	api *fakeAuthAPI
	ts  *httptest.Server

	mu    sync.Mutex
	now   time.Time
	appID string

	// lastPrincipal records what authenticate handed the (test) render
	// handler on the most recent successful authentication.
	lastPrincipal principal
}

func newAuthHarness(t *testing.T) *authHarness {
	t.Helper()
	return newAuthHarnessWithConfig(t, Config{})
}

func newAuthHarnessWithConfig(t *testing.T, cfg Config) *authHarness {
	t.Helper()
	h := &authHarness{t: t, api: &fakeAuthAPI{}, now: testClockBase, appID: testAppID}
	h.srv = newAuthTestServer(cfg, h.api, testCookieKey, h.clock)
	h.registerRoute()
	h.ts = httptest.NewServer(h.srv)
	t.Cleanup(h.ts.Close)
	return h
}

// newHarnessWithAPI is newAuthHarness for the tests that need a real
// *APIClient (revocation propagation) instead of the fake.
func newHarnessWithRealClient(t *testing.T, api authAPI) *authHarness {
	t.Helper()
	h := &authHarness{t: t, api: &fakeAuthAPI{}, now: testClockBase, appID: testAppID}
	h.srv = newAuthTestServer(Config{}, api, testCookieKey, h.clock)
	h.registerRoute()
	h.ts = httptest.NewServer(h.srv)
	t.Cleanup(h.ts.Close)
	return h
}

// registerRoute installs the one test route that calls authenticate. Route
// parsing itself belongs to W5/W6; this keeps the auth seam under test
// without inventing that task's routing.
func (h *authHarness) registerRoute() {
	h.srv.mux.HandleFunc("/apps/{pid}/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		channel := channelProduction
		if strings.HasSuffix(name, "@draft") {
			channel = channelDraft
			name = strings.TrimSuffix(name, "@draft")
		}
		resolved := resolvedApp{
			ProjectID:   r.PathValue("pid"),
			Name:        name,
			Channel:     channel,
			AppID:       h.resolvedAppID(),
			VersionHash: "deadbeef",
		}
		p, ok := h.srv.authenticate(w, r, resolved)
		if !ok {
			return
		}
		h.setLastPrincipal(p)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"kind": p.Kind, "id": p.ID})
	})
}

func (h *authHarness) clock() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.now
}

func (h *authHarness) advance(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.now = h.now.Add(d)
}

func (h *authHarness) resolvedAppID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.appID
}

func (h *authHarness) setResolvedAppID(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.appID = id
}

func (h *authHarness) setLastPrincipal(p principal) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastPrincipal = p
}

func (h *authHarness) principalSeen() principal {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastPrincipal
}

func (h *authHarness) appURL(suffix string) string {
	return h.ts.URL + "/apps/" + testProjectID + "/" + testAppName + suffix
}

// client returns a client that never follows redirects, so the 302 exchange
// response itself is observable (headers included).
func (h *authHarness) client() *http.Client {
	c := h.ts.Client()
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return c
}

func (h *authHarness) do(req *http.Request) *http.Response {
	h.t.Helper()
	resp, err := h.client().Do(req)
	if err != nil {
		h.t.Fatalf("request: %v", err)
	}
	return resp
}

func (h *authHarness) get(rawurl string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawurl, nil)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	return h.do(req)
}

// getWithCookie issues a GET carrying a session cookie signed for appID.
func (h *authHarness) getWithCookieFor(rawurl, appID, tokenID string, exp time.Time) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawurl, nil)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: signCookie(testCookieKey, appID, tokenID, exp)})
	return h.do(req)
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func decodeEnvelope(t *testing.T, resp *http.Response) errorBody {
	t.Helper()
	var e errorBody
	if err := json.Unmarshal([]byte(readBody(t, resp)), &e); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return e
}

// jsonGet issues a GET with Accept: application/json so error responses come
// back as the JSON envelope rather than the HTML page.
func (h *authHarness) jsonGet(rawurl string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawurl, nil)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept", "application/json")
	return h.do(req)
}

func tokenParam(tokenID, secret string) string {
	return "?token=" + url.QueryEscape(viewerTokenPrefix+tokenID+"."+secret)
}

// ---------------------------------------------------------------------------
// Step 1 — the one-time 302 exchange
// ---------------------------------------------------------------------------

func TestExchange_SetsCookieAndRedirectsToTokenFreeURL(t *testing.T) {
	h := newAuthHarness(t)
	h.api.correctSecret = testSecret

	resp := h.get(h.appURL(tokenParam(testTokenID, testSecret) + "&region=emea"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302 (body %q)", resp.StatusCode, readBody(t, resp))
	}

	loc := resp.Header.Get("Location")
	if loc == "" {
		t.Fatal("no Location header on the exchange response")
	}
	if strings.Contains(loc, "token") || strings.Contains(loc, testSecret) || strings.Contains(loc, testTokenID) {
		t.Fatalf("Location %q must be token-free", loc)
	}
	if !strings.HasPrefix(loc, "/apps/"+testProjectID+"/"+testAppName) {
		t.Fatalf("Location %q must be a relative, same-app path (no open redirect)", loc)
	}
	if !strings.Contains(loc, "region=emea") {
		t.Fatalf("Location %q must preserve non-reserved query params", loc)
	}

	if got := resp.Header.Get("Referrer-Policy"); got != "no-referrer" {
		t.Fatalf("Referrer-Policy on the 302 = %q, want %q", got, "no-referrer")
	}

	setCookie := resp.Header.Values("Set-Cookie")
	if len(setCookie) != 1 {
		t.Fatalf("Set-Cookie headers = %d, want 1 (%v)", len(setCookie), setCookie)
	}
	assertSessionCookieAttributes(t, setCookie[0])

	// The cookie must verify and carry the resolved app + token id, with a
	// 24h expiry measured from the fake clock.
	value := sessionCookieValue(t, setCookie[0])
	payload, err := parseCookie(testCookieKey, value)
	if err != nil {
		t.Fatalf("parseCookie on the issued cookie: %v", err)
	}
	if payload.AppID != testAppID || payload.TokenID != testTokenID {
		t.Fatalf("payload = %+v, want app_id %q token_id %q", payload, testAppID, testTokenID)
	}
	wantExp := testClockBase.Add(sessionCookieTTL).Unix()
	if payload.Exp != wantExp {
		t.Fatalf("payload.Exp = %d, want %d (24h from the fake clock)", payload.Exp, wantExp)
	}

	// Exactly one verify, and the secret was passed through verbatim.
	verify, _, _ := h.api.counts()
	if verify != 1 {
		t.Fatalf("VerifyToken calls = %d, want 1", verify)
	}
	if h.api.lastVerifySecret != testSecret || h.api.lastVerifyAppID != testAppID || h.api.lastVerifyTokenID != testTokenID {
		t.Fatalf("VerifyToken got (%q,%q,%q)", h.api.lastVerifyAppID, h.api.lastVerifyTokenID, h.api.lastVerifySecret)
	}
}

// assertSessionCookieAttributes pins the exact attribute set from
// contract-and-constraints.md: HttpOnly; Secure; SameSite=Lax;
// Path=/apps/{pid}/{name}; 24h TTL.
func assertSessionCookieAttributes(t *testing.T, raw string) {
	t.Helper()
	parts := strings.Split(raw, "; ")
	if len(parts) == 0 || !strings.HasPrefix(parts[0], sessionCookieName+"=") {
		t.Fatalf("Set-Cookie %q must start with %s=", raw, sessionCookieName)
	}
	got := map[string]bool{}
	for _, p := range parts[1:] {
		got[p] = true
	}
	want := []string{
		"Path=/apps/" + testProjectID + "/" + testAppName,
		"Max-Age=86400",
		"HttpOnly",
		"Secure",
		"SameSite=Lax",
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("Set-Cookie %q missing attribute %q", raw, w)
		}
		delete(got, w)
	}
	for extra := range got {
		t.Errorf("Set-Cookie %q has unexpected attribute %q", raw, extra)
	}
}

func sessionCookieValue(t *testing.T, raw string) string {
	t.Helper()
	nameValue := strings.Split(raw, "; ")[0]
	_, v, ok := strings.Cut(nameValue, "=")
	if !ok {
		t.Fatalf("malformed Set-Cookie %q", raw)
	}
	return v
}

func TestExchange_MalformedUnknownRevokedTokenGives403(t *testing.T) {
	cases := []struct {
		name        string
		token       string
		verifyOK    bool
		wantVerify  int
		description string
	}{
		{name: "missing vw_ prefix", token: testTokenID + "." + testSecret, wantVerify: 0},
		{name: "no dot separator", token: viewerTokenPrefix + testTokenID + testSecret, wantVerify: 0},
		{name: "empty secret", token: viewerTokenPrefix + testTokenID + ".", wantVerify: 0},
		{name: "empty token id", token: viewerTokenPrefix + "." + testSecret, wantVerify: 0},
		{name: "prefix only", token: viewerTokenPrefix, wantVerify: 0},
		{name: "unknown token id", token: viewerTokenPrefix + testTokenID + "." + testSecret, verifyOK: false, wantVerify: 1},
		{name: "revoked token", token: viewerTokenPrefix + testTokenID + "." + testSecret, verifyOK: false, wantVerify: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newAuthHarness(t)
			h.api.verifyOK = tc.verifyOK

			resp := h.jsonGet(h.appURL("?token=" + url.QueryEscape(tc.token)))
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", resp.StatusCode)
			}
			env := decodeEnvelope(t, resp)
			if env.Kind != errKindUnauthorized {
				t.Fatalf("kind = %q, want %q", env.Kind, errKindUnauthorized)
			}
			if env.RequestID == "" {
				t.Fatal("envelope carries no request_id")
			}
			if strings.Contains(env.Error, testSecret) {
				t.Fatalf("error message %q leaks the token secret", env.Error)
			}
			if resp.Header.Get("Set-Cookie") != "" {
				t.Fatal("a failed exchange must not set a session cookie")
			}
			if verify, _, _ := h.api.counts(); verify != tc.wantVerify {
				t.Fatalf("VerifyToken calls = %d, want %d (malformed tokens must never reach pipeline-api)", verify, tc.wantVerify)
			}
		})
	}
}

func TestExchange_EleventhFailureFromOneIPAndAppIsRateLimited(t *testing.T) {
	// httptest connects from loopback, so trusting loopback as a proxy is
	// what lets this test drive distinct client IPs through X-Forwarded-For
	// the way the real ingress would.
	h := newAuthHarnessWithConfig(t, trustLoopbackConfig(1))
	h.api.verifyOK = false

	attempt := func(ip string) *http.Response {
		req, err := http.NewRequest(http.MethodGet, h.appURL(tokenParam(testTokenID, testSecret)), nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-Forwarded-For", ip)
		return h.do(req)
	}

	for i := 1; i <= verifyFailuresPerMin; i++ {
		resp := attempt("203.0.113.7")
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("attempt %d: status = %d, want 403", i, resp.StatusCode)
		}
		resp.Body.Close()
	}

	resp := attempt("203.0.113.7")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("11th attempt status = %d, want 429 (body %q)", resp.StatusCode, readBody(t, resp))
	}
	if got := resp.Header.Get("Retry-After"); got != "60" {
		t.Fatalf("Retry-After = %q, want %q", got, "60")
	}
	env := decodeEnvelope(t, resp)
	if env.Kind != errKindRateLimited {
		t.Fatalf("kind = %q, want %q", env.Kind, errKindRateLimited)
	}

	// The throttle stops hammering pipeline-api: exactly 10 verifies so far.
	if verify, _, _ := h.api.counts(); verify != verifyFailuresPerMin {
		t.Fatalf("VerifyToken calls = %d, want %d (the 11th must not reach pipeline-api)", verify, verifyFailuresPerMin)
	}

	// A different client IP has its own bucket.
	other := attempt("198.51.100.9")
	if other.StatusCode != http.StatusForbidden {
		t.Fatalf("different IP status = %d, want 403 (buckets are per (IP, app))", other.StatusCode)
	}
	other.Body.Close()

	// A different app has its own bucket for the same IP.
	h.setResolvedAppID(testOtherAppID)
	otherApp := attempt("203.0.113.7")
	if otherApp.StatusCode != http.StatusForbidden {
		t.Fatalf("different app status = %d, want 403 (buckets are per (IP, app))", otherApp.StatusCode)
	}
	otherApp.Body.Close()

	// And the bucket refills: after a minute the same IP+app may try again.
	h.setResolvedAppID(testAppID)
	h.advance(61 * time.Second)
	again := attempt("203.0.113.7")
	if again.StatusCode != http.StatusForbidden {
		t.Fatalf("after 61s status = %d, want 403 (bucket must refill)", again.StatusCode)
	}
	again.Body.Close()
}

// ---------------------------------------------------------------------------
// Step 1 — cookie sessions
// ---------------------------------------------------------------------------

func TestCookie_HappyPathGivesViewerTokenPrincipal(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActive = true

	resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()

	if got := h.principalSeen(); got.Kind != principalKindViewerToken || got.ID != testTokenID {
		t.Fatalf("principal = %+v, want {viewer_token, %s}", got, testTokenID)
	}
	if _, active, _ := h.api.counts(); active != 1 {
		t.Fatalf("CheckTokenActive calls = %d, want 1", active)
	}
}

func TestCookie_ReplayedOnAnotherAppIs401(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActive = true

	// Cookie minted for app A; the route resolves app B.
	h.setResolvedAppID(testOtherAppID)
	resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body %q)", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()

	if _, active, _ := h.api.counts(); active != 0 {
		t.Fatalf("CheckTokenActive calls = %d, want 0 (the app binding is checked before any upstream call)", active)
	}
}

func TestCookie_ReplayedOnAnotherAppEnvelopeKind(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActive = true
	h.setResolvedAppID(testOtherAppID)

	req, _ := http.NewRequest(http.MethodGet, h.appURL(""), nil)
	req.Header.Set("Accept", "application/json")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: signCookie(testCookieKey, testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))})
	resp := h.do(req)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if env := decodeEnvelope(t, resp); env.Kind != errKindUnauthorized {
		t.Fatalf("kind = %q, want %q", env.Kind, errKindUnauthorized)
	}
}

func TestCookie_TamperedOrExpiredOrForeignKeyIs401(t *testing.T) {
	valid := signCookie(testCookieKey, testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
	seg, mac, _ := strings.Cut(valid, ".")

	forged := func() string {
		// Attacker rewrites the payload to another app but keeps the MAC.
		p, _ := json.Marshal(cookiePayload{AppID: testOtherAppID, TokenID: testTokenID, Exp: testClockBase.Add(sessionCookieTTL).Unix()})
		return base64.RawURLEncoding.EncodeToString(p) + "." + mac
	}()

	cases := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"no separator", seg},
		{"three segments", seg + "." + mac + ".extra"},
		{"payload rewritten, MAC kept", forged},
		{"MAC truncated", seg + "." + mac[:len(mac)-4]},
		{"MAC from another key", signCookieWithKey(t, "a-different-cookie-key", testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))},
		{"garbage", "not-a-cookie"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newAuthHarness(t)
			h.api.tokenActive = true

			req, _ := http.NewRequest(http.MethodGet, h.appURL(""), nil)
			req.Header.Set("Accept", "application/json")
			if tc.value != "" {
				req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: tc.value})
			}
			resp := h.do(req)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			if env := decodeEnvelope(t, resp); env.Kind != errKindUnauthorized {
				t.Fatalf("kind = %q, want unauthorized", env.Kind)
			}
			if _, active, _ := h.api.counts(); active != 0 {
				t.Fatalf("CheckTokenActive calls = %d, want 0 for an unauthenticated cookie", active)
			}
		})
	}
}

func signCookieWithKey(t *testing.T, key, appID, tokenID string, exp time.Time) string {
	t.Helper()
	return signCookie(key, appID, tokenID, exp)
}

func TestCookie_ExpiredIs401(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActive = true

	exp := testClockBase.Add(sessionCookieTTL)
	// Still inside the window: fine.
	resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, exp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pre-expiry status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// One second past exp: rejected, without any upstream call.
	before, activeBefore, _ := h.api.counts()
	_ = before
	h.advance(sessionCookieTTL + time.Second)
	resp2 := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, exp)
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("post-expiry status = %d, want 401", resp2.StatusCode)
	}
	resp2.Body.Close()
	if _, activeAfter, _ := h.api.counts(); activeAfter != activeBefore {
		t.Fatalf("CheckTokenActive was called for an expired cookie (%d -> %d)", activeBefore, activeAfter)
	}
}

func TestCookie_RevocationIsRecheckedOnEveryRequest(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActive = true

	const n = 5
	for i := 0; i < n; i++ {
		resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, resp.StatusCode)
		}
		resp.Body.Close()
	}
	if _, active, _ := h.api.counts(); active != n {
		t.Fatalf("CheckTokenActive calls = %d, want %d (once per cookie-authenticated request)", active, n)
	}
	if h.api.lastActiveAppID != testAppID || h.api.lastActiveTokenID != testTokenID {
		t.Fatalf("CheckTokenActive keyed on (%q,%q), want (%q,%q)",
			h.api.lastActiveAppID, h.api.lastActiveTokenID, testAppID, testTokenID)
	}

	// Revoked now: the very next request is rejected even though the cookie
	// signature is still perfectly valid.
	h.api.mu.Lock()
	h.api.tokenActive = false
	h.api.mu.Unlock()

	resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked-token status = %d, want 401 (body %q)", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()
}

// TestCookie_RevocationPropagatesWithin15sThroughRealAPIClient drives the
// real APIClient (with its 15s active-check cache) against a fake
// pipeline-api, proving the end-to-end revocation bound of spec §5.3: the
// cookie stays signature-valid, but the (app_id, token_id) recheck flips the
// answer once the cache TTL lapses.
func TestCookie_RevocationPropagatesWithin15sThroughRealAPIClient(t *testing.T) {
	var mu sync.Mutex
	active := true
	calls := 0

	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/viewer-tokens/active", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		a := active
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"active": a})
	})
	api := httptest.NewServer(mux)
	defer api.Close()

	var h *authHarness
	client := NewAPIClient(api.URL, "svc-token", WithClock(func() time.Time { return h.clock() }))
	defer client.Close()
	h = newHarnessWithRealClient(t, client)

	get := func() int {
		resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := get(); got != http.StatusOK {
		t.Fatalf("status = %d, want 200", got)
	}

	// Revoke. Inside the 15s positive-cache window the session survives —
	// that is the documented, intended bound, not a bug.
	mu.Lock()
	active = false
	mu.Unlock()
	h.advance(14 * time.Second)
	if got := get(); got != http.StatusOK {
		t.Fatalf("status inside the 15s cache window = %d, want 200", got)
	}

	// Past the TTL the recheck reaches pipeline-api and the session dies.
	h.advance(2 * time.Second)
	if got := get(); got != http.StatusUnauthorized {
		t.Fatalf("status after the 15s cache window = %d, want 401", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("upstream active-checks = %d, want 2 (one per TTL window)", calls)
	}
}

func TestCookie_ActiveCheckErrorFailsClosedAsUnavailable(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActiveErr = fmt.Errorf("pipeline-api down")

	resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (fail closed)", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestCookie_NoCredentialAtAllIs401(t *testing.T) {
	h := newAuthHarness(t)
	resp := h.jsonGet(h.appURL(""))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if env := decodeEnvelope(t, resp); env.Kind != errKindUnauthorized {
		t.Fatalf("kind = %q, want unauthorized", env.Kind)
	}
}

func TestAuth_UnwiredCookieKeyFailsClosed(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActive = true
	h.srv.cookieKey = "" // simulate a pod booted without the cookie-key Secret

	resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 — an empty HMAC key must never verify a cookie", resp.StatusCode)
	}
	resp.Body.Close()
	if _, active, _ := h.api.counts(); active != 0 {
		t.Fatalf("CheckTokenActive calls = %d, want 0", active)
	}
}

// ---------------------------------------------------------------------------
// Step 1 — @draft
// ---------------------------------------------------------------------------

func TestDraft_NeverAcceptsAViewerToken(t *testing.T) {
	h := newAuthHarness(t)
	h.api.correctSecret = testSecret // a *valid* token
	h.api.tokenActive = true
	h.api.session = SessionInfo{} // no platform session

	draft := h.ts.URL + "/apps/" + testProjectID + "/" + testAppName + "@draft"

	resp := h.jsonGet(draft + tokenParam(testTokenID, testSecret))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %q)", resp.StatusCode, readBody(t, resp))
	}
	if resp.Header.Get("Set-Cookie") != "" {
		t.Fatal("@draft must never issue a viewer session cookie")
	}
	if verify, _, _ := h.api.counts(); verify != 0 {
		t.Fatalf("VerifyToken calls = %d, want 0 — @draft must never verify a viewer token", verify)
	}
}

func TestDraft_ViewerCookieIsNotACredential(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActive = true
	h.api.session = SessionInfo{} // sessions/verify: nobody

	draft := h.ts.URL + "/apps/" + testProjectID + "/" + testAppName + "@draft"
	req, _ := http.NewRequest(http.MethodGet, draft, nil)
	req.Header.Set("Accept", "application/json")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: signCookie(testCookieKey, testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))})
	resp := h.do(req)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	resp.Body.Close()
	if _, active, _ := h.api.counts(); active != 0 {
		t.Fatalf("CheckTokenActive calls = %d, want 0 — the viewer path must not run on @draft", active)
	}
}

func TestDraft_PlatformSessionGivesPlatformUserPrincipal(t *testing.T) {
	h := newAuthHarness(t)
	h.api.session = SessionInfo{UserID: testUserID, ProjectMember: true}

	draft := h.ts.URL + "/apps/" + testProjectID + "/" + testAppName + "@draft"
	req, _ := http.NewRequest(http.MethodGet, draft, nil)
	req.Header.Set("Cookie", "datuplet_session=platform-session-value")
	resp := h.do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()

	if got := h.principalSeen(); got.Kind != principalKindPlatformUser || got.ID != testUserID {
		t.Fatalf("principal = %+v, want {platform_user, %s}", got, testUserID)
	}
	if h.api.lastSessionPID != testProjectID {
		t.Fatalf("VerifySession pid = %q, want %q", h.api.lastSessionPID, testProjectID)
	}
	if h.api.lastSessionCookies != "datuplet_session=platform-session-value" {
		t.Fatalf("VerifySession cookies = %q, want the Cookie header verbatim", h.api.lastSessionCookies)
	}
	if h.api.lastSessionAuthz != "" {
		t.Fatalf("VerifySession authz = %q, want empty (no bearer was presented)", h.api.lastSessionAuthz)
	}
}

func TestDraft_NonMemberIs403(t *testing.T) {
	h := newAuthHarness(t)
	h.api.session = SessionInfo{UserID: testUserID, ProjectMember: false}

	draft := h.ts.URL + "/apps/" + testProjectID + "/" + testAppName + "@draft"
	req, _ := http.NewRequest(http.MethodGet, draft, nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cookie", "datuplet_session=platform-session-value")
	resp := h.do(req)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	if env := decodeEnvelope(t, resp); env.Kind != errKindUnauthorized {
		t.Fatalf("kind = %q, want unauthorized", env.Kind)
	}
}

func TestDraft_SessionVerifyErrorFailsClosedAsUnavailable(t *testing.T) {
	h := newAuthHarness(t)
	h.api.sessionErr = fmt.Errorf("pipeline-api down")

	draft := h.ts.URL + "/apps/" + testProjectID + "/" + testAppName + "@draft"
	req, _ := http.NewRequest(http.MethodGet, draft, nil)
	req.Header.Set("Cookie", "datuplet_session=x")
	resp := h.do(req)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// Step 1 — bearer (CLI) + CSRF
// ---------------------------------------------------------------------------

func TestBearer_UsesSessionPathAndSkipsCSRFChecks(t *testing.T) {
	h := newAuthHarness(t)
	h.api.session = SessionInfo{UserID: testUserID, ProjectMember: true}

	// A POST with neither the render header nor an Origin: a browser could
	// never forge this, because it carries no ambient authority.
	req, _ := http.NewRequest(http.MethodPost, h.appURL(""), strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer platform-api-token")
	req.Header.Set("Content-Type", "application/json")
	resp := h.do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()

	if got := h.principalSeen(); got.Kind != principalKindPlatformUser || got.ID != testUserID {
		t.Fatalf("principal = %+v, want {platform_user, %s}", got, testUserID)
	}
	if h.api.lastSessionAuthz != "Bearer platform-api-token" {
		t.Fatalf("VerifySession authz = %q, want the Authorization header verbatim", h.api.lastSessionAuthz)
	}
	if _, active, _ := h.api.counts(); active != 0 {
		t.Fatalf("CheckTokenActive calls = %d, want 0 on the bearer path", active)
	}
}

func TestCookieAuthPOST_RequiresRenderHeaderAndSameOriginOrigin(t *testing.T) {
	cases := []struct {
		name       string
		header     string
		origin     string
		fetchSite  string
		wantStatus int
	}{
		{name: "header + same-origin Origin", header: "1", origin: "self", wantStatus: http.StatusOK},
		{name: "missing render header", header: "", origin: "self", wantStatus: http.StatusForbidden},
		{name: "wrong render header value", header: "0", origin: "self", wantStatus: http.StatusForbidden},
		{name: "missing Origin", header: "1", origin: "", wantStatus: http.StatusForbidden},
		{name: "cross-origin Origin", header: "1", origin: "https://evil.example", wantStatus: http.StatusForbidden},
		{name: "cross-site Sec-Fetch-Site", header: "1", origin: "self", fetchSite: "cross-site", wantStatus: http.StatusForbidden},
		{name: "same-origin Sec-Fetch-Site", header: "1", origin: "self", fetchSite: "same-origin", wantStatus: http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newAuthHarness(t)
			h.api.tokenActive = true

			req, _ := http.NewRequest(http.MethodPost, h.appURL(""), strings.NewReader(`{}`))
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Content-Type", "application/json")
			req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: signCookie(testCookieKey, testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))})
			if tc.header != "" {
				req.Header.Set(appRenderHeader, tc.header)
			}
			switch tc.origin {
			case "":
			case "self":
				req.Header.Set("Origin", h.ts.URL)
			default:
				req.Header.Set("Origin", tc.origin)
			}
			if tc.fetchSite != "" {
				req.Header.Set("Sec-Fetch-Site", tc.fetchSite)
			}

			resp := h.do(req)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %q)", resp.StatusCode, tc.wantStatus, readBody(t, resp))
			}
			resp.Body.Close()
		})
	}
}

func TestCookieAuthGET_IsNotSubjectToCSRFChecks(t *testing.T) {
	h := newAuthHarness(t)
	h.api.tokenActive = true

	// A plain navigation: no render header, no Origin.
	resp := h.getWithCookieFor(h.appURL(""), testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestSessionCookieAuthPOST_IsSubjectToCSRFChecks(t *testing.T) {
	h := newAuthHarness(t)
	h.api.session = SessionInfo{UserID: testUserID, ProjectMember: true}

	draft := h.ts.URL + "/apps/" + testProjectID + "/" + testAppName + "@draft"
	req, _ := http.NewRequest(http.MethodPost, draft, strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", "datuplet_session=platform-session-value")
	resp := h.do(req)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 — a cookie-authenticated POST carries ambient authority", resp.StatusCode)
	}
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// Step 1 — render rate limits (fake clock)
// ---------------------------------------------------------------------------

func newRateTestServer(now func() time.Time) *Server {
	return newAuthTestServer(Config{}, &fakeAuthAPI{}, testCookieKey, now)
}

func TestAllowRender_PerPrincipalBurst10ThenThrottled(t *testing.T) {
	clk := testClockBase
	s := newRateTestServer(func() time.Time { return clk })

	pk, ak := rateKeys(principal{Kind: principalKindViewerToken, ID: testTokenID}, testAppID)

	for i := 1; i <= renderRatePerPrincipalBurst; i++ {
		ok, retry := s.allowRender(pk, ak)
		if !ok {
			t.Fatalf("render %d denied (retryAfter %d), want allowed — burst is %d", i, retry, renderRatePerPrincipalBurst)
		}
	}
	ok, retry := s.allowRender(pk, ak)
	if ok {
		t.Fatalf("render %d allowed, want denied once the burst is spent", renderRatePerPrincipalBurst+1)
	}
	if retry < 1 {
		t.Fatalf("retryAfter = %d, want >= 1", retry)
	}
}

func TestAllowRender_SixtyFirstRenderInAMinuteIsThrottled(t *testing.T) {
	clk := testClockBase
	s := newRateTestServer(func() time.Time { return clk })
	pk, ak := rateKeys(principal{Kind: principalKindViewerToken, ID: testTokenID}, testAppID)

	allowed := 0
	var lastRetry int
	for i := 1; i <= 61; i++ {
		ok, retry := s.allowRender(pk, ak)
		if ok {
			allowed++
		}
		lastRetry = retry
		if i == 61 && ok {
			t.Fatalf("the 61st render in a minute was allowed, want 429")
		}
	}
	if allowed != renderRatePerPrincipalBurst {
		t.Fatalf("allowed = %d at a frozen clock, want exactly the burst (%d)", allowed, renderRatePerPrincipalBurst)
	}
	if lastRetry < 1 {
		t.Fatalf("retryAfter on the 61st = %d, want >= 1", lastRetry)
	}
}

func TestAllowRender_SustainedRateIsHonoredAcrossTheMinute(t *testing.T) {
	clk := testClockBase
	s := newRateTestServer(func() time.Time { return clk })
	pk, ak := rateKeys(principal{Kind: principalKindPlatformUser, ID: testUserID}, testAppID)

	// 60/min sustained = one render per second: 60 paced renders must all
	// pass (the burst is headroom on top, not part of the budget).
	for i := 0; i < 60; i++ {
		if ok, retry := s.allowRender(pk, ak); !ok {
			t.Fatalf("paced render %d denied (retryAfter %d), want allowed at the sustained rate", i, retry)
		}
		clk = clk.Add(time.Second)
	}
}

func TestAllowRender_PerAppThreeHundredFirstIsThrottled(t *testing.T) {
	clk := testClockBase
	s := newRateTestServer(func() time.Time { return clk })

	// Spread across distinct principals so only the per-app bucket can be
	// the one that trips.
	for i := 0; i < renderRatePerAppPerMin; i++ {
		pk, ak := rateKeys(principal{Kind: principalKindViewerToken, ID: fmt.Sprintf("token-%d", i)}, testAppID)
		if ok, retry := s.allowRender(pk, ak); !ok {
			t.Fatalf("render %d denied (retryAfter %d), want allowed below the per-app cap", i, retry)
		}
	}
	pk, ak := rateKeys(principal{Kind: principalKindViewerToken, ID: "token-fresh"}, testAppID)
	ok, retry := s.allowRender(pk, ak)
	if ok {
		t.Fatalf("render %d allowed, want denied by the per-app bucket", renderRatePerAppPerMin+1)
	}
	if retry < 1 {
		t.Fatalf("retryAfter = %d, want >= 1", retry)
	}

	// Another app is unaffected.
	pk2, ak2 := rateKeys(principal{Kind: principalKindViewerToken, ID: "token-fresh"}, testOtherAppID)
	if ok, _ := s.allowRender(pk2, ak2); !ok {
		t.Fatal("a second app must have its own per-app bucket")
	}
}

func TestAllowRender_DenialConsumesNothing(t *testing.T) {
	clk := testClockBase
	s := newRateTestServer(func() time.Time { return clk })

	pk, ak := rateKeys(principal{Kind: principalKindViewerToken, ID: testTokenID}, testAppID)
	for i := 0; i < renderRatePerPrincipalBurst; i++ {
		if ok, _ := s.allowRender(pk, ak); !ok {
			t.Fatalf("setup render %d denied", i)
		}
	}
	// 20 denied attempts by this principal...
	for i := 0; i < 20; i++ {
		if ok, _ := s.allowRender(pk, ak); ok {
			t.Fatalf("attempt %d allowed, want denied", i)
		}
	}
	// ...must not have drained the shared per-app bucket: it still has
	// exactly (300 - 10) admissions left. Each admission uses a distinct
	// principal so only the per-app bucket can be the one that trips.
	for i := 0; i < renderRatePerAppPerMin-renderRatePerPrincipalBurst; i++ {
		other, ak2 := rateKeys(principal{Kind: principalKindViewerToken, ID: fmt.Sprintf("another-%d", i)}, testAppID)
		if ak2 != ak {
			t.Fatalf("app keys differ (%q vs %q) — the test is not exercising the shared bucket", ak, ak2)
		}
		if ok, retry := s.allowRender(other, ak2); !ok {
			t.Fatalf("shared per-app admission %d denied (retryAfter %d) — denied renders must not consume tokens", i, retry)
		}
	}
	// And the 301st app-wide admission is denied, proving the accounting is
	// exact rather than merely generous.
	last, ak3 := rateKeys(principal{Kind: principalKindViewerToken, ID: "one-too-many"}, testAppID)
	if ok, _ := s.allowRender(last, ak3); ok {
		t.Fatal("the 301st app-wide render was allowed, want denied")
	}
}

func TestAllowRender_RetryAfterIsMaxOverViolatedBucketsMinimumOne(t *testing.T) {
	clk := testClockBase
	s := newRateTestServer(func() time.Time { return clk })

	// Drain the per-app bucket with 300 distinct principals, then drain one
	// principal's own bucket too, so BOTH buckets are violated. The
	// principal bucket refills at 1/s (worst case 1s), the app bucket at
	// 5/s (0.2s) — Retry-After must be the ceil of the larger wait.
	for i := 0; i < renderRatePerAppPerMin; i++ {
		pk, ak := rateKeys(principal{Kind: principalKindViewerToken, ID: fmt.Sprintf("t-%d", i)}, testAppID)
		if ok, _ := s.allowRender(pk, ak); !ok {
			t.Fatalf("setup render %d denied", i)
		}
	}
	pk, ak := rateKeys(principal{Kind: principalKindViewerToken, ID: "t-0"}, testAppID)
	ok, retry := s.allowRender(pk, ak)
	if ok {
		t.Fatal("expected a denial with both buckets exhausted")
	}
	if retry != 1 {
		t.Fatalf("retryAfter = %d, want 1 (ceil of the max bucket wait, minimum 1)", retry)
	}

	// A principal that has spent its whole burst waits ~10s for a full
	// refill but only ~1s for the next single admission: ceil(1s) = 1.
	if retry < 1 {
		t.Fatalf("retryAfter = %d must never be below 1", retry)
	}
}

func TestRateKeys_ArePerAppAndPerPrincipalKind(t *testing.T) {
	viewer := principal{Kind: principalKindViewerToken, ID: "same-id"}
	user := principal{Kind: principalKindPlatformUser, ID: "same-id"}

	vpk, vak := rateKeys(viewer, testAppID)
	upk, uak := rateKeys(user, testAppID)
	if vpk == upk {
		t.Fatalf("a viewer token and a platform user sharing an id must not share a bucket (%q)", vpk)
	}
	if vak != uak || vak != testAppID {
		t.Fatalf("app keys = (%q,%q), want both %q", vak, uak, testAppID)
	}
	opk, oak := rateKeys(viewer, testOtherAppID)
	if opk == vpk || oak == vak {
		t.Fatal("the same principal in two apps must get separate buckets")
	}
}

// ---------------------------------------------------------------------------
// Step 1 — the plaintext token transits exactly once
// ---------------------------------------------------------------------------

func TestExchange_PlaintextTokenReachesNoLogRedirectOrReferer(t *testing.T) {
	var logBuf bytes.Buffer
	origOut, origFlags, origPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(&logBuf)
	t.Cleanup(func() {
		log.SetOutput(origOut)
		log.SetFlags(origFlags)
		log.SetPrefix(origPrefix)
	})

	h := newAuthHarness(t)
	h.api.correctSecret = testSecret

	full := h.appURL(tokenParam(testTokenID, testSecret))
	resp := h.get(full)
	dump := dumpResponse(t, resp)

	if strings.Contains(dump, testSecret) {
		t.Fatalf("the exchange response echoes the plaintext secret:\n%s", dump)
	}
	if strings.Contains(resp.Header.Get("Location"), testSecret) {
		t.Fatal("Location leaks the plaintext secret")
	}
	if resp.Header.Get("Referer") != "" {
		t.Fatal("app-worker must never emit a Referer header")
	}
	if got := resp.Header.Get("Referrer-Policy"); got != "no-referrer" {
		t.Fatalf("Referrer-Policy = %q, want no-referrer on the 302 itself", got)
	}

	// A failed exchange must be equally clean.
	h2 := newAuthHarness(t)
	h2.api.verifyOK = false
	failed := h2.jsonGet(h2.appURL(tokenParam(testTokenID, testSecret)))
	failedDump := dumpResponse(t, failed)
	if strings.Contains(failedDump, testSecret) {
		t.Fatalf("the failure envelope echoes the plaintext secret:\n%s", failedDump)
	}

	if s := logBuf.String(); strings.Contains(s, testSecret) || strings.Contains(s, "token=") {
		t.Fatalf("captured log output contains the token or a tokened URL:\n%s", s)
	}
}

func dumpResponse(t *testing.T, resp *http.Response) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "%d\n", resp.StatusCode)
	for k, vs := range resp.Header {
		for _, v := range vs {
			fmt.Fprintf(&b, "%s: %s\n", k, v)
		}
	}
	b.WriteString(readBody(t, resp))
	return b.String()
}

// ---------------------------------------------------------------------------
// Step 1 — cookie format unit tests
// ---------------------------------------------------------------------------

func TestSignCookie_ShapeIsTwoBase64URLSegments(t *testing.T) {
	exp := testClockBase.Add(sessionCookieTTL)
	v := signCookie(testCookieKey, testAppID, testTokenID, exp)

	seg, mac, ok := strings.Cut(v, ".")
	if !ok {
		t.Fatalf("cookie value %q must be <payload>.<mac>", v)
	}
	if strings.Contains(mac, ".") {
		t.Fatalf("cookie value %q must have exactly two segments", v)
	}
	raw, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		t.Fatalf("payload segment is not base64url: %v", err)
	}
	var p cookiePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("payload segment is not the JSON payload: %v", err)
	}
	if p.AppID != testAppID || p.TokenID != testTokenID || p.Exp != exp.Unix() {
		t.Fatalf("payload = %+v", p)
	}
	macBytes, err := base64.RawURLEncoding.DecodeString(mac)
	if err != nil {
		t.Fatalf("mac segment is not base64url: %v", err)
	}
	if len(macBytes) != 32 {
		t.Fatalf("mac length = %d, want 32 (HMAC-SHA256)", len(macBytes))
	}

	// Round trip.
	got, err := parseCookie(testCookieKey, v)
	if err != nil {
		t.Fatalf("parseCookie: %v", err)
	}
	if got != p {
		t.Fatalf("round trip = %+v, want %+v", got, p)
	}
}

func TestParseCookie_VerifiesMACBeforeParsingThePayload(t *testing.T) {
	// A payload segment that is valid base64url but NOT valid JSON, carrying
	// a wrong MAC. If the payload were parsed first, the error would be a
	// JSON error; because the MAC is checked first, it is a signature error.
	bad := base64.RawURLEncoding.EncodeToString([]byte(`{"app_id": <<<not json`))
	value := bad + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 32))

	_, err := parseCookie(testCookieKey, value)
	if err == nil {
		t.Fatal("expected an error")
	}
	if err != errCookieBadSignature {
		t.Fatalf("err = %v, want errCookieBadSignature (the MAC must be verified before the payload is parsed)", err)
	}
}

func TestParseCookie_KeyBound(t *testing.T) {
	v := signCookie("key-a", testAppID, testTokenID, testClockBase.Add(sessionCookieTTL))
	if _, err := parseCookie("key-b", v); err != errCookieBadSignature {
		t.Fatalf("err = %v, want errCookieBadSignature under a different key", err)
	}
	if _, err := parseCookie("key-a", v); err != nil {
		t.Fatalf("err = %v, want nil under the signing key", err)
	}
}

func TestParseCookie_RejectsEmptyFieldsEvenWhenSigned(t *testing.T) {
	// A correctly-signed but semantically empty payload must not
	// authenticate anything.
	for _, tc := range []cookiePayload{
		{AppID: "", TokenID: testTokenID, Exp: testClockBase.Unix()},
		{AppID: testAppID, TokenID: "", Exp: testClockBase.Unix()},
		{AppID: testAppID, TokenID: testTokenID, Exp: 0},
	} {
		raw, _ := json.Marshal(tc)
		seg := base64.RawURLEncoding.EncodeToString(raw)
		value := seg + "." + base64.RawURLEncoding.EncodeToString(cookieMAC(testCookieKey, seg))
		if _, err := parseCookie(testCookieKey, value); err == nil {
			t.Fatalf("payload %+v was accepted, want rejected", tc)
		}
	}
}

func TestParseViewerToken(t *testing.T) {
	id, secret, ok := parseViewerToken(viewerTokenPrefix + testTokenID + "." + testSecret)
	if !ok || id != testTokenID || secret != testSecret {
		t.Fatalf("parseViewerToken = (%q,%q,%v)", id, secret, ok)
	}
	// A secret containing a dot must survive: only the FIRST dot separates.
	id, secret, ok = parseViewerToken(viewerTokenPrefix + testTokenID + ".a.b.c")
	if !ok || id != testTokenID || secret != "a.b.c" {
		t.Fatalf("parseViewerToken with a dotted secret = (%q,%q,%v)", id, secret, ok)
	}
	for _, bad := range []string{"", "vw_", testTokenID + "." + testSecret, "vw_." + testSecret, "vw_" + testTokenID + ".", "vw_" + testTokenID} {
		if _, _, ok := parseViewerToken(bad); ok {
			t.Fatalf("parseViewerToken(%q) accepted, want rejected", bad)
		}
	}
}

// ---------------------------------------------------------------------------
// Fix round — client-IP resolution is topology-safe
// ---------------------------------------------------------------------------

func xffRequest(remoteAddr string, xff ...string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/apps/p/n", nil)
	r.RemoteAddr = remoteAddr
	for _, v := range xff {
		r.Header.Add("X-Forwarded-For", v)
	}
	return r
}

func TestResolveClientIP_NoXFFUsesRemoteAddr(t *testing.T) {
	for _, trust := range []ProxyTrust{
		{}, // nobody trusted
		{CIDRs: parseTrustedProxies("10.0.0.0/8"), Hops: 1},
	} {
		r := xffRequest("10.0.0.1:5555")
		if got := resolveClientIP(r, trust); got != "10.0.0.1" {
			t.Fatalf("trust %+v: clientIP = %q, want 10.0.0.1", trust, got)
		}
	}
}

// TestResolveClientIP_IgnoresXFFFromUntrustedPeer is THE bypass regression
// test: with no configured trusted proxies (the default), or with a peer
// outside the configured CIDRs, X-Forwarded-For must be ignored entirely.
// Honoring it would let anyone who can reach app-worker rotate the header and
// walk past the 10-failures/min per (IP, app) limiter.
func TestResolveClientIP_IgnoresXFFFromUntrustedPeer(t *testing.T) {
	cases := []struct {
		name  string
		trust ProxyTrust
	}{
		{"no trusted proxies configured (default)", ProxyTrust{}},
		{"peer outside the trusted CIDRs", ProxyTrust{CIDRs: parseTrustedProxies("10.0.0.0/8"), Hops: 1}},
		{"hops configured but no CIDRs", ProxyTrust{Hops: 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := xffRequest("198.51.100.23:4444", "1.2.3.4, 5.6.7.8")
			if got := resolveClientIP(r, tc.trust); got != "198.51.100.23" {
				t.Fatalf("clientIP = %q, want the peer 198.51.100.23 (XFF must be ignored)", got)
			}
		})
	}
}

func TestResolveClientIP_TrustedPeerSelectsFirstUntrustedFromRight(t *testing.T) {
	// Peer 10.0.0.9 is the trusted ingress; 10.0.0.8 is a second trusted
	// proxy that appears in the chain. The client is the first untrusted
	// address scanning from the right.
	trust := ProxyTrust{CIDRs: parseTrustedProxies("10.0.0.0/8"), Hops: 1}
	r := xffRequest("10.0.0.9:1", "203.0.113.7, 10.0.0.8")
	if got := resolveClientIP(r, trust); got != "203.0.113.7" {
		t.Fatalf("clientIP = %q, want 203.0.113.7", got)
	}

	// Single trusted hop, single chain entry: the rightmost entry is the
	// client.
	r = xffRequest("10.0.0.9:1", "203.0.113.7")
	if got := resolveClientIP(r, trust); got != "203.0.113.7" {
		t.Fatalf("clientIP = %q, want 203.0.113.7", got)
	}
}

func TestResolveClientIP_AttackerPrependedEntriesDoNotShiftSelection(t *testing.T) {
	trust := ProxyTrust{CIDRs: parseTrustedProxies("10.0.0.0/8"), Hops: 1}

	base := resolveClientIP(xffRequest("10.0.0.9:1", "203.0.113.7"), trust)
	if base != "203.0.113.7" {
		t.Fatalf("baseline clientIP = %q, want 203.0.113.7", base)
	}

	// The attacker controls everything it sends, so it prepends junk — in one
	// header and split across several, with spoofed trusted-looking entries
	// too. Scanning from the right makes all of it irrelevant.
	for _, spoof := range []([]string){
		{"9.9.9.9, 203.0.113.7"},
		{"1.1.1.1, 2.2.2.2, 3.3.3.3, 203.0.113.7"},
		{"10.0.0.250, 203.0.113.7"},
		{"1.1.1.1", "2.2.2.2, 203.0.113.7"},
		{"not-an-ip, 203.0.113.7"},
	} {
		if got := resolveClientIP(xffRequest("10.0.0.9:1", spoof...), trust); got != base {
			t.Fatalf("XFF %v: clientIP = %q, want the unchanged %q", spoof, got, base)
		}
	}
}

func TestResolveClientIP_TwoHopConfiguration(t *testing.T) {
	// client -> CDN (address not enumerable) -> ingress 10.0.0.9 -> worker.
	// The ingress appended the CDN; the CDN set the client. Hops=2 skips the
	// one non-enumerable trusted hop at the right end.
	trust := ProxyTrust{CIDRs: parseTrustedProxies("10.0.0.9"), Hops: 2}
	r := xffRequest("10.0.0.9:1", "203.0.113.7, 198.51.100.50")
	if got := resolveClientIP(r, trust); got != "203.0.113.7" {
		t.Fatalf("clientIP = %q, want 203.0.113.7 (the CDN hop must be skipped)", got)
	}

	// Prepending more entries still cannot shift the selection.
	r = xffRequest("10.0.0.9:1", "9.9.9.9, 8.8.8.8, 203.0.113.7, 198.51.100.50")
	if got := resolveClientIP(r, trust); got != "203.0.113.7" {
		t.Fatalf("clientIP with prepended junk = %q, want 203.0.113.7", got)
	}

	// A chain shorter than the configured hop count falls back to the peer
	// rather than trusting whatever is left.
	r = xffRequest("10.0.0.9:1", "203.0.113.7")
	if got := resolveClientIP(r, trust); got != "10.0.0.9" {
		t.Fatalf("clientIP with an exhausted chain = %q, want the peer 10.0.0.9", got)
	}
}

func TestResolveClientIP_NormalizesAndRejectsGarbage(t *testing.T) {
	trust := ProxyTrust{CIDRs: parseTrustedProxies("10.0.0.0/8"), Hops: 1}

	// A selected entry that is not a parseable IP falls back to the peer: an
	// attacker-chosen string must never become a fresh rate-bucket key.
	if got := resolveClientIP(xffRequest("10.0.0.9:1", "totally-bogus"), trust); got != "10.0.0.9" {
		t.Fatalf("clientIP = %q, want the peer for an unparseable entry", got)
	}
	// host:port and IPv4-in-IPv6 forms canonicalize, so they cannot become
	// two buckets for one client.
	if got := resolveClientIP(xffRequest("10.0.0.9:1", "203.0.113.7:8080"), trust); got != "203.0.113.7" {
		t.Fatalf("clientIP = %q, want 203.0.113.7", got)
	}
	if got := resolveClientIP(xffRequest("10.0.0.9:1", "::ffff:203.0.113.7"), trust); got != "203.0.113.7" {
		t.Fatalf("clientIP = %q, want the unmapped 203.0.113.7", got)
	}
}

// TestExchange_XFFCannotBypassTheFailureLimiterFromAnUntrustedPeer is the
// HTTP-level form of the bypass regression: rotating X-Forwarded-For against
// a worker with no configured trusted proxies must NOT mint a fresh failure
// bucket per attempt — the limiter still buckets by RemoteAddr.
func TestExchange_XFFCannotBypassTheFailureLimiterFromAnUntrustedPeer(t *testing.T) {
	h := newAuthHarness(t) // Config{} — no trusted proxies
	h.api.verifyOK = false

	attempt := func(i int) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, h.appURL(tokenParam(testTokenID, testSecret)), nil)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", i))
		return h.do(req)
	}

	for i := 1; i <= verifyFailuresPerMin; i++ {
		resp := attempt(i)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("attempt %d: status = %d, want 403", i, resp.StatusCode)
		}
		resp.Body.Close()
	}
	resp := attempt(verifyFailuresPerMin + 1)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("attempt 11 with a fresh spoofed XFF: status = %d, want 429 — "+
			"an untrusted peer must not be able to pick its own rate bucket", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "60" {
		t.Fatalf("Retry-After = %q, want 60", got)
	}
	resp.Body.Close()
	if verify, _, _ := h.api.counts(); verify != verifyFailuresPerMin {
		t.Fatalf("VerifyToken calls = %d, want %d", verify, verifyFailuresPerMin)
	}
}

// ---------------------------------------------------------------------------
// Fix round — the verify-failure limiter is atomic (no TOCTOU)
// ---------------------------------------------------------------------------

// TestExchange_ConcurrentInvalidExchangesCannotExceedTheFailureBudget is the
// TOCTOU regression test. The proof is the count of calls that reach the
// upstream fake: with a peek-then-spend limiter, N concurrent invalid
// exchanges all observe capacity before any failure is recorded and all N
// reach pipeline-api.
func TestExchange_ConcurrentInvalidExchangesCannotExceedTheFailureBudget(t *testing.T) {
	h := newAuthHarness(t)
	h.api.verifyOK = false

	const attempts = 50
	// The barrier releases as soon as one more than the budget is inside the
	// fake concurrently — which can only happen if the limiter leaked. When
	// the limiter holds, the blocked calls fall through the short timeout.
	const barrier = verifyFailuresPerMin + 1
	var inside int32
	release := make(chan struct{})
	var once sync.Once
	h.api.verifyHook = func() {
		if atomic.AddInt32(&inside, 1) >= barrier {
			once.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-time.After(250 * time.Millisecond):
		}
	}

	// Resolve the client ONCE: h.client() mutates the shared
	// httptest.Server client's CheckRedirect, which is a data race if every
	// goroutine does it.
	client := h.client()

	var wg sync.WaitGroup
	statuses := make([]int, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodGet, h.appURL(tokenParam(testTokenID, testSecret)), nil)
			if err != nil {
				return
			}
			req.Header.Set("Accept", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			statuses[i] = resp.StatusCode
			resp.Body.Close()
		}(i)
	}
	wg.Wait()

	verify, _, _ := h.api.counts()
	if verify > verifyFailuresPerMin {
		t.Fatalf("upstream VerifyToken calls = %d for %d concurrent invalid exchanges, "+
			"want at most %d — the failure budget must be taken atomically",
			verify, attempts, verifyFailuresPerMin)
	}

	var forbidden, throttled int
	for _, s := range statuses {
		switch s {
		case http.StatusForbidden:
			forbidden++
		case http.StatusTooManyRequests:
			throttled++
		}
	}
	if forbidden+throttled != attempts {
		t.Fatalf("statuses: 403=%d 429=%d, want %d total", forbidden, throttled, attempts)
	}
	if forbidden > verifyFailuresPerMin {
		t.Fatalf("403 responses = %d, want at most %d", forbidden, verifyFailuresPerMin)
	}
	if throttled == 0 {
		t.Fatal("no request was throttled, so the limiter never engaged")
	}
}

func TestExchange_SuccessConsumesNoFailureBudget(t *testing.T) {
	h := newAuthHarness(t)
	h.api.correctSecret = testSecret

	// Far more successful exchanges than the failure budget: none may spend
	// a failure token, or a popular app would throttle its own viewers.
	for i := 0; i < verifyFailuresPerMin*3; i++ {
		resp := h.get(h.appURL(tokenParam(testTokenID, testSecret)))
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("exchange %d: status = %d, want 302", i, resp.StatusCode)
		}
		resp.Body.Close()
	}
	// The budget is still intact: 10 genuine failures remain available
	// before the 11th is throttled.
	h.api.mu.Lock()
	h.api.correctSecret = ""
	h.api.verifyOK = false
	h.api.mu.Unlock()

	for i := 1; i <= verifyFailuresPerMin; i++ {
		resp := h.jsonGet(h.appURL(tokenParam(testTokenID, testSecret)))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("failure %d: status = %d, want 403 — successes must not have spent the budget", i, resp.StatusCode)
		}
		resp.Body.Close()
	}
	resp := h.jsonGet(h.appURL(tokenParam(testTokenID, testSecret)))
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 on the 11th failure", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestExchange_UpstreamErrorConsumesNoFailureBudget(t *testing.T) {
	h := newAuthHarness(t)
	h.api.verifyErr = fmt.Errorf("pipeline-api down")

	// An outage is not a viewer's fault: many 503s must not exhaust the
	// (IP, app) failure budget.
	for i := 0; i < verifyFailuresPerMin*3; i++ {
		resp := h.jsonGet(h.appURL(tokenParam(testTokenID, testSecret)))
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("attempt %d: status = %d, want 503", i, resp.StatusCode)
		}
		resp.Body.Close()
	}

	h.api.mu.Lock()
	h.api.verifyErr = nil
	h.api.verifyOK = false
	h.api.mu.Unlock()

	for i := 1; i <= verifyFailuresPerMin; i++ {
		resp := h.jsonGet(h.appURL(tokenParam(testTokenID, testSecret)))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("failure %d: status = %d, want 403 — 503s must not have spent the budget", i, resp.StatusCode)
		}
		resp.Body.Close()
	}
	resp := h.jsonGet(h.appURL(tokenParam(testTokenID, testSecret)))
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 on the 11th genuine failure", resp.StatusCode)
	}
	resp.Body.Close()
}
