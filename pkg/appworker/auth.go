package appworker

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ---------------------------------------------------------------------------
// Viewer auth (RFC 028 spec §5.3, contract-and-constraints.md "app-worker")
//
// This file is the security boundary between the public internet and an app's
// data grant. Three credential channels exist, and no other:
//
//  1. the one-time `?token=vw_<id>.<secret>` exchange — the ONLY place a
//     plaintext viewer token ever transits; it is traded for a signed cookie
//     and a 302 to the token-free URL, so it never survives in the address
//     bar, history, an access log, or a Referer;
//  2. the signed session cookie (`datuplet_app_session`), which carries NO
//     secret — only `{app_id, token_id, exp}` under an HMAC — and which is
//     re-checked for revocation against pipeline-api on every request;
//  3. a platform credential (bearer api-token, or a platform session cookie)
//     verified by pipeline-api's sessions/verify — the only channel the
//     `@draft` channel accepts at all.
//
// Every failure path fails CLOSED. Every unauthenticated request gets a
// response written by authenticate itself.
// ---------------------------------------------------------------------------

// principal is who app-worker decided the caller is. Kind is one of
// principalKindViewerToken / principalKindPlatformUser; ID is the viewer
// token_id or the platform user_id respectively — never a secret. Both feed
// the render access log (spec §9's audit layer 2) and the per-principal rate
// bucket (spec §7).
type principal struct {
	Kind string
	ID   string
}

const (
	principalKindViewerToken  = "viewer_token"
	principalKindPlatformUser = "platform_user"
)

// Channel names as they appear in the resolve call and the render log.
const (
	channelProduction = "production"
	channelDraft      = "draft"
)

// resolvedApp is what the render path knows BEFORE authenticating — spec
// §4.2 step 1 is explicit that resolve must precede authentication, because
// viewer-token verification and the cookie↔app binding are both keyed by the
// resolved app_id (never by anything the client supplied).
//
// Name is the bare app name with no `@draft` suffix: it is the cookie's Path
// scope, so it must be identical for both channels of the same app.
type resolvedApp struct {
	ProjectID   string
	Name        string
	Channel     string
	AppID       string
	VersionHash string
}

func (ra resolvedApp) isDraft() bool { return ra.Channel == channelDraft }

// cookiePath is the session cookie's Path attribute:
// `/apps/{pid}/{name}` (contract-and-constraints.md). Path scoping is a
// convenience that keeps the cookie off unrelated apps' requests — the
// authorization control is the `cookie.app_id == resolved.AppID` comparison
// in authenticate, never the path (spec §5.3, §9).
func (ra resolvedApp) cookiePath() string {
	return "/apps/" + ra.ProjectID + "/" + ra.Name
}

// authAPI is the subset of *APIClient (W3) that viewer auth depends on.
// Declared as an interface so tests can inject a double; *APIClient
// satisfies it structurally.
type authAPI interface {
	VerifyToken(ctx context.Context, appID, tokenID, secret string) (bool, error)
	CheckTokenActive(ctx context.Context, appID, tokenID string) (bool, error)
	VerifySession(ctx context.Context, pid, cookies, authz string) (SessionInfo, error)
}

// Cookie + exchange constants, fixed by contract-and-constraints.md and
// spec §5.3.
const (
	// sessionCookieName is the viewer session cookie's name.
	sessionCookieName = "datuplet_app_session"
	// sessionCookieTTL is the cookie's lifetime (~24 h per spec §5.3).
	sessionCookieTTL = 24 * time.Hour

	// viewerTokenPrefix prefixes every plaintext viewer token:
	// `vw_<token_id>.<secret>`.
	viewerTokenPrefix = "vw_"
	// tokenQueryParam is the one-time exchange's query parameter. It is one
	// of the two reserved param names stripped before the guest sees
	// ctx.params (spec §6.5) — the other is `block`.
	tokenQueryParam = "token"

	// appRenderHeader is the shell's custom CSRF header (spec §5.3).
	appRenderHeader      = "X-Datuplet-App-Render"
	appRenderHeaderValue = "1"
)

// Rate limits, spec §7 verbatim.
const (
	// renderRatePerPrincipalPerMin / Burst: 60 renders/min sustained, burst
	// 10, keyed (app_id, token_id) or (app_id, user_id).
	renderRatePerPrincipalPerMin = 60
	renderRatePerPrincipalBurst  = 10
	// renderRatePerAppPerMin: 300 renders/min, keyed app_id. Spec §7 states
	// no separate burst for this bucket, so the burst equals the per-minute
	// budget — the bucket admits at most 300 in any minute however they
	// arrive, which is exactly what "300/min" means without a second knob.
	renderRatePerAppPerMin = 300

	// verifyFailuresPerMin: 10 verify failures/min per (client IP, app) →
	// 429 with Retry-After: 60 (spec §5.3). Since W3's negative cache
	// deliberately does NOT throttle wrong-secret attempts against an
	// ACTIVE token (doing so by (app_id, token_id) would let one wrong
	// guess lock out the legitimate holder), THIS limiter is the only
	// control bounding brute force against a live token id.
	verifyFailuresPerMin     = 10
	verifyFailureRetryAfterS = 60
)

// ---------------------------------------------------------------------------
// Cookie format
//
// value = base64url(json_payload) + "." + base64url(hmac)
//
//	json_payload = {"app_id","token_id","exp"}
//	hmac         = HMAC-SHA256(key, base64url(json_payload))
//
// The payload is JSON inside its own base64url segment specifically so the
// binary MAC never has to be `|`-split out of a delimiter-joined string:
// there is exactly one delimiter, and it separates two base64url-safe
// segments (base64url's alphabet excludes `.`). Do not "simplify" this back
// into a delimiter-split format.
// ---------------------------------------------------------------------------

type cookiePayload struct {
	AppID   string `json:"app_id"`
	TokenID string `json:"token_id"`
	Exp     int64  `json:"exp"`
}

var (
	// errCookieMalformed: the value isn't two base64url segments.
	errCookieMalformed = errors.New("appworker: malformed session cookie")
	// errCookieBadSignature: the MAC does not authenticate the payload
	// segment. Returned for ANY bad-MAC input — including one whose payload
	// segment is not even valid base64/JSON — because the MAC is verified
	// before the payload is decoded or parsed.
	errCookieBadSignature = errors.New("appworker: session cookie signature mismatch")
	// errCookiePayload: the payload authenticated but is not a usable
	// session (missing app_id/token_id/exp).
	errCookiePayload = errors.New("appworker: session cookie payload incomplete")
)

// cookieMAC computes HMAC-SHA256(key, segment) over the base64url payload
// SEGMENT (the encoded text), not the decoded JSON.
func cookieMAC(key, segment string) []byte {
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(segment))
	return m.Sum(nil)
}

// signCookie mints the session cookie value for (appID, tokenID, exp).
func signCookie(key, appID, tokenID string, exp time.Time) string {
	// cookiePayload marshals without error (three scalar fields).
	raw, _ := json.Marshal(cookiePayload{AppID: appID, TokenID: tokenID, Exp: exp.Unix()})
	segment := base64.RawURLEncoding.EncodeToString(raw)
	return segment + "." + base64.RawURLEncoding.EncodeToString(cookieMAC(key, segment))
}

// parseCookie authenticates a cookie value and returns its payload.
//
// Order is load-bearing: the MAC is recomputed over the RECEIVED first
// segment and compared with hmac.Equal (constant time) BEFORE the payload is
// base64-decoded or JSON-parsed. Untrusted JSON is never handed to the
// decoder until it has been authenticated.
//
// parseCookie does NOT check expiry or the app binding — those need the
// server's clock and the resolved app, and live in authenticate. Callers must
// not treat a nil error as "authorized".
func parseCookie(key, value string) (cookiePayload, error) {
	segment, macSegment, ok := strings.Cut(value, ".")
	if !ok || segment == "" || macSegment == "" || strings.Contains(macSegment, ".") {
		return cookiePayload{}, errCookieMalformed
	}

	mac, err := base64.RawURLEncoding.DecodeString(macSegment)
	if err != nil {
		return cookiePayload{}, errCookieMalformed
	}
	if !hmac.Equal(mac, cookieMAC(key, segment)) {
		return cookiePayload{}, errCookieBadSignature
	}

	// Authenticated from here on.
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		return cookiePayload{}, errCookieMalformed
	}
	var p cookiePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return cookiePayload{}, errCookieMalformed
	}
	if p.AppID == "" || p.TokenID == "" || p.Exp == 0 {
		return cookiePayload{}, errCookiePayload
	}
	return p, nil
}

// parseViewerToken splits `vw_<token_id>.<secret>`. Only the FIRST dot after
// the prefix separates, so a secret containing dots survives intact. Returns
// ok=false for anything else — callers must map that to the same 403 an
// unknown token gets, so the response is not an oracle for "well-formed but
// unknown" vs "malformed".
func parseViewerToken(raw string) (tokenID, secret string, ok bool) {
	rest, found := strings.CutPrefix(raw, viewerTokenPrefix)
	if !found {
		return "", "", false
	}
	tokenID, secret, found = strings.Cut(rest, ".")
	if !found || tokenID == "" || secret == "" {
		return "", "", false
	}
	return tokenID, secret, true
}

// clientIP is the per-(IP, app) verify-failure bucket's key, resolved under
// this server's configured proxy trust.
func (s *Server) clientIP(r *http.Request) string {
	return resolveClientIP(r, s.cfg.TrustedProxies)
}

// resolveClientIP determines the client address for rate-limit keying.
//
// X-Forwarded-For is entirely client-supplied unless something trustworthy
// rewrote it, so the rule is peer-anchored and fails toward "believe less":
//
//  1. The client IP is the immediate peer (`r.RemoteAddr`, port stripped).
//  2. If no trusted-proxy CIDRs are configured, stop. XFF is ignored
//     completely — this is the default, and it is what keeps the
//     10-failures/min per (IP, app) limiter (spec §5.3) from being bypassable
//     by rotating a header when app-worker is reachable directly.
//  3. If the peer is not inside a trusted CIDR, stop. A connection we did not
//     put there does not get to describe itself.
//  4. The peer IS a trusted proxy. Scan the XFF chain from the RIGHT — the
//     right-hand end is what our own infrastructure appended, so entries an
//     attacker prepended on the left can never shift the selection:
//     a. skip `Hops-1` rightmost entries (trusted hops in front of the peer
//     whose addresses we cannot enumerate, e.g. a CDN);
//     b. then keep skipping entries that are themselves trusted CIDRs;
//     c. the first remaining entry is the client — the first UNTRUSTED
//     address scanning from the right.
//  5. If the chain is exhausted, or the selected entry is not a parseable IP,
//     fall back to the peer. A garbage entry must never become a cache key
//     (unbounded key space) nor a fresh, attacker-chosen bucket.
//
// Topology config (`DATUPLET_APPWORKER_TRUSTED_PROXIES` /
// `_TRUSTED_PROXY_HOPS`, see ProxyTrust) is set by the chart in Part 7 (D1)
// once D0 establishes whether traffic terminates at the cluster ingress, at a
// reverse proxy inside pipeline-api, or directly in app-worker. The safe
// default until then is no trusted proxies.
func resolveClientIP(r *http.Request, trust ProxyTrust) string {
	peer := r.RemoteAddr
	if host, _, err := net.SplitHostPort(peer); err == nil {
		peer = host
	}
	if ip, ok := normalizeIP(peer); ok {
		peer = ip
	}

	if !trust.Enabled() || !trust.Contains(peer) {
		return peer
	}

	var chain []string
	for _, values := range r.Header.Values("X-Forwarded-For") {
		for _, part := range strings.Split(values, ",") {
			if part = strings.TrimSpace(part); part != "" {
				chain = append(chain, part)
			}
		}
	}

	i := len(chain) - 1
	for skip := trust.Hops - 1; skip > 0 && i >= 0; skip, i = skip-1, i-1 {
	}
	for i >= 0 && trust.Contains(canonicalOrRaw(chain[i])) {
		i--
	}
	if i < 0 {
		return peer
	}
	ip, ok := normalizeIP(chain[i])
	if !ok {
		return peer
	}
	return ip
}

// normalizeIP canonicalizes an XFF entry (or a peer address) to a bare IP
// string, accepting the `host:port` and `[v6]:port` forms some proxies emit.
// Canonicalizing means `::ffff:1.2.3.4` and `1.2.3.4` cannot become two
// separate rate buckets.
func normalizeIP(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	if a, err := netip.ParseAddr(strings.Trim(s, "[]")); err == nil {
		return a.Unmap().String(), true
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		if a, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
			return a.Unmap().String(), true
		}
	}
	return "", false
}

func canonicalOrRaw(s string) string {
	if ip, ok := normalizeIP(s); ok {
		return ip
	}
	return s
}

// ---------------------------------------------------------------------------
// authenticate
// ---------------------------------------------------------------------------

// authenticate decides who the caller is for the already-resolved app.
//
// Contract with the caller (W5/W6): when ok is false, authenticate has
// ALREADY written the complete response — either an error envelope or the 302
// exchange redirect — and the caller must return immediately without writing
// anything further. When ok is true, nothing has been written except
// possibly a Set-Cookie header.
//
// Channel rules:
//   - `@draft` accepts a platform credential and NOTHING else. A `?token=`
//     on a draft URL is rejected outright, and a viewer session cookie is
//     never even looked at.
//   - production accepts, in this order: the one-time `?token=` exchange, a
//     bearer platform credential (the CLI path), then the session cookie.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request, resolved resolvedApp) (principal, bool) {
	// A pod without its auth dependencies wired must never authenticate
	// anybody. An empty HMAC key in particular is catastrophic: HMAC with an
	// empty key is still a well-defined MAC, so any attacker who guessed the
	// key was unset could forge cookies at will.
	if s.api == nil || s.cookieKey == "" {
		s.fail(w, r, http.StatusServiceUnavailable, errKindUnavailable, "app-worker: viewer auth is not configured")
		return principal{}, false
	}

	tokenParam := r.URL.Query().Get(tokenQueryParam)

	if resolved.isDraft() {
		if tokenParam != "" {
			// Never verified, never exchanged, never logged.
			s.fail(w, r, http.StatusForbidden, errKindUnauthorized,
				"viewer tokens are not accepted on the draft channel")
			return principal{}, false
		}
		return s.authenticatePlatform(w, r, resolved)
	}

	if tokenParam != "" {
		return s.exchangeViewerToken(w, r, resolved, tokenParam)
	}

	if r.Header.Get("Authorization") != "" {
		return s.authenticatePlatform(w, r, resolved)
	}

	return s.authenticateCookie(w, r, resolved)
}

// exchangeViewerToken performs the one-time `?token=` exchange: verify, set
// the signed cookie, 302 to the token-free URL. It NEVER returns ok=true —
// the redirect is the response, and the viewer's next request authenticates
// by cookie. This is the only code path that ever sees a plaintext secret,
// and it neither logs it nor echoes it into any header or body.
func (s *Server) exchangeViewerToken(w http.ResponseWriter, r *http.Request, resolved resolvedApp, raw string) (principal, bool) {
	ip := s.clientIP(r)

	// Anti-hammering FIRST, and atomically: one admission is taken here,
	// before the parse and before any round trip, so an exhausted budget
	// costs pipeline-api/Postgres nothing (spec §5.3 — "the failure path must
	// not hammer Postgres") and concurrent attempts cannot all slip through
	// a check-then-act window.
	budget, ok := s.reserveVerifyAttempt(ip, resolved.AppID)
	if !ok {
		s.failRateLimited(w, r, verifyFailureRetryAfterS)
		return principal{}, false
	}

	tokenID, secret, ok := parseViewerToken(raw)
	if !ok {
		// A genuine failure: keep the admission.
		s.failViewerToken(w, r)
		return principal{}, false
	}

	valid, err := s.api.VerifyToken(r.Context(), resolved.AppID, tokenID, secret)
	if err != nil {
		// pipeline-api unreachable (or app-worker's own service credential
		// is wrong): fail closed as `unavailable`, never as a viewer denial
		// — and refund, since our own outage must not consume a viewer's
		// failure budget.
		budget.refund()
		s.fail(w, r, http.StatusServiceUnavailable, errKindUnavailable, "app-worker: cannot verify viewer token")
		return principal{}, false
	}
	if !valid {
		// A genuine failure: keep the admission.
		s.failViewerToken(w, r)
		return principal{}, false
	}

	// Success costs nothing.
	budget.refund()

	exp := s.now().Add(sessionCookieTTL)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    signCookie(s.cookieKey, resolved.AppID, tokenID, exp),
		Path:     resolved.cookiePath(),
		MaxAge:   int(sessionCookieTTL / time.Second),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	// Relative target (path + surviving query only) — never an absolute URL
	// built from client-controlled input, so this can't become an open
	// redirect.
	http.Redirect(w, r, tokenFreeTarget(r), http.StatusFound)
	return principal{}, false
}

// tokenFreeTarget rebuilds the current request's path with the reserved
// `token` param removed, so the plaintext leaves the address bar, the
// browser history, and every downstream log the moment the exchange
// completes.
func tokenFreeTarget(r *http.Request) string {
	q := r.URL.Query()
	q.Del(tokenQueryParam)
	target := r.URL.EscapedPath()
	if len(q) > 0 {
		target += "?" + q.Encode()
	}
	return target
}

// authenticateCookie is the steady-state viewer path. The cookie carries no
// secret, so the checks are: MAC → app binding → expiry → CSRF (POST only) →
// revocation recheck against pipeline-api.
func (s *Server) authenticateCookie(w http.ResponseWriter, r *http.Request, resolved resolvedApp) (principal, bool) {
	c, err := r.Cookie(sessionCookieName)
	if err != nil || c.Value == "" {
		s.fail(w, r, http.StatusUnauthorized, errKindUnauthorized, "no viewer session")
		return principal{}, false
	}

	payload, err := parseCookie(s.cookieKey, c.Value)
	if err != nil {
		s.fail(w, r, http.StatusUnauthorized, errKindUnauthorized, "invalid viewer session")
		return principal{}, false
	}

	// The mandatory cookie↔app binding (spec §5.3, §9). Path scoping is a
	// convenience; THIS is the control. A cookie minted for app A is worth
	// nothing against app B.
	if payload.AppID != resolved.AppID {
		s.fail(w, r, http.StatusUnauthorized, errKindUnauthorized, "invalid viewer session")
		return principal{}, false
	}

	if !s.now().Before(time.Unix(payload.Exp, 0)) {
		s.fail(w, r, http.StatusUnauthorized, errKindUnauthorized, "viewer session expired")
		return principal{}, false
	}

	// A cookie is ambient authority, so an unsafe method needs the CSRF
	// controls. Checked before the upstream revocation call so cross-site
	// spam costs pipeline-api nothing.
	if !s.checkCSRF(w, r) {
		return principal{}, false
	}

	// Revocation recheck on EVERY cookie-authenticated request (spec §5.3).
	// The cookie carries no secret, so VerifyToken cannot be used here —
	// CheckTokenActive answers "is this (app_id, token_id) row still live"
	// and is what makes a deleted viewer token kill its sessions within the
	// ≤15 s cache bound instead of the cookie's full 24 h.
	active, err := s.api.CheckTokenActive(r.Context(), resolved.AppID, payload.TokenID)
	if err != nil {
		s.fail(w, r, http.StatusServiceUnavailable, errKindUnavailable, "app-worker: cannot verify viewer session")
		return principal{}, false
	}
	if !active {
		s.fail(w, r, http.StatusUnauthorized, errKindUnauthorized, "viewer session revoked")
		return principal{}, false
	}

	return principal{Kind: principalKindViewerToken, ID: payload.TokenID}, true
}

// authenticatePlatform verifies a platform credential through pipeline-api's
// sessions/verify (spec §5.2) — app-worker holds no session-validation logic
// of its own. Both the bearer (CLI, §5.5) and the platform session cookie
// (UI `@draft` preview) arrive here; the bearer variant is exempt from the
// browser CSRF checks because it carries no ambient authority.
func (s *Server) authenticatePlatform(w http.ResponseWriter, r *http.Request, resolved resolvedApp) (principal, bool) {
	authz := r.Header.Get("Authorization")
	cookies := r.Header.Get("Cookie")
	if authz == "" && cookies == "" {
		s.fail(w, r, http.StatusUnauthorized, errKindUnauthorized, "authentication required")
		return principal{}, false
	}

	info, err := s.api.VerifySession(r.Context(), resolved.ProjectID, cookies, authz)
	if err != nil {
		s.fail(w, r, http.StatusServiceUnavailable, errKindUnavailable, "app-worker: cannot verify session")
		return principal{}, false
	}
	if info.UserID == "" {
		s.fail(w, r, http.StatusUnauthorized, errKindUnauthorized, "authentication required")
		return principal{}, false
	}
	if !info.ProjectMember {
		s.fail(w, r, http.StatusForbidden, errKindUnauthorized, "not a member of this project")
		return principal{}, false
	}

	// Cookie-only authority ⇒ CSRF applies. A bearer credential cannot be
	// attached by a cross-site attacker, so it is exempt (spec §5.3).
	if authz == "" && !s.checkCSRF(w, r) {
		return principal{}, false
	}

	return principal{Kind: principalKindPlatformUser, ID: info.UserID}, true
}

// checkCSRF enforces spec §5.3's controls on cookie-authenticated unsafe
// methods: the shell's custom header AND a same-origin Origin, plus a
// Sec-Fetch-Site sanity check when the browser supplies one. It writes the
// 403 itself and returns false on rejection.
//
// Safe methods are exempt: SameSite=Lax already blocks the cross-site
// cookie send that would make a state-changing GET possible, and a
// navigation carries no Origin at all.
func (s *Server) checkCSRF(w http.ResponseWriter, r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}

	if r.Header.Get(appRenderHeader) != appRenderHeaderValue {
		s.fail(w, r, http.StatusForbidden, errKindUnauthorized, "missing app-render header")
		return false
	}
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" {
		s.fail(w, r, http.StatusForbidden, errKindUnauthorized, "cross-site render request")
		return false
	}
	// A same-origin fetch always sends Origin on a POST, so a missing Origin
	// is treated as a failure rather than waved through.
	if origin := r.Header.Get("Origin"); origin == "" || origin != selfOrigin(r) {
		s.fail(w, r, http.StatusForbidden, errKindUnauthorized, "cross-site render request")
		return false
	}
	return true
}

// selfOrigin reconstructs this request's own origin for the Origin
// comparison. Behind the cluster ingress the TLS termination is upstream, so
// X-Forwarded-Proto is authoritative when present.
func selfOrigin(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = strings.TrimSpace(strings.Split(proto, ",")[0])
	}
	return scheme + "://" + r.Host
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

// limiterRegistry is a keyed set of token buckets sharing one limit/burst and
// one injected clock (so every TTL/rate assertion is fake-clock driven, never
// a sleep). Idle buckets are swept: a bucket untouched for longer than its
// own full-refill time is indistinguishable from a fresh one, so dropping it
// loses no state while keeping the map from growing without bound across a
// long-lived pod's key space.
type limiterRegistry struct {
	mu      sync.Mutex
	limit   rate.Limit
	burst   int
	idleTTL time.Duration
	now     func() time.Time
	entries map[string]*limiterEntry
}

type limiterEntry struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// limiterSweepThreshold is the entry count above which a sweep runs before a
// lookup. Below it the map is small enough that sweeping is pure overhead.
const limiterSweepThreshold = 1024

func newLimiterRegistry(perMinute, burst int, now func() time.Time) *limiterRegistry {
	limit := rate.Limit(float64(perMinute) / 60.0)
	// A bucket idle for its own full-refill time is back to full, so it
	// carries no information: safe to evict. Never less than a minute, so a
	// bucket whose window is a minute (the verify-failure bucket) is never
	// dropped mid-window.
	idle := time.Duration(float64(burst)/float64(limit)) * time.Second
	if idle < time.Minute {
		idle = time.Minute
	}
	return &limiterRegistry{
		limit:   limit,
		burst:   burst,
		idleTTL: idle,
		now:     now,
		entries: make(map[string]*limiterEntry),
	}
}

func (reg *limiterRegistry) limiter(key string) *rate.Limiter {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	now := reg.now()
	if len(reg.entries) >= limiterSweepThreshold {
		for k, e := range reg.entries {
			if now.Sub(e.lastSeen) >= reg.idleTTL {
				delete(reg.entries, k)
			}
		}
	}
	e, ok := reg.entries[key]
	if !ok {
		e = &limiterEntry{lim: rate.NewLimiter(reg.limit, reg.burst)}
		reg.entries[key] = e
	}
	e.lastSeen = now
	return e.lim
}

// rateKeys derives the two render-rate bucket keys for a principal in an app:
// per-principal `(app_id, kind, id)` and per-app `app_id` (spec §7). The kind
// is part of the principal key so a viewer token_id can never share a bucket
// with a platform user_id that happens to have the same string value.
func rateKeys(p principal, appID string) (principalKey, appKey string) {
	return appID + "\x00" + p.Kind + "\x00" + p.ID, appID
}

// allowRender applies both render-rate buckets atomically-enough: it reserves
// one admission in each, and if EITHER would have to wait, it cancels both
// reservations and reports the wait. A denied render therefore consumes
// nothing from either bucket — a principal being throttled must not eat the
// app's shared budget, and vice versa.
//
// retryAfter is ceil(max over the violated buckets of the seconds until that
// bucket admits one render), minimum 1 (spec §5.3).
func (s *Server) allowRender(principalKey, appKey string) (ok bool, retryAfter int) {
	now := s.now()

	principalRes := s.renderPrincipalLimits.limiter(principalKey).ReserveN(now, 1)
	appRes := s.renderAppLimits.limiter(appKey).ReserveN(now, 1)

	principalWait := reservationWait(principalRes, now)
	appWait := reservationWait(appRes, now)

	if principalWait <= 0 && appWait <= 0 {
		return true, 0
	}

	principalRes.CancelAt(now)
	appRes.CancelAt(now)

	worst := principalWait
	if appWait > worst {
		worst = appWait
	}
	return false, retryAfterSeconds(worst)
}

// reservationWait is how long a reservation must wait before acting. An
// impossible reservation (n above the burst — not reachable with n=1, but
// handled rather than assumed) counts as a full window.
func reservationWait(res *rate.Reservation, now time.Time) time.Duration {
	if !res.OK() {
		return time.Minute
	}
	return res.DelayFrom(now)
}

func retryAfterSeconds(d time.Duration) int {
	n := int(math.Ceil(d.Seconds()))
	if n < 1 {
		n = 1
	}
	return n
}

// verifyKey is the anti-hammering bucket's key: (client IP, app).
func verifyKey(ip, appID string) string { return ip + "\x00" + appID }

// verifyBudget is one admission taken from the (IP, app) verify-failure
// budget. Holding it means the token is ALREADY spent; refund gives it back.
type verifyBudget struct {
	res *rate.Reservation
	// at is the instant the reservation was taken. refund must cancel with
	// this same instant, not "now": rate.Reservation.CancelAt refuses to
	// restore a reservation whose timeToAct is already in the past, so
	// cancelling with a later clock reading (a real wall clock will have
	// advanced across the pipeline-api round trip) would silently refund
	// nothing.
	at time.Time
}

// refund returns the admission to the bucket. Safe on a zero value.
func (b verifyBudget) refund() {
	if b.res != nil {
		b.res.CancelAt(b.at)
	}
}

// reserveVerifyAttempt atomically tests AND consumes one admission from the
// (IP, app) failure budget, returning ok=false when the budget is spent.
//
// Atomicity is the point. The previous shape — peek TokensAt(now) >= 1, call
// pipeline-api, then spend on failure — was a TOCTOU window: N concurrent
// invalid exchanges all observed capacity before any of them recorded a
// failure, so all N reached VerifyToken and therefore Postgres. ReserveN does
// the test and the decrement in one critical section inside the limiter, so
// at most `burst` admissions exist per window no matter how many goroutines
// race for them.
//
// The caller KEEPS the admission on a genuine failure (malformed token,
// unknown/revoked token) and REFUNDS it otherwise — a successful exchange and
// an infrastructure error must both cost a viewer nothing.
func (s *Server) reserveVerifyAttempt(ip, appID string) (verifyBudget, bool) {
	now := s.now()
	res := s.verifyFailLimits.limiter(verifyKey(ip, appID)).ReserveN(now, 1)
	if !res.OK() || res.DelayFrom(now) > 0 {
		// No capacity in this window: give the (possibly future-dated)
		// reservation straight back so a throttled attempt doesn't push the
		// bucket's recovery further out.
		res.CancelAt(now)
		return verifyBudget{}, false
	}
	return verifyBudget{res: res, at: now}, true
}

// ---------------------------------------------------------------------------
// Error responses
// ---------------------------------------------------------------------------

// fail writes the §8 error envelope. Nothing about the request URL, the
// Referer, or any credential ever reaches the message — the arguments are
// always literals from this file.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, kind errorKind, msg string) {
	writeError(w, r, status, kind, msg)
}

// failViewerToken is the single response for malformed, unknown, and revoked
// viewer tokens alike: one status, one kind, one message. Distinguishing them
// would turn the exchange into an oracle for valid token ids.
func (s *Server) failViewerToken(w http.ResponseWriter, r *http.Request) {
	s.fail(w, r, http.StatusForbidden, errKindUnauthorized, "invalid viewer token")
}

// failRateLimited writes 429 + Retry-After (never below 1 second).
func (s *Server) failRateLimited(w http.ResponseWriter, r *http.Request, retryAfter int) {
	if retryAfter < 1 {
		retryAfter = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	s.fail(w, r, http.StatusTooManyRequests, errKindRateLimited, "rate limited")
}
