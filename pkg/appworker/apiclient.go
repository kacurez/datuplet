// apiclient.go implements app-worker's client to pipeline-api's internal
// worker-facing surface (spec §5.2, contract-and-constraints.md "app-worker
// (Part 3)" and its Internal-routes block). It also owns the per-process
// caches that keep a render fast: a resolve TTL cache, a content-addressed
// bundle LRU, and positive/negative verify-token caches (spec §7's cache
// TTLs). See task-P3-report.md / task-P5-report.md for the server side as
// actually shipped, and this file's package doc comments for the two
// hand-offs that are easy to get wrong:
//
//  1. sessions/verify needs the CALLER's credential in a SEPARATE header
//     (forwardedAuthorizationHeader) from app-worker's own service
//     credential (which always rides in Authorization).
//  2. Query hits a DIFFERENT route (/internal/v1/projects/{pid}/query, not
//     the browser-facing /api/v1/projects/{pid}/query) and authenticates
//     with the app's impersonation JWT ALONE — never the service
//     credential.
package appworker

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Cache TTLs (spec §7, verbatim). resolveCacheTTL and sessionCacheTTL share
// the same 15s bound (both are "how long can a promote/revocation take to
// propagate"); verifyTokenPositiveTTL/verifyTokenNegativeTTL are the
// asymmetric pair spec §5.3 calls out explicitly.
const (
	resolveCacheTTL        = 15 * time.Second
	sessionCacheTTL        = 15 * time.Second
	verifyTokenPositiveTTL = 15 * time.Second
	verifyTokenNegativeTTL = 60 * time.Second
	// tokenActiveCacheTTL is CheckTokenActive's single TTL (both true and
	// false answers) — spec §7's 15s verify-cache bound, the same
	// revocation-blast-radius figure VerifyToken's positive TTL uses.
	tokenActiveCacheTTL = 15 * time.Second

	// defaultBundleCacheBytes mirrors spec §7's "Memory model" planning
	// figure ("bundle LRU (capped, default 256 MiB)"). Operator-tunable via
	// WithBundleCacheBytes; not itself a spec §7 limits-table row (that
	// table caps a single bundle at 5 MB, a different axis — see
	// config.go's DefaultBundleMaxBytes).
	defaultBundleCacheBytes = 256 << 20

	defaultLogQueueSize   = 256
	defaultLogPostTimeout = 5 * time.Second
	defaultHTTPTimeout    = 30 * time.Second

	// channelDefault mirrors pipeline-api's own default (P3-report.md:
	// "channel omitted defaults to production"); normalizing client-side
	// keeps the resolve cache key and the wire request in agreement.
	channelDefault = "production"

	// forwardedAuthorizationHeader carries the CALLER's Authorization value
	// on sessions/verify. Authorization itself is unavailable for that
	// purpose on every internal call — it always carries app-worker's own
	// service credential (requireServiceToken reads it), and one header
	// cannot hold two bearer values. Value matches
	// apps.ForwardedAuthorizationHeader exactly (declared locally, not
	// imported, so app-worker stays decoupled from the control-plane
	// package — contract-and-constraints.md).
	forwardedAuthorizationHeader = "X-Datuplet-Forwarded-Authorization"
)

// Resolved is the answer to Resolve (spec §5.2): which app version a
// (project, name, channel) currently points at.
type Resolved struct {
	AppID       string
	VersionHash string
}

// SessionInfo is the answer to VerifySession (spec §5.2): whether the
// forwarded platform credential authenticates anybody, and — if so —
// whether that user is a project member. UserID == "" means "no session",
// which per P3-report.md is a 200, never an error.
type SessionInfo struct {
	UserID        string
	ProjectMember bool
}

// RenderLogRecord is one render-log row destined for
// POST /internal/v1/apps/{app_id}/logs (spec §6.6). Field names/shape
// mirror pkg/pipelineapi/apps.RenderLogRecord's wire encoding exactly (see
// that package's appendLogRequest/toRecord), but the type is declared
// locally: app-worker does not import the control-plane package.
//
// version_hash is mandatory server-side: a render that fails BEFORE a
// version is resolved has no version_hash and cannot be recorded here —
// log those to app-worker's own structured log instead (P3-report.md).
type RenderLogRecord struct {
	RequestID     string
	AppID         string
	VersionHash   string
	Channel       string
	PrincipalKind string
	PrincipalID   string
	StartedAt     time.Time
	DurationMS    int64
	Outcome       string
	LogText       string
	Error         string
}

// APIError is returned for any non-2xx response from an internal
// pipeline-api endpoint. Kind is the wire `kind` field — the spec §8
// control-plane subset pipeline-api emits: "bad_request" | "unauthorized" |
// "app_not_found" | "unavailable" (task-P3-report.md's Error-envelope
// section).
//
// # A 401 means app-worker's OWN credential is wrong
//
// Per P3-report.md: "A 401 from any internal call means your service
// credential is wrong — never that a viewer/session was rejected." Callers
// (W4/W5) must treat a 401 here as fail-closed `unavailable` toward the
// viewer and as an alertable misconfiguration, not as a per-viewer denial;
// this client does not editorialize that mapping, it just surfaces the
// status faithfully.
type APIError struct {
	StatusCode int
	Kind       string
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("appworker: pipeline-api %d %s: %s", e.StatusCode, e.Kind, e.Message)
}

// ErrLogQueueFull is returned by AppendLog when the async queue is full.
// Per spec §5.2/the brief, this must NEVER be treated as a render failure —
// AppendLog is fire-and-forget by design. A dropped log is a lost audit
// breadcrumb, not a render defect; callers should log/count it, never fail
// or retry-block a render on it.
var ErrLogQueueFull = errors.New("appworker: render-log queue full, dropping record")

// ---------------------------------------------------------------------------
// TTL cache (fake-clock driven; no error caching — see per-method comments)
// ---------------------------------------------------------------------------

type cacheEntry[V any] struct {
	value     V
	expiresAt time.Time
}

// ttlCache is a minimal, mutex-guarded TTL cache. The clock is injected so
// tests drive expiry deterministically with a fake clock instead of
// sleeping real wall-clock seconds (contract: "TTL behavior driven by a
// fake clock"). Only successful lookups are ever stored — a transport
// error or a pipeline-api 5xx is never cached, so a transient outage
// doesn't get pinned in place for the TTL window.
type ttlCache[V any] struct {
	mu    sync.Mutex
	clock func() time.Time
	data  map[string]cacheEntry[V]
}

func newTTLCache[V any](clock func() time.Time) *ttlCache[V] {
	return &ttlCache[V]{clock: clock, data: make(map[string]cacheEntry[V])}
}

func (c *ttlCache[V]) get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.data[key]
	if !ok || !c.clock().Before(e.expiresAt) {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (c *ttlCache[V]) set(key string, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = cacheEntry[V]{value: value, expiresAt: c.clock().Add(ttl)}
}

// doSingleflight coalesces concurrent callers sharing the same key into one
// upstream call — the "don't stampede the same key into N identical
// upstream calls" requirement, applied uniformly to every cached method
// (Resolve/VerifyToken/VerifySession) plus Bundle. Deliberately NOT applied
// to Impersonate (never cached, so there is nothing to coalesce toward —
// spec §5.4 wants one mint per render, not one mint per concurrent batch of
// renders) or AppendLog (fire-and-forget, no result to share).
func doSingleflight[T any](g *singleflight.Group, key string, fn func() (T, error)) (T, error) {
	v, err, _ := g.Do(key, func() (interface{}, error) {
		return fn()
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return v.(T), nil
}

// hashKey folds secret-bearing arguments (a viewer-token secret, a
// forwarded session credential) into a fixed-width cache key instead of
// concatenating them in the clear. This doesn't reduce in-process exposure
// (the plaintext already lives in the caller's argument), but it keeps the
// cache's key space from ever being mistaken for a safe-to-log value.
func hashKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// sha256Hex returns the hex-encoded SHA-256 digest of b — the exact form
// bundle content hashes take on the wire (spec §5.2).
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// Bundle LRU: content-addressed, byte-size-bounded (spec §7)
// ---------------------------------------------------------------------------

type bundleCacheEntry struct {
	hash string
	data []byte
}

// bundleLRU caches verified bundle bytes by content hash, evicting the
// least-recently-used entry under total-byte pressure (spec §7: "bundle LRU
// (capped, default 256 MiB)"). Bundles are content-addressed and therefore
// immutable once verified — there is no invalidation path, only eviction.
// Safe for concurrent use (app-worker renders concurrently by design).
type bundleLRU struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	order    *list.List // front = most recently used
	items    map[string]*list.Element
}

func newBundleLRU(maxBytes int64) *bundleLRU {
	return &bundleLRU{maxBytes: maxBytes, order: list.New(), items: make(map[string]*list.Element)}
}

// get returns a FRESH COPY of the cached bytes, never the cache's own
// backing array. Without this, a caller that mutates the returned slice
// (e.g. reuses it as a scratch buffer) would corrupt what every other
// renderer sees for the same content hash — defeating the entire premise
// of a content-addressed, supposedly-immutable cache.
func (c *bundleLRU) get(hash string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[hash]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	stored := el.Value.(*bundleCacheEntry).data
	out := make([]byte, len(stored))
	copy(out, stored)
	return out, true
}

// put stores a FRESH COPY of data, never the caller's own slice. Symmetric
// with get's copy-out: the cache must never alias a buffer it does not
// exclusively own, in either direction — a caller mutating the slice it
// passed to put (e.g. a reused read buffer) must not be able to corrupt an
// entry every other renderer will subsequently read.
func (c *bundleLRU) put(hash string, data []byte) {
	size := int64(len(data))
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[hash]; ok {
		c.order.MoveToFront(el)
		return
	}
	if size > c.maxBytes {
		// Larger than the whole cache budget: caching it would evict
		// everything else and then itself. A miss just means the next
		// Bundle() call re-fetches; correctness never depends on a hit.
		return
	}
	for c.curBytes+size > c.maxBytes && c.order.Len() > 0 {
		back := c.order.Back()
		entry := back.Value.(*bundleCacheEntry)
		c.order.Remove(back)
		delete(c.items, entry.hash)
		c.curBytes -= int64(len(entry.data))
	}
	owned := make([]byte, size)
	copy(owned, data)
	el := c.order.PushFront(&bundleCacheEntry{hash: hash, data: owned})
	c.items[hash] = el
	c.curBytes += size
}

// ---------------------------------------------------------------------------
// APIClient
// ---------------------------------------------------------------------------

// APIClient is app-worker's client to pipeline-api's internal API (spec
// §5.2). The exact method set below is fixed by
// task-W3-brief.md's Interfaces section — W4/W5/W6 call these names
// verbatim.
type APIClient struct {
	baseURL        string
	serviceToken   string
	httpClient     *http.Client
	clock          func() time.Time
	logPostTimeout time.Duration

	resolveCache *ttlCache[Resolved]
	resolveSF    singleflight.Group

	// verifyTokenPositiveCache, verifyTokenInactiveCache, and
	// verifyTokenWrongSecretCache are DELIBERATELY SEPARATE caches with
	// different key shapes — see VerifyToken's doc comment for the full
	// rationale (RFC 028 W3 fix round 2). Summary: positive and
	// wrong-secret-against-an-active-token both key on the FULL
	// (app_id, token_id, secret) tuple; only "this token itself is
	// inactive" keys on the (app_id, token_id) PAIR, since that is the only
	// one of the three facts that has no legitimate holder to lock out.
	// verifyTokenSF's key always includes the secret (the positive-cache
	// key), so concurrent identical requests still coalesce without letting
	// two DIFFERENT concurrent secrets for the same token share a
	// singleflight leader.
	verifyTokenPositiveCache    *ttlCache[bool]
	verifyTokenInactiveCache    *ttlCache[bool]
	verifyTokenWrongSecretCache *ttlCache[bool]
	verifyTokenSF               singleflight.Group

	tokenActiveCache *ttlCache[bool]
	tokenActiveSF    singleflight.Group

	sessionCache *ttlCache[SessionInfo]
	sessionSF    singleflight.Group

	bundles        *bundleLRU
	bundleSF       singleflight.Group
	maxBundleBytes int64

	logQueue chan RenderLogRecord
	stopOnce sync.Once
	stopCh   chan struct{}
	logDone  chan struct{}
}

// Option configures an APIClient at construction time.
type Option func(*APIClient)

// WithHTTPClient overrides the default *http.Client (30s timeout).
func WithHTTPClient(hc *http.Client) Option { return func(c *APIClient) { c.httpClient = hc } }

// WithClock overrides the client's notion of "now", used only for cache TTL
// expiry. Tests inject a fixed/advancing fake clock so the 15s/60s bounds
// are deterministic without sleeping real time.
func WithClock(now func() time.Time) Option { return func(c *APIClient) { c.clock = now } }

// WithBundleCacheBytes overrides the bundle LRU's total-byte budget (default
// 256 MiB, spec §7).
func WithBundleCacheBytes(n int64) Option {
	return func(c *APIClient) { c.bundles = newBundleLRU(n) }
}

// WithMaxBundleBytes overrides the hard ceiling Bundle will read from a
// single response body (default HardCapBundleMaxBytes, spec §4/§7's 5 MB
// structural bundle cap). Guards against a buggy or hostile internal
// endpoint forcing an oversized allocation before the hash is even checked.
func WithMaxBundleBytes(n int64) Option {
	return func(c *APIClient) { c.maxBundleBytes = n }
}

// WithLogQueueSize overrides the AppendLog async queue's capacity (default
// 256). A smaller size makes the drop-on-full path easy to exercise in
// tests.
func WithLogQueueSize(n int) Option {
	return func(c *APIClient) { c.logQueue = make(chan RenderLogRecord, n) }
}

// WithLogPostTimeout overrides the per-record background POST timeout
// (default 5s). Tests shrink this so a deliberately-slow fake server
// doesn't make the suite slow.
func WithLogPostTimeout(d time.Duration) Option {
	return func(c *APIClient) { c.logPostTimeout = d }
}

// NewAPIClient builds an APIClient talking to baseURL (pipeline-api's
// DATUPLET_API_URL) with serviceToken as the internal bearer credential
// (contents of DATUPLET_APPWORKER_SERVICE_TOKEN_FILE, per config.go). It
// starts a background goroutine that drains the AppendLog queue; call
// Close to stop it (best-effort drain) during shutdown.
func NewAPIClient(baseURL, serviceToken string, opts ...Option) *APIClient {
	c := &APIClient{
		baseURL:        strings.TrimSuffix(baseURL, "/"),
		serviceToken:   serviceToken,
		httpClient:     &http.Client{Timeout: defaultHTTPTimeout},
		clock:          time.Now,
		logPostTimeout: defaultLogPostTimeout,
		logQueue:       make(chan RenderLogRecord, defaultLogQueueSize),
		stopCh:         make(chan struct{}),
		logDone:        make(chan struct{}),
		maxBundleBytes: HardCapBundleMaxBytes,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.bundles == nil {
		c.bundles = newBundleLRU(defaultBundleCacheBytes)
	}
	c.resolveCache = newTTLCache[Resolved](c.clock)
	c.verifyTokenPositiveCache = newTTLCache[bool](c.clock)
	c.verifyTokenInactiveCache = newTTLCache[bool](c.clock)
	c.verifyTokenWrongSecretCache = newTTLCache[bool](c.clock)
	c.tokenActiveCache = newTTLCache[bool](c.clock)
	c.sessionCache = newTTLCache[SessionInfo](c.clock)

	go c.runLogLoop()
	return c
}

// Close stops the AppendLog background loop, best-effort draining whatever
// is already queued (each drained record gets its own logPostTimeout) before
// returning. Idempotent.
func (c *APIClient) Close() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	<-c.logDone
}

// ---------------------------------------------------------------------------
// Resolve
// ---------------------------------------------------------------------------

// Resolve maps (project, app name, channel) to the version app-worker
// should render (spec §5.2), cached ≤15s and keyed "pid|name|channel" (via
// hashKey, so the raw strings never sit in the key literally).
func (c *APIClient) Resolve(ctx context.Context, pid, name, channel string) (Resolved, error) {
	if channel == "" {
		channel = channelDefault
	}
	key := hashKey("resolve", pid, name, channel)
	if v, ok := c.resolveCache.get(key); ok {
		return v, nil
	}
	return doSingleflight(&c.resolveSF, key, func() (Resolved, error) {
		path := fmt.Sprintf("/internal/v1/apps/%s/%s/resolve?channel=%s",
			url.PathEscape(pid), url.PathEscape(name), url.QueryEscape(channel))
		var resp resolveResponseWire
		if err := c.doInternal(ctx, http.MethodGet, path, nil, &resp, nil); err != nil {
			return Resolved{}, err
		}
		res := Resolved{AppID: resp.AppID, VersionHash: resp.VersionHash}
		c.resolveCache.set(key, res, resolveCacheTTL)
		return res, nil
	})
}

type resolveResponseWire struct {
	AppID       string `json:"app_id"`
	VersionHash string `json:"version_hash"`
}

// ---------------------------------------------------------------------------
// Bundle
// ---------------------------------------------------------------------------

// Bundle fetches bundle bytes by content hash, verifying SHA-256 on receipt
// (spec §5.2) — a mismatch is an error and is NEVER written to the cache.
// Hits are served from the content-addressed LRU (≤256 MiB default);
// concurrent callers requesting the same hash share one upstream fetch.
//
// The read is bounded at maxBundleBytes+1 (default HardCapBundleMaxBytes,
// the spec §4/§7 structural 5 MB bundle cap): without a bound, a buggy or
// hostile internal endpoint could force an allocation far past that limit
// before the hash is even checked, since io.ReadAll has no size ceiling of
// its own. An oversized body is rejected as soon as the limit is exceeded,
// before the (pointless, since it's already known-oversized) hash check.
func (c *APIClient) Bundle(ctx context.Context, hash string) ([]byte, error) {
	if b, ok := c.bundles.get(hash); ok {
		return b, nil
	}
	return doSingleflight(&c.bundleSF, hash, func() ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			c.baseURL+"/internal/v1/bundles/"+url.PathEscape(hash), nil)
		if err != nil {
			return nil, fmt.Errorf("appworker: build bundle request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.serviceToken)
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("appworker: fetch bundle %s: %w", hash, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, parseAPIError(resp)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBundleBytes+1))
		if err != nil {
			return nil, fmt.Errorf("appworker: read bundle %s: %w", hash, err)
		}
		if int64(len(data)) > c.maxBundleBytes {
			return nil, fmt.Errorf("appworker: bundle %s exceeds %d-byte structural cap", hash, c.maxBundleBytes)
		}
		if got := sha256Hex(data); got != hash {
			// Deliberately NOT cached: spec §5.2's re-verification exists
			// precisely so a corrupted/mismatched transfer is never
			// trusted, let alone remembered for the next renderer.
			return nil, fmt.Errorf("appworker: bundle hash mismatch: want %s, got %s", hash, got)
		}
		c.bundles.put(hash, data)
		return data, nil
	})
}

// ---------------------------------------------------------------------------
// VerifyToken
// ---------------------------------------------------------------------------

// VerifyToken checks a viewer token's (app_id, token_id, secret) triple
// (spec §5.3). Results are cached under THREE deliberately different key
// shapes — this is the second fix round for the negative-cache design
// (RFC 028 W3 fix round 2), after the first round's pair-keyed negative
// cache turned out to over-correct into a NEW DoS: any single wrong-secret
// guess against a token poisoned the pair-level entry for 60s, which meant
// the LEGITIMATE holder presenting the correct secret got a cached `false`
// without ever reaching pipeline-api — a denial of service on a real
// viewer, triggerable by anyone who merely knows a token_id (no secret
// needed to trigger it).
//
// The fix: §5.3's "negative-cache per (app_id, token_id) for 60s" is about
// caching the fact "this token is inactive", not "this secret was wrong".
// Those are different facts with different safety properties, so they get
// different keys:
//
//   - A POSITIVE result ("ok:true") caches under the FULL tuple (app_id,
//     token_id, secret), at 15s. Unchanged across both fix rounds: a wrong
//     secret computes a different key by construction, so it can never hit
//     a positive entry meant for a different secret.
//   - A "token is INACTIVE" result (verify answered false, and a follow-up
//     CheckTokenActive confirms the token itself — unknown, revoked, or
//     app-mismatched — is not active) caches under (app_id, token_id)
//     ALONE, at 60s. This is safe to share across every secret: an inactive
//     token has NO correct secret and therefore no legitimate holder to
//     lock out, so pair-level caching here can never deny a real viewer.
//   - A "WRONG SECRET against an ACTIVE token" result caches under the FULL
//     tuple (app_id, token_id, secret), at 60s — i.e. back to the ORIGINAL
//     (pre-fix-round-1) behavior for this specific case. A wrong secret for
//     an active token never poisons any other secret's entry, so the
//     legitimate holder's correct secret always reaches a fresh check.
//     Consequence, stated plainly: varying the secret against an ACTIVE
//     token reaches pipeline-api on EVERY distinct guess — this cache does
//     NOT rate-limit that. Spec §5.3 assigns that job to app-worker's own
//     "10 verify failures/min per (client IP, app) → 429 Retry-After: 60"
//     limiter (W4), which is correctly keyed on the caller's IP (this
//     client, running inside pipeline-api's trust boundary, never sees
//     that) — duplicating it here at the wrong granularity is exactly what
//     the first fix round got wrong.
//
// The two negative cases are distinguished by asking CheckTokenActive
// (itself cached 15s, so this costs at most one extra call per token per
// 15s) whenever verify answers false. If that follow-up call itself errors,
// the false result is not cached under EITHER key — a transient failure to
// classify must never risk locking out a legitimate holder, so the safest
// choice is simply not to remember anything.
func (c *APIClient) VerifyToken(ctx context.Context, appID, tokenID, secret string) (bool, error) {
	positiveKey := hashKey("verify-token-pos", appID, tokenID, secret)
	if v, ok := c.verifyTokenPositiveCache.get(positiveKey); ok {
		return v, nil
	}
	inactiveKey := hashKey("verify-token-inactive", appID, tokenID)
	if _, ok := c.verifyTokenInactiveCache.get(inactiveKey); ok {
		return false, nil
	}
	wrongSecretKey := hashKey("verify-token-wrong-secret", appID, tokenID, secret)
	if _, ok := c.verifyTokenWrongSecretCache.get(wrongSecretKey); ok {
		return false, nil
	}
	return doSingleflight(&c.verifyTokenSF, positiveKey, func() (bool, error) {
		reqBody := verifyTokenRequestWire{AppID: appID, TokenID: tokenID, Secret: secret}
		var resp verifyTokenResponseWire
		if err := c.doInternal(ctx, http.MethodPost, "/internal/v1/viewer-tokens/verify", reqBody, &resp, nil); err != nil {
			return false, err
		}
		if resp.OK {
			c.verifyTokenPositiveCache.set(positiveKey, true, verifyTokenPositiveTTL)
			return true, nil
		}
		// Classify the failure before caching it anywhere.
		active, err := c.CheckTokenActive(ctx, appID, tokenID)
		switch {
		case err != nil:
			// Unclassifiable — cache nothing, per the doc comment above.
		case active:
			// Wrong secret against a LIVE token: never poison any other
			// secret's entry.
			c.verifyTokenWrongSecretCache.set(wrongSecretKey, false, verifyTokenNegativeTTL)
		default:
			// Token itself is inactive: safe to share across every secret.
			c.verifyTokenInactiveCache.set(inactiveKey, false, verifyTokenNegativeTTL)
		}
		return false, nil
	})
}

type verifyTokenRequestWire struct {
	AppID   string `json:"app_id"`
	TokenID string `json:"token_id"`
	Secret  string `json:"secret"`
}

type verifyTokenResponseWire struct {
	OK bool `json:"ok"`
}

// ---------------------------------------------------------------------------
// CheckTokenActive — the secret-less revocation recheck (RFC 028 W3 fix)
// ---------------------------------------------------------------------------

// CheckTokenActive answers "is this (app_id, token_id) still active?" —
// WITHOUT a secret. It is the client for pipeline-api's
// POST /internal/v1/viewer-tokens/active (added in this fix round), and
// exists specifically for app-worker's cookie-only revocation recheck (spec
// §5.3): the signed session cookie carries `{app_id, token_id, exp}` and NO
// secret (the plaintext transits exactly once, at the 302 exchange), so a
// cookie-authenticated request has nothing to present to VerifyToken.
//
// Cached 15s (spec §7's verify-cache TTL), keyed (app_id, token_id) — there
// is no secret to fold into the key here, and none is needed: this endpoint
// answers a strictly narrower question than VerifyToken ("is the token row
// still live", never "is this secret correct"), so there is no positive/
// negative asymmetry to get wrong — a "false" answer is exactly as safe to
// cache as a "true" one, both at the same 15s bound, matching the
// revocation blast-radius spec §5.3 requires.
func (c *APIClient) CheckTokenActive(ctx context.Context, appID, tokenID string) (bool, error) {
	key := hashKey("token-active", appID, tokenID)
	if v, ok := c.tokenActiveCache.get(key); ok {
		return v, nil
	}
	return doSingleflight(&c.tokenActiveSF, key, func() (bool, error) {
		reqBody := tokenActiveRequestWire{AppID: appID, TokenID: tokenID}
		var resp tokenActiveResponseWire
		if err := c.doInternal(ctx, http.MethodPost, "/internal/v1/viewer-tokens/active", reqBody, &resp, nil); err != nil {
			return false, err
		}
		c.tokenActiveCache.set(key, resp.Active, tokenActiveCacheTTL)
		return resp.Active, nil
	})
}

type tokenActiveRequestWire struct {
	AppID   string `json:"app_id"`
	TokenID string `json:"token_id"`
}

type tokenActiveResponseWire struct {
	Active bool `json:"active"`
}

// ---------------------------------------------------------------------------
// VerifySession — the hand-off that is easy to get wrong
// ---------------------------------------------------------------------------

// VerifySession authorizes an `@draft` preview by forwarding the caller's
// platform session credential to pipeline-api's session resolver (spec
// §5.2). Cached ≤15s per session.
//
// # Two credentials, two headers — do not conflate them
//
// Every internal call authenticates app-worker itself with the SERVICE
// credential in Authorization (requireServiceToken reads exactly that
// header, and only that header). sessions/verify ALSO needs the CALLER's
// own credential so pipeline-api's session resolver has something to
// authenticate — but Authorization is already spoken for. Per
// task-P3-report.md's fix round: that credential goes in
// forwardedAuthorizationHeader ("X-Datuplet-Forwarded-Authorization"),
// sent VERBATIM (including any "Bearer " scheme prefix) and ONLY when
// authz is non-empty — never synthesized, and never the service
// credential. Cookie sessions forward via the ordinary Cookie header
// (cookies may be empty too, for a bearer-only caller).
func (c *APIClient) VerifySession(ctx context.Context, pid, cookies, authz string) (SessionInfo, error) {
	key := hashKey("session", pid, cookies, authz)
	if v, ok := c.sessionCache.get(key); ok {
		return v, nil
	}
	return doSingleflight(&c.sessionSF, key, func() (SessionInfo, error) {
		reqBody := verifySessionRequestWire{PID: pid}
		var resp verifySessionResponseWire
		extra := func(req *http.Request) {
			if authz != "" {
				req.Header.Set(forwardedAuthorizationHeader, authz)
			}
			if cookies != "" {
				req.Header.Set("Cookie", cookies)
			}
		}
		if err := c.doInternal(ctx, http.MethodPost, "/internal/v1/sessions/verify", reqBody, &resp, extra); err != nil {
			return SessionInfo{}, err
		}
		info := SessionInfo{UserID: resp.UserID, ProjectMember: resp.ProjectMember}
		c.sessionCache.set(key, info, sessionCacheTTL)
		return info, nil
	})
}

type verifySessionRequestWire struct {
	PID string `json:"pid"`
}

type verifySessionResponseWire struct {
	UserID        string `json:"user_id"`
	ProjectMember bool   `json:"project_member"`
}

// ---------------------------------------------------------------------------
// Impersonate — NEVER cached
// ---------------------------------------------------------------------------

// Impersonate mints a fresh 60s impersonation JWT for the app's synthetic
// identity (spec §5.4). Deliberately uncached: every mint is audited
// server-side with a cryptographically random jti (task-P4-report.md's
// fix), specifically so each render is individually attributable. Reusing
// a jti across renders — which caching here would do — is a spec
// violation, not a performance optimization.
func (c *APIClient) Impersonate(ctx context.Context, appID string) (string, error) {
	reqBody := impersonateRequestWire{AppID: appID}
	var resp impersonateResponseWire
	if err := c.doInternal(ctx, http.MethodPost, "/internal/v1/impersonate", reqBody, &resp, nil); err != nil {
		return "", err
	}
	return resp.Token, nil
}

type impersonateRequestWire struct {
	AppID string `json:"app_id"`
}

type impersonateResponseWire struct {
	Token string `json:"token"`
}

// ---------------------------------------------------------------------------
// AppendLog — async, drop-on-full, never blocks a render
// ---------------------------------------------------------------------------

// AppendLog enqueues rec for a background POST to
// /internal/v1/apps/{app_id}/logs. It never blocks: a full queue drops the
// record and returns ErrLogQueueFull. Callers MUST NOT fail or delay a
// render on this error — the ring-buffer log is best-effort audit
// breadcrumb, not part of the render's own contract.
func (c *APIClient) AppendLog(_ context.Context, rec RenderLogRecord) error {
	select {
	case c.logQueue <- rec:
		return nil
	default:
		return ErrLogQueueFull
	}
}

// runLogLoop drains logQueue in the background until Close is called, then
// best-effort drains whatever remains (non-blocking) before exiting.
func (c *APIClient) runLogLoop() {
	defer close(c.logDone)
	for {
		select {
		case rec := <-c.logQueue:
			c.postLogBestEffort(rec)
		case <-c.stopCh:
			c.drainLogQueue()
			return
		}
	}
}

func (c *APIClient) drainLogQueue() {
	for {
		select {
		case rec := <-c.logQueue:
			c.postLogBestEffort(rec)
		default:
			return
		}
	}
}

func (c *APIClient) postLogBestEffort(rec RenderLogRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), c.logPostTimeout)
	defer cancel()
	_ = c.postLog(ctx, rec)
}

func (c *APIClient) postLog(ctx context.Context, rec RenderLogRecord) error {
	wire := renderLogRequestWire{
		RequestID:     rec.RequestID,
		AppID:         rec.AppID,
		VersionHash:   rec.VersionHash,
		Channel:       rec.Channel,
		PrincipalKind: rec.PrincipalKind,
		PrincipalID:   rec.PrincipalID,
		DurationMS:    rec.DurationMS,
		Outcome:       rec.Outcome,
		LogText:       rec.LogText,
		Error:         rec.Error,
	}
	if !rec.StartedAt.IsZero() {
		wire.StartedAt = rec.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	path := fmt.Sprintf("/internal/v1/apps/%s/logs", url.PathEscape(rec.AppID))
	return c.doInternal(ctx, http.MethodPost, path, wire, nil, nil)
}

type renderLogRequestWire struct {
	RequestID     string `json:"request_id"`
	AppID         string `json:"app_id,omitempty"`
	VersionHash   string `json:"version_hash"`
	Channel       string `json:"channel"`
	PrincipalKind string `json:"principal_kind"`
	PrincipalID   string `json:"principal_id"`
	StartedAt     string `json:"started_at,omitempty"`
	DurationMS    int64  `json:"duration_ms"`
	Outcome       string `json:"outcome"`
	LogText       string `json:"log_text"`
	Error         string `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// Query — the OTHER hand-off that is easy to get wrong
// ---------------------------------------------------------------------------

// Query proxies a query-service request through pipeline-api's APP query
// route (spec §5.4, task-P5-report.md), returning the raw *http.Response so
// the caller can stream status/body straight back to the viewer without an
// extra buffering hop.
//
// # This is NOT the browser route, and takes NO service credential
//
//   - Path is /internal/v1/projects/{pid}/query — added by P5 specifically
//     for app-worker. The browser/CLI route is /api/v1/projects/{pid}/query
//     and requires a platform session/bearer user, which an app is not.
//   - Authorization carries jwt ALONE (the app's impersonation JWT from
//     Impersonate) — never c.serviceToken. This route's gate is the JWT
//     itself (signature, iss, aud, token_kind="app"): the app token IS both
//     the audited principal and the forwarded catalog credential
//     (task-P5-report.md), so layering the service credential on top would
//     authenticate a fact the JWT already establishes, for no additional
//     exclusion — "do not normalize" this asymmetry away.
func (c *APIClient) Query(ctx context.Context, pid string, jwt string, body []byte) (*http.Response, error) {
	path := fmt.Sprintf("/internal/v1/projects/%s/query", url.PathEscape(pid))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("appworker: build query request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+jwt)
	return c.httpClient.Do(req)
}

// ---------------------------------------------------------------------------
// Shared HTTP plumbing
// ---------------------------------------------------------------------------

// doInternal performs one internal-API call authenticated with
// app-worker's service credential (Authorization: Bearer <serviceToken>),
// optionally applying extra (which may add sessions/verify's forwarded
// credential/cookie headers), and decodes a 2xx JSON body into out (skipped
// when out is nil, e.g. the logs endpoint's 204).
func (c *APIClient) doInternal(ctx context.Context, method, path string, reqBody, out any, extra func(*http.Request)) error {
	var body io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("appworker: marshal request: %w", err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("appworker: build request: %w", err)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// The service credential is set FIRST and unconditionally: it always
	// occupies Authorization, on every internal call, no exceptions. extra
	// (sessions/verify only) adds the caller's credential in a DIFFERENT
	// header — it must never touch Authorization.
	req.Header.Set("Authorization", "Bearer "+c.serviceToken)
	if extra != nil {
		extra(req)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("appworker: call pipeline-api %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseAPIError(resp)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("appworker: decode pipeline-api response: %w", err)
	}
	return nil
}

// parseAPIError decodes pipeline-api's {error, kind} envelope
// (task-P3-report.md's Error-envelope section) into an *APIError. A
// malformed/empty body still yields an APIError carrying the status code
// (Kind/Message empty) rather than a generic transport error, so callers
// can always type-assert.
func parseAPIError(resp *http.Response) error {
	var body struct {
		Error string `json:"error"`
		Kind  string `json:"kind"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 16<<10)).Decode(&body)
	return &APIError{StatusCode: resp.StatusCode, Kind: body.Kind, Message: body.Error}
}
