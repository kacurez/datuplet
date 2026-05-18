package catalogwriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MinRenewalInterval is the hard floor between consecutive lakekeeper
// vended-creds fetches. Even when the 50%-elapsed rule says "renew now"
// — e.g. a pathological 30-second STS TTL where 50% elapsed means 15s
// — VendedCreds will not call lakekeeper more often than once per
// minute. This bounds the worst case at one extra call/minute regardless
// of TTL.
const MinRenewalInterval = 60 * time.Second

// maxResponseBytes caps lakekeeper response payloads we read into
// memory. A vended-creds response is ~1 KiB; 1 MiB is generous and
// guards against runaway / hostile responses without affecting
// legitimate traffic.
const maxResponseBytes = 1 << 20

// defaultHTTPTimeout is the per-request timeout used when callers
// don't supply their own *http.Client. Long enough to tolerate a
// slow-but-alive lakekeeper, short enough that a hung peer doesn't
// pin a fetch goroutine forever.
const defaultHTTPTimeout = 30 * time.Second

// DefaultRenewalFraction is the fraction-of-issued-TTL after which
// VendedCreds prefers to renew. Worked examples:
//
//   - 15-min TTL → renew at 7m30s (then every 7m30s).
//   - 5-min TTL  → renew at 2m30s (≥ 60s floor honoured).
//   - 30-second TTL would renew at 15s but the 60s floor blocks it,
//     so the actual cadence falls back to "every 60s".
const DefaultRenewalFraction = 0.5

// VendedCreds caches a Creds value and refreshes it on demand from
// lakekeeper. Safe for concurrent callers. The renewal contract:
// renew when 50% of the issued TTL has elapsed, with a 60-second hard
// floor between renewals (MinRenewalInterval).
//
// Field guidance:
//
//   - LakekeeperURL is the catalog base URL (e.g.
//     http://lakekeeper:8181/catalog).
//   - Prefix is the per-warehouse URL prefix lakekeeper requires on
//     authenticated calls (often the warehouse UUID; obtained from a
//     prior `/v1/config` response). Pass empty string when the
//     deployment doesn't use a per-warehouse prefix.
//   - Namespace + Table identify the resource the caller will write.
//   - TokenProvider returns the bearer JWT for this (run, table, intent)
//     pair. Called on every fetch so a rotated token map is picked up
//     without re-creating the cache.
//   - RenewalFraction defaults to 0.5 (DefaultRenewalFraction) when
//     unset / non-positive.
//   - Now is the time source. Tests set this to a fake clock; production
//     leaves it nil and gets time.Now.
//   - HTTPClient is the lakekeeper transport. Defaults to a fresh
//     *http.Client with a 30-second per-request timeout (see
//     defaultHTTPTimeout). Inject your own to override e.g. for mTLS,
//     a longer timeout, or testing.
type VendedCreds struct {
	LakekeeperURL string
	// WarehouseName is the lakekeeper warehouse name (e.g. "datuplet"). When
	// Prefix is empty, VendedCreds will resolve the per-warehouse REST URL
	// prefix by calling `GET /v1/config?warehouse=<name>` on first fetch.
	WarehouseName string
	// ProjectID, when non-empty, is forwarded as the `x-project-id`
	// header on every lakekeeper REST call (config + STS-vended-creds).
	// Same semantics as catalogwriter.Client's ProjectID — routes the
	// fetch to the correct per-project lakekeeper scope.
	ProjectID       string
	Prefix          string
	Namespace       string
	Table           string
	TokenProvider   TokenProvider
	RenewalFraction float64
	Now             func() time.Time
	HTTPClient      *http.Client

	// ExpectedCredsType is consulted by parseCreds to fail-closed on
	// scheme mismatch. Set at construction time by the backend resolver
	// (which knows the scheme from the table location). MANDATORY — a
	// zero value causes parseCreds to return an error. Slice A wires
	// this on every existing S3 call site at the same time the field is
	// added, so there is no in-between window where callers can forget
	// it. See RFC 019 §4.1.
	ExpectedCredsType CredsType

	mu        sync.Mutex
	cached    Creds // interface — was *Creds (flat struct) before Slice A
	lastErr   error
	fetching  bool
	fetchDone chan struct{} // closed when the in-flight fetch finishes; nil when not fetching
}

// now is the time-source helper. Tests inject Now; production uses
// time.Now via the nil-default path.
func (v *VendedCreds) now() time.Time {
	if v.Now != nil {
		return v.Now()
	}
	return time.Now()
}

// fraction returns the renewal fraction with the default applied.
func (v *VendedCreds) fraction() float64 {
	if v.RenewalFraction <= 0 || v.RenewalFraction >= 1 {
		return DefaultRenewalFraction
	}
	return v.RenewalFraction
}

// shouldRenew encodes the renewal contract: renew when 50% of the
// issued TTL has elapsed, but never within MinRenewalInterval of the
// last fetch. Returns true when c is nil (first-fetch).
func (v *VendedCreds) shouldRenew(c Creds) bool {
	if c == nil {
		return true
	}
	now := v.now()
	exp := c.ExpiresAt()
	if !exp.IsZero() && !now.Before(exp) {
		// Already expired — must renew.
		return true
	}
	issued := c.IssuedAt()
	if issued.IsZero() || exp.IsZero() {
		// No TTL info: be conservative; renew (and let the floor below
		// throttle if the caller hammers Get repeatedly).
		return true
	}
	ttl := exp.Sub(issued)
	if ttl <= 0 {
		return true
	}
	target := time.Duration(float64(ttl) * v.fraction())
	elapsed := now.Sub(issued)
	if elapsed < MinRenewalInterval {
		// Within the hard floor; defer.
		return false
	}
	return elapsed >= target
}

// Get returns a usable set of credentials, fetching from lakekeeper if
// the cache is empty or due for renewal. The fetch path is serialized
// per-VendedCreds: concurrent callers either get the cached value or
// queue behind one in-flight fetch.
//
// On fetch failure the cached value is preserved if it is still
// unexpired — the data plane keeps using the old creds until they
// genuinely expire — but `lastErr` is recorded so a subsequent caller
// after expiry sees the failure surface. When the cache is empty, a
// fetch failure propagates immediately.
//
// Concurrency: callers that arrive while a fetch is in flight either
// (a) return the cached unexpired value or (b) park on `fetchDone`,
// the per-fetch broadcast channel set under v.mu. Replacing the prior
// recursive 50ms-poll path; no goroutine accumulation under sustained
// lakekeeper hangs.
func (v *VendedCreds) Get(ctx context.Context) (Creds, error) {
	for {
		v.mu.Lock()
		cur := v.cached
		due := v.shouldRenew(cur)
		if !due && cur != nil {
			out := cur
			v.mu.Unlock()
			return out, nil
		}
		if v.fetching {
			// Another goroutine is fetching. If we have an unexpired
			// cache, return it; otherwise wait on fetchDone — closed
			// when the in-flight fetch finishes (success or failure).
			if cur != nil && !v.now().After(cur.ExpiresAt()) {
				out := cur
				v.mu.Unlock()
				return out, nil
			}
			done := v.fetchDone
			v.mu.Unlock()
			if done == nil {
				// Defensive: shouldn't happen — fetching=true implies
				// fetchDone is non-nil. Fall through to retry the loop
				// after a tiny pause so we don't spin.
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Millisecond):
				}
				continue
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
			}
			// In-flight fetch finished; loop to either consume the new
			// cache or (if it failed) attempt our own fetch.
			continue
		}
		v.fetching = true
		v.fetchDone = make(chan struct{})
		done := v.fetchDone
		v.mu.Unlock()

		c, err := v.fetch(ctx)

		v.mu.Lock()
		v.fetching = false
		v.fetchDone = nil
		if err != nil {
			v.lastErr = err
			// Keep stale-but-unexpired creds usable. If cache was empty
			// or the cached value is past expiry, return the error so
			// the data plane fails loudly on renewal failure.
			if cur != nil && v.now().Before(cur.ExpiresAt()) {
				out := cur
				v.mu.Unlock()
				close(done)
				return out, nil
			}
			v.mu.Unlock()
			close(done)
			return nil, err
		}
		v.cached = c
		v.lastErr = nil
		out := c
		v.mu.Unlock()
		close(done)
		return out, nil
	}
}

// LastError returns the last fetch error encountered, if any. Cleared
// whenever a fetch succeeds. Useful for callers that want to surface a
// renewal failure in their own diagnostics without forcing another
// Get call.
func (v *VendedCreds) LastError() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.lastErr
}

// fetch performs the single lakekeeper roundtrip. Schema:
//
//	GET {LakekeeperURL}/v1/{Prefix}/namespaces/{ns}/tables/{tbl}
//	Authorization: Bearer <jwt>
//	Accept: application/json
//
// The response's `config` block carries the STS triple. Lakekeeper may
// also return per-table metadata in the same body — we ignore
// everything outside `config` for the purposes of this cache.
func (v *VendedCreds) fetch(ctx context.Context) (Creds, error) {
	// credType is used for metric labels on the error path. We know
	// ExpectedCredsType at construction; use it throughout so failures
	// before the parse step still carry a meaningful label.
	credType := string(v.ExpectedCredsType)

	if v.LakekeeperURL == "" {
		return nil, errors.New("catalogwriter: LakekeeperURL is required")
	}
	if v.Namespace == "" || v.Table == "" {
		return nil, errors.New("catalogwriter: Namespace + Table are required")
	}
	if v.TokenProvider == nil {
		return nil, errors.New("catalogwriter: TokenProvider is required")
	}
	tok, err := v.TokenProvider(ctx)
	if err != nil {
		credsRefreshFailuresTotal.WithLabelValues(credType, "token_provider").Inc()
		return nil, fmt.Errorf("catalogwriter: token provider: %w", err)
	}

	base := strings.TrimRight(v.LakekeeperURL, "/")
	// `Prefix` is the per-warehouse REST URL segment lakekeeper requires.
	// Iceberg-go's REST client discovers it via `GET /v1/config?warehouse=<name>`
	// at handshake time; VendedCreds builds URLs by hand so callers must
	// pass it in. If it's not provided BUT WarehouseName is, discover it
	// here on first fetch (`/v1/config` is cheap, ~100µs against in-cluster
	// lakekeeper). Once discovered the value is sticky for the lifetime
	// of this VendedCreds.
	if v.Prefix == "" && v.WarehouseName != "" {
		discovered, derr := v.discoverPrefix(ctx, base, tok, v.WarehouseName)
		if derr != nil {
			credsRefreshFailuresTotal.WithLabelValues(credType, "http").Inc()
			return nil, fmt.Errorf("catalogwriter: discover warehouse prefix: %w", derr)
		}
		v.Prefix = discovered
	}

	path := "/v1"
	if v.Prefix != "" {
		path += "/" + url.PathEscape(strings.Trim(v.Prefix, "/"))
	}
	path += "/namespaces/" + url.PathEscape(v.Namespace) + "/tables/" + url.PathEscape(v.Table)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		credsRefreshFailuresTotal.WithLabelValues(credType, "other").Inc()
		return nil, fmt.Errorf("catalogwriter: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")
	if v.ProjectID != "" {
		req.Header.Set("x-project-id", v.ProjectID)
	}

	httpClient := v.HTTPClient
	if httpClient == nil {
		// Default client carries a per-request timeout so a hung
		// lakekeeper doesn't pin a fetch goroutine indefinitely.
		// Callers that need a different transport (e.g. mTLS) inject
		// their own *http.Client and are responsible for setting
		// Timeout themselves.
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		credsRefreshFailuresTotal.WithLabelValues(credType, "http").Inc()
		return nil, fmt.Errorf("catalogwriter: lakekeeper GET: %w", err)
	}
	defer resp.Body.Close()

	// Bound the response read so a misbehaving / hostile lakekeeper
	// can't OOM the data gateway with a multi-GB body.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		credsRefreshFailuresTotal.WithLabelValues(credType, "http").Inc()
		return nil, fmt.Errorf("catalogwriter: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		credsRefreshFailuresTotal.WithLabelValues(credType, "http").Inc()
		return nil, fmt.Errorf("catalogwriter: lakekeeper GET: status %d body=%s", resp.StatusCode, scrubBody(body))
	}

	var parsed struct {
		Config map[string]any `json:"config"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		credsRefreshFailuresTotal.WithLabelValues(credType, "parse").Inc()
		return nil, fmt.Errorf("catalogwriter: unmarshal response: %w", err)
	}
	cfg := parsed.Config
	if cfg == nil {
		credsRefreshFailuresTotal.WithLabelValues(credType, "parse").Inc()
		return nil, errors.New("catalogwriter: lakekeeper response had no config block")
	}

	c, err := parseCreds(cfg, v.ExpectedCredsType)
	if err != nil {
		credsRefreshFailuresTotal.WithLabelValues(credType, "parse").Inc()
		return nil, fmt.Errorf("catalogwriter: %w", err)
	}
	// parseCreds returns a Creds with absolute Expires when the response
	// carried an epoch-ms claim; otherwise Expires is zero. VendedCreds
	// re-stamps Issued and the TTL fallback here so the renewal logic
	// uses a single clock source (v.now), which is what tests use to
	// drive deterministic renewal cadences.
	switch x := c.(type) {
	case S3Creds:
		x.Issued = v.now()
		if x.Expires.IsZero() {
			// Relative TTL preserved via raw value; convert here with v.now().
			if ttlSec := readString(cfg, "s3.session-ttl-seconds"); ttlSec != "" {
				if d, perr := time.ParseDuration(ttlSec + "s"); perr == nil && d > 0 && d < 24*time.Hour {
					x.Expires = x.Issued.Add(d)
				}
			}
		}
		if x.Expires.IsZero() {
			x.Expires = x.Issued.Add(15 * time.Minute)
		}
		c = x
	case GCSCreds:
		x.Issued = v.now()
		if x.Expires.IsZero() {
			x.Expires = x.Issued.Add(15 * time.Minute)
		}
		c = x
	}
	credsRefreshTotal.WithLabelValues(string(c.Type())).Inc()
	return c, nil
}

// parseCreds extracts a Creds value from a lakekeeper loadTable response's
// "config" block. expected tells the parser which credential family it
// should see — any mismatch (wrong family, mixed families, or no
// recognized keys) fails closed. See RFC 019 §4.2.
func parseCreds(cfg map[string]any, expected CredsType) (Creds, error) {
	hasS3 := cfg["s3.access-key-id"] != nil
	hasGCS := cfg["gcs.oauth2.token"] != nil

	if hasS3 && hasGCS {
		return nil, fmt.Errorf("lakekeeper response has BOTH s3.* and gcs.oauth2.* credential keys " +
			"— refusing ambiguous response (possible confused deputy / response tampering)")
	}
	switch expected {
	case CredsTypeS3:
		if hasGCS {
			return nil, fmt.Errorf("expected s3 credentials but lakekeeper returned gcs.oauth2.* keys (warehouse/backend mismatch)")
		}
		if !hasS3 {
			return nil, fmt.Errorf("lakekeeper response missing s3.access-key-id")
		}
		return parseS3Creds(cfg)
	case CredsTypeGCS:
		if hasS3 {
			return nil, fmt.Errorf("expected gcs credentials but lakekeeper returned s3.* keys (warehouse/backend mismatch)")
		}
		if !hasGCS {
			return nil, fmt.Errorf("lakekeeper response missing gcs.oauth2.token")
		}
		return parseGCSCreds(cfg)
	default:
		return nil, fmt.Errorf("VendedCreds.ExpectedCredsType is unset or unsupported (%q); callers must set it to CredsTypeS3 or CredsTypeGCS", expected)
	}
}

// parseS3Creds extracts an S3Creds from a lakekeeper response's config
// block. Caller must have already verified the s3.* family is the
// expected one (parseCreds handles this dispatch).
//
// Only the absolute-epoch-ms expiry path is set here. The relative
// `s3.session-ttl-seconds` form requires a clock source for `now`, so
// it is applied in VendedCreds.fetch where v.now() is available; this
// keeps parseS3Creds pure / clock-free and testable in isolation.
func parseS3Creds(cfg map[string]any) (Creds, error) {
	c := S3Creds{
		AccessKeyID:     readString(cfg, "s3.access-key-id"),
		SecretAccessKey: readString(cfg, "s3.secret-access-key"),
		SessionToken:    readString(cfg, "s3.session-token"),
		Region:          readString(cfg, "s3.region"),
		Endpoint:        readString(cfg, "s3.endpoint"),
	}
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return nil, errors.New("lakekeeper config block missing s3 access key fields")
	}
	if t, ok := readMillis(cfg, "s3.expires-at-ms"); ok {
		c.Expires = t
	}
	return c, nil
}

// readString pulls a string value from an unmarshalled JSON map. Missing
// or wrong-type values yield "".
func readString(cfg map[string]any, key string) string {
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return ""
}

// readMillis pulls an epoch-millisecond value from an unmarshalled JSON
// map, accepting either a JSON number (float64 after unmarshal) or a
// decimal string. Returns (time.Time, ok). ok=false on missing /
// wrong-type / non-positive values — the caller is expected to fall
// back to a default TTL.
func readMillis(cfg map[string]any, key string) (time.Time, bool) {
	raw, present := cfg[key]
	if !present {
		return time.Time{}, false
	}
	var ms int64
	switch x := raw.(type) {
	case float64:
		if x <= 0 {
			return time.Time{}, false
		}
		ms = int64(x)
	case json.Number:
		n, err := x.Int64()
		if err != nil || n <= 0 {
			return time.Time{}, false
		}
		ms = n
	case string:
		t, err := parseEpochMillis(x)
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	default:
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

// discoverPrefix queries lakekeeper's `/v1/config?warehouse=<name>` and
// returns `defaults.prefix` — the per-warehouse REST URL segment
// lakekeeper inserts after `/v1/`. Iceberg-go's REST catalog client
// performs the same handshake at NewCatalog time. We replicate it here
// so VendedCreds, which builds URLs by hand, doesn't need the prefix
// passed in from the call site (DG can construct VendedCreds without
// having to extract the warehouse-id from a loaded table).
func (v *VendedCreds) discoverPrefix(ctx context.Context, base, jwt, warehouse string) (string, error) {
	u := base + "/v1/config?warehouse=" + url.QueryEscape(warehouse)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/json")
	if v.ProjectID != "" {
		req.Header.Set("x-project-id", v.ProjectID)
	}

	httpClient := v.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("lakekeeper GET /v1/config: status %d body=%s", resp.StatusCode, scrubBody(body))
	}
	var parsed struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("unmarshal config: %w", err)
	}
	// Prefer overrides, fall back to defaults — that's how iceberg-go
	// resolves Catalog properties.
	if parsed.Overrides["prefix"] != "" {
		return parsed.Overrides["prefix"], nil
	}
	if parsed.Defaults["prefix"] != "" {
		return parsed.Defaults["prefix"], nil
	}
	return "", errors.New("lakekeeper /v1/config returned no `prefix` in defaults or overrides")
}

// bearerPattern matches `Bearer <token>` substrings so scrubBody can
// redact them before the body is included in a returned error. Lake-
// keeper is unlikely to echo the request's bearer token but defensive
// scrubbing here keeps a misconfigured backend from leaking secrets
// into operator logs.
var bearerPattern = regexp.MustCompile(`Bearer [^\s"'}]+`)

// ya29Pattern matches GCP OAuth2 access tokens (e.g. those returned
// by metadata-server / STS exchanges). Real tokens are ~140 chars of
// base64-url-safe content after the `ya29.` prefix; we accept any
// non-empty trailing run so partial / truncated tokens still get
// scrubbed.
var ya29Pattern = regexp.MustCompile(`ya29\.[A-Za-z0-9_\-+/]+`)

// jwtPattern matches bare JWTs that appear outside a `Bearer ` prefix —
// for example, token values echoed back in lakekeeper error JSON. The
// three-segment `eyJ…` structure is distinctive enough that false
// positives are negligible. Applied after bearerPattern so a
// Bearer-prefixed JWT becomes `Bearer [REDACTED]` and the standalone
// pattern never has to match it.
var jwtPattern = regexp.MustCompile(`eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+`)

// awsSigPattern matches X-Amz-* signed-URL query parameters (e.g.
// X-Amz-Security-Token, X-Amz-Signature) that appear when an S3
// pre-signed URL is logged in an error body. The parameter name is
// preserved; only the value is redacted.
var awsSigPattern = regexp.MustCompile(`X-Amz-[A-Za-z0-9\-]+=[^&\s"'}]+`)

// gcsSigPattern matches any `X-Goog-<word>=<value>` query parameter,
// covering signed-URL fields like `X-Goog-Signature`, `X-Goog-
// Credential`, `X-Goog-Date`, etc. Redaction is intentionally broad:
// if any one of these leaks alongside a signature, the URL can be
// replayed, so we strip the entire X-Goog-* surface.
var gcsSigPattern = regexp.MustCompile(`X-Goog-[A-Za-z0-9\-]+=[^&\s"}]+`)

// scrubBody truncates b to a sane length and redacts anything that
// looks like a bearer token, a GCP OAuth access token, or a GCS
// signed-URL parameter value. Used as the body interpolation in
// lakekeeper-error messages.
func scrubBody(b []byte) string {
	const max = 256
	s := string(b)
	if len(s) > max {
		s = s[:max] + "..."
	}
	s = bearerPattern.ReplaceAllString(s, "Bearer [REDACTED]")
	s = jwtPattern.ReplaceAllString(s, "<redacted-jwt>")
	s = ya29Pattern.ReplaceAllString(s, "<redacted-oauth2-token>")
	s = gcsSigPattern.ReplaceAllStringFunc(s, func(m string) string {
		// Keep the param name; redact the value.
		eq := strings.IndexByte(m, '=')
		if eq < 0 {
			return "<redacted>"
		}
		return m[:eq+1] + "<redacted>"
	})
	s = awsSigPattern.ReplaceAllStringFunc(s, func(m string) string {
		// Keep the param name; redact the value.
		eq := strings.IndexByte(m, '=')
		if eq < 0 {
			return "<redacted>"
		}
		return m[:eq+1] + "<redacted>"
	})
	return s
}

// parseEpochMillis converts a decimal epoch-ms string into a time.Time.
// Uses strconv.ParseInt so we get proper overflow detection rather than
// the silent wrap-around of a hand-rolled accumulator (CWE-190). On any
// parse failure or non-positive value we return an error so the caller
// can fall back to its TTL default; we never attribute meaning to a
// negative or zero timestamp.
func parseEpochMillis(s string) (time.Time, error) {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("not epoch ms: %q: %w", s, err)
	}
	if ms <= 0 {
		return time.Time{}, fmt.Errorf("non-positive epoch ms: %d", ms)
	}
	return time.UnixMilli(ms), nil
}

// parseGCSCreds extracts a GCSCreds from a lakekeeper response's config
// block. See RFC 019 §4.2 for the key family. The returned GCSCreds
// carries an OAuth bearer in OAuthToken — GCSCreds.String() redacts it,
// so default fmt verbs (%v / %+v / %s) are safe. RFC 019 §4.10.
//
// TTL fallback chain (first non-zero wins):
//  1. gcs.oauth2.token-expires-at (epoch-ms — the canonical key)
//  2. creds.expiration-time-ms    (legacy lakekeeper schema)
//  3. (nothing — Expires left zero; VendedCreds.fetch applies the
//     15-min default via v.now(), same pattern as parseS3Creds)
//
// Issued is left zero; VendedCreds.fetch re-stamps it using its clock
// source. Expires left zero in the fallback; VendedCreds.fetch applies
// the 15-min default via v.now() — clock-source consistency with
// parseS3Creds, avoids fake-clock skew in tests.
func parseGCSCreds(cfg map[string]any) (Creds, error) {
	tok := readString(cfg, "gcs.oauth2.token")
	if tok == "" {
		return nil, errors.New("missing gcs.oauth2.token")
	}
	c := GCSCreds{
		OAuthToken:      tok,
		GCPProjectID:    readString(cfg, "gcs.project-id"),
		RefreshEndpoint: readString(cfg, "gcs.oauth2.refresh-credentials-endpoint"),
	}
	if exp, ok := readMillis(cfg, "gcs.oauth2.token-expires-at"); ok {
		c.Expires = exp
	} else if exp, ok := readMillis(cfg, "creds.expiration-time-ms"); ok {
		c.Expires = exp
	}
	// (Else: leave c.Expires zero. VendedCreds.fetch applies the 15-minute
	//  default via v.now() — same pattern as parseS3Creds.)
	return c, nil
}
