// Package appworker implements the RFC 028 app-worker: a stateless HTTP
// service that renders user-authored dashboard apps inside per-request WASM
// sandboxes (pkg/appengine) and serves the resulting OutputDoc. See
// docs/superpowers/specs/2026-07-22-rfc-028-user-apps-wasm-workers-design.md
// §4-§8 and .superpowers/sdd/2026-07-23-rfc-028-user-apps-implementation/
// contract-and-constraints.md ("app-worker (Part 3)").
package appworker

import (
	"net/netip"
	"os"
	"strconv"
	"strings"
)

// Env var names (contract-and-constraints.md, "app-worker (Part 3)").
// DATUPLET_APPWORKER_* mirrors the appWorker.render.* values.yaml block
// (chart Part 7); DATUPLET_API_URL is shared with other pipeline-api
// clients.
const (
	EnvAPIURL           = "DATUPLET_API_URL"
	EnvListenAddr       = "DATUPLET_APPWORKER_LISTEN"
	EnvServiceTokenFile = "DATUPLET_APPWORKER_SERVICE_TOKEN_FILE"
	EnvCookieKeyFile    = "DATUPLET_APPWORKER_COOKIE_KEY_FILE"

	// EnvTrustedProxies / EnvTrustedProxyHops describe the proxy topology in
	// front of app-worker. They are the ONLY thing that makes
	// X-Forwarded-For trustworthy — see ProxyTrust and resolveClientIP in
	// auth.go. Unset (the default) means "no trusted proxies", i.e. XFF is
	// ignored entirely and the client IP is the immediate peer.
	EnvTrustedProxies   = "DATUPLET_APPWORKER_TRUSTED_PROXIES"
	EnvTrustedProxyHops = "DATUPLET_APPWORKER_TRUSTED_PROXY_HOPS"

	EnvTimeoutS            = "DATUPLET_APPWORKER_TIMEOUT_S"
	EnvMaxTimeoutS         = "DATUPLET_APPWORKER_MAX_TIMEOUT_S"
	EnvMemoryMiB           = "DATUPLET_APPWORKER_MEMORY_MIB"
	EnvMaxMemoryMiB        = "DATUPLET_APPWORKER_MAX_MEMORY_MIB"
	EnvQueriesPerRender    = "DATUPLET_APPWORKER_QUERIES_PER_RENDER"
	EnvMaxQueriesPerRender = "DATUPLET_APPWORKER_MAX_QUERIES_PER_RENDER"
	EnvOutputDocMaxBytes   = "DATUPLET_APPWORKER_OUTPUT_DOC_MAX_BYTES"
	EnvBundleMaxBytes      = "DATUPLET_APPWORKER_BUNDLE_MAX_BYTES"
	EnvPerAppInflight      = "DATUPLET_APPWORKER_PER_APP_INFLIGHT"
	EnvConcurrency         = "DATUPLET_APPWORKER_CONCURRENCY"
)

// Defaults + hard caps: spec §7 limits table, verbatim. Also mirrored in
// charts/datuplet-app/values.yaml's appWorker.render.* block (Part 7):
//
//	render: {timeoutS: 10, maxTimeoutS: 30, memoryMiB: 128, maxMemoryMiB: 256,
//	  queriesPerRender: 10, maxQueriesPerRender: 25, outputDocMaxBytes: 2097152,
//	  bundleMaxBytes: 5242880, perAppInflight: 2, concurrency: 8}
//
// HardCap* are the spec's absolute ceilings: the Max* fields below are
// themselves operator-tunable via env, but are clamped to these ceilings —
// they are not a second layer of operator-adjustable headroom.
const (
	// DefaultListenAddr is app-worker's default HTTP listen address.
	DefaultListenAddr = ":8090"

	DefaultTimeoutS = 10
	HardCapTimeoutS = 30

	DefaultMemoryMiB = 128
	HardCapMemoryMiB = 256

	DefaultQueriesPerRender = 10
	HardCapQueriesPerRender = 25

	// DefaultOutputDocMaxBytes = 2 MiB. Spec §7's Cap column reads "—", but
	// §6.3 states "Structural caps: ≤64 blocks/doc, OutputDoc ≤2 MiB" — this
	// is a hard structural ceiling app-worker itself enforces (W5), not a
	// mere default: an operator raising it would let a guest return a doc
	// larger than the "OutputDoc buffer (≤2 MiB)" §7's own per-pod render-slot
	// sizing math budgets per render. HardCapOutputDocMaxBytes carries the
	// same value; operators may configure LOWER, never higher.
	DefaultOutputDocMaxBytes = 2097152
	HardCapOutputDocMaxBytes = 2097152
	// DefaultBundleMaxBytes = 5242880 (contract literal; spec §7 calls it
	// "5 MB"). Spec §7's Cap column also reads "—", but §4 states app
	// bundles are "≤ 5 MB" as a design property, and pipeline-api's store
	// (Part 2) enforces this authoritatively with ErrBundleTooLarge — this
	// clamp is lower-risk (app-worker never accepts uploads) but gets the
	// same treatment for a uniform config-surface rule: clamp to the spec
	// maximum, don't special-case. HardCapBundleMaxBytes carries the same
	// value; operators may configure LOWER, never higher.
	DefaultBundleMaxBytes = 5242880
	HardCapBundleMaxBytes = 5242880
	// DefaultPerAppInflight has no cap column in spec §7 ("—") and no
	// structural-cap statement elsewhere in the spec (§7's own capacity
	// math treats it as the tunable that capacity planning solves for) — a
	// genuinely open operator knob, not clamped.
	DefaultPerAppInflight = 2
	// DefaultConcurrency is the render worker's own goroutine-pool size
	// (contract-and-constraints.md values.yaml block); not itself a spec §7
	// limits-table row and not called out anywhere else in the spec as a
	// structural ceiling — a genuinely open operator knob, not clamped.
	DefaultConcurrency = 8

	// wazeroPageBytes: a WASM linear-memory page is 64 KiB, fixed by the
	// WASM spec (not configurable).
	wazeroPageBytes = 64 * 1024
)

// RenderConfig holds the render-side limits, mirroring appWorker.render.* in
// values.yaml (chart Part 7) and the spec §7 limits table.
//
// Clamp policy (documented decision, not error): every *Default-style field
// (TimeoutS, MemoryMiB, QueriesPerRender) is clamped to its paired Max*
// field, and every Max* field is itself clamped to the spec's hard ceiling
// (HardCapTimeoutS / HardCapMemoryMiB / HardCapQueriesPerRender).
// OutputDocMaxBytes and BundleMaxBytes have no paired Max* field (spec §7's
// Cap column reads "—" for both) but DO have spec-stated structural ceilings
// elsewhere (§6.3: OutputDoc ≤2 MiB; §4: bundles ≤5 MB) — both clamp
// directly to HardCapOutputDocMaxBytes / HardCapBundleMaxBytes using the
// same clamp helper as everything else, so the config surface has one
// uniform rule instead of two special cases. An invalid or non-positive
// override (unparseable, zero, negative) falls back to the default instead
// of clamping, since there is no sane "ceiling" to clamp a garbage value
// toward.
//
// PerAppInflight and Concurrency are genuinely open operator knobs: neither
// has a Max* pair, a spec §7 Cap value, nor a structural-ceiling statement
// elsewhere in the spec, so neither is clamped.
//
// This mirrors the existing project convention in
// components/queryengine/cmd/query-worker/server.go, where a per-request
// timeout/max_rows/max_bytes above the worker's ceiling is clamped to that
// ceiling rather than rejected — an operator typo or an over-eager
// values.yaml override degrades safely to the nearest safe limit instead of
// crash-looping the pod at boot or (worse) silently allowing a
// resource-exhausting configuration through.
type RenderConfig struct {
	TimeoutS    int
	MaxTimeoutS int

	MemoryMiB    int
	MaxMemoryMiB int

	QueriesPerRender    int
	MaxQueriesPerRender int

	OutputDocMaxBytes int
	BundleMaxBytes    int
	PerAppInflight    int
	Concurrency       int
}

// DefaultTrustedProxyHops is the hop count assumed when trusted-proxy CIDRs
// ARE configured but no explicit hop count is: one trusted proxy in front of
// app-worker (the cluster ingress). It has no effect at all when no CIDRs are
// configured — trust is anchored on the peer address, never on a hop count
// alone.
const DefaultTrustedProxyHops = 1

// ProxyTrust describes which immediate peers are allowed to speak for a
// client via X-Forwarded-For, and how many trusted hops sit in front of
// app-worker.
//
// The zero value means "trust nobody", which is the SAFE DEFAULT and the
// value a pod runs with until the chart (Part 7 / D1) sets it. Rationale: an
// unauthenticated attacker who can reach app-worker directly controls every
// byte of X-Forwarded-For, so honoring it without first verifying the peer
// would make the (client IP, app) verify-failure limiter — the only bound on
// brute-forcing a live viewer token id (spec §5.3) — bypassable by rotating
// one header.
//
// D1 (chart) sets these once D0 establishes the real topology: whether
// traffic terminates at the cluster ingress, at a reverse proxy inside
// pipeline-api, or directly in app-worker. Until then, leaving them unset is
// correct rather than conservative: keying on the peer address over-groups
// viewers behind a shared proxy (they share one bucket) but never lets an
// attacker escape a bucket.
type ProxyTrust struct {
	// CIDRs are the networks a trusted proxy may connect from. Empty
	// disables XFF parsing completely.
	CIDRs []netip.Prefix
	// Hops is the number of trusted proxies in the chain, counting the
	// immediate peer. 1 = a single ingress; 2 = e.g. a CDN in front of the
	// ingress. Used to skip trusted hops whose addresses cannot be
	// enumerated as CIDRs.
	Hops int
}

// Enabled reports whether any proxy is trusted to supply X-Forwarded-For.
func (t ProxyTrust) Enabled() bool { return len(t.CIDRs) > 0 }

// Contains reports whether ip (a textual address) falls inside a trusted
// CIDR. An unparseable address is never trusted.
func (t ProxyTrust) Contains(ip string) bool {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	addr = addr.Unmap()
	for _, p := range t.CIDRs {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// parseTrustedProxies parses a comma-separated list of CIDRs and/or bare IP
// addresses (a bare address becomes a host-sized prefix). Unparseable entries
// are DROPPED rather than failing boot: the failure direction is "trust
// less", never "trust more", and LoadConfig never errors (see its doc).
func parseTrustedProxies(list string) []netip.Prefix {
	var out []netip.Prefix
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if p, err := netip.ParsePrefix(part); err == nil {
			out = append(out, p.Masked())
			continue
		}
		if a, err := netip.ParseAddr(part); err == nil {
			a = a.Unmap()
			out = append(out, netip.PrefixFrom(a, a.BitLen()))
		}
	}
	return out
}

// Config is app-worker's full boot-time configuration, assembled once by
// LoadConfig from the environment.
type Config struct {
	// APIURL is pipeline-api's base URL (DATUPLET_API_URL). W3 uses it to
	// build the internal-API client; not dialed here.
	APIURL string
	// ListenAddr is app-worker's HTTP listen address.
	ListenAddr string
	// ServiceTokenFile points at the mounted service-credential file
	// app-worker presents as Authorization to pipeline-api's internal
	// routes (Part 2). Not read here — W3 owns the client.
	ServiceTokenFile string
	// CookieKeyFile points at the mounted HMAC key file for the viewer
	// session cookie (spec §5.3). Not read here — a later task owns cookie
	// signing.
	CookieKeyFile string
	// TrustedProxies decides whether X-Forwarded-For may be believed at all
	// (see ProxyTrust). Zero value = trust nobody.
	TrustedProxies ProxyTrust
	Render         RenderConfig
}

// MemoryPages converts Render.MemoryMiB into wazero linear-memory pages (a
// page is 64 KiB, so pages = MiB * 16), clamped to the 256 MiB hard cap
// (4096 pages). Serve calls appengine.NewEngine(ctx, cfg.MemoryPages()) at
// boot; later tasks (W3+) depend on this exact name and signature
// (contract-and-constraints.md).
func (c Config) MemoryPages() uint32 {
	miB := c.Render.MemoryMiB
	if miB > HardCapMemoryMiB {
		miB = HardCapMemoryMiB
	}
	if miB < 0 {
		miB = 0
	}
	const pagesPerMiB = 1024 * 1024 / wazeroPageBytes // = 16
	return uint32(miB) * pagesPerMiB
}

// LoadConfig reads app-worker's configuration from the environment, applying
// the spec §7 defaults and clamping any override to its cap (see
// RenderConfig's clamp-policy doc). It never errors or exits: a
// missing/invalid/non-positive env var falls back to the default, and an
// otherwise-valid but out-of-range value clamps rather than failing boot.
func LoadConfig() Config {
	maxTimeoutS := clampToHardCap(envIntDefault(EnvMaxTimeoutS, HardCapTimeoutS), HardCapTimeoutS)
	timeoutS := clampToHardCap(envIntDefault(EnvTimeoutS, DefaultTimeoutS), maxTimeoutS)

	maxMemoryMiB := clampToHardCap(envIntDefault(EnvMaxMemoryMiB, HardCapMemoryMiB), HardCapMemoryMiB)
	memoryMiB := clampToHardCap(envIntDefault(EnvMemoryMiB, DefaultMemoryMiB), maxMemoryMiB)

	maxQueriesPerRender := clampToHardCap(envIntDefault(EnvMaxQueriesPerRender, HardCapQueriesPerRender), HardCapQueriesPerRender)
	queriesPerRender := clampToHardCap(envIntDefault(EnvQueriesPerRender, DefaultQueriesPerRender), maxQueriesPerRender)

	return Config{
		APIURL:           os.Getenv(EnvAPIURL),
		ListenAddr:       envStringDefault(EnvListenAddr, DefaultListenAddr),
		ServiceTokenFile: os.Getenv(EnvServiceTokenFile),
		CookieKeyFile:    os.Getenv(EnvCookieKeyFile),
		TrustedProxies: ProxyTrust{
			CIDRs: parseTrustedProxies(os.Getenv(EnvTrustedProxies)),
			Hops:  envIntDefault(EnvTrustedProxyHops, DefaultTrustedProxyHops),
		},
		Render: RenderConfig{
			TimeoutS:            timeoutS,
			MaxTimeoutS:         maxTimeoutS,
			MemoryMiB:           memoryMiB,
			MaxMemoryMiB:        maxMemoryMiB,
			QueriesPerRender:    queriesPerRender,
			MaxQueriesPerRender: maxQueriesPerRender,
			OutputDocMaxBytes:   clampToHardCap(envIntDefault(EnvOutputDocMaxBytes, DefaultOutputDocMaxBytes), HardCapOutputDocMaxBytes),
			BundleMaxBytes:      clampToHardCap(envIntDefault(EnvBundleMaxBytes, DefaultBundleMaxBytes), HardCapBundleMaxBytes),
			PerAppInflight:      envIntDefault(EnvPerAppInflight, DefaultPerAppInflight),
			Concurrency:         envIntDefault(EnvConcurrency, DefaultConcurrency),
		},
	}
}

// clampToHardCap returns cap if v exceeds it, else v unchanged.
func clampToHardCap(v, cap int) int {
	if v > cap {
		return cap
	}
	return v
}

// envStringDefault returns the value of key, or fallback if unset/empty.
func envStringDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envIntDefault parses key as a decimal integer, falling back to def when
// the env var is unset, unparseable, or non-positive (zero/negative are
// never sane limits here, so they revert to the default rather than
// propagating).
func envIntDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
