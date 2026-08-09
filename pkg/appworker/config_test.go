package appworker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfigDefaults asserts LoadConfig's zero-env defaults match the
// spec §7 limits table verbatim (also mirrored in
// charts/datuplet-app/values.yaml's appWorker.render.* block, per
// contract-and-constraints.md).
func TestLoadConfigDefaults(t *testing.T) {
	cfg := LoadConfig()

	if cfg.ListenAddr != ":8090" {
		t.Errorf("ListenAddr = %q, want :8090", cfg.ListenAddr)
	}
	if cfg.APIURL != "" {
		t.Errorf("APIURL = %q, want empty (env unset)", cfg.APIURL)
	}
	if cfg.ServiceTokenFile != "" {
		t.Errorf("ServiceTokenFile = %q, want empty (env unset)", cfg.ServiceTokenFile)
	}
	if cfg.CookieKeyFile != "" {
		t.Errorf("CookieKeyFile = %q, want empty (env unset)", cfg.CookieKeyFile)
	}

	r := cfg.Render
	want := RenderConfig{
		TimeoutS:            10,
		MaxTimeoutS:         30,
		MemoryMiB:           128,
		MaxMemoryMiB:        256,
		QueriesPerRender:    10,
		MaxQueriesPerRender: 25,
		OutputDocMaxBytes:   2097152,
		BundleMaxBytes:      5242880,
		PerAppInflight:      2,
		Concurrency:         8,
	}
	if r != want {
		t.Errorf("Render = %+v, want %+v", r, want)
	}
}

// TestLoadConfigStringOverrides asserts plain string env vars pass through
// unmodified.
func TestLoadConfigStringOverrides(t *testing.T) {
	t.Setenv(EnvAPIURL, "https://api.example.internal")
	t.Setenv(EnvListenAddr, ":9999")
	t.Setenv(EnvServiceTokenFile, "/var/run/secrets/token")
	t.Setenv(EnvCookieKeyFile, "/var/run/secrets/cookie-key")

	cfg := LoadConfig()

	if cfg.APIURL != "https://api.example.internal" {
		t.Errorf("APIURL = %q", cfg.APIURL)
	}
	if cfg.ListenAddr != ":9999" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.ServiceTokenFile != "/var/run/secrets/token" {
		t.Errorf("ServiceTokenFile = %q", cfg.ServiceTokenFile)
	}
	if cfg.CookieKeyFile != "/var/run/secrets/cookie-key" {
		t.Errorf("CookieKeyFile = %q", cfg.CookieKeyFile)
	}
}

// TestLoadConfigTrustedProxies covers the proxy-trust config surface, whose
// SAFE DEFAULT (trust nobody, ignore X-Forwarded-For) is what keeps the
// verify-failure limiter unbypassable until Part 7's D1 sets the real
// topology.
func TestLoadConfigTrustedProxies(t *testing.T) {
	t.Run("default trusts nobody", func(t *testing.T) {
		cfg := LoadConfig()
		if cfg.TrustedProxies.Enabled() {
			t.Fatalf("TrustedProxies must be disabled by default, got %+v", cfg.TrustedProxies)
		}
		if cfg.TrustedProxies.Hops != DefaultTrustedProxyHops {
			t.Errorf("Hops = %d, want %d", cfg.TrustedProxies.Hops, DefaultTrustedProxyHops)
		}
	})

	t.Run("CIDRs and bare addresses both parse", func(t *testing.T) {
		t.Setenv(EnvTrustedProxies, "10.0.0.0/8, 192.168.1.5 ,::1")
		cfg := LoadConfig()
		if !cfg.TrustedProxies.Enabled() {
			t.Fatal("TrustedProxies must be enabled")
		}
		if n := len(cfg.TrustedProxies.CIDRs); n != 3 {
			t.Fatalf("CIDRs = %d (%v), want 3", n, cfg.TrustedProxies.CIDRs)
		}
		for _, ip := range []string{"10.1.2.3", "192.168.1.5", "::1"} {
			if !cfg.TrustedProxies.Contains(ip) {
				t.Errorf("Contains(%q) = false, want true", ip)
			}
		}
		for _, ip := range []string{"11.1.2.3", "192.168.1.6", "not-an-ip", ""} {
			if cfg.TrustedProxies.Contains(ip) {
				t.Errorf("Contains(%q) = true, want false", ip)
			}
		}
	})

	t.Run("hop count override", func(t *testing.T) {
		t.Setenv(EnvTrustedProxies, "10.0.0.0/8")
		t.Setenv(EnvTrustedProxyHops, "2")
		if got := LoadConfig().TrustedProxies.Hops; got != 2 {
			t.Fatalf("Hops = %d, want 2", got)
		}
	})

	t.Run("garbage entries are dropped, never widened", func(t *testing.T) {
		t.Setenv(EnvTrustedProxies, "not-a-cidr, 10.0.0.0/8, 999.999.999.999/8, ")
		cfg := LoadConfig()
		if n := len(cfg.TrustedProxies.CIDRs); n != 1 {
			t.Fatalf("CIDRs = %v, want only the one valid entry", cfg.TrustedProxies.CIDRs)
		}
		if cfg.TrustedProxies.Contains("203.0.113.7") {
			t.Fatal("a dropped garbage entry must not widen trust")
		}
	})

	t.Run("invalid hop count falls back to the default", func(t *testing.T) {
		t.Setenv(EnvTrustedProxyHops, "-3")
		if got := LoadConfig().TrustedProxies.Hops; got != DefaultTrustedProxyHops {
			t.Fatalf("Hops = %d, want %d", got, DefaultTrustedProxyHops)
		}
	})
}

// TestLoadConfigCapsClampNotError is the load-bearing test for the
// clamp-vs-error decision: an operator override above a cap silently clamps
// to that cap rather than failing config load / boot. This mirrors the
// existing project convention in
// components/queryengine/cmd/query-worker/server.go (per-request
// timeout/max_rows/max_bytes clamp to the worker ceiling rather than being
// rejected).
func TestLoadConfigCapsClampNotError(t *testing.T) {
	t.Setenv(EnvTimeoutS, "100")            // > default maxTimeoutS (30)
	t.Setenv(EnvMemoryMiB, "99999")         // > default maxMemoryMiB (256)
	t.Setenv(EnvQueriesPerRender, "999")    // > default maxQueriesPerRender (25)
	t.Setenv(EnvMaxTimeoutS, "9999")        // > spec hard cap (30)
	t.Setenv(EnvMaxMemoryMiB, "999999")     // > spec hard cap (256)
	t.Setenv(EnvMaxQueriesPerRender, "500") // > spec hard cap (25)

	cfg := LoadConfig()
	r := cfg.Render

	if r.MaxTimeoutS != HardCapTimeoutS {
		t.Errorf("MaxTimeoutS = %d, want clamp to hard cap %d", r.MaxTimeoutS, HardCapTimeoutS)
	}
	if r.TimeoutS != HardCapTimeoutS {
		t.Errorf("TimeoutS = %d, want clamp to (clamped) MaxTimeoutS %d", r.TimeoutS, HardCapTimeoutS)
	}
	if r.MaxMemoryMiB != HardCapMemoryMiB {
		t.Errorf("MaxMemoryMiB = %d, want clamp to hard cap %d", r.MaxMemoryMiB, HardCapMemoryMiB)
	}
	if r.MemoryMiB != HardCapMemoryMiB {
		t.Errorf("MemoryMiB = %d, want clamp to (clamped) MaxMemoryMiB %d", r.MemoryMiB, HardCapMemoryMiB)
	}
	if r.MaxQueriesPerRender != HardCapQueriesPerRender {
		t.Errorf("MaxQueriesPerRender = %d, want clamp to hard cap %d", r.MaxQueriesPerRender, HardCapQueriesPerRender)
	}
	if r.QueriesPerRender != HardCapQueriesPerRender {
		t.Errorf("QueriesPerRender = %d, want clamp to (clamped) MaxQueriesPerRender %d", r.QueriesPerRender, HardCapQueriesPerRender)
	}
}

// TestLoadConfigOutputDocMaxBytesClampsToStructuralCap asserts an
// operator override above spec §6.3's structural OutputDoc ceiling (2 MiB)
// clamps to that ceiling rather than being honored verbatim (Codex review
// gate finding on commit 3d34d8d: spec §7's Cap column reads "—" for this
// row, but §6.3 calls ≤2 MiB a structural cap app-worker itself enforces).
func TestLoadConfigOutputDocMaxBytesClampsToStructuralCap(t *testing.T) {
	t.Setenv(EnvOutputDocMaxBytes, "104857600") // 100 MiB

	cfg := LoadConfig()

	if cfg.Render.OutputDocMaxBytes != HardCapOutputDocMaxBytes {
		t.Errorf("OutputDocMaxBytes = %d, want clamp to structural cap %d",
			cfg.Render.OutputDocMaxBytes, HardCapOutputDocMaxBytes)
	}
}

// TestLoadConfigBundleMaxBytesClampsToStructuralCap asserts an operator
// override above spec §4's "≤ 5 MB" bundle-size design property clamps to
// that ceiling. Lower-risk than OutputDoc (app-worker never accepts
// uploads; pipeline-api's store is the authoritative enforcer), but the
// config surface applies the same uniform rule rather than special-casing
// it.
func TestLoadConfigBundleMaxBytesClampsToStructuralCap(t *testing.T) {
	t.Setenv(EnvBundleMaxBytes, "52428800") // 50 MB

	cfg := LoadConfig()

	if cfg.Render.BundleMaxBytes != HardCapBundleMaxBytes {
		t.Errorf("BundleMaxBytes = %d, want clamp to structural cap %d",
			cfg.Render.BundleMaxBytes, HardCapBundleMaxBytes)
	}
}

// TestLoadConfigOutputDocAndBundleMaxBytesBelowCapHonored asserts an
// operator may configure LOWER values for both (the clamp is a ceiling,
// not a fixed value).
func TestLoadConfigOutputDocAndBundleMaxBytesBelowCapHonored(t *testing.T) {
	t.Setenv(EnvOutputDocMaxBytes, "1048576") // 1 MiB, below the 2 MiB cap
	t.Setenv(EnvBundleMaxBytes, "1000000")    // below the 5242880 cap

	cfg := LoadConfig()

	if cfg.Render.OutputDocMaxBytes != 1048576 {
		t.Errorf("OutputDocMaxBytes = %d, want 1048576 (below-cap override honored)", cfg.Render.OutputDocMaxBytes)
	}
	if cfg.Render.BundleMaxBytes != 1000000 {
		t.Errorf("BundleMaxBytes = %d, want 1000000 (below-cap override honored)", cfg.Render.BundleMaxBytes)
	}
}

// TestLoadConfigWithinCapHonored asserts a legitimate override that stays
// under its cap is honored verbatim (the clamp is a ceiling, not a reset to
// default).
func TestLoadConfigWithinCapHonored(t *testing.T) {
	t.Setenv(EnvTimeoutS, "20")
	t.Setenv(EnvMemoryMiB, "200")
	t.Setenv(EnvQueriesPerRender, "15")
	t.Setenv(EnvPerAppInflight, "5")
	t.Setenv(EnvConcurrency, "16")
	t.Setenv(EnvOutputDocMaxBytes, "1048576")
	t.Setenv(EnvBundleMaxBytes, "1000000")

	cfg := LoadConfig()
	r := cfg.Render

	if r.TimeoutS != 20 {
		t.Errorf("TimeoutS = %d, want 20", r.TimeoutS)
	}
	if r.MemoryMiB != 200 {
		t.Errorf("MemoryMiB = %d, want 200", r.MemoryMiB)
	}
	if r.QueriesPerRender != 15 {
		t.Errorf("QueriesPerRender = %d, want 15", r.QueriesPerRender)
	}
	if r.PerAppInflight != 5 {
		t.Errorf("PerAppInflight = %d, want 5", r.PerAppInflight)
	}
	if r.Concurrency != 16 {
		t.Errorf("Concurrency = %d, want 16", r.Concurrency)
	}
	if r.OutputDocMaxBytes != 1048576 {
		t.Errorf("OutputDocMaxBytes = %d, want 1048576", r.OutputDocMaxBytes)
	}
	if r.BundleMaxBytes != 1000000 {
		t.Errorf("BundleMaxBytes = %d, want 1000000", r.BundleMaxBytes)
	}
}

// TestLoadConfigInvalidFallsBackToDefault asserts a non-numeric or
// non-positive override falls back to the default rather than propagating
// a garbage/zero/negative value or panicking config load.
func TestLoadConfigInvalidFallsBackToDefault(t *testing.T) {
	t.Setenv(EnvTimeoutS, "not-a-number")
	t.Setenv(EnvMemoryMiB, "-5")
	t.Setenv(EnvQueriesPerRender, "0")

	cfg := LoadConfig()
	r := cfg.Render

	if r.TimeoutS != DefaultTimeoutS {
		t.Errorf("TimeoutS = %d, want default %d on invalid input", r.TimeoutS, DefaultTimeoutS)
	}
	if r.MemoryMiB != DefaultMemoryMiB {
		t.Errorf("MemoryMiB = %d, want default %d on negative input", r.MemoryMiB, DefaultMemoryMiB)
	}
	if r.QueriesPerRender != DefaultQueriesPerRender {
		t.Errorf("QueriesPerRender = %d, want default %d on zero input", r.QueriesPerRender, DefaultQueriesPerRender)
	}
}

// TestMemoryPages is the interface later tasks depend on verbatim: a page is
// 64 KiB, so pages = memoryMiB * 16, clamped to the 256 MiB cap (4096
// pages).
func TestMemoryPages(t *testing.T) {
	cases := []struct {
		name      string
		memoryMiB int
		want      uint32
	}{
		{"128MiB", 128, 2048},
		{"512MiBClampsTo256", 512, 4096},
		{"256MiBAtCap", 256, 4096},
		{"64MiB", 64, 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Render: RenderConfig{MemoryMiB: tc.memoryMiB, MaxMemoryMiB: HardCapMemoryMiB}}
			if got := cfg.MemoryPages(); got != tc.want {
				t.Errorf("MemoryPages() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestServePassesMemoryPagesToEngine asserts Serve calls the injected engine
// constructor with cfg.MemoryPages() at boot — the exact wiring W3+ depend on.
// A fake constructor is injected so this is assertable without compiling the
// real ~0.25s WASM engine (task-E1-report.md). Serve now also reads the
// mounted cookie-key and service-token Secrets before reaching the engine
// (W6 boot wiring), so the test supplies both plus a non-empty APIURL.
func TestServePassesMemoryPagesToEngine(t *testing.T) {
	cfg := Config{
		ListenAddr:       "127.0.0.1:0",
		APIURL:           "http://pipeline-api.test",
		CookieKeyFile:    writeTempSecret(t, "cookie-key", "cookie-hmac-key"),
		ServiceTokenFile: writeTempSecret(t, "service-token", "svc-token"),
		Render:           RenderConfig{MemoryMiB: 128, MaxMemoryMiB: HardCapMemoryMiB},
	}

	var gotPages uint32
	called := false
	fakeErr := errors.New("stop after boot: fake engine refuses to serve")
	newEngine := func(_ context.Context, pages uint32) (Engine, error) {
		called = true
		gotPages = pages
		// Return an error so Serve returns immediately after the boot call,
		// without needing to also stand up + tear down a real listener.
		return nil, fakeErr
	}

	err := Serve(context.Background(), cfg, newEngine)
	if !called {
		t.Fatal("Serve did not call the injected engine constructor")
	}
	if gotPages != cfg.MemoryPages() {
		t.Errorf("newEngine called with pages=%d, want cfg.MemoryPages()=%d", gotPages, cfg.MemoryPages())
	}
	if !errors.Is(err, fakeErr) {
		t.Errorf("Serve err = %v, want wrapped %v", err, fakeErr)
	}
}

// writeTempSecret writes content to a temp file and returns its path, for
// tests that exercise Serve's mounted-Secret reads.
func writeTempSecret(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp secret %s: %v", name, err)
	}
	return path
}
