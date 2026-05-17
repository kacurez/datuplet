package icebergjob

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// signTestToken creates a self-signed HS256 JWT with the given claims for
// testing ParseSnapshotSummaryFromToken. Signature verification is not
// exercised here — we use ParseUnverified, so any signing method works.
func signTestToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("sign test token: %v", err)
	}
	return signed
}

// TestBuildSnapshotSummary_RunToken verifies that a standard run-token
// (token_kind="run") produces:
//   - datuplet.actor from the "actor" claim
//   - datuplet.run-id from runID
//   - datuplet.run-mode = "cluster"
//   - datuplet.pipeline-api from the "iss" claim
func TestBuildSnapshotSummary_RunToken(t *testing.T) {
	t.Parallel()
	raw := signTestToken(t, jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-catalog",
		"sub":        "some-run-uuid",
		"actor":      "user-uuid-123",
		"token_kind": "run",
		"exp":        time.Now().Add(time.Hour).Unix(),
	})

	props := BuildSnapshotSummary(raw, "run-abc-123")
	if props == nil {
		t.Fatal("BuildSnapshotSummary returned nil for valid run-token")
	}

	cases := []struct{ key, want string }{
		{"datuplet.actor", "user-uuid-123"},
		{"datuplet.run-id", "run-abc-123"},
		{"datuplet.run-mode", "cluster"},
		{"datuplet.pipeline-api", "datuplet-api"},
	}
	for _, c := range cases {
		got, ok := props[c.key]
		if !ok {
			t.Errorf("key %q missing from props (got %v)", c.key, props)
			continue
		}
		if got != c.want {
			t.Errorf("props[%q] = %q; want %q", c.key, got, c.want)
		}
	}
}

// TestBuildSnapshotSummary_LocalCLIToken verifies that a local-cli token
// (token_kind="local-cli") maps run-mode to "local-cli".
func TestBuildSnapshotSummary_LocalCLIToken(t *testing.T) {
	t.Parallel()
	raw := signTestToken(t, jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-catalog",
		"sub":        "some-run-uuid",
		"actor":      "laptop-user-456",
		"token_kind": "local-cli",
		"exp":        time.Now().Add(time.Hour).Unix(),
	})

	props := BuildSnapshotSummary(raw, "run-local-789")
	if props == nil {
		t.Fatal("BuildSnapshotSummary returned nil for valid local-cli token")
	}

	if got := props["datuplet.run-mode"]; got != "local-cli" {
		t.Errorf("run-mode = %q; want %q", got, "local-cli")
	}
	if got := props["datuplet.actor"]; got != "laptop-user-456" {
		t.Errorf("actor = %q; want %q", got, "laptop-user-456")
	}
	if got := props["datuplet.run-id"]; got != "run-local-789" {
		t.Errorf("run-id = %q; want %q", got, "run-local-789")
	}
}

// TestBuildSnapshotSummary_EmptyToken verifies that an empty token string
// returns nil (no panic, no audit keys written).
func TestBuildSnapshotSummary_EmptyToken(t *testing.T) {
	t.Parallel()
	props := BuildSnapshotSummary("", "run-irrelevant")
	if props != nil {
		t.Errorf("expected nil for empty token, got %v", props)
	}
}

// TestBuildSnapshotSummary_MalformedToken verifies that a non-JWT string
// returns nil without panicking.
func TestBuildSnapshotSummary_MalformedToken(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{
		"notajwt",
		"a.b",                     // two segments (need three)
		"a.b.c.d",                 // four segments
		strings.Repeat("x", 512), // long garbage
	} {
		bad := bad
		t.Run(bad[:min(len(bad), 20)], func(t *testing.T) {
			t.Parallel()
			props := BuildSnapshotSummary(bad, "run-x")
			if props != nil {
				t.Errorf("expected nil for malformed token %q, got %v", bad, props)
			}
		})
	}
}

// TestBuildSnapshotSummary_UnknownTokenKind verifies that a JWT carrying an
// unrecognised token_kind (e.g. "service") returns nil instead of a map with
// an empty datuplet.run-mode key. This prevents mislabeling a snapshot when a
// non-run/non-local-cli token reaches iceberg-job by mistake.
func TestBuildSnapshotSummary_UnknownTokenKind(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"service", "user", "impersonation", "anything-else", ""} {
		kind := kind
		t.Run("token_kind="+kind, func(t *testing.T) {
			t.Parallel()
			raw := signTestToken(t, jwt.MapClaims{
				"iss":        "datuplet-api",
				"aud":        "datuplet-catalog",
				"sub":        "some-uuid",
				"actor":      "user-uuid",
				"token_kind": kind,
				"exp":        time.Now().Add(time.Hour).Unix(),
			})
			props := BuildSnapshotSummary(raw, "run-x")
			if props != nil {
				t.Errorf("expected nil for unknown token_kind=%q, got %v", kind, props)
			}
		})
	}
}

// TestRunModeFromTokenKind verifies the mapping table.
func TestRunModeFromTokenKind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		kind string
		want string
	}{
		{"run", "cluster"},
		{"local-cli", "local-cli"},
		{"user", ""},
		{"impersonation", ""},
		{"", ""},
		{"unknown", ""},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			got := RunModeFromTokenKind(tc.kind)
			if got != tc.want {
				t.Errorf("RunModeFromTokenKind(%q) = %q; want %q", tc.kind, got, tc.want)
			}
		})
	}
}

// TestBuildSnapshotSummary_InjectedRunModeClaimIsIgnored pins the
// unforgeability invariant: even if a caller embeds a "datuplet.run-mode"
// key directly in the JWT claims body (e.g. a hypothetical malicious actor
// who can craft their own token), that claim is IGNORED.
// BuildSnapshotSummary derives run-mode exclusively from token_kind.
//
// A JWT claim key containing a dot ("datuplet.run-mode") is unusual but
// syntactically valid per RFC 7519; this test confirms the code never reads
// such a claim regardless of its value.
func TestBuildSnapshotSummary_InjectedRunModeClaimIsIgnored(t *testing.T) {
	t.Parallel()

	// Build a run-token that also carries a forged "datuplet.run-mode" claim
	// attempting to override the legitimate derived value.
	raw := signTestToken(t, jwt.MapClaims{
		"iss":               "datuplet-api",
		"aud":               "datuplet-catalog",
		"sub":               "attacker-run-uuid",
		"actor":             "attacker-user",
		"token_kind":        "run",
		"datuplet.run-mode": "forged-value", // must be ignored
		"exp":               time.Now().Add(time.Hour).Unix(),
	})

	props := BuildSnapshotSummary(raw, "run-forged-001")
	if props == nil {
		t.Fatal("BuildSnapshotSummary returned nil for valid run-token with injected claim")
	}

	// run-mode must be "cluster" (from token_kind="run"), not "forged-value"
	// (from the injected datuplet.run-mode claim).
	if got := props["datuplet.run-mode"]; got != "cluster" {
		t.Errorf("datuplet.run-mode = %q; want %q (injected claim must not override token_kind-derived value)",
			got, "cluster")
	}
}

// TestBuildSnapshotSummary_EnvVarDoesNotControlRunMode documents the
// structural guarantee that env vars cannot influence the run-mode audit
// key. BuildSnapshotSummary has no env-var reads — it only reads JWT claims
// — so this is enforced at the type/call-graph level, not by runtime logic.
// This test exists to make the invariant searchable and to fail loudly if
// the function signature ever changes to accept an env-derived argument.
func TestBuildSnapshotSummary_EnvVarDoesNotControlRunMode(t *testing.T) {
	t.Parallel()

	// The function signature is: BuildSnapshotSummary(rawToken string, runID string).
	// No env parameter, no context parameter that could carry env state.
	// Verify the run-mode in the output matches the token_kind-derived value
	// regardless of what RUN_MODE would be set to in the environment.
	//
	// We intentionally do NOT call t.Setenv here — the point is that there
	// is no env var to set; the function simply does not read any.
	raw := signTestToken(t, jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-catalog",
		"sub":        "some-run-uuid",
		"actor":      "user-uuid",
		"token_kind": "local-cli",
		"exp":        time.Now().Add(time.Hour).Unix(),
	})

	props := BuildSnapshotSummary(raw, "run-env-test-001")
	if props == nil {
		t.Fatal("BuildSnapshotSummary returned nil for valid local-cli token")
	}

	// Must be "local-cli" from token_kind — not overridable via env.
	if got := props["datuplet.run-mode"]; got != "local-cli" {
		t.Errorf("datuplet.run-mode = %q; want %q", got, "local-cli")
	}
}

// min is a Go 1.20 builtin; keep a local copy for test compilation compatibility.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
