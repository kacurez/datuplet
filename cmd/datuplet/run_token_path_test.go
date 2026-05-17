package main

import "testing"

// TestResolveRunTokenPath_FlagOverridesEnv pins the flag-over-env precedence
// rule for --run-token-path / RUN_TOKEN_PATH on both `datuplet gateway` and
// `datuplet iceberg-job`. A future env-var refactor must not invert this.
//
// Invariant: when both the flag and the env var are set, the explicit
// --run-token-path value wins. This prevents a container-level env var from
// silently overriding an operator-supplied flag path.
func TestResolveRunTokenPath_FlagOverridesEnv(t *testing.T) {
	t.Parallel()
	got := resolveRunTokenPath("/bar", "/foo")
	if got != "/bar" {
		t.Errorf("resolveRunTokenPath(flag=/bar, env=/foo) = %q; want /bar (flag must win)", got)
	}
}

// TestResolveRunTokenPath_FallbacksToEnv pins the fallback-to-env behaviour
// when --run-token-path is not supplied. This is the normal container
// entrypoint path: the K8s controller injects RUN_TOKEN_PATH and the flag
// is left empty.
func TestResolveRunTokenPath_FallbacksToEnv(t *testing.T) {
	t.Parallel()
	got := resolveRunTokenPath("", "/foo")
	if got != "/foo" {
		t.Errorf("resolveRunTokenPath(flag=, env=/foo) = %q; want /foo (env fallback must work)", got)
	}
}

// TestResolveRunTokenPath_BothEmpty pins the empty-input edge case: when
// neither flag nor env is provided, the resolved path is empty — the
// caller (gateway / iceberg-job) then operates without a run-token.
func TestResolveRunTokenPath_BothEmpty(t *testing.T) {
	t.Parallel()
	got := resolveRunTokenPath("", "")
	if got != "" {
		t.Errorf("resolveRunTokenPath(flag=, env=) = %q; want empty", got)
	}
}
