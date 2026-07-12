package framework

import (
	"os"
	"testing"
)

// SkipOrFail marks an infrastructure-availability gap. Locally it skips
// (fast iteration against a partial stack); under E2E_REQUIRE=1 (set by
// CI) it FAILS — a green CI run must mean the suite actually ran.
// Deliberate gates (opt-in proofs, environment-capability detection,
// the E2E_K8S mode switch) stay plain t.Skip.
func SkipOrFail(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv("E2E_REQUIRE") == "1" {
		t.Fatalf("E2E_REQUIRE=1, refusing to skip: "+format, args...)
	}
	t.Skipf(format, args...)
}
