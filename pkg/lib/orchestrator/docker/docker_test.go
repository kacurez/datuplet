package docker

import (
	"strings"
	"testing"
)

// TestRunTokenBind exercises the helper that builds the read-only
// bind-mount string for the per-run JWT map. The bind shape — host
// path, the dockerRunTokenMountPath constant, and the trailing ":ro" —
// is the load-bearing contract DG / table-commit read from inside the
// container.
func TestRunTokenBind(t *testing.T) {
	if got := runTokenBind(""); got != "" {
		t.Errorf("runTokenBind(\"\") = %q, want empty (no mount)", got)
	}

	host := "/var/lib/datuplet/runtokens/abc123/tokens.json"
	got := runTokenBind(host)
	if got == "" {
		t.Fatal("runTokenBind(non-empty): unexpectedly empty")
	}
	// Shape: <host>:<container>:ro
	parts := strings.SplitN(got, ":", 3)
	if len(parts) != 3 {
		t.Fatalf("expected 3 colon-separated segments, got %d in %q", len(parts), got)
	}
	if parts[0] != host {
		t.Errorf("host segment = %q, want %q", parts[0], host)
	}
	if parts[1] != dockerRunTokenMountPath {
		t.Errorf("container segment = %q, want %q (the standard /var/run/secrets/datuplet-runtoken/tokens path DG/TableCommit read)", parts[1], dockerRunTokenMountPath)
	}
	if parts[2] != "ro" {
		t.Errorf("flags segment = %q, want \"ro\" (the JWT map must never be writable from inside the container)", parts[2])
	}
}

// TestDockerRunTokenMountPath pins the in-container path so accidental
// changes get caught — DG (`pkg/datagateway/runtoken.go`) and
// TableCommit (`pkg/tablecommit/runtoken.go`) hard-code the same string
// when they're invoked via env vars; renaming this constant without
// updating those callers would silently break local-mode auth.
func TestDockerRunTokenMountPath(t *testing.T) {
	const want = "/var/run/secrets/datuplet-runtoken/tokens"
	if dockerRunTokenMountPath != want {
		t.Errorf("dockerRunTokenMountPath = %q, want %q", dockerRunTokenMountPath, want)
	}
}
