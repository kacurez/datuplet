package vegaspec

import (
	"os"
	"testing"
)

// shellSchemaPath is pkg/appengine/vegaspec/ -> repo root -> ui/appshell/.
// Go test binaries run with the package directory as the working directory
// regardless of where `go test` was invoked from, so this relative path is
// stable in CI and locally.
const shellSchemaPath = "../../../ui/appshell/vegaspec.schema.json"

// TestVegaSchemaInSyncWithShell is the shared-schema drift guard required by
// task-V0-brief.md ("Shared Vega schema", finding 18) and documented in this
// package's doc comment: ui/appshell/vegaspec.schema.json (vendored into the
// browser shell, RFC 028 Part 4, for client-side defense-in-depth validation)
// must be a byte-for-byte copy of THIS package's schema.json — the
// authoritative artifact app-worker (W2/W5) enforces server-side.
//
// `make sync-appshell-schema` regenerates the shell's copy; CI runs that
// target and then `git diff --exit-code ui/appshell/vegaspec.schema.json` to
// catch drift in a PR. This test is the same check inside `go test ./...`,
// so a local dev loop (or any CI job that only runs `go test`) catches a
// forgotten sync without needing the separate shell-specific CI job.
func TestVegaSchemaInSyncWithShell(t *testing.T) {
	shellCopy, err := os.ReadFile(shellSchemaPath)
	if err != nil {
		t.Fatalf("cannot read %s — run `make sync-appshell-schema`: %v", shellSchemaPath, err)
	}
	if string(shellCopy) != schemaJSON {
		t.Fatalf("%s is out of sync with pkg/appengine/vegaspec/schema.json (the authoritative "+
			"artifact) — run `make sync-appshell-schema` and commit the result", shellSchemaPath)
	}
}
