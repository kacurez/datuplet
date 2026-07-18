package schemalint

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// repoRoot locates the repository root relative to this test file, which lives
// at <root>/pkg/pipeline/schemalint/builtin_test.go.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
}

// TestLint_BuiltinsCleanZeroIssues is the "100% form coverage for built-ins"
// guarantee: every components/*/schema.json must lint with ZERO issues.
func TestLint_BuiltinsCleanZeroIssues(t *testing.T) {
	root := repoRoot(t)
	matches, err := filepath.Glob(filepath.Join(root, "components", "*", "schema.json"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no components/*/schema.json found under %s", root)
	}
	for _, path := range matches {
		path := path
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			issues := Lint(data)
			if len(issues) != 0 {
				for _, i := range issues {
					t.Errorf("%s: [%s] %s: %s", path, i.Rule, i.Path, i.Message)
				}
				t.Fatalf("%s: expected zero issues, got %d", path, len(issues))
			}
		})
	}
}
