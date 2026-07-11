// Package framework — fixture setup helpers.
//
// PreCheck validates K8s infrastructure availability on the current kubectl
// context. Set DATUPLET_E2E_CONTEXT to guard against running against an
// unintended cluster.
package framework

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// PreCheck validates that the K8s infrastructure is available on the
// CURRENT kubectl context. It never switches contexts. Set
// DATUPLET_E2E_CONTEXT to guard against running the suite against an
// unintended cluster (mismatch = error, not a silent switch).
func PreCheck() error {
	out, err := exec.Command("kubectl", "config", "current-context").Output()
	if err != nil {
		return fmt.Errorf("kubectl context not available: %w", err)
	}
	current := strings.TrimSpace(string(out))
	if want := os.Getenv("DATUPLET_E2E_CONTEXT"); want != "" && current != want {
		return fmt.Errorf("kubectl context is %q, DATUPLET_E2E_CONTEXT wants %q — switch manually", current, want)
	}

	// The real availability check: the operator Deployment exists in the
	// e2e namespace (name is not release-prefixed — the chart renders a
	// bare `pipeline-operator`).
	ns := os.Getenv("DATUPLET_E2E_NAMESPACE")
	if ns == "" {
		ns = "datuplet-e2e"
	}
	if err := exec.Command("kubectl", "get", "deploy", "pipeline-operator", "-n", ns).Run(); err != nil {
		return fmt.Errorf("pipeline-operator not deployed in %s namespace (context %s): %w", ns, current, err)
	}
	return nil
}
