// Package framework — fixture setup helpers.
//
// PreCheck validates K8s infrastructure availability. Requires an OrbStack
// context with `make deploy-local` having run `pipeline-api admin authz-bootstrap`.
package framework

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// PreCheck validates that the K8s infrastructure is available.
//
//   - K8s: kubectl on orbstack context + pipeline-operator deployed.
func PreCheck() error {
	out, err := exec.Command("kubectl", "config", "current-context").Output()
	if err != nil {
		return fmt.Errorf("kubectl context not available: %w", err)
	}
	if !strings.Contains(string(out), "orbstack") {
		contexts, err := exec.Command("kubectl", "config", "get-contexts", "-o", "name").Output()
		if err != nil {
			return fmt.Errorf("failed to list kubectl contexts: %w", err)
		}
		var orbCtx string
		for _, ctx := range strings.Split(strings.TrimSpace(string(contexts)), "\n") {
			if strings.Contains(ctx, "orbstack") {
				orbCtx = ctx
				break
			}
		}
		if orbCtx == "" {
			return fmt.Errorf("no orbstack kubectl context found (current: %q)", strings.TrimSpace(string(out)))
		}
		if err := exec.Command("kubectl", "config", "use-context", orbCtx).Run(); err != nil {
			return fmt.Errorf("failed to switch to orbstack context %q: %w", orbCtx, err)
		}
	}

	// pipeline-operator lives in the cluster-singleton e2e namespace
	// (default datuplet-e2e, override via DATUPLET_E2E_NAMESPACE);
	// PipelineRun apply happens in the per-project namespace but the
	// operator deployment is here.
	ns := os.Getenv("DATUPLET_E2E_NAMESPACE")
	if ns == "" {
		ns = "datuplet-e2e"
	}
	if err := exec.Command("kubectl", "get", "deploy", "pipeline-operator", "-n", ns).Run(); err != nil {
		return fmt.Errorf("pipeline-operator not deployed in %s namespace: %w", ns, err)
	}
	return nil
}
