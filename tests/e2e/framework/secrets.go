package framework

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// writeSecretsDir creates a temp directory and writes one file per entry in data
// (file contents == value). Returns the absolute path. Caller is responsible for
// removing the directory via os.RemoveAll.
func writeSecretsDir(data map[string]string) (string, error) {
	dir, err := os.MkdirTemp("", "datuplet-e2e-secrets-*")
	if err != nil {
		return "", err
	}
	for name, value := range data {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o400); err != nil {
			os.RemoveAll(dir)
			return "", fmt.Errorf("write %s: %w", name, err)
		}
	}
	return dir, nil
}

// createK8sSecret provisions a Kubernetes Secret in the given namespace
// with the given name and string data. Any existing Secret with the same
// name is replaced (delete + create) so a stale run cannot shadow the new
// values.
//
// Post-Slice-7 the harness applies PipelineRuns to the per-project
// namespace (`datuplet-<lakekeeper-project-uuid>`); `secretsRef`
// resolves within that namespace, so the Secret MUST live there too.
// Caller passes the per-project namespace from the K8sBackend.
func createK8sSecret(ctx context.Context, name, namespace string, data map[string]string) error {
	if namespace == "" {
		namespace = os.Getenv("DATUPLET_E2E_NAMESPACE")
		if namespace == "" {
			namespace = "datuplet-e2e"
		}
	}
	// Best-effort delete; ignore errors (e.g. NotFound).
	_ = exec.CommandContext(ctx, "kubectl", "delete", "secret", name, "-n", namespace, "--ignore-not-found").Run()

	args := []string{"create", "secret", "generic", name, "-n", namespace}
	for k, v := range data {
		args = append(args, fmt.Sprintf("--from-literal=%s=%s", k, v))
	}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl create secret: %w\n%s", err, string(out))
	}
	return nil
}

// deleteK8sSecret removes a Secret created by createK8sSecret. Errors are
// ignored so cleanup never fails a test that already passed.
func deleteK8sSecret(ctx context.Context, name, namespace string) {
	if namespace == "" {
		namespace = os.Getenv("DATUPLET_E2E_NAMESPACE")
		if namespace == "" {
			namespace = "datuplet-e2e"
		}
	}
	_ = exec.CommandContext(ctx, "kubectl", "delete", "secret", name, "-n", namespace, "--ignore-not-found").Run()
}
