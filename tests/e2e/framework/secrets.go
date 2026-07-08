package framework

import (
	"fmt"
	"os"
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

// Managed-secrets era (RFC 026 P1.5): the old createK8sSecret/deleteK8sSecret
// pair hand-created a Secret named "<runPrefix>-secrets" for a pipeline's
// (now-deleted) spec.secretsRef.name to point at. That whole mechanism is
// gone — secrets are written through pipeline-api's write-only managed API
// (PUT /api/v1/projects/{pid}/secrets/{key}) into the single per-project
// Secret (datuplet-project-secrets), and referenced from component.config via
// a whole-scalar $[name]. See scenarios_secrets_test.go for the scenario that
// exercises this end to end.
