// Package framework — K8s tier backend for the e2e test harness.
//
// The K8s tier applies PipelineRun YAML directly via kubectl rather
// than going through pipeline-api's HTTP trigger. That sidesteps the
// session-cookie auth surface, so we mint our own per-run JWT
// in-process with the same shape pipeline-api would use, write it to
// a Secret the operator can read, and patch the PipelineRun to
// reference it.
//
// Key design decisions:
//
//   - **Per-project namespace.** PipelineRuns land in
//     `datuplet-<project-uuid>`; the harness mirrors that. The
//     namespace is created (if absent) at fixture init via kubectl.
//
//   - **Single-token shape.** The Secret carries one JWT under the
//     `token` key. The token is bound to a chosen test user via
//     MintTestUserRunToken so FGA grants gate lakekeeper access.
//
//   - **Test-user binding.** Every K8sBackend.RunPipeline call mints
//     against k.RunAsUser; defaults to AliceID (project_admin) so
//     pre-existing scenarios keep working without per-scenario
//     user-selection plumbing. FGA-matrix tests override per scenario.
package framework

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// K8sBackend implements Backend by applying K8s CRDs via kubectl.
// Constructed with a per-suite FGAHarness so every run is scoped to
// the harness's lakekeeper Project (and per-project namespace).
type K8sBackend struct {
	Harness   *FGAHarness
	Namespace string
	RunPrefix string
	// RunAsUser names the TestUser whose identity the per-run JWT
	// carries. Defaults to AliceID (project_admin) so existing
	// scenarios pass without modification. FGA-matrix scenarios
	// override per-scenario.
	RunAsUser uuid.UUID

	Resources []string // resource names to clean up (e.g. "pipelinerun/foo")
}

// NewK8sBackend creates a K8sBackend bound to the harness's
// per-project namespace. Idempotent: re-creating the same namespace
// is a kubectl no-op via --dry-run filtering.
func NewK8sBackend(h *FGAHarness, runPrefix string) (*K8sBackend, error) {
	if h == nil {
		return nil, errors.New("NewK8sBackend: harness is required")
	}
	if h.LakekeeperProjectID == "" {
		return nil, errors.New("NewK8sBackend: harness has no LakekeeperProjectID")
	}
	// We use the lakekeeper project UUID as the K8s project UUID for
	// e2e — the harness-allocated project plays both roles. Production
	// splits these (Postgres-side Datuplet project UUID drives the
	// namespace, lakekeeper UUID lives on a label) but for e2e they
	// converge.
	lkProjectUUID, err := uuid.Parse(h.LakekeeperProjectID)
	if err != nil {
		return nil, fmt.Errorf("parse lakekeeper project UUID %q: %w", h.LakekeeperProjectID, err)
	}
	ns := "datuplet-" + lkProjectUUID.String()
	if err := ensureNamespace(context.Background(), ns, lkProjectUUID); err != nil {
		return nil, fmt.Errorf("ensure namespace %q: %w", ns, err)
	}
	return &K8sBackend{
		Harness:   h,
		Namespace: ns,
		RunPrefix: runPrefix,
		RunAsUser: AliceID,
	}, nil
}

// ensureNamespace kubectl-applies a Namespace with the
// datuplet.io/project-id label so the operator's
// namespace/project-id cross-check stays satisfied. Idempotent.
func ensureNamespace(ctx context.Context, name string, projectID uuid.UUID) error {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
  labels:
    datuplet.io/project-id: %s
`, name, projectID.String())
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply Namespace: %w\n%s", err, string(out))
	}
	return nil
}

// RunPipeline applies the pipeline YAML and polls until completion.
func (k *K8sBackend) RunPipeline(ctx context.Context, pipelineYAML string, opts RunOpts) (*RunResult, error) {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()

	prName, err := extractResourceName(pipelineYAML, "PipelineRun")
	if err != nil {
		return nil, fmt.Errorf("extracting PipelineRun name: %w", err)
	}

	runID := uuid.New()
	tokenSecretName := prName + "-token"
	if err := k.attachRunToken(ctx, pipelineYAML, prName, tokenSecretName, runID); err != nil {
		return nil, fmt.Errorf("attach run token: %w", err)
	}

	out, err := exec.CommandContext(ctx, "kubectl", "apply", "-f", pipelineYAML).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("kubectl apply failed: %w\n%s", err, string(out))
	}
	k.Resources = append(k.Resources, "pipelinerun/"+prName)
	k.Resources = append(k.Resources, "secret/"+tokenSecretName)

	pName, err := extractResourceName(pipelineYAML, "Pipeline")
	if err == nil && pName != "" {
		k.Resources = append(k.Resources, "pipeline/"+pName)
	}

	var phase string
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for PipelineRun %s: last phase=%q", prName, phase)
		case <-ticker.C:
			phaseOut, err := exec.CommandContext(ctx, "kubectl", "get", "pipelinerun", prName,
				"-n", k.Namespace,
				"-o", "jsonpath={.status.phase}").Output()
			if err != nil {
				continue // resource may not exist yet
			}
			phase = strings.TrimSpace(string(phaseOut))

			switch phase {
			case "Succeeded":
				logs := k.fetchLogs(ctx, prName)
				return &RunResult{
					Success:  true,
					ExitCode: 0,
					Logs:     logs,
					Duration: time.Since(start),
					RunID:    runID,
				}, nil
			case "FailedUser":
				logs := k.fetchLogs(ctx, prName)
				return &RunResult{
					Success:       false,
					ExitCode:      1,
					FailureType:   "FailedUser",
					StatusMessage: k.fetchStatusMessage(ctx, prName),
					Logs:          logs,
					Duration:      time.Since(start),
					RunID:         runID,
				}, nil
			case "FailedApplication":
				logs := k.fetchLogs(ctx, prName)
				return &RunResult{
					Success:       false,
					ExitCode:      20,
					FailureType:   "FailedApplication",
					StatusMessage: k.fetchStatusMessage(ctx, prName),
					Logs:          logs,
					Duration:      time.Since(start),
					RunID:         runID,
				}, nil
			case "Pending", "Running", "":
				// keep polling
			default:
				logs := k.fetchLogs(ctx, prName)
				return &RunResult{
					Success:       false,
					FailureType:   phase,
					StatusMessage: k.fetchStatusMessage(ctx, prName),
					Logs:          logs,
					Duration:      time.Since(start),
					RunID:         runID,
				}, nil
			}
		}
	}
}

// fetchStatusMessage retrieves the .status.message from a PipelineRun.
func (k *K8sBackend) fetchStatusMessage(ctx context.Context, prName string) string {
	out, err := exec.CommandContext(ctx, "kubectl", "get", "pipelinerun", prName,
		"-n", k.Namespace,
		"-o", "jsonpath={.status.message}").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// fetchLogs retrieves logs from all pods associated with a PipelineRun.
func (k *K8sBackend) fetchLogs(ctx context.Context, prName string) string {
	podOut, err := exec.CommandContext(ctx, "kubectl", "get", "pods",
		"-n", k.Namespace,
		"-l", "datuplet.io/pipelinerun="+prName,
		"-o", "jsonpath={.items[*].metadata.name}").Output()
	if err != nil {
		return fmt.Sprintf("[error listing pods: %v]", err)
	}

	podNames := strings.Fields(strings.TrimSpace(string(podOut)))
	if len(podNames) == 0 {
		return "[no pods found]"
	}

	var logs strings.Builder
	for _, pod := range podNames {
		logOut, err := exec.CommandContext(ctx, "kubectl", "logs", pod,
			"-n", k.Namespace,
			"--all-containers").Output()
		if err != nil {
			fmt.Fprintf(&logs, "--- %s [error: %v] ---\n", pod, err)
			continue
		}
		fmt.Fprintf(&logs, "--- %s ---\n%s\n", pod, string(logOut))
	}
	return logs.String()
}

// Cleanup deletes all tracked resources. Best-effort — errors are
// joined and returned but never fatal.
func (k *K8sBackend) Cleanup(ctx context.Context) error {
	var errs []string

	for _, res := range k.Resources {
		out, err := exec.CommandContext(ctx, "kubectl", "delete", res,
			"-n", k.Namespace,
			"--ignore-not-found").CombinedOutput()
		if err != nil {
			errs = append(errs, fmt.Sprintf("delete %s: %v (%s)", res, err, strings.TrimSpace(string(out))))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

// attachRunToken mints the per-run JWT for k.RunAsUser, creates a
// Secret holding it, and rewrites the rendered YAML in place so the
// PipelineRun references the Secret via spec.runTokenRef.name AND
// every doc's metadata.namespace targets k.Namespace.
//
// The Secret carries a single `token` key with one JWT
// (sub=<run-uuid>, actor=<user-uuid>, jti=run-tok-<run-uuid>).
func (k *K8sBackend) attachRunToken(ctx context.Context, yamlPath, prName, tokenSecretName string, runID uuid.UUID) error {
	raw, err := os.ReadFile(yamlPath)
	if err != nil {
		return fmt.Errorf("read pipeline YAML: %w", err)
	}
	pipelineDoc, err := extractPipelineDoc(raw)
	if err != nil {
		return fmt.Errorf("find Pipeline doc in %s: %w", yamlPath, err)
	}
	pipelineName := pipelineNameFromYAML(pipelineDoc)

	if k.Harness == nil || k.Harness.Signer == nil {
		return errors.New("attachRunToken: harness or signer not initialised")
	}
	jwt, err := MintTestUserRunToken(ctx, k.Harness.Signer,
		k.RunAsUser, runID, k.Harness.LakekeeperProjectID, k.Harness.WarehouseName, pipelineName)
	if err != nil {
		return fmt.Errorf("mint run token: %w", err)
	}
	// Mirror pkg/pipelineapi/runbackend/k8s.go::TriggerRun step 2: write
	// the synthetic-run-user FGA tuple BEFORE the PipelineRun lands.
	// Without this, DG's first lakekeeper call (post-vended-creds) gets
	// 403 "list_warehouses forbidden" because the run-token's identity
	// (user:oidc~<run-uuid>) has no grants on the per-test lakekeeper
	// Project. The harness bypasses pipeline-api so production's
	// 4-step compensating ordering is collapsed: write tuple + apply
	// resources are both best-effort here, and cleanup() at scenario
	// end deletes the tuple.
	if k.Harness.Authorizer != nil {
		runUserTuple := authz.Tuple{
			User:     authz.UserObject(runID.String()).String(),
			Relation: "editor",
			Object:   authz.ProjectObject(k.Harness.LakekeeperProjectID),
		}
		if err := k.Harness.Authorizer.WriteTuples(ctx, []authz.Tuple{runUserTuple}); err != nil {
			return fmt.Errorf("write synthetic-run-user FGA tuple: %w", err)
		}
	}
	if err := createRunTokenSecret(ctx, k.Namespace, tokenSecretName, jwt, runID.String()); err != nil {
		return fmt.Errorf("create token secret: %w", err)
	}
	if err := rewriteYAMLWithRunToken(yamlPath, runID, tokenSecretName, k.Namespace, k.Harness.LakekeeperProjectID); err != nil {
		return fmt.Errorf("inject runTokenRef + namespace: %w", err)
	}
	return nil
}

// extractResourceName reads a YAML file and finds the metadata.name
// for a given kind. Simple line-by-line parser that handles
// multi-document YAML. Used by RunPipeline to discover the
// PipelineRun + Pipeline names for cleanup tracking.
func extractResourceName(yamlPath, kind string) (string, error) {
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", yamlPath, err)
	}

	docs := strings.Split(string(data), "---")
	target := "kind: " + kind

	for _, doc := range docs {
		scanner := bufio.NewScanner(strings.NewReader(doc))
		foundKind := false
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == target || line == target+" " {
				foundKind = true
				continue
			}
			if foundKind && strings.HasPrefix(line, "name:") {
				name := strings.TrimSpace(strings.TrimPrefix(line, "name:"))
				name = strings.Trim(name, "\"'") // strip YAML quotes
				if name != "" {
					return name, nil
				}
			}
		}
	}
	return "", fmt.Errorf("kind %s not found in %s", kind, yamlPath)
}
