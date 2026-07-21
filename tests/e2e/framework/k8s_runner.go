// Package framework — K8s tier backend for the e2e test harness.
//
// RFC 027 E2: the K8s tier drives runs through pipeline-api's public REST
// surface — the same path a real user takes — instead of hand-crafting a
// PipelineRun CR and kubectl-applying it. RunPipeline authenticates as the
// scenario's user, PUTs the envelope-free PipelineDoc, POSTs a run, and polls
// the run to a terminal phase. pipeline-api's trigger path owns everything the
// harness used to do by hand: it mints the per-run JWT, writes the
// synthetic-run-user FGA tuple, and creates the PipelineRun CR + run-token /
// snapshot Secrets. The harness no longer mints tokens, writes tuples, or
// rewrites/apply-s CRs on the run path.
//
// Key design decisions:
//
//   - **Per-project namespace.** Runs land in `datuplet-<project-uuid>`. The
//     namespace is created (if absent) at fixture init via kubectl; pipeline-api
//     ensures its own project namespace at trigger time as well.
//
//   - **User binding via HTTP session.** Every K8sBackend.RunPipeline call
//     authenticates as k.RunAsUser (defaults to AliceID / project_admin) so
//     the run identity is the authenticated caller, exactly as in production.
//     apiCredsFor (run_http.go) maps the TestUser to login credentials.
//
//   - **Log + cleanup by run-id.** Component/gateway pods carry the
//     `datuplet.io/run-id` label (pkg/k8s/controllers/pipelinerun_jobs.go), so
//     logs are fetched and PipelineRuns are torn down by that label — no CR
//     name bookkeeping required.
package framework

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

// K8sBackend implements Backend by driving pipeline-api over HTTP.
// Constructed with a per-suite FGAHarness so every run is scoped to
// the harness's lakekeeper Project (and per-project namespace).
type K8sBackend struct {
	Harness   *FGAHarness
	Namespace string
	RunPrefix string
	// RunAsUser names the TestUser whose identity the run authenticates
	// as against pipeline-api. Defaults to AliceID (project_admin) so
	// existing scenarios pass without modification. FGA-matrix scenarios
	// override per-scenario.
	RunAsUser uuid.UUID

	Resources []string // legacy kubectl "kind/name" cleanup entries (unused by the HTTP path)

	// apiSession caches the RunAsUser login cookie for the backend's lifetime.
	apiSession string
	// putPipelines / runIDs track what to tear down at Cleanup.
	putPipelines []string
	runIDs       []uuid.UUID
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

// RunPipeline uploads the PipelineDoc, triggers a run, and polls it to a
// terminal phase — all through pipeline-api's public REST API.
//
// pipelinePath is a path to a rendered PipelineDoc YAML file (as produced by
// RenderPipeline); the doc name is parsed from it via config.Parse.
func (k *K8sBackend) RunPipeline(ctx context.Context, pipelinePath string, opts RunOpts) (*RunResult, error) {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()

	raw, err := os.ReadFile(pipelinePath)
	if err != nil {
		return nil, fmt.Errorf("read pipeline doc %s: %w", pipelinePath, err)
	}
	doc, err := config.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse pipeline doc %s: %w", pipelinePath, err)
	}
	if doc.Name == "" {
		return nil, fmt.Errorf("pipeline doc %s has no top-level name", pipelinePath)
	}
	name := doc.Name

	// project = harness lakekeeper project UUID (the e2e convergence noted in
	// NewK8sBackend). pipeline-api resolves {pid} against a real projects row.
	projectID := k.Harness.LakekeeperProjectID
	base := PipelineAPIBaseURL()

	session, err := k.apiSessionFor(ctx)
	if err != nil {
		return nil, err
	}

	if err := apiPutPipelineDoc(ctx, base, session, projectID, name, raw); err != nil {
		return nil, fmt.Errorf("PUT pipeline: %w", err)
	}
	k.trackPipeline(name)

	runID, err := apiTriggerRun(ctx, base, session, projectID, name)
	if err != nil {
		return nil, fmt.Errorf("trigger run: %w", err)
	}
	k.runIDs = append(k.runIDs, runID)

	var lastPhase, lastMessage string
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for run %s (pipeline %s): last phase=%q", runID, name, lastPhase)
		case <-ticker.C:
			view, err := apiGetRun(ctx, base, session, projectID, runID.String())
			if err != nil {
				continue // transient; keep polling until the deadline
			}
			if view.Phase != "" {
				lastPhase = view.Phase
			}
			if view.Message != "" {
				lastMessage = view.Message
			}

			switch view.Phase {
			case "Succeeded":
				return &RunResult{
					Success:  true,
					ExitCode: 0,
					Logs:     k.fetchLogs(ctx, runID),
					Duration: time.Since(start),
					RunID:    runID,
				}, nil
			case "FailedUser":
				return &RunResult{
					Success:       false,
					ExitCode:      1,
					FailureType:   "FailedUser",
					StatusMessage: lastMessage,
					Logs:          k.fetchLogs(ctx, runID),
					Duration:      time.Since(start),
					RunID:         runID,
				}, nil
			case "FailedApplication":
				return &RunResult{
					Success:       false,
					ExitCode:      20,
					FailureType:   "FailedApplication",
					StatusMessage: lastMessage,
					Logs:          k.fetchLogs(ctx, runID),
					Duration:      time.Since(start),
					RunID:         runID,
				}, nil
			case "Pending", "Running", "":
				// keep polling
			default:
				// Any other terminal phase (Cancelled / Expired / …).
				return &RunResult{
					Success:       false,
					FailureType:   view.Phase,
					StatusMessage: lastMessage,
					Logs:          k.fetchLogs(ctx, runID),
					Duration:      time.Since(start),
					RunID:         runID,
				}, nil
			}
		}
	}
}

// apiSessionFor logs the backend's RunAsUser into pipeline-api and caches the
// session cookie for the backend's lifetime.
func (k *K8sBackend) apiSessionFor(ctx context.Context) (string, error) {
	if k.apiSession != "" {
		return k.apiSession, nil
	}
	email, password, ok := apiCredsFor(k.RunAsUser)
	if !ok {
		return "", fmt.Errorf("no pipeline-api login credentials mapped for RunAsUser %s "+
			"(E5: provision a DB user + FGA grants for this identity before driving the HTTP run path as it)", k.RunAsUser)
	}
	cookie, err := apiLogin(ctx, PipelineAPIBaseURL(), email, password)
	if err != nil {
		return "", fmt.Errorf("login as %s: %w", email, err)
	}
	k.apiSession = cookie
	return cookie, nil
}

// trackPipeline records a PUT pipeline name once for Cleanup.
func (k *K8sBackend) trackPipeline(name string) {
	for _, p := range k.putPipelines {
		if p == name {
			return
		}
	}
	k.putPipelines = append(k.putPipelines, name)
}

// fetchLogs retrieves logs from all pods for a run, selected by the
// datuplet.io/run-id label the controller stamps on every Job/Pod.
func (k *K8sBackend) fetchLogs(ctx context.Context, runID uuid.UUID) string {
	podOut, err := exec.CommandContext(ctx, "kubectl", "get", "pods",
		"-n", k.Namespace,
		"-l", "datuplet.io/run-id="+runID.String(),
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

// Cleanup tears down what the run created. Best-effort — errors are
// joined and returned but never fatal.
func (k *K8sBackend) Cleanup(ctx context.Context) error {
	var errs []string

	// PipelineRuns (and their owner-ref'd children: pods, run-token + snapshot
	// Secrets) by run-id label.
	for _, id := range k.runIDs {
		out, err := exec.CommandContext(ctx, "kubectl", "delete", "pipelinerun",
			"-l", "datuplet.io/run-id="+id.String(),
			"-n", k.Namespace, "--ignore-not-found").CombinedOutput()
		if err != nil {
			errs = append(errs, fmt.Sprintf("delete pipelinerun run-id=%s: %v (%s)", id, err, strings.TrimSpace(string(out))))
		}
	}

	// Stored pipelines via the API, so re-runs of the same fixture under the
	// same project don't collide.
	if k.apiSession != "" && k.Harness != nil {
		for _, name := range k.putPipelines {
			if err := apiDeletePipeline(ctx, PipelineAPIBaseURL(), k.apiSession, k.Harness.LakekeeperProjectID, name); err != nil {
				errs = append(errs, fmt.Sprintf("delete pipeline %s: %v", name, err))
			}
		}
	}

	// Legacy kubectl "kind/name" resources (unused by the HTTP path; retained
	// so any externally-appended entries are still cleaned up).
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
