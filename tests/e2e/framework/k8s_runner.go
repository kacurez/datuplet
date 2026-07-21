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

	// DatupletProjectID is the Datuplet projects-store UUID used as {pid}
	// for PUT / trigger / get / delete, and the basis of the run namespace
	// (datuplet-<DatupletProjectID>). This is NOT the lakekeeper project UUID
	// — pipeline-api resolves {pid} against a real projects row (GetByID).
	// NewK8sBackend resolves and caches it (from the harness's lakekeeper
	// project, by name); callers that construct the backend as a literal (e.g.
	// the secrets ladder) set it directly to target a specific project.
	DatupletProjectID string

	Resources []string // legacy kubectl "kind/name" cleanup entries (unused by the HTTP path)

	// apiSession caches the RunAsUser login cookie for the backend's lifetime.
	apiSession string
	// putPipelines / runIDs track what to tear down at Cleanup.
	putPipelines []string
	runIDs       []uuid.UUID
}

// NewK8sBackend creates a K8sBackend bound to the harness's
// per-project namespace. Idempotent: re-creating the same namespace
// is a kubectl no-op.
//
// It resolves the Datuplet projects-store row bound to the harness's lakekeeper
// project (find-or-create by name, then discover its UUID — see
// resolveDatupletProjectID) and uses THAT UUID both as {pid} and as the basis
// of the run namespace. The two are distinct in production (and now in e2e):
// pipeline-api resolves {pid} against projects.GetByID and creates the run in
// datuplet-<datuplet-project-uuid> (pkg/pipelineapi/k8s.NamespaceForProject),
// so the harness must fetch logs / clean up from that same namespace.
func NewK8sBackend(h *FGAHarness, runPrefix string) (*K8sBackend, error) {
	if h == nil {
		return nil, errors.New("NewK8sBackend: harness is required")
	}
	if h.LakekeeperProjectID == "" {
		return nil, errors.New("NewK8sBackend: harness has no LakekeeperProjectID")
	}
	k := &K8sBackend{
		Harness:   h,
		RunPrefix: runPrefix,
		RunAsUser: AliceID,
	}
	ctx := context.Background()
	pid, err := k.ensureDatupletProject(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve Datuplet project for lakekeeper project %q: %w",
			h.LakekeeperProjectName, err)
	}
	pidUUID, err := uuid.Parse(pid)
	if err != nil {
		return nil, fmt.Errorf("parse Datuplet project UUID %q: %w", pid, err)
	}
	k.Namespace = "datuplet-" + pidUUID.String()
	// Belt-and-braces: pipeline-api ensures this namespace at trigger time too,
	// with the same datuplet.io/project-id label (EnsureProjectNamespace). We
	// pre-create it with the matching label so log-fetch / cleanup have a target
	// even before the first trigger, and so a pre-existing namespace's label
	// agrees with what pipeline-api would set.
	if err := ensureNamespace(ctx, k.Namespace, pidUUID); err != nil {
		return nil, fmt.Errorf("ensure namespace %q: %w", k.Namespace, err)
	}
	return k, nil
}

// ensureDatupletProject returns the Datuplet projects-store UUID this backend
// drives {pid} against, resolving+caching it on first use. A literal-constructed
// backend that already set DatupletProjectID (e.g. the secrets ladder) short-
// circuits without any resolution.
func (k *K8sBackend) ensureDatupletProject(ctx context.Context) (string, error) {
	if k.DatupletProjectID != "" {
		return k.DatupletProjectID, nil
	}
	id, err := resolveDatupletProjectID(ctx, k.Harness)
	if err != nil {
		return "", err
	}
	k.DatupletProjectID = id
	return id, nil
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

	// {pid} is the Datuplet projects-store UUID (resolved from the harness's
	// lakekeeper project by name), NOT the lakekeeper UUID: pipeline-api
	// resolves {pid} against a real projects row (projects.GetByID).
	projectID, err := k.ensureDatupletProject(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve Datuplet project: %w", err)
	}
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
//
// FGA IDENTITY (RFC 027 E2 review, Finding 2): SetupFGABootstrap seeds the
// FIXED TestUser UUIDs (AliceID/BobID/…), but the HTTP run identity is the
// authenticated DB user's REAL UUID — random for a `create-user`-minted account,
// and the seeded-admin's own UUID for Alice. mustHaveRelation checks that real
// UUID, so a grant keyed to the fixed UUID would not apply. This method
// therefore (a) provisions a real DB login user for non-admin identities
// (Alice maps to the pre-seeded admin), (b) logs in, and (c) seeds the run's
// REAL UUID with the SAME project relation SetupFGABootstrap grants the fixed
// UUID. The token-mint path (k8s_token.go, exercised by k8s_token_test.go and
// the buildK8sVerifier browse impersonation) still uses the fixed UUIDs, which
// SeedFGAGrants keeps granting — both identities are covered.
func (k *K8sBackend) apiSessionFor(ctx context.Context) (string, error) {
	if k.apiSession != "" {
		return k.apiSession, nil
	}
	email, password, ok := apiCredsFor(k.RunAsUser)
	if !ok {
		return "", fmt.Errorf("no pipeline-api login credentials mapped for RunAsUser %s "+
			"(extend apiCredsFor + provision a DB user for this identity before driving the HTTP run path as it)", k.RunAsUser)
	}
	// Non-admin identities need a real DB login user provisioned; Alice maps to
	// the admin account register.sh already seeded, so skip create-user for her.
	if k.RunAsUser != AliceID {
		if err := adminCreateUser(ctx, email, password); err != nil {
			return "", fmt.Errorf("provision DB user %s: %w", email, err)
		}
	}
	cookie, err := apiLogin(ctx, PipelineAPIBaseURL(), email, password)
	if err != nil {
		return "", fmt.Errorf("login as %s: %w", email, err)
	}
	if err := k.ensureRunAsUserGrant(ctx, cookie); err != nil {
		return "", fmt.Errorf("seed FGA grant for %s: %w", email, err)
	}
	k.apiSession = cookie
	return cookie, nil
}

// ensureRunAsUserGrant threads the run's REAL DB UUID into the same project
// relation SetupFGABootstrap grants this TestUser's fixed UUID, so the
// run-trigger's mustHaveRelation FGA check passes for the authenticated caller.
// No-op for a no-grant identity (dora-like: relation == "").
func (k *K8sBackend) ensureRunAsUserGrant(ctx context.Context, cookie string) error {
	relation := relationForTestUser(k.RunAsUser)
	if relation == "" {
		return nil
	}
	realUUID, err := apiMeUserID(ctx, PipelineAPIBaseURL(), cookie)
	if err != nil {
		return fmt.Errorf("resolve real UUID: %w", err)
	}
	// seedProjectGrant writes on authz.ProjectObject(h.LakekeeperProjectID) —
	// the exact object mustHaveRelation checks (proj.LakekeeperProjectID) — for
	// the real UUID. Idempotent.
	return seedProjectGrant(ctx, k.Harness, realUUID, relation)
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
	// same project don't collide. Uses the resolved Datuplet project UUID (the
	// {pid} the PUT/trigger used), never the lakekeeper UUID.
	if k.apiSession != "" && k.DatupletProjectID != "" {
		for _, name := range k.putPipelines {
			if err := apiDeletePipeline(ctx, PipelineAPIBaseURL(), k.apiSession, k.DatupletProjectID, name); err != nil {
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
