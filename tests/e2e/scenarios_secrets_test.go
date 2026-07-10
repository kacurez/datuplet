// Package e2e — RFC 026 P1.5 Task S9: secrets ladder + snapshot-immutability
// e2e coverage.
//
// TestSecretsLadder drives the managed write-only secrets API end to end
// against a live cluster: the S7 HTTP pre-flight ladder (save-warn /
// trigger-reject) AND the S3 controller-level admission snapshot (missing-key
// FailedUser with zero pods, snapshot immutability across a project-secret
// rotation, ownerRef garbage collection).
package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// secretsLadderPipelineName is the name of the throwaway Pipeline used for
// the HTTP-only save-warn / trigger-reject assertions (a)/(b). It is never
// actually run to completion — POST /runs rejects it with 400 while the key
// is missing, and the pipeline is deleted again at the end of the test.
const secretsLadderPipelineName = "secrets-ladder-pipeline"

// secretsLadderPipelineYAML references $[api_token] exactly like
// pipelines/k8s/secrets-happy.yaml, but as a bare Pipeline doc (no
// PipelineRun) suitable for PUT /api/v1/projects/{pid}/pipelines/{name}.
const secretsLadderPipelineYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: secrets-ladder-pipeline
  namespace: datuplet
spec:
  stages:
    - name: extract
      components:
        - name: json-extractor
          component: http-json-extractor
          version: dev
          config:
            url: "https://jsonplaceholder.typicode.com/posts"
            api_token: "$[api_token]"
          outputs:
            defaultBucket: secrets-ladder-bucket
            defaultWriteMode: FULL_LOAD
`

// TestSecretsLadder exercises RFC 026 P1.5's full secrets lifecycle:
//
//	(a) PUT /pipelines with a missing $[api_token] key -> 200 + warning finding.
//	(b) POST .../runs with the key still missing -> 400.
//	(c) kubectl-path run (bypassing the HTTP pre-flight) with the key still
//	    missing -> FailedUser, zero pods, zero Jobs.
//	(d) PUT /secrets/api_token, then the SAME kubectl-path run -> succeeds.
//	(e) the per-run snapshot Secret exists while the run's PipelineRun is
//	    still around, and is garbage-collected when the PipelineRun is deleted.
//	(f) rotating api_token's value after the run was admitted does NOT change
//	    the already-created snapshot (immutability).
//
// # Why a dedicated project instead of the shared per-suite one
//
// pipeline-api's secrets/pipeline/trigger HTTP endpoints all resolve the URL
// {pid} against a real `projects` row in Postgres (mustHaveRelation, see
// pkg/pipelineapi/http/pipeline_handlers.go). SetupFGABootstrap
// (framework/bootstrap.go) only ever provisions a *lakekeeper* Project for
// the whole suite — it never creates the matching Datuplet DB row. That is
// the exact same gap TestFGAMatrix_UnauthorisedTrigger (scenarios_test.go)
// already documents and skips on, so h.LakekeeperProjectID cannot be used
// directly as {pid} here either.
//
// This test bridges the gap the same way an operator would: by running
// `pipeline-api admin create-project` (idempotent — safe to call on every
// run) against the harness's OWN lakekeeper project name. admin
// create-project's lakekeeper step probes by name before allocating
// (cmd/pipeline-api/admin.go, adminCreateProject Step 2), so passing
// h.LakekeeperProjectName makes it find-and-reuse h.LakekeeperProjectID —
// and the "datuplet" warehouse SetupFGABootstrap already attached to it —
// instead of provisioning a second, warehouse-less Project. The result is a
// Datuplet Postgres project row whose K8s namespace (datuplet-<postgres-id>)
// is a stable target for BOTH the managed-secrets HTTP API and a
// kubectl-applied PipelineRun, so a literal PUT /secrets/{key} write is
// visible to the exact namespace the controller admits the run into.
//
// # Two code paths, deliberately both exercised
//
// (a)/(b) go through pipeline-api's HTTP endpoints (PUT /pipelines,
// POST /runs) — the S7 *pre-flight* ladder, which rejects before any
// PipelineRun is ever created. (c)/(d) kubectl-apply the PipelineRun CRD
// directly via K8sBackend (the same mechanism every other K8s-tier scenario
// in this suite uses), bypassing the HTTP pre-flight entirely. That's the
// only way, in this harness, to reach the S3 admission-time defense-in-depth
// check inside the operator with a key that is genuinely missing — POST
// /runs would otherwise always intercept first.
func TestSecretsLadder(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		t.Skip("SharedHarness nil — SetupFGABootstrap must have run in TestMain")
	}
	if err := framework.PreCheck(); err != nil {
		t.Skipf("precheck failed: %v", err)
	}
	if !framework.PipelineAPIReachable() {
		t.Skip("pipeline-api not reachable on NodePort 30081 — start port-forward")
	}

	ctx := context.Background()

	projectID, err := ensureSecretsLadderProject(ctx, h)
	if err != nil {
		t.Skipf("could not provision secrets-ladder project: %v", err)
	}
	namespace := "datuplet-" + projectID

	session := getAdminSession(t)

	// Bootstrap the project namespace + managed Secret + secrets RBAC via
	// the SAME lazy-ensure path production uses (pkg8s.EnsureProjectNamespace
	// inside handlePutSecret) — admin create-project's own --with-namespace
	// flag needs a kubeconfig that isn't available from inside a kubectl-exec
	// shell, so a throwaway PUT is the reliable way to get there. Immediately
	// delete the key again so the "missing key" assertions below aren't
	// vacuous because of this bootstrap write, or because a previous run of
	// this very test (or a stale key from before this key was ever deleted)
	// left api_token set on this stable, reused project.
	if status, body, err := putSecretHTTP(ctx, session, projectID, "api_token", "bootstrap-placeholder"); err != nil {
		t.Fatalf("PUT /secrets/api_token (bootstrap): %v", err)
	} else if status != http.StatusNoContent {
		t.Fatalf("PUT /secrets/api_token (bootstrap): status=%d, want 204; body=%s", status, body)
	}
	if status, body, err := deleteSecretHTTP(ctx, session, projectID, "api_token"); err != nil {
		t.Fatalf("DELETE /secrets/api_token (setup): %v", err)
	} else if status != http.StatusNoContent {
		t.Fatalf("DELETE /secrets/api_token (setup): status=%d, want 204; body=%s", status, body)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _, _ = deleteSecretHTTP(cleanupCtx, session, projectID, "api_token")
		_, _, _ = deletePipelineHTTP(cleanupCtx, session, projectID, secretsLadderPipelineName)
	})

	// ------------------------------------------------------------------
	// (a) PUT /pipelines with a missing key -> 200 + warning finding.
	// ------------------------------------------------------------------
	status, body, err := putPipelineHTTP(ctx, session, projectID, secretsLadderPipelineName, []byte(secretsLadderPipelineYAML))
	if err != nil {
		t.Fatalf("PUT /pipelines (missing key): %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("PUT /pipelines (missing key): status=%d, want 200; body=%s", status, body)
	}
	var saveResp struct {
		Findings []struct {
			Path     string `json:"path"`
			Message  string `json:"message"`
			Severity string `json:"severity"`
		} `json:"findings"`
	}
	if err := json.Unmarshal(body, &saveResp); err != nil {
		t.Fatalf("decode PUT /pipelines response: %v (body=%s)", err, body)
	}
	foundWarning := false
	for _, f := range saveResp.Findings {
		if f.Severity == "warning" && strings.Contains(f.Message, "api_token") {
			foundWarning = true
		}
	}
	if !foundWarning {
		t.Errorf("PUT /pipelines (missing key): no warning finding naming api_token; findings=%+v", saveResp.Findings)
	}

	// ------------------------------------------------------------------
	// (b) POST .../runs with the key still missing -> 400.
	// ------------------------------------------------------------------
	status, body, err = triggerRunSessionHTTP(ctx, session, projectID, secretsLadderPipelineName)
	if err != nil {
		t.Fatalf("POST /runs (missing key): %v", err)
	}
	if status != http.StatusBadRequest {
		t.Fatalf("POST /runs (missing key): status=%d, want 400; body=%s", status, body)
	}
	if !strings.Contains(string(body), "api_token") {
		t.Errorf("POST /runs (missing key) 400 body does not name api_token: %s", body)
	}

	// ------------------------------------------------------------------
	// (c) kubectl-path run with the key still missing -> FailedUser, zero
	// pods, zero Jobs. Constructed directly (not via NewK8sBackend) so its
	// Namespace can target the secrets-ladder project's namespace instead
	// of the shared per-suite one; every other field mirrors what
	// NewK8sBackend sets up.
	// ------------------------------------------------------------------
	kb := &framework.K8sBackend{
		Harness:   h,
		Namespace: namespace,
		RunPrefix: runPrefix,
		RunAsUser: framework.AliceID,
	}
	defer kb.Cleanup(context.Background())

	pipelinesDir, _ := filepath.Abs("pipelines")
	testDataDir, _ := filepath.Abs("testdata")
	templatePath := pipelinesDir + "/k8s/secrets-happy.yaml"
	rendered, err := framework.RenderPipeline(templatePath, framework.TemplateVars{
		RunPrefix:   runPrefix,
		TestDataDir: testDataDir,
	})
	if err != nil {
		t.Fatalf("render secrets-happy.yaml: %v", err)
	}
	defer os.Remove(rendered)

	prName := runPrefix + "-secrets-happy-run"

	result, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3"})
	if err != nil {
		t.Fatalf("kubectl-path run (missing key): %v", err)
	}
	if result.Success || result.FailureType != "FailedUser" {
		t.Fatalf("kubectl-path run (missing key): got success=%v failureType=%q message=%q, want FailedUser",
			result.Success, result.FailureType, result.StatusMessage)
	}
	if !strings.Contains(result.StatusMessage, "api_token") {
		t.Errorf("kubectl-path run (missing key) status message does not name api_token: %q", result.StatusMessage)
	}
	assertNoPodsForRun(t, namespace, prName)
	assertNoJobsForRun(t, namespace, prName)

	// The per-run snapshot Secret is created at admission time
	// (snapshotRunSecrets, called from handlePending — see
	// pkg/k8s/controllers/pipelinerun_controller.go), independently of
	// Job/Pod creation. So "no pods/Jobs" does NOT by itself imply "no
	// snapshot" — the missing-key rejection must happen before/without
	// ever writing a snapshot, and that has to be checked explicitly.
	// Name derived exactly as runSecretsName does
	// (pkg/k8s/controllers/pipelinerun_jobs.go):
	// "datuplet-runsecrets-" + first 8 chars of the run UUID (shortID,
	// pkg/k8s/controllers/pipelinerun_controller.go).
	missingKeySnapshotName := "datuplet-runsecrets-" + result.RunID.String()[:8]
	if secretExists(t, namespace, missingKeySnapshotName) {
		t.Errorf("snapshot Secret %q exists after a missing-key run that never reached admission-time success; want none",
			missingKeySnapshotName)
	}

	// ------------------------------------------------------------------
	// Delete the terminal PipelineRun left behind by the missing-key
	// attempt and wait for it to be fully gone before re-running.
	//
	// The controller's Reconcile switches on pipelineRun.Status.Phase
	// and no-ops once that phase is terminal (FailedUser / Succeeded /
	// FailedApplication) — see pkg/k8s/controllers/pipelinerun_controller.go's
	// phase switch — regardless of what spec.runId says. Re-applying the
	// SAME object with a new spec.runId (as attachRunToken does on every
	// kb.RunPipeline call) would NOT re-admit it: kubectl apply only
	// mutates spec, the reconcile loop would see the still-terminal
	// FailedUser status and return immediately, and RunPipeline's poll
	// loop below would report that stale status as if it were a fresh
	// run. Deleting the object first forces the next kubectl apply to
	// create a brand-new PipelineRun (phase ""), which handlePending
	// actually admits — so (d)/(e)/(f) below exercise a genuinely fresh
	// run rather than reading back the first attempt's cached status.
	// The snapshot-Secret non-existence was already asserted explicitly
	// above, so there is nothing left to garbage-collect for this object.
	// ------------------------------------------------------------------
	if out, err := exec.CommandContext(ctx, "kubectl", "delete", "pipelinerun", prName,
		"-n", namespace, "--ignore-not-found").CombinedOutput(); err != nil {
		t.Fatalf("kubectl delete pipelinerun (missing-key attempt): %v\n%s", err, out)
	}
	waitForPipelineRunGone(t, namespace, prName, 20*time.Second)

	// ------------------------------------------------------------------
	// (d) PUT the key via the managed API, then apply a fresh PipelineRun
	// under the same name (now genuinely absent, so kubectl apply creates
	// rather than no-ops) -> succeeds.
	// ------------------------------------------------------------------
	const firstValue = "e2e-ladder-secret-v1"
	status, body, err = putSecretHTTP(ctx, session, projectID, "api_token", firstValue)
	if err != nil {
		t.Fatalf("PUT /secrets/api_token: %v", err)
	}
	if status != http.StatusNoContent {
		t.Fatalf("PUT /secrets/api_token: status=%d, want 204; body=%s", status, body)
	}

	result, err = kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3"})
	if err != nil {
		t.Fatalf("kubectl-path run (key present): %v", err)
	}
	if !result.Success {
		t.Fatalf("kubectl-path run (key present): expected success, got failureType=%q message=%q logs=%s",
			result.FailureType, result.StatusMessage, result.Logs)
	}

	// ------------------------------------------------------------------
	// (e, part 1) The per-run snapshot Secret exists while the run's
	// PipelineRun object is still around. Name derived exactly as
	// runSecretsName does (pkg/k8s/controllers/pipelinerun_jobs.go):
	// "datuplet-runsecrets-" + first 8 chars of the run UUID.
	// ------------------------------------------------------------------
	snapshotName := "datuplet-runsecrets-" + result.RunID.String()[:8]
	if !secretExists(t, namespace, snapshotName) {
		t.Fatalf("snapshot Secret %q does not exist after a successful run", snapshotName)
	}

	// ------------------------------------------------------------------
	// (f) Rotating the project secret's value after the run was admitted
	// must NOT change the already-created snapshot (immutability).
	// ------------------------------------------------------------------
	const secondValue = "e2e-ladder-secret-v2-rotated"
	status, body, err = putSecretHTTP(ctx, session, projectID, "api_token", secondValue)
	if err != nil {
		t.Fatalf("PUT /secrets/api_token (rotate): %v", err)
	}
	if status != http.StatusNoContent {
		t.Fatalf("PUT /secrets/api_token (rotate): status=%d, want 204; body=%s", status, body)
	}
	got := decodedSecretValue(t, namespace, snapshotName, "api_token")
	if got != firstValue {
		t.Errorf("snapshot %q api_token = %q after rotating the project secret, want unchanged %q (immutability broken)",
			snapshotName, got, firstValue)
	}

	// ------------------------------------------------------------------
	// (e, part 2) Deleting the PipelineRun garbage-collects the snapshot
	// (ownerRef).
	// ------------------------------------------------------------------
	if out, err := exec.CommandContext(ctx, "kubectl", "delete", "pipelinerun", prName,
		"-n", namespace, "--ignore-not-found").CombinedOutput(); err != nil {
		t.Fatalf("kubectl delete pipelinerun: %v\n%s", err, out)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if !secretExists(t, namespace, snapshotName) {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("snapshot Secret %q was not garbage-collected within 20s of deleting PipelineRun %q", snapshotName, prName)
			break
		}
		time.Sleep(1 * time.Second)
	}
}

// ensureSecretsLadderProject creates (or idempotently reuses) a Datuplet
// project row named after the harness's own lakekeeper project
// (h.LakekeeperProjectName), by running `pipeline-api admin create-project`
// inside the pipeline-api Pod. See TestSecretsLadder's docstring for why.
//
// Returns the Postgres project UUID (string form). Safe to call once per
// test process, and safe across repeated `go test` invocations — the
// underlying admin subcommand tolerates an already-existing project/tuple.
func ensureSecretsLadderProject(ctx context.Context, h *framework.FGAHarness) (string, error) {
	podName := queryFindPipelineAPIPod(ctx)
	if podName == "" {
		return "", fmt.Errorf("pipeline-api pod not found in namespace %q", queryE2ENamespace)
	}
	lakekeeperURL := fmt.Sprintf("http://lakekeeper.%s.svc.cluster.local:8181", queryE2ENamespace)
	openfgaURL := fmt.Sprintf("http://openfga.%s.svc.cluster.local:8080", queryE2ENamespace)

	out, err := exec.CommandContext(ctx, "kubectl", "exec", podName, "-n", queryE2ENamespace,
		"--", "/usr/local/bin/pipeline-api", "admin", "create-project",
		"--name="+h.LakekeeperProjectName,
		"--creator-email="+queryAdminEmail,
		"--lakekeeper-url="+lakekeeperURL,
		"--openfga-url="+openfgaURL,
	).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("admin create-project: %w\noutput: %s", err, string(out))
	}
	id, perr := parseCreateProjectID(string(out))
	if perr != nil {
		return "", fmt.Errorf("parse create-project output: %w\noutput: %s", perr, string(out))
	}
	return id, nil
}

// parseCreateProjectID extracts the "id: <uuid>" line adminCreateProject
// prints on both the fresh-create and idempotent-reuse branches
// (cmd/pipeline-api/admin.go, adminCreateProject).
func parseCreateProjectID(output string) (string, error) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if id, ok := strings.CutPrefix(line, "id:"); ok {
			return strings.TrimSpace(id), nil
		}
	}
	return "", fmt.Errorf("no %q line found in output", "id:")
}

// --- pipeline-api HTTP helpers (session-cookie auth, project-scoped) ---

func putSecretHTTP(ctx context.Context, cookie, projectID, key, value string) (int, []byte, error) {
	body, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		return 0, nil, err
	}
	u := fmt.Sprintf("%s/api/v1/projects/%s/secrets/%s", framework.PipelineAPIBaseURL(), projectID, key)
	return doSessionRequest(ctx, http.MethodPut, u, cookie, "application/json", body)
}

func deleteSecretHTTP(ctx context.Context, cookie, projectID, key string) (int, []byte, error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/secrets/%s", framework.PipelineAPIBaseURL(), projectID, key)
	return doSessionRequest(ctx, http.MethodDelete, u, cookie, "", nil)
}

func putPipelineHTTP(ctx context.Context, cookie, projectID, name string, yaml []byte) (int, []byte, error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/pipelines/%s", framework.PipelineAPIBaseURL(), projectID, name)
	return doSessionRequest(ctx, http.MethodPut, u, cookie, "application/x-yaml", yaml)
}

func deletePipelineHTTP(ctx context.Context, cookie, projectID, name string) (int, []byte, error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/pipelines/%s", framework.PipelineAPIBaseURL(), projectID, name)
	return doSessionRequest(ctx, http.MethodDelete, u, cookie, "", nil)
}

func triggerRunSessionHTTP(ctx context.Context, cookie, projectID, name string) (int, []byte, error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/pipelines/%s/runs", framework.PipelineAPIBaseURL(), projectID, name)
	return doSessionRequest(ctx, http.MethodPost, u, cookie, "application/json", []byte("{}"))
}

func doSessionRequest(ctx context.Context, method, url, cookie, contentType string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: cookie})
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody, nil
}

// --- kubectl assertion helpers ---

func assertNoPodsForRun(t *testing.T, namespace, prName string) {
	t.Helper()
	out, err := exec.Command("kubectl", "get", "pods", "-n", namespace,
		"-l", "datuplet.io/pipelinerun="+prName,
		"-o", "jsonpath={.items[*].metadata.name}").Output()
	if err != nil {
		t.Fatalf("kubectl get pods: %v", err)
	}
	if names := strings.TrimSpace(string(out)); names != "" {
		t.Errorf("expected zero pods for PipelineRun %q, found: %s", prName, names)
	}
}

func assertNoJobsForRun(t *testing.T, namespace, prName string) {
	t.Helper()
	out, err := exec.Command("kubectl", "get", "jobs", "-n", namespace,
		"-l", "datuplet.io/pipelinerun="+prName,
		"-o", "jsonpath={.items[*].metadata.name}").Output()
	if err != nil {
		t.Fatalf("kubectl get jobs: %v", err)
	}
	if names := strings.TrimSpace(string(out)); names != "" {
		t.Errorf("expected zero Jobs for PipelineRun %q, found: %s", prName, names)
	}
}

// waitForPipelineRunGone polls until `kubectl get pipelinerun <name>`
// reports NotFound, or fails the test if that doesn't happen within
// timeout. Used to make sure a terminal PipelineRun from a prior
// attempt is fully removed before re-applying the same name, since the
// controller no-ops Reconcile for objects already in a terminal phase.
func waitForPipelineRunGone(t *testing.T, namespace, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := exec.Command("kubectl", "get", "pipelinerun", name, "-n", namespace).Run()
		if err != nil {
			return // kubectl get exits non-zero once the object is gone.
		}
		if time.Now().After(deadline) {
			t.Fatalf("PipelineRun %q was not deleted within %s", name, timeout)
		}
		time.Sleep(1 * time.Second)
	}
}

func secretExists(t *testing.T, namespace, name string) bool {
	t.Helper()
	err := exec.Command("kubectl", "get", "secret", name, "-n", namespace).Run()
	return err == nil
}

func decodedSecretValue(t *testing.T, namespace, secretName, key string) string {
	t.Helper()
	out, err := exec.Command("kubectl", "get", "secret", secretName, "-n", namespace,
		"-o", "jsonpath={.data."+key+"}").Output()
	if err != nil {
		t.Fatalf("kubectl get secret %s: %v", secretName, err)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("decode secret %s key %s: %v", secretName, key, err)
	}
	return string(decoded)
}
