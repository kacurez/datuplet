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
// pipelines/k8s/secrets-happy.yaml, as an envelope-free PipelineDoc (RFC 027)
// suitable for PUT /api/v1/projects/{pid}/pipelines/{name}.
const secretsLadderPipelineYAML = `name: secrets-ladder-pipeline
stages:
  - name: extract
    components:
      - name: json-extractor
        component: http-json-extractor
        version: dev
        config:
          url: "http://e2e-http-fixture.datuplet-e2e.svc.cluster.local/posts"
          api_token: "$[api_token]"
        outputs:
          defaultBucket: secrets-ladder-bucket
          defaultWriteMode: FULL_LOAD
`

// TestSecretsLadder exercises RFC 026 P1.5's secrets lifecycle over the RFC 027
// HTTP run path:
//
//	(a) PUT /pipelines with a missing $[api_token] key -> 200 + warning finding.
//	(b) POST .../runs with the key still missing -> 400 (the S7 pre-flight gate).
//	(c) PUT /secrets/api_token, then run the pipeline (via pipeline-api) -> succeeds.
//	(d) the per-run snapshot Secret exists while the run's PipelineRun is
//	    still around, and is garbage-collected when the PipelineRun is deleted.
//	(e) rotating api_token's value after the run was admitted does NOT change
//	    the already-created snapshot (immutability).
//
// # Why a dedicated project instead of the shared per-suite one
//
// pipeline-api's secrets/pipeline/trigger HTTP endpoints all resolve the URL
// {pid} against a real `projects` row in Postgres (mustHaveRelation, see
// pkg/pipelineapi/http/pipeline_handlers.go). SetupFGABootstrap
// (framework/bootstrap.go) only ever provisions a *lakekeeper* Project for
// the whole suite — it never creates the matching Datuplet DB row, so
// h.LakekeeperProjectID cannot be used directly as {pid} here.
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
// is a stable target for BOTH the managed-secrets HTTP API and the run
// pipeline-api creates, so a literal PUT /secrets/{key} write is visible to the
// exact namespace the controller admits the run into.
//
// # Why the old kubectl-admission "missing key" case was dropped (RFC 027 E2)
//
// Pre-RFC-027 this test also kubectl-applied a PipelineRun with a genuinely
// missing key to reach the controller's admission-time defense-in-depth
// (FailedUser, zero pods). The migrated K8sBackend now drives runs through
// pipeline-api's POST /runs, whose S7 pre-flight rejects a missing $[api_token]
// with 400 (case (b)) BEFORE any PipelineRun / pod is created — so that
// admission path is unreachable from the API surface this harness now uses. It
// is covered by the controller's own unit tests; here (b) proves the gate and
// (c) proves the happy path once the secret is set.
func TestSecretsLadder(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil — SetupFGABootstrap must have run in TestMain")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable on NodePort 30081 — start port-forward")
	}

	ctx := context.Background()

	projectID, err := ensureSecretsLadderProject(ctx, h)
	if err != nil {
		framework.SkipOrFail(t, "could not provision secrets-ladder project: %v", err)
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
	// Backend targeting the secrets-ladder project. Constructed as a literal
	// (not NewK8sBackend) so it targets THIS project's {pid} + namespace
	// directly: DatupletProjectID is the Postgres project UUID (the {pid} the
	// PUT/trigger use), Namespace the matching datuplet-<pid>.
	// ------------------------------------------------------------------
	kb := &framework.K8sBackend{
		Harness:           h,
		Namespace:         namespace,
		RunPrefix:         runPrefix,
		RunAsUser:         framework.AliceID,
		DatupletProjectID: projectID,
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

	// ------------------------------------------------------------------
	// (c) PUT the key via the managed API, then run the pipeline through
	// pipeline-api -> the pre-flight now passes and the run succeeds.
	// ------------------------------------------------------------------
	const firstValue = "e2e-ladder-secret-v1"
	status, body, err = putSecretHTTP(ctx, session, projectID, "api_token", firstValue)
	if err != nil {
		t.Fatalf("PUT /secrets/api_token: %v", err)
	}
	if status != http.StatusNoContent {
		t.Fatalf("PUT /secrets/api_token: status=%d, want 204; body=%s", status, body)
	}

	result, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3"})
	if err != nil {
		t.Fatalf("run (key present): %v", err)
	}
	if !result.Success {
		t.Fatalf("run (key present): expected success, got failureType=%q message=%q logs=%s",
			result.FailureType, result.StatusMessage, result.Logs)
	}

	// ------------------------------------------------------------------
	// (d, part 1) The per-run snapshot Secret exists while the run's
	// PipelineRun object is still around. Name derived exactly as
	// runSecretsName does (pkg/k8s/controllers/pipelinerun_jobs.go):
	// "datuplet-runsecrets-" + first 8 chars of the run UUID.
	// ------------------------------------------------------------------
	snapshotName := "datuplet-runsecrets-" + result.RunID.String()[:8]
	if !secretExists(t, namespace, snapshotName) {
		t.Fatalf("snapshot Secret %q does not exist after a successful run", snapshotName)
	}

	// ------------------------------------------------------------------
	// (e) Rotating the project secret's value after the run was admitted
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
	// (d, part 2) Deleting the PipelineRun garbage-collects the snapshot
	// (ownerRef). The run's CR name is generated by pipeline-api, so select
	// by the datuplet.io/run-id label the controller stamps on every
	// PipelineRun / Pod (not a harness-chosen name).
	// ------------------------------------------------------------------
	if out, err := exec.CommandContext(ctx, "kubectl", "delete", "pipelinerun",
		"-l", "datuplet.io/run-id="+result.RunID.String(),
		"-n", namespace, "--ignore-not-found").CombinedOutput(); err != nil {
		t.Fatalf("kubectl delete pipelinerun (run-id=%s): %v\n%s", result.RunID, err, out)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if !secretExists(t, namespace, snapshotName) {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("snapshot Secret %q was not garbage-collected within 20s of deleting the run's PipelineRun (run-id=%s)",
				snapshotName, result.RunID)
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
