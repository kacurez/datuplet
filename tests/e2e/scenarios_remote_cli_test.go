// Package e2e — datuplet run --remote integration smoke.
//
// TestRemoteCLI_LoginAndRun brings up a Helm-installed cluster (via
// install-smoke.sh or make e2e-up-k8s), exercises `datuplet login --remote`
// and `datuplet run --remote` against the live pipeline-api NodePort (30081),
// then asserts the resulting iceberg snapshot carries run_mode=local-cli via
// the /api/v1/storage/projects/{pid}/tables/{ns}/{t}/snapshots endpoint.
//
// # Skip conditions
//
//   - E2E_K8S != "1"
//   - framework.SharedHarness() == nil (FGA bootstrap not run)
//   - pipeline-api NodePort (localhost:30081) not reachable
//   - Docker daemon not reachable (datuplet run --remote shells out to docker)
//   - ./bin/datuplet binary not found (build with `make build` first)
//
// # Network topology
//
// The test runs on the developer's Mac (OrbStack). `datuplet run --remote`
// spawns Docker containers that need to reach lakekeeper. The cluster's
// lakekeeper service is ClusterIP-only in the Helm chart, so the test sets
// up a kubectl port-forward to an ephemeral localhost port and sets
// pipelineApi.lakekeeperPublicUrl → http://host.docker.internal:<port>/catalog
// so the /api/v1/auth/token response returns the correct URL for containers.
//
// # Helm namespace
//
// The test targets the `datuplet-smoke` namespace created by
// tests/helm/install-smoke.sh. The make e2e-remote-cli target calls
// install-smoke.sh before running this test.
package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// TestRemoteCLI_LoginAndRun is the remote-CLI integration smoke:
//
//  1. Skip unless E2E_K8S=1, SharedHarness set, pipeline-api reachable, docker available.
//  2. Read admin creds from the K8s admin-creds Secret (namespace datuplet-smoke).
//  3. Port-forward lakekeeper to an ephemeral localhost port.
//  4. Patch pipeline-api PIPELINE_API_LAKEKEEPER_PUBLIC_URL so /auth/token
//     returns http://host.docker.internal:<port>/catalog.
//  5. Run `./bin/datuplet login --remote http://localhost:30081` (subprocess, stdin piped).
//  6. Run `./bin/datuplet run --remote http://localhost:30081 <pipeline.yaml>`.
//  7. Session-login to pipeline-api; query snapshots endpoint.
//  8. Assert ≥1 snapshot with run_mode=local-cli and non-empty actor.
func TestRemoteCLI_LoginAndRun(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		t.Skip("K8s tier requires SetupFGABootstrap to have run in TestMain — see framework/bootstrap.go")
	}

	if !framework.PipelineAPIReachable() {
		t.Skip("pipeline-api NodePort (localhost:30081) not reachable — is the cluster up?")
	}

	if err := checkDockerAvailableForRemoteCLI(); err != nil {
		t.Skipf("docker not available: %v", err)
	}

	datupletBin := remoteCLIBinPath(t)

	const (
		// helmNamespace is the namespace used by install-smoke.sh and the
		// e2e-remote-cli make target.
		helmNamespace = "datuplet-smoke"
		// helmRelease is the Helm release name used by install-smoke.sh.
		helmRelease = "datuplet"
		// pipelineAPIURL is the NodePort exposed by the chart's pipeline-api service.
		pipelineAPIURL = "http://localhost:30081"
		// adminEmail is the seeded admin email (chart initAdmin.email default).
		adminEmail = "admin@datuplet.local"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// -----------------------------------------------------------------------
	// Step 1: Read admin password from the K8s admin-creds Secret.
	// -----------------------------------------------------------------------
	// This test is gated on a Helm-installed cluster (datuplet-smoke
	// namespace) — it's invoked by `make e2e-remote-cli` after
	// tests/helm/install-smoke.sh runs. When the broader `make e2e-k8s`
	// suite runs against deploy-local.sh's `datuplet-e2e` namespace
	// (which has no admin-creds Secret), this test soft-skips rather
	// than failing — the Helm-install path is its own e2e flow.
	adminPassword, err := readAdminPasswordFromSecret(ctx, helmNamespace, helmRelease)
	if err != nil {
		t.Skipf("admin-creds Secret not found in %s/%s — TestRemoteCLI requires a Helm install (run via `make e2e-remote-cli`, not `make e2e-k8s`): %v", helmNamespace, helmRelease, err)
	}
	t.Logf("admin credentials read from K8s Secret (namespace=%s)", helmNamespace)

	// -----------------------------------------------------------------------
	// Step 2: Port-forward lakekeeper to an ephemeral localhost port.
	// We use an ephemeral port so we don't conflict with the port-forward
	// that make e2e-up-k8s may already have on port 8181.
	// -----------------------------------------------------------------------
	lkSvc := "svc/" + helmRelease + "-lakekeeper"
	lkPort, stopLKPF, err := startPortForward(ctx, helmNamespace, lkSvc, 8181)
	if err != nil {
		t.Fatalf("port-forward lakekeeper: %v", err)
	}
	defer stopLKPF()
	t.Logf("lakekeeper port-forward: localhost:%d → cluster:8181", lkPort)

	// URL that Docker containers use to reach lakekeeper on the host machine.
	lkPublicURL := fmt.Sprintf("http://host.docker.internal:%d/catalog", lkPort)

	// -----------------------------------------------------------------------
	// Step 3: Patch pipeline-api to return lkPublicURL from /api/v1/auth/token.
	// -----------------------------------------------------------------------
	origLKURL, err := getPipelineAPIEnv(ctx, helmNamespace, helmRelease, "PIPELINE_API_LAKEKEEPER_PUBLIC_URL")
	if err != nil {
		t.Logf("note: could not read current PIPELINE_API_LAKEKEEPER_PUBLIC_URL (ok if absent): %v", err)
	}
	if err := patchPipelineAPIEnv(ctx, helmNamespace, helmRelease, "PIPELINE_API_LAKEKEEPER_PUBLIC_URL", lkPublicURL); err != nil {
		t.Fatalf("patch PIPELINE_API_LAKEKEEPER_PUBLIC_URL: %v", err)
	}
	t.Cleanup(func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := patchPipelineAPIEnv(restoreCtx, helmNamespace, helmRelease, "PIPELINE_API_LAKEKEEPER_PUBLIC_URL", origLKURL); err != nil {
			t.Logf("warning: restore PIPELINE_API_LAKEKEEPER_PUBLIC_URL failed: %v", err)
		}
	})

	deployName := helmRelease + "-pipeline-api"
	waitRollout(t, ctx, helmNamespace, deployName)
	t.Logf("pipeline-api rolled out with updated PIPELINE_API_LAKEKEEPER_PUBLIC_URL=%s", lkPublicURL)

	// -----------------------------------------------------------------------
	// Step 4: datuplet login --remote — subprocess with piped stdin.
	// HOME is set to a t.TempDir() so credential files don't pollute the
	// developer's ~/.datuplet.
	// -----------------------------------------------------------------------
	fakeHome := t.TempDir()
	loginInput := fmt.Sprintf("%s\n%s\n", adminEmail, adminPassword)

	loginCmd := exec.CommandContext(ctx, datupletBin, "login", "--remote", pipelineAPIURL)
	loginCmd.Stdin = strings.NewReader(loginInput)
	loginCmd.Env = append(os.Environ(), "HOME="+fakeHome)
	loginOut, err := loginCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("datuplet login failed: %v\noutput:\n%s", err, loginOut)
	}
	t.Logf("datuplet login output:\n%s", loginOut)

	tokenPath := filepath.Join(fakeHome, ".datuplet", "token")
	if _, err := os.Stat(tokenPath); err != nil {
		t.Fatalf("~/.datuplet/token not created: %v", err)
	}

	// -----------------------------------------------------------------------
	// Step 5: datuplet run --remote — subprocess with same fakeHome.
	// Render the simple-extract pipeline template to a temp file.
	// -----------------------------------------------------------------------
	pipelinesDir, err := filepath.Abs("pipelines")
	if err != nil {
		t.Fatalf("resolve pipelines dir: %v", err)
	}
	templatePath := filepath.Join(pipelinesDir, "docker/simple-extract.yaml")
	vars := framework.TemplateVars{RunPrefix: runPrefix + "-remote"}
	renderedPipeline, err := framework.RenderPipeline(templatePath, vars)
	if err != nil {
		t.Fatalf("render pipeline template: %v", err)
	}
	defer os.Remove(renderedPipeline)

	// 6-minute timeout for the run itself: data-generator is fast (~10 rows),
	// but Docker image pulls on a cold cache can take a minute.
	runCtx, runCancel := context.WithTimeout(ctx, 6*time.Minute)
	defer runCancel()

	runCmd := exec.CommandContext(runCtx, datupletBin, "run", "--remote", pipelineAPIURL, renderedPipeline)
	runCmd.Env = append(os.Environ(), "HOME="+fakeHome)
	runOut, err := runCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("datuplet run --remote failed: %v\noutput:\n%s", err, runOut)
	}
	t.Logf("datuplet run --remote output:\n%s", runOut)

	// -----------------------------------------------------------------------
	// Step 6: Session-login to pipeline-api and discover the project ID.
	// -----------------------------------------------------------------------
	sessionCookie, userID, err := remoteCLISessionLogin(ctx, pipelineAPIURL, adminEmail, adminPassword)
	if err != nil {
		t.Fatalf("session login for snapshot check: %v", err)
	}
	t.Logf("session login OK: user_id=%s", userID)

	projectID, err := remoteCLIFindProject(ctx, pipelineAPIURL, sessionCookie, "default")
	if err != nil {
		t.Fatalf("find project ID: %v", err)
	}
	t.Logf("project ID: %s", projectID)

	// -----------------------------------------------------------------------
	// Step 7: Poll for the snapshot (iceberg commit job may not be done yet).
	// -----------------------------------------------------------------------
	ns := runPrefix + "-remote-raw"
	table := "products"

	var snaps []remoteCLISnapshotEntry
	pollDeadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(pollDeadline) {
		snaps, err = remoteCLIFetchSnapshots(ctx, pipelineAPIURL, sessionCookie, projectID, ns, table)
		if err == nil && len(snaps) > 0 {
			break
		}
		t.Logf("polling for snapshots: %v", err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		t.Fatalf("fetch snapshots for %s/%s: %v", ns, table, err)
	}
	if len(snaps) == 0 {
		t.Fatalf("no snapshots found for %s/%s after run --remote completed", ns, table)
	}
	t.Logf("found %d snapshot(s) for %s/%s", len(snaps), ns, table)

	// -----------------------------------------------------------------------
	// Step 8: Assert at least one snapshot has run_mode=local-cli.
	// -----------------------------------------------------------------------
	var foundLocalCLI bool
	for _, s := range snaps {
		t.Logf("  snapshot %d: run_mode=%q actor=%q run_id=%q", s.SnapshotID, s.RunMode, s.Actor, s.RunID)
		if s.RunMode == "local-cli" {
			if s.Actor == "" {
				t.Errorf("snapshot %d: run_mode=local-cli but actor is empty", s.SnapshotID)
			}
			foundLocalCLI = true
		}
	}
	if !foundLocalCLI {
		t.Errorf("no snapshot with run_mode=local-cli found in %d snapshot(s): %+v", len(snaps), snaps)
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

// remoteCLIBinPath returns the absolute path to ./bin/datuplet and skips the
// test if the binary does not exist (build with `make build` first).
func remoteCLIBinPath(t *testing.T) string {
	t.Helper()
	// The e2e tests run from tests/e2e/, so walk up two levels to the repo root.
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	binPath := filepath.Join(repoRoot, "bin", "datuplet")
	if _, err := os.Stat(binPath); err != nil {
		t.Skipf("./bin/datuplet not found (%v) — run `make build` first", err)
	}
	return binPath
}

// checkDockerAvailableForRemoteCLI returns an error if docker is not accessible.
func checkDockerAvailableForRemoteCLI() error {
	out, err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}").Output()
	if err != nil {
		return fmt.Errorf("docker info: %w", err)
	}
	if strings.TrimSpace(string(out)) == "" {
		return fmt.Errorf("docker info returned empty server version")
	}
	return nil
}

// startPortForward starts `kubectl port-forward -n <ns> <target> <localPort>:<remotePort>`
// using an OS-assigned local port. Returns the local port and a stop function.
// The port-forward process is killed when stopFn is called or when ctx expires.
func startPortForward(ctx context.Context, namespace, target string, remotePort int) (int, func(), error) {
	// Ask the OS for a free port then release it before kubectl uses it.
	// There is a small TOCTOU window, but kubectl port-forward will fail
	// loudly if the port was taken and we can surface the error clearly.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, nil, fmt.Errorf("find free port: %w", err)
	}
	localPort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cmd := exec.CommandContext(ctx, "kubectl", "port-forward",
		"-n", namespace,
		target,
		fmt.Sprintf("%d:%d", localPort, remotePort),
	)
	if err := cmd.Start(); err != nil {
		return 0, nil, fmt.Errorf("start kubectl port-forward: %w", err)
	}

	stop := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}

	// Poll until the TCP port accepts connections.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", localPort), 500*time.Millisecond)
		if dialErr == nil {
			conn.Close()
			return localPort, stop, nil
		}
		time.Sleep(300 * time.Millisecond)
	}

	stop()
	return 0, nil, fmt.Errorf("port-forward %s:%d → localhost:%d did not become ready within 20s", target, remotePort, localPort)
}

// readAdminPasswordFromSecret reads the admin password from the
// `<release>-admin-creds` K8s Secret in the given namespace.
func readAdminPasswordFromSecret(ctx context.Context, namespace, release string) (string, error) {
	secretName := release + "-admin-creds"
	if !strings.Contains(release, "datuplet") {
		secretName = release + "-datuplet-admin-creds"
	}
	out, err := exec.CommandContext(ctx, "kubectl", "-n", namespace,
		"get", "secret", secretName,
		"-o", "jsonpath={.data.password}").Output()
	if err != nil {
		return "", fmt.Errorf("kubectl get secret %s/%s: %w", namespace, secretName, err)
	}
	b64 := strings.TrimSpace(string(out))
	if b64 == "" {
		return "", fmt.Errorf("Secret %s/%s has empty .data.password", namespace, secretName)
	}
	// Decode in Go — avoids relying on the OS `base64` tool whose flags differ
	// between GNU coreutils (--decode) and macOS BSD (-D).
	decoded, decErr := base64.StdEncoding.DecodeString(b64)
	if decErr != nil {
		// K8s Secrets always use standard base64, but fall back to URL-safe
		// alphabet just in case.
		decoded, decErr = base64.URLEncoding.DecodeString(b64)
		if decErr != nil {
			return "", fmt.Errorf("base64 decode Secret %s/%s: %w", namespace, secretName, decErr)
		}
	}
	return strings.TrimSpace(string(decoded)), nil
}

// getPipelineAPIEnv reads a named env var from the pipeline-api Deployment's
// first container. Returns "" if the var is absent (not an error).
func getPipelineAPIEnv(ctx context.Context, namespace, release, envName string) (string, error) {
	deployName := release + "-pipeline-api"
	if !strings.Contains(release, "datuplet") {
		deployName = release + "-datuplet-pipeline-api"
	}
	jsonPath := fmt.Sprintf(
		`{.spec.template.spec.containers[0].env[?(@.name=="%s")].value}`,
		envName,
	)
	out, err := exec.CommandContext(ctx, "kubectl", "-n", namespace,
		"get", "deployment", deployName, "-o", "jsonpath="+jsonPath).Output()
	if err != nil {
		return "", fmt.Errorf("kubectl get deployment %s: %w", deployName, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// patchPipelineAPIEnv sets or clears a named env var on the pipeline-api
// Deployment via a strategic merge patch. An empty value clears the var
// (sets it to ""), which for PIPELINE_API_LAKEKEEPER_PUBLIC_URL causes the
// /auth/token endpoint to return an empty lakekeeper_url — acceptable for
// the cleanup path.
func patchPipelineAPIEnv(ctx context.Context, namespace, release, envName, value string) error {
	deployName := release + "-pipeline-api"
	if !strings.Contains(release, "datuplet") {
		deployName = release + "-datuplet-pipeline-api"
	}
	patch := fmt.Sprintf(
		`{"spec":{"template":{"spec":{"containers":[{"name":"pipeline-api","env":[{"name":%q,"value":%q}]}]}}}}`,
		envName, value,
	)
	out, err := exec.CommandContext(ctx, "kubectl", "-n", namespace,
		"patch", "deployment", deployName,
		"--type=strategic", "--patch", patch).CombinedOutput()
	if err != nil {
		return fmt.Errorf("patch deployment %s: %w\n%s", deployName, err, string(out))
	}
	return nil
}

// waitRollout waits for a Deployment rollout to complete (up to 2 minutes).
// Errors are logged but do not fail the test — the pipeline-api may still be
// responsive with the old pod while the new one starts.
func waitRollout(t *testing.T, ctx context.Context, namespace, deployName string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(waitCtx, "kubectl", "-n", namespace,
		"rollout", "status", "deployment/"+deployName, "--timeout=120s").CombinedOutput()
	if err != nil {
		t.Logf("rollout status for %s: %v\n%s", deployName, err, string(out))
	}
}

// remoteCLISessionLogin POSTs to /api/v1/auth/login and returns the session
// cookie + user_id. The session cookie is required for subsequent storage API calls.
func remoteCLISessionLogin(ctx context.Context, baseURL, email, password string) (cookie, userID string, err error) {
	body, err := json.Marshal(map[string]string{"email": email, "password": password})
	if err != nil {
		return "", "", err
	}
	url := strings.TrimRight(baseURL, "/") + "/api/v1/auth/login"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", "", fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", "", fmt.Errorf("login HTTP %d: %s", resp.StatusCode, string(b))
	}

	var loginResp struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return "", "", fmt.Errorf("decode login response: %w", err)
	}

	for _, c := range resp.Cookies() {
		if c.Name == "pipeline_api_session" {
			return c.Value, loginResp.UserID, nil
		}
	}
	return "", "", fmt.Errorf("no pipeline_api_session cookie in response")
}

// remoteCLIFindProject lists the user's projects and returns the ID of the
// one matching projectName. Falls back to the first project when only one exists.
func remoteCLIFindProject(ctx context.Context, baseURL, sessionCookie, projectName string) (string, error) {
	url := strings.TrimRight(baseURL, "/") + "/api/v1/projects"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: sessionCookie})

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("list projects HTTP %d: %s", resp.StatusCode, string(b))
	}

	var projects []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&projects); err != nil {
		return "", fmt.Errorf("decode projects: %w", err)
	}
	for _, p := range projects {
		if p.Name == projectName {
			return p.ID, nil
		}
	}
	if len(projects) == 1 {
		return projects[0].ID, nil
	}
	return "", fmt.Errorf("project %q not found in %d projects: %+v", projectName, len(projects), projects)
}

// remoteCLISnapshotEntry mirrors storage.snapshotHistoryEntry.
type remoteCLISnapshotEntry struct {
	SnapshotID   int64     `json:"snapshot_id"`
	CommittedAt  time.Time `json:"committed_at"`
	Actor        string    `json:"actor"`
	RunID        string    `json:"run_id"`
	RunMode      string    `json:"run_mode"`
	PipelineAPI  string    `json:"pipeline_api"`
	AddedRecords int64     `json:"added_records"`
}

// remoteCLIFetchSnapshots calls the snapshots endpoint for the given project/ns/table.
func remoteCLIFetchSnapshots(ctx context.Context, baseURL, sessionCookie, projectID, ns, table string) ([]remoteCLISnapshotEntry, error) {
	url := fmt.Sprintf("%s/api/v1/storage/projects/%s/tables/%s/%s/snapshots",
		strings.TrimRight(baseURL, "/"), projectID, ns, table)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: sessionCookie})

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("table %s/%s not found yet (HTTP 404)", ns, table)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("snapshots HTTP %d: %s", resp.StatusCode, string(b))
	}

	var snaps []remoteCLISnapshotEntry
	if err := json.NewDecoder(resp.Body).Decode(&snaps); err != nil {
		return nil, fmt.Errorf("decode snapshots: %w", err)
	}
	return snaps, nil
}
