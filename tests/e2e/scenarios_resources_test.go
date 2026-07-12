// Package e2e — RFC 026 P3 Task T10: resource contract (default / clamp /
// diff-gate) end-to-end coverage, plus the one-time platform-superadmin
// bootstrap grant the API-mode scenarios depend on.
//
// The resource contract has two enforcement layers (RFC 026 §4.4), and this
// file exercises both live:
//
//   - Layer 2 (controller admission, direct-kubectl path):
//     TestResources_RegistryDefaultsApplied asserts the registry
//     resources.default lands on a component that sets none;
//     TestResources_ClampedAtAdmission asserts an over-max spec is clamped to
//     the registry max, frozen into status.resolvedSpec, noted on
//     status.message, and the run still proceeds to success.
//
//   - Layer 1 (pipeline-api PUT diff-gate):
//     TestResources_OverMaxModificationRejected asserts a NON-superadmin PUT
//     that modifies a component's resources is 403'd (the modification gate
//     wins over the over-max 400 — T6 precedence);
//     TestResources_SuperadminPutSucceeds asserts the granted superadmin can
//     PUT a within-max resources block where the non-superadmin is blocked.
//
// These don't fit the declarative Scenario{}/Assertion{} shape in
// scenarios_test.go: the kubectl-path tests inspect Job container resources /
// status.resolvedSpec / status.message directly, and the API-path tests drive
// pipeline-api's HTTP PUT as two distinct identities. They mirror
// TestRegistry_* (scenarios_registry_test.go) and TestSecretsLadder
// (scenarios_secrets_test.go) respectively.
package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// ──────────────────────────────────────────────────────────────────────────
// Superadmin bootstrap grant (invoked once from TestMain).
// ──────────────────────────────────────────────────────────────────────────

// execPipelineAPIAdmin runs `pipeline-api admin <args...>` inside the
// pipeline-api Pod (which already carries DATABASE_URL / SIGNING_KEY_FILE /
// OPENFGA_* env), returning the combined output. Mirrors
// ensureSecretsLadderProject's kubectl-exec pattern.
func execPipelineAPIAdmin(ctx context.Context, args ...string) (string, error) {
	pod := queryFindPipelineAPIPod(ctx)
	if pod == "" {
		return "", fmt.Errorf("pipeline-api pod not found in namespace %q", queryE2ENamespace)
	}
	full := append([]string{"exec", pod, "-n", queryE2ENamespace, "--",
		"/usr/local/bin/pipeline-api", "admin"}, args...)
	out, err := exec.CommandContext(ctx, "kubectl", full...).CombinedOutput()
	return string(out), err
}

// isFGAAlreadyExists reports whether a `pipeline-api admin grant` invocation
// failed only because the tuple was already written (idempotent re-run).
// Mirrors framework.isAlreadyExistsErr / cmd/pipeline-api/admin_authz.go.
func isFGAAlreadyExists(out string) bool {
	return strings.Contains(out, "already exists") ||
		strings.Contains(out, "cannot write a tuple")
}

// grantAdminSuperadmin grants FGA platform superadmin (server.admin) to the
// e2e admin identity (admin@datuplet.local) via
// `pipeline-api admin grant --user <admin> --superadmin`, run inside the
// pipeline-api Pod. This is the Option-A UUID-subject seed (RFC 026 P3 Task
// T4): the CLI resolves the admin's DB UUID, discovers the server:<uuid>
// singleton from the FGA /changes feed, and writes
// (user:oidc~<uuid>, admin, server:<uuid>).
//
// ORDERING: called once from TestMain AFTER SetupFGABootstrap succeeds — i.e.
// after the cluster's install-time `pipeline-api admin lakekeeper-bootstrap`
// has already created the server:<uuid> object the grant discovers. Idempotent:
// an already-granted tuple is treated as success so repeated `go test` runs
// against the same cluster don't fail.
func grantAdminSuperadmin() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lkURL := fmt.Sprintf("http://lakekeeper.%s.svc.cluster.local:8181", queryE2ENamespace)
	fgaURL := fmt.Sprintf("http://openfga.%s.svc.cluster.local:8080", queryE2ENamespace)
	out, err := execPipelineAPIAdmin(ctx, "grant",
		"--user="+queryAdminEmail, "--superadmin",
		"--lakekeeper-url="+lkURL, "--openfga-url="+fgaURL)
	if err != nil {
		if isFGAAlreadyExists(out) {
			return nil
		}
		return fmt.Errorf("admin grant --superadmin: %w\noutput: %s", err, out)
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────────────
// (a) Registry defaults applied — kubectl path, component sets no resources.
// ──────────────────────────────────────────────────────────────────────────

func TestResources_RegistryDefaultsApplied(t *testing.T) {
	h := registrySkipUnlessReady(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())
	ns := kb.Namespace

	rendered := renderRegistryFixture(t, "resource-default.yaml")
	prName := runPrefix + "-resource-default-run"

	result, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3"})
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got failureType=%q message=%q logs=%s",
			result.FailureType, result.StatusMessage, result.Logs)
	}

	jobName := waitForComponentJob(t, ns, prName, "gen", 60*time.Second)

	// The built Job container carries the registry resources.default verbatim.
	assertJobResource(t, ns, jobName, "limits", "cpu", framework.DataGeneratorDefaultLimCPU)
	assertJobResource(t, ns, jobName, "limits", "memory", framework.DataGeneratorDefaultLimMemory)
	assertJobResource(t, ns, jobName, "requests", "cpu", framework.DataGeneratorDefaultReqCPU)
	assertJobResource(t, ns, jobName, "requests", "memory", framework.DataGeneratorDefaultReqMemory)

	// The same default is frozen into status.resolvedSpec (admission snapshot).
	assertResolvedResource(t, ns, prName, "limits", "cpu", framework.DataGeneratorDefaultLimCPU)
	assertResolvedResource(t, ns, prName, "requests", "memory", framework.DataGeneratorDefaultReqMemory)
}

// ──────────────────────────────────────────────────────────────────────────
// (c) Clamped at admission — kubectl path, over-max spec clamped to max.
// ──────────────────────────────────────────────────────────────────────────

func TestResources_ClampedAtAdmission(t *testing.T) {
	h := registrySkipUnlessReady(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())
	ns := kb.Namespace

	rendered := renderRegistryFixture(t, "resource-clamp.yaml")
	prName := runPrefix + "-resource-clamp-run"

	type runOutcome struct {
		result *framework.RunResult
		err    error
	}
	resultCh := make(chan runOutcome, 1)
	go func() {
		result, runErr := kb.RunPipeline(ctx, rendered, framework.RunOpts{
			StorageType: "s3",
			Timeout:     5 * time.Minute,
		})
		resultCh <- runOutcome{result, runErr}
	}()

	// The Job existing proves admission completed and the controller clamp ran.
	jobName := waitForComponentJob(t, ns, prName, "gen", 90*time.Second)

	// (3, read first) The clamp note is on status.message DURING the run —
	// the success path (handleTerminal) overwrites it with "All stages
	// completed successfully", so read it before joining the run goroutine.
	msg, err := kubectlJSONPath("pipelinerun", prName, ns, "{.status.message}")
	if err != nil {
		t.Fatalf("read status.message: %v", err)
	}
	if !strings.Contains(msg, "clamped") || !strings.Contains(msg, "gen") {
		t.Errorf("status.message %q does not note the resource clamp for component %q", msg, "gen")
	}

	// (1) The Job container carries the clamped-to-max limits; the within-max
	// requests pass through unchanged.
	assertJobResource(t, ns, jobName, "limits", "cpu", framework.DataGeneratorMaxCPU)       // 4 -> 2
	assertJobResource(t, ns, jobName, "limits", "memory", framework.DataGeneratorMaxMemory) // 1Gi -> 512Mi
	assertJobResource(t, ns, jobName, "requests", "cpu", "100m")
	assertJobResource(t, ns, jobName, "requests", "memory", "128Mi")

	// (2) The clamp is frozen into status.resolvedSpec (never re-derived).
	assertResolvedResource(t, ns, prName, "limits", "cpu", framework.DataGeneratorMaxCPU)
	assertResolvedResource(t, ns, prName, "limits", "memory", framework.DataGeneratorMaxMemory)

	// (4) The clamped run still PROCEEDS to success.
	outcome := <-resultCh
	if outcome.err != nil {
		t.Fatalf("run pipeline: %v", outcome.err)
	}
	if !outcome.result.Success {
		t.Fatalf("clamped run did not succeed (RFC 026 §4.4: clamped run proceeds): failureType=%q message=%q logs=%s",
			outcome.result.FailureType, outcome.result.StatusMessage, outcome.result.Logs)
	}
}

// assertJobResource asserts the "component" container's resources.<kind>.<name>
// on Job jobName equals want (kind ∈ {limits, requests}; name ∈ {cpu, memory}).
func assertJobResource(t *testing.T, ns, jobName, kind, name, want string) {
	t.Helper()
	path := fmt.Sprintf(`{.spec.template.spec.containers[?(@.name=="component")].resources.%s.%s}`, kind, name)
	got, err := kubectlJSONPath("job", jobName, ns, path)
	if err != nil {
		t.Fatalf("read job %s resources.%s.%s: %v", jobName, kind, name, err)
	}
	if got != want {
		t.Errorf("job %s resources.%s.%s = %q, want %q", jobName, kind, name, got, want)
	}
}

// assertResolvedResource asserts the first component's frozen
// status.resolvedSpec resources.<kind>.<name> equals want.
func assertResolvedResource(t *testing.T, ns, prName, kind, name, want string) {
	t.Helper()
	path := fmt.Sprintf("{.status.resolvedSpec.stages[0].components[0].resources.%s.%s}", kind, name)
	got, err := kubectlJSONPath("pipelinerun", prName, ns, path)
	if err != nil {
		t.Fatalf("read resolvedSpec resources.%s.%s: %v", kind, name, err)
	}
	if got != want {
		t.Errorf("resolvedSpec resources.%s.%s = %q, want %q", kind, name, got, want)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// (b) + (d) API PUT diff-gate — non-superadmin blocked, superadmin allowed.
// ──────────────────────────────────────────────────────────────────────────

const (
	resourcesNonadminEmail    = "e2e-resources-nonadmin@datuplet.local"
	resourcesNonadminPassword = "changeme-resources"
	resourcesOverMaxName      = "resource-gate-overmax"
	resourcesWithinMaxName    = "resource-gate-within"
)

// resourcesOverMaxPipelineYAML sets a component resources block whose
// limits.cpu (4) exceeds the registry max (2) — over-max AND a modification.
const resourcesOverMaxPipelineYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: resource-gate-overmax
  namespace: datuplet
spec:
  stages:
    - name: generate
      components:
        - name: gen
          component: data-generator
          version: v0.0.1
          config:
            tables:
              - name: og
                random:
                  schema: {id: int}
                  limit: {rowsCount: 10}
          resources:
            limits:
              cpu: "4"
              memory: 1Gi
          outputs:
            defaultBucket: resource-gate-overmax-bucket
            defaultWriteMode: APPEND
`

// resourcesWithinMaxPipelineYAML sets a component resources block entirely
// within the registry max (cpu 1 <= 2, memory 256Mi <= 512Mi).
const resourcesWithinMaxPipelineYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: resource-gate-within
  namespace: datuplet
spec:
  stages:
    - name: generate
      components:
        - name: gen
          component: data-generator
          version: v0.0.1
          config:
            tables:
              - name: wi
                random:
                  schema: {id: int}
                  limit: {rowsCount: 10}
          resources:
            limits:
              cpu: "1"
              memory: 256Mi
            requests:
              cpu: "100m"
              memory: 128Mi
          outputs:
            defaultBucket: resource-gate-within-bucket
            defaultWriteMode: APPEND
`

var (
	resourcesAPIOnce            sync.Once
	resourcesAPIProjectID       string
	resourcesAPINonadminSession string
	resourcesAPISetupErr        error
)

// ensureResourcesAPIFixture provisions the shared state the API-path gate
// scenarios need, once per test process:
//
//   - a Datuplet project row for the harness's lakekeeper project (reusing
//     ensureSecretsLadderProject — idempotent `admin create-project`), so
//     {pid} resolves and mustHaveRelation runs a real FGA check;
//   - a NON-superadmin DB user granted `editor` on that project, so it reaches
//     the resources diff-gate with data_admin but WITHOUT superadmin;
//   - that user's session cookie.
//
// The superadmin identity is the standard admin session (getAdminSession) —
// admin@datuplet.local, granted platform superadmin once in TestMain
// (grantAdminSuperadmin). Returns (projectID, adminSession, nonadminSession).
func ensureResourcesAPIFixture(t *testing.T) (projectID, adminSession, nonadminSession string) {
	t.Helper()
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil — E2E_K8S=1 + bootstrap must have run in TestMain")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable on NodePort 30081 — start port-forward")
	}

	resourcesAPIOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		pid, err := ensureSecretsLadderProject(ctx, h)
		if err != nil {
			resourcesAPISetupErr = fmt.Errorf("ensure project: %w", err)
			return
		}

		// Non-superadmin DB user. Idempotent — admin create-user tolerates an
		// already-existing email (cmd/pipeline-api/admin.go).
		if out, err := execPipelineAPIAdmin(ctx, "create-user",
			"--email="+resourcesNonadminEmail, "--password="+resourcesNonadminPassword); err != nil {
			resourcesAPISetupErr = fmt.Errorf("create-user: %w\n%s", err, out)
			return
		}
		// Grant that user editor on the project so mustHaveRelation("data_admin")
		// passes (editor unions into data_admin) — but it is NOT superadmin, so
		// the resources modification gate applies to it.
		lkURL := fmt.Sprintf("http://lakekeeper.%s.svc.cluster.local:8181", queryE2ENamespace)
		fgaURL := fmt.Sprintf("http://openfga.%s.svc.cluster.local:8080", queryE2ENamespace)
		if out, err := execPipelineAPIAdmin(ctx, "grant",
			"--user="+resourcesNonadminEmail, "--project="+h.LakekeeperProjectName, "--role=editor",
			"--lakekeeper-url="+lkURL, "--openfga-url="+fgaURL); err != nil && !isFGAAlreadyExists(out) {
			resourcesAPISetupErr = fmt.Errorf("grant editor: %w\n%s", err, out)
			return
		}

		cookie, _, err := querySessionLogin(ctx, framework.PipelineAPIBaseURL(),
			resourcesNonadminEmail, resourcesNonadminPassword)
		if err != nil {
			resourcesAPISetupErr = fmt.Errorf("nonadmin session login: %w", err)
			return
		}
		resourcesAPIProjectID = pid
		resourcesAPINonadminSession = cookie
	})
	if resourcesAPISetupErr != nil {
		framework.SkipOrFail(t, "resources API fixture setup failed: %v", resourcesAPISetupErr)
	}
	// getAdminSession has its own once + skip-on-failure; admin was granted
	// superadmin in TestMain.
	return resourcesAPIProjectID, getAdminSession(t), resourcesAPINonadminSession
}

// (b) A non-superadmin PUT that sets a component resources block above the
// registry max is 403'd by the modification gate — NOT 400'd for over-max
// (T6 precedence: the diff-gate runs before the over-max findings). The 403
// body points the caller at superadmin.
func TestResources_OverMaxModificationRejected(t *testing.T) {
	projectID, adminSession, nonadmin := ensureResourcesAPIFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() {
		// The 403 stores nothing, but clean up defensively in case a prior run
		// left the pipeline behind. Admin holds project_admin (data_admin).
		_, _, _ = deletePipelineHTTP(context.Background(), adminSession, projectID, resourcesOverMaxName)
	})

	status, body, err := putPipelineHTTP(ctx, nonadmin, projectID, resourcesOverMaxName, []byte(resourcesOverMaxPipelineYAML))
	if err != nil {
		t.Fatalf("PUT /pipelines (nonadmin, over-max resources): %v", err)
	}
	if status != http.StatusForbidden {
		t.Fatalf("PUT /pipelines (nonadmin, over-max resources): status=%d, want 403 (modification gate beats over-max 400); body=%s",
			status, body)
	}
	if !strings.Contains(strings.ToLower(string(body)), "superadmin") {
		t.Errorf("403 body does not mention superadmin: %s", body)
	}
}

// (d) The granted superadmin can PUT a within-max resources block where the
// non-superadmin is blocked. Covers both halves of the T6 gate.
func TestResources_SuperadminPutSucceeds(t *testing.T) {
	projectID, adminSession, nonadmin := ensureResourcesAPIFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() {
		_, _, _ = deletePipelineHTTP(context.Background(), adminSession, projectID, resourcesWithinMaxName)
	})

	// Non-superadmin: even a within-max block is a modification → 403 +
	// superadmin (the diff-gate is modification-based, not over-max-based).
	status, body, err := putPipelineHTTP(ctx, nonadmin, projectID, resourcesWithinMaxName, []byte(resourcesWithinMaxPipelineYAML))
	if err != nil {
		t.Fatalf("PUT /pipelines (nonadmin, within-max resources): %v", err)
	}
	if status != http.StatusForbidden {
		t.Fatalf("PUT /pipelines (nonadmin, within-max resources): status=%d, want 403 (modification gate); body=%s",
			status, body)
	}
	if !strings.Contains(strings.ToLower(string(body)), "superadmin") {
		t.Errorf("nonadmin 403 body does not mention superadmin: %s", body)
	}

	// Superadmin: the same within-max block is accepted (bypasses the
	// modification gate; within max so no over-max 400). 204, or 200 if the
	// save carries non-blocking warnings.
	status, body, err = putPipelineHTTP(ctx, adminSession, projectID, resourcesWithinMaxName, []byte(resourcesWithinMaxPipelineYAML))
	if err != nil {
		t.Fatalf("PUT /pipelines (superadmin, within-max resources): %v", err)
	}
	if status != http.StatusNoContent && status != http.StatusOK {
		t.Fatalf("PUT /pipelines (superadmin, within-max resources): status=%d, want 204 or 200; body=%s",
			status, body)
	}
}
