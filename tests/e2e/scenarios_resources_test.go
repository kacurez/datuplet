// Package e2e — RFC 026 P3 Task T10: resource contract (default / diff-gate)
// end-to-end coverage, plus the one-time platform-superadmin bootstrap grant
// the API-mode scenarios depend on.
//
// The resource contract has two enforcement layers (RFC 026 §4.4, §4.6):
//
//   - Layer 1 (pipeline-api PUT policy): TestResources_PutGate drives
//     pipeline-api's PUT as two identities — a non-superadmin is 403'd by the
//     modification gate for ANY resources block (it wins over the over-max
//     400 by T6 precedence); a superadmin is 400'd for an over-max block and
//     accepted (204) for a within-max block.
//
//   - Layer 2 (controller admission): the registry resources.default lands on a
//     component that sets none — asserted by TestRegistry_UnpinnedResolvesStable
//     (scenarios_registry_test.go), whose fixture sets no resources block. The
//     controller's OVER-MAX CLAMP is the direct-kubectl defence-in-depth
//     (validate.Policy is nil on that path — policy.go); it is unreachable from
//     the HTTP surface this harness drives (RFC 027 E2/E5) and is covered by the
//     controller's own unit tests (pkg/k8s/controllers/resource_clamp*_test.go).
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
// API PUT diff-gate — non-superadmin blocked, superadmin allowed.
// ──────────────────────────────────────────────────────────────────────────

const (
	resourcesNonadminEmail    = "e2e-resources-nonadmin@datuplet.local"
	resourcesNonadminPassword = "changeme-resources"
	resourcesOverMaxName      = "resource-gate-overmax"
	resourcesWithinMaxName    = "resource-gate-within"
)

// resourcesOverMaxPipelineYAML sets a component resources block whose
// cpu limit (4) exceeds the registry max (2) — over-max AND a modification.
// Envelope-free PipelineDoc (RFC 027): resources is the flat memory/cpu
// limits shape (config.ResourceSpec), not the CR limits/requests block.
const resourcesOverMaxPipelineYAML = `name: resource-gate-overmax
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
          cpu: "4"
          memory: 1Gi
        outputs:
          defaultBucket: resource-gate-overmax-bucket
          defaultWriteMode: APPEND
`

// resourcesWithinMaxPipelineYAML sets a component resources block entirely
// within the registry max (cpu 1 <= 2, memory 256Mi <= 512Mi). Envelope-free
// PipelineDoc (RFC 027): flat memory/cpu limits — the CR requests split is
// dropped (registry resources.default supplies requests at admission).
const resourcesWithinMaxPipelineYAML = `name: resource-gate-within
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
          cpu: "1"
          memory: 256Mi
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

// TestResources_PutGate covers both halves of the T6 resources gate on PUT
// /pipelines:
//
//   - nonadmin: any component resources block is a modification → 403 whose
//     body points at superadmin. For the over-max doc this proves the
//     modification gate beats the over-max 400 (T6 precedence: the diff-gate
//     runs before the over-max findings); for the within-max doc it proves the
//     gate is modification-based, not over-max-based.
//   - superadmin over-max: bypasses the modification gate but is rejected
//     pre-flight with a 400 naming every offending limit ("exceeds registry
//     max"; cpu 4 > 2 and memory 1Gi > 512Mi).
//   - superadmin within-max: accepted — 204, or 200 if the save carries
//     non-blocking warnings.
func TestResources_PutGate(t *testing.T) {
	projectID, adminSession, nonadmin := ensureResourcesAPIFixture(t)
	t.Cleanup(func() {
		// Rejected PUTs store nothing, but clean up defensively in case a prior
		// run left a pipeline behind. Admin holds project_admin (data_admin).
		_, _, _ = deletePipelineHTTP(context.Background(), adminSession, projectID, resourcesOverMaxName)
		_, _, _ = deletePipelineHTTP(context.Background(), adminSession, projectID, resourcesWithinMaxName)
	})

	t.Run("nonadmin-403", func(t *testing.T) {
		for _, tc := range []struct {
			label, name, yaml string
		}{
			{"over-max resources", resourcesOverMaxName, resourcesOverMaxPipelineYAML},
			{"within-max resources", resourcesWithinMaxName, resourcesWithinMaxPipelineYAML},
		} {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			status, body, err := putPipelineHTTP(ctx, nonadmin, projectID, tc.name, []byte(tc.yaml))
			cancel()
			if err != nil {
				t.Errorf("PUT /pipelines (nonadmin, %s): %v", tc.label, err)
				continue
			}
			if status != http.StatusForbidden {
				t.Errorf("PUT /pipelines (nonadmin, %s): status=%d, want 403 (modification gate); body=%s",
					tc.label, status, body)
				continue
			}
			if !strings.Contains(strings.ToLower(string(body)), "superadmin") {
				t.Errorf("nonadmin 403 body (%s) does not mention superadmin: %s", tc.label, body)
			}
		}
	})

	t.Run("superadmin-over-max-400", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		status, body, err := putPipelineHTTP(ctx, adminSession, projectID, resourcesOverMaxName, []byte(resourcesOverMaxPipelineYAML))
		if err != nil {
			t.Fatalf("PUT /pipelines (superadmin, over-max resources): %v", err)
		}
		if status != http.StatusBadRequest {
			t.Fatalf("PUT /pipelines (superadmin, over-max resources): status=%d, want 400; body=%s", status, body)
		}
		msg := string(body)
		if !strings.Contains(msg, "exceeds registry max") {
			t.Errorf("rejection does not describe the over-max violation: %s", msg)
		}
		if !strings.Contains(msg, "resources.limits.cpu") {
			t.Errorf("rejection does not name the over-max cpu limit: %s", msg)
		}
		if !strings.Contains(msg, "resources.limits.memory") {
			t.Errorf("rejection does not name the over-max memory limit: %s", msg)
		}
	})

	t.Run("superadmin-within-max-204", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		status, body, err := putPipelineHTTP(ctx, adminSession, projectID, resourcesWithinMaxName, []byte(resourcesWithinMaxPipelineYAML))
		if err != nil {
			t.Fatalf("PUT /pipelines (superadmin, within-max resources): %v", err)
		}
		if status != http.StatusNoContent && status != http.StatusOK {
			t.Fatalf("PUT /pipelines (superadmin, within-max resources): status=%d, want 204 or 200; body=%s",
				status, body)
		}
	})
}
