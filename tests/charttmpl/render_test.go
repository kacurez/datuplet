// Package charttmpl holds `helm template` content-assertion tests for the
// datuplet-app chart. It is a test-only package in the root Go module (there
// is no non-test source here) and lives OUTSIDE charts/datuplet-app/ on
// purpose: the chart's .helmignore does not exclude *.go, so a test file under
// the chart dir would be packaged into the released .tgz.
//
// These tests shell out to the `helm` binary and assert on specific rendered
// stanzas (the app-worker secret mounts, the enable-guard, probes, hardening,
// PDB) rather than diffing whole-file golden renders — the fit the RFC 028 D1
// brief asks for. When `helm` is not on PATH (e.g. a unit-test CI job without
// it) each test skips; the repo's per-chart `helm lint`/`helm template` smoke
// job (.github/workflows/pr.yml) still exercises a full render there.
package charttmpl

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// chartDir returns the absolute path to charts/datuplet-app, resolved from this
// test file's location so it is independent of the test's working directory.
func chartDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// thisFile = <repo>/tests/charttmpl/render_test.go
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "charts", "datuplet-app"))
}

// helmOrSkip skips the test when the helm binary is unavailable.
func helmOrSkip(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm not on PATH — skipping chart-render assertions")
	}
	return path
}

// renderShowOnly runs `helm template --show-only <tmpl>` and returns stdout.
// It fails the test on any helm error.
func renderShowOnly(t *testing.T, showOnly string, extraSets ...string) string {
	t.Helper()
	helm := helmOrSkip(t)
	args := []string{"template", chartDir(t),
		"--set", "appWorker.enabled=true",
		"--set", "queryWorker.enabled=true",
		"--show-only", showOnly,
	}
	args = append(args, extraSets...)
	out, err := exec.Command(helm, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %s failed: %v\n%s", showOnly, err, out)
	}
	return string(out)
}

// mustContain asserts every needle appears in haystack.
func mustContain(t *testing.T, what, haystack string, needles ...string) {
	t.Helper()
	for _, n := range needles {
		if !strings.Contains(haystack, n) {
			t.Errorf("%s: expected rendered output to contain %q\n--- output ---\n%s", what, n, haystack)
		}
	}
}

// TestAppWorkerChart_SecretsMountBothSides is the core D1 assertion: the shared
// datuplet-app-worker Secret's service-token reaches BOTH app-worker and
// pipeline-api (same secret, same key path), the cookie-hmac-key + API URL
// reach app-worker (so it does not crash-loop at boot), and pipeline-api
// receives ONLY the service-token key.
func TestAppWorkerChart_SecretsMountBothSides(t *testing.T) {
	aw := renderShowOnly(t, "templates/app-worker/deployment.yaml")
	mustContain(t, "app-worker deployment", aw,
		"name: DATUPLET_APPWORKER_SERVICE_TOKEN_FILE",
		`value: "/var/run/secrets/datuplet-app-worker/service-token"`,
		"name: DATUPLET_APPWORKER_COOKIE_KEY_FILE",
		`value: "/var/run/secrets/datuplet-app-worker/cookie-hmac-key"`,
		"name: DATUPLET_API_URL",
		"secretName: datuplet-app-worker",
	)

	api := renderShowOnly(t, "templates/pipeline-api/deployment.yaml")
	mustContain(t, "pipeline-api deployment", api,
		"name: DATUPLET_APPS_INTERNAL_TOKEN_FILE",
		`value: "/var/run/secrets/datuplet-app-worker/service-token"`,
		"secretName: datuplet-app-worker",
		"key: service-token", // only-key projection — pipeline-api never gets cookie-hmac-key
	)
	// pipeline-api must NOT project the cookie-hmac-key nor read it: check the
	// concrete projection/env markers (a doc comment may mention the name).
	for _, forbidden := range []string{"key: cookie-hmac-key", "path: cookie-hmac-key", "DATUPLET_APPWORKER_COOKIE_KEY_FILE"} {
		if strings.Contains(api, forbidden) {
			t.Errorf("pipeline-api must NOT receive the cookie-hmac-key; found %q in the render:\n%s", forbidden, api)
		}
	}
}

// TestAppWorkerChart_ProbesAndHardening asserts the readiness/liveness split
// (/readyz gated on engine compile, /healthz always live) and the pod-hardening
// fields the contract mandates.
func TestAppWorkerChart_ProbesAndHardening(t *testing.T) {
	aw := renderShowOnly(t, "templates/app-worker/deployment.yaml")
	mustContain(t, "app-worker hardening", aw,
		"path: /readyz",
		"path: /healthz",
		"automountServiceAccountToken: false",
		"runAsNonRoot: true",
		"runAsUser: 1000",
		"allowPrivilegeEscalation: false",
		`drop: ["ALL"]`,
		"type: RuntimeDefault",
		"preStop:",
		"terminationGracePeriodSeconds: 30",
		// Stricter rootfs than query-worker: app-worker runs untrusted user JS.
		"readOnlyRootFilesystem: true",
		// The one writable surface: a /tmp emptyDir volume + its mount.
		"mountPath: /tmp",
		"emptyDir: {}",
	)
}

// TestAppWorkerChart_PDBMinAvailable asserts the PodDisruptionBudget exists with
// the spec-fixed minAvailable: 1.
func TestAppWorkerChart_PDBMinAvailable(t *testing.T) {
	pdb := renderShowOnly(t, "templates/app-worker/poddisruptionbudget.yaml")
	mustContain(t, "app-worker PDB", pdb,
		"kind: PodDisruptionBudget",
		"minAvailable: 1",
		"app.kubernetes.io/name: app-worker",
	)
}

// TestAppWorkerChart_NetworkPolicyEgressIsScoped asserts app-worker's egress is
// DNS + pipeline-api ONLY — it must NOT reach lakekeeper (:8181) or the object
// store (:9000), because app-worker holds zero storage credentials (D0 §C4).
func TestAppWorkerChart_NetworkPolicyEgressIsScoped(t *testing.T) {
	np := renderShowOnly(t, "templates/app-worker/networkpolicy.yaml")
	mustContain(t, "app-worker networkpolicy", np,
		"name: pipeline-api",
		"port: 8081",
		"port: 53",
	)
	for _, forbidden := range []string{"name: lakekeeper", "name: minio", "port: 8181", "port: 9000"} {
		if strings.Contains(np, forbidden) {
			t.Errorf("app-worker NetworkPolicy must not grant data-plane egress; found %q:\n%s", forbidden, np)
		}
	}
}

// TestAppWorkerChart_FailsWithoutQueryWorker asserts the hard-dependency guard:
// enabling appWorker without queryWorker aborts rendering with the contract
// message (RFC 028 §8).
func TestAppWorkerChart_FailsWithoutQueryWorker(t *testing.T) {
	helm := helmOrSkip(t)
	out, err := exec.Command(helm, "template", chartDir(t),
		"--set", "appWorker.enabled=true",
		"--set", "queryWorker.enabled=false",
	).CombinedOutput()
	if err == nil {
		t.Fatalf("expected helm template to fail when queryWorker is disabled, but it succeeded:\n%s", out)
	}
	if !strings.Contains(string(out), "appWorker requires queryWorker.enabled=true") {
		t.Errorf("expected the RFC 028 §8 guard message; got:\n%s", out)
	}
}

// TestAppWorkerChart_DisabledRendersNothing asserts the enable-guard: with
// appWorker.enabled=false neither the PDB nor pipeline-api's internal-token env
// is rendered (all four app-worker resources + the pipeline-api mount appear
// and disappear together).
func TestAppWorkerChart_DisabledRendersNothing(t *testing.T) {
	helm := helmOrSkip(t)
	out, err := exec.Command(helm, "template", chartDir(t),
		"--set", "appWorker.enabled=false",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template with appWorker disabled failed: %v\n%s", err, out)
	}
	rendered := string(out)
	for _, forbidden := range []string{"kind: PodDisruptionBudget", "DATUPLET_APPS_INTERNAL_TOKEN_FILE", "app.kubernetes.io/name: app-worker"} {
		if strings.Contains(rendered, forbidden) {
			t.Errorf("appWorker.enabled=false must render no app-worker artifacts; found %q", forbidden)
		}
	}
}
