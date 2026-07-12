// Package e2e — RFC 026 P2 Task R11: component-registry enforcement + freeze
// scenarios, exercised live against the ComponentDefinitions the framework
// registers in TestMain (see framework/components_bootstrap.go).
//
// These don't fit the declarative Scenario{}/Assertion{} shape in
// scenarios_test.go: they need to inspect PipelineRun.status.components /
// status.resolvedSpec and Job container images directly, and the freeze
// scenario needs to interleave a kubectl patch mid-run. Standalone Test*
// functions using framework.NewK8sBackend directly, mirroring
// TestSecretsLadder's approach in scenarios_secrets_test.go.
package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// kubectlJSONPath runs `kubectl get <kind> <name> -n <ns> -o jsonpath=<path>`
// and returns the trimmed output.
func kubectlJSONPath(kind, name, ns, path string) (string, error) {
	out, err := exec.Command("kubectl", "get", kind, name, "-n", ns,
		"-o", "jsonpath="+path).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// waitForComponentJob polls until a Job exists for (prName, componentName)
// via the operator's standard labels, returning its name. Fails the test if
// no Job shows up within timeout — used to prove admission actually built
// the Job from the frozen snapshot before a mid-run mutation is applied.
func waitForComponentJob(t *testing.T, ns, prName, componentName string, timeout time.Duration) string {
	t.Helper()
	sel := "datuplet.io/pipelinerun=" + prName + ",datuplet.io/component=" + componentName
	deadline := time.Now().Add(timeout)
	for {
		out, err := exec.Command("kubectl", "get", "jobs", "-n", ns, "-l", sel,
			"-o", "jsonpath={.items[0].metadata.name}").Output()
		name := strings.TrimSpace(string(out))
		if err == nil && name != "" {
			return name
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a Job matching %q in namespace %s", sel, ns)
		}
		time.Sleep(2 * time.Second)
	}
}

// jobComponentImage returns the "component" container's image for jobName.
func jobComponentImage(t *testing.T, ns, jobName string) string {
	t.Helper()
	img, err := kubectlJSONPath("job", jobName, ns,
		`{.spec.template.spec.containers[?(@.name=="component")].image}`)
	if err != nil {
		t.Fatalf("kubectl get job %s/%s image: %v", ns, jobName, err)
	}
	return img
}

// registrySkipUnlessReady is the shared skip ladder for every test in this
// file: needs E2E_K8S=1, a live harness (bootstrap incl. component
// registration — see framework/components_bootstrap.go), and kubectl access.
func registrySkipUnlessReady(t *testing.T) *framework.FGAHarness {
	t.Helper()
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil — E2E_K8S=1 + bootstrap (incl. component registration) must have run in TestMain")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	return h
}

// renderRegistryFixture renders a pipelines/k8s/<name>.yaml template with
// just RunPrefix (none of these fixtures need TestDataDir).
func renderRegistryFixture(t *testing.T, name string) string {
	t.Helper()
	pipelinesDir, err := filepath.Abs("pipelines")
	if err != nil {
		t.Fatalf("abs pipelines dir: %v", err)
	}
	rendered, err := framework.RenderPipeline(pipelinesDir+"/k8s/"+name,
		framework.TemplateVars{RunPrefix: runPrefix})
	if err != nil {
		t.Fatalf("render %s: %v", name, err)
	}
	t.Cleanup(func() { os.Remove(rendered) })
	return rendered
}

// ──────────────────────────────────────────────────────────────────────────
// Unknown component → FailedUser, zero pods, zero Jobs.
// ──────────────────────────────────────────────────────────────────────────

func TestRegistry_UnknownComponent(t *testing.T) {
	h := registrySkipUnlessReady(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())

	rendered := renderRegistryFixture(t, "unknown-component.yaml")
	prName := runPrefix + "-unknown-component-run"

	result, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3"})
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
	if result.Success || result.FailureType != "FailedUser" {
		t.Fatalf("got success=%v failureType=%q message=%q, want FailedUser",
			result.Success, result.FailureType, result.StatusMessage)
	}
	if !strings.Contains(result.StatusMessage, "does-not-exist") {
		t.Errorf("status message does not name the unknown component %q: %q", "does-not-exist", result.StatusMessage)
	}
	assertNoPodsForRun(t, kb.Namespace, prName)
	assertNoJobsForRun(t, kb.Namespace, prName)
}

// ──────────────────────────────────────────────────────────────────────────
// Config violates the resolved (stable) schema → FailedUser, zero pods.
//
// Deviation from the RFC's test-impact wording (flagged per the task
// brief): the "dev" registration every built-in gets is deliberately
// PERMISSIVE ({"type":"object"}) so local iteration never blocks on schema
// drift — it cannot reject anything. This scenario therefore pins
// data-generator to the STABLE v0.0.1 registration (the only one carrying
// the real, additionalProperties:false schema) to exercise the rejection
// path at all.
// ──────────────────────────────────────────────────────────────────────────

func TestRegistry_ErrorSchemaInvalid(t *testing.T) {
	h := registrySkipUnlessReady(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())

	rendered := renderRegistryFixture(t, "error-schema-invalid.yaml")
	prName := runPrefix + "-error-schema-invalid-run"

	result, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3"})
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
	if result.Success || result.FailureType != "FailedUser" {
		t.Fatalf("got success=%v failureType=%q message=%q, want FailedUser (schema violation)",
			result.Success, result.FailureType, result.StatusMessage)
	}
	if result.StatusMessage == "" {
		t.Errorf("expected a non-empty status message describing the schema violation")
	}
	assertNoPodsForRun(t, kb.Namespace, prName)
	assertNoJobsForRun(t, kb.Namespace, prName)
}

// ──────────────────────────────────────────────────────────────────────────
// Unpinned data-generator resolves the only stable version (v0.0.1) and the
// resolution is recorded verbatim in status.components.
// ──────────────────────────────────────────────────────────────────────────

func TestRegistry_UnpinnedResolvesStable(t *testing.T) {
	h := registrySkipUnlessReady(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())

	rendered := renderRegistryFixture(t, "unpinned-resolution.yaml")
	prName := runPrefix + "-unpinned-resolution-run"

	result, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3"})
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got failureType=%q message=%q logs=%s",
			result.FailureType, result.StatusMessage, result.Logs)
	}

	component, err := kubectlJSONPath("pipelinerun", prName, kb.Namespace, "{.status.components[0].component}")
	if err != nil {
		t.Fatalf("read status.components[0].component: %v", err)
	}
	if component != "data-generator" {
		t.Errorf("status.components[0].component = %q, want %q", component, "data-generator")
	}

	version, err := kubectlJSONPath("pipelinerun", prName, kb.Namespace, "{.status.components[0].version}")
	if err != nil {
		t.Fatalf("read status.components[0].version: %v", err)
	}
	if version != framework.DataGeneratorStableVersion {
		t.Errorf("status.components[0].version = %q, want %q (unpinned resolution must skip the dev prerelease)",
			version, framework.DataGeneratorStableVersion)
	}

	image, err := kubectlJSONPath("pipelinerun", prName, kb.Namespace, "{.status.components[0].image}")
	if err != nil {
		t.Fatalf("read status.components[0].image: %v", err)
	}
	if image != framework.DataGeneratorStableImage {
		t.Errorf("status.components[0].image = %q, want %q", image, framework.DataGeneratorStableImage)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Freeze: a long-running generator is admitted, then a mid-run kubectl
// patch to BOTH the live Pipeline's config and the registry's "dev" image
// must have zero effect on the already-admitted run — it completes from
// the admission-time snapshot (RFC 026 §4.3 / Task R6's resolve-&-freeze).
// ──────────────────────────────────────────────────────────────────────────

func TestRegistry_Freeze_MidRunMutation(t *testing.T) {
	h := registrySkipUnlessReady(t)

	// This test mutates the SHARED "dev" ComponentDefinition every other
	// scenario in this process resolves. Restore the registered baseline
	// unconditionally so later tests (regardless of file/declaration order —
	// Go runs this package's tests sequentially, no t.Parallel here) don't
	// see the poisoned image.
	t.Cleanup(func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := framework.RegisterBuiltinComponents(restoreCtx); err != nil {
			t.Logf("WARN: failed to restore builtin ComponentDefinitions after freeze test: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())
	ns := kb.Namespace

	rendered := renderRegistryFixture(t, "freeze-longrunning.yaml")
	prName := runPrefix + "-freeze-longrunning-run"
	pipelineName := runPrefix + "-freeze-longrunning"

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

	// Wait for the component Job to exist: proof that admission completed
	// and startStage already built the Job from the frozen snapshot, before
	// any mutation is applied.
	jobName := waitForComponentJob(t, ns, prName, "slow-generator", 90*time.Second)
	preImage := jobComponentImage(t, ns, jobName)
	if preImage == "" {
		t.Fatalf("Job %s/%s has no component container image", ns, jobName)
	}
	preGeneration, err := kubectlJSONPath("pipelinerun", prName, ns, "{.status.pipelineGeneration}")
	if err != nil {
		t.Fatalf("read pre-patch status.pipelineGeneration: %v", err)
	}
	preRowsCount, err := kubectlJSONPath("pipelinerun", prName, ns,
		"{.status.resolvedSpec.stages[0].components[0].config.tables[0].random.limit.rowsCount}")
	if err != nil {
		t.Fatalf("read pre-patch resolvedSpec rowsCount: %v", err)
	}
	if preRowsCount == "" {
		t.Fatalf("status.resolvedSpec rowsCount empty before mid-run mutation")
	}

	// Mid-run mutation 1: patch the LIVE Pipeline's config. handleRunning
	// (pkg/k8s/controllers/pipelinerun_controller.go) never re-reads the
	// live Pipeline once admitted — this must have zero effect.
	if out, patchErr := exec.CommandContext(ctx, "kubectl", "patch", "pipeline", pipelineName,
		"-n", ns, "--type=json", "-p",
		`[{"op":"replace","path":"/spec/stages/0/components/0/config/tables/0/random/limit/rowsCount","value":1}]`,
	).CombinedOutput(); patchErr != nil {
		t.Fatalf("kubectl patch pipeline config: %v\n%s", patchErr, out)
	}
	liveGenAfterPatch, _ := kubectlJSONPath("pipeline", pipelineName, ns, "{.metadata.generation}")
	t.Logf("live Pipeline generation after config patch: %s (frozen admission generation: %s)", liveGenAfterPatch, preGeneration)

	// Mid-run mutation 2: patch the registry's "dev" image (index 0 —
	// RegisterBuiltinComponents always registers "dev" first). The resolved
	// component/image were frozen into status.components at admission; a
	// live registry change must never leak into an in-flight run.
	const poisonedImage = "datuplet/data-generator:frozen-proof-should-never-be-pulled"
	if out, patchErr := exec.CommandContext(ctx, "kubectl", "patch", "componentdefinition", "data-generator",
		"--type=json", "-p",
		`[{"op":"replace","path":"/spec/versions/0/image","value":"`+poisonedImage+`"}]`,
	).CombinedOutput(); patchErr != nil {
		t.Fatalf("kubectl patch componentdefinition data-generator: %v\n%s", patchErr, out)
	}

	outcome := <-resultCh
	if outcome.err != nil {
		t.Fatalf("run pipeline: %v", outcome.err)
	}
	result := outcome.result
	if !result.Success {
		t.Fatalf("expected the run to complete from the frozen snapshot despite the mid-run mutation, got failureType=%q message=%q logs=%s",
			result.FailureType, result.StatusMessage, result.Logs)
	}

	postImage := jobComponentImage(t, ns, jobName)
	if postImage != preImage {
		t.Errorf("Job component image changed after mid-run registry patch: pre=%q post=%q (freeze broken)", preImage, postImage)
	}
	if strings.Contains(postImage, "frozen-proof-should-never-be-pulled") {
		t.Errorf("Job component image was mutated to the patched registry image %q — freeze broken", poisonedImage)
	}

	postGeneration, err := kubectlJSONPath("pipelinerun", prName, ns, "{.status.pipelineGeneration}")
	if err != nil {
		t.Fatalf("read post-patch status.pipelineGeneration: %v", err)
	}
	if postGeneration != preGeneration {
		t.Errorf("status.pipelineGeneration changed after mid-run patch: pre=%q post=%q (freeze broken)", preGeneration, postGeneration)
	}

	postRowsCount, err := kubectlJSONPath("pipelinerun", prName, ns,
		"{.status.resolvedSpec.stages[0].components[0].config.tables[0].random.limit.rowsCount}")
	if err != nil {
		t.Fatalf("read post-patch resolvedSpec rowsCount: %v", err)
	}
	if postRowsCount != preRowsCount {
		t.Errorf("status.resolvedSpec rowsCount changed after mid-run Pipeline config patch: pre=%q post=%q (want unchanged — resolvedSpec must not track the live Pipeline)",
			preRowsCount, postRowsCount)
	}
}
