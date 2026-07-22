// Package e2e — RFC 027 E3: agent-loop e2e scenario.
//
// This drives the REAL `datuplet` CLI binary through the full agent loop an
// LLM (or a scripting user) would run headlessly against a live cluster:
//
//	1. discover a component's config schema   (components get --schema)
//	2. write a doc that is WRONG              (omits a required config field)
//	3. validate → learn the error            (pipeline validate → exit 1 + finding)
//	4. fix the doc                           (add the required config)
//	5. validate → clean                      (pipeline validate → exit 0)
//	6. persist it                            (pipeline put)
//	7. run it and block for the result       (trigger --wait --json → Succeeded)
//	8. verify the output table row shape      (duckdb over the run's warehouse)
//
// It is the doc-API path exercised end to end THROUGH the binary, not the HTTP
// client — so it also covers the CLI's headless auth precedence (§7):
// $DATUPLET_API_TOKEN + $DATUPLET_REMOTE + $DATUPLET_PROJECT, no ~/.datuplet
// state. The bearer token is a cli-api JWT the harness signs for the seeded
// admin (framework.MintAdminCLIToken); the project is the Datuplet
// projects-store UUID (framework.ResolveDatupletProjectID) bound to the
// harness's lakekeeper project — the same project + warehouse the fixtures
// write to.
//
// Gating (matches every other K8s scenario): requires E2E_K8S=1 and a live
// harness (SetupFGABootstrap + RegisterBuiltinComponents ran in TestMain). A
// cluster-less `go test ./tests/e2e/...` t.Skips here and stays green.
//
// # E5 VERIFICATION ASSUMPTIONS (no cluster available at authoring time)
//
// This test COMPILES + vets + skips cluster-lessly here; its behaviour is
// proven on the live OrbStack cluster at the E5 gate. E5 must confirm:
//   - The `datuplet` root binary builds from ./cmd/datuplet (it links iceberg-go
//     via the datupleticeio blank-import; build, not run, is what this test needs).
//   - `datuplet components get data-generator --schema` resolves the STABLE
//     v0.0.1 registration (real schema with required `tables`) — RegisterBuiltinComponents
//     registers exactly that, and resolveVersion with no --version picks the
//     highest stable semver. If the registry's default changes, the schema-key
//     assertion below (properties.tables) still holds for any data-generator schema.
//   - `pipeline validate` on the tables-omitting doc pinned to v0.0.1 emits an
//     error-severity finding whose path/message names the missing config.tables
//     (the stable schema's `required:["tables"]`). We match leniently (path under
//     …config, "tables" in path OR message) so the exact JSON-Schema error phrasing
//     is not load-bearing.
//   - `trigger --wait --json` reaches phase Succeeded for the corrected doc, and
//     the data-generator v0.0.1 image writes the `daily_summary` literal table to
//     the resolved project's warehouse, readable via the lakekeeper Resolver.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// cliResult captures one `datuplet` invocation's outcome.
type cliResult struct {
	stdout   string
	stderr   string
	exitCode int
}

// runCLI runs the built datuplet binary with the given args and env, capturing
// stdout and stderr separately (trigger --json / validate --json write machine
// output to stdout; human logs go to stderr). A non-zero exit is NOT a fatal
// error here — the agent loop deliberately asserts specific exit codes.
func runCLI(ctx context.Context, t *testing.T, bin string, env []string, args ...string) cliResult {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	res := cliResult{stdout: stdout.String(), stderr: stderr.String()}
	if cmd.ProcessState != nil {
		res.exitCode = cmd.ProcessState.ExitCode()
	} else if err != nil {
		// Process never started (e.g. ctx cancelled before exec) — surface it.
		t.Fatalf("datuplet %s: failed to run: %v", strings.Join(args, " "), err)
	}
	t.Logf("datuplet %s → exit %d\n--stdout--\n%s\n--stderr--\n%s",
		strings.Join(args, " "), res.exitCode, res.stdout, res.stderr)
	return res
}

// repoRoot resolves the repository root robustly from this test file's location
// (tests/e2e/scenarios_agent_loop_test.go → up two levels), so the CLI build
// works regardless of the test's working directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed — cannot locate repo root")
	}
	root, err := filepath.Abs(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

func TestAgentLoop_SchemaToVerify(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil — E2E_K8S=1 + bootstrap (incl. component registration) must have run in TestMain")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	// Step 0: build the datuplet CLI once (from the ROOT module).
	root := repoRoot(t)
	bin := filepath.Join(t.TempDir(), "datuplet")
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/datuplet")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build datuplet CLI: %v\n%s", err, string(out))
	}

	// Step 0b: resolve the Datuplet project UUID + mint the CLI bearer token,
	// then assemble the headless env the CLI reads (§7): no ~/.datuplet needed.
	pid, err := framework.ResolveDatupletProjectID(ctx, h)
	if err != nil {
		t.Fatalf("resolve Datuplet project id: %v", err)
	}
	token, err := framework.MintAdminCLIToken(ctx, h, time.Hour)
	if err != nil {
		t.Fatalf("mint admin cli-api token: %v", err)
	}
	env := append(os.Environ(),
		"DATUPLET_REMOTE="+framework.PipelineAPIBaseURL(),
		"DATUPLET_API_TOKEN="+token,
		"DATUPLET_PROJECT="+pid,
	)

	// ── Step 1: discover the data-generator config schema ──────────────────
	// `components get --schema` prints the resolved version's configSchema
	// verbatim; assert it parses as a JSON Schema exposing a `tables` property.
	sr := runCLI(ctx, t, bin, env, "components", "get", "data-generator", "--schema")
	if sr.exitCode != 0 {
		t.Fatalf("components get --schema: want exit 0, got %d", sr.exitCode)
	}
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal([]byte(sr.stdout), &schema); err != nil {
		t.Fatalf("components get --schema: stdout is not JSON: %v\nstdout: %s", err, sr.stdout)
	}
	if _, ok := schema.Properties["tables"]; !ok {
		t.Fatalf("data-generator schema has no `properties.tables`; properties: %v", schema.Properties)
	}

	pipelineName := runPrefix + "-agent-loop"
	bucket := runPrefix + "-agent"
	t.Cleanup(func() {
		// Best-effort teardown so re-runs under the same project don't collide.
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		runCLI(cctx, t, bin, env, "pipeline", "delete", pipelineName, "-y")
	})

	dir := t.TempDir()

	// ── Step 2: write + validate a WRONG doc (omits required config.tables) ─
	// Pinned to v0.0.1 (the STABLE, schema-enforcing registration) — the "dev"
	// version's schema is permissive and would NOT reject the omission.
	badYAML := "name: " + pipelineName + "\n" +
		"stages:\n" +
		"  - name: generate\n" +
		"    components:\n" +
		"      - name: gen\n" +
		"        component: data-generator\n" +
		"        version: " + framework.DataGeneratorStableVersion + "\n" +
		"        config: {}\n" +
		"        outputs:\n" +
		"          defaultBucket: " + bucket + "\n" +
		"          defaultWriteMode: FULL_LOAD\n"
	badPath := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(badPath, []byte(badYAML), 0o600); err != nil {
		t.Fatalf("write bad.yaml: %v", err)
	}

	vr := runCLI(ctx, t, bin, env, "pipeline", "validate", "-f", badPath, "--json")
	if vr.exitCode != 1 {
		t.Fatalf("validate bad.yaml: want exit 1 (error-severity finding), got %d", vr.exitCode)
	}
	var badResp struct {
		Findings []struct {
			Path     string `json:"path"`
			Message  string `json:"message"`
			Severity string `json:"severity"`
		} `json:"findings"`
	}
	if err := json.Unmarshal([]byte(vr.stdout), &badResp); err != nil {
		t.Fatalf("validate --json: stdout is not JSON: %v\nstdout: %s", err, vr.stdout)
	}
	foundTablesFinding := false
	for _, f := range badResp.Findings {
		if f.Severity != "error" {
			continue
		}
		// The missing-required error anchors at (or under) the component's
		// config; the phrasing of the JSON-Schema message is not load-bearing,
		// so match leniently: config in the path AND "tables" in path or message.
		if strings.Contains(f.Path, "config") &&
			(strings.Contains(f.Path, "tables") || strings.Contains(f.Message, "tables")) {
			foundTablesFinding = true
			break
		}
	}
	if !foundTablesFinding {
		t.Fatalf("validate bad.yaml: no error-severity finding naming config.tables; findings: %+v", badResp.Findings)
	}

	// ── Step 3: write + validate the CORRECTED doc ─────────────────────────
	// Adds the required config.tables (a literal daily_summary table — a
	// deterministic 3-row output for the duckdb assertions in step 5). This is
	// a self-contained data-generator pipeline, the "fix" for the bad doc above.
	goodYAML := "name: " + pipelineName + "\n" +
		"stages:\n" +
		"  - name: generate\n" +
		"    components:\n" +
		"      - name: gen\n" +
		"        component: data-generator\n" +
		"        version: " + framework.DataGeneratorStableVersion + "\n" +
		"        config:\n" +
		"          tables:\n" +
		"            - name: daily_summary\n" +
		"              literal:\n" +
		"                columns: [day, total]\n" +
		"                rows:\n" +
		"                  - [\"2026-07-01\", 10]\n" +
		"                  - [\"2026-07-02\", 20]\n" +
		"                  - [\"2026-07-03\", 30]\n" +
		"        outputs:\n" +
		"          defaultBucket: " + bucket + "\n" +
		"          defaultWriteMode: FULL_LOAD\n"
	goodPath := filepath.Join(dir, "good.yaml")
	if err := os.WriteFile(goodPath, []byte(goodYAML), 0o600); err != nil {
		t.Fatalf("write good.yaml: %v", err)
	}

	gvr := runCLI(ctx, t, bin, env, "pipeline", "validate", "-f", goodPath, "--json")
	if gvr.exitCode != 0 {
		t.Fatalf("validate good.yaml: want exit 0 (no error finding), got %d\nstdout: %s", gvr.exitCode, gvr.stdout)
	}

	// ── Step 4: persist the corrected doc ──────────────────────────────────
	pr := runCLI(ctx, t, bin, env, "pipeline", "put", "-f", goodPath)
	if pr.exitCode != 0 {
		t.Fatalf("pipeline put: want exit 0, got %d", pr.exitCode)
	}

	// ── Step 5: trigger and block for the terminal phase ───────────────────
	// Flags precede the positional (Go's flag package stops at the first
	// non-flag). --timeout bounds --wait well inside the test's ctx.
	tr := runCLI(ctx, t, bin, env, "trigger", "--wait", "--json", "--timeout", "8m", pipelineName)
	if tr.exitCode != 0 {
		t.Fatalf("trigger --wait: want exit 0 (Succeeded), got %d\nstdout: %s\nstderr: %s", tr.exitCode, tr.stdout, tr.stderr)
	}
	var runOut struct {
		ID    string `json:"id"`
		Phase string `json:"phase"`
	}
	if err := json.Unmarshal([]byte(tr.stdout), &runOut); err != nil {
		t.Fatalf("trigger --json: stdout is not JSON: %v\nstdout: %s", err, tr.stdout)
	}
	if runOut.Phase != "Succeeded" {
		t.Fatalf("trigger: run %s ended in phase %q, want Succeeded", runOut.ID, runOut.Phase)
	}

	// ── Step 6: verify the output table row shape via duckdb ───────────────
	target, err := framework.K8sQueryTarget(t, ctx, h)
	if err != nil {
		t.Fatalf("build K8s query target: %v", err)
	}
	if target.IsEmpty() {
		t.Skip("MinIO unreachable — skipping duckdb output verification (data-plane run already Succeeded)")
	}
	framework.AssertSchema(t, target, bucket, "daily_summary", map[string]string{
		"day":   "", // presence-only: literal type inference is not load-bearing here
		"total": "",
	})
	framework.AssertRowCount(t, target, bucket, "daily_summary", 3)
}
