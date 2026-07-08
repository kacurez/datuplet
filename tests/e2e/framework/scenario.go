package framework

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// sharedHarness is the per-process FGAHarness wired up at TestMain.
// SetSharedHarness is the single setter; RunScenario reads it lazily
// per scenario. Stays nil when E2E_K8S=1 was not set.
var (
	sharedHarnessMu sync.RWMutex
	sharedHarness   *FGAHarness
)

// SetSharedHarness installs the per-process FGAHarness. Called once
// from TestMain after SetupFGABootstrap returns. Calling it twice in
// the same process replaces the existing pointer (mostly useful for
// testing the harness itself).
func SetSharedHarness(h *FGAHarness) {
	sharedHarnessMu.Lock()
	defer sharedHarnessMu.Unlock()
	sharedHarness = h
}

// SharedHarness returns the per-process FGAHarness or nil if
// SetSharedHarness hasn't been called. Scenarios skip when it's nil
// — the dev's missing the bootstrap step.
func SharedHarness() *FGAHarness {
	sharedHarnessMu.RLock()
	defer sharedHarnessMu.RUnlock()
	return sharedHarness
}

// MinIO credentials for K8s cluster (matches utils/deploy/k8s/minio.yaml)
const (
	minioAccessKey    = "minioadmin"
	minioSecretKey    = "minioadmin"
	minioBucket       = "datuplet"
	minioNodePort     = "30900"
	minioLocalAddress = "localhost:" + minioNodePort
)

// Scenario defines an end-to-end test scenario.
type Scenario struct {
	Name           string
	Description    string
	SetupPipeline  string        // optional: runs before Pipeline to seed baseline data
	SetupDelay     time.Duration // optional: sleep between setup pipeline and main pipeline
	BackdateSetup  time.Duration // optional: backdate snapshot timestamps after setup (avoids real delay)
	BackdateBucket string        // bucket containing the table to backdate (supports {{.RunPrefix}})
	BackdateTable  string        // table whose snapshots to backdate
	K8sPipeline    string        // relative path to K8s pipeline YAML template (empty = skip)
	RunTwice       bool          // for APPEND/FULL_LOAD tests
	ExpectError    bool
	Assertions     []Assertion

	// User, when non-nil, overrides which TestUser identity the K8s backend
	// mints the per-run JWT for. Defaults to AliceID (project_admin) when
	// zero. Used by FGA-grant-matrix scenarios that need to assert
	// per-user authorization outcomes.
	User uuid.UUID

	// SkipReason, when non-empty, causes RunScenario to t.Skip the scenario
	// with the given message. Used to mark scenarios blocked on upstream-library
	// or component-redesign work that's tracked separately.
	SkipReason string

	// Timeout overrides the default per-scenario outer context (5 min) and
	// the K8s backend's per-pipeline wait deadline (3 min). When zero, the
	// existing defaults stand. Used by long-running proofs that need a
	// larger budget than the regression suite's small scenarios.
	Timeout time.Duration
}

// Assertion defines a single check to run after pipeline execution.
type Assertion struct {
	Type string // "row_count", "min_row_count", "schema", "column_absent", "query", "table_exists", "browse_table_succeeds", "exit_code", "failure_type"

	Bucket string // bucket name (may contain {{.RunPrefix}})
	Table  string

	ExpectedCount    int
	ExpectedColumns  map[string]string // for schema
	Column           string            // for column_absent
	SQL              string            // for query (use {{TABLE}} placeholder)
	ExpectedRows     []map[string]any  // for query
	ExpectedExitCode int               // for exit_code
	ExpectedFailure  string            // for failure_type
}

// RunScenario is the main test runner for an e2e scenario.
// It executes the scenario against the K8s backend, rendering pipeline
// templates, executing pipelines, and checking assertions.
func RunScenario(t *testing.T, sc Scenario, runPrefix string, pipelinesDir string, testDataDir string) {
	t.Helper()

	// Per-scenario skip. Surfaced via Scenario.SkipReason for tests blocked
	// on upstream-library or component-redesign work.
	if sc.SkipReason != "" {
		t.Skip(sc.SkipReason)
		return
	}

	h := SharedHarness()
	if h == nil {
		t.Skip("K8s e2e requires E2E_K8S=1 and SetupFGABootstrap to have run in TestMain — see framework/bootstrap.go")
		return
	}

	if err := PreCheck(); err != nil {
		t.Skipf("precheck failed: %v", err)
		return
	}

	if sc.K8sPipeline == "" {
		t.Skipf("no K8s pipeline template defined for scenario %s", sc.Name)
		return
	}

	outerTimeout := 5 * time.Minute
	if sc.Timeout > 0 {
		outerTimeout = sc.Timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), outerTimeout)
	defer cancel()

	kb, err := NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	// Override RunAsUser when the scenario declares a specific
	// test identity (FGA-matrix scenarios). Zero UUID falls
	// through to the default AliceID set by NewK8sBackend.
	if sc.User != uuid.Nil {
		kb.RunAsUser = sc.User
	}

	defer kb.Cleanup(ctx)

	// Build query target for K8s MinIO via NodePort, with a
	// lakekeeper-vended Resolver so assertions hit lakekeeper-allocated
	// UUID-keyed paths.
	queryTarget := k8sQueryTarget(t)
	resolver, err := buildK8sVerifier(ctx, h)
	if err != nil {
		t.Fatalf("build lakekeeper verifier: %v", err)
	}
	queryTarget.Resolver = resolver

	vars := TemplateVars{
		RunPrefix:   runPrefix,
		TestDataDir: testDataDir,
	}

	// Run setup pipeline if specified (seeds baseline data).
	if sc.SetupPipeline != "" {
		setupPath := pipelinesDir + "/" + sc.SetupPipeline
		setupRendered, err := RenderPipeline(setupPath, vars)
		if err != nil {
			t.Fatalf("render setup pipeline %s: %v", setupPath, err)
		}
		defer os.Remove(setupRendered)

		result, err := kb.RunPipeline(ctx, setupRendered, RunOpts{
			StorageType: "s3",
		})
		if err != nil {
			t.Fatalf("run setup pipeline: %v", err)
		}
		if !result.Success {
			t.Fatalf("setup pipeline failed (exit %d):\n  logs: %s", result.ExitCode, result.Logs)
		}

		if sc.SetupDelay > 0 {
			t.Logf("waiting %s after setup pipeline...", sc.SetupDelay)
			time.Sleep(sc.SetupDelay)
		}

		// Backdate snapshot timestamps so delta reads find an "old" baseline
		// without actually waiting. (warehouseDir is empty for K8s tier —
		// backdating is only meaningful for local filesystem mode.)
		if sc.BackdateSetup > 0 {
			t.Logf("note: BackdateSetup is set but K8s tier does not support local backdating; skipped")
		}
	}

	// Render the pipeline template.
	templatePath := pipelinesDir + "/" + sc.K8sPipeline
	rendered, err := RenderPipeline(templatePath, vars)
	if err != nil {
		t.Fatalf("render pipeline template %s: %v", templatePath, err)
	}
	defer os.Remove(rendered)

	// Run the pipeline (optionally twice for append/full-load tests).
	runs := 1
	if sc.RunTwice {
		runs = 2
	}

	var lastResult *RunResult
	for i := 0; i < runs; i++ {
		result, err := kb.RunPipeline(ctx, rendered, RunOpts{
			StorageType: "s3",
			Timeout:     sc.Timeout,
		})
		if err != nil {
			t.Fatalf("run pipeline (attempt %d/%d): %v", i+1, runs, err)
		}
		lastResult = result

		if sc.ExpectError && result.Success {
			t.Fatalf("expected pipeline to fail but it succeeded (attempt %d/%d)\n  logs: %s", i+1, runs, result.Logs)
		}
		if !sc.ExpectError && !result.Success {
			t.Fatalf("expected pipeline to succeed but it failed (attempt %d/%d, exit code %d, type %s)\n  message: %s\n  logs: %s",
				i+1, runs, result.ExitCode, result.FailureType, result.StatusMessage, result.Logs)
		}
	}

	// Run assertions.
	for _, a := range sc.Assertions {
		bucket := strings.ReplaceAll(a.Bucket, "{{.RunPrefix}}", runPrefix)

		switch a.Type {
		case "exit_code":
			if lastResult.ExitCode != a.ExpectedExitCode {
				t.Errorf("exit code mismatch: got %d, want %d", lastResult.ExitCode, a.ExpectedExitCode)
			}

		case "failure_type":
			if lastResult.FailureType != a.ExpectedFailure {
				t.Errorf("failure type mismatch: got %q, want %q", lastResult.FailureType, a.ExpectedFailure)
			}

		case "table_exists":
			if queryTarget.IsEmpty() {
				t.Logf("skipping table_exists assertion (no query target)")
				continue
			}
			AssertTableExists(t, queryTarget, bucket, a.Table)

		case "browse_table_succeeds":
			if queryTarget.IsEmpty() {
				t.Logf("skipping browse_table_succeeds assertion (no query target)")
				continue
			}
			AssertBrowseTableSucceeds(t, queryTarget, bucket, a.Table)

		case "row_count":
			if queryTarget.IsEmpty() {
				t.Logf("skipping row_count assertion (no query target)")
				continue
			}
			AssertRowCount(t, queryTarget, bucket, a.Table, a.ExpectedCount)

		case "min_row_count":
			if queryTarget.IsEmpty() {
				t.Logf("skipping min_row_count assertion (no query target)")
				continue
			}
			AssertMinRowCount(t, queryTarget, bucket, a.Table, a.ExpectedCount)

		case "schema":
			if queryTarget.IsEmpty() {
				t.Logf("skipping schema assertion (no query target)")
				continue
			}
			AssertSchema(t, queryTarget, bucket, a.Table, a.ExpectedColumns)

		case "column_absent":
			if queryTarget.IsEmpty() {
				t.Logf("skipping column_absent assertion (no query target)")
				continue
			}
			AssertColumnAbsent(t, queryTarget, bucket, a.Table, a.Column)

		case "query":
			if queryTarget.IsEmpty() {
				t.Logf("skipping query assertion (no query target)")
				continue
			}
			AssertQuery(t, queryTarget, bucket, a.Table, a.SQL, a.ExpectedRows)

		default:
			t.Errorf("unknown assertion type: %q", a.Type)
		}
	}
}

// buildK8sVerifier opens a lakekeeper REST catalog connection scoped
// to the harness's per-suite Project. Uses the alice impersonation
// token so the verifier itself has read access (alice is project_admin
// in the FGA matrix). FGA-matrix scenarios can override the token via
// NewVerifier directly.
//
// The token is minted fresh each scenario rather than cached because
// MintImpersonation has a 60-second TTL — a long-running suite will
// otherwise hit an expired token mid-assertion.
func buildK8sVerifier(ctx context.Context, h *FGAHarness) (*LakekeeperVerifier, error) {
	tp := func(ctx context.Context) (string, error) {
		tok, err := MintTestUserImpersonation(ctx, h.Signer, AliceID)
		if err != nil {
			return "", err
		}
		return tok.Reveal(), nil
	}
	// LakekeeperVerifier holds the tokenProvider closure and invokes it
	// per LocationFor call so tokens never expire on long pipeline runs.
	return NewVerifier(ctx, h, catalogwriter.TokenProvider(tp))
}

// k8sQueryTarget builds a QueryTarget for K8s MinIO.
// It tries the NodePort first, then falls back to port-forward.
func k8sQueryTarget(t *testing.T) QueryTarget {
	t.Helper()

	// Try NodePort first (localhost:30900)
	conn, err := net.DialTimeout("tcp", minioLocalAddress, 2*time.Second)
	if err == nil {
		conn.Close()
		t.Logf("K8s MinIO reachable via NodePort at %s", minioLocalAddress)
		return QueryTarget{
			S3Endpoint:  minioLocalAddress,
			S3AccessKey: minioAccessKey,
			S3SecretKey: minioSecretKey,
			S3Bucket:    minioBucket,
		}
	}

	// Fall back to port-forward
	t.Logf("NodePort %s not reachable, starting kubectl port-forward...", minioLocalAddress)
	ns := os.Getenv("DATUPLET_E2E_NAMESPACE")
	if ns == "" {
		ns = "datuplet-e2e"
	}
	cmd := exec.Command("kubectl", "port-forward", "svc/minio", "30900:9000", "-n", ns)
	if err := cmd.Start(); err != nil {
		t.Logf("WARNING: could not start port-forward: %v (K8s data assertions will be skipped)", err)
		return QueryTarget{}
	}

	// Register cleanup
	t.Cleanup(func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	})

	// Wait for port to be ready
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if conn, err := net.DialTimeout("tcp", minioLocalAddress, 1*time.Second); err == nil {
			conn.Close()
			t.Logf("K8s MinIO reachable via port-forward at %s", minioLocalAddress)
			return QueryTarget{
				S3Endpoint:  minioLocalAddress,
				S3AccessKey: minioAccessKey,
				S3SecretKey: minioSecretKey,
				S3Bucket:    minioBucket,
			}
		}
	}

	t.Logf("WARNING: port-forward to MinIO did not become ready (K8s data assertions will be skipped)")
	if cmd.Process != nil {
		cmd.Process.Kill()
	}
	return QueryTarget{}
}
