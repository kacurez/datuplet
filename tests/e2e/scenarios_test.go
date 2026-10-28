// Package e2e — end-to-end scenario suite for Datuplet.
//
// # Live-run requirements
//
// The harness compiles and the scenarios are written for the current
// production shape. A live run requires an OrbStack context with `make local-up`
// or `make k8s-up` having run `pipeline-api admin authz-bootstrap`:
//
//	# OrbStack only — AKS / other contexts are not supported.
//	make deploy-local
//	E2E_K8S=1 go test -v -count=1 -timeout=20m ./tests/e2e/...
//
// K8s is the only supported deployment tier.
//
// # FGA-matrix scenario
//
// Two test identities are seeded at fixture init (TestMain → SetupFGABootstrap):
//
//	alice   project_admin  trigger + browse (default identity of every scenario)
//	bob     editor         trigger + browse (via data_admin chain)
//
// bob's positive path is exercised by the fga-matrix-bob scenario below. The
// viewer and no-grant negative-authz paths are covered at the unit level in
// pkg/pipelineapi/http (mustHaveRelation → 403); see the note above the
// fga-matrix scenario below.
package e2e

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

var scenarios = []framework.Scenario{
	// ============================================
	// Write modes
	// ============================================
	{
		// One pipeline, two extractors in one stage: FULL_LOAD replaces (still
		// 100 rows after two runs) while APPEND accumulates (200 rows).
		// Fallback: if run 2 exits 20 after a stale `datuplet.io/cancel="true"`
		// projection (the cancel-watch race that kept the old APPEND scenario
		// skipped), re-skip the APPEND half; that race is still open.
		Name:        "write-modes",
		Description: "FULL_LOAD replaces and APPEND accumulates across two runs (K8s)",
		K8sPipeline: "k8s/write-modes.yaml",
		RunTwice:    true,
		Assertions: []framework.Assertion{
			{Type: "row_count", Bucket: "{{.RunPrefix}}-full", Table: "data", ExpectedCount: 100},
			{Type: "row_count", Bucket: "{{.RunPrefix}}-append", Table: "data", ExpectedCount: 200},
		},
	},

	// ============================================
	// Multi-stage ETL
	// ============================================
	{
		Name:        "multi-table-join",
		Description: "DuckDB join: posts + users on K8s",
		K8sPipeline: "k8s/multi-table-join.yaml",
		// DuckDB COPY TO output is naturally unstamped, which is what
		// iceberg-go's AddFiles requires.
		Assertions: []framework.Assertion{
			{Type: "row_count", Bucket: "{{.RunPrefix}}-joined", Table: "post_details", ExpectedCount: 100},
			{Type: "schema", Bucket: "{{.RunPrefix}}-joined", Table: "post_details",
				ExpectedColumns: map[string]string{
					"post_id":      "",
					"title":        "",
					"author_name":  "",
					"author_email": "",
				}},
		},
	},

	// ============================================
	// Error scenarios
	// ============================================
	{
		Name:        "error-bad-config",
		Description: "Component fails with user error; assert exit_code=1 + FailedUser",
		K8sPipeline: "k8s/error-bad-config.yaml",
		ExpectError: true,
		// data-generator's userErrorMessage trigger works correctly in unit
		// tests + isolated K8s runs (~1/5 pass rate observed), but the
		// operator + harness race on the terminal-phase value when the
		// component exits while gateway sidecar is still running. Operator
		// sometimes captures the component's exit-1 (FailedUser) and
		// sometimes overrides with a sidecar/commit-side FailedApplication.
		// Needs operator-side terminal-phase determinism work.
		SkipReason: "operator/harness terminal-phase race on user-error path; operator-side terminal-phase determinism is a known open improvement",
		Assertions: []framework.Assertion{
			{Type: "exit_code", ExpectedExitCode: 1},
			{Type: "failure_type", ExpectedFailure: "FailedUser"},
		},
	},
	// PUT-time validator rejections (an invalid pipeline is refused with HTTP
	// 400 before any run is created, so it cannot reach the terminal failure
	// phase this declarative Scenario{} shape asserts on) are covered by
	// TestRegistry_ErrorSchemaInvalid in scenarios_registry_test.go and by the
	// validate steps of TestAgentLoop_SchemaToVerify.

	// Secret handling ($[name] resolution via the managed write-only secrets
	// API) is exercised by TestSecretsLadder in scenarios_secrets_test.go —
	// it needs a dedicated Datuplet project (for literal PUT /secrets/{key}
	// + PUT /pipelines calls) and both an HTTP-triggered and a kubectl-applied
	// run, which don't fit the declarative Scenario{} shape used here.

	// ============================================================
	// FGA-grant-matrix scenario
	//
	// alice (project_admin) is the default identity of every other
	// scenario, so only bob (editor, data_admin chain) gets a dedicated
	// scenario: it triggers a pipeline run and then asserts the resulting
	// table is browsable. The run authenticates as bob via
	// K8sBackend.RunAsUser = User.
	//
	// Negative paths (viewer / no-grant): the pipeline-api HTTP trigger
	// endpoint (POST /api/v1/projects/:pid/pipelines/:name/runs) returning
	// 403 is exercised by mustHaveRelation handler unit tests in
	// pkg/pipelineapi/http, not by an e2e scenario: the e2e negative-trigger
	// path needs a cli-api Bearer JWT (or session cookie) whose subject is a
	// Datuplet DB user row whose UUID matches the FGA-seeded test-user UUID —
	// infrastructure this harness does not have (create-user mints a random
	// UUID; the only test-user token helper mints an
	// impersonation/datuplet-catalog token the REST resolver rejects). See
	// docs/superpowers/plans for the RFC 024 W7 decision.
	// ============================================================
	{
		Name:        "fga-matrix-bob-trigger-and-browse",
		Description: "bob (editor, data_admin chain) triggers a pipeline and browses the result — both succeed",
		K8sPipeline: "k8s/http-json-extract.yaml",
		User:        framework.BobID,
		Assertions: []framework.Assertion{
			{Type: "row_count", Bucket: "{{.RunPrefix}}-api", Table: "data", ExpectedCount: 100},
			{Type: "schema", Bucket: "{{.RunPrefix}}-api", Table: "data",
				ExpectedColumns: map[string]string{
					"userId": "",
					"id":     "",
					"title":  "",
					"body":   "",
				}},
			{Type: "browse_table_succeeds", Bucket: "{{.RunPrefix}}-api", Table: "data"},
		},
	},
}

func TestScenarios(t *testing.T) {
	// K8s is the only supported deployment tier.
	// FGA-aware test users (alice/bob) are seeded at fixture init via
	// TestMain → SetupFGABootstrap; per-test users are bound via
	// K8sBackend.RunAsUser.

	pipelinesDir, _ := filepath.Abs("pipelines")

	for _, sc := range scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			framework.RunScenario(t, sc, runPrefix, pipelinesDir)
		})
	}
}

// TestBigDataJoinProof exercises the Arrow IPC streaming + materialize path
// with a ~400MB facts table joined to a 10-row dimension table under a
// 256Mi ephemeral-storage cap. Inputs stream parquet → arrow → DuckDB
// arrow_scan → CREATE TABLE materialize, then JOIN runs against the
// materialized table.
//
// Skipped by default — opt in via:
//
//	RFC011_BIG_DATA_PROOF=1 E2E_K8S=1 go test -count=1 -v \
//	  -run TestBigDataJoinProof ./tests/e2e/...
//
// Wall time ~5-10 min (5M-row generation + JOIN). Don't put it in the
// regular scenarios slice — it's intentionally one-shot.
func TestBigDataJoinProof(t *testing.T) {
	if os.Getenv("RFC011_BIG_DATA_PROOF") != "1" {
		t.Skip("RFC011_BIG_DATA_PROOF=1 to run")
	}
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required (big-data join proof is K8s-only)")
	}
	pipelinesDir, _ := filepath.Abs("pipelines")
	sc := framework.Scenario{
		Name:        "large-input-join",
		Description: "Big-data join proof: ~400MB facts JOIN 10-row dimensions under 256Mi ephemeral-storage cap",
		K8sPipeline: "k8s/manual-large-input-join.yaml",
		// 15-minute budget: 5M-row generation + JOIN under a 256Mi
		// ephemeral-storage cap. Default 5-min framework outer ctx is
		// too tight for this scale.
		Timeout: 15 * time.Minute,
		Assertions: []framework.Assertion{
			{Type: "table_exists", Bucket: "{{.RunPrefix}}-out", Table: "summary"},
			{Type: "row_count", Bucket: "{{.RunPrefix}}-out", Table: "summary", ExpectedCount: 4},
			{Type: "schema", Bucket: "{{.RunPrefix}}-out", Table: "summary",
				ExpectedColumns: map[string]string{
					"category":    "",
					"row_count":   "",
					"total_value": "",
				}},
		},
	}
	framework.RunScenario(t, sc, runPrefix, pipelinesDir)
}
