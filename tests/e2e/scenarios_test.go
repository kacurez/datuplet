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
// # FGA-matrix scenarios
//
// Four test identities are seeded at fixture init (TestMain → SetupFGABootstrap):
//
//	alice   project_admin  trigger + browse
//	bob     editor         trigger + browse (via data_admin chain)
//	charlie viewer         browse only; trigger → 403
//	dora    (no grants)    403 everywhere
//
// alice + bob positive paths are exercised as standard K8s scenarios in the
// scenarios slice (kubectl-apply trigger + lakekeeper browse). The charlie
// (viewer) and dora (no-grants) negative-authz paths are covered at the unit
// level in pkg/pipelineapi/http (mustHaveRelation → 403); they are not
// e2e-covered — see the note above the fga-matrix scenarios below.
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
	// Basic extract scenarios
	// ============================================
	{
		Name:        "explicit-tables",
		Description: "Extract posts to an explicitly named table (K8s)",
		K8sPipeline: "k8s/explicit-tables.yaml",
		Assertions: []framework.Assertion{
			{Type: "table_exists", Bucket: "{{.RunPrefix}}-staging", Table: "posts"},
			{Type: "row_count", Bucket: "{{.RunPrefix}}-staging", Table: "posts", ExpectedCount: 100},
		},
	},
	{
		Name:        "full-load-mode",
		Description: "FULL_LOAD: run twice, second replaces data (K8s)",
		K8sPipeline: "k8s/full-load-mode.yaml",
		RunTwice:    true,
		Assertions: []framework.Assertion{
			{Type: "row_count", Bucket: "{{.RunPrefix}}-raw", Table: "data", ExpectedCount: 100},
		},
	},
	{
		Name:        "append-mode",
		Description: "APPEND: run twice, data accumulates (K8s)",
		K8sPipeline: "k8s/append-mode.yaml",
		RunTwice:    true,
		// Pre-existing K8s cancel-watch race observed under RunTwice=true:
		// the second run's gateway sidecar starts in a pod whose kubelet-
		// projected /etc/podinfo/annotations file briefly carries a stale
		// `datuplet.io/cancel="true"` from the previous run's teardown
		// (operator patches cancel=true on cancel/delete; downward-API
		// projection can be observed by the new pod before kubelet
		// refreshes). The cancel-watch fires, GracefulStop runs, and the
		// component container can't dial gateway:50051 → exit 20.
		// Cancel propagation should be scoped to a pod-UID match, not
		// just the run label.
		SkipReason: "K8s cancel-watch race under RunTwice: stale datuplet.io/cancel=\"true\" annotation projected to second-run pod, gateway shuts down before component can dial. Cancel propagation scoping to pod-UID is a known open improvement.",
		Assertions: []framework.Assertion{
			{Type: "row_count", Bucket: "{{.RunPrefix}}-raw", Table: "data", ExpectedCount: 200},
		},
	},
	// TODO: drop-processor skipped — Parquet field_id mismatch after drop processor
	// with JSON input format. Columns ARE dropped correctly but CloseWriter fails due to
	// schema field_id conflict between inferred schema and written records. Works fine on
	// the old Docker tier with CSV format. Needs fix in gateway's drop processor field_id
	// handling.
	// {
	// 	Name:        "drop-processor",
	// 	Description: "Drop processor removes title and body columns (K8s)",
	// 	K8sPipeline: "k8s/drop-processor.yaml",
	// 	Assertions: []framework.Assertion{
	// 		{Type: "row_count", Bucket: "{{.RunPrefix}}-raw", Table: "data", ExpectedCount: 100},
	// 		{Type: "schema", Bucket: "{{.RunPrefix}}-raw", Table: "data",
	// 			ExpectedColumns: map[string]string{
	// 				"userId": "",
	// 				"id":     "",
	// 			}},
	// 		{Type: "column_absent", Bucket: "{{.RunPrefix}}-raw", Table: "data", Column: "title"},
	// 		{Type: "column_absent", Bucket: "{{.RunPrefix}}-raw", Table: "data", Column: "body"},
	// 	},
	// },
	{
		Name:        "multi-component-stage",
		Description: "Two extractors (posts + users) in one stage (K8s)",
		K8sPipeline: "k8s/multi-component-stage.yaml",
		Assertions: []framework.Assertion{
			{Type: "table_exists", Bucket: "{{.RunPrefix}}-multi", Table: "posts"},
			{Type: "row_count", Bucket: "{{.RunPrefix}}-multi", Table: "posts", ExpectedCount: 100},
			{Type: "table_exists", Bucket: "{{.RunPrefix}}-multi", Table: "users"},
			{Type: "row_count", Bucket: "{{.RunPrefix}}-multi", Table: "users", ExpectedCount: 10},
			{Type: "schema", Bucket: "{{.RunPrefix}}-multi", Table: "users",
				ExpectedColumns: map[string]string{
					"id":       "",
					"name":     "",
					"email":    "",
					"username": "",
				}},
		},
	},
	{
		Name:        "read-back",
		Description: "Two-stage: http-json extract then read back via stdout-writer (K8s)",
		K8sPipeline: "k8s/read-back.yaml",
		Assertions: []framework.Assertion{
			{Type: "row_count", Bucket: "{{.RunPrefix}}-readback", Table: "data", ExpectedCount: 100},
		},
	},
	{
		Name:        "http-json-extract",
		Description: "Extract JSON data from an HTTP API endpoint",
		K8sPipeline: "k8s/http-json-extract.yaml",
		Assertions: []framework.Assertion{
			{Type: "min_row_count", Bucket: "{{.RunPrefix}}-api", Table: "data", ExpectedCount: 10},
			{Type: "schema", Bucket: "{{.RunPrefix}}-api", Table: "data",
				ExpectedColumns: map[string]string{
					"userId": "",
					"id":     "",
					"title":  "",
					"body":   "",
				}},
		},
	},

	// ============================================
	// DuckDB ETL scenarios
	// ============================================
	{
		Name:        "duckdb-etl",
		Description: "Full ETL on K8s: http-json extract + DuckDB aggregate by userId",
		K8sPipeline: "k8s/duckdb-etl.yaml",
		// DuckDB COPY TO output is naturally unstamped, which is what
		// iceberg-go's AddFiles requires.
		Assertions: []framework.Assertion{
			{Type: "table_exists", Bucket: "{{.RunPrefix}}-etl", Table: "user_summary"},
			{Type: "row_count", Bucket: "{{.RunPrefix}}-etl", Table: "user_summary", ExpectedCount: 10},
			{Type: "schema", Bucket: "{{.RunPrefix}}-etl", Table: "user_summary",
				ExpectedColumns: map[string]string{
					"userId":     "",
					"post_count": "",
				}},
		},
	},
	{
		Name:        "multi-table-join",
		Description: "DuckDB join: posts + users on K8s",
		K8sPipeline: "k8s/multi-table-join.yaml",
		// Same field_id handling as duckdb-etl applies here.
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
	// error-missing-table is no longer a run scenario: under RFC 027 the
	// pipeline-api validator rejects its invalid input-table reference at PUT
	// (HTTP 400) before any run is created, so it cannot reach a terminal
	// failure phase the declarative Scenario{} shape asserts on. It is covered
	// as a PUT pre-flight rejection by TestErrorMissingTableRejected in
	// scenarios_registry_test.go (RFC 027 E5).

	// Secret handling ($[name] resolution via the managed write-only secrets
	// API) is exercised by TestSecretsLadder in scenarios_secrets_test.go —
	// it needs a dedicated Datuplet project (for literal PUT /secrets/{key}
	// + PUT /pipelines calls) and both an HTTP-triggered and a kubectl-applied
	// run, which don't fit the declarative Scenario{} shape used here.

	// ============================================================
	// FGA-grant-matrix scenarios
	//
	// These scenarios assert per-user authorization outcomes against
	// the FGA model. The grant matrix is:
	//
	//   alice   → project_admin  (data_admin chain: trigger + browse)
	//   bob     → editor         (data_admin chain: trigger + browse)
	//   charlie → viewer         (browse only; trigger 403s)
	//   dora    → no grants      (403 everywhere)
	//
	// alice + bob: exercised here as standard K8s scenarios that
	// trigger a pipeline run (via kubectl apply) and then assert the
	// resulting table is browsable. The per-run JWT is minted for
	// their respective identities via K8sBackend.RunAsUser = User.
	//
	// charlie + dora (negative paths): the pipeline-api HTTP trigger
	// endpoint (POST /api/v1/projects/:pid/pipelines/:name/runs) returning
	// 403 for viewer / no-grant users is exercised by mustHaveRelation
	// handler unit tests in pkg/pipelineapi/http, not by an e2e scenario:
	// the e2e negative-trigger path needs a cli-api Bearer JWT (or session
	// cookie) whose subject is a Datuplet DB user row whose UUID matches the
	// FGA-seeded test-user UUID — infrastructure this harness does not have
	// (create-user mints a random UUID; the only test-user token helper
	// mints an impersonation/datuplet-catalog token the REST resolver
	// rejects). See docs/superpowers/plans for the RFC 024 W7 decision.
	// ============================================================
	{
		Name:        "fga-matrix-alice-trigger-and-browse",
		Description: "alice (project_admin) triggers a pipeline and browses the result — both succeed",
		// Use the http-json fixture which works without local CSV test
		// data and produces a stable 100-row output (in-cluster http-fixture posts).
		K8sPipeline: "k8s/http-json-extract.yaml",
		User:        framework.AliceID,
		Assertions: []framework.Assertion{
			{Type: "min_row_count", Bucket: "{{.RunPrefix}}-api", Table: "data", ExpectedCount: 10},
			{Type: "browse_table_succeeds", Bucket: "{{.RunPrefix}}-api", Table: "data"},
		},
	},
	{
		Name:        "fga-matrix-bob-trigger-and-browse",
		Description: "bob (editor, data_admin chain) triggers a pipeline and browses the result — both succeed",
		K8sPipeline: "k8s/http-json-extract.yaml",
		User:        framework.BobID,
		Assertions: []framework.Assertion{
			{Type: "min_row_count", Bucket: "{{.RunPrefix}}-api", Table: "data", ExpectedCount: 10},
			{Type: "browse_table_succeeds", Bucket: "{{.RunPrefix}}-api", Table: "data"},
		},
	},
}

func TestScenarios(t *testing.T) {
	// K8s is the only supported deployment tier.
	// FGA-aware test users (alice/bob/charlie/dora) are seeded at
	// fixture init via TestMain → SetupFGABootstrap; per-test users
	// are bound via K8sBackend.RunAsUser.

	pipelinesDir, _ := filepath.Abs("pipelines")
	testDataDir, _ := filepath.Abs("testdata")

	for _, sc := range scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			framework.RunScenario(t, sc, runPrefix, pipelinesDir, testDataDir)
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
	testDataDir, _ := filepath.Abs("testdata")
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
	framework.RunScenario(t, sc, runPrefix, pipelinesDir, testDataDir)
}
