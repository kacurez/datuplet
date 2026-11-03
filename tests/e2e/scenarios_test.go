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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/apache/iceberg-go"

	gatewaypb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
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

// TestV3VariantPartitioned proves partitioned Iceberg v3 + VARIANT end to end
// on the cluster. http-json-extractor lands manifests/variant-events.json
// (nested objects and arrays, integers above 2^53) as a VARIANT column in a
// table partitioned by identity(category) and bucket[4](id); sql-transform
// carries the payload into a second table and adds a derived variant. It then
// checks both tables' metadata and payloads (duckdb CLI 2.x), a query-worker
// preview, a gateway FORMAT_JSON read, and, after an iceberg-go merge-on-read
// delete of one row, a re-run of the transform that the gateway serves with
// exactly one decoded file.
func TestV3VariantPartitioned(t *testing.T) {
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "K8s e2e requires E2E_K8S=1 and SetupFGABootstrap to have run in TestMain — see framework/bootstrap.go")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	framework.RequireDuckDBV2(t)
	// Resolved before any run: these helpers may skip, and a skip after the
	// runs would hide the delete / decoded-file assertions.
	adminSession, queryPID := getAdminSession(t), getQueryProjectID(t)
	assertSameTransformSQL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	payloads, derived := loadVariantFixture(t)
	kb, err := framework.NewK8sBackend(h, runPrefix)
	if err != nil {
		t.Fatalf("init K8s backend: %v", err)
	}
	defer kb.Cleanup(context.Background())
	target := framework.NewK8sQueryTarget(ctx, t, h)
	rawNS, outNS := runPrefix+"-v3raw", runPrefix+"-v3out"

	run := func(fixture string) *framework.RunResult {
		t.Helper()
		rendered, err := framework.RenderPipeline(filepath.Join("pipelines", "k8s", fixture), framework.TemplateVars{RunPrefix: runPrefix})
		if err != nil {
			t.Fatalf("render %s: %v", fixture, err)
		}
		defer os.Remove(rendered)
		res, err := kb.RunPipeline(ctx, rendered, framework.RunOpts{StorageType: "s3", Timeout: 5 * time.Minute})
		if err != nil {
			t.Fatalf("run %s: %v", fixture, err)
		}
		if !res.Success {
			t.Fatalf("run %s failed (exit %d, %s): %s\nlogs:\n%s", fixture, res.ExitCode, res.FailureType, res.StatusMessage, res.Logs)
		}
		return res
	}

	run("v3-variant.yaml")

	framework.AssertIcebergTable(t, target, rawNS, "events", 3, []string{"identity(category)", "bucket[4](id)"})
	framework.AssertIcebergTable(t, target, outNS, "events_out", 3, nil)
	framework.AssertIcebergColumnTypes(t, target, rawNS, "events", map[string]iceberg.Type{
		"category": iceberg.PrimitiveTypes.String,
		"id":       iceberg.PrimitiveTypes.Int64,
		"payload":  iceberg.VariantType{},
	})
	framework.AssertIcebergColumnTypes(t, target, outNS, "events_out", map[string]iceberg.Type{
		"payload": iceberg.VariantType{},
		"derived": iceberg.VariantType{},
	})
	framework.AssertVariantJSON(t, target, rawNS, "events", "payload", payloads)
	framework.AssertVariantJSON(t, target, outNS, "events_out", "payload", payloads)
	framework.AssertVariantJSON(t, target, outNS, "events_out", "derived", derived)

	assertVariantPreview(t, ctx, adminSession, queryPID, rawNS, "events", payloads)
	assertGatewayJSONRead(t, ctx, kb, rawNS, "events", payloads)

	// id 2 shares the (alpha, bucket 0) file with id 1, so the delete leaves
	// a deletion vector on that file rather than dropping a whole file.
	const deletedID = 2
	del, err := framework.DeleteRowsMergeOnRead(ctx, h, rawNS, "events", iceberg.EqualTo(iceberg.Reference("id"), int64(deletedID)))
	if err != nil {
		t.Fatalf("merge-on-read delete of id %d: %v", deletedID, err)
	}
	if del.FilesWithDV != 1 || del.FilesWithDelete != 0 || del.DataFiles < 2 {
		t.Fatalf("after the delete: %+v; want one deletion vector, no other delete files, several data files", del)
	}
	delete(payloads, deletedID)
	delete(derived, deletedID)

	rerun := run("v3-variant-rerun.yaml")
	summary := framework.FindReadSummary(rerun.Logs, rawNS, "events")
	if summary == "" {
		t.Fatalf("no gateway read summary for %s.events in the re-run logs:\n%s", rawNS, rerun.Logs)
	}
	got := framework.ParseReadSummary(summary)
	want := map[string]string{
		"raw_files":     strconv.Itoa(del.DataFiles - 1),
		"decoded_files": "1",
		"rows":          strconv.Itoa(len(payloads)),
		"outcome":       "ok",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("re-run read summary %s=%q, want %q (%s)", k, got[k], v, summary)
		}
	}
	t.Logf("re-run read: %s", summary)

	framework.AssertVariantJSON(t, target, rawNS, "events", "payload", payloads)
	framework.AssertVariantJSON(t, target, outNS, "events_out", "payload", payloads)
	framework.AssertVariantJSON(t, target, outNS, "events_out", "derived", derived)
}

// assertSameTransformSQL checks that the rendered v3-variant.yaml and
// v3-variant-rerun.yaml carry byte-equal transform-stage SQL, so the re-run
// repeats exactly the first run's transform.
func assertSameTransformSQL(t *testing.T) {
	t.Helper()
	sqlOf := func(fixture string) string {
		rendered, err := framework.RenderPipeline(filepath.Join("pipelines", "k8s", fixture), framework.TemplateVars{RunPrefix: runPrefix})
		if err != nil {
			t.Fatalf("render %s: %v", fixture, err)
		}
		defer os.Remove(rendered)
		raw, err := os.ReadFile(rendered)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := config.Parse(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", fixture, err)
		}
		for _, st := range doc.Stages {
			if st.Name == "transform" && len(st.Components) == 1 {
				if sql, ok := st.Components[0].Config["sql"].(string); ok && sql != "" {
					return sql
				}
			}
		}
		t.Fatalf("%s: no transform stage with a single component carrying config.sql", fixture)
		return ""
	}
	if a, b := sqlOf("v3-variant.yaml"), sqlOf("v3-variant-rerun.yaml"); a != b {
		t.Fatalf("transform SQL differs between v3-variant.yaml and v3-variant-rerun.yaml:\n%s\n---\n%s", a, b)
	}
}

// loadVariantFixture returns the fixture's payloads and the transform's
// expected derived values ({"id", "category"}), keyed by id, as exact JSON.
func loadVariantFixture(t *testing.T) (payloads, derived map[int64]any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("manifests", "variant-events.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		Category string          `json:"category"`
		ID       int64           `json:"id"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("variant-events.json: %v", err)
	}
	payloads, derived = map[int64]any{}, map[int64]any{}
	for _, r := range rows {
		v, err := framework.ParseJSONExact(r.Payload)
		if err != nil {
			t.Fatalf("variant-events.json id %d: %v", r.ID, err)
		}
		payloads[r.ID] = v
		derived[r.ID] = map[string]any{"id": json.Number(strconv.FormatInt(r.ID, 10)), "category": r.Category}
	}
	return payloads, derived
}

// assertVariantPreview checks that the query-worker storage preview returns
// the payload column as JSON text equal to want.
func assertVariantPreview(t *testing.T, ctx context.Context, session, pid, ns, table string, want map[int64]any) {
	t.Helper()
	status, body, err := storageGETPreview(ctx, session, pid, ns, table)
	if err != nil {
		t.Fatalf("storage preview %s.%s: %v", ns, table, err)
	}
	if status != http.StatusOK {
		t.Fatalf("storage preview %s.%s: status %d: %s", ns, table, status, truncateLog(body, 1024))
	}
	var resp struct {
		Columns []struct{ Name, Type string } `json:"columns"`
		Rows    [][]json.RawMessage           `json:"rows"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("storage preview body: %v: %s", err, truncateLog(body, 1024))
	}
	idCol, payloadCol := -1, -1
	for i, c := range resp.Columns {
		switch c.Name {
		case "id":
			idCol = i
		case "payload":
			payloadCol = i
		}
	}
	if idCol < 0 || payloadCol < 0 {
		t.Fatalf("storage preview columns %v lack id or payload", resp.Columns)
	}
	got := map[int64]any{}
	for _, row := range resp.Rows {
		id, err := strconv.ParseInt(string(row[idCol]), 10, 64)
		if err != nil {
			t.Fatalf("preview id %s: %v", row[idCol], err)
		}
		var text string
		if err := json.Unmarshal(row[payloadCol], &text); err != nil {
			t.Fatalf("preview payload for id %d is %s, want JSON text: %v", id, row[payloadCol], err)
		}
		v, err := framework.ParseJSONExact([]byte(text))
		if err != nil {
			t.Fatalf("preview payload for id %d: %q is not JSON: %v", id, text, err)
		}
		got[id] = v
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("preview payloads:\n got  %v\n want %v", got, want)
	}
}

// assertGatewayJSONRead reads ns.table as FORMAT_JSON through a live run's
// Data Gateway and checks the payloads come back embedded as JSON values
// (objects and arrays, not strings) equal to want.
func assertGatewayJSONRead(t *testing.T, ctx context.Context, kb *framework.K8sBackend, ns, table string, want map[int64]any) {
	t.Helper()
	holder, err := framework.RenderPipeline(filepath.Join("pipelines", "k8s", "v3-variant-holder.yaml"), framework.TemplateVars{RunPrefix: runPrefix})
	if err != nil {
		t.Fatalf("render holder pipeline: %v", err)
	}
	defer os.Remove(holder)
	read, err := framework.ReadThroughRunGateway(ctx, kb, holder, "holder", ns, table, gatewaypb.DataFormat_FORMAT_JSON)
	if err != nil {
		t.Fatalf("gateway FORMAT_JSON read of %s.%s: %v", ns, table, err)
	}
	got := map[int64]any{}
	for _, chunk := range read.Chunks {
		var rows []map[string]json.RawMessage
		if err := json.Unmarshal(chunk, &rows); err != nil {
			t.Fatalf("gateway JSON chunk: %v: %s", err, truncateLog(chunk, 1024))
		}
		for _, row := range rows {
			id, err := strconv.ParseInt(string(row["id"]), 10, 64)
			if err != nil {
				t.Fatalf("gateway JSON id %s: %v", row["id"], err)
			}
			p := bytes.TrimSpace(row["payload"])
			if len(p) == 0 || (p[0] != '{' && p[0] != '[') {
				t.Fatalf("gateway JSON payload for id %d is %s, want an embedded object or array", id, p)
			}
			v, err := framework.ParseJSONExact(p)
			if err != nil {
				t.Fatalf("gateway JSON payload for id %d: %v", id, err)
			}
			got[id] = v
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gateway FORMAT_JSON payloads:\n got  %v\n want %v", got, want)
	}
	t.Logf("gateway FORMAT_JSON read: %s", read.Summary)
}
