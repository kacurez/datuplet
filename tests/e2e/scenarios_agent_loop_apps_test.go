// Package e2e — RFC 028 user-apps agent-flow e2e scenario (Task D3, spec §11
// para 2). Distinct from TestApps_ViewerAndSecurity (scenarios_apps_test.go,
// Task D2, spec §11 para 1): that scenario drives raw HTTP to prove the
// viewer/security wire contract (302 exchange, cookie attributes, injection,
// revocation, ...). THIS scenario drives the REAL compiled `datuplet` CLI
// binary through the agent build-test-ship loop an LLM (or a script) runs
// headlessly, mirroring scenarios_agent_loop_test.go's
// TestAgentLoop_SchemaToVerify pattern (runCLI/repoRoot, headless auth via
// $DATUPLET_REMOTE/$DATUPLET_API_TOKEN/$DATUPLET_PROJECT — no ~/.datuplet
// state):
//
//  1. apps init                          -> scaffold files exist, no network
//  2. (substitute D2's pre-bundled fixture for the scaffold's app.js —
//     esbuild is NOT run in CI; task-D0-report.md §D5)
//  3. apps put                           -> {app_id, version_hash}
//  4. apps render --channel draft --json -> a valid OutputDoc, exit 0
//  5. apps put (a SECOND, THROWING bundle) -> a new draft version
//  6. apps render --channel draft --json -> exit 1, the ONE
//     {error, kind, request_id, author_log} object, author_log NON-NULL and
//     matching THIS render's request_id (proves the CLI fetched the
//     matching render-log record, not a stale/unrelated one)
//  7. apps promote <the FIRST, working version's hash> -> production
//  8. apps token create                  -> a fresh vw_<id>.<secret>
//  9. viewer `?token=` exchange + cookie render -> 200, a valid OutputDoc
//     (raw HTTP — the CLI has no `--token=` viewer flag, so this last leg
//     reuses scenarios_apps_test.go's own viewer helpers verbatim, the same
//     technique TestApps_ViewerAndSecurity's exchange stage uses)
//
// # Why a local reverse proxy stands in for "the ingress"
//
// `datuplet apps render` builds its target as
// `--remote/$DATUPLET_REMOTE + "/apps/{pid}/{name}[@draft]"`
// (cmd/datuplet/apps.go's appsRenderURL) — i.e. it assumes `/apps/*` is
// reachable on the SAME host as every other subcommand's `/api/v1/*` route.
// Tasks D0/D1 established that NO chart-shipped Ingress exists
// (docs/known-limitations.md): app-worker's Service is ClusterIP-only,
// reached in this suite via a per-pod `kubectl port-forward` (the identical
// mechanism scenarios_apps_test.go's appsStartWorkerForwards already uses),
// and pipeline-api is reached via its NodePort (framework.PipelineAPIBaseURL).
// In production this gap is the operator's own Ingress, which MUST route
// both `/ui` and `/apps` onto one host — the same note this task adds to
// docs/known-limitations.md. agentLoopStartLocalIngress below is that
// missing piece, stood up for the duration of this test only: an in-process
// httputil.ReverseProxy fronting BOTH services under one local URL, so the
// CLI's single-`--remote` design has something real to talk to. This is not
// a workaround for a CLI bug — it is the harness supplying the exact routing
// topology the shipped system requires an operator to provide.
//
// Gating (identical to scenarios_apps_test.go / scenarios_agent_loop_test.go):
// requires E2E_K8S=1, a live SharedHarness, PreCheck, and pipeline-api
// reachable. A cluster-less `go test ./tests/e2e/...` t.Skips here and stays
// green — no fake-green: every assertion below is a real, load-bearing check
// against the CLI's actual exit code and stdout shape.
package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// agentLoopThrowsFixtureRel is the committed pre-bundled failure-case fixture
// for this task's failure-case step — a bundle whose render() unconditionally
// throws. No NS/TABLE/VERSION substitution needed (see the file's own header
// comment): it fails identically regardless of channel/params/warehouse state.
const agentLoopThrowsFixtureRel = "scenarios/testdata/apps/throws/app.js"

// agentLoopScaffoldFiles are the files `datuplet apps init` must write —
// mirrors cmd/datuplet/apps_scaffold/'s actual tree (task-C1-report.md).
var agentLoopScaffoldFiles = []string{"app.js", "datuplet.d.ts", "esbuild.mjs", "package.json", "README.md"}

// agentLoopRenderFailure mirrors the CLI's appsRenderFailureJSON
// (cmd/datuplet/apps.go) — hand-mirrored locally since that type is
// unexported in package main. AuthorLog is a json.RawMessage so "null" vs an
// object is distinguishable without a throwaway intermediate type.
type agentLoopRenderFailure struct {
	Error     string          `json:"error"`
	Kind      string          `json:"kind"`
	RequestID string          `json:"request_id"`
	AuthorLog json.RawMessage `json:"author_log"`
}

// agentLoopAuthorLogRecord decodes just enough of the author_log record
// (mirrors pkg/pipelineapi/apps's renderLogJSON / cmd/datuplet's
// appsRenderLogRecord) to prove it is the MATCHING record for this render —
// not merely present.
type agentLoopAuthorLogRecord struct {
	RequestID string `json:"request_id"`
	Outcome   string `json:"outcome"`
	Error     string `json:"error"`
}

// agentLoopPutResponse mirrors appPutResponseJSON (cmd/datuplet/apps.go).
type agentLoopPutResponse struct {
	AppID       string `json:"app_id"`
	VersionHash string `json:"version_hash"`
}

// agentLoopTokenResponse mirrors appTokenCreateResponseJSON (cmd/datuplet/apps.go).
type agentLoopTokenResponse struct {
	TokenID string `json:"token_id"`
	Token   string `json:"token"`
}

func TestAgentLoop_UserApps(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil — SetupFGABootstrap must have run in TestMain")
	}
	if err := framework.PreCheck(); err != nil {
		framework.SkipOrFail(t, "precheck failed: %v", err)
	}
	if !framework.PipelineAPIReachable() {
		framework.SkipOrFail(t, "pipeline-api not reachable on NodePort 30081 — start port-forward")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// ── Step 0: build the datuplet CLI (mirrors scenarios_agent_loop_test.go) ──
	root := repoRoot(t)
	bin := filepath.Join(t.TempDir(), "datuplet")
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/datuplet")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build datuplet CLI: %v\n%s", err, string(out))
	}

	// ── Step 0b: headless auth + the SAME project every other apps assertion
	// in this suite targets. framework.ResolveDatupletProjectID and
	// getQueryProjectID both resolve h.LakekeeperProjectName to the identical
	// Datuplet project row (admin_exec.go / scenarios_query_test.go), so the
	// seeded table + FGA grant below and the CLI's $DATUPLET_PROJECT agree.
	pid, err := framework.ResolveDatupletProjectID(ctx, h)
	if err != nil {
		t.Fatalf("resolve Datuplet project id: %v", err)
	}
	token, err := framework.MintAdminCLIToken(ctx, h, time.Hour)
	if err != nil {
		t.Fatalf("mint admin cli-api token: %v", err)
	}

	// The CLI's bearer authenticates draft renders via app-worker's
	// authenticatePlatform -> pipeline-api sessions/verify, which requires
	// ProjectMember == true (pkg/appworker/auth.go). ensureAdminFGAGrant
	// (scenarios_query_test.go) grants project_admin to the SAME admin user
	// this bearer represents (e2eAdminEmail == queryAdminEmail).
	session := getAdminSession(t)
	ensureAdminFGAGrant(t, h, session)

	// Reuse the same seeded 100-row warehouse table every other apps/query
	// scenario in this suite reads (ensureQueryTable, scenarios_query_test.go).
	ns, table := ensureQueryTable(t)
	if !appsSafeIdent(ns) || !appsSafeIdent(table) {
		t.Fatalf("seeded ns/table are not identifier-safe: ns=%q table=%q", ns, table)
	}

	// app-worker reachability: at least one ready replica + a working
	// port-forward. Reuses scenarios_apps_test.go's helpers verbatim — no
	// second port-forward mechanism invented here. Unlike D2 this scenario
	// does not need multi-replica coverage, so want=1 suffices.
	ready := appsWaitDeploymentReady(ctx, t, 1, 90*time.Second)
	if ready < 0 {
		framework.SkipOrFail(t, "app-worker Deployment not found — appWorker.enabled + values-app.yaml override + image built? (make docker-build-app-worker)")
	}
	if ready < 1 {
		t.Fatalf("app-worker has 0 ready replicas — cannot drive the agent loop's render/viewer steps")
	}
	workers := appsStartWorkerForwards(ctx, t)
	if len(workers) == 0 {
		t.Fatalf("no app-worker pod reachable via port-forward (appsStartWorkerForwards returned none)")
	}
	appWorkerBase := workers[0].base

	// The local stand-in "ingress" — see the file header comment. Every apps
	// subcommand (put/render/promote/token) the CLI runs below goes through
	// this one URL.
	remote := agentLoopStartLocalIngress(t, framework.PipelineAPIBaseURL(), appWorkerBase)

	env := append(os.Environ(),
		"DATUPLET_REMOTE="+remote,
		"DATUPLET_API_TOKEN="+token,
		"DATUPLET_PROJECT="+pid,
	)

	appName := runPrefix + "-agentloop"
	t.Cleanup(func() {
		// Best-effort teardown so re-runs under the same project don't
		// collide (mirrors scenarios_agent_loop_test.go's own cleanup).
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		runCLI(cctx, t, bin, env, "apps", "delete", appName)
	})

	// ── Step 1: apps init — local scaffold, no network ─────────────────────
	appDir := filepath.Join(t.TempDir(), "agentloop-app")
	initRes := runCLI(ctx, t, bin, env, "apps", "init", appDir)
	if initRes.exitCode != 0 {
		t.Fatalf("apps init: want exit 0, got %d\nstdout=%s\nstderr=%s", initRes.exitCode, initRes.stdout, initRes.stderr)
	}
	for _, f := range agentLoopScaffoldFiles {
		p := filepath.Join(appDir, f)
		info, statErr := os.Stat(p)
		if statErr != nil {
			t.Fatalf("apps init: scaffold file %s missing: %v", f, statErr)
		}
		if info.IsDir() {
			t.Fatalf("apps init: scaffold entry %s is a directory, want a file", f)
		}
	}

	// ── Step 2: substitute D2's pre-bundled fixture for the scaffold's
	// app.js (esbuild is NOT run in CI — reuse appsBundle/appWorkerFixtureRel
	// from scenarios_apps_test.go exactly, no re-bundling here). ────────────
	happyFixture, err := os.ReadFile(appWorkerFixtureRel)
	if err != nil {
		t.Fatalf("read fixture bundle %s: %v", appWorkerFixtureRel, err)
	}
	happyBundle := appsBundle(t, happyFixture, ns, table, "agentloop-v1")
	bundlePath := filepath.Join(appDir, "app.js")
	if err := os.WriteFile(bundlePath, happyBundle, 0o644); err != nil {
		t.Fatalf("write substituted bundle over scaffold app.js: %v", err)
	}

	// ── Step 3: apps put -> {app_id, version_hash} ──────────────────────────
	putRes := runCLI(ctx, t, bin, env, "apps", "put", appName, "--bundle", bundlePath, "--json")
	if putRes.exitCode != 0 {
		t.Fatalf("apps put (happy bundle): want exit 0, got %d\nstdout=%s\nstderr=%s", putRes.exitCode, putRes.stdout, putRes.stderr)
	}
	var putOut agentLoopPutResponse
	if err := json.Unmarshal([]byte(strings.TrimSpace(putRes.stdout)), &putOut); err != nil {
		t.Fatalf("apps put: stdout is not the {app_id,version_hash} object: %v\nstdout=%s", err, putRes.stdout)
	}
	if putOut.AppID == "" || putOut.VersionHash == "" {
		t.Fatalf("apps put: empty app_id/version_hash in %+v", putOut)
	}
	happyVersionHash := putOut.VersionHash
	t.Logf("apps put (happy) -> app_id=%s version_hash=%s", putOut.AppID, happyVersionHash)

	// ── Step 4: apps render --channel draft --json -> a valid OutputDoc ────
	renderRes := runCLI(ctx, t, bin, env, "apps", "render", appName, "--channel", "draft", "--json")
	if renderRes.exitCode != 0 {
		t.Fatalf("apps render (happy, draft): want exit 0, got %d\nstdout=%s\nstderr=%s", renderRes.exitCode, renderRes.stdout, renderRes.stderr)
	}
	doc, err := appsParseDoc([]byte(strings.TrimSpace(renderRes.stdout)))
	if err != nil {
		t.Fatalf("apps render (happy, draft): stdout is not a valid OutputDoc: %v\nstdout=%s", err, renderRes.stdout)
	}
	if doc.Title == "" {
		t.Errorf("apps render (happy, draft): OutputDoc has an empty title")
	}
	if got, ok := appsMetric(doc, "kpis"); !ok || got != 100 {
		t.Errorf("apps render (happy, draft): kpis metric = %v (ok=%v), want 100 (the real seeded table, proving this hit the actual warehouse)", got, ok)
	}
	t.Logf("apps render (happy, draft) -> OutputDoc title=%q", doc.Title)

	// ── Step 5: put a SECOND, THROWING bundle to the SAME app's draft ──────
	// (put always targets draft — this models the agent iterating: v1 worked,
	// v2 is a regression the agent is about to discover via render).
	throwsFixture, err := os.ReadFile(agentLoopThrowsFixtureRel)
	if err != nil {
		t.Fatalf("read throwing fixture %s: %v", agentLoopThrowsFixtureRel, err)
	}
	throwsPath := filepath.Join(t.TempDir(), "throws-app.js")
	if err := os.WriteFile(throwsPath, throwsFixture, 0o644); err != nil {
		t.Fatalf("write throwing bundle: %v", err)
	}
	putThrowsRes := runCLI(ctx, t, bin, env, "apps", "put", appName, "--bundle", throwsPath, "--json")
	if putThrowsRes.exitCode != 0 {
		t.Fatalf("apps put (throwing bundle): want exit 0, got %d\nstdout=%s\nstderr=%s", putThrowsRes.exitCode, putThrowsRes.stdout, putThrowsRes.stderr)
	}
	var putThrowsOut agentLoopPutResponse
	if err := json.Unmarshal([]byte(strings.TrimSpace(putThrowsRes.stdout)), &putThrowsOut); err != nil {
		t.Fatalf("apps put (throwing bundle): stdout is not the {app_id,version_hash} object: %v\nstdout=%s", err, putThrowsRes.stdout)
	}
	if putThrowsOut.VersionHash == happyVersionHash {
		t.Fatalf("apps put (throwing bundle): version_hash unchanged from the happy bundle (%s) — the throwing fixture did not actually change draft", happyVersionHash)
	}

	// ── Step 6: apps render --channel draft --json -> exit 1, ONE
	// {error, kind, request_id, author_log} object, author_log NON-NULL and
	// MATCHING this render's own request_id. ───────────────────────────────
	//
	// Render-log append is async fire-and-forget (spec §6.6; app-worker
	// enqueues and returns immediately — task-D2-report.md flags this same
	// property). The CLI issues its author-log fetch IMMEDIATELY after the
	// failed render, within the same process invocation, so there is a real
	// (small) race between "the failure response landed" and "the record is
	// queryable via GET .../logs?request_id=...". Retrying the WHOLE `apps
	// render` call (a fresh render, a fresh request_id every attempt, still
	// requiring AuthorLog.RequestID == the OUTER object's RequestID on the
	// attempt that succeeds) rides out that race honestly: every attempt is
	// fully independent and must satisfy every assertion — this does not
	// weaken what's being proved, it only tolerates queue latency the same
	// way task-D2-report.md's own render-log stage polls and nudges renders.
	var failObj agentLoopRenderFailure
	var authorLog agentLoopAuthorLogRecord
	loopStart := time.Now()
	deadline := loopStart.Add(15 * time.Second)
	attempt := 0
	for {
		attempt++
		failRes := runCLI(ctx, t, bin, env, "apps", "render", appName, "--channel", "draft", "--json")
		if failRes.exitCode != 1 {
			t.Fatalf("apps render (throwing, draft) attempt %d: want exit 1 (render failure, user-error class), got %d\nstdout=%s\nstderr=%s",
				attempt, failRes.exitCode, failRes.stdout, failRes.stderr)
		}
		trimmed := strings.TrimSpace(failRes.stdout)
		// Exactly ONE JSON value on stdout: decode, then confirm the decoder
		// has nothing left to read (More() false) — stronger than a bare
		// line-count check, and robust to incidental whitespace.
		dec := json.NewDecoder(strings.NewReader(trimmed))
		failObj = agentLoopRenderFailure{}
		if err := dec.Decode(&failObj); err != nil {
			t.Fatalf("apps render (throwing, draft) attempt %d: stdout is not the {error,kind,request_id,author_log} object: %v\nstdout=%s",
				attempt, err, failRes.stdout)
		}
		if dec.More() {
			t.Fatalf("apps render (throwing, draft) attempt %d: stdout carries more than ONE JSON value: %s", attempt, trimmed)
		}
		if failObj.Kind == "" {
			t.Fatalf("apps render (throwing, draft) attempt %d: failure object has an empty kind: %s", attempt, trimmed)
		}
		if failObj.RequestID == "" {
			t.Fatalf("apps render (throwing, draft) attempt %d: failure object has an empty request_id: %s", attempt, trimmed)
		}

		hasAuthorLog := len(failObj.AuthorLog) > 0 && string(failObj.AuthorLog) != "null"
		if hasAuthorLog {
			authorLog = agentLoopAuthorLogRecord{}
			if err := json.Unmarshal(failObj.AuthorLog, &authorLog); err != nil {
				t.Fatalf("apps render (throwing, draft) attempt %d: author_log is not valid JSON: %v\nauthor_log=%s", attempt, err, failObj.AuthorLog)
			}
			if authorLog.RequestID == failObj.RequestID {
				t.Logf("apps render (throwing, draft) attempt %d: non-null author_log matches request_id=%s after %s",
					attempt, failObj.RequestID, time.Since(loopStart).Round(time.Millisecond))
				break
			}
			t.Logf("apps render (throwing, draft) attempt %d: author_log present but request_id mismatch (object=%s, log=%s) — retrying",
				attempt, failObj.RequestID, authorLog.RequestID)
		}
		if time.Now().After(deadline) {
			t.Fatalf("apps render (throwing, draft): author_log never became non-null AND matching within 15s across %d attempts; last object: %s",
				attempt, trimmed)
		}
		time.Sleep(1 * time.Second)
	}
	if authorLog.Outcome != "render_error" {
		t.Errorf("author_log outcome = %q, want render_error", authorLog.Outcome)
	}

	// ── Step 7: apps promote — the FIRST (working) version's hash, even
	// though draft currently points at the throwing bundle just put above.
	// promote is explicit-hash, CAS-addressed, never "whatever draft is now"
	// (spec §5.1) — this is exactly why the viewer's production render below
	// succeeds despite the most recent draft upload being broken. ─────────
	promoteRes := runCLI(ctx, t, bin, env, "apps", "promote", appName, "--version", happyVersionHash)
	if promoteRes.exitCode != 0 {
		t.Fatalf("apps promote: want exit 0, got %d\nstdout=%s\nstderr=%s", promoteRes.exitCode, promoteRes.stdout, promoteRes.stderr)
	}

	// ── Step 8: apps token create -> a fresh viewer token ───────────────────
	tokenRes := runCLI(ctx, t, bin, env, "apps", "token", "create", appName, "--json")
	if tokenRes.exitCode != 0 {
		t.Fatalf("apps token create: want exit 0, got %d\nstdout=%s\nstderr=%s", tokenRes.exitCode, tokenRes.stdout, tokenRes.stderr)
	}
	var tokOut agentLoopTokenResponse
	if err := json.Unmarshal([]byte(strings.TrimSpace(tokenRes.stdout)), &tokOut); err != nil {
		t.Fatalf("apps token create: stdout is not the {token_id,token} object: %v\nstdout=%s", err, tokenRes.stdout)
	}
	if !strings.HasPrefix(tokOut.Token, "vw_") || !strings.Contains(tokOut.Token, ".") {
		t.Fatalf("apps token create: minted token has unexpected shape: %q", tokOut.Token)
	}

	// ── Step 9: the viewer side — `?token=` exchange, then a cookie render.
	// The CLI has no `--token=` viewer flag (§5.5's CLI is the AUTHOR
	// surface; the viewer flow is deliberately raw HTTP, per C2's report), so
	// this leg reuses scenarios_apps_test.go's own viewer helpers verbatim —
	// the same technique TestApps_ViewerAndSecurity's exchange stage uses. ──
	cookie := appsExchangeForCookie(ctx, t, appWorkerBase, pid, appName, tokOut.Token)
	if cookie == "" {
		t.Fatalf("token exchange yielded no session cookie")
	}
	resp, body, err := appsRender(ctx, appWorkerBase, pid, appName, false, nil, "application/json", appsSessionCookies(cookie))
	if err != nil {
		t.Fatalf("viewer render with token-exchanged cookie: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("viewer render with token-exchanged cookie: status=%d, want 200; body=%s", resp.StatusCode, truncateLog(body, 512))
	}
	viewerDoc, err := appsParseDoc(body)
	if err != nil {
		t.Fatalf("viewer render: parse OutputDoc: %v (body=%s)", err, truncateLog(body, 512))
	}
	if viewerDoc.Title != doc.Title {
		t.Errorf("viewer render title = %q, want %q (the promoted happy version)", viewerDoc.Title, doc.Title)
	}
	if got, ok := appsMetric(viewerDoc, "kpis"); !ok || got != 100 {
		t.Errorf("viewer render kpis metric = %v (ok=%v), want 100", got, ok)
	}
	t.Logf("viewer token %q -> 200, title=%q (end to end: init -> put -> render -> failure -> promote -> token create -> viewer)",
		tokOut.TokenID, viewerDoc.Title)
}

// agentLoopStartLocalIngress starts an in-process HTTP reverse proxy for the
// duration of the test, fronting BOTH pipeline-api and app-worker under ONE
// local URL: requests under `/apps/` go to appWorkerBase, everything else to
// pipelineAPIBase. This is what makes `datuplet apps`'s single-`--remote`
// design (put/get/list/delete/promote/token/logs on `/api/v1/*`, render on
// `/apps/*`) work end-to-end against a cluster that ships no Ingress — see
// the file header comment and this task's docs/known-limitations.md note
// ("operator must route /apps under the same host as /ui"). Torn down via
// t.Cleanup.
func agentLoopStartLocalIngress(t *testing.T, pipelineAPIBase, appWorkerBase string) string {
	t.Helper()
	apiTarget, err := url.Parse(pipelineAPIBase)
	if err != nil {
		t.Fatalf("agentLoopStartLocalIngress: parse pipeline-api base %q: %v", pipelineAPIBase, err)
	}
	appTarget, err := url.Parse(appWorkerBase)
	if err != nil {
		t.Fatalf("agentLoopStartLocalIngress: parse app-worker base %q: %v", appWorkerBase, err)
	}

	apiProxy := httputil.NewSingleHostReverseProxy(apiTarget)
	appProxy := httputil.NewSingleHostReverseProxy(appTarget)

	mux := http.NewServeMux()
	mux.Handle("/apps/", appProxy)
	mux.Handle("/", apiProxy)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}
