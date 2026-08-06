// Package e2e — RFC 028 user-apps viewer + security e2e scenario (Task D2).
//
// This file exercises the full viewer + security chain from spec §11's first
// e2e paragraph against the REAL OrbStack cluster: upload → draft render
// (platform session vs viewer token) → promote → mint viewer token →
// `?token=` exchange → cookie → HTML/JSON render off a REAL warehouse table →
// query_audit app-principal attribution → SQL-injection-as-bound-literal →
// cross-app cookie replay → token revocation propagation → v2 promote
// propagation → render-log record.
//
// It requires (identical gate to scenarios_query_test.go):
//   - E2E_K8S=1
//   - SetupFGABootstrap ran in TestMain (SharedHarness != nil)
//   - pipeline-api reachable on its NodePort (framework.PipelineAPIReachable)
//   - the app-worker Deployment Running (appWorker.enabled=true is now the
//     base-chart default; tests/e2e/values-app.yaml pins its image + resources)
//
// Author-side calls (upload / promote / token / logs) go to pipeline-api's
// author routes over HTTP with the admin session cookie (task-P2-report.md).
// Viewer-side calls go straight to app-worker — the security assertions
// (302-strip, cookie attributes, revocation, injection) need raw HTTP the CLI
// deliberately hides (its render client refuses redirects and scrubs URLs), so
// this scenario does NOT drive the CLI for the viewer flow.
//
// app-worker's Service is ClusterIP:8090 with no NodePort (task-D1-report.md),
// so the scenario reaches it via a PER-POD `kubectl port-forward` — per pod
// (not the Service, which pins one backend) so the v2-propagation stage can
// poll every replica independently.
//
// The warehouse table is the SAME 100-row table scenarios_query_test.go seeds
// (ensureQueryTable): namespace "<runPrefix>-api", table "data", schema
// userId/id/title/body. The fixture app filters on the real `title` column.
package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// appSessionCookieName is app-worker's viewer session cookie name, fixed by
// contract (pkg/appworker/auth.go sessionCookieName).
const appSessionCookieName = "datuplet_app_session"

// appWorkerFixtureRel is the committed pre-bundled fixture, relative to the
// test's working directory (tests/e2e). esbuild is NOT run in CI — this is the
// uploaded artifact verbatim, after appsBundle() substitutes its tokens.
const appWorkerFixtureRel = "scenarios/testdata/apps/sales/app.js"

// appPropagationBound is the ≤15 s eventual-consistency window spec §5.1/§5.3
// give for resolve-cache promote propagation and viewer-token revocation, plus
// slack for the port-forward + poll cadence on a busy e2e cluster.
const appPropagationBound = 20 * time.Second

// TestApps_ViewerAndSecurity is the single staged D2 scenario. Each t.Run is
// one acceptance step (spec §11); later stages read state (app ids, hashes,
// viewer token, session cookie) captured by earlier ones and t.Skip cleanly if
// a prerequisite stage did not complete, so the FIRST real failure is the
// signal rather than a cascade.
func TestApps_ViewerAndSecurity(t *testing.T) {
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

	ctx := context.Background()

	// Reuse the query suite's seeded table + admin session + project id.
	ns, table := ensureQueryTable(t)
	if !appsSafeIdent(ns) || !appsSafeIdent(table) {
		t.Fatalf("seeded ns/table are not identifier-safe (the fixture double-quotes them into SQL): ns=%q table=%q", ns, table)
	}
	session := getAdminSession(t)
	// The admin must have project_admin on the harness project (implies
	// data_admin + describe — the author routes' relations) AND the app
	// impersonation identity's later query runs on this same project.
	ensureAdminFGAGrant(t, h, session)
	pid := getQueryProjectID(t)
	apiBase := framework.PipelineAPIBaseURL()

	// The final stage proves EVERY replica flips to v2 within ≤15 s, which is
	// only meaningful with ≥2 replicas (values-app.yaml sets appWorker.replicas:
	// 2). A single-replica run would fake-green that multi-replica claim, so <2
	// is a FAILURE, not a skip. Skip only when the Deployment is entirely absent
	// (wrong/older stack — environment not provisioned), consistent with the
	// suite's other "stack not up" skips.
	const wantReplicas = 2
	ready := appsWaitDeploymentReady(ctx, t, wantReplicas, 90*time.Second)
	if ready < 0 {
		framework.SkipOrFail(t, "app-worker Deployment not found — appWorker.enabled + values-app.yaml override + image built? (make docker-build-app-worker)")
	}
	if ready < wantReplicas {
		t.Fatalf("app-worker has %d ready replicas, want >= %d (values-app.yaml appWorker.replicas) — cannot prove multi-replica v2 propagation", ready, wantReplicas)
	}

	// Per-pod port-forwards (per POD, not the Service — a Service port-forward
	// pins ONE backend) so the promote-propagation stage can poll EVERY replica
	// independently. Require ≥ wantReplicas DISTINCT reachable pods, else the
	// all-replicas v2 check would silently degrade to a single-replica check.
	workers := appsStartWorkerForwards(ctx, t)
	distinctPods := map[string]bool{}
	for _, w := range workers {
		distinctPods[w.pod] = true
	}
	if len(distinctPods) < wantReplicas {
		t.Fatalf("only %d distinct app-worker pod(s) reachable via port-forward, need >= %d — the all-replicas v2 check would degrade to a single-replica check", len(distinctPods), wantReplicas)
	}
	appBase := workers[0].base
	t.Logf("app-worker reachable via %d per-pod port-forward(s): %v", len(workers), workers)

	// Two apps: `sales` is the app under test; `sales2` is the cross-app cookie
	// replay target. runPrefix ("e2e-xxxx") keeps both names DNS-label-valid
	// and unique per run.
	salesName := runPrefix + "-sales"
	otherName := runPrefix + "-sales2"

	fixture, err := os.ReadFile(appWorkerFixtureRel)
	if err != nil {
		t.Fatalf("read fixture bundle %s: %v", appWorkerFixtureRel, err)
	}
	salesV1 := appsBundle(t, fixture, ns, table, "v1")
	otherV1 := appsBundle(t, fixture, ns, table, "v1")

	// ── upload (draft) ──────────────────────────────────────────────────────
	// PUT moves the draft pointer and registers the app identity (P2); it never
	// touches production. Fatal here: nothing downstream is meaningful without
	// the app existing.
	salesAppID, salesV1Hash, err := appsPut(ctx, apiBase, session, pid, salesName, salesV1)
	if err != nil {
		t.Fatalf("upload sales app (draft): %v", err)
	}
	t.Logf("uploaded %s -> app_id=%s version_hash=%s (draft)", salesName, salesAppID, salesV1Hash)

	_, otherHash, err := appsPut(ctx, apiBase, session, pid, otherName, otherV1)
	if err != nil {
		t.Fatalf("upload sales2 app (draft): %v", err)
	}

	// State captured across stages.
	var (
		viewerToken    string // vw_<id>.<secret>, plaintext (mint-once)
		viewerTokenID  string
		v1Cookie       string // datuplet_app_session value from the exchange
		salesV2Hash    string
		v2Cookie       string // fresh viewer session after the first token is revoked
		lastRenderReq  string // a request_id we can look up in the render logs
		principalMatch string // the app principal we assert query_audit carries
	)
	principalMatch = "app-" + salesAppID

	// ── draft renders under a platform session, but NOT a viewer token ───────
	t.Run("draft_renders_under_platform_session", func(t *testing.T) {
		resp, body, err := appsRender(ctx, appBase, pid, salesName, true /*draft*/, nil,
			"application/json", []*http.Cookie{{Name: "pipeline_api_session", Value: session}})
		if err != nil {
			t.Fatalf("draft render: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("draft render under platform session: status=%d body=%s", resp.StatusCode, truncateLog(body, 512))
		}
		doc, err := appsParseDoc(body)
		if err != nil {
			t.Fatalf("draft render: parse OutputDoc: %v (body=%s)", err, truncateLog(body, 512))
		}
		if doc.Title != "Sales overview v1" {
			t.Errorf("draft render title = %q, want %q", doc.Title, "Sales overview v1")
		}
		// Proves the draft render reached the REAL warehouse under the app
		// identity (query_audit + P5) — the seed table has 100 rows.
		if got, ok := appsMetric(doc, "kpis"); !ok || got != 100 {
			t.Errorf("draft render kpis metric = %v (ok=%v), want 100", got, ok)
		}
	})

	t.Run("draft_rejects_viewer_token", func(t *testing.T) {
		// W4 row 16: a `?token=` on the draft channel is rejected outright with
		// 403 BEFORE any verification — so a syntactically valid dummy token is
		// the strongest form of this assertion (draft never accepts tokens,
		// ever). Real minting happens after promote, below.
		q := url.Values{"token": {"vw_00000000-0000-0000-0000-000000000000.dummysecret"}}
		resp, body, err := appsRender(ctx, appBase, pid, salesName, true /*draft*/, q, "application/json", nil)
		if err != nil {
			t.Fatalf("draft render with viewer token: %v", err)
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("draft render with viewer token: status=%d, want 403 (draft never accepts tokens); body=%s",
				resp.StatusCode, truncateLog(body, 512))
		}
	})

	// ── promote by hash (CAS) ────────────────────────────────────────────────
	t.Run("promote_v1_to_production", func(t *testing.T) {
		if err := appsPromote(ctx, apiBase, session, pid, salesName, salesV1Hash, nil); err != nil {
			t.Fatalf("promote sales v1: %v", err)
		}
		if err := appsPromote(ctx, apiBase, session, pid, otherName, otherHash, nil); err != nil {
			t.Fatalf("promote sales2 v1 (cookie-replay target): %v", err)
		}
	})

	// ── mint a viewer token ──────────────────────────────────────────────────
	t.Run("mint_viewer_token", func(t *testing.T) {
		id, tok, err := appsMintToken(ctx, apiBase, session, pid, salesName)
		if err != nil {
			t.Fatalf("mint viewer token: %v", err)
		}
		if !strings.HasPrefix(tok, "vw_") || !strings.Contains(tok, ".") {
			t.Fatalf("minted token has unexpected shape: %q", tok)
		}
		viewerToken, viewerTokenID = tok, id
		t.Logf("minted viewer token id=%s", id)
	})

	// ── one-time `?token=` exchange: 302 strips the token, sets the cookie ───
	t.Run("token_exchange_strips_token_and_sets_cookie", func(t *testing.T) {
		if viewerToken == "" {
			t.Skip("prerequisite mint_viewer_token did not complete")
		}
		// region=emea is an unrelated param that MUST survive the redirect
		// (only `token` is stripped — W4 tokenFreeTarget).
		q := url.Values{"token": {viewerToken}, "region": {"emea"}}
		resp, body, err := appsRender(ctx, appBase, pid, salesName, false /*production*/, q, "", nil)
		if err != nil {
			t.Fatalf("token exchange: %v", err)
		}
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("token exchange: status=%d, want 302; body=%s", resp.StatusCode, truncateLog(body, 512))
		}

		// 302 strips the token from the redirect target (W4).
		loc := resp.Header.Get("Location")
		secret := viewerToken[strings.IndexByte(viewerToken, '.')+1:]
		if loc == "" || strings.Contains(loc, "token") || strings.Contains(loc, secret) || strings.Contains(loc, viewerTokenID) {
			t.Errorf("redirect Location must be token-free: %q", loc)
		}
		wantPrefix := "/apps/" + pid + "/" + salesName
		if !strings.HasPrefix(loc, wantPrefix) {
			t.Errorf("redirect Location = %q, want prefix %q", loc, wantPrefix)
		}
		if !strings.Contains(loc, "region=emea") {
			t.Errorf("redirect Location dropped the non-token param: %q", loc)
		}
		// Referrer-Policy on the 302 itself (spec §5.3 — belt-and-braces so the
		// tokened URL can't ride a Referer out of the exchange).
		if got := resp.Header.Get("Referrer-Policy"); got != "no-referrer" {
			t.Errorf("Referrer-Policy on 302 = %q, want no-referrer", got)
		}

		// Cookie attributes: HttpOnly; Secure; SameSite=Lax; Path=/apps/{pid}/{name}.
		var c *http.Cookie
		for _, ck := range resp.Cookies() {
			if ck.Name == appSessionCookieName {
				c = ck
			}
		}
		if c == nil {
			t.Fatalf("exchange set no %s cookie; Set-Cookie=%q", appSessionCookieName, resp.Header.Values("Set-Cookie"))
		}
		if !c.HttpOnly {
			t.Error("session cookie missing HttpOnly")
		}
		if !c.Secure {
			t.Error("session cookie missing Secure")
		}
		if c.SameSite != http.SameSiteLaxMode {
			t.Errorf("session cookie SameSite = %v, want Lax", c.SameSite)
		}
		if c.Path != wantPrefix {
			t.Errorf("session cookie Path = %q, want %q", c.Path, wantPrefix)
		}
		if c.Value == "" || strings.Contains(c.Value, secret) {
			t.Errorf("session cookie value is empty or leaks the secret")
		}

		// The plaintext leaks NOWHERE beyond being consumed: neither the full
		// vw_<id>.<secret> token nor its bare secret may appear in the 302
		// response body or in ANY response header (Set-Cookie included — the
		// signed cookie encodes app_id/token_id/exp+HMAC, never the raw token
		// or the secret). Location-only checking (the old form) would miss a
		// leak into the body or a non-Location header.
		for _, leak := range []struct{ name, val string }{
			{"full token", viewerToken},
			{"bare secret", secret},
		} {
			if strings.Contains(string(body), leak.val) {
				t.Errorf("302 response body leaks the %s", leak.name)
			}
			for hName, hVals := range resp.Header {
				for _, hv := range hVals {
					if strings.Contains(hv, leak.val) {
						t.Errorf("302 response header %q leaks the %s: %q", hName, leak.name, hv)
					}
				}
			}
		}
		v1Cookie = c.Value
	})

	// ── follow with the cookie: HTML shell renders ───────────────────────────
	t.Run("cookie_render_html", func(t *testing.T) {
		if v1Cookie == "" {
			t.Skip("prerequisite token exchange did not set a cookie")
		}
		resp, body, err := appsRender(ctx, appBase, pid, salesName, false, nil,
			"" /*navigation, no Accept: application/json*/, appsSessionCookies(v1Cookie))
		if err != nil {
			t.Fatalf("cookie HTML render: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("cookie HTML render: status=%d body=%s", resp.StatusCode, truncateLog(body, 512))
		}
		if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
			t.Errorf("Content-Type = %q, want text/html", ct)
		}
		if resp.Header.Get("Content-Security-Policy") == "" {
			t.Error("shell HTML missing Content-Security-Policy header")
		}
		bs := string(body)
		if !strings.Contains(bs, `id="dtp-root"`) || !strings.Contains(bs, `id="dtp-doc"`) {
			t.Errorf("shell HTML missing dtp-root / dtp-doc mount points; body=%s", truncateLog(body, 512))
		}
		if !strings.Contains(bs, "Sales overview v1") {
			t.Errorf("shell HTML does not embed the rendered doc title; body=%s", truncateLog(body, 512))
		}
	})

	// ── JSON render returns a doc with expected rows from the real table ─────
	t.Run("cookie_render_json_expected_rows", func(t *testing.T) {
		if v1Cookie == "" {
			t.Skip("prerequisite token exchange did not set a cookie")
		}
		resp, body, err := appsRender(ctx, appBase, pid, salesName, false, nil,
			"application/json", appsSessionCookies(v1Cookie))
		if err != nil {
			t.Fatalf("cookie JSON render: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("cookie JSON render: status=%d body=%s", resp.StatusCode, truncateLog(body, 512))
		}
		doc, err := appsParseDoc(body)
		if err != nil {
			t.Fatalf("cookie JSON render: parse OutputDoc: %v (body=%s)", err, truncateLog(body, 512))
		}
		if got, ok := appsMetric(doc, "kpis"); !ok || got != 100 {
			t.Errorf("kpis metric = %v (ok=%v), want 100 (real seeded table)", got, ok)
		}
		rows := appsTableRows(doc, "sample")
		if len(rows) == 0 {
			t.Errorf("sample table returned 0 rows on the happy path (no country filter); want >0")
		}
		if len(rows) > 5 {
			t.Errorf("sample table returned %d rows, want <=5 (LIMIT 5)", len(rows))
		}
	})

	// ── query_audit attributes the APP principal, with jti + ok outcome ──────
	t.Run("query_audit_attributes_app_principal_jti_and_ok", func(t *testing.T) {
		// Every sales render above ran datuplet.query under the app's
		// per-render impersonation JWT; pipeline-api's query route logs one
		// "query_audit" slog line carrying principal=app-<app_id>, the token's
		// crypto-random jti (P4/P5), and the outcome. That slog line is the only
		// e2e-observable attribution channel, so we scrape pipeline-api's pod
		// logs. A bare "query_audit"+principal grep (the old form) would
		// FAKE-GREEN a regression that dropped the jti or flipped the outcome, so
		// we parse the line and require BOTH an exact principal match AND a
		// NON-EMPTY jti AND outcome==ok.
		var found bool
		var sample, gotJTI, gotOutcome string
		deadline := time.Now().Add(15 * time.Second)
		for {
			lines, _ := appsPipelineAPILogLines(ctx)
			for _, ln := range lines {
				if !strings.Contains(ln, "query_audit") {
					continue
				}
				if appsExtractField(ln, "principal") != principalMatch {
					continue
				}
				jti := appsExtractField(ln, "jti")
				outcome := appsExtractField(ln, "outcome")
				if jti != "" && outcome == "ok" {
					found, sample, gotJTI, gotOutcome = true, ln, jti, outcome
					break
				}
			}
			if found || time.Now().After(deadline) {
				break
			}
			// Nudge a fresh successful render so a matching audit line appears.
			_, _, _ = appsRender(ctx, appBase, pid, salesName, false, nil, "application/json", appsSessionCookies(v1Cookie))
			time.Sleep(2 * time.Second)
		}
		if !found {
			t.Fatalf("no query_audit line with principal=%s AND non-empty jti AND outcome=ok found in pipeline-api logs", principalMatch)
		}
		t.Logf("query_audit: principal=%s jti=%s outcome=%s (%s)", principalMatch, gotJTI, gotOutcome, truncateLog([]byte(sample), 200))
	})

	// ── bound-param POSITIVE control, THEN injection binds as a literal ──────
	t.Run("bound_param_positive_control_then_injection", func(t *testing.T) {
		if v1Cookie == "" {
			t.Skip("prerequisite token exchange did not set a cookie")
		}

		// POSITIVE CONTROL first. Without it, the injection's "zero rows" could
		// fake-green a bind path that ALWAYS returns zero (i.e. one that never
		// filters at all). Render with a legitimate country == a real seeded
		// `title` and assert the sample returns ONLY rows carrying that title —
		// proving $country binds AND filters for a genuine value.
		known := appsDiscoverKnownTitle(ctx, t, session, pid, ns, table)
		{
			q := url.Values{"country": {known}}
			resp, body, err := appsRender(ctx, appBase, pid, salesName, false, q,
				"application/json", appsSessionCookies(v1Cookie))
			if err != nil {
				t.Fatalf("positive-control render: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("positive-control render: status=%d body=%s", resp.StatusCode, truncateLog(body, 512))
			}
			doc, err := appsParseDoc(body)
			if err != nil {
				t.Fatalf("positive-control render: parse OutputDoc: %v (body=%s)", err, truncateLog(body, 512))
			}
			rows := appsTableRows(doc, "sample")
			if len(rows) == 0 {
				t.Fatalf("positive control: country=%q returned 0 rows — $country did not bind/filter for a legitimate value", known)
			}
			if len(rows) > 5 {
				t.Errorf("positive control: %d rows, want <=5 (LIMIT 5)", len(rows))
			}
			for i, r := range rows {
				if title := appsRowString(r, 1); title != known {
					t.Errorf("positive control row %d title=%q, want %q (the filter must return only matching rows)", i, title, known)
				}
			}
		}

		// NEGATIVE (injection). `?country=' UNION SELECT ...` binds as a VARCHAR
		// literal → the `title` column never equals that garbage → zero rows,
		// 200, no error leak. If it were string-interpolated instead, the UNION
		// would return extra rows (row count changes) — so a regression to
		// interpolation FAILS this stage.
		injection := `' UNION SELECT id, body FROM "` + ns + `"."` + table + `" --`
		q := url.Values{"country": {injection}}
		resp, body, err := appsRender(ctx, appBase, pid, salesName, false, q,
			"application/json", appsSessionCookies(v1Cookie))
		if err != nil {
			t.Fatalf("injection render: %v", err)
		}
		// No error leak: a bound literal is not a SQL/render error.
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("injection render: status=%d, want 200 (bound literal, no error leak); body=%s",
				resp.StatusCode, truncateLog(body, 512))
		}
		doc, err := appsParseDoc(body)
		if err != nil {
			t.Fatalf("injection render: parse OutputDoc (must be a doc, not an error envelope): %v (body=%s)",
				err, truncateLog(body, 512))
		}
		// The filtered table matched the garbage literal against `title` → zero
		// rows. If the UNION had executed, it would have produced rows.
		if rows := appsTableRows(doc, "sample"); len(rows) != 0 {
			t.Errorf("injection: sample table has %d rows, want 0 (the value must bind as a literal, not run)", len(rows))
		}
		// The unfiltered count is untouched by the bound filter — proof the
		// query still ran normally and nothing extra was injected.
		if got, ok := appsMetric(doc, "kpis"); !ok || got != 100 {
			t.Errorf("injection: kpis metric = %v (ok=%v), want 100 (unfiltered count unaffected)", got, ok)
		}
	})

	// ── a cookie minted for app A is worthless against app B → 401 ───────────
	t.Run("cookie_replay_second_app_401", func(t *testing.T) {
		if v1Cookie == "" {
			t.Skip("prerequisite token exchange did not set a cookie")
		}
		resp, body, err := appsRender(ctx, appBase, pid, otherName, false, nil,
			"application/json", appsSessionCookies(v1Cookie))
		if err != nil {
			t.Fatalf("cross-app replay render: %v", err)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("cookie for %s replayed against %s: status=%d, want 401; body=%s",
				salesName, otherName, resp.StatusCode, truncateLog(body, 512))
		}
	})

	// ── revoke the viewer token → sessions die within ≤15 s ──────────────────
	t.Run("revoke_token_propagates_401_within_15s", func(t *testing.T) {
		if v1Cookie == "" || viewerTokenID == "" {
			t.Skip("prerequisite token exchange / mint did not complete")
		}
		// Sanity: the cookie authenticates right now.
		if resp, _, err := appsRender(ctx, appBase, pid, salesName, false, nil, "application/json", appsSessionCookies(v1Cookie)); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("pre-revoke cookie render should be 200 (got err=%v status=%v)", err, appsStatus(resp))
		}
		if err := appsDeleteToken(ctx, apiBase, session, pid, salesName, viewerTokenID); err != nil {
			t.Fatalf("revoke viewer token: %v", err)
		}
		start := time.Now()
		deadline := start.Add(appPropagationBound)
		for {
			resp, _, err := appsRender(ctx, appBase, pid, salesName, false, nil, "application/json", appsSessionCookies(v1Cookie))
			if err == nil && resp.StatusCode == http.StatusUnauthorized {
				t.Logf("revocation propagated to 401 after %s", time.Since(start).Round(time.Millisecond))
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("viewer session still valid %s after revoke (want 401 within ≤15 s + slack); last status=%v err=%v",
					appPropagationBound, appsStatus(resp), err)
			}
			time.Sleep(time.Second)
		}
	})

	// ── promote v2 → every replica serves it within ≤15 s ────────────────────
	t.Run("promote_v2_all_replicas_within_15s", func(t *testing.T) {
		// Upload v2 (distinct title → distinct content hash → a genuine second
		// immutable version), then CAS-promote it over v1.
		salesV2 := appsBundle(t, fixture, ns, table, "v2")
		_, hash, err := appsPut(ctx, apiBase, session, pid, salesName, salesV2)
		if err != nil {
			t.Fatalf("upload sales v2: %v", err)
		}
		salesV2Hash = hash
		if hash == salesV1Hash {
			t.Fatalf("v2 bundle hashed identically to v1 (%s) — the version marker did not change the bytes", hash)
		}
		expected := salesV1Hash
		if err := appsPromote(ctx, apiBase, session, pid, salesName, salesV2Hash, &expected); err != nil {
			t.Fatalf("promote sales v2 (CAS over v1): %v", err)
		}

		// The first viewer token was revoked above; mint a fresh one and
		// exchange it so we can drive authenticated production renders.
		id, tok, err := appsMintToken(ctx, apiBase, session, pid, salesName)
		if err != nil {
			t.Fatalf("mint viewer token (v2 poll): %v", err)
		}
		_ = id
		v2Cookie = appsExchangeForCookie(ctx, t, appBase, pid, salesName, tok)
		if v2Cookie == "" {
			t.Fatalf("v2 poll: token exchange yielded no session cookie")
		}

		// Poll EVERY replica's own port-forward until it serves the v2 title,
		// bounded by ≤15 s + slack, and record which DISTINCT pods flipped. This
		// is where per-pod forwarding earns its keep (a Service port-forward pins
		// one backend). Asserting ≥ wantReplicas distinct pods served v2 is what
		// stops this degrading into a single-replica check.
		servedV2 := map[string]bool{}
		for i, w := range workers {
			start := time.Now()
			deadline := start.Add(appPropagationBound)
			var lastTitle string
			for {
				resp, body, err := appsRender(ctx, w.base, pid, salesName, false, nil, "application/json", appsSessionCookies(v2Cookie))
				if err == nil && resp.StatusCode == http.StatusOK {
					if doc, derr := appsParseDoc(body); derr == nil {
						lastTitle = doc.Title
						if doc.Title == "Sales overview v2" {
							servedV2[w.pod] = true
							t.Logf("replica %d pod=%s (%s) serving v2 after %s", i, w.pod, w.base, time.Since(start).Round(time.Millisecond))
							break
						}
					}
				}
				if time.Now().After(deadline) {
					t.Fatalf("replica %d pod=%s (%s) still not serving v2 after %s (last title=%q)", i, w.pod, w.base, appPropagationBound, lastTitle)
				}
				time.Sleep(time.Second)
			}
		}
		if len(servedV2) < wantReplicas {
			t.Fatalf("only %d distinct pod(s) observed serving v2, need >= %d — the poll did not cover all replicas", len(servedV2), wantReplicas)
		}
	})

	// ── a render-log record exists with request_id + principal fields ────────
	t.Run("render_log_has_request_id_and_principal", func(t *testing.T) {
		// The v2 renders above (viewer_token principal) each enqueue a render
		// log via app-worker's async AppendLog (spec §6.6). Poll the author
		// logs endpoint for the newest production/viewer_token record.
		var rec appsRenderLog
		deadline := time.Now().Add(15 * time.Second)
		for {
			recs, err := appsListLogs(ctx, apiBase, session, pid, salesName)
			if err == nil {
				for _, r := range recs {
					if r.Channel == "production" && r.PrincipalKind == "viewer_token" && r.Outcome == "ok" {
						rec = r
						break
					}
				}
			}
			if rec.RequestID != "" || time.Now().After(deadline) {
				break
			}
			// Nudge another render so a fresh record is enqueued.
			if v2Cookie != "" {
				_, _, _ = appsRender(ctx, appBase, pid, salesName, false, nil, "application/json", appsSessionCookies(v2Cookie))
			}
			time.Sleep(2 * time.Second)
		}
		if rec.RequestID == "" {
			t.Fatalf("no production/viewer_token render-log record appeared for %s", salesName)
		}
		if !appsLooksLikeUUID(rec.RequestID) {
			t.Errorf("render-log request_id %q is not a UUID", rec.RequestID)
		}
		if rec.PrincipalID == "" {
			t.Errorf("render-log record has empty principal_id")
		}
		if salesV2Hash != "" && rec.VersionHash != salesV2Hash {
			t.Errorf("render-log version_hash = %q, want v2 %q", rec.VersionHash, salesV2Hash)
		}
		lastRenderReq = rec.RequestID

		// The request_id-keyed lookup (spec §5.1 / §6.6) returns exactly that
		// record — the same id the §8 envelope and access log carry.
		one, err := appsGetLogByRequestID(ctx, apiBase, session, pid, salesName, lastRenderReq)
		if err != nil {
			t.Fatalf("logs?request_id=%s: %v", lastRenderReq, err)
		}
		if one.RequestID != lastRenderReq {
			t.Errorf("logs?request_id lookup returned request_id %q, want %q", one.RequestID, lastRenderReq)
		}
		if one.PrincipalKind != "viewer_token" {
			t.Errorf("logs?request_id record principal_kind = %q, want viewer_token", one.PrincipalKind)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Fixture bundling (no esbuild — placeholder substitution on a committed IIFE)
// ─────────────────────────────────────────────────────────────────────────────

// appsBundle substitutes the fixture's author-config tokens (warehouse table
// identifier + version marker) and fails loudly if any placeholder survives.
func appsBundle(t *testing.T, fixture []byte, ns, table, version string) []byte {
	t.Helper()
	out := bytes.ReplaceAll(fixture, []byte("__DTP_E2E_NS__"), []byte(ns))
	out = bytes.ReplaceAll(out, []byte("__DTP_E2E_TABLE__"), []byte(table))
	out = bytes.ReplaceAll(out, []byte("__DTP_APP_VERSION__"), []byte(version))
	if bytes.Contains(out, []byte("__DTP_")) {
		t.Fatalf("fixture bundle has an unreplaced placeholder after substitution (drift?)")
	}
	return out
}

// appsSafeIdent guards the ns/table before they are double-quoted into SQL by
// the fixture. The seeded values are "<runPrefix>-api"/"data"; this rejects
// anything that could break out of an identifier quote.
func appsSafeIdent(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

// ─────────────────────────────────────────────────────────────────────────────
// Author API (pipeline-api) — session-cookie authenticated
// ─────────────────────────────────────────────────────────────────────────────

// appsPut uploads a bundle (base64) to the draft channel (P2). Returns the
// app_id + the new version's content hash.
func appsPut(ctx context.Context, base, session, pid, name string, bundle []byte) (appID, versionHash string, err error) {
	body, _ := json.Marshal(map[string]string{"bundle_base64": base64.StdEncoding.EncodeToString(bundle)})
	u := fmt.Sprintf("%s/api/v1/projects/%s/apps/%s", base, pid, name)
	status, respBody, err := appsAuthorDo(ctx, http.MethodPut, u, session, body)
	if err != nil {
		return "", "", err
	}
	if status != http.StatusOK {
		return "", "", fmt.Errorf("PUT app %s: status=%d body=%s", name, status, truncateLog(respBody, 512))
	}
	var out struct {
		AppID       string `json:"app_id"`
		VersionHash string `json:"version_hash"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", "", fmt.Errorf("decode PUT response: %w (body=%s)", err, truncateLog(respBody, 256))
	}
	if out.AppID == "" || out.VersionHash == "" {
		return "", "", fmt.Errorf("PUT response missing app_id/version_hash: %s", truncateLog(respBody, 256))
	}
	return out.AppID, out.VersionHash, nil
}

// appsPromote CAS-promotes a version to production (P2). expected==nil omits
// the CAS precondition (first promote / "no expectation").
func appsPromote(ctx context.Context, base, session, pid, name, version string, expected *string) error {
	req := struct {
		Version            string  `json:"version"`
		ExpectedProduction *string `json:"expectedProduction,omitempty"`
	}{Version: version, ExpectedProduction: expected}
	body, _ := json.Marshal(req)
	u := fmt.Sprintf("%s/api/v1/projects/%s/apps/%s/promote", base, pid, name)
	status, respBody, err := appsAuthorDo(ctx, http.MethodPost, u, session, body)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("promote %s -> %s: status=%d body=%s", name, version, status, truncateLog(respBody, 512))
	}
	return nil
}

// appsMintToken mints a viewer token (P2); the plaintext is returned once.
func appsMintToken(ctx context.Context, base, session, pid, name string) (tokenID, token string, err error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/apps/%s/tokens", base, pid, name)
	status, body, err := appsAuthorDo(ctx, http.MethodPost, u, session, []byte("{}"))
	if err != nil {
		return "", "", err
	}
	if status != http.StatusCreated {
		return "", "", fmt.Errorf("mint token for %s: status=%d body=%s", name, status, truncateLog(body, 512))
	}
	var out struct {
		TokenID string `json:"token_id"`
		Token   string `json:"token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", "", fmt.Errorf("decode token response: %w", err)
	}
	return out.TokenID, out.Token, nil
}

// appsDeleteToken revokes a viewer token (P2).
func appsDeleteToken(ctx context.Context, base, session, pid, name, tokenID string) error {
	u := fmt.Sprintf("%s/api/v1/projects/%s/apps/%s/tokens/%s", base, pid, name, tokenID)
	status, body, err := appsAuthorDo(ctx, http.MethodDelete, u, session, nil)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("delete token %s: status=%d body=%s", tokenID, status, truncateLog(body, 256))
	}
	return nil
}

// appsRenderLog mirrors the author-route render-log JSON (P2 renderLogJSON).
type appsRenderLog struct {
	RequestID     string `json:"request_id"`
	VersionHash   string `json:"version_hash"`
	Channel       string `json:"channel"`
	PrincipalKind string `json:"principal_kind"`
	PrincipalID   string `json:"principal_id"`
	Outcome       string `json:"outcome"`
}

func appsListLogs(ctx context.Context, base, session, pid, name string) ([]appsRenderLog, error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/apps/%s/logs", base, pid, name)
	status, body, err := appsAuthorDo(ctx, http.MethodGet, u, session, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list logs: status=%d body=%s", status, truncateLog(body, 256))
	}
	var recs []appsRenderLog
	if err := json.Unmarshal(body, &recs); err != nil {
		return nil, fmt.Errorf("decode logs list: %w", err)
	}
	return recs, nil
}

func appsGetLogByRequestID(ctx context.Context, base, session, pid, name, requestID string) (appsRenderLog, error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/apps/%s/logs?request_id=%s", base, pid, name, url.QueryEscape(requestID))
	status, body, err := appsAuthorDo(ctx, http.MethodGet, u, session, nil)
	if err != nil {
		return appsRenderLog{}, err
	}
	if status != http.StatusOK {
		return appsRenderLog{}, fmt.Errorf("get log by request_id: status=%d body=%s", status, truncateLog(body, 256))
	}
	var rec appsRenderLog
	if err := json.Unmarshal(body, &rec); err != nil {
		return appsRenderLog{}, fmt.Errorf("decode single log: %w", err)
	}
	return rec, nil
}

// appsAuthorDo issues one author-API request with the admin session cookie.
func appsAuthorDo(ctx context.Context, method, u, session string, body []byte) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return 0, nil, fmt.Errorf("build %s %s: %w", method, u, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: session})
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: %w", method, u, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, respBody, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Viewer API (app-worker) — raw HTTP, redirects never auto-followed
// ─────────────────────────────────────────────────────────────────────────────

// appsRender issues one render request to app-worker and returns the raw
// response (redirects are NOT followed, so the 302 exchange is observable).
func appsRender(ctx context.Context, base, pid, name string, draft bool, params url.Values, accept string, cookies []*http.Cookie) (*http.Response, []byte, error) {
	path := "/apps/" + pid + "/" + name
	if draft {
		path += "@draft"
	}
	u := base + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build render request: %w", err)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	for _, c := range cookies {
		req.AddCookie(c)
	}
	client := &http.Client{
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("render %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return resp, body, nil
}

// appsExchangeForCookie performs a `?token=` exchange and returns the session
// cookie value (fatal on any deviation).
func appsExchangeForCookie(ctx context.Context, t *testing.T, base, pid, name, token string) string {
	t.Helper()
	resp, body, err := appsRender(ctx, base, pid, name, false, url.Values{"token": {token}}, "", nil)
	if err != nil {
		t.Fatalf("token exchange: %v", err)
	}
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("token exchange: status=%d, want 302; body=%s", resp.StatusCode, truncateLog(body, 256))
	}
	for _, c := range resp.Cookies() {
		if c.Name == appSessionCookieName {
			return c.Value
		}
	}
	return ""
}

func appsSessionCookies(value string) []*http.Cookie {
	return []*http.Cookie{{Name: appSessionCookieName, Value: value}}
}

func appsStatus(resp *http.Response) any {
	if resp == nil {
		return "<nil>"
	}
	return resp.StatusCode
}

// ─────────────────────────────────────────────────────────────────────────────
// OutputDoc helpers
// ─────────────────────────────────────────────────────────────────────────────

type appsDoc struct {
	OutputDoc int    `json:"outputDoc"`
	Title     string `json:"title"`
	Blocks    []struct {
		ID    string `json:"id"`
		Type  string `json:"type"`
		Items []struct {
			Label string          `json:"label"`
			Value json.RawMessage `json:"value"`
		} `json:"items"`
		Rows []json.RawMessage `json:"rows"`
	} `json:"blocks"`
}

// appsParseDoc decodes an OutputDoc and rejects an error envelope masquerading
// as one (an error body has no outputDoc:1).
func appsParseDoc(body []byte) (appsDoc, error) {
	var doc appsDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return appsDoc{}, err
	}
	if doc.OutputDoc != 1 {
		return appsDoc{}, fmt.Errorf("not an OutputDoc (outputDoc=%d) — likely an error envelope", doc.OutputDoc)
	}
	return doc, nil
}

// appsMetric returns the first metric item's value for the named block as an
// int (accepts JSON number or numeric string, matching query serialization).
func appsMetric(doc appsDoc, blockID string) (int, bool) {
	for _, b := range doc.Blocks {
		if b.ID != blockID || b.Type != "metric" || len(b.Items) == 0 {
			continue
		}
		return appsAsInt(b.Items[0].Value)
	}
	return 0, false
}

// appsTableRows returns the rows of the named table block.
func appsTableRows(doc appsDoc, blockID string) []json.RawMessage {
	for _, b := range doc.Blocks {
		if b.ID == blockID && b.Type == "table" {
			return b.Rows
		}
	}
	return nil
}

// appsRowString returns the string cell at idx of a table row (a JSON array),
// or "" if the row is not an array or the cell is not a string.
func appsRowString(row json.RawMessage, idx int) string {
	var cells []json.RawMessage
	if err := json.Unmarshal(row, &cells); err != nil || idx < 0 || idx >= len(cells) {
		return ""
	}
	var s string
	if err := json.Unmarshal(cells[idx], &s); err != nil {
		return ""
	}
	return s
}

// appsDiscoverKnownTitle returns a real `title` value from the seeded table via
// the admin query route — the legitimate value the positive bind control
// filters on, so the injection's zero-rows result is proven meaningful (not a
// bind path that always returns nothing). Reuses postQuery/queryResult from the
// query scenario.
func appsDiscoverKnownTitle(ctx context.Context, t *testing.T, session, pid, ns, table string) string {
	t.Helper()
	sql := fmt.Sprintf(`SELECT title FROM "%s"."%s" WHERE title IS NOT NULL ORDER BY id LIMIT 1`, ns, table)
	status, body, err := postQuery(ctx, session, pid, queryRequest{SQL: sql})
	if err != nil || status != http.StatusOK {
		t.Fatalf("discover known title: status=%d err=%v body=%s", status, err, truncateLog(body, 256))
	}
	var res queryResult
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("discover known title: decode: %v", err)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		t.Fatalf("discover known title: query returned no rows")
	}
	title, ok := res.Rows[0][0].(string)
	if !ok || title == "" {
		t.Fatalf("discover known title: title cell is not a non-empty string: %#v", res.Rows[0][0])
	}
	return title
}

// appsAsInt tolerates a JSON number ("100") or a numeric string ("\"100\"").
func appsAsInt(raw json.RawMessage) (int, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return int(n), true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		var f float64
		if _, err := fmt.Sscanf(s, "%g", &f); err == nil {
			return int(f), true
		}
	}
	return 0, false
}

func appsLooksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, r := range s {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			isHex := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster access — app-worker port-forwards + pipeline-api log scraping
// ─────────────────────────────────────────────────────────────────────────────

// appsWaitDeploymentReady polls app-worker's ready-replica count until it
// reaches `want` or `timeout` elapses, returning the last observed count. It
// returns -1 only when the Deployment cannot be found at all (kubectl lookup
// error for the whole window) so the caller can SKIP (env not provisioned)
// rather than FAIL; an existing-but-under-replicated Deployment returns its
// real count (0/1/…) so the caller can fail the multi-replica requirement.
func appsWaitDeploymentReady(ctx context.Context, t *testing.T, want int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := -1
	for {
		out, err := exec.CommandContext(ctx, "kubectl", "get", "deployment", "app-worker",
			"-n", queryE2ENamespace,
			"-o", "jsonpath={.status.readyReplicas}").Output()
		if err != nil {
			// Deployment absent (or kubectl unavailable): keep -1 so the caller
			// skips rather than fails.
			if time.Now().After(deadline) {
				return last
			}
			time.Sleep(2 * time.Second)
			continue
		}
		n := 0
		if s := strings.TrimSpace(string(out)); s != "" {
			_, _ = fmt.Sscanf(s, "%d", &n)
		}
		last = n
		if n >= want || time.Now().After(deadline) {
			return last
		}
		time.Sleep(2 * time.Second)
	}
}

// appsWorkerFwd is one app-worker pod reachable via its own per-pod
// port-forward. The pod identity is carried so the v2-propagation stage can
// assert it observed ≥2 DISTINCT replicas serving the new version.
type appsWorkerFwd struct {
	pod  string
	base string
}

// appsStartWorkerForwards port-forwards every app-worker pod on its own local
// port and returns the ones that became ready (/readyz==200). Each forward is
// killed on test cleanup.
func appsStartWorkerForwards(ctx context.Context, t *testing.T) []appsWorkerFwd {
	t.Helper()
	out, err := exec.CommandContext(ctx, "kubectl", "get", "pods",
		"-n", queryE2ENamespace,
		"-l", "app.kubernetes.io/name=app-worker",
		"-o", "jsonpath={.items[*].metadata.name}").Output()
	if err != nil {
		t.Logf("list app-worker pods failed: %v", err)
		return nil
	}
	pods := strings.Fields(strings.TrimSpace(string(out)))
	var fwds []appsWorkerFwd
	for i, pod := range pods {
		if w, ok := appsStartPodForward(t, pod, 38090+i); ok {
			fwds = append(fwds, w)
		}
	}
	return fwds
}

// appsStartPodForward starts one `kubectl port-forward pod/<pod>` and waits for
// /readyz==200 (the engine has compiled). Returns the forward when ready.
func appsStartPodForward(t *testing.T, pod string, localPort int) (appsWorkerFwd, bool) {
	t.Helper()
	cmd := exec.Command("kubectl", "port-forward", "pod/"+pod,
		fmt.Sprintf("%d:8090", localPort), "-n", queryE2ENamespace)
	if err := cmd.Start(); err != nil {
		t.Logf("port-forward %s: start failed: %v", pod, err)
		return appsWorkerFwd{}, false
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	base := fmt.Sprintf("http://localhost:%d", localPort)
	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		resp, err := client.Get(base + "/readyz")
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return appsWorkerFwd{pod: pod, base: base}, true
		}
	}
	t.Logf("port-forward %s (%s): /readyz never became 200", pod, base)
	return appsWorkerFwd{}, false
}

// appsPipelineAPILogLines returns recent log lines aggregated across ALL
// pipeline-api pods.
func appsPipelineAPILogLines(ctx context.Context) ([]string, error) {
	out, err := exec.CommandContext(ctx, "kubectl", "logs",
		"-l", "app.kubernetes.io/name=pipeline-api",
		"-n", queryE2ENamespace,
		"--tail=5000", "--prefix").CombinedOutput()
	if err != nil {
		return nil, err
	}
	return strings.Split(string(out), "\n"), nil
}

// appsExtractField pulls one structured-log field value from a line, tolerating
// both slog's default logfmt (key=value, value bare or "quoted") and a JSON
// handler ("key":"value"). Returns "" when the field is absent or empty. Used
// to assert the query_audit line carries a NON-EMPTY jti and outcome=ok rather
// than merely mentioning the principal.
func appsExtractField(line, key string) string {
	if i := strings.Index(line, `"`+key+`":"`); i >= 0 {
		rest := line[i+len(key)+4:]
		if j := strings.IndexByte(rest, '"'); j >= 0 {
			return rest[:j]
		}
		return ""
	}
	if i := strings.Index(line, key+"="); i >= 0 {
		rest := line[i+len(key)+1:]
		if len(rest) > 0 && rest[0] == '"' {
			rest = rest[1:]
			if j := strings.IndexByte(rest, '"'); j >= 0 {
				return rest[:j]
			}
			return ""
		}
		if j := strings.IndexAny(rest, " \t\n"); j >= 0 {
			return rest[:j]
		}
		return rest
	}
	return ""
}
