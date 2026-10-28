package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureStdoutAndStderr runs fn with both os.Stdout and os.Stderr redirected
// to pipes and returns what each received. render's failure path splits its
// output by stream (the machine-readable object → stdout in --json mode; the
// human-formatted block → stderr in text mode), so the text-mode test needs
// to see both at once.
func captureStdoutAndStderr(t *testing.T, fn func()) (stdout, stderr string) {
	t.Helper()
	origOut, origErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdout): %v", err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stderr): %v", err)
	}
	os.Stdout, os.Stderr = wOut, wErr
	defer func() { os.Stdout, os.Stderr = origOut, origErr }()

	fn()

	_ = wOut.Close()
	_ = wErr.Close()
	var bufOut, bufErr bytes.Buffer
	if _, err := io.Copy(&bufOut, rOut); err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	if _, err := io.Copy(&bufErr, rErr); err != nil {
		t.Fatalf("read captured stderr: %v", err)
	}
	return bufOut.String(), bufErr.String()
}

// exitCodeOf mirrors main.go's `case "apps"` dispatch: an *exitCodeErr
// carries its own process exit code, everything else is the default 1, nil
// is 0. The agent loop branches on render's 1-vs-20 split, so the tests pin
// it here rather than only asserting "err != nil".
func exitCodeOf(err error) int {
	if err == nil {
		return 0
	}
	var ece *exitCodeErr
	if errors.As(err, &ece) {
		return ece.code
	}
	return 1
}

// --- validateAppName (pure) ---

// TestValidateAppName pins the server's own regex
// (^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$, pkg/pipelineapi/apps.appNamePattern)
// against the shapes the C1 gate review called out, plus a few more: a
// leading hyphen ("-leading-hyphen") is included here even though, reached
// via the CLI, it never gets this far (parseAppsFlags's pre-existing
// leading-dash-means-flag convention intercepts it first) — this proves the
// regex itself independently rejects that shape too.
func TestValidateAppName(t *testing.T) {
	invalid := append([]string{
		"", "A", "aB", "1_2", "a.", "a b", strings.Repeat("a", 64),
	}, invalidAppNames...)
	for _, name := range invalid {
		if err := validateAppName(name); err == nil {
			t.Errorf("validateAppName(%q) = nil, want an error", name)
		} else if !strings.Contains(err.Error(), appNamePattern) {
			t.Errorf("validateAppName(%q) error should name the pattern: %v", name, err)
		}
	}

	valid := []string{"a", "a1", "sales-overview", "x", "abc-def-123", strings.Repeat("a", 63)}
	for _, name := range valid {
		if err := validateAppName(name); err != nil {
			t.Errorf("validateAppName(%q) = %v, want nil", name, err)
		}
	}
}

// --- parseAppsFlags (pure) ---

func TestParseAppsFlags(t *testing.T) {
	positional, remote, tokenFile, project, bundle, channel, requestID, params, version, expectedProduction, asJSON, err := parseAppsFlags(
		[]string{"sales-overview", "--remote", "http://x", "--project=proj1", "--bundle", "b.js", "--json"})
	if err != nil {
		t.Fatalf("parseAppsFlags: %v", err)
	}
	if len(positional) != 1 || positional[0] != "sales-overview" {
		t.Errorf("positional = %v, want [sales-overview]", positional)
	}
	if remote != "http://x" {
		t.Errorf("remote = %q", remote)
	}
	if tokenFile != "" {
		t.Errorf("tokenFile = %q, want empty", tokenFile)
	}
	if project != "proj1" {
		t.Errorf("project = %q, want proj1", project)
	}
	if bundle != "b.js" {
		t.Errorf("bundle = %q, want b.js", bundle)
	}
	if channel != "" || requestID != "" || len(params) != 0 {
		t.Errorf("channel/requestID/params should be zero-valued here: %q %q %v", channel, requestID, params)
	}
	if version != "" || expectedProduction != "" {
		t.Errorf("version/expectedProduction should be zero-valued here: %q %q", version, expectedProduction)
	}
	if !asJSON {
		t.Error("asJSON = false, want true")
	}

	// render's flags: --channel plus a REPEATABLE --param, gathered in order.
	_, _, _, _, _, channel, _, params, _, _, _, err = parseAppsFlags(
		[]string{"sales-overview", "--channel", "draft", "--param", "days=7", "--param=country=DE"})
	if err != nil {
		t.Fatalf("parseAppsFlags (render): %v", err)
	}
	if channel != "draft" {
		t.Errorf("channel = %q, want draft", channel)
	}
	if len(params) != 2 || params[0] != "days=7" || params[1] != "country=DE" {
		t.Errorf("params = %v, want [days=7 country=DE]", params)
	}

	// logs' flag: --request-id.
	_, _, _, _, _, _, requestID, _, _, _, _, err = parseAppsFlags(
		[]string{"sales-overview", "--request-id", "req-abc"})
	if err != nil {
		t.Fatalf("parseAppsFlags (logs): %v", err)
	}
	if requestID != "req-abc" {
		t.Errorf("requestID = %q, want req-abc", requestID)
	}

	// promote's flags: --version plus optional --expected-production.
	_, _, _, _, _, _, _, _, version, expectedProduction, _, err = parseAppsFlags(
		[]string{"sales-overview", "--version", "abc123", "--expected-production=def456"})
	if err != nil {
		t.Fatalf("parseAppsFlags (promote): %v", err)
	}
	if version != "abc123" {
		t.Errorf("version = %q, want abc123", version)
	}
	if expectedProduction != "def456" {
		t.Errorf("expectedProduction = %q, want def456", expectedProduction)
	}

	if _, _, _, _, _, _, _, _, _, _, _, err := parseAppsFlags([]string{"--bogus"}); err == nil {
		t.Error("expected error for unknown flag")
	}
	for _, flag := range []string{"--remote", "--bundle", "--channel", "--param", "--request-id", "--version", "--expected-production"} {
		if _, _, _, _, _, _, _, _, _, _, _, err := parseAppsFlags([]string{flag}); err == nil {
			t.Errorf("expected error for %s missing a value", flag)
		}
	}
}

// --- apps init (local, no network) ---

func TestRunAppsInit_WritesScaffoldIncludingBuildScript(t *testing.T) {
	tmp := t.TempDir()
	// Nested: init must also create missing parent directories.
	dir := filepath.Join(tmp, "nested", "sales-overview")

	if err := runAppsInit([]string{dir}); err != nil {
		t.Fatalf("runAppsInit: %v", err)
	}

	wantFiles := []string{"app.js", "datuplet.d.ts", "esbuild.mjs", "package.json", "README.md"}
	for _, name := range wantFiles {
		p := filepath.Join(dir, name)
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("scaffold file %s missing: %v", name, err)
		}
		if info.Size() == 0 {
			t.Errorf("scaffold file %s is empty", name)
		}
	}

	// app.js: an ESM export, not the IIFE the build step produces.
	appJS, err := os.ReadFile(filepath.Join(dir, "app.js"))
	if err != nil {
		t.Fatalf("read app.js: %v", err)
	}
	if !strings.Contains(string(appJS), "export async function render(ctx)") {
		t.Errorf("app.js does not export an ESM render(ctx) function:\n%s", appJS)
	}
	if !strings.Contains(string(appJS), "datuplet.query(") {
		t.Errorf("app.js does not call datuplet.query(...):\n%s", appJS)
	}

	// datuplet.d.ts: types ctx, datuplet.query, and OutputDoc blocks.
	dts, err := os.ReadFile(filepath.Join(dir, "datuplet.d.ts"))
	if err != nil {
		t.Fatalf("read datuplet.d.ts: %v", err)
	}
	for _, want := range []string{"RenderContext", "query(", "OutputDoc", "ChartBlock", "TableBlock"} {
		if !strings.Contains(string(dts), want) {
			t.Errorf("datuplet.d.ts missing %q:\n%s", want, dts)
		}
	}

	// esbuild.mjs: the actual bundle invocation (spec §6.2 / Appendix A),
	// not just a comment referencing it.
	esbuildScript, err := os.ReadFile(filepath.Join(dir, "esbuild.mjs"))
	if err != nil {
		t.Fatalf("read esbuild.mjs: %v", err)
	}
	for _, want := range []string{"bundle: true", `format: "iife"`, `globalName: "__dtp_app"`} {
		if !strings.Contains(string(esbuildScript), want) {
			t.Errorf("esbuild.mjs missing %q:\n%s", want, esbuildScript)
		}
	}

	// package.json: a `build` script that runs the bundler, so `npm run
	// build` is a working one-command bundle (not just a README line).
	pkgJSON, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err != nil {
		t.Fatalf("read package.json: %v", err)
	}
	var pkg struct {
		Scripts struct {
			Build string `json:"build"`
		} `json:"scripts"`
		DevDependencies map[string]string `json:"devDependencies"`
	}
	if err := json.Unmarshal(pkgJSON, &pkg); err != nil {
		t.Fatalf("package.json is not valid JSON: %v", err)
	}
	if pkg.Scripts.Build == "" {
		t.Error("package.json has no scripts.build")
	}
	if _, ok := pkg.DevDependencies["esbuild"]; !ok {
		t.Error("package.json does not declare esbuild as a devDependency")
	}
}

func TestRunAppsInit_RefusesNonEmptyDir(t *testing.T) {
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "existing")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("pre-existing"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	err := runAppsInit([]string{dir})
	if err == nil {
		t.Fatal("expected an error for a non-empty directory, got nil")
	}
	if !strings.Contains(err.Error(), "not empty") && !strings.Contains(err.Error(), "empty") {
		t.Errorf("error should mention the directory is non-empty: %v", err)
	}
	// The pre-existing file must survive, and no scaffold file should have
	// been written alongside it.
	if _, err := os.Stat(filepath.Join(dir, "app.js")); err == nil {
		t.Error("app.js was written into a non-empty directory; init should have refused first")
	}
	kept, err := os.ReadFile(filepath.Join(dir, "keep.txt"))
	if err != nil || string(kept) != "pre-existing" {
		t.Errorf("pre-existing file was modified or removed: %v, %q", err, kept)
	}
}

// --- apps put/get/list/delete (httptest fake API) ---

// appsFakeBehaviour lets each test configure only the endpoints it
// exercises; unconfigured endpoints fail loudly if hit (mirrors
// trigger_test.go's fakeBehaviour / pipeline_test.go's pipelineFakeBehaviour).
//
// onRender serves the render endpoint app-worker owns in production
// (`/apps/{pid}/{name}[@draft]`, spec §4.1's ingress path prefix — same host
// as pipeline-api, no separate URL); onLogs serves pipeline-api's author
// render-log route (`/api/v1/projects/{pid}/apps/{name}/logs`). They share
// one httptest server precisely because the real deployment serves both on
// one ingress host — which is what lets `apps render` fetch the matching
// author log from the same resolved.Remote.
type appsFakeBehaviour struct {
	onPut         func(w http.ResponseWriter, r *http.Request)
	onGet         func(w http.ResponseWriter, r *http.Request)
	onList        func(w http.ResponseWriter, r *http.Request)
	onDelete      func(w http.ResponseWriter, r *http.Request)
	onRender      func(w http.ResponseWriter, r *http.Request)
	onLogs        func(w http.ResponseWriter, r *http.Request)
	onPromote     func(w http.ResponseWriter, r *http.Request)
	onCreateToken func(w http.ResponseWriter, r *http.Request)
	onListTokens  func(w http.ResponseWriter, r *http.Request)
	onDeleteToken func(w http.ResponseWriter, r *http.Request)
}

// newAppsFakeServer serves the exact author-route patterns registered by
// pkg/pipelineapi/apps.Handlers.Register, using the same Go 1.22+
// method+wildcard ServeMux patterns as the real handler set.
func newAppsFakeServer(t *testing.T, b appsFakeBehaviour) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	notConfigured := func(name string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, name+" not configured", http.StatusInternalServerError)
		}
	}
	mux.HandleFunc("PUT /api/v1/projects/{pid}/apps/{name}", func(w http.ResponseWriter, r *http.Request) {
		if b.onPut != nil {
			b.onPut(w, r)
			return
		}
		notConfigured("onPut")(w, r)
	})
	// The logs route must be registered before the plain {name} GET so the
	// more-specific `/logs` suffix wins (Go 1.22 mux picks the most specific
	// pattern regardless of registration order, but keeping them adjacent is
	// clearer).
	mux.HandleFunc("GET /api/v1/projects/{pid}/apps/{name}/logs", func(w http.ResponseWriter, r *http.Request) {
		if b.onLogs != nil {
			b.onLogs(w, r)
			return
		}
		notConfigured("onLogs")(w, r)
	})
	mux.HandleFunc("GET /api/v1/projects/{pid}/apps/{name}", func(w http.ResponseWriter, r *http.Request) {
		if b.onGet != nil {
			b.onGet(w, r)
			return
		}
		notConfigured("onGet")(w, r)
	})
	mux.HandleFunc("GET /api/v1/projects/{pid}/apps", func(w http.ResponseWriter, r *http.Request) {
		if b.onList != nil {
			b.onList(w, r)
			return
		}
		notConfigured("onList")(w, r)
	})
	mux.HandleFunc("DELETE /api/v1/projects/{pid}/apps/{name}", func(w http.ResponseWriter, r *http.Request) {
		if b.onDelete != nil {
			b.onDelete(w, r)
			return
		}
		notConfigured("onDelete")(w, r)
	})
	// Render endpoint (app-worker in production; same ingress host as the
	// author routes above). `{name}` captures the `@draft` suffix verbatim —
	// the CLI must send `@draft` un-escaped so the worker can split on it.
	mux.HandleFunc("GET /apps/{pid}/{name}", func(w http.ResponseWriter, r *http.Request) {
		if b.onRender != nil {
			b.onRender(w, r)
			return
		}
		notConfigured("onRender")(w, r)
	})
	mux.HandleFunc("POST /api/v1/projects/{pid}/apps/{name}/promote", func(w http.ResponseWriter, r *http.Request) {
		if b.onPromote != nil {
			b.onPromote(w, r)
			return
		}
		notConfigured("onPromote")(w, r)
	})
	// GET and POST on the identical /tokens path are separate Go 1.22+
	// method-prefixed registrations (create is POST; list — see
	// runAppsTokenList's doc comment for why this route does not exist on
	// the real server yet — is GET), mirroring how PUT/GET/DELETE already
	// coexist on plain /apps/{name} above.
	mux.HandleFunc("POST /api/v1/projects/{pid}/apps/{name}/tokens", func(w http.ResponseWriter, r *http.Request) {
		if b.onCreateToken != nil {
			b.onCreateToken(w, r)
			return
		}
		notConfigured("onCreateToken")(w, r)
	})
	mux.HandleFunc("GET /api/v1/projects/{pid}/apps/{name}/tokens", func(w http.ResponseWriter, r *http.Request) {
		if b.onListTokens != nil {
			b.onListTokens(w, r)
			return
		}
		notConfigured("onListTokens")(w, r)
	})
	mux.HandleFunc("DELETE /api/v1/projects/{pid}/apps/{name}/tokens/{token_id}", func(w http.ResponseWriter, r *http.Request) {
		if b.onDeleteToken != nil {
			b.onDeleteToken(w, r)
			return
		}
		notConfigured("onDeleteToken")(w, r)
	})
	return httptest.NewServer(mux)
}

// writeTempBundle writes n bytes (sparse — content is irrelevant to these
// tests except the base64 round-trip one) to a temp file and returns its
// path.
func writeTempBundle(t *testing.T, content []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "bundle.js")
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	return p
}

func TestRunAppsPut_UploadsBase64AndPrintsResult(t *testing.T) {
	const bundleSrc = "export async function render(ctx){return {outputDoc:1,title:'t',blocks:[]}}"
	tests := []struct {
		name   string
		asJSON bool
	}{{"json", true}, {"text", false}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotPID, gotName, gotAuth, gotContentType string
			var gotBundleB64 string

			srv := newAppsFakeServer(t, appsFakeBehaviour{
				onPut: func(w http.ResponseWriter, r *http.Request) {
					gotPID = r.PathValue("pid")
					gotName = r.PathValue("name")
					gotAuth = r.Header.Get("Authorization")
					gotContentType = r.Header.Get("Content-Type")
					var body struct {
						BundleBase64 string `json:"bundle_base64"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						http.Error(w, "bad body: "+err.Error(), http.StatusBadRequest)
						return
					}
					gotBundleB64 = body.BundleBase64
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"app_id":"app-123","version_hash":"deadbeef"}`))
				},
			})
			defer srv.Close()
			setHeadlessEnv(t, srv.URL)

			bundlePath := writeTempBundle(t, []byte(bundleSrc))

			args := []string{"sales-overview", "--project", "proj1", "--bundle", bundlePath}
			if tc.asJSON {
				args = append(args, "--json")
			}
			var runErr error
			out := captureStdout(t, func() {
				runErr = runAppsPut(args)
			})
			if runErr != nil {
				t.Fatalf("runAppsPut: %v", runErr)
			}

			if gotPID != "proj1" {
				t.Errorf("pid = %q, want proj1", gotPID)
			}
			if gotName != "sales-overview" {
				t.Errorf("name = %q, want sales-overview", gotName)
			}
			if gotAuth != "Bearer headless-token" {
				t.Errorf("auth = %q", gotAuth)
			}
			if gotContentType != "application/json" {
				t.Errorf("content-type = %q, want application/json", gotContentType)
			}
			decoded, err := base64.StdEncoding.DecodeString(gotBundleB64)
			if err != nil {
				t.Fatalf("bundle_base64 did not decode: %v", err)
			}
			if string(decoded) != bundleSrc {
				t.Errorf("decoded bundle = %q, want %q", decoded, bundleSrc)
			}

			if !tc.asJSON {
				if !strings.Contains(out, "app-123") || !strings.Contains(out, "deadbeef") {
					t.Errorf("text output missing app_id/version_hash:\n%s", out)
				}
				return
			}
			var resp appPutResponseJSON
			if err := json.Unmarshal([]byte(out), &resp); err != nil {
				t.Fatalf("--json output is not valid JSON: %v\noutput: %s", err, out)
			}
			if resp.AppID != "app-123" || resp.VersionHash != "deadbeef" {
				t.Errorf("decoded response = %+v, unexpected", resp)
			}
		})
	}
}

// TestRunAppsPut_BundleTooLargeErrorsLocallyBeforeUpload proves the 5 MB raw
// cap (spec §7/§4) is enforced BEFORE any network I/O: --remote points at a
// port nothing listens on, so any attempt to actually send the request would
// surface as a connection error, not this size error.
func TestRunAppsPut_BundleTooLargeErrorsLocallyBeforeUpload(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bundle.js")
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create bundle: %v", err)
	}
	if err := f.Truncate(5*1024*1024 + 1); err != nil {
		t.Fatalf("truncate bundle: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close bundle: %v", err)
	}

	err = runAppsPut([]string{"sales-overview", "--remote", "http://127.0.0.1:1", "--project", "proj1", "--bundle", p})
	if err == nil {
		t.Fatal("expected a local size-limit error, got nil")
	}
	if strings.Contains(err.Error(), "connection") || strings.Contains(err.Error(), "refused") {
		t.Errorf("error looks like it came from a network call, not the local size check: %v", err)
	}
	if !strings.Contains(err.Error(), "5") || !strings.Contains(strings.ToLower(err.Error()), "exceed") {
		t.Errorf("error should mention the 5 MB limit: %v", err)
	}
}

func TestRunAppsGet_DecodesDetailShape(t *testing.T) {
	const fixture = `{
	  "app_id": "app-123",
	  "name": "sales-overview",
	  "created_at": "2026-07-31T00:00:00Z",
	  "channels": {"draft": {"version_hash": "deadbeef", "updated_at": "2026-07-31T00:01:00Z"}},
	  "versions": [{"hash": "deadbeef", "size_bytes": 42, "created_at": "2026-07-31T00:01:00Z"}]
	}`
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onGet: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(fixture))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runAppsGet([]string{"sales-overview", "--project", "proj1", "--json"})
	})
	if runErr != nil {
		t.Fatalf("runAppsGet: %v", runErr)
	}
	var detail appJSON
	if err := json.Unmarshal([]byte(out), &detail); err != nil {
		t.Fatalf("--json output is not valid JSON: %v", err)
	}
	if detail.Name != "sales-overview" || detail.AppID != "app-123" {
		t.Errorf("decoded = %+v, unexpected", detail)
	}
	if len(detail.Versions) != 1 || detail.Versions[0].Hash != "deadbeef" {
		t.Errorf("versions = %+v, unexpected", detail.Versions)
	}

	// Text mode: human-readable, but must still surface the full version
	// hash (needed verbatim for a later `promote --version`).
	out = captureStdout(t, func() {
		runErr = runAppsGet([]string{"sales-overview", "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runAppsGet (text): %v", runErr)
	}
	for _, want := range []string{"sales-overview", "app-123", "draft", "deadbeef"} {
		if !strings.Contains(out, want) {
			t.Errorf("text output missing %q:\n%s", want, out)
		}
	}
}

func TestRunAppsList_JSONPassthroughAndTable(t *testing.T) {
	const fixture = `[
	  {"app_id":"app-1","name":"sales-overview","created_at":"2026-07-31T00:00:00Z","channels":{"draft":{"version_hash":"deadbeef","updated_at":"2026-07-31T00:01:00Z"}}},
	  {"app_id":"app-2","name":"ops-health","created_at":"2026-07-30T00:00:00Z","channels":{}}
	]`
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onList: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(fixture))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runAppsList([]string{"--project", "proj1", "--json"})
	})
	if runErr != nil {
		t.Fatalf("runAppsList --json: %v", runErr)
	}
	var items []appJSON
	if err := json.Unmarshal([]byte(out), &items); err != nil {
		t.Fatalf("--json output is not valid JSON: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}

	out = captureStdout(t, func() {
		runErr = runAppsList([]string{"--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runAppsList: %v", runErr)
	}
	for _, want := range []string{"sales-overview", "ops-health"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}
}

func TestRunAppsDelete_Success(t *testing.T) {
	var gotMethod, gotName string
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onDelete: func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotName = r.PathValue("name")
			w.WriteHeader(http.StatusNoContent)
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	if err := runAppsDelete([]string{"sales-overview", "--project", "proj1"}); err != nil {
		t.Fatalf("runAppsDelete: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("method = %s, want DELETE", gotMethod)
	}
	if gotName != "sales-overview" {
		t.Errorf("name = %q, want sales-overview", gotName)
	}
}

// --- app-name validation (C1 gate fix round, Major finding) ---
//
// url.PathEscape does NOT encode ".": url.PathEscape("..") == "..". An
// unvalidated name reaching appsURL could therefore build a path like
// /api/v1/projects/{pid}/apps/.. that a ServeMux/proxy/intermediary
// canonicalizes to a different endpoint before any handler sees it. Every
// named command (put/get/delete) must reject a bad name LOCALLY — before
// loadRemoteArgs or any network call — using the server's own regex
// (^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$).
//
// Each case below points --remote at a port nothing listens on (matching
// the 5 MB cap test's technique): a rejection that reaches the network
// layer surfaces as a connection error, not this validation error, so the
// assertions distinguish the two rather than just checking "err != nil".

var invalidAppNames = []string{
	"..", "a/b", "x@draft", "a?b", "-leading-hyphen", "trailing-hyphen-",
}

// assertLocalRejection asserts err is a local (pre-network) rejection. Callers
// point --remote at a port nothing listens on, so a rejection that reached the
// network layer would surface as a connection error instead.
func assertLocalRejection(t *testing.T, err error, wantContains string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a local rejection, got nil")
	}
	if strings.Contains(err.Error(), "refused") || strings.Contains(err.Error(), "connection") {
		t.Errorf("error looks like it came from a network call, not a local rejection: %v", err)
	}
	if wantContains != "" && !strings.Contains(err.Error(), wantContains) {
		t.Errorf("error should contain %q: %v", wantContains, err)
	}
}

// TestApps_InvalidNameOrTokenIDRejectedLocally is the wiring table: every
// named command must reject a bad name (".." — the shape url.PathEscape does
// not neutralize) locally, and token delete its malformed token_id via its OWN
// validator (validateTokenID, never validateAppName). The full name/ID grammar
// is pinned by TestValidateAppName / TestValidateTokenID.
func TestApps_InvalidNameOrTokenIDRejectedLocally(t *testing.T) {
	const remote = "http://127.0.0.1:1"
	bundlePath := writeTempBundle(t, []byte("export async function render(ctx){}"))
	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{"get", func() error { return runAppsGet([]string{"..", "--remote", remote, "--project", "proj1"}) }, "invalid app name"},
		{"delete", func() error { return runAppsDelete([]string{"..", "--remote", remote, "--project", "proj1"}) }, "invalid app name"},
		{"put", func() error {
			return runAppsPut([]string{"..", "--remote", remote, "--project", "proj1", "--bundle", bundlePath})
		}, "invalid app name"},
		{"render", func() error {
			return runAppsRender([]string{"..", "--remote", remote, "--project", "proj1", "--channel", "draft"})
		}, "invalid app name"},
		{"promote", func() error {
			return runAppsPromote([]string{"..", "--remote", remote, "--project", "proj1", "--version", testVersionHash})
		}, "invalid app name"},
		{"token create", func() error { return runAppsTokenCreate([]string{"..", "--remote", remote, "--project", "proj1"}) }, "invalid app name"},
		{"token delete malformed token id", func() error {
			return runAppsTokenDelete([]string{"sales-overview", "not-a-uuid", "--remote", remote, "--project", "proj1"})
		}, "invalid token id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertLocalRejection(t, tc.run(), tc.want)
		})
	}
}

// TestApps_UsageErrorsRejectedLocally covers arity / missing-required-flag /
// unknown-subcommand errors: all must fail locally, before any network call.
func TestApps_UsageErrorsRejectedLocally(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{"init no args", func() error { return runAppsInit(nil) }, ""},
		{"init more than one arg", func() error { return runAppsInit([]string{"a", "b"}) }, ""},
		{"put missing --bundle", func() error { return runAppsPut([]string{"sales-overview", "--project", "proj1"}) }, ""},
		{"list positional arg", func() error { return runAppsList([]string{"unexpected"}) }, ""},
		{"delete no name", func() error { return runAppsDelete([]string{"--project", "proj1"}) }, ""},
		{"delete more than one name", func() error { return runAppsDelete([]string{"a", "b", "--project", "proj1"}) }, ""},
		{"promote missing --version", func() error {
			return runAppsPromote([]string{"sales-overview", "--remote", "http://127.0.0.1:1", "--project", "proj1"})
		}, "--version"},
		{"token delete missing token_id", func() error { return runAppsTokenDelete([]string{"sales-overview", "--project", "proj1"}) }, ""},
		{"token delete no positional args", func() error { return runAppsTokenDelete([]string{"--project", "proj1"}) }, ""},
		{"token unknown subcommand", func() error { return runAppsToken([]string{"bogus", "sales-overview"}) }, ""},
		{"token no subcommand", func() error { return runAppsToken(nil) }, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertLocalRejection(t, tc.run(), tc.want)
		})
	}
}

// --- non-regular bundle files (C1 gate fix round, Minor finding) ---

// TestRunAppsPut_RejectsNonRegularBundleFile proves a non-regular path
// (a directory, standing in for a FIFO/device per the review's suggested
// fallback) is rejected before any network call, rather than being
// Stat()'d for a possibly-bogus size and silently let through.
func TestRunAppsPut_RejectsNonRegularBundleFile(t *testing.T) {
	dir := t.TempDir() // a directory is not a regular file
	err := runAppsPut([]string{"sales-overview", "--remote", "http://127.0.0.1:1", "--project", "proj1", "--bundle", dir})
	if err == nil {
		t.Fatal("expected an error for a non-regular bundle path, got nil")
	}
	if strings.Contains(err.Error(), "refused") || strings.Contains(err.Error(), "connection") {
		t.Errorf("error looks like it came from a network call, not the local regular-file check: %v", err)
	}
	if !strings.Contains(err.Error(), "not a regular file") {
		t.Errorf("error should say the bundle path is not a regular file: %v", err)
	}
}

// --- project resolution: every network subcommand reuses loadRemoteArgs's
// deterministic error verbatim (no bespoke reimplementation) ---

// TestRunApps_ProjectResolutionErrorNamesRemedies proves that when the
// project cannot be resolved (here: multiple projects on disk, no --project
// and no $DATUPLET_PROJECT to disambiguate), the apps commands surface
// loadRemoteArgs/resolveProject's own deterministic error rather than a
// generic failure — it must name the --project remedy and list the
// available projects, exactly like every other project-scoped subcommand.
func TestRunApps_ProjectResolutionErrorNamesRemedies(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "2099-01-01T00:00:00Z",
		PipelineAPIURL: "http://api",
		Projects: []clusterMetaProject{
			{ID: "p-alpha", Name: "alpha", LakekeeperProjectID: "lk-alpha"},
			{ID: "p-beta", Name: "beta", LakekeeperProjectID: "lk-beta"},
		},
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	err := runAppsList([]string{"--remote", "http://api"})
	if err == nil {
		t.Fatal("expected a project-resolution error, got nil")
	}
	if !strings.Contains(err.Error(), "--project") {
		t.Errorf("error should name the --project remedy: %v", err)
	}
	if !strings.Contains(err.Error(), "available:") ||
		!strings.Contains(err.Error(), "alpha") || !strings.Contains(err.Error(), "beta") {
		t.Errorf("error should list the available projects: %v", err)
	}
}

// TestRunApps_NoRemoteResolvesErrors proves the same reuse for the
// remote/api-token leg of the chain (no cluster.json, no env, no flags): the
// error must point at `datuplet login`, matching every other subcommand.
func TestRunApps_NoRemoteResolvesErrors(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	err := runAppsList(nil)
	if err == nil {
		t.Fatal("expected an error when nothing resolves, got nil")
	}
	if !strings.Contains(err.Error(), "datuplet login") {
		t.Errorf("error should point at `datuplet login`: %v", err)
	}
}

// --- apps render (the agent's test step: implement → put → render → assert) ---

const renderDocFixture = `{"outputDoc":1,"title":"Sales overview","blocks":[]}`

// renderLogFixture is a renderLogJSON record (the shape
// pkg/pipelineapi/apps.renderLogToJSON emits). request_id ties it to the
// render envelope below.
const renderLogFixture = `{"request_id":"req-1","app_id":"app-1","version_hash":"deadbeef","channel":"draft","principal_kind":"platform_user","principal_id":"user-1","started_at":"2026-07-31T00:00:00Z","duration_ms":42,"outcome":"render_error","log_text":"TypeError: cannot read x","error":"the app threw"}`

// TestRunAppsRender_SendsBearerAcceptAndPrintsDoc is the core §5.5 success
// path: `render --channel draft --param days=7 --json` must hit
// `/apps/{pid}/{name}@draft?days=7` with the author's bearer credential and
// `Accept: application/json`, and print the OutputDoc verbatim on 200. The
// default (and explicit production) channel targets the bare route, never
// `@draft`.
func TestRunAppsRender_SendsBearerAcceptAndPrintsDoc(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantPath string
		wantDays string
		asJSON   bool
	}{
		{
			name:     "draft with param, json",
			args:     []string{"sales-overview", "--project", "proj1", "--channel", "draft", "--param", "days=7", "--json"},
			wantPath: "/apps/proj1/sales-overview@draft", // literal @draft, not %40
			wantDays: "7",
			asJSON:   true,
		},
		{
			name:     "channel omitted is production",
			args:     []string{"sales-overview", "--project", "proj1"},
			wantPath: "/apps/proj1/sales-overview", // no @draft
		},
		{
			name:     "explicit production",
			args:     []string{"sales-overview", "--project", "proj1", "--channel", "production"},
			wantPath: "/apps/proj1/sales-overview", // no @draft
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotAuth, gotAccept, gotDays string
			srv := newAppsFakeServer(t, appsFakeBehaviour{
				onRender: func(w http.ResponseWriter, r *http.Request) {
					gotPath = r.URL.Path
					gotAuth = r.Header.Get("Authorization")
					gotAccept = r.Header.Get("Accept")
					gotDays = r.URL.Query().Get("days")
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(renderDocFixture))
				},
			})
			defer srv.Close()
			setHeadlessEnv(t, srv.URL)

			var runErr error
			out := captureStdout(t, func() {
				runErr = runAppsRender(tc.args)
			})
			if runErr != nil {
				t.Fatalf("runAppsRender(%v): %v", tc.args, runErr)
			}
			if gotPath != tc.wantPath {
				t.Errorf("path = %q, want %q", gotPath, tc.wantPath)
			}
			if gotAuth != "Bearer headless-token" {
				t.Errorf("auth = %q, want Bearer headless-token", gotAuth)
			}
			if gotAccept != "application/json" {
				t.Errorf("accept = %q, want application/json", gotAccept)
			}
			if gotDays != tc.wantDays {
				t.Errorf("days param = %q, want %q", gotDays, tc.wantDays)
			}
			if !tc.asJSON {
				return
			}
			// Success prints the OutputDoc verbatim — the agent asserts on this.
			var doc map[string]any
			if err := json.Unmarshal([]byte(out), &doc); err != nil {
				t.Fatalf("stdout is not the OutputDoc JSON: %v\noutput: %s", err, out)
			}
			if doc["title"] != "Sales overview" {
				t.Errorf("doc title = %v, want Sales overview", doc["title"])
			}
		})
	}
}

// TestRunAppsRender_ErrorEnvelopeFetchesAuthorLogAndPrintsOneObject is the
// §5.5 failure contract: a non-200 error envelope becomes exactly ONE
// machine-readable object {error, kind, request_id, author_log}, with the
// author log fetched via logs?request_id=<id>, and exit code 1 (user-error
// class — the render reached the service and reported a failure).
func TestRunAppsRender_ErrorEnvelopeFetchesAuthorLogAndPrintsOneObject(t *testing.T) {
	var gotLogsRequestID string
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onRender: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"render failed","kind":"render_error","request_id":"req-1"}`))
		},
		onLogs: func(w http.ResponseWriter, r *http.Request) {
			gotLogsRequestID = r.URL.Query().Get("request_id")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(renderLogFixture))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runAppsRender([]string{"sales-overview", "--project", "proj1", "--channel", "draft", "--json"})
	})
	if code := exitCodeOf(runErr); code != 1 {
		t.Fatalf("exit code = %d, want 1 (render failure is user-error class): %v", code, runErr)
	}
	if gotLogsRequestID != "req-1" {
		t.Errorf("author-log fetch used request_id %q, want req-1", gotLogsRequestID)
	}
	// Exactly one object; the four required keys; author_log carries the record.
	var obj map[string]any
	dec := json.NewDecoder(strings.NewReader(out))
	if err := dec.Decode(&obj); err != nil {
		t.Fatalf("stdout is not one JSON object: %v\noutput: %s", err, out)
	}
	if dec.More() {
		t.Errorf("stdout has more than one JSON value (must be exactly ONE object):\n%s", out)
	}
	if obj["error"] != "render failed" || obj["kind"] != "render_error" || obj["request_id"] != "req-1" {
		t.Errorf("envelope fields wrong: %+v", obj)
	}
	al, ok := obj["author_log"].(map[string]any)
	if !ok {
		t.Fatalf("author_log should be the render-log object, got %T (%v)", obj["author_log"], obj["author_log"])
	}
	if al["log_text"] != "TypeError: cannot read x" {
		t.Errorf("author_log.log_text = %v, want the captured stack", al["log_text"])
	}
}

// TestRunAppsRender_AuthorLogLookupFailureLeavesNullObject proves author_log is
// JSON null (key present) when the log lookup fails, the stdout object shape
// stays {error,kind,request_id,author_log:null}, and the run still exits 1. A
// genuine 404 (aged out / never existed) stays silent — null is the expected
// signal — while a NON-404 failure also emits a one-line stderr diagnostic
// naming the request_id, so the agent can tell "log route broken" from "log
// aged out".
func TestRunAppsRender_AuthorLogLookupFailureLeavesNullObject(t *testing.T) {
	tests := []struct {
		name       string
		logsStatus int
		logsBody   string
		wantNote   bool
	}{
		{"logs 404 stays silent", http.StatusNotFound, `{"error":"no render log for that request_id"}`, false},
		{"logs 500 emits stderr note", http.StatusInternalServerError, `{"error":"boom"}`, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newAppsFakeServer(t, appsFakeBehaviour{
				onRender: func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"render failed","kind":"render_error","request_id":"req-1"}`))
				},
				onLogs: func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, tc.logsBody, tc.logsStatus)
				},
			})
			defer srv.Close()
			setHeadlessEnv(t, srv.URL)

			var runErr error
			stdout, stderr := captureStdoutAndStderr(t, func() {
				runErr = runAppsRender([]string{"sales-overview", "--project", "proj1", "--channel", "draft", "--json"})
			})
			if code := exitCodeOf(runErr); code != 1 {
				t.Fatalf("exit code = %d, want 1: %v", code, runErr)
			}
			var obj map[string]any
			if err := json.Unmarshal([]byte(stdout), &obj); err != nil {
				t.Fatalf("stdout is not one JSON object: %v\n%s", err, stdout)
			}
			if al, present := obj["author_log"]; !present || al != nil {
				t.Errorf("author_log must be present and null, got present=%v value=%v", present, al)
			}
			hasNote := strings.Contains(stderr, "could not fetch author log")
			if hasNote != tc.wantNote {
				t.Errorf("stderr diagnostic present = %v, want %v; stderr:\n%s", hasNote, tc.wantNote, stderr)
			}
			if tc.wantNote && !strings.Contains(stderr, "req-1") {
				t.Errorf("the diagnostic should name the request_id; stderr:\n%s", stderr)
			}
		})
	}
}

// TestRunAppsRender_TextModeFailureGoesToStderr proves text-mode failure emits
// the human-formatted fields + log excerpt on stderr (not the JSON object on
// stdout), and still exits 1.
func TestRunAppsRender_TextModeFailureGoesToStderr(t *testing.T) {
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onRender: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"render failed","kind":"render_error","request_id":"req-1"}`))
		},
		onLogs: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(renderLogFixture))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	stdout, stderr := captureStdoutAndStderr(t, func() {
		runErr = runAppsRender([]string{"sales-overview", "--project", "proj1", "--channel", "draft"})
	})
	if code := exitCodeOf(runErr); code != 1 {
		t.Fatalf("exit code = %d, want 1: %v", code, runErr)
	}
	// No machine-readable object on stdout in text mode.
	if strings.Contains(stdout, "author_log") || strings.Contains(stdout, "{") {
		t.Errorf("text-mode stdout should be empty of the JSON object, got:\n%s", stdout)
	}
	for _, want := range []string{"render_error", "req-1", "render failed", "TypeError: cannot read x"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("text-mode stderr should carry %q; stderr:\n%s", want, stderr)
		}
	}
}

// TestRunAppsRender_InvalidFlagsRejectedLocally proves a bad --channel value
// (rather than silently rendering production or building a malformed path) and
// a --param without '=' both error locally, before any network call.
func TestRunAppsRender_InvalidFlagsRejectedLocally(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"invalid channel", []string{"sales-overview", "--remote", "http://127.0.0.1:1", "--project", "proj1", "--channel", "prod"}, "channel"},
		{"invalid param", []string{"sales-overview", "--remote", "http://127.0.0.1:1", "--project", "proj1", "--channel", "draft", "--param", "noequalshere"}, "param"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertLocalRejection(t, runAppsRender(tc.args), tc.want)
		})
	}
}

// --- apps logs ---

func TestRunAppsLogs_ListJSONAndTable(t *testing.T) {
	const listFixture = `[
	  {"request_id":"req-2","app_id":"app-1","version_hash":"deadbeef","channel":"draft","principal_kind":"platform_user","principal_id":"user-1","started_at":"2026-07-31T00:02:00Z","duration_ms":12,"outcome":"ok","log_text":""},
	  {"request_id":"req-1","app_id":"app-1","version_hash":"deadbeef","channel":"draft","principal_kind":"platform_user","principal_id":"user-1","started_at":"2026-07-31T00:00:00Z","duration_ms":42,"outcome":"render_error","log_text":"boom","error":"the app threw"}
	]`
	var sawRequestID string
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onLogs: func(w http.ResponseWriter, r *http.Request) {
			sawRequestID = r.URL.Query().Get("request_id")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(listFixture))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runAppsLogs([]string{"sales-overview", "--project", "proj1", "--json"})
	})
	if runErr != nil {
		t.Fatalf("runAppsLogs --json: %v", runErr)
	}
	if sawRequestID != "" {
		t.Errorf("list mode must not send request_id, got %q", sawRequestID)
	}
	var items []map[string]any
	if err := json.Unmarshal([]byte(out), &items); err != nil {
		t.Fatalf("--json output is not a JSON array: %v\n%s", err, out)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}

	out = captureStdout(t, func() {
		runErr = runAppsLogs([]string{"sales-overview", "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runAppsLogs (text): %v", runErr)
	}
	for _, want := range []string{"req-2", "req-1", "ok", "render_error"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}
}

func TestRunAppsLogs_ByRequestIDPrintsOne(t *testing.T) {
	var sawRequestID string
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onLogs: func(w http.ResponseWriter, r *http.Request) {
			sawRequestID = r.URL.Query().Get("request_id")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(renderLogFixture))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runAppsLogs([]string{"sales-overview", "--project", "proj1", "--request-id", "req-1", "--json"})
	})
	if runErr != nil {
		t.Fatalf("runAppsLogs --request-id: %v", runErr)
	}
	if sawRequestID != "req-1" {
		t.Errorf("request_id query = %q, want req-1", sawRequestID)
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(out), &rec); err != nil {
		t.Fatalf("--json output is not a single JSON object: %v\n%s", err, out)
	}
	if rec["request_id"] != "req-1" {
		t.Errorf("record request_id = %v, want req-1", rec["request_id"])
	}
}

// --- C2 fix round: no --param value in render error text; author-log fetch
//     failure distinguishable from a 404 ---

// TestRunAppsRender_TransportErrorDoesNotLeakParamValues pins RFC 028 §9's
// invariant that request URLs / params carrying secrets must not be logged.
// The CLI is the agent-facing surface where a credential may arrive as a
// filter --param, so a connection-refused transport error must NOT echo the
// query string (which the raw request URL and the *url.Error from client.Do
// both embed) into the returned error or anything printed. Exit stays 20.
func TestRunAppsRender_TransportErrorDoesNotLeakParamValues(t *testing.T) {
	setHeadlessEnv(t, "http://127.0.0.1:1") // nothing listens here
	var err error
	stdout, stderr := captureStdoutAndStderr(t, func() {
		err = runAppsRender([]string{"sales-overview", "--project", "proj1", "--channel", "draft", "--param", "secret=SHIBBOLETH"})
	})
	if code := exitCodeOf(err); code != 20 {
		t.Fatalf("exit code = %d, want 20 (transport failure): %v", code, err)
	}
	for label, s := range map[string]string{"error": err.Error(), "stdout": stdout, "stderr": stderr} {
		if strings.Contains(s, "SHIBBOLETH") {
			t.Errorf("%s leaked the --param VALUE: %q", label, s)
		}
		if strings.Contains(s, "secret=") {
			t.Errorf("%s leaked the --param key=value: %q", label, s)
		}
	}
}

// TestRunAppsRender_NonEnvelopeBodyIsNotEchoed (C2 fix 2) closes the residual
// leak: the non-envelope branch fires when the response is NOT app-worker's
// envelope — i.e. an intermediary (ingress/proxy/LB) error page, which often
// reflects the requested URI (query included) back into arbitrary HTML.
// Echoing that body would re-open the --param leak the rest of the fix closed,
// so the CLI must drop the untrusted body entirely and print only the status +
// the already-redacted path.
func TestRunAppsRender_NonEnvelopeBodyIsNotEchoed(t *testing.T) {
	const reflected = "The requested URL /apps/p1/app?secret=SHIBBOLETH failed"
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onRender: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("<html><body>" + reflected + "</body></html>"))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var err error
	stdout, stderr := captureStdoutAndStderr(t, func() {
		err = runAppsRender([]string{"app", "--project", "p1", "--channel", "draft", "--param", "secret=SHIBBOLETH"})
	})
	if code := exitCodeOf(err); code != 20 {
		t.Fatalf("exit code = %d, want 20: %v", code, err)
	}
	for label, s := range map[string]string{"error": err.Error(), "stdout": stdout, "stderr": stderr} {
		if strings.Contains(s, "SHIBBOLETH") {
			t.Errorf("%s leaked the reflected --param value: %q", label, s)
		}
		if strings.Contains(s, "secret=") {
			t.Errorf("%s leaked secret=: %q", label, s)
		}
		if strings.Contains(s, "The requested URL") {
			t.Errorf("%s echoed the untrusted proxy body: %q", label, s)
		}
	}
	// Still actionable: names the status and the redacted (query-free) path.
	if !strings.Contains(err.Error(), "502") {
		t.Errorf("error should name the HTTP status 502: %v", err)
	}
	if !strings.Contains(err.Error(), "/apps/p1/app") {
		t.Errorf("error should name the redacted request path: %v", err)
	}
}

// --- shared fixtures: promote/token (C3) ---

// testVersionHash/testOldVersionHash are well-formed 64-hex-char content
// hashes (the app_versions.hash shape: hex.EncodeToString(sha256.Sum256(...)),
// store.go's PutVersion) — built via strings.Repeat so their length is
// self-evidently correct rather than hand-counted.
var (
	testVersionHash    = strings.Repeat("ab12cd34", 8) // 64 hex chars
	testOldVersionHash = strings.Repeat("98fe76dc", 8) // 64 hex chars, distinct from testVersionHash
)

const (
	testTokenID                = "550e8400-e29b-41d4-a716-446655440000"
	testTokenSecret            = "vw_" + testTokenID + ".SUPER-SECRET-VALUE-DO-NOT-LOG"
	tokenCreateResponseFixture = `{"token_id":"` + testTokenID + `","token":"` + testTokenSecret + `"}`
	// tokenListFixture is deliberately "leaky": each entry carries a sensitive
	// field the CLI must never echo.
	tokenListFixture = `[{"token_id":"` + testTokenID + `","created_at":"2026-07-31T00:00:00Z","revoked_at":null,"secret_hash":"deadbeefdeadbeef"},` +
		`{"token_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","created_at":"2026-07-30T00:00:00Z","revoked_at":"2026-07-30T12:00:00Z","secret_hash":"deadbeefdeadbeef"}]`
)

// --- validateVersionHash / validateTokenID (pure) ---

func TestValidateVersionHash(t *testing.T) {
	if len(testVersionHash) != 64 {
		t.Fatalf("test fixture bug: testVersionHash is %d chars, want 64", len(testVersionHash))
	}
	if got, err := validateVersionHash(testVersionHash); err != nil || got != testVersionHash {
		t.Errorf("validateVersionHash(%q) = (%q, %v), want (%q, nil)", testVersionHash, got, err, testVersionHash)
	}
	// Upper-case hex is accepted but normalized to lower-case (matching the
	// server's hex.EncodeToString output byte-for-byte, since app_versions.hash
	// is compared with plain string equality).
	upper := strings.ToUpper(testVersionHash)
	if got, err := validateVersionHash(upper); err != nil || got != testVersionHash {
		t.Errorf("validateVersionHash(%q) = (%q, %v), want normalized to %q", upper, got, err, testVersionHash)
	}

	invalid := []string{"", "deadbeef", testVersionHash + "0", strings.Repeat("g", 64), "../../etc/passwd", "not-a-hash"}
	for _, h := range invalid {
		if _, err := validateVersionHash(h); err == nil {
			t.Errorf("validateVersionHash(%q) = nil error, want an error", h)
		}
	}
}

func TestValidateTokenID(t *testing.T) {
	got, err := validateTokenID(testTokenID)
	if err != nil {
		t.Fatalf("validateTokenID(%q): %v", testTokenID, err)
	}
	if got != testTokenID {
		t.Errorf("validateTokenID(%q) = %q, want unchanged (already canonical)", testTokenID, got)
	}

	upper := strings.ToUpper(testTokenID)
	if got, err := validateTokenID(upper); err != nil || got != testTokenID {
		t.Errorf("validateTokenID(%q) = (%q, %v), want (%q, nil) — canonicalized to lower-case", upper, got, err, testTokenID)
	}

	for _, bad := range []string{"", "not-a-uuid", "sales-overview", "..", "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"} {
		if _, err := validateTokenID(bad); err == nil {
			t.Errorf("validateTokenID(%q) = nil error, want an error", bad)
		}
	}
}

// --- apps promote (CAS-repoint production, spec §5.1) ---

// TestRunAppsPromote_RequestShapeAndOutput proves the request shape: on a first
// promote expectedProduction is OMITTED (not sent as an empty string); the CAS
// precondition round-trips into the body when --expected-production is passed;
// and the server's production_version is surfaced in both text and --json mode.
func TestRunAppsPromote_RequestShapeAndOutput(t *testing.T) {
	tests := []struct {
		name         string
		extraArgs    []string
		wantExpected string // "" means the field must be omitted
		asJSON       bool
	}{
		{"first promote omits expectedProduction (text)", nil, "", false},
		{"sends expectedProduction when provided", []string{"--expected-production", testOldVersionHash}, testOldVersionHash, false},
		{"json passes through server body", []string{"--json"}, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotBody map[string]any
			srv := newAppsFakeServer(t, appsFakeBehaviour{
				onPromote: func(w http.ResponseWriter, r *http.Request) {
					if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
						http.Error(w, "bad body: "+err.Error(), http.StatusBadRequest)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"production_version":"` + testVersionHash + `"}`))
				},
			})
			defer srv.Close()
			setHeadlessEnv(t, srv.URL)

			args := append([]string{"sales-overview", "--project", "proj1", "--version", testVersionHash}, tc.extraArgs...)
			var runErr error
			out := captureStdout(t, func() {
				runErr = runAppsPromote(args)
			})
			if runErr != nil {
				t.Fatalf("runAppsPromote: %v", runErr)
			}
			if gotBody["version"] != testVersionHash {
				t.Errorf("body version = %v, want %s", gotBody["version"], testVersionHash)
			}
			if tc.wantExpected == "" {
				if _, present := gotBody["expectedProduction"]; present {
					t.Errorf("expectedProduction should be OMITTED from the body, got %v", gotBody)
				}
			} else if gotBody["expectedProduction"] != tc.wantExpected {
				t.Errorf("body expectedProduction = %v, want %s", gotBody["expectedProduction"], tc.wantExpected)
			}
			if !tc.asJSON {
				if !strings.Contains(out, testVersionHash) {
					t.Errorf("text output should show the new production version:\n%s", out)
				}
				return
			}
			var decoded appsPromoteResponseJSON
			if err := json.Unmarshal([]byte(out), &decoded); err != nil {
				t.Fatalf("--json output is not valid JSON: %v\noutput: %s", err, out)
			}
			if decoded.ProductionVersion != testVersionHash {
				t.Errorf("production_version = %q, want %s", decoded.ProductionVersion, testVersionHash)
			}
		})
	}
}

// TestRunAppsPromote_CASConflictExitsDistinctCodeWithClearMessage is the core
// §5.1/§5.5 contract: a 409 must produce a message that clearly explains
// someone promoted a different version meanwhile and to re-fetch/retry, AND a
// distinct, non-zero exit code the agent loop can branch on without parsing
// text — never the plain default exit 1 every other local/user error uses,
// and never render's transport-class exit 20 (promote never talks to
// app-worker).
func TestRunAppsPromote_CASConflictExitsDistinctCodeWithClearMessage(t *testing.T) {
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onPromote: func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"production has moved since expectedProduction was read; re-read the app and retry"}`, http.StatusConflict)
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	err := runAppsPromote([]string{"sales-overview", "--project", "proj1",
		"--version", testVersionHash, "--expected-production", testOldVersionHash})
	if err == nil {
		t.Fatal("expected an error for a 409 CAS conflict, got nil")
	}
	if code := exitCodeOf(err); code != appsPromoteCASConflictExitCode {
		t.Errorf("exit code = %d, want %d (the CAS-conflict code, distinct from 0/1/20)", code, appsPromoteCASConflictExitCode)
	}
	lower := strings.ToLower(err.Error())
	if !strings.Contains(lower, "promot") {
		t.Errorf("error should clearly say a promote conflict occurred: %v", err)
	}
	if !strings.Contains(lower, "retry") {
		t.Errorf("error should tell the caller to retry: %v", err)
	}
}

// TestRunAppsPromote_MalformedHashRejectedLocally proves a malformed --version
// or --expected-production value is rejected LOCALLY (before any network
// call) — the "basic shape check", not full server-side validation.
func TestRunAppsPromote_MalformedHashRejectedLocally(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"version not-a-hash", []string{"--version", "not-a-hash"}},
		{"version too short", []string{"--version", "deadbeef"}},
		{"version non-hex", []string{"--version", strings.Repeat("g", 64)}},
		{"expected-production not-a-hash", []string{"--version", testVersionHash, "--expected-production", "not-a-hash"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"sales-overview", "--remote", "http://127.0.0.1:1", "--project", "proj1"}, tc.args...)
			assertLocalRejection(t, runAppsPromote(args), "invalid version hash")
		})
	}
}

// --- apps token create/list/delete (viewer-token lifecycle, spec §5.3) ---

// TestRunAppsTokenCreate_PrintsSecretExactlyOnceWithStoreNote is the core
// §5.3/§5.5 contract: the plaintext secret is shown EXACTLY once, ever,
// together with a note that it will not be shown again — in both --json and
// text mode.
func TestRunAppsTokenCreate_PrintsSecretExactlyOnceWithStoreNote(t *testing.T) {
	modes := []struct {
		name   string
		asJSON bool
	}{{"text", false}, {"json", true}}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			var gotMethod, gotPath string
			srv := newAppsFakeServer(t, appsFakeBehaviour{
				onCreateToken: func(w http.ResponseWriter, r *http.Request) {
					gotMethod = r.Method
					gotPath = r.URL.Path
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte(tokenCreateResponseFixture))
				},
			})
			defer srv.Close()
			setHeadlessEnv(t, srv.URL)

			args := []string{"sales-overview", "--project", "proj1"}
			if m.asJSON {
				args = append(args, "--json")
			}
			var runErr error
			stdout, stderr := captureStdoutAndStderr(t, func() {
				runErr = runAppsTokenCreate(args)
			})
			if runErr != nil {
				t.Fatalf("runAppsTokenCreate: %v", runErr)
			}
			if gotMethod != http.MethodPost {
				t.Errorf("method = %s, want POST", gotMethod)
			}
			if gotPath != "/api/v1/projects/proj1/apps/sales-overview/tokens" {
				t.Errorf("path = %q", gotPath)
			}

			combined := stdout + stderr
			if n := strings.Count(combined, testTokenSecret); n != 1 {
				t.Errorf("secret appeared %d times across stdout+stderr, want exactly 1:\nstdout:\n%s\nstderr:\n%s", n, stdout, stderr)
			}
			lowerStderr := strings.ToLower(stderr)
			if !strings.Contains(lowerStderr, "store") {
				t.Errorf("stderr should say to store the token now: %q", stderr)
			}
			if !strings.Contains(lowerStderr, "shown again") {
				t.Errorf("stderr should say it will not be shown again: %q", stderr)
			}

			if m.asJSON {
				var decoded appTokenCreateResponseJSON
				if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
					t.Fatalf("--json stdout is not valid JSON: %v\nstdout: %s", err, stdout)
				}
				if decoded.TokenID != testTokenID || decoded.Token != testTokenSecret {
					t.Errorf("decoded = %+v, unexpected", decoded)
				}
			} else if strings.TrimSpace(stdout) != testTokenSecret {
				t.Errorf("text-mode stdout = %q, want exactly the token value", stdout)
			}
		})
	}
}

// TestRunAppsTokenList_ShowsIDsNeverSecrets is the core "never prints a
// secret" contract: --json DECODES the server response into
// appTokenSummaryJSON and RE-ENCODES that (rather than printing the raw body
// verbatim), so an unexpected sensitive field in the response ("secret_hash"
// in the fixture) can never reach the output in either mode —
// appTokenSummaryJSON has no field for it to decode into.
func TestRunAppsTokenList_ShowsIDsNeverSecrets(t *testing.T) {
	var gotMethod, gotPath string
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onListTokens: func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(tokenListFixture))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runAppsTokenList([]string{"sales-overview", "--project", "proj1", "--json"})
	})
	if runErr != nil {
		t.Fatalf("runAppsTokenList --json: %v", runErr)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("method = %s, want GET", gotMethod)
	}
	if gotPath != "/api/v1/projects/proj1/apps/sales-overview/tokens" {
		t.Errorf("path = %q", gotPath)
	}
	if strings.Contains(out, "vw_") || strings.Contains(out, "secret") || strings.Contains(out, "deadbeef") {
		t.Fatalf("--json output must project through appTokenSummaryJSON (decode+re-encode), never carry a secret; got:\n%s", out)
	}
	var items []appTokenSummaryJSON
	if err := json.Unmarshal([]byte(out), &items); err != nil {
		t.Fatalf("--json output is not a JSON array of appTokenSummaryJSON: %v\n%s", err, out)
	}
	if len(items) != 2 || items[0].TokenID != testTokenID {
		t.Fatalf("decoded items = %+v, want 2 entries, first with token_id %s", items, testTokenID)
	}

	out = captureStdout(t, func() {
		runErr = runAppsTokenList([]string{"sales-overview", "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runAppsTokenList (text): %v", runErr)
	}
	if !strings.Contains(out, testTokenID) {
		t.Errorf("table output missing the token id:\n%s", out)
	}
	if strings.Contains(out, "vw_") || strings.Contains(out, "secret") || strings.Contains(out, "deadbeef") {
		t.Errorf("table output must never contain a secret:\n%s", out)
	}
}

func TestRunAppsTokenDelete_CallsDeleteWithID(t *testing.T) {
	var gotMethod, gotName, gotTokenID string
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onDeleteToken: func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotName = r.PathValue("name")
			gotTokenID = r.PathValue("token_id")
			w.WriteHeader(http.StatusNoContent)
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	if err := runAppsTokenDelete([]string{"sales-overview", testTokenID, "--project", "proj1"}); err != nil {
		t.Fatalf("runAppsTokenDelete: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("method = %s, want DELETE", gotMethod)
	}
	if gotName != "sales-overview" {
		t.Errorf("name = %q, want sales-overview", gotName)
	}
	if gotTokenID != testTokenID {
		t.Errorf("token_id = %q, want %s", gotTokenID, testTokenID)
	}
}

// --- 401/403: doAuthedRequest has no special case for either (confirmed by
// inspection — it returns the raw status/body to the caller), so the only
// thing to prove is that the shared generic non-2xx branch each new
// subcommand already has produces a clear, non-nil, non-zero-exit error
// rather than a panic or a false success. ---

// TestApps_HTTPErrorStatusesSurfaceAsExit1 proves each subcommand's shared
// non-2xx branch turns a 400/401/403/404 into a non-nil error with the plain
// user-error exit code 1 — never a panic, a false success, the 409 CAS-conflict
// code or render's transport-class 20. (doAuthedRequest has no special case
// for 401/403: it returns the raw status/body to the caller.)
func TestApps_HTTPErrorStatusesSurfaceAsExit1(t *testing.T) {
	tests := []struct {
		name   string
		status int
		run    func() error
	}{
		{"get/404", http.StatusNotFound, func() error { return runAppsGet([]string{"nope", "--project", "proj1"}) }},
		{"delete/404", http.StatusNotFound, func() error { return runAppsDelete([]string{"nope", "--project", "proj1"}) }},
		{"logs-by-request-id/404", http.StatusNotFound, func() error {
			return runAppsLogs([]string{"sales-overview", "--project", "proj1", "--request-id", "missing"})
		}},
		{"promote/404", http.StatusNotFound, func() error {
			return runAppsPromote([]string{"nope", "--project", "proj1", "--version", testVersionHash})
		}},
		// A hash that never existed for this app is a plain user error, DISTINCT
		// from the 409 CAS-conflict's own exit code.
		{"promote-unknown-version/400", http.StatusBadRequest, func() error {
			return runAppsPromote([]string{"sales-overview", "--project", "proj1", "--version", testVersionHash})
		}},
		{"token-create/404", http.StatusNotFound, func() error { return runAppsTokenCreate([]string{"nope", "--project", "proj1"}) }},
		{"token-delete/404", http.StatusNotFound, func() error {
			return runAppsTokenDelete([]string{"sales-overview", testTokenID, "--project", "proj1"})
		}},
		{"promote/401", http.StatusUnauthorized, func() error {
			return runAppsPromote([]string{"sales-overview", "--project", "proj1", "--version", testVersionHash})
		}},
		{"token-create/403", http.StatusForbidden, func() error { return runAppsTokenCreate([]string{"sales-overview", "--project", "proj1"}) }},
		{"token-list/401", http.StatusUnauthorized, func() error { return runAppsTokenList([]string{"sales-overview", "--project", "proj1"}) }},
		{"token-delete/403", http.StatusForbidden, func() error {
			return runAppsTokenDelete([]string{"sales-overview", testTokenID, "--project", "proj1"})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deny := func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, `{"error":"denied"}`, tc.status)
			}
			srv := newAppsFakeServer(t, appsFakeBehaviour{
				onGet:         deny,
				onDelete:      deny,
				onLogs:        deny,
				onPromote:     deny,
				onCreateToken: deny,
				onListTokens:  deny,
				onDeleteToken: deny,
			})
			defer srv.Close()
			setHeadlessEnv(t, srv.URL)

			err := tc.run()
			if err == nil {
				t.Fatalf("expected an error for HTTP %d, got nil", tc.status)
			}
			if code := exitCodeOf(err); code != 1 {
				t.Errorf("exit code = %d, want 1: %v", code, err)
			}
		})
	}
}
