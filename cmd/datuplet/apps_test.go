package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	positional, remote, tokenFile, project, bundle, asJSON, err := parseAppsFlags(
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
	if !asJSON {
		t.Error("asJSON = false, want true")
	}

	if _, _, _, _, _, _, err := parseAppsFlags([]string{"--bogus"}); err == nil {
		t.Error("expected error for unknown flag")
	}
	if _, _, _, _, _, _, err := parseAppsFlags([]string{"--remote"}); err == nil {
		t.Error("expected error for --remote missing a value")
	}
	if _, _, _, _, _, _, err := parseAppsFlags([]string{"--bundle"}); err == nil {
		t.Error("expected error for --bundle missing a value")
	}
}

// --- apps init (local, no network) ---

func TestRunAppsInit_WritesScaffoldIncludingBuildScript(t *testing.T) {
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "sales-overview")

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

func TestRunAppsInit_RequiresExactlyOneArg(t *testing.T) {
	if err := runAppsInit(nil); err == nil {
		t.Error("expected error with no args")
	}
	if err := runAppsInit([]string{"a", "b"}); err == nil {
		t.Error("expected error with more than one arg")
	}
}

func TestRunAppsInit_CreatesMissingParentDirs(t *testing.T) {
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "nested", "sales-overview")
	if err := runAppsInit([]string{dir}); err != nil {
		t.Fatalf("runAppsInit: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "app.js")); err != nil {
		t.Fatalf("app.js not written: %v", err)
	}
}

// --- apps put/get/list/delete (httptest fake API) ---

// appsFakeBehaviour lets each test configure only the endpoints it
// exercises; unconfigured endpoints fail loudly if hit (mirrors
// trigger_test.go's fakeBehaviour / pipeline_test.go's pipelineFakeBehaviour).
type appsFakeBehaviour struct {
	onPut    func(w http.ResponseWriter, r *http.Request)
	onGet    func(w http.ResponseWriter, r *http.Request)
	onList   func(w http.ResponseWriter, r *http.Request)
	onDelete func(w http.ResponseWriter, r *http.Request)
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

func TestRunAppsPut_UploadsBase64AndPrintsResultJSON(t *testing.T) {
	const bundleSrc = "export async function render(ctx){return {outputDoc:1,title:'t',blocks:[]}}"
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

	var runErr error
	out := captureStdout(t, func() {
		runErr = runAppsPut([]string{"sales-overview", "--project", "proj1", "--bundle", bundlePath, "--json"})
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

	var resp appPutResponseJSON
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		t.Fatalf("--json output is not valid JSON: %v\noutput: %s", err, out)
	}
	if resp.AppID != "app-123" || resp.VersionHash != "deadbeef" {
		t.Errorf("decoded response = %+v, unexpected", resp)
	}
}

func TestRunAppsPut_TextModePrintsAppIDAndVersionHash(t *testing.T) {
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onPut: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"app_id":"app-123","version_hash":"deadbeef"}`))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	bundlePath := writeTempBundle(t, []byte("export async function render(ctx){}"))

	var runErr error
	out := captureStdout(t, func() {
		runErr = runAppsPut([]string{"sales-overview", "--project", "proj1", "--bundle", bundlePath})
	})
	if runErr != nil {
		t.Fatalf("runAppsPut: %v", runErr)
	}
	if !strings.Contains(out, "app-123") || !strings.Contains(out, "deadbeef") {
		t.Errorf("text output missing app_id/version_hash:\n%s", out)
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

func TestRunAppsPut_MissingBundleFlagErrors(t *testing.T) {
	if err := runAppsPut([]string{"sales-overview", "--project", "proj1"}); err == nil {
		t.Fatal("expected error when --bundle is omitted")
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

func TestRunAppsGet_NotFound(t *testing.T) {
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onGet: func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"app not found"}`, http.StatusNotFound)
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	err := runAppsGet([]string{"nope", "--project", "proj1"})
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found': %v", err)
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

func TestRunAppsList_RejectsPositionalArgs(t *testing.T) {
	if err := runAppsList([]string{"unexpected"}); err == nil {
		t.Fatal("expected error for a positional arg")
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

func TestRunAppsDelete_NotFound(t *testing.T) {
	srv := newAppsFakeServer(t, appsFakeBehaviour{
		onDelete: func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"app not found"}`, http.StatusNotFound)
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	err := runAppsDelete([]string{"nope", "--project", "proj1"})
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found': %v", err)
	}
}

func TestRunAppsDelete_RequiresExactlyOneName(t *testing.T) {
	if err := runAppsDelete([]string{"--project", "proj1"}); err == nil {
		t.Error("expected error with no name")
	}
	if err := runAppsDelete([]string{"a", "b", "--project", "proj1"}); err == nil {
		t.Error("expected error with more than one name")
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

func assertLocalNameRejection(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a local rejection for name %q, got nil", name)
	}
	if strings.Contains(err.Error(), "refused") || strings.Contains(err.Error(), "connection") {
		t.Errorf("name %q: error looks like it came from a network call, not a local rejection: %v", name, err)
	}
	// A name starting with "-" (e.g. "-leading-hyphen") never reaches
	// validateAppName at all: parseAppsFlags's pre-existing convention
	// (shared with pipeline.go's/components.go's hand-rolled parsers) treats
	// any leading-dash argument as an unrecognized FLAG, not a positional
	// value — itself a legitimate local, before-network rejection, just from
	// a different, older layer than the new name check. Every other shape
	// here reaches validateAppName and must carry its message; the leading-
	// hyphen shape is separately proven to fail the regex itself in
	// TestValidateAppName.
	if strings.HasPrefix(name, "-") {
		if !strings.Contains(err.Error(), "unknown flag") {
			t.Errorf("name %q: expected the flag parser's rejection, got: %v", name, err)
		}
		return
	}
	if !strings.Contains(err.Error(), "invalid app name") {
		t.Errorf("name %q: error should name the validation rule: %v", name, err)
	}
}

func TestRunAppsGet_InvalidNameRejectedLocally(t *testing.T) {
	for _, name := range invalidAppNames {
		t.Run(name, func(t *testing.T) {
			err := runAppsGet([]string{name, "--remote", "http://127.0.0.1:1", "--project", "proj1"})
			assertLocalNameRejection(t, name, err)
		})
	}
}

func TestRunAppsDelete_InvalidNameRejectedLocally(t *testing.T) {
	for _, name := range invalidAppNames {
		t.Run(name, func(t *testing.T) {
			err := runAppsDelete([]string{name, "--remote", "http://127.0.0.1:1", "--project", "proj1"})
			assertLocalNameRejection(t, name, err)
		})
	}
}

func TestRunAppsPut_InvalidNameRejectedLocally(t *testing.T) {
	bundlePath := writeTempBundle(t, []byte("export async function render(ctx){}"))
	for _, name := range invalidAppNames {
		t.Run(name, func(t *testing.T) {
			err := runAppsPut([]string{name, "--remote", "http://127.0.0.1:1", "--project", "proj1", "--bundle", bundlePath})
			assertLocalNameRejection(t, name, err)
		})
	}
}

// TestRunAppsGet_ValidNameNotRejectedByNameCheck proves a well-formed name
// sails past local validation: the call still fails (the remote is
// unreachable), but for a DIFFERENT reason than name validation.
func TestRunAppsGet_ValidNameNotRejectedByNameCheck(t *testing.T) {
	err := runAppsGet([]string{"sales-overview", "--remote", "http://127.0.0.1:1", "--project", "proj1"})
	if err == nil {
		t.Fatal("expected an error (unreachable remote), got nil")
	}
	if strings.Contains(err.Error(), "invalid app name") {
		t.Errorf("a valid name must not be rejected by local validation: %v", err)
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
