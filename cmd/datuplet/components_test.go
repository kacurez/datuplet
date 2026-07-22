package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// --- resolveVersion (pure) ---

func TestResolveVersion(t *testing.T) {
	detail := componentDetailJSON{
		Name:           "data-generator",
		DefaultVersion: "v1.0.0",
		Versions: []versionJSON{
			{Version: "v0.9.0", ConfigSchema: `{"schema":"v0.9.0"}`},
			{Version: "v1.0.0", ConfigSchema: `{"schema":"v1.0.0"}`},
			{Version: "v1.2.0", ConfigSchema: `{"schema":"v1.2.0"}`},
			{Version: "dev", Prerelease: true, ConfigSchema: `{"schema":"dev"}`},
		},
	}

	t.Run("explicit hit", func(t *testing.T) {
		v, err := resolveVersion(detail, "v0.9.0")
		if err != nil {
			t.Fatalf("resolveVersion: %v", err)
		}
		if v.Version != "v0.9.0" {
			t.Errorf("Version = %q, want v0.9.0", v.Version)
		}
	})

	t.Run("explicit miss errors", func(t *testing.T) {
		_, err := resolveVersion(detail, "v9.9.9")
		if err == nil {
			t.Fatal("expected error for unknown --version, got nil")
		}
		if !strings.Contains(err.Error(), "v9.9.9") {
			t.Errorf("error should mention the requested version: %v", err)
		}
	})

	t.Run("default present returns default", func(t *testing.T) {
		v, err := resolveVersion(detail, "")
		if err != nil {
			t.Fatalf("resolveVersion: %v", err)
		}
		if v.Version != "v1.0.0" {
			t.Errorf("Version = %q, want v1.0.0 (defaultVersion)", v.Version)
		}
	})

	t.Run("default absent falls back to highest stable", func(t *testing.T) {
		noDefault := detail
		noDefault.DefaultVersion = ""
		v, err := resolveVersion(noDefault, "")
		if err != nil {
			t.Fatalf("resolveVersion: %v", err)
		}
		if v.Version != "v1.2.0" {
			t.Errorf("Version = %q, want v1.2.0 (highest stable)", v.Version)
		}
	})

	t.Run("all prerelease errors", func(t *testing.T) {
		allPrerelease := componentDetailJSON{
			Name: "wip-component",
			Versions: []versionJSON{
				{Version: "dev", Prerelease: true},
				{Version: "nightly", Prerelease: true},
			},
		}
		_, err := resolveVersion(allPrerelease, "")
		if err == nil {
			t.Fatal("expected error when all versions are prerelease, got nil")
		}
	})

	t.Run("defaultVersion set but not registered errors", func(t *testing.T) {
		badDefault := componentDetailJSON{
			Name:           "broken",
			DefaultVersion: "v9.9.9",
			Versions:       []versionJSON{{Version: "v1.0.0"}},
		}
		_, err := resolveVersion(badDefault, "")
		if err == nil {
			t.Fatal("expected error when defaultVersion isn't in Versions, got nil")
		}
	})

	t.Run("non-semver stable-looking versions are skipped", func(t *testing.T) {
		noStable := componentDetailJSON{
			Name:     "odd",
			Versions: []versionJSON{{Version: "1.2.3"}, {Version: "latest"}},
		}
		_, err := resolveVersion(noStable, "")
		if err == nil {
			t.Fatal("expected error when no version matches the vMAJOR.MINOR.PATCH pattern")
		}
	})
}

// --- HTTP-layer fetch helpers ---

const componentsListFixture = `[
  {
    "name": "data-generator",
    "displayName": "Data Generator",
    "description": "Generates synthetic rows.",
    "deprecated": false,
    "defaultVersion": "v1.0.0",
    "io": {"inputs": "none", "outputs": "required"},
    "versions": [
      {"version": "v1.0.0", "prerelease": false, "image": "datuplet/data-generator:v1.0.0"}
    ]
  },
  {
    "name": "sql-transform",
    "displayName": "SQL Transform",
    "description": "Runs SQL via DuckDB.",
    "deprecated": true,
    "defaultVersion": "v0.9.1",
    "io": {"inputs": "required", "outputs": "required"},
    "versions": [
      {"version": "v0.9.1", "prerelease": false, "image": "datuplet/sql-transform:v0.9.1"}
    ]
  }
]`

const componentDetailFixture = `{
  "name": "data-generator",
  "displayName": "Data Generator",
  "description": "Generates synthetic rows.",
  "deprecated": false,
  "defaultVersion": "v1.0.0",
  "io": {"inputs": "none", "outputs": "required"},
  "versions": [
    {"version": "v0.9.0", "prerelease": false, "image": "datuplet/data-generator:v0.9.0", "configSchema": "{\"type\":\"object\"}"},
    {"version": "v1.0.0", "prerelease": false, "image": "datuplet/data-generator:v1.0.0", "configSchema": "{\"type\":\"object\",\"v\":1}"}
  ]
}`

// newComponentsFakeServer serves GET /api/v1/components and
// GET /api/v1/components/{name} with fixed fixtures. detailStatus lets
// tests exercise the 404 path.
func newComponentsFakeServer(t *testing.T, listBody, detailBody string, detailStatus int) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/components", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/components" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(listBody))
	})
	mux.HandleFunc("/api/v1/components/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(detailStatus)
		_, _ = w.Write([]byte(detailBody))
	})
	return httptest.NewServer(mux)
}

func TestFetchComponentsListDecodesSummaryShape(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, componentDetailFixture, http.StatusOK)
	defer srv.Close()

	body, items, err := fetchComponentsList(context.Background(), srv.URL, "tok")
	if err != nil {
		t.Fatalf("fetchComponentsList: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}
	if items[0].Name != "data-generator" || items[0].IO.Outputs != "required" {
		t.Errorf("items[0] = %+v, unexpected", items[0])
	}
	if items[1].Deprecated != true {
		t.Errorf("items[1].Deprecated = %v, want true", items[1].Deprecated)
	}
	var raw []map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("raw body is not the passthrough JSON array: %v", err)
	}
}

func TestFetchComponentDetailDecodesConfigSchema(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, componentDetailFixture, http.StatusOK)
	defer srv.Close()

	_, detail, err := fetchComponentDetail(context.Background(), srv.URL, "tok", "data-generator")
	if err != nil {
		t.Fatalf("fetchComponentDetail: %v", err)
	}
	if len(detail.Versions) != 2 {
		t.Fatalf("len(Versions) = %d, want 2", len(detail.Versions))
	}
	if detail.Versions[1].ConfigSchema != `{"type":"object","v":1}` {
		t.Errorf("ConfigSchema = %q, unexpected", detail.Versions[1].ConfigSchema)
	}
}

func TestFetchComponentDetailNotFound(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, `{"error":"not found"}`, http.StatusNotFound)
	defer srv.Close()

	_, _, err := fetchComponentDetail(context.Background(), srv.URL, "tok", "nope")
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found': %v", err)
	}
}

// --- CLI-layer: stdout rendering, via env-var headless auth ---

// setHeadlessEnv points loadRemoteArgs at remote via the RFC 027 §7
// headless env vars, bypassing ~/.datuplet entirely (mirrors
// TestLoadRemoteArgs_HeadlessNoDatupletFileRead in remote_test.go).
func setHeadlessEnv(t *testing.T, remote string) {
	t.Helper()
	t.Setenv("HOME", "/nonexistent-for-test")
	t.Setenv("DATUPLET_API_TOKEN", "headless-token")
	t.Setenv("DATUPLET_REMOTE", remote)
}

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	return buf.String()
}

func TestRunComponentsListTableColumns(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, componentDetailFixture, http.StatusOK)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runComponentsList(nil)
	})
	if runErr != nil {
		t.Fatalf("runComponentsList: %v", runErr)
	}
	for _, want := range []string{"NAME", "DISPLAY", "DEFAULT", "IO", "DEPRECATED", "data-generator", "sql-transform"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRunComponentsListJSONPassthrough(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, componentDetailFixture, http.StatusOK)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runComponentsList([]string{"--json"})
	})
	if runErr != nil {
		t.Fatalf("runComponentsList --json: %v", runErr)
	}
	var items []componentSummaryJSON
	if err := json.Unmarshal([]byte(out), &items); err != nil {
		t.Fatalf("--json output is not valid JSON: %v\noutput: %s", err, out)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}
}

// TestRunComponentsGetSchemaOutputVerbatim pins the exact --schema
// contract: the resolved version's configSchema bytes, verbatim, plus a
// single trailing newline — nothing else (no headers, no extra text).
func TestRunComponentsGetSchemaOutputVerbatim(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, componentDetailFixture, http.StatusOK)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runComponentsGet([]string{"data-generator", "--schema"})
	})
	if runErr != nil {
		t.Fatalf("runComponentsGet --schema: %v", runErr)
	}
	want := `{"type":"object","v":1}` + "\n" // defaultVersion v1.0.0's schema
	if out != want {
		t.Errorf("stdout = %q, want %q", out, want)
	}
}

func TestRunComponentsGetSchemaWithExplicitVersion(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, componentDetailFixture, http.StatusOK)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runComponentsGet([]string{"data-generator", "--version", "v0.9.0", "--schema"})
	})
	if runErr != nil {
		t.Fatalf("runComponentsGet --version --schema: %v", runErr)
	}
	want := `{"type":"object"}` + "\n"
	if out != want {
		t.Errorf("stdout = %q, want %q", out, want)
	}
}

func TestRunComponentsGetSchemaUnknownVersionErrors(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, componentDetailFixture, http.StatusOK)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	err := runComponentsGet([]string{"data-generator", "--version", "v9.9.9", "--schema"})
	if err == nil {
		t.Fatal("expected error for unknown --version, got nil")
	}
}

func TestRunComponentsGetJSONPassthrough(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, componentDetailFixture, http.StatusOK)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runComponentsGet([]string{"data-generator", "--json"})
	})
	if runErr != nil {
		t.Fatalf("runComponentsGet --json: %v", runErr)
	}
	var detail componentDetailJSON
	if err := json.Unmarshal([]byte(out), &detail); err != nil {
		t.Fatalf("--json output is not valid JSON: %v", err)
	}
	if detail.Name != "data-generator" {
		t.Errorf("Name = %q, want data-generator", detail.Name)
	}
}

func TestRunComponentsGetNotFound(t *testing.T) {
	srv := newComponentsFakeServer(t, componentsListFixture, `{"error":"not found"}`, http.StatusNotFound)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	err := runComponentsGet([]string{"nope"})
	if err == nil {
		t.Fatal("expected error for unknown component, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found': %v", err)
	}
}

func TestRunComponentsGetJSONAndSchemaMutuallyExclusive(t *testing.T) {
	err := runComponentsGet([]string{"data-generator", "--json", "--schema"})
	if err == nil {
		t.Fatal("expected error when both --json and --schema are set")
	}
}

func TestRunComponentsListRejectsSchema(t *testing.T) {
	err := runComponentsList([]string{"--schema"})
	if err == nil {
		t.Fatal("expected error: list does not support --schema")
	}
}

func TestParseComponentsFlags(t *testing.T) {
	positional, remote, tokenFile, version, asJSON, asSchema, err := parseComponentsFlags(
		[]string{"data-generator", "--remote", "http://x", "--version=v1.0.0", "--json"})
	if err != nil {
		t.Fatalf("parseComponentsFlags: %v", err)
	}
	if len(positional) != 1 || positional[0] != "data-generator" {
		t.Errorf("positional = %v, want [data-generator]", positional)
	}
	if remote != "http://x" {
		t.Errorf("remote = %q", remote)
	}
	if tokenFile != "" {
		t.Errorf("tokenFile = %q, want empty", tokenFile)
	}
	if version != "v1.0.0" {
		t.Errorf("version = %q", version)
	}
	if !asJSON || asSchema {
		t.Errorf("asJSON=%v asSchema=%v, want true/false", asJSON, asSchema)
	}

	if _, _, _, _, _, _, err := parseComponentsFlags([]string{"--bogus"}); err == nil {
		t.Error("expected error for unknown flag")
	}
	if _, _, _, _, _, _, err := parseComponentsFlags([]string{"--remote"}); err == nil {
		t.Error("expected error for --remote missing a value")
	}
}
