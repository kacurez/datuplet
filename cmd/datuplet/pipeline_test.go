package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- pure helper tests ---

func TestExtractDocName(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"yaml top-level name", "name: foo\nstages: []\n", "foo"},
		{"json top-level name", `{"name":"bar","stages":[]}`, "bar"},
		{"missing name", "stages: []\n", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := extractDocName([]byte(c.body))
			if err != nil {
				t.Fatalf("extractDocName: %v", err)
			}
			if got != c.want {
				t.Errorf("extractDocName(%q) = %q, want %q", c.body, got, c.want)
			}
		})
	}
}

func TestSniffContentType(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"json object", `{"name":"foo"}`, "application/json"},
		{"json with leading whitespace", "  \n\t{\"name\":\"foo\"}", "application/json"},
		{"yaml", "name: foo\n", "application/yaml"},
		{"empty body", "", "application/yaml"},
		{"whitespace only", "   \n\t", "application/yaml"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := sniffContentType([]byte(c.body)); got != c.want {
				t.Errorf("sniffContentType(%q) = %q, want %q", c.body, got, c.want)
			}
		})
	}
}

// --- HTTP-layer tests ---

// pipelineFakeBehaviour lets each test configure only the endpoints it
// exercises; unconfigured endpoints fail loudly if hit (mirrors
// trigger_test.go's fakeBehaviour).
type pipelineFakeBehaviour struct {
	onGet  func(w http.ResponseWriter, r *http.Request)
	onPut  func(w http.ResponseWriter, r *http.Request)
	onList func(w http.ResponseWriter, r *http.Request)
}

// newPipelineFakeServer serves the /api/v1/projects/{pid}/pipelines[...]
// routes. List and detail GETs share the same URL prefix (only the
// trailing "/pipelines" vs "/pipelines/{name}" segment differs), so
// routing is done by method + path shape rather than a strict pattern.
func newPipelineFakeServer(t *testing.T, b pipelineFakeBehaviour) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/projects/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			if b.onPut != nil {
				b.onPut(w, r)
			} else {
				http.Error(w, "onPut not configured", http.StatusInternalServerError)
			}
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pipelines"):
			if b.onList != nil {
				b.onList(w, r)
			} else {
				http.Error(w, "onList not configured", http.StatusInternalServerError)
			}
		case r.Method == http.MethodGet:
			if b.onGet != nil {
				b.onGet(w, r)
			} else {
				http.Error(w, "onGet not configured", http.StatusInternalServerError)
			}
		default:
			http.Error(w, "unexpected method "+r.Method, http.StatusInternalServerError)
		}
	})
	return httptest.NewServer(mux)
}

// TestRunPipelineGetDefaultHitsFormatYAML pins the default `get` contract:
// it must hit ?format=yaml and print the server's response body verbatim
// (RFC 027 §5.2 / S6's deterministic-YAML rendering).
func TestRunPipelineGetDefaultHitsFormatYAML(t *testing.T) {
	var gotQuery string
	const wantYAML = "name: mypipe\nstages: []\n"
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onGet: func(w http.ResponseWriter, r *http.Request) {
			gotQuery = r.URL.RawQuery
			w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(wantYAML))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runPipelineGet([]string{"mypipe", "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelineGet: %v", runErr)
	}
	if gotQuery != "format=yaml" {
		t.Errorf("query = %q, want %q", gotQuery, "format=yaml")
	}
	if out != wantYAML {
		t.Errorf("stdout = %q, want %q (verbatim)", out, wantYAML)
	}
}

// TestRunPipelineGetJSONHitsPlainEndpoint pins the `--json` contract: no
// ?format= param, and the detail JSON (including `doc` as an object) is
// printed verbatim rather than being re-decoded and re-serialized.
func TestRunPipelineGetJSONHitsPlainEndpoint(t *testing.T) {
	var gotQuery string
	const jsonBody = `{"id":"abc","name":"mypipe","doc":{"name":"mypipe","stages":[]},"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-02T00:00:00Z"}`
	// The real server's json.Encoder.Encode appends exactly one trailing
	// newline (pkg/pipelineapi/http/json.go) — mirror that here so this
	// test can pin byte-for-byte fidelity, not just content equality.
	wireBody := jsonBody + "\n"
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onGet: func(w http.ResponseWriter, r *http.Request) {
			gotQuery = r.URL.RawQuery
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(wireBody))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runPipelineGet([]string{"mypipe", "--json", "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelineGet --json: %v", runErr)
	}
	if gotQuery != "" {
		t.Errorf("query = %q, want empty (no format param)", gotQuery)
	}
	// Byte-for-byte fidelity: exactly the server's body, one trailing
	// newline — not zero (would mean truncation) and not two (would mean
	// datuplet added its own on top of the encoder's, printing a blank
	// line after the JSON).
	if out != wireBody {
		t.Errorf("stdout = %q, want server body verbatim (byte-for-byte): %q", out, wireBody)
	}
	if strings.HasSuffix(out, "\n\n") {
		t.Errorf("stdout = %q, ends with two newlines; want exactly one (no extra fmt.Println newline)", out)
	}
	var decoded pipelineDetailJSON
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("output does not decode as pipelineDetailJSON: %v", err)
	}
	if string(decoded.Doc) != `{"name":"mypipe","stages":[]}` {
		t.Errorf("Doc = %s, want the raw doc object", decoded.Doc)
	}
}

// TestRunPipelinePutJSONBodySendsJSONContentType pins the put content-type
// sniffing: a `{`-prefixed body must be sent as application/json.
func TestRunPipelinePutJSONBodySendsJSONContentType(t *testing.T) {
	var gotContentType string
	var gotBody []byte
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onPut: func(w http.ResponseWriter, r *http.Request) {
			gotContentType = r.Header.Get("Content-Type")
			gotBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusNoContent)
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	dir := t.TempDir()
	f := filepath.Join(dir, "pipe.json")
	const content = `{"name":"mypipe","stages":[]}`
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	var runErr error
	captureStdout(t, func() {
		runErr = runPipelinePut([]string{"-f", f, "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelinePut: %v", runErr)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	if string(gotBody) != content {
		t.Errorf("body sent = %q, want %q", gotBody, content)
	}
}

// TestRunPipelinePutYAMLBodySendsYAMLContentType is the YAML-body mirror
// of the JSON test above.
func TestRunPipelinePutYAMLBodySendsYAMLContentType(t *testing.T) {
	var gotContentType string
	var gotBody []byte
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onPut: func(w http.ResponseWriter, r *http.Request) {
			gotContentType = r.Header.Get("Content-Type")
			gotBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusNoContent)
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	dir := t.TempDir()
	f := filepath.Join(dir, "pipe.yaml")
	const content = "name: mypipe\nstages: []\n"
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	var runErr error
	captureStdout(t, func() {
		runErr = runPipelinePut([]string{"-f", f, "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelinePut: %v", runErr)
	}
	if gotContentType != "application/yaml" {
		t.Errorf("Content-Type = %q, want application/yaml", gotContentType)
	}
	if string(gotBody) != content {
		t.Errorf("body sent = %q, want %q", gotBody, content)
	}
}

// TestRunPipelinePutNameMismatchErrorsLocally verifies the positional-vs-
// doc-name mismatch is caught before any network call — no --remote/env is
// configured here, so a false pass would surface as a network dial error
// (not a mismatch error) rather than a false positive.
func TestRunPipelinePutNameMismatchErrorsLocally(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "pipe.yaml")
	if err := os.WriteFile(f, []byte("name: foo\nstages: []\n"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	err := runPipelinePut([]string{"bar", "-f", f})
	if err == nil {
		t.Fatal("expected a mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Errorf("error = %q, want it to mention mismatch", err.Error())
	}
}

// TestRunPipelinePutNoNameErrors pins the updated no-name error wording:
// it must now point at the doc's `name` field, not the old metadata.name.
func TestRunPipelinePutNoNameErrors(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "pipe.yaml")
	if err := os.WriteFile(f, []byte("stages: []\n"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	err := runPipelinePut([]string{"-f", f})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.HasSuffix(err.Error(), "set name in the doc") {
		t.Errorf("error = %q, want suffix %q", err.Error(), "set name in the doc")
	}
}

// TestRunPipelineListRendersDescriptionColumn pins the S6 list-description
// contract: a DESCRIPTION column appears when at least one item carries a
// non-empty description.
func TestRunPipelineListRendersDescriptionColumn(t *testing.T) {
	const listBody = `[
		{"name":"p1","description":"desc one","updated_at":"2026-01-01T00:00:00Z"},
		{"name":"p2","description":"","updated_at":"2026-01-02T00:00:00Z"}
	]`
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onList: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(listBody))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runPipelineList([]string{"--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelineList: %v", runErr)
	}
	if !strings.Contains(out, "DESCRIPTION") {
		t.Errorf("output missing DESCRIPTION header:\n%s", out)
	}
	if !strings.Contains(out, "desc one") {
		t.Errorf("output missing description value:\n%s", out)
	}
}

// TestRunPipelineListOmitsDescriptionColumnWhenAbsent guards the fallback
// path: an older server (or an all-empty-description response) keeps the
// original two-column table.
func TestRunPipelineListOmitsDescriptionColumnWhenAbsent(t *testing.T) {
	const listBody = `[{"name":"p1","updated_at":"2026-01-01T00:00:00Z"}]`
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onList: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(listBody))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runPipelineList([]string{"--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelineList: %v", runErr)
	}
	if strings.Contains(out, "DESCRIPTION") {
		t.Errorf("output should not contain DESCRIPTION header when absent:\n%s", out)
	}
}
