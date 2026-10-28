package main

import (
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
	onGet      func(w http.ResponseWriter, r *http.Request)
	onPut      func(w http.ResponseWriter, r *http.Request)
	onList     func(w http.ResponseWriter, r *http.Request)
	onValidate func(w http.ResponseWriter, r *http.Request)
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
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/pipelines/validate"):
			if b.onValidate != nil {
				b.onValidate(w, r)
			} else {
				http.Error(w, "onValidate not configured", http.StatusInternalServerError)
			}
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

// TestRunPipelineGet pins the `get` contract. Default: hit ?format=yaml and
// print the server's response body verbatim (RFC 027 §5.2 / S6's
// deterministic-YAML rendering). `--json`: no ?format= param, and the detail
// JSON (including `doc` as an object) is printed byte-for-byte rather than
// being re-decoded and re-serialized.
func TestRunPipelineGet(t *testing.T) {
	const wantYAML = "name: mypipe\nstages: []\n"
	const jsonBody = `{"id":"abc","name":"mypipe","doc":{"name":"mypipe","stages":[]},"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-02T00:00:00Z"}`
	// The real server's json.Encoder.Encode appends exactly one trailing
	// newline (pkg/pipelineapi/http/json.go) — mirror that here so this
	// test can pin byte-for-byte fidelity, not just content equality.
	const wireBody = jsonBody + "\n"

	tests := []struct {
		name        string
		args        []string
		contentType string
		body        string
		wantQuery   string
		asJSON      bool
	}{
		{"default hits format=yaml", []string{"mypipe", "--project", "proj1"}, "text/yaml; charset=utf-8", wantYAML, "format=yaml", false},
		{"json hits plain endpoint", []string{"mypipe", "--json", "--project", "proj1"}, "application/json", wireBody, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotQuery string
			srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
				onGet: func(w http.ResponseWriter, r *http.Request) {
					gotQuery = r.URL.RawQuery
					w.Header().Set("Content-Type", tc.contentType)
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(tc.body))
				},
			})
			defer srv.Close()
			setHeadlessEnv(t, srv.URL)

			var runErr error
			out := captureStdout(t, func() {
				runErr = runPipelineGet(tc.args)
			})
			if runErr != nil {
				t.Fatalf("runPipelineGet: %v", runErr)
			}
			if gotQuery != tc.wantQuery {
				t.Errorf("query = %q, want %q", gotQuery, tc.wantQuery)
			}
			// Byte-for-byte fidelity: exactly the server's body — for JSON, one
			// trailing newline: not zero (truncation) and not two (datuplet
			// added its own on top of the encoder's).
			if out != tc.body {
				t.Errorf("stdout = %q, want server body verbatim (byte-for-byte): %q", out, tc.body)
			}
			if !tc.asJSON {
				return
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
		})
	}
}

// TestRunPipelinePutSendsSniffedContentType pins the put content-type
// sniffing: a `{`-prefixed body must be sent as application/json, a YAML body
// as application/yaml, and the body goes over the wire unchanged.
func TestRunPipelinePutSendsSniffedContentType(t *testing.T) {
	tests := []struct {
		name            string
		file            string
		content         string
		wantContentType string
	}{
		{"json body", "pipe.json", `{"name":"mypipe","stages":[]}`, "application/json"},
		{"yaml body", "pipe.yaml", "name: mypipe\nstages: []\n", "application/yaml"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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

			f := filepath.Join(t.TempDir(), tc.file)
			if err := os.WriteFile(f, []byte(tc.content), 0o644); err != nil {
				t.Fatalf("write temp file: %v", err)
			}

			var runErr error
			captureStdout(t, func() {
				runErr = runPipelinePut([]string{"-f", f, "--project", "proj1"})
			})
			if runErr != nil {
				t.Fatalf("runPipelinePut: %v", runErr)
			}
			if gotContentType != tc.wantContentType {
				t.Errorf("Content-Type = %q, want %q", gotContentType, tc.wantContentType)
			}
			if string(gotBody) != tc.content {
				t.Errorf("body sent = %q, want %q", gotBody, tc.content)
			}
		})
	}
}

// TestRunPipelinePutNameErrorsLocally verifies the positional-vs-doc-name
// mismatch and the missing-name case are caught before any network call — no
// --remote/env is configured here, so a false pass would surface as a network
// dial error rather than the expected one. The no-name error must point at
// the doc's `name` field, not the old metadata.name.
func TestRunPipelinePutNameErrorsLocally(t *testing.T) {
	tests := []struct {
		name       string
		doc        string
		posArgs    []string
		wantSuffix string
		wantHas    string
	}{
		{"positional name mismatch", "name: foo\nstages: []\n", []string{"bar"}, "", "mismatch"},
		{"no name anywhere", "stages: []\n", nil, "set name in the doc", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "pipe.yaml")
			if err := os.WriteFile(f, []byte(tc.doc), 0o644); err != nil {
				t.Fatalf("write temp file: %v", err)
			}

			err := runPipelinePut(append(tc.posArgs, "-f", f))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if tc.wantHas != "" && !strings.Contains(err.Error(), tc.wantHas) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), tc.wantHas)
			}
			if tc.wantSuffix != "" && !strings.HasSuffix(err.Error(), tc.wantSuffix) {
				t.Errorf("error = %q, want suffix %q", err.Error(), tc.wantSuffix)
			}
		})
	}
}

// TestRunPipelineListDescriptionColumn pins the S6 list-description contract:
// a DESCRIPTION column appears when at least one item carries a non-empty
// description, and an older server (or an all-empty-description response)
// keeps the original two-column table.
func TestRunPipelineListDescriptionColumn(t *testing.T) {
	tests := []struct {
		name            string
		body            string
		wantDescription bool
	}{
		{"column rendered when a description exists", `[
		{"name":"p1","description":"desc one","updated_at":"2026-01-01T00:00:00Z"},
		{"name":"p2","description":"","updated_at":"2026-01-02T00:00:00Z"}
	]`, true},
		{"column omitted when absent", `[{"name":"p1","updated_at":"2026-01-01T00:00:00Z"}]`, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
				onList: func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(tc.body))
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
			if got := strings.Contains(out, "DESCRIPTION"); got != tc.wantDescription {
				t.Errorf("DESCRIPTION header present = %v, want %v:\n%s", got, tc.wantDescription, out)
			}
			if tc.wantDescription && !strings.Contains(out, "desc one") {
				t.Errorf("output missing description value:\n%s", out)
			}
		})
	}
}

// TestRunPipelineListJSONPrintsBodyVerbatim pins the `list --json` contract:
// the server's JSON is printed byte-for-byte, not re-decoded/re-serialized
// and not given an extra trailing newline on top of the encoder's own
// (RFC 027 C3 fix 2 — mirrors TestRunPipelineGetJSONHitsPlainEndpoint).
func TestRunPipelineListJSONPrintsBodyVerbatim(t *testing.T) {
	const jsonBody = `[{"name":"p1","description":"desc one","updated_at":"2026-01-01T00:00:00Z"}]`
	// The real server's json.Encoder.Encode appends exactly one trailing
	// newline (pkg/pipelineapi/http/json.go) — mirror that here so this
	// test can pin byte-for-byte fidelity, not just content equality.
	wireBody := jsonBody + "\n"
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onList: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(wireBody))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var runErr error
	out := captureStdout(t, func() {
		runErr = runPipelineList([]string{"--json", "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelineList --json: %v", runErr)
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
	var decoded []pipelineRefJSON
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("output does not decode as []pipelineRefJSON: %v", err)
	}
	if len(decoded) != 1 || decoded[0].Name != "p1" {
		t.Errorf("decoded = %+v, want one item named p1", decoded)
	}
}

// --- validate (RFC 027 C4) ---

// TestRunPipelineValidateExitCodes pins the agent-facing exit-code contract:
// no findings and warning-only findings exit 0 (nil error; the table still
// renders, a friendly "no findings" rather than a blank one), while any
// error-severity finding is a non-nil error with the default exit 1 (NOT an
// exitCodeErr, which is reserved for transport failures) with the table
// still rendered so a human sees why.
func TestRunPipelineValidateExitCodes(t *testing.T) {
	tests := []struct {
		name     string
		resp     string
		wantExit int
		wantOut  []string
	}{
		{"clean", `{"findings":[]}`, 0, []string{"no findings"}},
		{"warnings only", `{"findings":[{"path":"stages[0].config.foo","message":"deprecated field","severity":"warning"}]}`, 0, []string{"WARNING", "deprecated field"}},
		{"errors", `{"findings":[{"path":"stages[0].component","message":"unknown component \"bogus\"","severity":"error"}]}`, 1, []string{"ERROR", "unknown component"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotMethod, gotPath, gotQuery string
			srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
				onValidate: func(w http.ResponseWriter, r *http.Request) {
					gotMethod = r.Method
					gotPath = r.URL.Path
					gotQuery = r.URL.RawQuery
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(tc.resp))
				},
			})
			defer srv.Close()
			setHeadlessEnv(t, srv.URL)

			f := filepath.Join(t.TempDir(), "pipe.yaml")
			if err := os.WriteFile(f, []byte("name: mypipe\nstages: []\n"), 0o644); err != nil {
				t.Fatalf("write temp file: %v", err)
			}

			var runErr error
			out := captureStdout(t, func() {
				runErr = runPipelineValidate([]string{"-f", f, "--project", "proj1"})
			})
			if code := exitCodeOf(runErr); code != tc.wantExit {
				t.Fatalf("exit code = %d, want %d (err = %v)", code, tc.wantExit, runErr)
			}
			var ece *exitCodeErr
			if errors.As(runErr, &ece) {
				t.Errorf("error = %v, must NOT be an exitCodeErr — findings are exit 0/1, not a transport failure", runErr)
			}
			if gotMethod != http.MethodPost {
				t.Errorf("method = %q, want POST", gotMethod)
			}
			if !strings.HasSuffix(gotPath, "/pipelines/validate") {
				t.Errorf("path = %q, want suffix /pipelines/validate", gotPath)
			}
			if gotQuery != "" {
				t.Errorf("query = %q, want empty (no --name given)", gotQuery)
			}
			for _, want := range tc.wantOut {
				if !strings.Contains(out, want) {
					t.Errorf("output = %q, want it to contain %q", out, want)
				}
			}
		})
	}
}

// TestRunPipelineValidateNamePassesQueryParam pins the --name wiring: it
// must be forwarded as ?name= so the server engages the update-mode
// resource-gate diff (spec §5.2/§7).
func TestRunPipelineValidateNamePassesQueryParam(t *testing.T) {
	var gotQuery string
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onValidate: func(w http.ResponseWriter, r *http.Request) {
			gotQuery = r.URL.RawQuery
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"findings":[]}`))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	dir := t.TempDir()
	f := filepath.Join(dir, "pipe.yaml")
	if err := os.WriteFile(f, []byte("name: mypipe\nstages: []\n"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	var runErr error
	captureStdout(t, func() {
		runErr = runPipelineValidate([]string{"-f", f, "--name", "mypipe", "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelineValidate: %v", runErr)
	}
	if gotQuery != "name=mypipe" {
		t.Errorf("query = %q, want %q", gotQuery, "name=mypipe")
	}
}

// TestRunPipelineValidateJSONPrintsBodyVerbatim pins the `--json` contract:
// the server's findings body is printed byte-for-byte (same fidelity
// convention as get --json / list --json — RFC 027 C3).
func TestRunPipelineValidateJSONPrintsBodyVerbatim(t *testing.T) {
	const jsonBody = `{"findings":[{"path":"x","message":"y","severity":"warning"}]}`
	wireBody := jsonBody + "\n"
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onValidate: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(wireBody))
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	dir := t.TempDir()
	f := filepath.Join(dir, "pipe.yaml")
	if err := os.WriteFile(f, []byte("name: mypipe\nstages: []\n"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	var runErr error
	out := captureStdout(t, func() {
		runErr = runPipelineValidate([]string{"-f", f, "--json", "--project", "proj1"})
	})
	if runErr != nil {
		t.Fatalf("runPipelineValidate --json: %v", runErr)
	}
	if out != wireBody {
		t.Errorf("stdout = %q, want server body verbatim (byte-for-byte): %q", out, wireBody)
	}
	if strings.HasSuffix(out, "\n\n") {
		t.Errorf("stdout = %q, ends with two newlines; want exactly one", out)
	}
}

// TestRunPipelineValidateServerErrorExitsTransportCode pins the "the
// request itself failed" contract: a 500 from the validate endpoint must
// NOT be treated as findings — it surfaces as an exitCodeErr with code >=2
// and a message describing the failure (main.go prints this to stderr).
func TestRunPipelineValidateServerErrorExitsTransportCode(t *testing.T) {
	srv := newPipelineFakeServer(t, pipelineFakeBehaviour{
		onValidate: func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		},
	})
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	dir := t.TempDir()
	f := filepath.Join(dir, "pipe.yaml")
	if err := os.WriteFile(f, []byte("name: mypipe\nstages: []\n"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	var runErr error
	captureStdout(t, func() {
		runErr = runPipelineValidate([]string{"-f", f, "--project", "proj1"})
	})
	if runErr == nil {
		t.Fatal("runPipelineValidate: expected a non-nil error on server 500")
	}
	var ece *exitCodeErr
	if !errors.As(runErr, &ece) {
		t.Fatalf("error = %v (%T), want an *exitCodeErr", runErr, runErr)
	}
	if ece.code < 2 {
		t.Errorf("exitCodeErr.code = %d, want >=2", ece.code)
	}
	if runErr.Error() == "" {
		t.Error("error message is empty; want a message describing the failure (printed to stderr by main.go)")
	}
}
