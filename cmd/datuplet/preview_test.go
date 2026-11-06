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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const previewTestDoc = "name: demo\nstages: []\n"

func writePreviewDoc(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "doc.yaml")
	if err := os.WriteFile(p, []byte(previewTestDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// previewFake serves a scripted sequence of GET /previews/{id} bodies.
type previewFake struct {
	mu        sync.Mutex
	postBody  map[string]any
	gets      int32
	gotQuery  []string
	getBodies []string
}

func (f *previewFake) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/previews"):
			b, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			_ = json.Unmarshal(b, &f.postBody)
			f.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"id":"p1","workspace":"default","phase":"Pending","scratch_namespaces":[],"expires_at":"2026-01-01T00:00:00Z"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/previews/p1"):
			n := int(atomic.AddInt32(&f.gets, 1))
			i := n - 1
			if i >= len(f.getBodies) {
				i = len(f.getBodies) - 1
			}
			_, _ = w.Write([]byte(f.getBodies[i]))
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	})
}

func fastPoll(t *testing.T) {
	t.Helper()
	old := previewPollInterval
	previewPollInterval = 5 * time.Millisecond
	t.Cleanup(func() { previewPollInterval = old })
}

func TestPreviewRun_WaitsAndPrintsJSON(t *testing.T) {
	fastPoll(t)
	last := `{"id":"p1","phase":"Succeeded","result_ready":true,"outputs":[]}` + "\n"
	f := &previewFake{getBodies: []string{
		`{"id":"p1","phase":"Running","result_ready":false}`,
		`{"id":"p1","phase":"Succeeded","result_ready":false}`,
		last,
	}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	var err error
	out := captureStdout(t, func() {
		err = runPreview([]string{"-f", writePreviewDoc(t), "--json", "--project", "proj1", "--rows", "50"})
	})
	if err != nil {
		t.Fatalf("runPreview: %v", err)
	}
	if out != last {
		t.Errorf("stdout = %q, want last body %q", out, last)
	}
	if got := atomic.LoadInt32(&f.gets); got != 3 {
		t.Errorf("GETs = %d, want 3", got)
	}
	if f.postBody["row_limit"] != float64(50) {
		t.Errorf("row_limit = %v, want 50", f.postBody["row_limit"])
	}
	if _, ok := f.postBody["until_stage"]; ok {
		t.Errorf("until_stage must be omitted when unset: %v", f.postBody)
	}
	doc, _ := f.postBody["doc"].(map[string]any)
	if doc["name"] != "demo" {
		t.Errorf("doc = %v, want parsed object with name demo", f.postBody["doc"])
	}
}

func previewExitCodeOf(err error) int {
	if err == nil {
		return 0
	}
	var ece *exitCodeErr
	if errors.As(err, &ece) {
		return ece.code
	}
	return 1
}

func TestPreviewRun_FailedUserExit1(t *testing.T) {
	fastPoll(t)
	f := &previewFake{getBodies: []string{`{"id":"p1","phase":"FailedUser","message":"bad config","result_ready":true}`}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	_ = captureStdout(t, func() { err = runPreview([]string{"-f", writePreviewDoc(t), "--json"}) })
	if got := previewExitCodeOf(err); got != 1 {
		t.Errorf("exit = %d (%v), want 1", got, err)
	}
}

func TestPreviewRun_FailedApplicationExit20(t *testing.T) {
	fastPoll(t)
	f := &previewFake{getBodies: []string{`{"id":"p1","phase":"FailedApplication","result_ready":true}`}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	_ = captureStdout(t, func() { err = runPreview([]string{"-f", writePreviewDoc(t), "--json"}) })
	if got := previewExitCodeOf(err); got != 20 {
		t.Errorf("exit = %d (%v), want 20", got, err)
	}
}

func TestPreviewRun_ValidationExit1(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"validation failed","findings":[{"path":"stages[0].name","message":"required","severity":"error"}]}`))
	}))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	out := captureStdout(t, func() { err = runPreview([]string{"-f", writePreviewDoc(t)}) })
	if got := previewExitCodeOf(err); got != 1 {
		t.Errorf("exit = %d (%v), want 1", got, err)
	}
	if !strings.Contains(out, "stages[0].name") || !strings.Contains(out, "required") {
		t.Errorf("findings not printed: %q", out)
	}
}

func TestPreviewRun_ConflictExit1(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"too_many_previews","running_in_workspace":2,"running_in_project":3,"live_in_project":9}`))
	}))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	err := runPreview([]string{"-f", writePreviewDoc(t)})
	if got := previewExitCodeOf(err); got != 1 {
		t.Errorf("exit = %d (%v), want 1", got, err)
	}
	if err == nil || !strings.Contains(err.Error(), "running_in_workspace=2") {
		t.Errorf("error should carry counts: %v", err)
	}
}

func TestPreviewRun_WorkspaceFromEnv(t *testing.T) {
	fastPoll(t)
	f := &previewFake{getBodies: []string{`{"id":"p1","phase":"Succeeded","result_ready":true}`}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	t.Setenv("DATUPLET_WORKSPACE", "w1")
	_ = captureStdout(t, func() { _ = runPreview([]string{"-f", writePreviewDoc(t), "--json"}) })
	if f.postBody["workspace"] != "w1" {
		t.Errorf("workspace = %v, want w1", f.postBody["workspace"])
	}
}

func TestPreviewRun_HumanSummary(t *testing.T) {
	fastPoll(t)
	body := `{"id":"p1","phase":"Succeeded","result_ready":true,
	"stages":[{"name":"extract","phase":"Succeeded","duration_ms":8300}],
	"outputs":[{"bucket":"raw","table":"data","scratch_table":"pv-ab12cd34-raw.data","rows":2,"at_cap":false,
	  "schema":[{"name":"id","type":"long","nullable":true}],
	  "sample":{"columns":[{"name":"id","type":"BIGINT"}],"rows":[[1],[2]],"truncated":false}}],
	"warnings":["something odd"]}`
	f := &previewFake{getBodies: []string{body}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	out := captureStdout(t, func() { err = runPreview([]string{"-f", writePreviewDoc(t)}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"stage extract Succeeded 8.3s", "raw.data rows=2 at_cap=false", "pv-ab12cd34-raw.data", "id", "long", "something odd"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestPreviewDeleteWorkspace_CallsBulkRoute(t *testing.T) {
	var gotMethod, gotQuery, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotQuery, gotPath = r.Method, r.URL.RawQuery, r.URL.Path
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"deleted":["a","b"],"failed":{}}`))
	}))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	_ = captureStdout(t, func() {
		err = runPreview([]string{"delete", "--workspace", "w1", "--all", "--project", "proj1"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/api/v1/projects/proj1/previews" || gotQuery != "workspace=w1" {
		t.Errorf("got %s %s ?%s", gotMethod, gotPath, gotQuery)
	}
}

func TestPreviewDeleteAll_RejectsStarWorkspace(t *testing.T) {
	setHeadlessEnv(t, "http://127.0.0.1:1")
	err := runPreview([]string{"delete", "--workspace", "*", "--all", "--project", "proj1"})
	if err == nil || !strings.Contains(err.Error(), "explicit") {
		t.Errorf("want explicit-workspace error, got %v", err)
	}
}

func TestPreviewListAndCancel(t *testing.T) {
	var gotQuery string
	var cancelled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			gotQuery = r.URL.RawQuery
			_, _ = w.Write([]byte(`{"previews":[{"id":"p1","workspace":"w1","phase":"Running","doc_name":"d","until_stage":null,"created_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T01:00:00Z"}],"next_cursor":null}`))
		case strings.HasSuffix(r.URL.Path, "/cancel"):
			cancelled = true
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	out := captureStdout(t, func() { err = runPreview([]string{"list", "--workspace", "*", "--project", "proj1"}) })
	if err != nil || gotQuery != "workspace=%2A" && gotQuery != "workspace=*" {
		t.Errorf("list: err=%v query=%q", err, gotQuery)
	}
	if !strings.Contains(out, "p1") || !strings.Contains(out, "Running") {
		t.Errorf("list output: %q", out)
	}
	if err := runPreview([]string{"cancel", "--project", "proj1", "p1"}); err != nil || !cancelled {
		t.Errorf("cancel: err=%v cancelled=%v", err, cancelled)
	}
}

func shrinkSlack(t *testing.T) {
	t.Helper()
	old := previewTimeoutSlack
	previewTimeoutSlack = 50 * time.Millisecond
	t.Cleanup(func() { previewTimeoutSlack = old })
}

func TestPreviewRun_TimeoutCancelsExit1(t *testing.T) {
	fastPoll(t)
	shrinkSlack(t)
	var cancels int32
	f := &previewFake{getBodies: []string{`{"id":"p1","phase":"Running","result_ready":false}`}}
	inner := f.handler(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cancel") {
			atomic.AddInt32(&cancels, 1)
			w.WriteHeader(http.StatusAccepted)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	_ = captureStdout(t, func() {
		err = runPreview([]string{"-f", writePreviewDoc(t), "--timeout", "10ms", "--json"})
	})
	if got := previewExitCodeOf(err); got != 1 {
		t.Errorf("exit = %d (%v), want 1", got, err)
	}
	if atomic.LoadInt32(&cancels) != 1 {
		t.Errorf("cancel POSTs = %d, want 1", cancels)
	}
}

func TestPreviewRun_Retries5xxGet(t *testing.T) {
	fastPoll(t)
	var gets int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"id":"p1"}`))
			return
		}
		if atomic.AddInt32(&gets, 1) == 1 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"id":"p1","phase":"Succeeded","result_ready":true}`))
	}))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	_ = captureStdout(t, func() { err = runPreview([]string{"-f", writePreviewDoc(t), "--json"}) })
	if err != nil {
		t.Fatalf("runPreview: %v", err)
	}
	if got := atomic.LoadInt32(&gets); got != 2 {
		t.Errorf("GETs = %d, want 2", got)
	}
}

func TestPreviewRun_Sustained5xxExit20AndCancels(t *testing.T) {
	fastPoll(t)
	shrinkSlack(t)
	var cancels int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cancel"):
			atomic.AddInt32(&cancels, 1)
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"id":"p1"}`))
		default:
			http.Error(w, "db down", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	var err error
	_ = captureStdout(t, func() {
		err = runPreview([]string{"-f", writePreviewDoc(t), "--timeout", "10ms", "--json"})
	})
	if got := previewExitCodeOf(err); got != 20 {
		t.Errorf("exit = %d (%v), want 20", got, err)
	}
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Errorf("error should include last failure: %v", err)
	}
	if atomic.LoadInt32(&cancels) != 1 {
		t.Errorf("cancel POSTs = %d, want 1", cancels)
	}
}

func TestPreviewRun_StdinPreservesNestedInt(t *testing.T) {
	fastPoll(t)
	f := &previewFake{getBodies: []string{`{"id":"p1","phase":"Succeeded","result_ready":true}`}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)

	r, w, _ := os.Pipe()
	_, _ = w.WriteString("name: demo\nstages:\n- name: s\n  components:\n  - name: g\n    config:\n      limit: {rowsCount: 7}\n")
	w.Close()
	orig := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = orig }()

	_ = captureStdout(t, func() { _ = runPreview([]string{"-f", "-", "--json"}) })
	f.mu.Lock()
	defer f.mu.Unlock()
	b, _ := json.Marshal(f.postBody["doc"])
	if !strings.Contains(string(b), `"rowsCount":7`) {
		t.Errorf("doc = %s, want rowsCount as number 7", b)
	}
}

func TestPreviewRun_HelpExit0(t *testing.T) {
	var err error
	out := captureStdout(t, func() { err = runPreview([]string{"run", "--help"}) })
	if err != nil || !strings.Contains(out, "run flags") {
		t.Errorf("err=%v out=%q", err, out)
	}
	out = captureStdout(t, func() { err = runPreview([]string{"get", "-h"}) })
	if err != nil || !strings.Contains(out, "run flags") {
		t.Errorf("get -h: err=%v out=%q", err, out)
	}
}
