package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

const runsListFixture = `{
  "runs": [
    {"id":"r1","pipeline_name":"daily","phase":"Succeeded","created_at":"2026-07-22T10:00:00Z","completed_at":"2026-07-22T10:01:00Z"},
    {"id":"r2","pipeline_name":"daily","phase":"Running","current_stage":"transform","created_at":"2026-07-22T10:05:00Z"}
  ],
  "next_cursor": "CUR123"
}`

const runDetailFixture = `{
  "id":"r1","pipeline_name":"daily","phase":"Succeeded",
  "project_id":"proj-uuid","pipeline_id":"pl-uuid",
  "created_at":"2026-07-22T10:00:00Z","completed_at":"2026-07-22T10:01:00Z",
  "timeline":[
    {"name":"extract","phase":"Succeeded","duration_ms":1200},
    {"name":"load","phase":"Succeeded","duration_ms":3400,"message":"wrote daily_summary"}
  ]
}`

// capturedReq records what the fake server last received, for URL/query/auth
// assertions.
type capturedReq struct {
	mu   sync.Mutex
	path string
	rawQ string
	auth string
	hits int
}

func (c *capturedReq) record(r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.path = r.URL.Path
	c.rawQ = r.URL.RawQuery
	c.auth = r.Header.Get("Authorization")
	c.hits++
}

// newRunsFakeServer serves the list endpoint at
// /api/v1/projects/{pid}/runs and the detail endpoint at .../runs/{id}.
// detailStatus lets a test exercise the 404 path.
func newRunsFakeServer(t *testing.T, cap *capturedReq, detailStatus int) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	// The runs collection: exact-suffix "/runs".
	mux.HandleFunc("/api/v1/projects/", func(w http.ResponseWriter, r *http.Request) {
		cap.record(r)
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/runs") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(runsListFixture))
			return
		}
		// .../runs/{id}
		if strings.Contains(r.URL.Path, "/runs/") {
			w.WriteHeader(detailStatus)
			if detailStatus == http.StatusOK {
				_, _ = w.Write([]byte(runDetailFixture))
			} else {
				_, _ = w.Write([]byte(`{"error":"run not found"}`))
			}
			return
		}
		http.NotFound(w, r)
	})
	return httptest.NewServer(mux)
}

func TestFetchRunsList_DecodesAndBuildsRequest(t *testing.T) {
	cap := &capturedReq{}
	srv := newRunsFakeServer(t, cap, http.StatusOK)
	defer srv.Close()

	q := url.Values{}
	q.Set("pipeline", "daily")
	q.Set("phase", "Running")
	q.Set("limit", "5")
	_, resp, err := fetchRunsList(context.Background(), srv.URL, "tok", "proj-uuid", q)
	if err != nil {
		t.Fatalf("fetchRunsList: %v", err)
	}
	if len(resp.Runs) != 2 || resp.Runs[0].ID != "r1" || resp.Runs[1].Phase != "Running" {
		t.Fatalf("decoded runs unexpected: %+v", resp.Runs)
	}
	if resp.NextCursor == nil || *resp.NextCursor != "CUR123" {
		t.Errorf("NextCursor = %v, want CUR123", resp.NextCursor)
	}
	if cap.path != "/api/v1/projects/proj-uuid/runs" {
		t.Errorf("request path = %q, want /api/v1/projects/proj-uuid/runs", cap.path)
	}
	if cap.auth != "Bearer tok" {
		t.Errorf("auth header = %q, want 'Bearer tok'", cap.auth)
	}
	for _, want := range []string{"pipeline=daily", "phase=Running", "limit=5"} {
		if !strings.Contains(cap.rawQ, want) {
			t.Errorf("query %q missing %q", cap.rawQ, want)
		}
	}
}

func TestFetchRunDetail_DecodesTimeline(t *testing.T) {
	cap := &capturedReq{}
	srv := newRunsFakeServer(t, cap, http.StatusOK)
	defer srv.Close()

	_, d, status, err := fetchRunDetail(context.Background(), srv.URL, "tok", "proj-uuid", "r1")
	if err != nil {
		t.Fatalf("fetchRunDetail: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if d.ID != "r1" || d.Phase != "Succeeded" {
		t.Errorf("detail summary unexpected: %+v", d.runSummary)
	}
	if len(d.Timeline) != 2 || d.Timeline[0].Name != "extract" || d.Timeline[1].Message != "wrote daily_summary" {
		t.Errorf("timeline unexpected: %+v", d.Timeline)
	}
	if cap.path != "/api/v1/projects/proj-uuid/runs/r1" {
		t.Errorf("request path = %q, want .../runs/r1", cap.path)
	}
}

func TestFetchRunDetail_NotFoundReturnsStatus(t *testing.T) {
	cap := &capturedReq{}
	srv := newRunsFakeServer(t, cap, http.StatusNotFound)
	defer srv.Close()

	_, _, status, err := fetchRunDetail(context.Background(), srv.URL, "tok", "proj-uuid", "nope")
	if err != nil {
		t.Fatalf("fetchRunDetail should not error on 404 (caller maps it): %v", err)
	}
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
}

// --- CLI layer via headless env (loadRemoteArgs fast path) ---

func TestRunsList_Headless(t *testing.T) {
	cap := &capturedReq{}
	srv := newRunsFakeServer(t, cap, http.StatusOK)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	t.Setenv("DATUPLET_PROJECT", "proj-uuid")

	if err := runRunsList([]string{"--phase", "Running", "--pipeline", "daily", "--json"}); err != nil {
		t.Fatalf("runRunsList: %v", err)
	}
	if cap.path != "/api/v1/projects/proj-uuid/runs" {
		t.Errorf("path = %q", cap.path)
	}
	if cap.auth != "Bearer headless-token" {
		t.Errorf("auth = %q, want 'Bearer headless-token'", cap.auth)
	}
	if !strings.Contains(cap.rawQ, "phase=Running") || !strings.Contains(cap.rawQ, "pipeline=daily") {
		t.Errorf("query = %q missing filters", cap.rawQ)
	}
}

func TestRunsGet_Headless(t *testing.T) {
	cap := &capturedReq{}
	srv := newRunsFakeServer(t, cap, http.StatusOK)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	t.Setenv("DATUPLET_PROJECT", "proj-uuid")

	if err := runRunsGet([]string{"r1", "--json"}); err != nil {
		t.Fatalf("runRunsGet: %v", err)
	}
	if cap.path != "/api/v1/projects/proj-uuid/runs/r1" {
		t.Errorf("path = %q, want .../runs/r1", cap.path)
	}
}

func TestRunsGet_NotFoundFriendlyError(t *testing.T) {
	cap := &capturedReq{}
	srv := newRunsFakeServer(t, cap, http.StatusNotFound)
	defer srv.Close()
	setHeadlessEnv(t, srv.URL)
	t.Setenv("DATUPLET_PROJECT", "proj-uuid")

	err := runRunsGet([]string{"missing-run"})
	if err == nil {
		t.Fatal("expected a not-found error, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should say not found: %v", err)
	}
}

func TestRunsFlagValidation(t *testing.T) {
	t.Run("list rejects a positional", func(t *testing.T) {
		if err := runRunsList([]string{"some-id"}); err == nil || !strings.Contains(err.Error(), "positional") {
			t.Errorf("want positional error, got %v", err)
		}
	})
	t.Run("get requires an id", func(t *testing.T) {
		if err := runRunsGet([]string{"--json"}); err == nil || !strings.Contains(err.Error(), "run-id") {
			t.Errorf("want usage error naming run-id, got %v", err)
		}
	})
	t.Run("unknown flag errors", func(t *testing.T) {
		if _, err := parseRunsFlags([]string{"--bogus"}); err == nil {
			t.Error("want error for unknown flag")
		}
	})
	t.Run("limit must be int", func(t *testing.T) {
		if err := runRunsList([]string{"--limit", "abc"}); err == nil || !strings.Contains(err.Error(), "integer") {
			t.Errorf("want integer error, got %v", err)
		}
	})
	t.Run("flags parse in any order with equals form", func(t *testing.T) {
		f, err := parseRunsFlags([]string{"--pipeline=daily", "--limit", "10", "--json"})
		if err != nil {
			t.Fatalf("parseRunsFlags: %v", err)
		}
		if f.pipeline != "daily" || f.limit != "10" || !f.asJSON {
			t.Errorf("parsed = %+v", f)
		}
	})
}
