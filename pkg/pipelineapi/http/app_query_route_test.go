package http_test

// Integration test for the RFC 028 P5 app-principal query route: a direct
// HTTP call mimicking app-worker's APIClient.Query (cmd/app-worker/ does not
// exist yet — Part 3 — so this drives the same wire contract it will use)
// through the FULL pipeline-api HTTP stack (real net/http.Server via
// httptest, real ServeMux registration from server.go) to a fake
// query-worker. A gate-level unit test alone (queryproxy/app_query_test.go)
// cannot prove two things this test is the only place that can prove:
//   - the principal set by the app-token branch actually reaches the
//     query_audit log line as emitted by the real, fully-wired handler
//     chain (not just the *handler the queryproxy package tests construct
//     directly);
//   - a blocking query is cancelled end-to-end when the CALLER's request
//     context is cancelled — this requires a real network round trip
//     (client → pipeline-api → query-worker), which an httptest.ResponseRecorder
//     cannot exercise.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate/projectgatetest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/queryproxy"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// syncBuffer is a mutex-guarded io.Writer so a slog handler written from an
// httptest server's request-handling goroutine can be safely read back on
// the test goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newAppQueryTestServer wires a minimal-but-real pipeline-api Server (no DB
// pool needed — the app query route touches neither) with a *queryproxy.Core
// pointed at workerURL, and returns an httptest.Server exposing it over a
// real listener.
func newAppQueryTestServer(t *testing.T, workerURL, lkPID, warehouse string, signer *tokens.Signer) *httptest.Server {
	t.Helper()
	core, err := queryproxy.NewCore(queryproxy.Config{
		WorkerURL: workerURL,
		Gate:      projectgatetest.AllowAll(lkPID, warehouse),
	}, signer)
	if err != nil {
		t.Fatalf("queryproxy.NewCore: %v", err)
	}
	srv := apihttp.NewServer(nil).WithSigner(signer).WithQueryCore(core)
	return httptest.NewServer(srv.Handler())
}

// TestAppQueryRoute_NotConfigured mirrors TestServer_QueryRoute_NotConfigured
// (server_test.go) for the new app-principal route: absent query-service
// wiring, POST /internal/v1/projects/{pid}/query must 404 rather than panic
// or fall through to some other handler.
func TestAppQueryRoute_NotConfigured(t *testing.T) {
	srv := apihttp.NewServer(nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	pid := uuid.NewString()
	resp, err := http.Post(ts.URL+"/internal/v1/projects/"+pid+"/query", "application/json", strings.NewReader(`{"sql":"SELECT 1"}`))
	if err != nil {
		t.Fatalf("POST /internal/v1/projects/%s/query: %v", pid, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestAppQueryRoute_PrincipalReachesAudit(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`))
	}))
	defer worker.Close()

	signer := mustNewSigner(t)
	appID := uuid.NewString()
	lkPID := "lk-proj-audit-int"
	ts := newAppQueryTestServer(t, worker.URL, lkPID, "wh", signer)
	defer ts.Close()

	tok, jti, err := tokens.MintAppToken(signer, appID, lkPID)
	if err != nil {
		t.Fatalf("MintAppToken: %v", err)
	}
	wantSub := "app-" + appID

	var logBuf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	defer slog.SetDefault(prev)

	pid := uuid.NewString()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/internal/v1/projects/"+pid+"/query",
		strings.NewReader(`{"sql":"SELECT 1"}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok.Reveal())
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	line := findAuditLineInLogs(t, logBuf.String())
	if line["principal"] != wantSub {
		t.Fatalf("query_audit principal = %v, want %q (the app's JWT subject, RFC 028 P5)", line["principal"], wantSub)
	}
	if line["jti"] != jti {
		t.Fatalf("query_audit jti = %v, want %q (the app token's own jti — joins back to impersonation_minted)", line["jti"], jti)
	}
	if line["outcome"] != "ok" {
		t.Fatalf("query_audit outcome = %v, want ok", line["outcome"])
	}
}

func findAuditLineInLogs(t *testing.T, logs string) map[string]any {
	t.Helper()
	for _, ln := range strings.Split(strings.TrimSpace(logs), "\n") {
		if ln == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			continue
		}
		if m["msg"] == "query_audit" {
			return m
		}
	}
	t.Fatalf("no query_audit line found in captured logs: %s", logs)
	return nil
}

func TestAppQueryRoute_CancelledContextAbortsBlockingQuery(t *testing.T) {
	// The fake query-worker blocks on the REQUEST'S OWN context (as the
	// real query-worker would while a long-running DuckDB statement holds
	// the connection open) rather than on a fixed sleep, so this proves
	// real end-to-end propagation rather than a coincidence of timing.
	// workerNaturalCompletion bounds how long the fake worker blocks when
	// cancellation does NOT arrive, purely so this fake server can never
	// hang httptest.Server.Close() (which waits for in-flight handlers) if
	// propagation were broken — it is not itself part of the assertion.
	// The assertion below is stronger and more deterministic than "did the
	// worker observe r.Context().Done()" (whose timing depends on OS-level
	// socket teardown between two localhost servers, which is not something
	// this code controls): the client request must return in well under
	// workerNaturalCompletion, proving it was actually aborted rather than
	// merely outlasted by the worker's own fallback.
	const workerNaturalCompletion = 3 * time.Second
	entered := make(chan struct{})
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-r.Context().Done():
		case <-time.After(workerNaturalCompletion):
		}
	}))
	defer worker.Close()

	signer := mustNewSigner(t)
	appID := uuid.NewString()
	lkPID := "lk-proj-cancel-int"
	ts := newAppQueryTestServer(t, worker.URL, lkPID, "wh", signer)
	defer ts.Close()

	tok, _, err := tokens.MintAppToken(signer, appID, lkPID)
	if err != nil {
		t.Fatalf("MintAppToken: %v", err)
	}

	pid := uuid.NewString()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/internal/v1/projects/"+pid+"/query",
		strings.NewReader(`{"sql":"SELECT 1"}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok.Reveal())
	req.Header.Set("Content-Type", "application/json")

	type outcome struct {
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- outcome{err: err}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("query-worker never received the request")
	}

	cancel()
	cancelledAt := time.Now()

	// Bound is well under workerNaturalCompletion: if propagation were
	// broken, this call would only return once the worker's own fallback
	// fires (workerNaturalCompletion), which this bound would catch.
	const wantReturnWithin = 1500 * time.Millisecond
	select {
	case o := <-done:
		if o.err == nil {
			t.Fatal("expected the client request to fail once its context was cancelled")
		}
		if elapsed := time.Since(cancelledAt); elapsed > wantReturnWithin {
			t.Fatalf("client request took %s to return after cancellation (want < %s) — "+
				"it appears to have waited for the worker's natural completion rather than "+
				"being aborted, meaning cancellation did not propagate to the outbound query",
				elapsed, wantReturnWithin)
		}
	case <-time.After(wantReturnWithin):
		t.Fatal("client request did not return promptly after its context was cancelled — cancellation propagation is broken")
	}
}
