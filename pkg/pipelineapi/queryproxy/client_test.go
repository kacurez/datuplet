package queryproxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// testInternalToken is a fixed internal-query token fixture shared by the
// retry tests below (mirrors the inline tokens.QueryToken(...) literals used
// throughout this file).
func testInternalToken(t *testing.T) tokens.QueryToken {
	t.Helper()
	return tokens.QueryToken("a.b.c")
}

func TestWorkerClient_Do_SendsBearerAndBody(t *testing.T) {
	const internalJWT = "internal-jwt-secret-value"
	const catalogJWT = "catalog-jwt-secret-value"

	var gotAuth string
	var gotReq workerRequest
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`))
	}))
	defer srv.Close()

	c, err := newWorkerClient(srv.URL, 300, 30*time.Second, 10<<20)
	if err != nil {
		t.Fatalf("newWorkerClient: %v", err)
	}

	body := workerRequest{
		SQL:        "SELECT 1",
		CatalogJWT: catalogJWT,
		Warehouse:  "proj/wh",
		TimeoutS:   60,
		MaxRows:    1000,
		MaxBytes:   1024,
	}
	resp, err := c.Do(context.Background(), tokens.QueryToken(internalJWT), body)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if resp.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.status)
	}
	if gotPath != "/internal/query" {
		t.Fatalf("path = %q, want /internal/query", gotPath)
	}
	// Proves the internal-token Reveal() is wired into the Bearer header.
	if gotAuth != "Bearer "+internalJWT {
		t.Fatalf("Authorization = %q, want Bearer %s", gotAuth, internalJWT)
	}
	// Proves the catalog JWT travels verbatim in the body.
	if gotReq.CatalogJWT != catalogJWT {
		t.Fatalf("catalog_jwt = %q, want %q", gotReq.CatalogJWT, catalogJWT)
	}
	if gotReq.SQL != "SELECT 1" || gotReq.Warehouse != "proj/wh" {
		t.Fatalf("unexpected body: %+v", gotReq)
	}
}

func TestWorkerClient_Do_PassesThroughErrorBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestTimeout)
		_, _ = w.Write([]byte(`{"error":"query timed out","kind":"timeout"}`))
	}))
	defer srv.Close()

	c, _ := newWorkerClient(srv.URL, 300, 30*time.Second, 10<<20)
	resp, err := c.Do(context.Background(), tokens.QueryToken("x"), workerRequest{SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.status != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408", resp.status)
	}
	if !strings.Contains(string(resp.body), `"kind":"timeout"`) {
		t.Fatalf("body = %q, want timeout kind", resp.body)
	}
}

func TestWorkerClient_Do_TransportErrorOmitsTokenAndHasHost(t *testing.T) {
	// Point at a closed port so Do returns a transport error.
	c, err := newWorkerClient("http://127.0.0.1:1/", 300, 30*time.Second, 10<<20)
	if err != nil {
		t.Fatalf("newWorkerClient: %v", err)
	}
	const internalJWT = "do-not-log-me"
	_, doErr := c.Do(context.Background(), tokens.QueryToken(internalJWT), workerRequest{
		SQL:        "SELECT 1",
		CatalogJWT: "catalog-do-not-log",
	})
	if doErr == nil {
		t.Fatal("expected transport error")
	}
	msg := doErr.Error()
	if strings.Contains(msg, internalJWT) || strings.Contains(msg, "catalog-do-not-log") {
		t.Fatalf("transport error leaked a token: %q", msg)
	}
	// Host is fine (and useful) to include.
	if !strings.Contains(msg, "127.0.0.1:1") {
		t.Fatalf("transport error should name the host, got %q", msg)
	}
}

// TestDo_ResponseCapBoundsBuffering pins the defense-in-depth bound on what
// the proxy will buffer from a misbehaving worker (review finding: unbounded
// io.ReadAll).
func TestDo_ResponseCapBoundsBuffering(t *testing.T) {
	huge := strings.Repeat("x", 1<<20) // 1 MiB of junk
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < 8; i++ { // 8 MiB total, cap below
			_, _ = w.Write([]byte(huge))
		}
	}))
	defer srv.Close()

	c, err := newWorkerClient(srv.URL, 300, 30*time.Second, 1<<20) // cap = 2MiB+64KiB
	if err != nil {
		t.Fatalf("newWorkerClient: %v", err)
	}
	_, doErr := c.Do(context.Background(), tokens.QueryToken("t.t.t"), workerRequest{SQL: "SELECT 1"})
	if doErr == nil || !strings.Contains(doErr.Error(), "cap") {
		t.Fatalf("expected response-cap error, got %v", doErr)
	}
}

// TestDo_RefusesRedirects pins the CheckRedirect policy: the worker URL is
// static cluster config; any redirect means misconfiguration or an attempt
// to move the Bearer header elsewhere.
func TestDo_RefusesRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:1/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	c, err := newWorkerClient(srv.URL, 300, 30*time.Second, 10<<20)
	if err != nil {
		t.Fatalf("newWorkerClient: %v", err)
	}
	_, doErr := c.Do(context.Background(), tokens.QueryToken("t.t.t"), workerRequest{SQL: "SELECT 1"})
	if doErr == nil || !strings.Contains(doErr.Error(), "redirect") {
		t.Fatalf("expected redirect-refused error, got %v", doErr)
	}
}

// TestWorkerClient_RetriesOn429 pins the capacity-retry behavior: a worker
// 429 (admission rejection, safe-by-construction pre-execution) is retried
// exactly once. The retry succeeds if a different backend picks it up; a
// worker that always 429s is hit exactly twice (no more), and Do surfaces the
// final 429 rather than looping or erroring.
func TestWorkerClient_RetriesOn429(t *testing.T) {
	cases := []struct {
		name       string
		alwaysBusy bool
		wantStatus int
		wantHits   int32
	}{
		{"retry_succeeds", false, http.StatusOK, 2},
		{"no_second_retry", true, http.StatusTooManyRequests, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hits int32
			worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if n := atomic.AddInt32(&hits, 1); tc.alwaysBusy || n == 1 {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte(`{"error":"server at capacity","kind":"capacity"}`))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"schema":[],"rows":[],"truncated":false,"stats":{"duration_ms":1}}`))
			}))
			defer worker.Close()

			c, err := newWorkerClient(worker.URL, 300, 30*time.Second, 10*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.Do(context.Background(), testInternalToken(t), workerRequest{SQL: "SELECT 1", CatalogJWT: "a.b.c", Warehouse: "p/w"})
			if err != nil {
				t.Fatal(err)
			}
			if resp.status != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.status, tc.wantStatus)
			}
			if got := atomic.LoadInt32(&hits); got != tc.wantHits {
				t.Fatalf("worker hits = %d, want %d (exactly one retry)", got, tc.wantHits)
			}
		})
	}
}
