package queryengine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// TestReadOnlyCatalogProxy drives the proxy against an httptest upstream: reads
// and the metrics report pass through with the bearer intact, every write is
// refused with the iceberg-shaped 403 and never reaches the upstream, and the
// /v1/config uri is pointed back at the proxy.
func TestReadOnlyCatalogProxy(t *testing.T) {
	const bearer = "Bearer test-token"
	var (
		mu    sync.Mutex
		hits  []string
		auths []string
	)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits = append(hits, r.Method+" "+r.URL.Path)
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/catalog/v1/config" {
			_, _ = io.WriteString(w, `{"defaults":{"uri":"http://evil.example/catalog"},"overrides":{"uri":"http://evil.example/catalog","prefix":"p"}}`)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer up.Close()

	p, err := startReadOnlyCatalogProxy(up.URL + "/catalog")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if !strings.HasPrefix(p.url, "http://127.0.0.1:") || !strings.HasSuffix(p.url, "/catalog") {
		t.Fatalf("proxy url = %q, want a 127.0.0.1 loopback URL ending in /catalog", p.url)
	}

	do := func(method, path string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, p.url+path, strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", bearer)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	upstreamHits := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(hits)
	}

	forwarded := []struct{ method, path string }{
		{http.MethodGet, "/v1/p/namespaces/ns/tables/t"},
		{http.MethodHead, "/v1/p/namespaces/ns/tables/t"},
		{http.MethodPost, "/v1/p/namespaces/ns/tables/t/metrics"},
	}
	for _, c := range forwarded {
		n := upstreamHits()
		if resp := do(c.method, c.path); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s = %d, want 200 (forwarded)", c.method, c.path, resp.StatusCode)
		}
		if upstreamHits() != n+1 {
			t.Fatalf("%s %s did not reach the upstream", c.method, c.path)
		}
	}

	refused := []struct{ method, path string }{
		{http.MethodPost, "/v1/p/transactions/commit"},
		{http.MethodPost, "/v1/p/namespaces/ns/tables/t"},
		{http.MethodPost, "/v1/p/namespaces/ns/tables/t/../../../../transactions/commit/../metrics"},
		{http.MethodPost, "/v1/p/namespaces"},
		{http.MethodDelete, "/v1/p/namespaces/ns/tables/t"},
		{http.MethodPut, "/v1/p/namespaces/ns/tables/t"},
	}
	before := upstreamHits()
	for _, c := range refused {
		resp := do(c.method, c.path)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s = %d, want 403", c.method, c.path, resp.StatusCode)
		}
		var body struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    int    `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("%s %s: 403 body is not iceberg-REST JSON: %v", c.method, c.path, err)
		}
		if !strings.Contains(body.Error.Message, catalogWriteRefused) || body.Error.Code != http.StatusForbidden || body.Error.Type == "" {
			t.Fatalf("%s %s: 403 body = %+v, want the %q iceberg error", c.method, c.path, body.Error, catalogWriteRefused)
		}
	}
	if n := upstreamHits() - before; n != 0 {
		t.Fatalf("%d refused write(s) reached the upstream: %v", n, hits[before:])
	}

	resp := do(http.MethodGet, "/v1/config")
	var cfg struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		t.Fatalf("decode /v1/config: %v", err)
	}
	if cfg.Defaults["uri"] != p.url || cfg.Overrides["uri"] != p.url {
		t.Fatalf("/v1/config uri not rewritten to the proxy %q: defaults=%v overrides=%v", p.url, cfg.Defaults, cfg.Overrides)
	}
	if cfg.Overrides["prefix"] != "p" {
		t.Fatalf("/v1/config rewrite lost other keys: %v", cfg.Overrides)
	}

	mu.Lock()
	defer mu.Unlock()
	for i, a := range auths {
		if a != bearer {
			t.Fatalf("upstream request %q carried Authorization %q, want it passed through", hits[i], a)
		}
	}
}

// TestReadOnlyCatalogProxyUpstreamError keeps the real upstream cause visible
// when the catalog is unreachable, and Close stops the listener.
func TestReadOnlyCatalogProxyUpstreamError(t *testing.T) {
	p, err := startReadOnlyCatalogProxy("http://127.0.0.1:1/catalog")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(p.url + "/v1/config")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(string(b), "127.0.0.1:1") || !strings.Contains(string(b), "refused") {
		t.Fatalf("unreachable upstream: %d %s, want 502 naming the upstream cause", resp.StatusCode, b)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := http.Get(p.url + "/v1/config"); err == nil {
		t.Fatal("proxy still serving after Close")
	}
}
