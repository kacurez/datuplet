package queryengine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// catalogWriteRefused is the marker in the iceberg-REST error the proxy returns
// for a refused catalog write.
const catalogWriteRefused = "read-only query engine: catalog write refused"

// readOnlyCatalogProxy fronts the lakekeeper iceberg-REST catalog for one
// engine on a loopback port and refuses every catalog write: any method other
// than GET/HEAD, except the POST .../metrics scan report. It is the catalog-side
// guarantee that user SQL cannot commit, independent of whether an
// iceberg-extension function honours the READ_ONLY attach
// (iceberg_rewrite_data_files in duckdb-iceberg 4c0c0874 does not).
//
// It never logs: requests carry the bearer JWT and loadTable responses carry
// vended storage credentials.
type readOnlyCatalogProxy struct {
	srv       *http.Server
	transport *http.Transport
	url       string
}

func startReadOnlyCatalogProxy(upstream string) (*readOnlyCatalogProxy, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse catalog URL: %w", err)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, fmt.Errorf("catalog URL scheme %q: want http or https", target.Scheme)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &readOnlyCatalogProxy{
		transport: http.DefaultTransport.(*http.Transport).Clone(),
		url:       "http://" + ln.Addr().String() + strings.TrimSuffix(target.Path, "/"),
	}

	rp := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: target.Scheme, Host: target.Host})
	rp.Transport = p.transport
	base := rp.Director
	rp.Director = func(r *http.Request) {
		base(r)
		r.Host = target.Host
		r.Header.Del("Accept-Encoding") // keep /v1/config rewritable
	}
	rp.ModifyResponse = p.rewriteConfig
	// Keep the real upstream cause (dial/TLS error) visible in the attach error
	// instead of a bare 502. Transport errors carry the URL, never headers.
	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		writeIcebergError(w, http.StatusBadGateway, "ServiceUnavailableException",
			"read-only catalog proxy: upstream "+target.Host+": "+err.Error())
	}
	rp.ErrorLog = log.New(io.Discard, "", 0)

	p.srv = &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !catalogRequestAllowed(r) {
				writeIcebergError(w, http.StatusForbidden, "ForbiddenException", catalogWriteRefused)
				return
			}
			rp.ServeHTTP(w, r)
		}),
		ErrorLog: log.New(io.Discard, "", 0),
	}
	go func() { _ = p.srv.Serve(ln) }()
	return p, nil
}

// rewriteConfig points any `uri` in the /v1/config handshake (overrides or
// defaults) back at the proxy, so the catalog cannot steer the extension past it.
func (p *readOnlyCatalogProxy) rewriteConfig(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK || !strings.HasSuffix(resp.Request.URL.Path, "/v1/config") {
		return nil
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	var cfg map[string]any
	if json.Unmarshal(b, &cfg) == nil {
		for _, k := range []string{"overrides", "defaults"} {
			if m, ok := cfg[k].(map[string]any); ok {
				if _, has := m["uri"]; has {
					m["uri"] = p.url
				}
			}
		}
		if nb, err := json.Marshal(cfg); err == nil {
			b = nb
		}
	}
	resp.Body = io.NopCloser(bytes.NewReader(b))
	resp.ContentLength = int64(len(b))
	resp.Header.Set("Content-Length", strconv.Itoa(len(b)))
	return nil
}

func catalogRequestAllowed(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return true
	case http.MethodPost:
		// The scan report, POST /v1/{prefix}/namespaces/{ns}/tables/{t}/metrics.
		// A cleaned path only, so "/commit/../metrics" cannot pass the suffix test.
		p := r.URL.Path
		return path.Clean(p) == p && strings.Contains(p, "/tables/") && strings.HasSuffix(p, "/metrics")
	}
	return false
}

func writeIcebergError(w http.ResponseWriter, code int, typ, msg string) {
	body, _ := json.Marshal(map[string]any{"error": map[string]any{"message": msg, "type": typ, "code": code}})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func (p *readOnlyCatalogProxy) Close() error {
	err := p.srv.Close()
	p.transport.CloseIdleConnections()
	return err
}
