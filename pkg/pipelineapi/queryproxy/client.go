package queryproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// workerRequest is the JSON body POSTed to the query-worker's
// /internal/query endpoint. The shape mirrors the worker's wire contract
// (components/queryengine/cmd/query-worker/server.go: queryRequest). The
// CatalogJWT field carries the query-scoped catalog JWT verbatim; it is
// set from the catalog token's Reveal() at the handler call site (the
// second bearer-credential audit point — see handler.go).
type workerRequest struct {
	SQL        string `json:"sql"`
	CatalogJWT string `json:"catalog_jwt"`
	Warehouse  string `json:"warehouse"`
	TimeoutS   int    `json:"timeout_s"`
	MaxRows    int    `json:"max_rows"`
	MaxBytes   int    `json:"max_bytes"`
}

// workerResponse is the decoded outcome of one /internal/query call. The
// body is carried as raw bytes: on the 200 path the handler streams it
// straight through to the client (no re-encode of the rows), and on the
// error path the handler inspects the worker's {"error","kind"} JSON.
type workerResponse struct {
	status int
	body   []byte
}

// workerClient is the HTTP client for the pipeline-api → query-worker hop.
type workerClient struct {
	base    *url.URL
	hc      *http.Client
	respCap int64 // hard bound on bytes buffered from a worker response
}

// newWorkerClient parses workerURL and builds a client whose timeout sits
// just ABOVE the worker's own maximum (maxTimeoutS + slack). The ordering
// is deliberate: the worker enforces the real query timeout via DuckDB
// interrupt and returns a structured 408; if our transport timeout fired
// first we would lose that classification and surface an opaque 502. The
// extra slack covers the worker's response-serialisation + network time.
func newWorkerClient(workerURL string, maxTimeoutS int, slack time.Duration, maxRespBytes int) (*workerClient, error) {
	if workerURL == "" {
		return nil, fmt.Errorf("queryproxy: WorkerURL is required")
	}
	u, err := url.Parse(workerURL)
	if err != nil {
		return nil, fmt.Errorf("queryproxy: parse WorkerURL: %w", err)
	}
	// respCap bounds what we buffer from the worker (defense in depth
	// against a buggy/compromised worker): the worker's own MaxBytes
	// ceiling governs the logical result size; 2x + 64KiB covers JSON
	// envelope overhead.
	return &workerClient{
		base:    u,
		respCap: int64(maxRespBytes)*2 + 64*1024,
		hc: &http.Client{
			Timeout: time.Duration(maxTimeoutS)*time.Second + slack,
			// The worker URL is static cluster-internal config; a redirect
			// can only mean misconfiguration or an active attempt to move
			// the Bearer header elsewhere. Refuse them all.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return fmt.Errorf("queryproxy: unexpected redirect from query-worker (refused)")
			},
		},
	}, nil
}

// Do POSTs body to {base}/internal/query with the internal-query JWT in
// the Authorization header and returns the worker's status + raw body.
//
// SECURITY: the internalToken.Reveal() call below is the ONLY site that
// un-redacts the internal-query bearer credential — the audit point for
// the pipeline-api → query-worker hop. The catalog JWT inside body is
// likewise revealed once, at the handler call site that builds body.
// Neither the token nor the request body is ever logged here. Transport
// errors are wrapped WITHOUT the path/query (which is fixed and
// secret-free anyway); the host is fine to surface for debugging.
func (c *workerClient) Do(ctx context.Context, internalToken tokens.QueryToken, body workerRequest) (workerResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return workerResponse{}, fmt.Errorf("queryproxy: marshal worker request: %w", err)
	}

	endpoint := c.base.JoinPath("internal", "query").String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return workerResponse{}, fmt.Errorf("queryproxy: build worker request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// AUDIT POINT (internal hop): the sole Reveal() of the internal-query
	// token. Sets the Bearer header the worker's verifier checks.
	req.Header.Set("Authorization", "Bearer "+internalToken.Reveal())

	resp, err := c.hc.Do(req)
	if err != nil {
		// Wrap with host only (no token, no body). req.URL.Host is
		// secret-free; the query/path carries nothing sensitive.
		return workerResponse{}, fmt.Errorf("queryproxy: call query-worker %s: %w", c.base.Host, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, c.respCap+1))
	if err != nil {
		return workerResponse{}, fmt.Errorf("queryproxy: read query-worker response from %s: %w", c.base.Host, err)
	}
	if int64(len(respBody)) > c.respCap {
		return workerResponse{}, fmt.Errorf("queryproxy: query-worker response exceeded %d-byte cap", c.respCap)
	}
	return workerResponse{status: resp.StatusCode, body: respBody}, nil
}
