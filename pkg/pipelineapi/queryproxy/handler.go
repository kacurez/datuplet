// Package queryproxy implements the pipeline-api side of the RFC 022
// ad-hoc SQL query path (mode a): the authenticated POST /api/v1/query
// handler. It clamps the request's resource limits to operator-configured
// ceilings, enforces a per-principal in-flight cap, mints the two
// short-lived JWTs the hop needs (a query-scoped catalog JWT the worker
// presents to lakekeeper, and an internal-query JWT that authenticates the
// pipeline-api → query-worker call), proxies to the query-worker Service,
// and translates the worker's wire outcomes into client-facing HTTP
// status codes.
//
// Route wiring (POST /api/v1/query in pkg/pipelineapi/http) is Task 2.5;
// this package only provides the handler + its dependencies.
package queryproxy

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// Default resource-limit constants for Config (RFC 022 §5.2/§5.4). These
// are the package defaults applied when Config leaves a field zero; the
// real values come from the §5.4 `query:` chart block via env in main.go
// (wiring is Task 2.5).
const (
	defaultTimeoutS = 60
	maxTimeoutS     = 300

	defaultMaxRows = 1000
	maxMaxRows     = 10000

	defaultMaxBytes = 1 * 1024 * 1024  // 1 MiB
	maxMaxBytes     = 10 * 1024 * 1024 // 10 MiB

	perPrincipalInflight = 2

	// catalogTTLSlack pads the catalog JWT's TTL past the clamped query
	// timeout so the token stays valid for the whole query (including the
	// worker's response time). MintQueryToken clamps the final TTL to
	// MaxQueryTokenLifetime (330s) regardless.
	catalogTTLSlack = 30 * time.Second
)

// maxBodyBytes caps the request body the handler will read. The body is
// just {sql,timeout_s,max_rows,max_bytes}; 64 KiB is a generous ceiling
// on SQL text that still rejects an abusive payload before it reaches the
// worker (the worker imposes its own 1 MiB cap downstream).
const maxBodyBytes = 64 * 1024

// Config holds the operator-tunable knobs for the query proxy. Values come
// from the §5.4 `query:` chart block via env in main.go (Task 2.5); the
// zero value of each numeric/duration field is replaced by the package
// default in newConfig.
type Config struct {
	// WorkerURL is the query-worker Service base URL, e.g.
	// "http://query-worker.datuplet.svc.cluster.local:8080". Required.
	WorkerURL string

	DefaultTimeoutS int // default 60
	MaxTimeoutS     int // default 300

	DefaultMaxRows int // default 1000
	MaxMaxRows     int // default 10000

	DefaultMaxBytes int // default 1 MiB
	MaxMaxBytes     int // default 10 MiB

	// PerPrincipalInflight is the per-caller concurrent-query cap (§5.2).
	// Default 2.
	PerPrincipalInflight int

	// Warehouse is the project-qualified "projectID/warehouse" opaque
	// string the worker passes to lakekeeper's iceberg-REST attach (§6.1:
	// the attach arg MUST be project-qualified). It comes from the same
	// source the storage handlers resolve their warehouse from
	// (storage warehouse_resolver.go / catalog_proxy.go); pipeline-api
	// forwards it verbatim. Required for queries to resolve tables.
	Warehouse string

	// CatalogTTLSlack pads the catalog JWT TTL beyond the clamped timeout.
	// Default 30s.
	CatalogTTLSlack time.Duration
}

// withDefaults returns a copy of cfg with zero-valued fields filled from
// the package defaults.
func (c Config) withDefaults() Config {
	if c.DefaultTimeoutS <= 0 {
		c.DefaultTimeoutS = defaultTimeoutS
	}
	if c.MaxTimeoutS <= 0 {
		c.MaxTimeoutS = maxTimeoutS
	}
	if c.DefaultMaxRows <= 0 {
		c.DefaultMaxRows = defaultMaxRows
	}
	if c.MaxMaxRows <= 0 {
		c.MaxMaxRows = maxMaxRows
	}
	if c.DefaultMaxBytes <= 0 {
		c.DefaultMaxBytes = defaultMaxBytes
	}
	if c.MaxMaxBytes <= 0 {
		c.MaxMaxBytes = maxMaxBytes
	}
	if c.PerPrincipalInflight <= 0 {
		c.PerPrincipalInflight = perPrincipalInflight
	}
	if c.CatalogTTLSlack <= 0 {
		c.CatalogTTLSlack = catalogTTLSlack
	}
	return c
}

// queryRequest is the client-facing JSON body for POST /api/v1/query. Only
// `sql` is required; the limit fields are optional and clamped server-side.
type queryRequest struct {
	SQL      string `json:"sql"`
	TimeoutS *int   `json:"timeout_s,omitempty"`
	MaxRows  *int   `json:"max_rows,omitempty"`
	MaxBytes *int   `json:"max_bytes,omitempty"`
}

// errorBody is the JSON error envelope returned to the client:
// {"error": "...", "kind": "..."}. Mirrors the query-worker's wire shape
// so a kind passed straight through (e.g. sql_error) stays stable end to
// end.
type errorBody struct {
	Error string `json:"error"`
	Kind  string `json:"kind"`
}

// Handler builds the POST /api/v1/query http.Handler. It is constructed
// from operator config + the JWT signer (the same *tokens.Signer wired
// onto the rest of pipeline-api); the worker client and per-principal gate
// are derived from cfg internally so callers (Task 2.5) pass only the two
// long-lived dependencies. It returns an error when WorkerURL is unset.
//
// The handler assumes it runs behind auth.WithUser, which guarantees an
// authenticated *store.User in the request context; it still defends with
// a 401 if absent.
func Handler(cfg Config, signer *tokens.Signer) (http.Handler, error) {
	cfg = cfg.withDefaults()
	if signer == nil {
		return nil, errors.New("queryproxy: signer is required")
	}
	client, err := newWorkerClient(cfg.WorkerURL, cfg.MaxTimeoutS, cfg.CatalogTTLSlack, cfg.MaxMaxBytes)
	if err != nil {
		return nil, err
	}
	g := newGate(cfg.PerPrincipalInflight)
	return &handler{cfg: cfg, signer: signer, client: client, gate: g}, nil
}

// handler is the concrete POST /api/v1/query implementation.
type handler struct {
	cfg    Config
	signer *tokens.Signer
	client *workerClient
	gate   *gate
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Authenticated user from ctx (auth.WithUser puts it there). The
	//    middleware normally guarantees this; the 401 is defence in depth.
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "not authenticated")
		return
	}
	sub := user.ID.String()

	// 2. Decode + validate the body (≤64 KiB SQL-text cap).
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid request body")
		return
	}
	if req.SQL == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "sql is required")
		return
	}

	// 3. Clamp limits to [1..max], default when unset/non-positive.
	timeoutS := clamp(req.TimeoutS, h.cfg.DefaultTimeoutS, h.cfg.MaxTimeoutS)
	maxRows := clamp(req.MaxRows, h.cfg.DefaultMaxRows, h.cfg.MaxMaxRows)
	maxBytes := clamp(req.MaxBytes, h.cfg.DefaultMaxBytes, h.cfg.MaxMaxBytes)

	// 4. Per-principal in-flight gate (§5.2). A full gate is the CALLER's
	//    own concurrency exceeding its budget → 429 rate_limited, retry
	//    helps once one of their queries finishes. (Distinct from the
	//    worker-wide saturation case in step 6, which is 503 capacity.)
	if !h.gate.Acquire(sub) {
		w.Header().Set("Retry-After", "2")
		writeError(w, http.StatusTooManyRequests, "rate_limited",
			"too many queries in flight for this principal")
		return
	}
	defer h.gate.Release(sub)

	// 5. Mint the two short-lived JWTs. TTL = clamped timeout + slack so
	//    the catalog token outlives the query; MintQueryToken clamps to
	//    330s regardless. A mint failure is a server-side/config problem.
	ttl := time.Duration(timeoutS)*time.Second + h.cfg.CatalogTTLSlack
	catalogTok, err := tokens.MintQueryToken(r.Context(), h.signer, ttl)
	if err != nil {
		// Redacted: catalogTok is a redacting QueryToken; err carries no JWT.
		slog.Error("queryproxy: mint catalog token failed", "sub", sub, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "failed to mint query credentials")
		return
	}
	internalTok, err := tokens.MintInternalQueryToken(r.Context(), h.signer, ttl)
	if err != nil {
		slog.Error("queryproxy: mint internal token failed", "sub", sub, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "failed to mint query credentials")
		return
	}

	// 6. Proxy to the query-worker. AUDIT POINT (catalog hop): catalogTok
	//    is revealed exactly once here, into the worker-request body that
	//    the worker forwards to lakekeeper. The internal token is revealed
	//    once inside client.Do for the Authorization header.
	body := workerRequest{
		SQL:        req.SQL,
		CatalogJWT: catalogTok.Reveal(),
		Warehouse:  h.cfg.Warehouse,
		TimeoutS:   timeoutS,
		MaxRows:    maxRows,
		MaxBytes:   maxBytes,
	}
	resp, err := h.client.Do(r.Context(), internalTok, body)
	if err != nil {
		// Transport error / connection refused: the worker is unreachable.
		// err is host-only, no token/body — safe to log.
		slog.Error("queryproxy: query-worker transport error", "sub", sub, "err", err)
		writeError(w, http.StatusBadGateway, "internal", "query service unavailable")
		return
	}

	h.translate(w, sub, resp)
}

// translate maps the worker's HTTP outcome to the client-facing response.
//
//   - 200 → passthrough: copy the worker's result bytes verbatim (do NOT
//     re-decode/re-encode the rows) and set Content-Type. This preserves
//     the queryengine Result JSON exactly, including numeric fidelity.
//   - timeout (worker 408) → 408 timeout.
//   - result_too_large / request_too_large (worker 413) → 413, passthrough.
//   - capacity (worker 429) → 503 capacity + Retry-After. This is the
//     WORKER being saturated (its per-pod admission semaphore is full),
//     which is a SERVER-side condition, NOT the caller exceeding their own
//     budget. We deliberately distinguish it from the per-principal gate's
//     429 rate_limited (step 4): a 429 here would falsely tell the caller
//     "you are issuing too many queries" when in fact the shared worker is
//     busy. RFC §10 conflates both into 429 rate_limited; we diverge for a
//     clearer client signal and document it here.
//   - sql_error / bad_request (worker 400) → 400, passthrough the worker's
//     error text so the user sees the DuckDB/parse message.
//   - unauthorized (worker 401) → 502 internal: our internal-query token
//     was rejected by the worker. That is a config/clock/key-rotation bug
//     on our side, never the user's fault — surface it as a gateway error,
//     not a 401 (which would wrongly tell the user to re-authenticate).
//   - anything else → 502 internal.
func (h *handler) translate(w http.ResponseWriter, sub string, resp workerResponse) {
	kind := decodeKind(resp.body)

	switch resp.status {
	case http.StatusOK:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(resp.body)
		return
	case http.StatusRequestTimeout:
		writeError(w, http.StatusRequestTimeout, "timeout", workerMsg(resp.body, "query timed out"))
		return
	case http.StatusRequestEntityTooLarge:
		// Covers both result_too_large and request_too_large.
		k := kind
		if k == "" {
			k = "result_too_large"
		}
		writeError(w, http.StatusRequestEntityTooLarge, k, workerMsg(resp.body, "result too large"))
		return
	case http.StatusTooManyRequests:
		// Worker saturated (capacity) → 503, see method doc for the
		// 503-vs-429 rationale.
		w.Header().Set("Retry-After", "2")
		writeError(w, http.StatusServiceUnavailable, "capacity", "query service is busy, retry shortly")
		return
	case http.StatusBadRequest:
		// sql_error / bad_request: passthrough the worker's text + kind so
		// the user sees the engine's message.
		k := kind
		if k == "" {
			k = "sql_error"
		}
		writeError(w, http.StatusBadRequest, k, workerMsg(resp.body, "query failed"))
		return
	case http.StatusUnauthorized:
		// Internal token rejected — our bug, not the user's.
		slog.Error("queryproxy: query-worker rejected internal token (config/clock/key bug)", "sub", sub)
		writeError(w, http.StatusBadGateway, "internal", "query service unavailable")
		return
	default:
		slog.Error("queryproxy: unexpected query-worker status", "sub", sub, "status", resp.status)
		writeError(w, http.StatusBadGateway, "internal", "query service unavailable")
		return
	}
}

// clamp resolves an optional client-supplied limit: nil or non-positive →
// def; otherwise min(v, max). The lower bound is implicitly 1 because a
// supplied value ≤0 falls back to def (which is ≥1).
func clamp(v *int, def, max int) int {
	if v == nil || *v <= 0 {
		return def
	}
	if *v > max {
		return max
	}
	return *v
}

// decodeKind best-effort extracts the worker's "kind" field from an error
// body. Returns "" when the body is not the expected JSON shape.
func decodeKind(body []byte) string {
	var e errorBody
	if err := json.Unmarshal(body, &e); err != nil {
		return ""
	}
	return e.Kind
}

// workerMsg best-effort extracts the worker's "error" message, falling
// back to def when the body is not the expected JSON shape or is empty.
func workerMsg(body []byte, def string) string {
	var e errorBody
	if err := json.Unmarshal(body, &e); err != nil || e.Error == "" {
		return def
	}
	return e.Error
}

// writeError writes the {"error","kind"} JSON envelope with the given
// status. Never includes token or SQL material.
func writeError(w http.ResponseWriter, status int, kind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: msg, Kind: kind})
}
