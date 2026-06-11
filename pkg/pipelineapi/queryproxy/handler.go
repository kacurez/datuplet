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
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

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
//
// Audit emission (RFC 022 §5.5) is wired automatically: the package-level
// promauto counter (pipelineapi_query_requests_total) is used. For tests
// that need an isolated counter, use HandlerWithAudit instead.
func Handler(cfg Config, signer *tokens.Signer) (http.Handler, error) {
	return HandlerWithAudit(cfg, signer, nil) // nil → use package-level promauto counter
}

// handler is the concrete POST /api/v1/query implementation.
type handler struct {
	cfg          Config
	signer       *tokens.Signer
	client       *workerClient
	gate         *gate
	auditCounter *prometheus.CounterVec // nil → use package-level promauto counter
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Authenticated user from ctx (auth.WithUser puts it there). The
	//    middleware normally guarantees this; the 401 is defence in depth.
	//    Unauthenticated requests do NOT emit an audit record — without a
	//    principal there is nothing to correlate with lakekeeper logs.
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "not authenticated")
		return
	}
	sub := user.ID.String()

	// Steps 2–6 run under the audit deferred emit (serveWithAudit). The
	// deferred emit fires exactly once regardless of which exit path is
	// taken — the structural guarantee replaces per-branch discipline.
	h.serveWithAudit(w, r, sub, time.Now())
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
