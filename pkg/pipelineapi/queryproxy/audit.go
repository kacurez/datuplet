package queryproxy

// RFC 022 §5.5 structured audit emission for the query proxy.
//
// Exactly one audit record is emitted per request that passes authentication
// (i.e. once we have a principal sub). Unauthenticated requests produce no
// record: without a sub there is nothing to correlate with lakekeeper logs.
//
// For outcomes where no catalog token was minted (rate_limited when the
// per-principal gate fires before mint, bad_request before the SQL is known),
// the jti field is the empty string — this is documented and intentional.
// Similarly, statement_hash is empty when sql is empty (bad_request path
// before SQL validation).
//
// NOTE: lakekeeper FGA denials currently surface inside outcome=sql_error
// (the worker returns 400 with a DuckDB/lakekeeper error message). We do NOT
// string-match the worker error body for "403" to distinguish them: doing so
// would be fragile and lakekeeper's own logs carry the FGA detail. Reviewers
// should be aware that outcome=sql_error covers both user-SQL mistakes and
// lakekeeper-level authorization failures.
//
// Prometheus surface: pipelineapi_query_requests_total is a CounterVec
// labeled solely by "outcome" (low cardinality). The statement hash goes in
// the log line only — never in a metric label. /metrics exposure for
// pipeline-api is wired in pkg/pipelineapi/http/server.go (already done via
// promhttp); the counter below registers on the default registry and will
// appear there automatically. See Task 2.5 note in handler.go for route
// wiring.

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// queryRequestsTotal is the package-level Prometheus counter registered on
// the default registry via promauto. Labeled only by "outcome" to keep
// cardinality low; statement hashes live in the slog line only.
//
// /metrics exposure for pipeline-api is already wired in
// pkg/pipelineapi/http/server.go via promhttp on the default registry.
// This counter will appear there without further wiring (Task 2.5 note).
var queryRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "pipelineapi_query_requests_total",
	Help: "Total ad-hoc SQL query requests processed by the query proxy, labeled by outcome.",
}, []string{"outcome"})

// auditRecord holds the fields emitted in the single "query_audit" slog line
// for one request. All fields start at their zero values and are updated as
// the request progresses; the deferred emit in the handler captures the final
// state so exactly-one-emission is structural, not per-branch discipline.
//
// SECURITY INVARIANT: this struct must never hold raw SQL or a raw JWT string.
// statement_hash is the first 16 hex chars of SHA-256(sql); jti is extracted
// from the minted catalog token's payload (cross-system correlation ID only).
type auditRecord struct {
	principal     string  // authenticated subject UUID
	jti           string  // jti from the catalog token payload; "" if not minted
	statementHash string  // sha256[:16] hex of the raw SQL; "" for empty SQL
	durationMS    int64   // wall time of the full handler in milliseconds
	outcome       string  // ok / bad_request / rate_limited / timeout / result_too_large / capacity / sql_error / internal
	truncated     bool    // true only when the worker 200 body has {"truncated":true}
}

// emitAudit logs the record as a single slog.Info "query_audit" line and
// increments the Prometheus counter. counter may be nil (defensive); when nil
// only the log line is emitted.
//
// Called exactly once per authenticated request from a deferred closure in the
// handler — the deferred closure captures the mutable *auditRecord so it sees
// the final field values.
func emitAudit(logger *slog.Logger, counter *prometheus.CounterVec, rec *auditRecord) {
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("query_audit",
		"principal", rec.principal,
		"jti", rec.jti,
		"statement_hash", rec.statementHash,
		"duration_ms", rec.durationMS,
		"outcome", rec.outcome,
		"truncated", rec.truncated,
	)
	if counter != nil {
		counter.WithLabelValues(rec.outcome).Inc()
	}
}

// extractJTIFromToken performs an UNVERIFIED base64url-decode of the JWT
// payload segment to extract the "jti" claim. Verification is intentionally
// skipped: we minted this token ourselves moments ago, so re-verifying it
// would be circular and add latency. The result is used only as a
// cross-system correlation ID in the audit log.
//
// Returns "" when the token is malformed or carries no jti.
func extractJTIFromToken(rawJWT string) string {
	parts := strings.Split(rawJWT, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64URLDecode(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		JTI string `json:"jti"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return claims.JTI
}

// base64URLDecode decodes a standard base64url string (no padding required).
func base64URLDecode(s string) ([]byte, error) {
	// jwt segments are base64url without padding; add padding back.
	switch len(s) % 4 {
	case 1:
		// Residue 1 is structurally invalid base64 — fail explicitly
		// rather than relying on the decoder.
		return nil, errors.New("invalid base64url segment length")
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return base64.URLEncoding.DecodeString(s)
}

// base64URLEncode encodes b as base64url without padding (JWT segment format).
// Used in tests that build synthetic JWT payloads.
func base64URLEncode(b []byte) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString(b), "=")
}

// statementHash returns the first 16 hex characters of SHA-256(sql). Returns
// "" for an empty sql string. 16 hex chars (64 bits) gives bounded cardinality
// for log correlation while being collision-resistant enough for audit use.
// The raw SQL is never stored or logged anywhere.
func statementHash(sql string) string {
	if sql == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(sql))
	// fmt.Sprintf hex would work too, but manual encoding avoids an import
	// and is marginally faster.
	const hexChars = "0123456789abcdef"
	var buf [16]byte
	for i := 0; i < 8; i++ {
		buf[2*i] = hexChars[sum[i]>>4]
		buf[2*i+1] = hexChars[sum[i]&0x0f]
	}
	return string(buf[:])
}

// decodeTruncated reads the worker 200 body's truncated flag WITHOUT a full
// JSON parse: bodies run to 10MiB+ and this sits on the hot path of every
// successful query. The byte search is safe because the worker serializes
// the envelope itself (encoding/json: no space after the colon, top-level
// field, name unique within the Result shape).
func decodeTruncated(body []byte) bool {
	return bytes.Contains(body, []byte(`"truncated":true`))
}

// HandlerWithAudit is like Handler but accepts an explicit *prometheus.CounterVec
// for testing. This allows tests to use an isolated registry and measure
// counter deltas without racing against the package-level promauto singleton.
// Production callers should use Handler, which wires the package-level counter.
func HandlerWithAudit(cfg Config, signer *tokens.Signer, counter *prometheus.CounterVec) (http.Handler, error) {
	cfg = cfg.withDefaults()
	if signer == nil {
		return nil, errors.New("queryproxy: signer is required")
	}
	if cfg.Gate == nil {
		return nil, errors.New("queryproxy: Gate is required")
	}
	client, err := newWorkerClient(cfg.WorkerURL, cfg.MaxTimeoutS, cfg.CatalogTTLSlack, cfg.MaxMaxBytes)
	if err != nil {
		return nil, err
	}
	g := newGate(cfg.PerPrincipalInflight)
	return &handler{cfg: cfg, signer: signer, client: client, gate: g, auditCounter: counter}, nil
}

// auditedHandler wraps ServeHTTP with the deferred audit emit. It is called
// from the handler's ServeHTTP after authentication so the principal is
// guaranteed to be known. The mutable rec is updated by the various handler
// branches before the deferred emit fires.
func (h *handler) serveWithAudit(w http.ResponseWriter, r *http.Request, sub string, start time.Time) {
	// outcome defaults to "internal" so an unexpected exit (e.g. a panic
	// unwinding through the deferred emit) never mints an empty-string
	// Prometheus label.
	rec := &auditRecord{principal: sub, outcome: "internal"}
	// Snapshot the active logger at request entry rather than re-reading
	// slog.Default() inside the deferred emit. The emit can fire after the
	// request goroutine has handed back (and, in tests that swap the global
	// logger per-case, after a different test has taken over slog.Default());
	// binding it here keeps each request's audit line on the logger that was
	// active when the request started.
	logger := slog.Default()
	// Exactly one emit per call — structural guarantee from deferred.
	defer func() {
		rec.durationMS = time.Since(start).Milliseconds()
		counter := h.auditCounter
		if counter == nil {
			counter = queryRequestsTotal
		}
		emitAudit(logger, counter, rec)
	}()

	// 2. Project gate: {pid} → FGA datuplet_member → project-qualified
	//    warehouse. Runs before the body decode — an unauthorized caller
	//    learns nothing about the body-validation surface.
	warehouse, gerr := h.cfg.Gate.QualifiedWarehouse(r.Context(), sub, r.PathValue("pid"))
	if gerr != nil {
		rec.outcome = gerr.Kind
		writeError(w, gerr.Status, gerr.Kind, gerr.Msg)
		return
	}

	// 3. Decode + validate the body (≤64 KiB SQL-text cap).
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		rec.outcome = "bad_request"
		writeError(w, http.StatusBadRequest, "bad_request", "invalid request body")
		return
	}
	if req.SQL == "" {
		rec.outcome = "bad_request"
		writeError(w, http.StatusBadRequest, "bad_request", "sql is required")
		return
	}
	rec.statementHash = statementHash(req.SQL)

	// 4. Clamp limits.
	timeoutS := clamp(req.TimeoutS, h.cfg.DefaultTimeoutS, h.cfg.MaxTimeoutS)
	maxRows := clamp(req.MaxRows, h.cfg.DefaultMaxRows, h.cfg.MaxMaxRows)
	maxBytes := clamp(req.MaxBytes, h.cfg.DefaultMaxBytes, h.cfg.MaxMaxBytes)

	// 5. Per-principal in-flight gate.
	if !h.gate.Acquire(sub) {
		rec.outcome = "rate_limited"
		// jti remains "" — no catalog token minted yet.
		w.Header().Set("Retry-After", "2")
		writeError(w, http.StatusTooManyRequests, "rate_limited",
			"too many queries in flight for this principal")
		return
	}
	defer h.gate.Release(sub)

	// 6. Mint the two short-lived JWTs.
	ttl := time.Duration(timeoutS)*time.Second + h.cfg.CatalogTTLSlack
	catalogTok, err := tokens.MintQueryToken(r.Context(), h.signer, ttl)
	if err != nil {
		slog.Error("queryproxy: mint catalog token failed", "sub", sub, "err", err)
		rec.outcome = "internal"
		writeError(w, http.StatusInternalServerError, "internal", "failed to mint query credentials")
		return
	}
	// Extract jti from the minted catalog token for cross-system correlation.
	// Unverified decode is intentional — we minted this token ourselves.
	rec.jti = extractJTIFromToken(catalogTok.Reveal())

	internalTok, err := tokens.MintInternalQueryToken(r.Context(), h.signer, ttl)
	if err != nil {
		slog.Error("queryproxy: mint internal token failed", "sub", sub, "err", err)
		rec.outcome = "internal"
		writeError(w, http.StatusInternalServerError, "internal", "failed to mint query credentials")
		return
	}

	// 7. Proxy to the query-worker.
	body := workerRequest{
		SQL:        req.SQL,
		CatalogJWT: catalogTok.Reveal(),
		Warehouse:  warehouse,
		TimeoutS:   timeoutS,
		MaxRows:    maxRows,
		MaxBytes:   maxBytes,
	}
	resp, err := h.client.Do(r.Context(), internalTok, body)
	if err != nil {
		slog.Error("queryproxy: query-worker transport error", "sub", sub, "err", err)
		rec.outcome = "internal"
		writeError(w, http.StatusBadGateway, "internal", "query service unavailable")
		return
	}

	h.translateWithAudit(w, sub, resp, rec)
}

// allowedWorkerKinds is the closed set of worker-supplied kinds accepted as
// outcome values (and therefore Prometheus label values). Anything else from
// the wire is normalized to the path's fallback — a compromised or buggy
// worker must not be able to mint unbounded metric label cardinality.
var allowedWorkerKinds = map[string]bool{
	"sql_error":         true,
	"bad_request":       true,
	"result_too_large":  true,
	"request_too_large": true,
}

// sanitizeKind clamps a worker-supplied kind to the allowlist, falling back
// to the translate path's own default.
func sanitizeKind(k, fallback string) string {
	if allowedWorkerKinds[k] {
		return k
	}
	return fallback
}

// translateWithAudit is like translate but also fills rec.outcome and
// rec.truncated from the worker response before writing to w.
func (h *handler) translateWithAudit(w http.ResponseWriter, sub string, resp workerResponse, rec *auditRecord) {
	kind := decodeKind(resp.body)

	switch resp.status {
	case http.StatusOK:
		rec.outcome = "ok"
		rec.truncated = decodeTruncated(resp.body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(resp.body)
		return
	case http.StatusRequestTimeout:
		rec.outcome = "timeout"
		writeError(w, http.StatusRequestTimeout, "timeout", workerMsg(resp.body, "query timed out"))
		return
	case http.StatusRequestEntityTooLarge:
		k := sanitizeKind(kind, "result_too_large")
		rec.outcome = k
		writeError(w, http.StatusRequestEntityTooLarge, k, workerMsg(resp.body, "result too large"))
		return
	case http.StatusTooManyRequests:
		rec.outcome = "capacity"
		w.Header().Set("Retry-After", "2")
		writeError(w, http.StatusServiceUnavailable, "capacity", "query service is busy, retry shortly")
		return
	case http.StatusBadRequest:
		k := sanitizeKind(kind, "sql_error")
		rec.outcome = k
		writeError(w, http.StatusBadRequest, k, workerMsg(resp.body, "query failed"))
		return
	case http.StatusUnauthorized:
		slog.Error("queryproxy: query-worker rejected internal token (config/clock/key bug)", "sub", sub)
		rec.outcome = "internal"
		writeError(w, http.StatusBadGateway, "internal", "query service unavailable")
		return
	default:
		slog.Error("queryproxy: unexpected query-worker status", "sub", sub, "status", resp.status)
		rec.outcome = "internal"
		writeError(w, http.StatusBadGateway, "internal", "query service unavailable")
		return
	}
}

