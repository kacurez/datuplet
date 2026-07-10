package queryproxy

// Core is the reusable execution seam for the ad-hoc SQL query path (RFC
// 025 Task 1.1). It owns the same gates, worker client, signer, and audit
// counter as the HTTP handler; HTTPHandler() exposes the existing POST
// /api/v1/projects/{pid}/query http.Handler unchanged. A second consumer
// (Task 3.1, the storage-UI query route) drives the same *handler via
// executeRaw directly, bypassing the HTTP request/response plumbing.
//
// This file intentionally contains no new behaviour: NewCore performs the
// exact validation + construction HandlerWithAudit performed before this
// refactor, and Handler/HandlerWithAudit become thin wrappers around it.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// Result mirrors queryengine.Result's wire shape (decoded form). Exported
// for Task 3.1 consumers that need to unmarshal the raw bytes executeRaw
// returns.
type Result struct {
	Schema    []ResultColumn `json:"schema"`
	Rows      [][]any        `json:"rows"`
	Truncated bool           `json:"truncated"`
}

// ResultColumn is one entry of Result.Schema.
type ResultColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryError is the typed non-200 outcome of executeRaw/translateRaw. Kind
// doubles as the audit outcome label, so it stays within the same
// allowlisted vocabulary as before (see sanitizeKind in audit.go).
type QueryError struct {
	Status int
	Kind   string
	Msg    string
}

// Error implements the error interface.
func (e *QueryError) Error() string { return e.Msg }

// queryLimits bundles the already-clamped resource limits threaded through
// executeRaw. Clamping itself stays in the HTTP-specific caller
// (serveWithAudit) since it depends on the client-supplied request body.
type queryLimits struct {
	timeoutS int
	maxRows  int
	maxBytes int
}

// Core owns the gates, worker client, signer, and audit counter shared by
// every consumer of the query-execution path.
type Core struct {
	h *handler
}

// NewCore performs the validation + client + gate construction previously
// done inline in HandlerWithAudit, and stores the resulting *handler. The
// audit counter is left nil (package-level promauto counter); use
// HandlerWithAudit if an explicit counter is required (tests only).
func NewCore(cfg Config, signer *tokens.Signer) (*Core, error) {
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
	return &Core{h: &handler{cfg: cfg, signer: signer, client: client, gate: g}}, nil
}

// HTTPHandler returns the POST /api/v1/projects/{pid}/query http.Handler.
func (c *Core) HTTPHandler() http.Handler {
	return c.h
}

// executeRaw mints the catalog + internal JWTs, POSTs to the worker, and
// translates the outcome. On 200 it fills rec.jti/outcome/truncated and
// returns the raw Result JSON (zero re-encode for the HTTP pass-through
// path). On any other outcome it returns a *QueryError whose Kind is the
// audit outcome. It does NOT touch the per-principal gate or the body
// clamps — those stay with the caller.
func (h *handler) executeRaw(ctx context.Context, sub, warehouse, sql string, lim queryLimits, rec *auditRecord) ([]byte, *QueryError) {
	ttl := time.Duration(lim.timeoutS)*time.Second + h.cfg.CatalogTTLSlack
	catalogTok, err := tokens.MintQueryToken(ctx, h.signer, ttl)
	if err != nil {
		slog.Error("queryproxy: mint catalog token failed", "sub", sub, "err", err)
		return nil, &QueryError{http.StatusInternalServerError, "internal", "failed to mint query credentials"}
	}
	// Extract jti from the minted catalog token for cross-system correlation.
	// Unverified decode is intentional — we minted this token ourselves.
	rec.jti = extractJTIFromToken(catalogTok.Reveal())

	internalTok, err := tokens.MintInternalQueryToken(ctx, h.signer, ttl)
	if err != nil {
		slog.Error("queryproxy: mint internal token failed", "sub", sub, "err", err)
		return nil, &QueryError{http.StatusInternalServerError, "internal", "failed to mint query credentials"}
	}

	resp, err := h.client.Do(ctx, internalTok, workerRequest{
		SQL:        sql,
		CatalogJWT: catalogTok.Reveal(),
		Warehouse:  warehouse,
		TimeoutS:   lim.timeoutS,
		MaxRows:    lim.maxRows,
		MaxBytes:   lim.maxBytes,
	})
	if err != nil {
		slog.Error("queryproxy: query-worker transport error", "sub", sub, "err", err)
		return nil, &QueryError{http.StatusBadGateway, "internal", "query service unavailable"}
	}
	return h.translateRaw(sub, resp, rec)
}
