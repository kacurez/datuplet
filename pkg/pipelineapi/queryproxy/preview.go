package queryproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// PreviewLimits bundles the resource limits a storage-preview caller
// applies to Core.Preview. Unlike the console query path (queryRequest),
// these are server-generated constants (storage.previewRowCap /
// previewByteCap), not client-supplied — so there is no clamp step here.
type PreviewLimits struct {
	TimeoutS int
	MaxRows  int
	MaxBytes int
}

// Preview runs the server-generated storage-preview statement through the
// same mint → worker → translate pipeline as the console (RFC 025 §4.1).
// The caller (storage prologue) has already authorized the project and
// resolved qualifiedWarehouse; ns/table must be pre-validated by
// storage.ValidIdentifier — QuoteIdent here is defense in depth. Previews
// hold their own per-principal gate (cap 1) so they never starve, or get
// starved by, console queries. One query_audit record is emitted.
func (c *Core) Preview(ctx context.Context, sub, qualifiedWarehouse, ns, table string, lim PreviewLimits) (*Result, *QueryError) {
	sql := fmt.Sprintf("SELECT * FROM lk.%s.%s LIMIT %d", QuoteIdent(ns), QuoteIdent(table), lim.MaxRows)

	start := time.Now()
	rec := &auditRecord{principal: sub, outcome: "internal", statementHash: statementHash(sql)}
	logger := slog.Default()
	// Armed before the gate check so a rate-limited preview still emits a
	// query_audit line, matching the console path (serveWithAudit).
	defer func() {
		rec.durationMS = time.Since(start).Milliseconds()
		counter := c.h.auditCounter
		if counter == nil {
			counter = queryRequestsTotal
		}
		emitAudit(logger, counter, rec)
	}()

	if !c.h.previewGate.Acquire(sub) {
		rec.outcome = "rate_limited"
		return nil, &QueryError{http.StatusTooManyRequests, "rate_limited", "a preview is already running for this user"}
	}
	defer c.h.previewGate.Release(sub)

	raw, qerr := c.h.executeRaw(ctx, sub, qualifiedWarehouse, sql,
		queryLimits{timeoutS: lim.TimeoutS, maxRows: lim.MaxRows, maxBytes: lim.MaxBytes}, rec)
	if qerr != nil {
		rec.outcome = qerr.Kind
		return nil, qerr
	}
	var res Result
	if err := json.Unmarshal(raw, &res); err != nil {
		rec.outcome = "internal"
		return nil, &QueryError{http.StatusBadGateway, "internal", "malformed worker response"}
	}
	return &res, nil
}
