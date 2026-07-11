// Package projectgate centralizes the per-request prologue shared by the
// storage handlers and the query proxy: pid parse → lakekeeper-project-ID
// map → FGA datuplet_member check → warehouse resolution, with one bounded
// error taxonomy (bad_request / forbidden / unavailable / internal).
package projectgate

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// Gate owns the project-scoped request prologue shared by the storage
// handlers and the query proxy: pid parse → lakekeeper-project-ID map →
// FGA datuplet_member → warehouse resolution, with ONE error taxonomy.
type Gate struct {
	LakekeeperProjectIDFor func(ctx context.Context, datupletProjectID uuid.UUID) (string, error)
	Authorizer             authz.Authorizer
	// WarehouseFor returns the BARE warehouse name for a lakekeeper project.
	WarehouseFor func(ctx context.Context, lakekeeperProjectID string) (string, error)
}

// Error carries the HTTP mapping for a failed gate step. Kind doubles as
// the audit/metric outcome label; the set is bounded:
// bad_request / forbidden / unavailable / internal.
type Error struct {
	Status int
	Kind   string
	Msg    string
}

// Message strings deliberately preserve the storage handlers' current
// wording so the existing handler tests keep passing after the Task 0.2
// refactor.

// Authorize validates pid, maps it to the lakekeeper project ID, and
// enforces FGA datuplet_member for userID.
func (g *Gate) Authorize(ctx context.Context, userID, pid string) (uuid.UUID, string, *Error) {
	parsed, err := uuid.Parse(pid)
	if err != nil {
		return uuid.Nil, "", &Error{Status: http.StatusBadRequest, Kind: "bad_request", Msg: "invalid project id"}
	}
	// Nil deps = authz-disabled / partially-wired deployment. Soft-degrade
	// with 503 — never panic (main.go leaves authzr nil when
	// OPENFGA_MODEL_VERSION is unset).
	if g.LakekeeperProjectIDFor == nil || g.Authorizer == nil {
		return uuid.Nil, "", &Error{Status: http.StatusServiceUnavailable, Kind: "unavailable", Msg: "project authz not yet provisioned"}
	}
	lkPID, err := g.LakekeeperProjectIDFor(ctx, parsed)
	if err != nil || lkPID == "" {
		return uuid.Nil, "", &Error{Status: http.StatusServiceUnavailable, Kind: "unavailable", Msg: "project authz not yet provisioned"}
	}
	userStr := authz.UserObject(userID).String()
	allowed, err := g.Authorizer.Check(ctx, userStr, "datuplet_member", authz.ProjectObject(lkPID))
	if err != nil {
		if errors.Is(err, authz.ErrAuthzUnavailable) {
			return uuid.Nil, "", &Error{Status: http.StatusServiceUnavailable, Kind: "unavailable", Msg: "authz backend unavailable"}
		}
		return uuid.Nil, "", &Error{Status: http.StatusInternalServerError, Kind: "internal", Msg: "check authz"}
	}
	if !allowed {
		return uuid.Nil, "", &Error{Status: http.StatusForbidden, Kind: "forbidden", Msg: "forbidden"}
	}
	return parsed, lkPID, nil
}

// Warehouse resolves the bare warehouse name for an authorized lkPID.
func (g *Gate) Warehouse(ctx context.Context, lakekeeperProjectID string) (string, *Error) {
	if g.WarehouseFor == nil {
		return "", &Error{Status: http.StatusServiceUnavailable, Kind: "unavailable", Msg: "no warehouse resolver configured"}
	}
	name, err := g.WarehouseFor(ctx, lakekeeperProjectID)
	if err != nil || name == "" {
		return "", &Error{Status: http.StatusServiceUnavailable, Kind: "unavailable", Msg: "no warehouse registered for project"}
	}
	return name, nil
}

// QualifiedWarehouse = Authorize + Warehouse + "lkPID/name" composition
// (plain concatenation — never path.Join). The query proxy's one-call path.
func (g *Gate) QualifiedWarehouse(ctx context.Context, userID, pid string) (string, *Error) {
	_, lkPID, gerr := g.Authorize(ctx, userID, pid)
	if gerr != nil {
		return "", gerr
	}
	name, gerr := g.Warehouse(ctx, lkPID)
	if gerr != nil {
		return "", gerr
	}
	return lkPID + "/" + name, nil
}
