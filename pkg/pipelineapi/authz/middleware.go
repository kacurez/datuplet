// Package authz: middleware design notes.
//
// # Design decision: inline `Check` per handler, NOT a decorator
//
// Each project-scoped handler uses a fine-grained
// `authzr.Check(ctx, sub, relation, authz.<TypedConstructor>(<id>))`
// rather than an `IsMember` middleware. Two shapes were on the table:
//
//   - **Inline Check** — each handler explicitly calls authzr.Check.
//     Most-common pattern in pipeline-api today (matches the `mustMember`
//     style); reads naturally next to the path-value parsing; ~3 lines
//     per handler. Failure modes are visible at the call site.
//
//   - **Decorator middleware** — `requireRelation("data_admin",
//     projectFromPath)` wraps the handler. Less repetition, but obscures
//     which relation a route requires unless you read the route table,
//     and forces every handler to share one error envelope.
//
// We chose **inline**. The handler-by-handler check keeps the relation
// choice visible in code review, and the decorator's "shared" path is
// small enough that the indirection isn't worth the abstraction cost. If
// the relation-set grows so that the same triple repeats across many
// handlers, revisit.
//
// # Architectural rule
//
// Typed object constructors only — never raw strings like "project:" + pid.
// ProjectObject, WarehouseObject, NamespaceObject, TableObject, UserObject
// are the canonical constructors; the type system rejects accidental drift.
//
// # The shape of an inline check
//
// Within a project-scoped handler:
//
//	user, ok := auth.UserFromContext(r.Context())
//	if !ok { writeError(w, http.StatusUnauthorized, ...); return }
//	pid, err := uuid.Parse(r.PathValue("pid"))
//	if err != nil { writeError(w, http.StatusBadRequest, ...); return }
//	allowed, err := authzr.Check(r.Context(),
//	    authz.UserObject(user.ID.String()).String(),
//	    "describe", // or "data_admin" for writes — canonical lakekeeper
//	    // relations the editor/viewer leaf tuples chain into via the FGA
//	    // model, so a Datuplet `editor` grant satisfies `data_admin` checks.
//	    authz.ProjectObject(<lakekeeper-project-id-for-pid>))
//	if errors.Is(err, authz.ErrAuthzUnavailable) {
//	    writeError(w, http.StatusServiceUnavailable, ...); return
//	}
//	if err != nil { writeError(w, http.StatusInternalServerError, ...); return }
//	if !allowed { writeError(w, http.StatusForbidden, ...); return }
//
// The Datuplet→lakekeeper project-id translation lives in the Server's
// `lakekeeperIDFor(pid)` helper so handlers don't repeat it.
//
// # Error mapping
//
//   - allowed=true  → handler proceeds.
//   - allowed=false → 403 Forbidden. (IDs alone are not authorization.
//   - ErrAuthzUnavailable → 503 Service Unavailable.
//   - other error → 500 Internal Server Error.
//
// This file holds documentation only. The actual checks are inlined into
// the handlers; there is no shared decorator function.
package authz
