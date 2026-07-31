// handlers.go holds the author-facing HTTP routes of the user-apps control
// plane (RFC 028 spec §5.1). They are ordinary pipeline-api routes: the
// caller is a platform user resolved by auth.WithUser, and every route is
// gated on a project-scoped OpenFGA relation — the same shape the pipeline
// routes use (pkg/pipelineapi/http.mustHaveRelation).
//
// The worker-facing /internal/v1/* routes (service-credential bearer) are a
// separate surface and land in internal.go (P3).
package apps

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// FGA relations gating the author routes. Deliberately the same two the
// pipeline routes use (pipeline_handlers.go): reads take "describe", writes
// take "data_admin". A non-member of the project holds neither, so every
// route 403s for them.
const (
	relationRead  = "describe"
	relationWrite = "data_admin"
)

// appNamePattern is the app-name grammar (spec §4.1): a DNS label —
// lowercase alphanumerics and '-', not starting or ending with '-', 1..63
// chars. App names are unique per project and appear in the viewer URL
// (/apps/{pid}/{name}), so they are held to the same shape as a K8s label.
const appNamePattern = `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`

var appNameRe = regexp.MustCompile(appNamePattern)

// maxUploadBodyBytes caps the PUT body. The bundle itself is capped at
// MaxBundleBytes (5 MB raw) by the store; base64 inflates that by 4/3
// (~6.7 MB) and the JSON envelope adds a few bytes, so 8 MiB leaves the
// "bundle just over the raw cap" case to produce the specific 413 from
// ErrBundleTooLarge rather than an opaque body-too-large one.
const maxUploadBodyBytes = 8 << 20

// timeLayout matches the pipeline handlers' RFC3339 serialization.
const timeLayout = "2006-01-02T15:04:05Z07:00"

// ProjectLookup is the minimal project seam the author routes need: the
// lakekeeper project UUID that the FGA `project:<uuid>` check object is built
// from (projects.lakekeeper_project_id — distinct from the Datuplet project
// UUID in {pid}).
//
// Returns:
//   - ("", err wrapping ErrNotFound) — no such Datuplet project → 404.
//   - ("", nil) — the project exists but lakekeeper hasn't provisioned it
//     yet → 503, matching mustHaveRelation's soft-degrade.
type ProjectLookup interface {
	LakekeeperProjectID(ctx context.Context, projectID uuid.UUID) (string, error)
}

// Handlers is the author-route handler set. All fields are required; the
// server wires it only when every dependency is present (mirroring the other
// route blocks' nil-gates).
type Handlers struct {
	Store    *Store
	Identity IdentityManager
	Authz    authz.Authorizer
	Projects ProjectLookup
}

// Register registers the author routes on mux. mw wraps every handler —
// production passes auth.WithUser(resolver, next) so each handler can read
// the caller from the request context; a nil mw registers the handlers bare
// (only useful in tests that inject the user themselves).
//
// Kept as one method so pipeline-api's Handler() gains a single block and
// the route list stays next to the handlers it names.
func (h *Handlers) Register(mux *http.ServeMux, mw func(http.Handler) http.Handler) {
	route := func(pattern string, fn http.HandlerFunc) {
		var handler http.Handler = fn
		if mw != nil {
			handler = mw(fn)
		}
		mux.Handle(pattern, handler)
	}
	route("PUT /api/v1/projects/{pid}/apps/{name}", h.handlePutApp)
	route("GET /api/v1/projects/{pid}/apps", h.handleListApps)
	route("GET /api/v1/projects/{pid}/apps/{name}", h.handleGetApp)
	route("DELETE /api/v1/projects/{pid}/apps/{name}", h.handleDeleteApp)
	route("POST /api/v1/projects/{pid}/apps/{name}/promote", h.handlePromote)
	route("GET /api/v1/projects/{pid}/apps/{name}/logs", h.handleLogs)
	route("POST /api/v1/projects/{pid}/apps/{name}/tokens", h.handleCreateToken)
	route("DELETE /api/v1/projects/{pid}/apps/{name}/tokens/{token_id}", h.handleDeleteToken)
}

// ---------------------------------------------------------------------------
// Wire shapes
// ---------------------------------------------------------------------------

// putAppRequest is the upload body. **JSON + base64, not multipart**
// (spec §12 resolved note): a single JSON shape is the simplest thing for
// both the CLI (`datuplet apps put`) and the browser UI to produce, and it
// keeps the route consistent with every other pipeline-api endpoint. The
// hash/immutability contract is unaffected — the store hashes the decoded
// raw bytes.
type putAppRequest struct {
	BundleBase64 string `json:"bundle_base64"`
}

type putAppResponse struct {
	AppID       string `json:"app_id"`
	VersionHash string `json:"version_hash"`
}

type channelJSON struct {
	VersionHash string `json:"version_hash"`
	UpdatedAt   string `json:"updated_at"`
}

type versionJSON struct {
	Hash      string `json:"hash"`
	SizeBytes int64  `json:"size_bytes"`
	CreatedAt string `json:"created_at"`
}

// appJSON is both the list entry and the detail shape; Versions is populated
// on the detail route only (list stays cheap).
type appJSON struct {
	AppID     string                 `json:"app_id"`
	Name      string                 `json:"name"`
	CreatedAt string                 `json:"created_at"`
	Channels  map[string]channelJSON `json:"channels"`
	Versions  []versionJSON          `json:"versions,omitempty"`
}

// promoteRequest is the CAS promote body (spec §5.1). ExpectedProduction is
// a pointer so an explicit `null` and an omitted field both mean "production
// is not set yet" — Store.Promote's "" sentinel.
type promoteRequest struct {
	Version            string  `json:"version"`
	ExpectedProduction *string `json:"expectedProduction"`
}

type promoteResponse struct {
	ProductionVersion string `json:"production_version"`
}

type tokenResponse struct {
	TokenID string `json:"token_id"`
	// Token is the plaintext `vw_<token_id>.<secret>` viewer token. It is
	// returned EXACTLY ONCE, here, at mint: only SHA-256(salt||secret) is
	// stored, so no route can ever surface it again (spec §5.3).
	Token string `json:"token"`
}

type renderLogJSON struct {
	RequestID     string `json:"request_id"`
	AppID         string `json:"app_id"`
	VersionHash   string `json:"version_hash"`
	Channel       string `json:"channel"`
	PrincipalKind string `json:"principal_kind"`
	PrincipalID   string `json:"principal_id"`
	StartedAt     string `json:"started_at"`
	DurationMS    int64  `json:"duration_ms"`
	Outcome       string `json:"outcome"`
	LogText       string `json:"log_text"`
	Error         string `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// handlePutApp upserts the app and uploads a bundle version, moving the
// app's `draft` channel to it. Production is never touched here (spec §5.1);
// promotion is the separate CAS route.
func (h *Handlers) handlePutApp(w http.ResponseWriter, r *http.Request) {
	userID, projectID, ok := h.authorize(w, r, relationWrite)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if !appNameRe.MatchString(name) {
		writeError(w, http.StatusBadRequest,
			"invalid app name: must be a DNS label matching "+appNamePattern)
		return
	}

	bundle, ok := readBundle(w, r)
	if !ok {
		return
	}

	app, ok := h.upsertApp(w, r, projectID, name)
	if !ok {
		return
	}

	v, err := h.Store.PutVersion(r.Context(), app.ID, bundle)
	switch {
	case errors.Is(err, ErrBundleTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("bundle exceeds the %d byte limit", MaxBundleBytes))
		return
	case errors.Is(err, ErrProjectQuota):
		writeError(w, http.StatusConflict,
			fmt.Sprintf("project app-bundle storage quota (%d bytes) exceeded; delete unused apps or versions", MaxProjectBytes))
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "store app version")
		return
	}

	audit("put_version", userID, projectID, app.ID, "app", name, "version_hash", v.Hash, "size_bytes", v.SizeBytes)
	writeJSON(w, http.StatusOK, putAppResponse{AppID: app.ID, VersionHash: v.Hash})
}

// upsertApp returns the existing app row or creates it, and makes sure the
// app identity's FGA `viewer` tuple exists (spec §5.4). The row is written
// before the tuple — the reverse of delete — and fga_registered is only
// flipped once Register has returned, so a half-failed registration is
// retried by the next upload instead of silently leaving an app that can
// read nothing.
func (h *Handlers) upsertApp(w http.ResponseWriter, r *http.Request, projectID uuid.UUID, name string) (*App, bool) {
	ctx := r.Context()
	app, err := h.Store.Get(ctx, projectID, name)
	if errors.Is(err, ErrNotFound) {
		app, err = h.Store.Create(ctx, projectID, name)
		if errors.Is(err, ErrAlreadyExists) {
			// Lost a concurrent create; the row now exists.
			app, err = h.Store.Get(ctx, projectID, name)
		}
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get or create app")
		return nil, false
	}
	if !app.FGARegistered {
		if err := h.Identity.Register(ctx, app.ID, app.ProjectID); err != nil {
			slog.Error("apps: register app identity", "app", app.ID, "project", app.ProjectID, "err", err)
			writeError(w, http.StatusServiceUnavailable, "could not register the app identity; retry")
			return nil, false
		}
		if err := h.Store.SetFGARegistered(ctx, app.ID); err != nil {
			writeError(w, http.StatusInternalServerError, "record app identity registration")
			return nil, false
		}
		app.FGARegistered = true
	}
	return app, true
}

func (h *Handlers) handleListApps(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := h.authorize(w, r, relationRead)
	if !ok {
		return
	}
	summaries, err := h.Store.List(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list apps")
		return
	}
	out := make([]appJSON, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, appJSON{
			AppID:     s.ID,
			Name:      s.Name,
			CreatedAt: s.CreatedAt.Format(timeLayout),
			Channels:  channelMap(s.Channels),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handlers) handleGetApp(w http.ResponseWriter, r *http.Request) {
	app, _, ok := h.resolveApp(w, r, relationRead)
	if !ok {
		return
	}
	channels, err := h.Store.Channels(r.Context(), app.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get app channels")
		return
	}
	versions, err := h.Store.ListVersions(r.Context(), app.ID, 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list app versions")
		return
	}
	out := appJSON{
		AppID:     app.ID,
		Name:      app.Name,
		CreatedAt: app.CreatedAt.Format(timeLayout),
		Channels:  channelMap(channels),
		Versions:  make([]versionJSON, 0, len(versions)),
	}
	for _, v := range versions {
		out.Versions = append(out.Versions, versionJSON{
			Hash:      v.Hash,
			SizeBytes: v.SizeBytes,
			CreatedAt: v.CreatedAt.Format(timeLayout),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleDeleteApp removes the app. **Ordering is a security invariant**
// (spec §5.4): the FGA tuple is deleted FIRST via IdentityManager.Unregister,
// and only then the rows. Doing it the other way round would leave a live
// `viewer` tuple for an app id nobody can see or clean up any more; doing it
// this way, a failure between the two steps leaves an app that can read
// nothing (fail-closed) and whose delete is safely retryable — the same
// discipline as runbackend.K8sBackend.CancelRun's "tuple delete FIRST".
func (h *Handlers) handleDeleteApp(w http.ResponseWriter, r *http.Request) {
	app, userID, ok := h.resolveApp(w, r, relationWrite)
	if !ok {
		return
	}
	if err := h.Identity.Unregister(r.Context(), app.ID, app.ProjectID); err != nil {
		slog.Error("apps: unregister app identity", "app", app.ID, "project", app.ProjectID, "err", err)
		writeError(w, http.StatusServiceUnavailable,
			"could not revoke the app identity; the app was NOT deleted, retry")
		return
	}
	if err := h.Store.Delete(r.Context(), app.ID); err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "app not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "delete app")
		return
	}
	projectUUID, _ := uuid.Parse(app.ProjectID)
	audit("delete_app", userID, projectUUID, app.ID, "app", app.Name)
	w.WriteHeader(http.StatusNoContent)
}

// handlePromote repoints `production` under a compare-and-swap on the
// current production hash (spec §5.1): mismatch → 409, unknown version →
// 400. Promotion is eventually consistent — workers cache slug→version
// resolution for up to 15 s, so replicas may serve mixed versions for that
// long after this returns.
func (h *Handlers) handlePromote(w http.ResponseWriter, r *http.Request) {
	app, userID, ok := h.resolveApp(w, r, relationWrite)
	if !ok {
		return
	}
	var req promoteRequest
	if !readJSONBody(w, r, &req) {
		return
	}
	if req.Version == "" {
		writeError(w, http.StatusBadRequest, `"version" is required (the content hash to promote)`)
		return
	}
	// An explicit null / omitted expectedProduction is Store.Promote's ""
	// sentinel for "production is not set yet".
	expected := ""
	if req.ExpectedProduction != nil {
		expected = *req.ExpectedProduction
	}
	err := h.Store.Promote(r.Context(), app.ID, req.Version, expected)
	switch {
	case errors.Is(err, ErrCASMismatch):
		writeError(w, http.StatusConflict,
			"production has moved since expectedProduction was read; re-read the app and retry")
		return
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusBadRequest, "unknown version hash for this app")
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "promote app version")
		return
	}
	projectUUID, _ := uuid.Parse(app.ProjectID)
	audit("promote", userID, projectUUID, app.ID, "app", app.Name,
		"version_hash", req.Version, "expected_production", expected)
	writeJSON(w, http.StatusOK, promoteResponse{ProductionVersion: req.Version})
}

// handleLogs serves the author-facing render logs (spec §6.6). Without
// ?request_id it returns the newest records as an array; with one it returns
// that single record, or 404 when no record with that id exists (never did,
// or aged out of the ring buffer) — which is what the CLI's `apps render`
// failure path keys its `author_log: null` on.
func (h *Handlers) handleLogs(w http.ResponseWriter, r *http.Request) {
	app, _, ok := h.resolveApp(w, r, relationRead)
	if !ok {
		return
	}
	if requestID := r.URL.Query().Get("request_id"); requestID != "" {
		if _, err := uuid.Parse(requestID); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request_id")
			return
		}
		recs, err := h.Store.GetRenderLogs(r.Context(), app.ID, requestID, 0)
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "no render log for that request_id")
			return
		}
		if err != nil || len(recs) == 0 {
			writeError(w, http.StatusInternalServerError, "get render log")
			return
		}
		writeJSON(w, http.StatusOK, renderLogToJSON(recs[0]))
		return
	}

	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = n
		if limit > MaxRenderLogsPerApp {
			limit = MaxRenderLogsPerApp
		}
	}
	recs, err := h.Store.GetRenderLogs(r.Context(), app.ID, "", limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list render logs")
		return
	}
	out := make([]renderLogJSON, 0, len(recs))
	for _, rec := range recs {
		out = append(out, renderLogToJSON(rec))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleCreateToken mints a viewer token. The response carries the plaintext
// `vw_<token_id>.<secret>` — the ONLY time it ever transits (spec §5.3); the
// audit line records the token_id only, never the secret.
func (h *Handlers) handleCreateToken(w http.ResponseWriter, r *http.Request) {
	app, userID, ok := h.resolveApp(w, r, relationWrite)
	if !ok {
		return
	}
	tokenID, secret, err := h.Store.MintToken(r.Context(), app.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "mint viewer token")
		return
	}
	projectUUID, _ := uuid.Parse(app.ProjectID)
	audit("mint_viewer_token", userID, projectUUID, app.ID, "app", app.Name, "token_id", tokenID)
	writeJSON(w, http.StatusCreated, tokenResponse{
		TokenID: tokenID,
		Token:   "vw_" + tokenID + "." + secret,
	})
}

// handleDeleteToken revokes a viewer token. Revocation kills live viewer
// sessions within the worker's ≤15 s verify-cache window (spec §5.3).
func (h *Handlers) handleDeleteToken(w http.ResponseWriter, r *http.Request) {
	app, userID, ok := h.resolveApp(w, r, relationWrite)
	if !ok {
		return
	}
	tokenID := r.PathValue("token_id")
	if _, err := uuid.Parse(tokenID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid token id")
		return
	}
	// Scoped by (app_id, token_id): a token belonging to another app is
	// "not found" here, never revocable through this app's path.
	if err := h.Store.RevokeToken(r.Context(), app.ID, tokenID); err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "viewer token not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "revoke viewer token")
		return
	}
	projectUUID, _ := uuid.Parse(app.ProjectID)
	audit("revoke_viewer_token", userID, projectUUID, app.ID, "app", app.Name, "token_id", tokenID)
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

// authorize is the project-scoped authz guard, mirroring
// pkg/pipelineapi/http.mustHaveRelation 1:1 (including its error mapping) so
// the app routes behave exactly like the pipeline routes for an
// unauthenticated caller, a non-member, a malformed {pid}, an unknown
// project, and an FGA outage.
//
//   - 401 no authenticated user (auth.WithUser normally answers this first)
//   - 400 malformed {pid}
//   - 404 unknown project
//   - 503 lakekeeper project not provisioned yet / FGA backend unavailable
//   - 403 FGA says no
//   - 500 unexpected error
func (h *Handlers) authorize(w http.ResponseWriter, r *http.Request, relation string) (userID, projectID uuid.UUID, ok bool) {
	user, authed := auth.UserFromContext(r.Context())
	if !authed {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	pid, err := uuid.Parse(r.PathValue("pid"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid project id")
		return
	}
	lakekeeperPID, err := h.Projects.LakekeeperProjectID(r.Context(), pid)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get project")
		return
	}
	if lakekeeperPID == "" {
		writeError(w, http.StatusServiceUnavailable,
			"project authz not yet provisioned (lakekeeper project pending)")
		return
	}
	allowed, err := h.Authz.Check(r.Context(),
		authz.UserObject(user.ID.String()).String(), relation, authz.ProjectObject(lakekeeperPID))
	if errors.Is(err, authz.ErrAuthzUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "authz backend unavailable")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "check authz")
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	return user.ID, pid, true
}

// resolveApp is authorize + the {name} lookup every non-list route needs.
// Returns the app row and the authenticated user's id.
func (h *Handlers) resolveApp(w http.ResponseWriter, r *http.Request, relation string) (*App, uuid.UUID, bool) {
	userID, projectID, ok := h.authorize(w, r, relation)
	if !ok {
		return nil, uuid.UUID{}, false
	}
	app, err := h.Store.Get(r.Context(), projectID, r.PathValue("name"))
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "app not found")
		return nil, uuid.UUID{}, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get app")
		return nil, uuid.UUID{}, false
	}
	return app, userID, true
}

// readBundle decodes the upload body into raw bundle bytes. base64 is
// accepted in padded/unpadded and standard/URL alphabets so neither the CLI
// nor the browser has to care which one their language's encoder emits.
func readBundle(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	var req putAppRequest
	if !readJSONBody(w, r, &req) {
		return nil, false
	}
	if req.BundleBase64 == "" {
		writeError(w, http.StatusBadRequest, `"bundle_base64" is required`)
		return nil, false
	}
	bundle, err := decodeBase64(req.BundleBase64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bundle_base64 is not valid base64")
		return nil, false
	}
	if len(bundle) == 0 {
		writeError(w, http.StatusBadRequest, "bundle is empty")
		return nil, false
	}
	return bundle, true
}

func decodeBase64(s string) ([]byte, error) {
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.RawURLEncoding,
	} {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("apps: not valid base64")
}

// readJSONBody strict-decodes a size-capped JSON body. An oversize body is a
// 413; anything else unreadable is a 400. A missing body decodes as the zero
// value (the token routes take no body).
func readJSONBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) || strings.Contains(err.Error(), "http: request body too large") {
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("body exceeds %d bytes", maxUploadBodyBytes))
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func channelMap(refs []ChannelRef) map[string]channelJSON {
	out := make(map[string]channelJSON, len(refs))
	for _, c := range refs {
		out[c.Channel] = channelJSON{
			VersionHash: c.VersionHash,
			UpdatedAt:   c.UpdatedAt.Format(timeLayout),
		}
	}
	return out
}

func renderLogToJSON(rec RenderLogRecord) renderLogJSON {
	return renderLogJSON{
		RequestID:     rec.RequestID,
		AppID:         rec.AppID,
		VersionHash:   rec.VersionHash,
		Channel:       rec.Channel,
		PrincipalKind: rec.PrincipalKind,
		PrincipalID:   rec.PrincipalID,
		StartedAt:     rec.StartedAt.UTC().Format(time.RFC3339Nano),
		DurationMS:    rec.DurationMS,
		Outcome:       rec.Outcome,
		LogText:       rec.LogText,
		Error:         rec.Error,
	}
}

// audit emits one structured line per mutating author action, mirroring the
// query proxy's single-record/single-emit discipline
// (pkg/pipelineapi/queryproxy/audit.go). Fixed low-cardinality keys first,
// then per-action detail. Never carries a viewer-token secret or a bundle.
func audit(action string, userID, projectID uuid.UUID, appID string, kv ...any) {
	args := append([]any{
		"action", action,
		"actor", userID.String(),
		"project_id", projectID.String(),
		"app_id", appID,
	}, kv...)
	slog.Info("apps: author audit", args...)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
