// internal.go holds the six worker-facing routes of the user-apps control
// plane (RFC 028 spec §5.2) — the ONLY pipeline-api surface app-worker talks
// to. They are a different security surface from the author routes in
// handlers.go: the caller is not a platform user but the app-worker process
// itself, authenticated with one shared service credential, and every
// response uses the kind-carrying error envelope (spec §8) that app-worker
// and the viewer-facing surface share.
package apps

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// Error kinds carried by the internal envelope. Subset of spec §8's kind
// vocabulary — the kinds a *control-plane* response can produce. The
// render-side kinds (render_error, timeout, rate_limited, capacity) are
// app-worker's to emit, never pipeline-api's.
const (
	kindBadRequest   = "bad_request"
	kindUnauthorized = "unauthorized"
	kindNotFound     = "app_not_found"
	kindUnavailable  = "unavailable"
)

// bundleCacheControl is the immutable-caching directive on the bundle-fetch
// response. Bundles are content-addressed (the hash IS the identity), so a
// hit can never go stale: one year + `immutable` lets every hop between
// pipeline-api and the worker's own cache keep it forever.
const bundleCacheControl = "public, max-age=31536000, immutable"

// maxInternalBodyBytes caps internal POST bodies. Larger than the 16 KiB
// viewer-facing POST cap (spec §7) because the render-log body legitimately
// carries up to MaxRenderLogBytes of captured guest output plus an error and
// a stack; the slack covers JSON escaping of that payload.
const maxInternalBodyBytes = 512 << 10

// MaxRenderLogBytes is the per-render captured-log cap (spec §7: log ≤64 KiB
// per render). Oversize log_text is truncated, not rejected — a render that
// logged too much still deserves a log record, and the author is better
// served by a truncated log than by no log at all.
const MaxRenderLogBytes = 64 << 10

// channelProduction/channelDraft are the only two channels (spec §4.1); the
// DB has the same CHECK constraint. Validated here so a worker typo is a
// crisp 400 rather than an empty resolve.
const (
	channelProduction = "production"
	channelDraft      = "draft"
)

// principalKinds is the render-log principal taxonomy (spec §9):
// viewer_token (opaque viewer token) or platform_user (bearer CLI render or
// session-authenticated UI @draft preview).
var principalKinds = map[string]bool{"viewer_token": true, "platform_user": true}

// bundleHashRe matches a hex SHA-256 content hash — the shape every
// {hash}/version_hash path or field must have (the DB columns are CHAR(64),
// which would silently space-pad anything shorter).
var bundleHashRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ---------------------------------------------------------------------------
// Service credential
// ---------------------------------------------------------------------------

// ServiceTokenFileEnv names the file holding the internal service credential
// (a K8s Secret projected into the pipeline-api Pod). Part 7 owns the chart
// values/secret/mount; this package only reads the path from the environment.
const ServiceTokenFileEnv = "DATUPLET_APPS_INTERNAL_TOKEN_FILE"

// ForwardedAuthorizationHeader carries the CALLER's credential on
// sessions/verify. It exists because `Authorization` is already taken: that
// header holds app-worker's service credential (requireServiceToken reads
// it), and one header cannot carry two bearer values. app-worker puts the
// viewer's/CLI's `Authorization` value here instead; handleVerifySession
// moves it back into `Authorization` on a private copy of the request before
// handing it to the session resolver. Cookie-based sessions need none of
// this — the `Cookie` header rides through untouched.
const ForwardedAuthorizationHeader = "X-Datuplet-Forwarded-Authorization"

// ServiceToken is the shared bearer credential that gates every internal
// route. It stores only the SHA-256 digest of the secret and compares
// digests, so the comparison is both constant-time AND length-independent
// (subtle.ConstantTimeCompare short-circuits on differing lengths, which
// would leak the credential's length).
//
// The zero value is unusable; construct via NewServiceToken /
// LoadServiceToken / ServiceTokenFromEnv.
type ServiceToken struct {
	digest [sha256.Size]byte
}

// NewServiceToken builds a ServiceToken from a secret string. An empty
// secret is rejected: a zero-length credential would make every request
// with an empty bearer succeed.
func NewServiceToken(secret string) (*ServiceToken, error) {
	if strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("apps: internal service token is empty")
	}
	return &ServiceToken{digest: sha256.Sum256([]byte(secret))}, nil
}

// LoadServiceToken reads the credential from path. Surrounding whitespace
// (notably the trailing newline a `kubectl create secret --from-literal` or
// an editor leaves behind) is trimmed, so the file's exact byte layout is
// not part of the credential.
func LoadServiceToken(path string) (*ServiceToken, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("apps: read internal service token: %w", err)
	}
	return NewServiceToken(strings.TrimSpace(string(raw)))
}

// ServiceTokenFromEnv loads the credential named by ServiceTokenFileEnv.
// Returns (nil, nil) when the variable is unset or empty — the caller then
// leaves the internal routes unregistered (they 404), matching every other
// soft-degrade gate in pipeline-api's Handler().
func ServiceTokenFromEnv() (*ServiceToken, error) {
	path := strings.TrimSpace(os.Getenv(ServiceTokenFileEnv))
	if path == "" {
		return nil, nil
	}
	return LoadServiceToken(path)
}

// Matches reports whether presented is the configured credential, in
// constant time with respect to the secret's content and length.
func (t *ServiceToken) Matches(presented string) bool {
	if t == nil || presented == "" {
		return false
	}
	got := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(got[:], t.digest[:]) == 1
}

// String redacts the credential so it can never reach a log line via %v/%s,
// mirroring the tokens package's redacting wrappers.
func (t *ServiceToken) String() string { return "ServiceToken(redacted)" }

// ---------------------------------------------------------------------------
// Handler set
// ---------------------------------------------------------------------------

// InternalHandlers is the worker-facing handler set. Every field is required;
// pipeline-api's Handler() registers the block only when all of them are
// wired (absent config ⇒ the routes 404 rather than half-working).
type InternalHandlers struct {
	Store    *Store
	Identity IdentityManager
	Authz    authz.Authorizer
	Projects ProjectLookup
	// Resolver is the SAME session resolver auth.WithUser wraps for the
	// browser/CLI routes. sessions/verify holds no session-validation logic
	// of its own: it forwards the caller's credential headers into this
	// resolver (spec §5.2).
	Resolver auth.UserResolver
	// Token is the shared service credential every route is gated on.
	Token *ServiceToken
}

// RegisterInternal registers the six internal routes on mux, each already
// wrapped in the service-credential gate.
//
// The gate is applied HERE rather than accepted as a caller-supplied
// middleware (the shape Handlers.Register uses) on purpose: it makes it
// structurally impossible to register an internal route without
// authentication. There is no "bare" registration mode for this surface.
func (h *InternalHandlers) RegisterInternal(mux *http.ServeMux) {
	route := func(pattern string, fn http.HandlerFunc) {
		mux.Handle(pattern, h.requireServiceToken(fn))
	}
	route("GET /internal/v1/apps/{pid}/{name}/resolve", h.handleResolve)
	route("GET /internal/v1/bundles/{hash}", h.handleBundle)
	route("POST /internal/v1/viewer-tokens/verify", h.handleVerifyViewerToken)
	route("POST /internal/v1/sessions/verify", h.handleVerifySession)
	route("POST /internal/v1/impersonate", h.handleImpersonate)
	route("POST /internal/v1/apps/{app_id}/logs", h.handleAppendLog)
}

// requireServiceToken is the only authentication on the internal surface:
// one unscoped bearer credential, compared in constant time against the
// contents of ServiceTokenFileEnv.
//
// # Why there is a 401 path but deliberately no 403 path
//
// Spec §5.2 says internal endpoints answer "401/403 on a bad or
// out-of-scope service credential". In v1 there is exactly ONE credential
// and its scope IS these six endpoints — there are no per-endpoint or
// per-project scopes to be out of, so no request can ever be
// "authenticated but out of scope". The 403 branch is therefore absent BY
// DESIGN, not by omission. Introducing scoped credentials later (e.g. a
// per-worker-deployment credential limited to a project set) is where the
// 403 belongs; until then, adding one would be dead code that implies a
// boundary the system does not enforce.
func (h *InternalHandlers) requireServiceToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok || !h.Token.Matches(presented) {
			// Never logs the presented value, and never distinguishes
			// "missing" from "wrong" to the caller.
			slog.Info("apps: internal request rejected",
				"method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
			writeInternalError(w, http.StatusUnauthorized, kindUnauthorized,
				"invalid or missing internal service credential")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken extracts the credential from an `Authorization: Bearer <tok>`
// header. The scheme match is case-insensitive per RFC 7235; anything else
// (Basic, a bare token, an empty value) is not a bearer credential.
func bearerToken(header string) (string, bool) {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	tok := strings.TrimSpace(header[len(prefix):])
	return tok, tok != ""
}

// ---------------------------------------------------------------------------
// Wire shapes
// ---------------------------------------------------------------------------

type resolveResponse struct {
	AppID       string `json:"app_id"`
	VersionHash string `json:"version_hash"`
}

type verifyTokenRequest struct {
	AppID   string `json:"app_id"`
	TokenID string `json:"token_id"`
	Secret  string `json:"secret"`
}

type verifyTokenResponse struct {
	OK bool `json:"ok"`
}

type verifySessionRequest struct {
	PID string `json:"pid"`
}

type verifySessionResponse struct {
	// UserID is "" when the forwarded credential authenticates nobody.
	UserID string `json:"user_id"`
	// ProjectMember is the project-scoped FGA read relation on {pid}.
	ProjectMember bool `json:"project_member"`
}

type impersonateRequest struct {
	AppID string `json:"app_id"`
}

type impersonateResponse struct {
	Token string `json:"token"`
}

// appendLogRequest is one render-log record (spec §6.6). app_id comes from
// the path; the optional body field is accepted when it agrees, so
// app-worker can marshal the same record shape the author route serializes
// (renderLogJSON) without stripping a field.
type appendLogRequest struct {
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
// GET /internal/v1/apps/{pid}/{name}/resolve?channel=production|draft
// ---------------------------------------------------------------------------

// handleResolve maps (project, app name, channel) to the version the worker
// should render. The worker caches the answer ≤15 s (spec §5.2), which is
// why a promote is only eventually visible.
func (h *InternalHandlers) handleResolve(w http.ResponseWriter, r *http.Request) {
	pid, err := uuid.Parse(r.PathValue("pid"))
	if err != nil {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, "invalid project id")
		return
	}
	channel := r.URL.Query().Get("channel")
	if channel == "" {
		// Production is the viewer-facing default; @draft is opt-in.
		channel = channelProduction
	}
	if channel != channelProduction && channel != channelDraft {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest,
			`channel must be "production" or "draft"`)
		return
	}
	res, err := h.Store.Resolve(r.Context(), pid, r.PathValue("name"), channel)
	if errors.Is(err, ErrNotFound) {
		// Unknown app AND "channel has no version yet" are the same answer
		// to the worker: there is nothing to render here.
		writeInternalError(w, http.StatusNotFound, kindNotFound, "no such app or channel")
		return
	}
	if err != nil {
		slog.Error("apps: internal resolve", "project", pid, "name", r.PathValue("name"), "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable, "could not resolve the app")
		return
	}
	writeJSON(w, http.StatusOK, resolveResponse{AppID: res.AppID, VersionHash: res.VersionHash})
}

// ---------------------------------------------------------------------------
// GET /internal/v1/bundles/{hash}
// ---------------------------------------------------------------------------

// handleBundle serves raw bundle bytes by content hash. Content-addressed
// and immutable — but STILL behind the service credential: content
// addressing hides a bundle from someone who cannot guess its hash, which is
// not an authorization control (hashes travel through resolve responses,
// author APIs and logs). The worker re-verifies SHA-256 on receipt
// (spec §5.2), so a wrong body is caught there too.
func (h *InternalHandlers) handleBundle(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	if !bundleHashRe.MatchString(hash) {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest,
			"hash must be a hex SHA-256 digest")
		return
	}
	bundle, err := h.Store.GetBundle(r.Context(), hash)
	if errors.Is(err, ErrNotFound) {
		// No Cache-Control here: a miss must never be cached as immutable —
		// the very next upload can make this hash resolvable.
		writeInternalError(w, http.StatusNotFound, kindNotFound, "no such bundle")
		return
	}
	if err != nil {
		slog.Error("apps: internal bundle fetch", "hash", hash, "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable, "could not read the bundle")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", bundleCacheControl)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bundle)
}

// ---------------------------------------------------------------------------
// POST /internal/v1/viewer-tokens/verify
// ---------------------------------------------------------------------------

// handleVerifyViewerToken answers "is this (app, token_id, secret) live?".
// A wrong secret, an unknown token_id, another app's token and a revoked
// token are all the same answer — `{"ok":false}`, HTTP 200 — so the caller
// (and anyone probing through it) learns nothing beyond yes/no.
//
// # Rate limiting is app-worker's job, not this endpoint's
//
// Spec §5.3 assigns the abuse controls to the worker: positive results
// cached ≤15 s, failures negative-cached 60 s per (app_id, token_id), and
// 10 failures/min per (client IP, app) → 429 `Retry-After: 60`. The client
// IP is the VIEWER's, which only app-worker sees — pipeline-api sees the
// worker's pod IP and would bucket every viewer together. Implementing a
// limiter here would therefore be both wrong-keyed and a duplicate.
func (h *InternalHandlers) handleVerifyViewerToken(w http.ResponseWriter, r *http.Request) {
	var req verifyTokenRequest
	if !readInternalJSON(w, r, &req) {
		return
	}
	if _, err := uuid.Parse(req.AppID); err != nil {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, "invalid app_id")
		return
	}
	if _, err := uuid.Parse(req.TokenID); err != nil {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, "invalid token_id")
		return
	}
	if req.Secret == "" {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, `"secret" is required`)
		return
	}
	ok, err := h.Store.VerifyToken(r.Context(), req.AppID, req.TokenID, req.Secret)
	if err != nil {
		slog.Error("apps: internal viewer-token verify", "app_id", req.AppID, "token_id", req.TokenID, "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable, "could not verify the viewer token")
		return
	}
	writeJSON(w, http.StatusOK, verifyTokenResponse{OK: ok})
}

// ---------------------------------------------------------------------------
// POST /internal/v1/sessions/verify
// ---------------------------------------------------------------------------

// handleVerifySession validates a forwarded platform session credential and
// reports project membership — the mechanism behind app-worker's `@draft`
// previews (spec §5.2). The credential is handed to the SAME
// auth.UserResolver the browser routes use, so there is exactly one session
// implementation in the system.
//
// # How app-worker forwards the caller's credential (part of the contract)
//
//   - Cookie sessions (browser): forward the `Cookie` header verbatim. It
//     rides through untouched.
//   - Bearer sessions (the CLI's api-token renders, spec §5.5): forward the
//     caller's `Authorization` value in ForwardedAuthorizationHeader —
//     `Authorization` itself is unavailable, since it carries app-worker's
//     own service credential and one header cannot hold two bearer values.
//
// This handler rebuilds the credential view for the resolver on a private
// copy of the request: ForwardedAuthorizationHeader becomes `Authorization`
// and is itself removed. When no forwarding header is present,
// `Authorization` is REMOVED rather than left in place — otherwise the
// resolver would be handed app-worker's service credential as if it were a
// user credential, and a bearer-capable resolver would authenticate the
// worker as whatever principal that token maps to.
//
// A credential that authenticates nobody is a 200 with `user_id: ""` and
// `project_member: false`, NOT a 401. On this surface 401 means one thing
// only — the service credential is bad — so app-worker can tell "deny this
// viewer" apart from "I am misconfigured".
func (h *InternalHandlers) handleVerifySession(w http.ResponseWriter, r *http.Request) {
	var req verifySessionRequest
	if !readInternalJSON(w, r, &req) {
		return
	}
	pid, err := uuid.Parse(req.PID)
	if err != nil {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, "invalid pid")
		return
	}

	// The resolver may write to its ResponseWriter (PostgresResolver
	// refreshes the session cookie). That Set-Cookie is addressed to the
	// viewer's browser, not to app-worker — discard it instead of letting it
	// ride out on the internal response.
	user, authed, err := h.Resolver.UserFor(discardResponseWriter{}, callerCredentialRequest(r))
	if err != nil {
		slog.Error("apps: internal session verify", "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable, "could not verify the session")
		return
	}
	if !authed {
		writeJSON(w, http.StatusOK, verifySessionResponse{})
		return
	}

	lakekeeperPID, err := h.Projects.LakekeeperProjectID(r.Context(), pid)
	if errors.Is(err, ErrNotFound) {
		// "Is this user a member of a project that does not exist?" — no.
		writeJSON(w, http.StatusOK, verifySessionResponse{UserID: user.ID.String()})
		return
	}
	if err != nil {
		slog.Error("apps: internal session verify: project lookup", "project", pid, "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable, "could not read the project")
		return
	}
	if lakekeeperPID == "" {
		// Unprovisioned project: there is nothing to authorize against, so
		// fail closed with a retryable status rather than answering "false"
		// (which the worker would cache as a denial).
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable,
			"project authz not yet provisioned (lakekeeper project pending)")
		return
	}
	// Same relation the author read routes use: "can see this project's
	// apps" is exactly project membership for @draft-preview purposes.
	member, err := h.Authz.Check(r.Context(),
		authz.UserObject(user.ID.String()).String(), relationRead, authz.ProjectObject(lakekeeperPID))
	if err != nil {
		slog.Error("apps: internal session verify: authz", "project", pid, "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable, "authz backend unavailable")
		return
	}
	writeJSON(w, http.StatusOK, verifySessionResponse{UserID: user.ID.String(), ProjectMember: member})
}

// callerCredentialRequest returns a shallow copy of r whose Authorization
// header holds the CALLER's credential rather than app-worker's service
// credential: the value of ForwardedAuthorizationHeader when present, and
// nothing at all when absent.
//
// The Header map is CLONED, never mutated in place — http.Header is shared by
// reference, and the original still has to carry the service credential for
// anything reading the request after this handler (middleware, access logs,
// the caller's own copy in tests).
func callerCredentialRequest(r *http.Request) *http.Request {
	out := r.Clone(r.Context())
	// r.Clone deep-copies Header, so these edits cannot reach the original.
	if forwarded := out.Header.Get(ForwardedAuthorizationHeader); forwarded != "" {
		out.Header.Set("Authorization", forwarded)
	} else {
		out.Header.Del("Authorization")
	}
	out.Header.Del(ForwardedAuthorizationHeader)
	return out
}

// discardResponseWriter swallows everything written to it. Used to give the
// session resolver a writer whose side effects (cookie refresh) must not
// escape into the internal response.
type discardResponseWriter struct{}

func (discardResponseWriter) Header() http.Header         { return make(http.Header) }
func (discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (discardResponseWriter) WriteHeader(int)             {}

// ---------------------------------------------------------------------------
// POST /internal/v1/impersonate
// ---------------------------------------------------------------------------

// handleImpersonate mints a fresh, short-lived impersonation JWT for the
// app's synthetic identity (spec §5.2/§5.4).
//
// Two invariants:
//   - The subject is derived SERVER-SIDE from the app row. The worker names
//     an app_id and nothing else; it can never ask for an arbitrary
//     identity, nor for an app in a project it named itself (the project
//     comes from the row).
//   - No caching, anywhere: every call mints a fresh token and every mint is
//     audited. A cached token would outlive the FGA state it was minted
//     against.
func (h *InternalHandlers) handleImpersonate(w http.ResponseWriter, r *http.Request) {
	var req impersonateRequest
	if !readInternalJSON(w, r, &req) {
		return
	}
	if _, err := uuid.Parse(req.AppID); err != nil {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, "invalid app_id")
		return
	}
	app, ok := h.mustGetApp(r.Context(), w, req.AppID)
	if !ok {
		return
	}
	token, err := h.Identity.Mint(r.Context(), app.ID, app.ProjectID)
	if err != nil {
		slog.Error("apps: internal impersonate", "app_id", app.ID, "project", app.ProjectID, "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable,
			"could not mint the app credential")
		return
	}
	projectUUID, _ := uuid.Parse(app.ProjectID)
	// Audited unconditionally (spec §5.2: "every mint is audited"). The
	// actor is the service principal — the internal surface has no user.
	internalAudit("impersonate", projectUUID, app.ID, "app", app.Name,
		"jwt_subject", AppJWTSubject(app.ID))
	writeJSON(w, http.StatusOK, impersonateResponse{Token: token})
}

// ---------------------------------------------------------------------------
// POST /internal/v1/apps/{app_id}/logs
// ---------------------------------------------------------------------------

// handleAppendLog appends one render-log record to the app's ring buffer
// (spec §6.6). The store trims by both bounds (newest 200 / retention age)
// on every append, so this route needs no cleanup of its own.
func (h *InternalHandlers) handleAppendLog(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("app_id")
	if _, err := uuid.Parse(appID); err != nil {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, "invalid app id")
		return
	}
	var req appendLogRequest
	if !readInternalJSON(w, r, &req) {
		return
	}
	rec, ok := req.toRecord(w, appID)
	if !ok {
		return
	}
	if _, ok := h.mustGetApp(r.Context(), w, appID); !ok {
		return
	}
	if err := h.Store.AppendRenderLog(r.Context(), rec); err != nil {
		slog.Error("apps: internal append render log", "app_id", appID, "request_id", rec.RequestID, "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable, "could not append the render log")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// toRecord validates the posted record and converts it. Validation is strict
// because the columns are: version_hash is CHAR(64) (a shorter value would
// be silently space-padded and never match a real hash again) and
// principal_kind is a fixed taxonomy.
//
// Note for app-worker: a render that fails BEFORE a version is resolved has
// no version_hash and therefore cannot be recorded here — those belong in
// the worker's own structured log, not the per-app author ring buffer.
func (req *appendLogRequest) toRecord(w http.ResponseWriter, appID string) (RenderLogRecord, bool) {
	bad := func(msg string) (RenderLogRecord, bool) {
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, msg)
		return RenderLogRecord{}, false
	}
	if req.AppID != "" && req.AppID != appID {
		return bad("app_id in the body does not match the path")
	}
	if _, err := uuid.Parse(req.RequestID); err != nil {
		return bad("request_id must be a UUID")
	}
	if !bundleHashRe.MatchString(req.VersionHash) {
		return bad("version_hash must be a hex SHA-256 digest")
	}
	if req.Channel != channelProduction && req.Channel != channelDraft {
		return bad(`channel must be "production" or "draft"`)
	}
	if !principalKinds[req.PrincipalKind] {
		return bad(`principal_kind must be "viewer_token" or "platform_user"`)
	}
	if req.Outcome == "" {
		return bad(`"outcome" is required`)
	}
	if req.DurationMS < 0 {
		return bad("duration_ms must not be negative")
	}
	startedAt := time.Now().UTC()
	if req.StartedAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, req.StartedAt)
		if err != nil {
			return bad("started_at must be an RFC3339 timestamp")
		}
		startedAt = parsed
	}
	logText := req.LogText
	if len(logText) > MaxRenderLogBytes {
		logText = logText[:MaxRenderLogBytes]
	}
	errText := req.Error
	if len(errText) > MaxRenderLogBytes {
		errText = errText[:MaxRenderLogBytes]
	}
	return RenderLogRecord{
		RequestID:     req.RequestID,
		AppID:         appID,
		VersionHash:   req.VersionHash,
		Channel:       req.Channel,
		PrincipalKind: req.PrincipalKind,
		PrincipalID:   req.PrincipalID,
		StartedAt:     startedAt,
		DurationMS:    req.DurationMS,
		Outcome:       req.Outcome,
		LogText:       logText,
		Error:         errText,
	}, true
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

// mustGetApp loads the app row by id, answering 404 for an unknown app.
// Every route that takes an app_id from the worker goes through it: the row
// is what makes the request meaningful (impersonate derives the project from
// it, logs need the FK to exist).
func (h *InternalHandlers) mustGetApp(ctx context.Context, w http.ResponseWriter, appID string) (*App, bool) {
	app, err := h.Store.GetByID(ctx, appID)
	if errors.Is(err, ErrNotFound) {
		writeInternalError(w, http.StatusNotFound, kindNotFound, "no such app")
		return nil, false
	}
	if err != nil {
		slog.Error("apps: internal get app", "app_id", appID, "err", err)
		writeInternalError(w, http.StatusServiceUnavailable, kindUnavailable, "could not read the app")
		return nil, false
	}
	return app, true
}

// readInternalJSON strict-decodes a size-capped JSON body, answering with the
// kind-carrying envelope (unlike handlers.go's readJSONBody, which uses the
// author routes' plain {"error": …} shape).
func readInternalJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxInternalBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) || strings.Contains(err.Error(), "http: request body too large") {
			writeInternalError(w, http.StatusRequestEntityTooLarge, kindBadRequest,
				fmt.Sprintf("body exceeds %d bytes", maxInternalBodyBytes))
			return false
		}
		writeInternalError(w, http.StatusBadRequest, kindBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// writeInternalError emits the standard kind-carrying error envelope
// (spec §8) that the internal and viewer-facing surfaces share:
// {"error": "...", "kind": "..."}.
//
// This differs from the author routes' plain {"error": "..."} (handlers.go's
// writeError), which follows pipeline-api's house convention for
// platform-user-facing routes. The split is intentional: `kind` exists so
// app-worker (and the viewer surface it serves) can branch on the failure
// class without string-matching, and it is app-worker that needs it.
// request_id — the envelope's third field on the viewer surface — is
// app-worker's to add; it owns the id and this response is not the one the
// viewer sees.
func writeInternalError(w http.ResponseWriter, status int, kind, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "kind": kind})
}

// internalAudit emits one structured line per audited internal action,
// matching handlers.go's author `audit` field layout so both surfaces are
// queryable together. The actor is the service principal (the internal API
// has no platform user).
func internalAudit(action string, projectID uuid.UUID, appID string, kv ...any) {
	args := append([]any{
		"action", action,
		"actor", "service:app-worker",
		"project_id", projectID.String(),
		"app_id", appID,
	}, kv...)
	slog.Info("apps: internal audit", args...)
}
