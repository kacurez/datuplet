package tokens

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
)

// Token kinds. The verifier cross-checks `aud` against `token_kind`:
//
//	aud=datuplet-api           requires token_kind ∈ {user, cli-api}
//	aud=datuplet-catalog       requires token_kind ∈ {run, impersonation, local-cli, query, app}
//	aud=datuplet-query-worker  requires token_kind = internal-query
//
// User tokens are emitted by the user-login flow and consumed by
// pipeline-api itself (NOT lakekeeper). Run tokens are emitted at run
// trigger and consumed by lakekeeper. Impersonation tokens are minted
// per storage-browse request and consumed by lakekeeper. Local-CLI
// tokens are minted by `POST /api/v1/auth/token` for the `datuplet run
// --remote` flow and consumed by lakekeeper exactly like impersonation
// tokens, but with a 1h TTL so a developer's laptop session lasts a
// working hour.
// TokenKindQuery and TokenKindInternalQuery are the RFC 022 ad-hoc-query
// kinds. A `query` token is presented by the DuckDB engine to lakekeeper
// (aud=datuplet-catalog) — lakekeeper authorizes via FGA using `sub`, so
// the caller's own grants apply (viewers get read-only vended creds). An
// `internal-query` token authenticates pipeline-api→query-worker hops
// (aud=datuplet-query-worker) and must never be accepted by lakekeeper.
// TokenKindApp is the RFC 028 user-apps kind: a 60s credential for an app's
// synthetic identity, minted per render by MintAppToken. It shares
// impersonation's audience and consumer (lakekeeper via the query path), but
// stays a DISTINCT kind so pipeline-api's own query route — and any future
// kind-aware policy — can tell "this came from a user app" from "this came
// from a human's interactive session" without pattern-matching on `sub`.
const (
	TokenKindUser          = "user"
	TokenKindRun           = "run"
	TokenKindImpersonation = "impersonation"
	TokenKindLocalCLI      = "local-cli"
	TokenKindCLIAPI        = "cli-api"
	TokenKindQuery         = "query"
	TokenKindInternalQuery = "internal-query"
	TokenKindApp           = "app"
)

// Default JWT claim constants — issuer + token-type used by run tokens.
const (
	tokenIssuer = "datuplet-api"
	tokenType   = "run"
)

// TableTokenAudience is the fixed JWT `aud` claim for tokens consumed by
// lakekeeper (run tokens + impersonation tokens). The literal also serves
// as the source of truth pipeline-api signs against — anything else is
// verifier-rejected.
const TableTokenAudience = "datuplet-catalog"

// APITokenAudience is the fixed JWT aud claim for tokens consumed by
// pipeline-api itself (not lakekeeper). Used by the bearer-JWT auth
// resolver to scope CLI bearer tokens to this service.
const APITokenAudience = "datuplet-api"

// QueryWorkerAudience is the fixed JWT aud claim for internal-query tokens
// authenticating the pipeline-api→query-worker hop (RFC 022). The
// query-worker verifier rejects any other audience.
const QueryWorkerAudience = "datuplet-query-worker"

// MaxQueryTokenLifetime caps the TTL minted on query / internal-query
// tokens: RFC 022 §5.2 max query timeout (300s) + 30s slack. A leaked
// query JWT can vend storage creds for at most this window. Callers
// typically pass min(timeout+30s, this) — MintQueryToken /
// MintInternalQueryToken clamp to it defensively regardless.
const MaxQueryTokenLifetime = 330 * time.Second

// ImpersonationLifetime is the short TTL minted on impersonation tokens.
// 60s is enough for one storage-browse round-trip; a longer ceiling would
// turn a leaked impersonation JWT into a meaningful exfil window.
const ImpersonationLifetime = 60 * time.Second

// AppTokenLifetime is the TTL minted on user-app tokens (RFC 028 §5.4).
// Same 60s reasoning as ImpersonationLifetime: it covers one render's worth
// of queries, and every render mints afresh — nothing caches an app
// credential, so there is no reason to widen the window.
const AppTokenLifetime = 60 * time.Second

// AppSubjectPrefix is prepended to an app's UUID to form the JWT `sub` of its
// synthetic identity: "app-<uuid>".
//
// It lives here, next to the mint, so there is exactly ONE definition of the
// app subject shape in the tree — pkg/pipelineapi/apps.AppJWTSubject composes
// it from this constant rather than re-spelling the literal. Two independent
// spellings of an identity prefix is precisely how a system ends up writing
// FGA tuples for a subject its tokens never claim.
const AppSubjectPrefix = "app-"

// JTIForRunID returns the deterministic jti for the per-run JWT.
// One jti per run; cancellation is handled via FGA tuple deletion.
func JTIForRunID(runID string) string {
	return "run-tok-" + runID
}

// RunSpec describes the claims for the single per-run JWT. One token per
// run, identity `sub: user:<run-uuid>` (synthetic), audience
// `datuplet-catalog`.
//
// Note the deliberate absence of an `Actor` field: the actor claim is
// derived from the request context inside MintRunToken, never
// caller-supplied. Type-level enforcement makes audit-forgery impossible
// at the API surface.
type RunSpec struct {
	// RunID is the run UUID. Also forms the synthetic identity
	// `user:<RunID>` (the `oidc~` prefix is added at write time when
	// the FGA tuple is composed).
	RunID string

	// ProjectID is informational; lakekeeper reads it for audit.
	ProjectID string

	// PipelineName is informational.
	PipelineName string

	// Warehouse is the lakekeeper warehouse name; DG and TableCommit use it
	// for routing.
	Warehouse string

	// Audience overrides the default datuplet-catalog. Empty is normal.
	Audience string

	// Lifetime is required; the verifier rejects tokens without exp.
	// Callers typically pass runbackend.RunTokenLifetime (24h).
	Lifetime time.Duration
}

// MintRunToken produces a per-run RS256 JWT bound to the synthetic run
// identity. The actor claim is derived from `subjectFromCtx(ctx)` — the
// authenticated session subject — so MintRunToken cannot be tricked into
// minting on behalf of someone else. NEVER add an Actor field to RunSpec.
//
// Claims:
//
//	iss=datuplet-api
//	aud=datuplet-catalog
//	sub=<run-uuid>            (raw UUID; lakekeeper composes user:oidc~<sub>)
//	actor=<creator-uuid>      (raw UUID; same composition rule)
//	token_kind="run"
//	jti=run-tok-<run-uuid>
//	exp=now+Lifetime
//	project_id, run_id, pipeline_name (informational)
//
// Note on the raw-UUID `sub` shape: lakekeeper normalises every JWT subject
// into a fully-prefixed FGA user object as `<idp_id>~<sub>` and then
// `user:<that>` (idp_id is fixed to "oidc" at deploy). If `sub` already
// carried the prefixes, we'd end up with `user:oidc~user:oidc~<uuid>` on
// the FGA side and never match Datuplet's tuple writes (which always use
// authz.UserObject = `user:oidc~<uuid>`). Carrying the full prefixes
// in `sub` would produce `user:oidc~user:oidc~<uuid>` on the FGA side
// and never match Datuplet's tuple writes; raw is the correct shape.
func MintRunToken(ctx context.Context, signer *Signer, spec RunSpec) (string, error) {
	if signer == nil {
		return "", errors.New("signer is required")
	}
	if spec.RunID == "" {
		return "", errors.New("RunID is required")
	}
	if spec.Lifetime <= 0 {
		return "", errors.New("Lifetime must be positive (tokens without exp are rejected)")
	}
	actor, err := subjectFromCtx(ctx)
	if err != nil {
		return "", err
	}

	aud := spec.Audience
	if aud == "" {
		aud = TableTokenAudience
	}
	now := time.Now()
	jti := JTIForRunID(spec.RunID)
	// Synthetic identity for the run token: raw UUID. Lakekeeper composes
	// `user:oidc~<sub>` for every FGA Check internally — we MUST NOT
	// pre-compose it here or the FGA query will read
	// `user:oidc~user:oidc~<uuid>` and never match Datuplet's tuples
	// (written as `user:oidc~<uuid>` via authz.UserObject).
	syntheticSub := spec.RunID

	claims := jwt.MapClaims{
		"iss":           tokenIssuer,
		"aud":           aud,
		"sub":           syntheticSub,
		"actor":         actor,
		"token_kind":    TokenKindRun,
		"token_type":    tokenType, // legacy; lakekeeper currently ignores
		"project_id":    spec.ProjectID,
		"warehouse":     spec.Warehouse,
		"run_id":        spec.RunID,
		"pipeline_name": spec.PipelineName,
		"iat":           now.Unix(),
		"nbf":           now.Unix(),
		"exp":           now.Add(spec.Lifetime).Unix(),
		"jti":           jti,
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID

	s, err := tok.SignedString(signer.Private())
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	return s, nil
}

// MintImpersonation produces a short-lived impersonation JWT for the
// authenticated subject in ctx. Same `sub`/`actor` (the user is acting
// as themselves; the kind tells lakekeeper this is a query-time token,
// not a long-lived run token).
//
// The returned ImpersonationToken is a redacting wrapper — its String()
// / GoString() methods return "[redacted impersonation token]" so a
// stray %v in an error chain doesn't leak the JWT into logs. Callers
// that need to attach the token to an HTTP request must use
// `tok.Reveal()` explicitly.
//
// The brief takes NO `sub` argument — same audit-forgery prevention as
// MintRunToken: the subject is derived from ctx via subjectFromCtx.
func MintImpersonation(ctx context.Context, signer *Signer) (ImpersonationToken, error) {
	if signer == nil {
		return "", errors.New("signer is required")
	}
	sub, err := subjectFromCtx(ctx)
	if err != nil {
		return "", err
	}

	now := time.Now()
	claims := jwt.MapClaims{
		"iss":        tokenIssuer,
		"aud":        TableTokenAudience,
		"sub":        sub,
		"actor":      sub,
		"token_kind": TokenKindImpersonation,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(ImpersonationLifetime).Unix(),
		"jti":        fmt.Sprintf("imp-%s-%d", sub, now.UnixNano()),
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID

	s, err := tok.SignedString(signer.Private())
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	return ImpersonationToken(s), nil
}

// MintAppToken produces a short-lived catalog JWT for a user app's synthetic
// identity (RFC 028 §5.4). app-worker obtains one per render via
// POST /internal/v1/impersonate and presents it on the query path; lakekeeper
// authorizes it via FGA against the `viewer` tuple pipeline-api wrote for the
// app at create time.
//
// Claims:
//
//	iss=datuplet-api
//	aud=datuplet-catalog
//	sub=app-<app-uuid>        (BARE; lakekeeper composes user:oidc~<sub>)
//	actor=app-<app-uuid>      (the app acts as itself — there is no human
//	                           in the loop at render time; the human who
//	                           uploaded the bundle is audited separately by
//	                           the author routes)
//	token_kind="app"
//	project_id=<lakekeeper project id>   (informational; audit)
//	app_id=<app-uuid>                    (informational; audit)
//	iat/nbf=now, exp=now+AppTokenLifetime (60s)
//	jti=app-tok-<sub>-<unixnano>
//
// Unlike MintImpersonation / MintQueryToken this takes NO context and derives
// nothing from an authenticated session — an app is not a *store.User, so
// there is no ctx-bound subject to read (see the RFC 028 P0 preflight §B).
// The audit-forgery defence those minters get from ctx is provided here by the
// CALLER instead: the only call site resolves appUUID from the app row it just
// loaded by primary key, so a client can never name the identity it gets. Do
// not add a caller-supplied `sub` or `actor` parameter.
//
// Returns the ImpersonationToken redacting wrapper (same audience/consumer
// family, so it inherits RFC 019 §4.10 redaction for free) plus the token's
// jti. The jti is returned SEPARATELY and deliberately: it is the only part of
// the credential that may appear in an audit line, and returning it spares
// every caller from re-parsing the JWT just to log which mint happened.
func MintAppToken(signer *Signer, appUUID, projectID string) (ImpersonationToken, string, error) {
	if signer == nil {
		return "", "", errors.New("signer is required")
	}
	if appUUID == "" {
		return "", "", errors.New("appUUID is required")
	}
	// Fail closed: an app token whose project is unknown could not be
	// authorized against any `project:<id>` tuple anyway.
	if projectID == "" {
		return "", "", errors.New("projectID is required")
	}

	now := time.Now()
	sub := AppSubjectPrefix + appUUID
	jti := fmt.Sprintf("app-tok-%s-%d", sub, now.UnixNano())
	claims := jwt.MapClaims{
		"iss":        tokenIssuer,
		"aud":        TableTokenAudience,
		"sub":        sub,
		"actor":      sub,
		"token_kind": TokenKindApp,
		"project_id": projectID,
		"app_id":     appUUID,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(AppTokenLifetime).Unix(),
		"jti":        jti,
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID

	s, err := tok.SignedString(signer.Private())
	if err != nil {
		return "", "", fmt.Errorf("sign: %w", err)
	}
	return ImpersonationToken(s), jti, nil
}

// MintQueryToken produces a query-scoped catalog JWT for the
// authenticated subject in ctx (RFC 022). The DuckDB engine presents it
// to lakekeeper's iceberg-REST endpoint; lakekeeper authorizes via FGA
// using `sub`, so the caller's own grants apply — viewers get read-only
// vended creds. Subject derivation matches MintImpersonation
// (subjectFromCtx → raw UUID), so MintQueryToken cannot mint on behalf
// of someone else.
//
// Claims: iss=datuplet-api, aud=datuplet-catalog, token_kind="query",
// sub=actor=<caller-uuid>, per-request jti, iat/nbf=now,
// exp=now+min(ttl, MaxQueryTokenLifetime).
//
// ttl is clamped to MaxQueryTokenLifetime (330s = §5.2 timeout 300s +
// 30s slack). A non-positive ttl is an error — tokens without a sane exp
// are rejected, mirroring the Lifetime<=0 guard on MintRunToken /
// MintServiceToken.
//
// Returns the redacting QueryToken wrapper (RFC 019 §4.10); callers
// attach it via Reveal() at the HTTP-header audit point.
func MintQueryToken(ctx context.Context, signer *Signer, ttl time.Duration) (QueryToken, error) {
	return mintQueryFamily(ctx, signer, ttl, TableTokenAudience, TokenKindQuery, "qry")
}

// MintInternalQueryToken produces the internal hop token authenticating
// pipeline-api→query-worker requests (RFC 022). Same iss/sub/actor/jti
// discipline and TTL clamp as MintQueryToken, but aud=datuplet-query-worker
// and token_kind="internal-query" so it is verifier-rejected anywhere
// else (notably lakekeeper). Short-lived: clamped to MaxQueryTokenLifetime.
func MintInternalQueryToken(ctx context.Context, signer *Signer, ttl time.Duration) (QueryToken, error) {
	return mintQueryFamily(ctx, signer, ttl, QueryWorkerAudience, TokenKindInternalQuery, "iqry")
}

// mintQueryFamily is the shared body for the RFC 022 query / internal-query
// minters: identical claim shape and TTL clamp, differing only in aud,
// token_kind, and jti prefix.
func mintQueryFamily(ctx context.Context, signer *Signer, ttl time.Duration, aud, kind, jtiPrefix string) (QueryToken, error) {
	if signer == nil {
		return "", errors.New("signer is required")
	}
	if ttl <= 0 {
		return "", errors.New("ttl must be positive (tokens without exp are rejected)")
	}
	sub, err := subjectFromCtx(ctx)
	if err != nil {
		return "", err
	}
	if ttl > MaxQueryTokenLifetime {
		ttl = MaxQueryTokenLifetime
	}

	now := time.Now()
	// token_type deliberately omitted: it is a MintRunToken/MintServiceToken
	// legacy field (lakekeeper ignores it); new kinds carry token_kind only.
	claims := jwt.MapClaims{
		"iss":        tokenIssuer,
		"aud":        aud,
		"sub":        sub,
		"actor":      sub,
		"token_kind": kind,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(ttl).Unix(),
		"jti":        fmt.Sprintf("%s-%s-%d", jtiPrefix, sub, now.UnixNano()),
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID

	s, err := tok.SignedString(signer.Private())
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	return QueryToken(s), nil
}

// subjectFromCtx extracts the raw user UUID from ctx. Returns an error
// when no user is bound; MintRunToken / MintImpersonation refuse to mint
// anonymous-actor tokens.
//
// Returns the raw UUID (no `user:` / `oidc~` prefixes) — lakekeeper
// composes `user:oidc~<sub>` itself when normalising the JWT into an FGA
// user object. Pre-composing here causes the doubled-prefix bug described
// on MintRunToken.
//
// The audit-forgery argument: every token-mint call site must satisfy
// "the actor in the JWT == the authenticated session subject". By
// reading from ctx instead of accepting an actor argument, the type
// system enforces it — no caller can pass a forged identity.
func subjectFromCtx(ctx context.Context) (string, error) {
	user, ok := auth.UserFromContext(ctx)
	if !ok || user == nil {
		return "", errors.New("MintRunToken/MintImpersonation: no authenticated user in ctx (callers must run inside auth.WithUser)")
	}
	return user.ID.String(), nil
}

// ServiceTokenSpec describes a non-run service JWT used by pipeline-api's
// own tooling (currently the lakekeeper-bootstrap admin subcommand and
// the storage proxy's lakekeeper calls).
type ServiceTokenSpec struct {
	Subject  string        // required — short identifier for the caller
	Audience string        // optional; defaults to TableTokenAudience
	Lifetime time.Duration // required; service tokens still need exp
}

// MintServiceToken produces an RS256 JWT for an internal pipeline-api
// service caller. Carries `token_use="service"` so a verifier can reject
// it from data-plane RPC paths if it ever leaks.
func MintServiceToken(signer *Signer, spec ServiceTokenSpec) (string, error) {
	if signer == nil {
		return "", errors.New("signer is required")
	}
	if spec.Subject == "" {
		return "", errors.New("Subject is required")
	}
	if spec.Lifetime <= 0 {
		return "", errors.New("Lifetime must be positive (tokens without exp are rejected)")
	}

	aud := spec.Audience
	if aud == "" {
		aud = TableTokenAudience
	}
	now := time.Now()
	jti := "svc-tok-" + spec.Subject
	claims := jwt.MapClaims{
		"iss":        tokenIssuer,
		"aud":        aud,
		"sub":        spec.Subject,
		"token_type": "service",
		"token_use":  "service",
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(spec.Lifetime).Unix(),
		"jti":        jti,
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID

	s, err := tok.SignedString(signer.Private())
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	return s, nil
}
