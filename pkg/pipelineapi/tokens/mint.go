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
//	aud=datuplet-api      requires token_kind=user
//	aud=datuplet-catalog  requires token_kind ∈ {run, impersonation, local-cli}
//
// User tokens are emitted by the user-login flow and consumed by
// pipeline-api itself (NOT lakekeeper). Run tokens are emitted at run
// trigger and consumed by lakekeeper. Impersonation tokens are minted
// per storage-browse request and consumed by lakekeeper. Local-CLI
// tokens are minted by `POST /api/v1/auth/token` for the `datuplet run
// --remote` flow and consumed by lakekeeper exactly like impersonation
// tokens, but with a 1h TTL so a developer's laptop session lasts a
// working hour.
const (
	TokenKindUser          = "user"
	TokenKindRun           = "run"
	TokenKindImpersonation = "impersonation"
	TokenKindLocalCLI      = "local-cli"
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

// ImpersonationLifetime is the short TTL minted on impersonation tokens.
// 60s is enough for one storage-browse round-trip; a longer ceiling would
// turn a leaked impersonation JWT into a meaningful exfil window.
const ImpersonationLifetime = 60 * time.Second

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
