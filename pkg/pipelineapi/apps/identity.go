package apps

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// IdentityManager owns the FGA registration and impersonation-minting
// lifecycle for an app's synthetic identity (spec §5.4). The author handlers
// and the internal API depend only on this interface; identityManager below
// is the concrete FGA+mint implementation. Mint does NOT reuse
// tokens.MintImpersonation — that minter takes no subject argument and
// derives one from a ctx-bound *store.User, which an app is not; see
// .superpowers/sdd/2026-07-23-rfc-028-user-apps-implementation/
// task-P0-report.md §B/§C for the full evidence trail.
//
// Create -> Register writes the app's `viewer` tuple on the project.
// Delete -> Unregister deletes that tuple FIRST, then the caller deletes
// the app's rows (mirrors runbackend.K8sBackend.CancelRun's tuple-before-
// rows ordering). recorderIdentity below is a test fake that records call
// order so callers can assert this sequencing without a real FGA backend.
type IdentityManager interface {
	// Register grants the app identity a read-only ("viewer") FGA relation
	// on the project. Called once at app creation.
	Register(ctx context.Context, appID, projectID string) error
	// Unregister revokes the app identity's FGA relation on the project.
	// Must be called, and complete, BEFORE the app's rows are deleted.
	Unregister(ctx context.Context, appID, projectID string) error
	// Mint issues a short-lived impersonation JWT for the app identity,
	// scoped to projectID. The JWT `sub` claim is AppJWTSubject(appID);
	// the FGA subject it authorizes as is AppFGASubject(appID).
	Mint(ctx context.Context, appID, projectID string) (token string, err error)
}

// AppJWTSubject returns the raw JWT `sub` claim minted for an app's
// impersonation token: "app-<uuid>" — no "oidc~"/"user:" prefix.
//
// This is the bare, unprefixed form (mirroring MintRunToken's bare run
// UUID, tokens/mint.go:161-166): lakekeeper (and pipeline-api's own FGA
// tuple writes) compose the "oidc~"/"user:" prefixing downstream, at
// authz.UserObject. Pre-composing the prefix here would double it — see
// AppFGASubject and task-P0-report.md §C for the evidence trail
// (authz/types.go:80-90, tokens/mint.go:132-166, mint_test.go:151-156).
//
// The prefix comes from tokens.AppSubjectPrefix — the same constant
// tokens.MintAppToken builds the `sub` claim from — so this helper and the
// mint cannot drift apart.
func AppJWTSubject(appUUID string) string {
	return tokens.AppSubjectPrefix + appUUID
}

// AppFGASubject returns the OpenFGA user string the app's `viewer` tuple is
// written for, and that a downstream authorization check must target:
// "user:oidc~app-<uuid>". Always compose it via authz.UserObject (never
// hand-roll the "oidc~"/"user:" prefix) so this stays correct if that
// helper's normalization rules ever change.
func AppFGASubject(appUUID string) string {
	return authz.UserObject(AppJWTSubject(appUUID)).String()
}

// identityManager is the concrete, production IdentityManager (RFC 028 §5.4).
//
// Everything it does is expressed against ONE lakekeeper project object,
// resolved per call from the Datuplet project UUID the callers hand it (P2's
// handlers and P3's internal routes both pass App.ProjectID — the Datuplet
// id, never the lakekeeper one). Resolving it here rather than at the call
// sites keeps the "which project id goes in the tuple?" decision in a single
// place: the FGA object and the JWT's project_id claim are always the
// LAKEKEEPER id, and an app can never be registered against, or minted for, a
// project whose lakekeeper counterpart does not exist yet.
type identityManager struct {
	authz    authz.Authorizer
	signer   *tokens.Signer
	projects ProjectLookup
}

// NewIdentityManager returns the production IdentityManager.
//
// signer may be nil in a deployment without a signing key: Register and
// Unregister still work (they are pure FGA operations), and Mint fails
// explicitly — which the internal route maps to 503 — rather than handing
// app-worker an empty credential.
func NewIdentityManager(a authz.Authorizer, signer *tokens.Signer, projects ProjectLookup) IdentityManager {
	return &identityManager{authz: a, signer: signer, projects: projects}
}

// Register writes the app identity's `viewer` tuple on the project. Idempotent
// (a duplicate tuple is not an error), so a retried create or a second upload
// after a half-failed registration converges instead of wedging the app.
func (m *identityManager) Register(ctx context.Context, appID, projectID string) error {
	lakekeeperPID, err := m.resolveProject(ctx, appID, projectID)
	if err != nil {
		return err
	}
	// Bare subject in, composed subject out: authz.WriteProjectTuple runs it
	// through UserObject, so the tuple's user is exactly AppFGASubject(appID).
	if err := authz.WriteProjectTuple(ctx, m.authz,
		AppJWTSubject(appID), authz.RelationViewer, lakekeeperPID); err != nil {
		return fmt.Errorf("apps: register identity for app %s: %w", appID, err)
	}
	identityAudit("app_identity_created", appID, projectID, lakekeeperPID,
		"fga_subject", AppFGASubject(appID), "relation", authz.RelationViewer)
	return nil
}

// Unregister deletes that tuple. Callers MUST complete this before deleting
// the app's rows (spec §5.4) — a surviving tuple for a deleted app id is a
// grant nobody can see or clean up. An already-missing tuple is tolerated so
// a retried delete succeeds.
func (m *identityManager) Unregister(ctx context.Context, appID, projectID string) error {
	lakekeeperPID, err := m.resolveProject(ctx, appID, projectID)
	if err != nil {
		return err
	}
	if err := authz.DeleteProjectTuple(ctx, m.authz,
		AppJWTSubject(appID), authz.RelationViewer, lakekeeperPID); err != nil {
		return fmt.Errorf("apps: unregister identity for app %s: %w", appID, err)
	}
	identityAudit("app_identity_deleted", appID, projectID, lakekeeperPID,
		"fga_subject", AppFGASubject(appID), "relation", authz.RelationViewer)
	return nil
}

// Mint issues a FRESH 60 s catalog JWT for the app identity. Nothing caches
// the result — not here, not in the internal route, not in app-worker (spec
// §5.4): reusing a jti across renders would make the audit trail unable to
// attribute a query to a render, and would let a credential outlive the FGA
// state it was minted against.
//
// The returned string is the revealed JWT. It is the ONLY value in this
// package that must never reach a log line; the audit record below carries the
// jti and app id instead.
func (m *identityManager) Mint(ctx context.Context, appID, projectID string) (string, error) {
	lakekeeperPID, err := m.resolveProject(ctx, appID, projectID)
	if err != nil {
		return "", err
	}
	tok, jti, err := tokens.MintAppToken(m.signer, appID, lakekeeperPID)
	if err != nil {
		return "", fmt.Errorf("apps: mint app token for %s: %w", appID, err)
	}
	// Contract shape: {app_id, jti} (+ the non-token operational context every
	// record in this package carries). The jti is the ONLY token-derived value
	// that may be logged — jwt_subject is deliberately NOT here even though it
	// is harmless and recoverable from app_id, because "the jti is the only
	// thing from the token" is an invariant a test can check, and a list of
	// individually-harmless extras is not.
	identityAudit("impersonation_minted", appID, projectID, lakekeeperPID,
		"jti", jti, "ttl_seconds", int64(tokens.AppTokenLifetime.Seconds()))
	return tok.Reveal(), nil
}

// resolveProject validates the arguments and maps the Datuplet project UUID to
// the lakekeeper project id every FGA object and JWT claim is built from.
//
// Argument validation runs BEFORE any backend call, so a malformed request
// never reaches OpenFGA or the project store; and an unknown or
// still-provisioning project is an error, never an empty id — a tuple on
// `project:` (or a token naming no project) would be a grant against an object
// that does not exist.
func (m *identityManager) resolveProject(ctx context.Context, appID, projectID string) (string, error) {
	if appID == "" {
		return "", errors.New("apps: identity: appID is required")
	}
	parsed, err := uuid.Parse(projectID)
	if err != nil {
		return "", fmt.Errorf("apps: identity: invalid project id %q: %w", projectID, err)
	}
	if m.projects == nil {
		return "", errors.New("apps: identity: no project lookup configured")
	}
	// Belt and braces: Handler()'s nil-gate already refuses to register the
	// app routes without an authorizer, so this is unreachable in production —
	// but a clear error beats a nil-pointer panic if a future call site
	// constructs the manager differently.
	if m.authz == nil {
		return "", errors.New("apps: identity: no authorizer configured")
	}
	lakekeeperPID, err := m.projects.LakekeeperProjectID(ctx, parsed)
	if err != nil {
		return "", fmt.Errorf("apps: identity: look up lakekeeper project for %s: %w", projectID, err)
	}
	if lakekeeperPID == "" {
		return "", fmt.Errorf("apps: identity: lakekeeper project for %s is not provisioned yet", projectID)
	}
	return lakekeeperPID, nil
}

// identityAudit emits one structured line per app-identity lifecycle event
// (spec §9), in the same field layout as handlers.go's `audit` and
// internal.go's `internalAudit` so all three surfaces are queryable together.
//
// It NEVER carries a token. impersonation_minted records the jti — the
// credential's audit handle — and nothing that reconstructs the JWT.
func identityAudit(action, appID, projectID, lakekeeperProjectID string, kv ...any) {
	args := append([]any{
		"action", action,
		"actor", "service:app-identity",
		"app_id", appID,
		"project_id", projectID,
		"lakekeeper_project_id", lakekeeperProjectID,
	}, kv...)
	slog.Info("apps: identity audit", args...)
}

// RecorderIdentity is a test fake IdentityManager that records every call
// (method + args) in order instead of touching FGA/JWT machinery. P2's
// tests use this to assert tuple-then-rows delete ordering (Unregister
// must be observed before the caller removes the app's store rows).
//
// Exported (unlike the brief's lowercase "recorderIdentity" shorthand)
// because every test file in this codebase's pipelineapi tree uses the
// external `_test` package convention (package apps_test, not package
// apps) — see e.g. pkg/pipelineapi/tokens/mint_test.go,
// pkg/pipelineapi/auth/session_test.go — so an unexported fake would be
// invisible to P2/P3's test files.
type RecorderIdentity struct {
	Calls []string
}

var _ IdentityManager = (*RecorderIdentity)(nil)

func (r *RecorderIdentity) Register(ctx context.Context, appID, projectID string) error {
	r.Calls = append(r.Calls, "Register:"+appID+":"+projectID)
	return nil
}

func (r *RecorderIdentity) Unregister(ctx context.Context, appID, projectID string) error {
	r.Calls = append(r.Calls, "Unregister:"+appID+":"+projectID)
	return nil
}

func (r *RecorderIdentity) Mint(ctx context.Context, appID, projectID string) (string, error) {
	r.Calls = append(r.Calls, "Mint:"+appID+":"+projectID)
	return "fake-token-" + appID, nil
}
