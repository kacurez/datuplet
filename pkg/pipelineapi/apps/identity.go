package apps

import (
	"context"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// IdentityManager owns the FGA registration and impersonation-minting
// lifecycle for an app's synthetic identity (spec §5.4). Store/handlers
// (P2) and the internal API (P3) depend only on this interface; the
// concrete FGA+mint implementation is wired in P4 (see identityManager
// below and .superpowers/sdd/2026-07-23-rfc-028-user-apps-implementation/
// task-P0-report.md §B/§C for why Mint cannot reuse tokens.MintImpersonation
// as-is).
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
func AppJWTSubject(appUUID string) string {
	return "app-" + appUUID
}

// AppFGASubject returns the OpenFGA user string the app's `viewer` tuple is
// written for, and that a downstream authorization check must target:
// "user:oidc~app-<uuid>". Always compose it via authz.UserObject (never
// hand-roll the "oidc~"/"user:" prefix) so this stays correct if that
// helper's normalization rules ever change.
func AppFGASubject(appUUID string) string {
	return authz.UserObject(AppJWTSubject(appUUID)).String()
}

// identityManager is the concrete, production IdentityManager. Its FGA
// registration and token-minting wiring is implemented in P4 (RFC 028
// Part 4) — see task-P0-report.md §B for the settled mint mechanism
// (a new tokens.MintAppToken, since tokens.MintImpersonation takes no
// subject parameter and derives its subject from a ctx-bound
// *store.User, which an app is not). Until then every method panics so
// any code path that reaches it fails loudly instead of silently
// no-opping; P4 may need to change NewIdentityManager's signature to
// inject its dependencies (authz.Authorizer, a *tokens.Signer, a project
// lookup) — that is expected and not a compatibility concern for P1.
type identityManager struct{}

// NewIdentityManager returns the production IdentityManager. Every method
// panics until P4 fills in the concrete implementation.
func NewIdentityManager() IdentityManager {
	return &identityManager{}
}

func (*identityManager) Register(ctx context.Context, appID, projectID string) error {
	panic("implemented in P4")
}

func (*identityManager) Unregister(ctx context.Context, appID, projectID string) error {
	panic("implemented in P4")
}

func (*identityManager) Mint(ctx context.Context, appID, projectID string) (string, error) {
	panic("implemented in P4")
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
