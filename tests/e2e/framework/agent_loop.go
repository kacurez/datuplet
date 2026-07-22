// Package framework — RFC 027 E3 (agent-loop e2e) additive helpers.
//
// scenarios_agent_loop_test.go drives the REAL `datuplet` CLI binary end to
// end (discover schema → validate → fix → put → trigger → verify) over the
// headless auth surface (RFC 027 §7). These three thin wrappers expose the
// package-internal E2 machinery that path needs to the e2e test package:
//
//   - ResolveDatupletProjectID: the Datuplet projects-store UUID pipeline-api
//     resolves {pid} against — the value the CLI carries in $DATUPLET_PROJECT.
//   - MintAdminCLIToken: the cli-api bearer JWT the CLI carries in
//     $DATUPLET_API_TOKEN (the shape BearerJWTResolver accepts).
//   - K8sQueryTarget: the S3 + lakekeeper-vended-Resolver QueryTarget the
//     duckdb assertion helpers need to verify the run's output table.
//
// All three reuse existing E2 internals verbatim so E3 provisions its project,
// authenticates, and verifies output exactly the way the migrated scenarios do.
package framework

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// ResolveDatupletProjectID find-or-creates the Datuplet projects-store row
// bound to the harness's lakekeeper project (by NAME) and returns its Postgres
// UUID — the value pipeline-api resolves {pid} against (projects.GetByID) and
// the agent-loop CLI passes as $DATUPLET_PROJECT. Idempotent (cached per
// lakekeeper project name for the test process). Thin exported wrapper over the
// package-internal resolveDatupletProjectID (admin_exec.go) so the e2e test
// package can drive the same binding the migrated CR-path scenarios use.
func ResolveDatupletProjectID(ctx context.Context, h *FGAHarness) (string, error) {
	return resolveDatupletProjectID(ctx, h)
}

// MintAdminCLIToken mints a cli-api bearer JWT for the seeded e2e admin,
// suitable for $DATUPLET_API_TOKEN on the CLI's headless fast path
// (loadRemoteArgs: no --token-file + $DATUPLET_REMOTE + $DATUPLET_API_TOKEN).
//
// The token must satisfy BearerJWTResolver (pkg/pipelineapi/auth): RS256,
// iss=datuplet-api, aud=datuplet-api, token_kind=cli-api, exp present, and
// sub = a REAL DB user UUID GetUserByID resolves and that is not disabled.
// tokens.MintCLIAPIToken sets iss/aud/token_kind/exp; the `sub`/`actor` claim
// is derived from the auth context (never an argument — audit-forgery posture),
// so we thread the admin's REAL DB UUID resolved via GET /auth/me. That is
// deliberately NOT the fixed TestUser UUID: it is the same UUID
// `admin create-project --creator-email=<admin>` grants project_admin to, so
// the run-trigger's mustHaveRelation FGA check also passes for this token.
func MintAdminCLIToken(ctx context.Context, h *FGAHarness, lifetime time.Duration) (string, error) {
	if h == nil || h.Signer == nil {
		return "", errors.New("MintAdminCLIToken: harness with a Signer is required")
	}
	cookie, err := apiLogin(ctx, PipelineAPIBaseURL(), e2eAdminEmail, e2eAdminPassword)
	if err != nil {
		return "", fmt.Errorf("admin login for CLI token: %w", err)
	}
	realUUID, err := apiMeUserID(ctx, PipelineAPIBaseURL(), cookie)
	if err != nil {
		return "", fmt.Errorf("resolve admin real DB UUID: %w", err)
	}
	uid, err := uuid.Parse(realUUID)
	if err != nil {
		return "", fmt.Errorf("parse admin UUID %q: %w", realUUID, err)
	}
	authCtx := auth.WithCtxUser(ctx, &store.User{ID: uid, Email: e2eAdminEmail})
	signed, _, err := tokens.MintCLIAPIToken(authCtx, h.Signer, lifetime)
	if err != nil {
		return "", fmt.Errorf("mint cli-api token: %w", err)
	}
	return signed, nil
}

// K8sQueryTarget builds the K8s-tier QueryTarget (MinIO S3 endpoint via
// NodePort/port-forward + a lakekeeper-vended Resolver) the duckdb assertion
// helpers (AssertSchema / AssertRowCount) need to read a run's output table.
// Mirrors RunScenario's own target assembly (scenario.go) so E3 verifies output
// the same way the migrated scenarios do. The returned target may be empty
// (IsEmpty) when MinIO is unreachable — callers should skip data assertions in
// that case, matching RunScenario's behaviour.
func K8sQueryTarget(t *testing.T, ctx context.Context, h *FGAHarness) (QueryTarget, error) {
	t.Helper()
	target := k8sQueryTarget(t)
	resolver, err := buildK8sVerifier(ctx, h)
	if err != nil {
		return QueryTarget{}, fmt.Errorf("build lakekeeper verifier: %w", err)
	}
	target.Resolver = resolver
	return target, nil
}
