// Package framework — cluster-signer loading + per-test-user token minting.
//
// The harness needs to mint JWTs that pass through pipeline-api's live
// JWKS verifier (and onward to lakekeeper's OIDC validator). Both
// cluster mode and local mode publish a long-lived RS256 keypair;
// loadClusterSigner / LoadSigner pull the private half so the harness
// can mint impersonation + run tokens against it directly.
//
// Cluster-mode fetch shells to kubectl rather than using a real K8s
// client. That keeps the test framework dependency-light (the live e2e
// module already shells to kubectl for PipelineRun apply / status
// polling), but it also means any kubectl auth issues surface as opaque
// "exec failed" errors.
package framework

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// loadClusterSigner shells to `kubectl get secret signing-key` in the
// e2e namespace (default `datuplet-e2e`, override via
// DATUPLET_E2E_NAMESPACE), decodes the embedded PEM, writes it to a
// temp file, and loads it via tokens.LoadPrivateKeyFromPEMFile.
func loadClusterSigner(ctx context.Context, keyID string) (*tokens.Signer, error) {
	ns := os.Getenv("DATUPLET_E2E_NAMESPACE")
	if ns == "" {
		ns = "datuplet-e2e"
	}
	// The platform signing-key Secret is named "signing-key" by the charts
	// (datuplet-infra keygen Job; the prefix helper is intentionally empty —
	// "namespace disambiguates" — and register.sh references
	// SIGNING_KEY_SECRET="signing-key"). Overridable for non-default installs.
	secretName := os.Getenv("DATUPLET_SIGNING_KEY_SECRET")
	if secretName == "" {
		secretName = "signing-key"
	}
	out, err := exec.CommandContext(ctx, "kubectl",
		"-n", ns, "get", "secret", secretName,
		"-o", `jsonpath={.data.signing-key\.pem}`).Output()
	if err != nil {
		return nil, fmt.Errorf(
			"read %s Secret via kubectl (is the platform deployed in the %s namespace? "+
				"set DATUPLET_LOCAL_DIR for local-mode runs): %w", secretName, ns, err)
	}
	b64 := strings.TrimSpace(string(out))
	if b64 == "" {
		return nil, errors.New("pipeline-api-signing-key Secret has empty signing-key.pem field")
	}
	pemBytes, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("decode signing-key PEM (base64): %w", err)
	}
	f, err := os.CreateTemp("", "e2e-signing-*.pem")
	if err != nil {
		return nil, fmt.Errorf("tempfile for signing PEM: %w", err)
	}
	path := f.Name()
	if _, err := f.Write(pemBytes); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("write signing PEM: %w", err)
	}
	f.Close()
	// Remove the tempfile after LoadPrivateKeyFromPEMFile reads it so the
	// signing key doesn't sit in /tmp for the lifetime of the process. The
	// returned *Signer holds the parsed *rsa.PrivateKey in memory; the file
	// is no longer needed once Load returns.
	defer os.Remove(path)
	return tokens.LoadPrivateKeyFromPEMFile(path, keyID)
}

// MintTestUserImpersonation mints a short-lived impersonation JWT for
// one of the hard-coded TestUsers (alice/bob/charlie/dora). The token
// is suitable for HTTP calls into pipeline-api's storage browse
// endpoints AND onward to lakekeeper's iceberg REST surface — both
// validate against the live JWKS.
//
// The actor claim is derived from the per-call ctx via
// tokens.MintImpersonation's subjectFromCtx, so this helper builds a
// fake but valid auth context first via auth.WithCtxUser.
//
// Note the deliberate use of MintImpersonation (60-second TTL) rather
// than MintRunToken: storage browse expects token_kind=impersonation.
// For run tokens, see MintTestUserRunToken below.
func MintTestUserImpersonation(ctx context.Context, signer *tokens.Signer, userID uuid.UUID) (tokens.ImpersonationToken, error) {
	if signer == nil {
		return "", errors.New("MintTestUserImpersonation: signer is required")
	}
	user := &store.User{ID: userID, Email: testUserEmail(userID)}
	authCtx := auth.WithCtxUser(ctx, user)
	return tokens.MintImpersonation(authCtx, signer)
}

// MintTestUserRunToken mints a per-run RS256 JWT: one JWT per (run, audience),
// `sub=<run-uuid>`, `actor=<user-uuid>`, `aud=datuplet-catalog`,
// jti=run-tok-<run-uuid>.
//
// projectID is forwarded as the JWT `project_id` claim — pass
// FGAHarness.LakekeeperProjectID. DG / TableCommit forward it as the
// x-project-id header on every lakekeeper REST call, so the value MUST
// be the lakekeeper Project UUID, not the Datuplet project UUID.
//
// warehouse is forwarded as the JWT `warehouse` claim — pass
// FGAHarness.WarehouseName. The JWT validator requires non-empty.
//
// runID is the run UUID; the harness typically generates a fresh one
// per scenario via uuid.New().
func MintTestUserRunToken(ctx context.Context, signer *tokens.Signer,
	userID uuid.UUID, runID uuid.UUID, projectID, warehouse, pipelineName string,
) (string, error) {
	if signer == nil {
		return "", errors.New("MintTestUserRunToken: signer is required")
	}
	user := &store.User{ID: userID, Email: testUserEmail(userID)}
	authCtx := auth.WithCtxUser(ctx, user)
	return tokens.MintRunToken(authCtx, signer, tokens.RunSpec{
		RunID:        runID.String(),
		ProjectID:    projectID,
		Warehouse:    warehouse,
		PipelineName: pipelineName,
		Lifetime:     RunTokenLifetime,
	})
}

// testUserEmail looks up the email for a UUID in the TestUsers
// matrix, falling back to "<uuid>@datuplet.test". Email is
// informational on the auth context (store.User.Email is never
// validated against the JWT subject) so a missing entry just degrades
// log readability, not test correctness.
func testUserEmail(id uuid.UUID) string {
	for _, u := range TestUsers {
		if u.ID == id {
			return u.Email
		}
	}
	return id.String() + "@datuplet.test"
}
