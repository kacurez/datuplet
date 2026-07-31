package apps_test

// identity_manager_test.go covers the CONCRETE IdentityManager (RFC 028 P4):
// the FGA `viewer` tuple write/delete and the per-render app-token mint.
// The two-subject rule is the thing under test — every assertion here exists
// so that a refactor cannot silently swap the JWT `sub` for the FGA user
// string (or vice versa), which would leave authorization quietly broken
// rather than loudly failing:
//
//	AppJWTSubject(appUUID) = "app-<uuid>"            → the JWT `sub` claim
//	AppFGASubject(appUUID) = "user:oidc~app-<uuid>"  → the FGA tuple's user
//
// and lakekeeper's own normalisation (`user:oidc~<sub>`) must land back on
// AppFGASubject — asserted in TestIdentityManager_MintSubjectNormalisesToTupleUser.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/apps"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// recorderAuthz is an ordering-recording authz.Authorizer. It keeps the real
// tuple set (so Check answers truthfully after a Register) AND an ordered log
// of every write/delete/check, which is what lets the tests assert the exact
// tuple shape rather than "some tuple was written".
//
// writeErr/deleteErr inject backend failures — in particular the OpenFGA
// "already exists" / "cannot delete a tuple which does not exist" wordings the
// idempotency swallows key off.
type recorderAuthz struct {
	mu        sync.Mutex
	tuples    map[string]struct{}
	ops       []string
	writeErr  error
	deleteErr error
}

func newRecorderAuthz() *recorderAuthz {
	return &recorderAuthz{tuples: make(map[string]struct{})}
}

func tupleKey(t authz.Tuple) string {
	return t.User + "|" + t.Relation + "|" + t.Object.String()
}

func (r *recorderAuthz) Check(_ context.Context, user, relation string, obj authz.Object) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := user + "|" + relation + "|" + obj.String()
	r.ops = append(r.ops, "Check:"+key)
	_, ok := r.tuples[key]
	return ok, nil
}

func (r *recorderAuthz) BatchCheck(context.Context, []authz.CheckQuery) ([]bool, []error) {
	panic("unused")
}

func (r *recorderAuthz) ListObjects(context.Context, string, string, authz.ObjectType) ([]authz.Object, error) {
	panic("unused")
}

func (r *recorderAuthz) WriteTuples(_ context.Context, ts []authz.Tuple) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range ts {
		r.ops = append(r.ops, "Write:"+tupleKey(t))
	}
	if r.writeErr != nil {
		return r.writeErr
	}
	for _, t := range ts {
		r.tuples[tupleKey(t)] = struct{}{}
	}
	return nil
}

func (r *recorderAuthz) DeleteTuples(_ context.Context, ts []authz.Tuple) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range ts {
		r.ops = append(r.ops, "Delete:"+tupleKey(t))
	}
	if r.deleteErr != nil {
		return r.deleteErr
	}
	for _, t := range ts {
		delete(r.tuples, tupleKey(t))
	}
	return nil
}

func (r *recorderAuthz) opsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

var _ authz.Authorizer = (*recorderAuthz)(nil)

// identitySigner builds a throwaway RS256 signer, mirroring
// pkg/pipelineapi/tokens/mint_test.go's sharedSigner (there is no shared JWKS
// test helper in this repo — see task-P0-report.md §E).
func identitySigner(t *testing.T) *tokens.Signer {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	dir := t.TempDir()
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	path := filepath.Join(dir, "priv.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o400); err != nil {
		t.Fatalf("write key: %v", err)
	}
	signer, err := tokens.LoadPrivateKeyFromPEMFile(path, "key-p4")
	if err != nil {
		t.Fatalf("load key: %v", err)
	}
	return signer
}

// captureAudit swaps the default slog logger for a JSON handler writing into a
// buffer, and returns the decoded records plus the raw text (the raw text is
// what the "no token in any log line" assertion greps).
type auditCapture struct {
	buf *strings.Builder
}

func captureAudit(t *testing.T) *auditCapture {
	t.Helper()
	c := &auditCapture{buf: &strings.Builder{}}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(c.buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

func (c *auditCapture) raw() string { return c.buf.String() }

// records decodes every captured line into a map.
func (c *auditCapture) records(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(c.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// findAction returns the single captured record whose "action" equals want.
func (c *auditCapture) findAction(t *testing.T, want string) map[string]any {
	t.Helper()
	var hits []map[string]any
	for _, rec := range c.records(t) {
		if a, _ := rec["action"].(string); a == want {
			hits = append(hits, rec)
		}
	}
	if len(hits) != 1 {
		t.Fatalf("captured %d audit records with action=%q, want exactly 1; log:\n%s", len(hits), want, c.raw())
	}
	return hits[0]
}

// identityHarness is the concrete manager plus its recording deps.
type identityHarness struct {
	mgr      apps.IdentityManager
	authz    *recorderAuthz
	projects *fakeProjects
	signer   *tokens.Signer
}

const testLakekeeperPID = "lk-project-p4"

func newIdentityHarness(t *testing.T) *identityHarness {
	t.Helper()
	h := &identityHarness{
		authz:    newRecorderAuthz(),
		projects: &fakeProjects{lakekeeperID: testLakekeeperPID},
		signer:   identitySigner(t),
	}
	h.mgr = apps.NewIdentityManager(h.authz, h.signer, h.projects)
	return h
}

// ---------------------------------------------------------------------------
// Register
// ---------------------------------------------------------------------------

// TestIdentityManager_RegisterWritesViewerTuple pins the whole tuple, not just
// its user: `viewer` on project:<LAKEKEEPER project id> (not the Datuplet one)
// for AppFGASubject(appID).
func TestIdentityManager_RegisterWritesViewerTuple(t *testing.T) {
	h := newIdentityHarness(t)
	appID := uuid.NewString()
	projectID := uuid.NewString()

	if err := h.mgr.Register(context.Background(), appID, projectID); err != nil {
		t.Fatalf("Register: %v", err)
	}

	want := "Write:" + apps.AppFGASubject(appID) + "|" + authz.RelationViewer +
		"|" + authz.ProjectObject(testLakekeeperPID).String()
	ops := h.authz.opsSnapshot()
	if len(ops) != 1 || ops[0] != want {
		t.Fatalf("FGA ops = %v, want exactly [%s]", ops, want)
	}
	// The tuple user must be the SINGLE-prefixed form. A doubled prefix is the
	// exact silent-authorization-failure this test exists to catch.
	if strings.Count(ops[0], "user:oidc~") != 1 {
		t.Errorf("tuple user is not singly prefixed: %q", ops[0])
	}
	if strings.Contains(ops[0], "user:oidc~user:") {
		t.Errorf("double-prefixed FGA subject: %q", ops[0])
	}
	// And it must NOT be the raw JWT subject (the other half of the mix-up).
	if strings.Contains(ops[0], "Write:"+apps.AppJWTSubject(appID)+"|") {
		t.Errorf("tuple was written for the JWT subject, not the FGA subject: %q", ops[0])
	}
}

// TestIdentityManager_RegisterEmitsAudit asserts the app_identity_created
// audit line and its fields.
func TestIdentityManager_RegisterEmitsAudit(t *testing.T) {
	h := newIdentityHarness(t)
	cap := captureAudit(t)
	appID := uuid.NewString()
	projectID := uuid.NewString()

	if err := h.mgr.Register(context.Background(), appID, projectID); err != nil {
		t.Fatalf("Register: %v", err)
	}
	rec := cap.findAction(t, "app_identity_created")
	for k, want := range map[string]string{
		"app_id":                appID,
		"project_id":            projectID,
		"lakekeeper_project_id": testLakekeeperPID,
		"fga_subject":           apps.AppFGASubject(appID),
		"relation":              authz.RelationViewer,
	} {
		if got, _ := rec[k].(string); got != want {
			t.Errorf("audit %s = %q, want %q", k, got, want)
		}
	}
}

// TestIdentityManager_RegisterIsIdempotent: OpenFGA errors on a strict
// duplicate tuple; a retried create (or a second upload after a half-failed
// registration) must not hard-fail.
func TestIdentityManager_RegisterIsIdempotent(t *testing.T) {
	h := newIdentityHarness(t)
	h.authz.writeErr = errors.New("rpc error: cannot write a tuple which already exists: user: 'user:oidc~app-x'")
	if err := h.mgr.Register(context.Background(), uuid.NewString(), uuid.NewString()); err != nil {
		t.Fatalf("Register on an already-existing tuple must succeed, got %v", err)
	}
}

func TestIdentityManager_RegisterPropagatesBackendError(t *testing.T) {
	h := newIdentityHarness(t)
	h.authz.writeErr = errors.New("openfga: connection refused")
	err := h.mgr.Register(context.Background(), uuid.NewString(), uuid.NewString())
	if err == nil {
		t.Fatal("Register must propagate a real FGA failure (the caller answers 503)")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error does not wrap the backend cause: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Unregister
// ---------------------------------------------------------------------------

func TestIdentityManager_UnregisterDeletesViewerTuple(t *testing.T) {
	h := newIdentityHarness(t)
	appID := uuid.NewString()
	projectID := uuid.NewString()
	ctx := context.Background()

	if err := h.mgr.Register(ctx, appID, projectID); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := h.mgr.Unregister(ctx, appID, projectID); err != nil {
		t.Fatalf("Unregister: %v", err)
	}

	ops := h.authz.opsSnapshot()
	wantDelete := "Delete:" + apps.AppFGASubject(appID) + "|" + authz.RelationViewer +
		"|" + authz.ProjectObject(testLakekeeperPID).String()
	if len(ops) != 2 || ops[1] != wantDelete {
		t.Fatalf("FGA ops = %v, want the write then exactly [%s]", ops, wantDelete)
	}
	// The tuple really is gone: a Check for the app's FGA subject now denies.
	allowed, err := h.authz.Check(ctx, apps.AppFGASubject(appID), authz.RelationViewer,
		authz.ProjectObject(testLakekeeperPID))
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if allowed {
		t.Error("the app identity still has `viewer` after Unregister")
	}
}

func TestIdentityManager_UnregisterEmitsAudit(t *testing.T) {
	h := newIdentityHarness(t)
	appID := uuid.NewString()
	projectID := uuid.NewString()
	if err := h.mgr.Register(context.Background(), appID, projectID); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cap := captureAudit(t)
	if err := h.mgr.Unregister(context.Background(), appID, projectID); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	rec := cap.findAction(t, "app_identity_deleted")
	if got, _ := rec["app_id"].(string); got != appID {
		t.Errorf("audit app_id = %q, want %q", got, appID)
	}
	if got, _ := rec["fga_subject"].(string); got != apps.AppFGASubject(appID) {
		t.Errorf("audit fga_subject = %q, want %q", got, apps.AppFGASubject(appID))
	}
}

// TestIdentityManager_UnregisterToleratesMissingTuple: deleting an app whose
// tuple is already gone (a retried delete) must succeed, or the app rows can
// never be removed.
func TestIdentityManager_UnregisterToleratesMissingTuple(t *testing.T) {
	h := newIdentityHarness(t)
	h.authz.deleteErr = errors.New("rpc error: cannot delete a tuple which does not exist: user: 'user:oidc~app-x'")
	if err := h.mgr.Unregister(context.Background(), uuid.NewString(), uuid.NewString()); err != nil {
		t.Fatalf("Unregister on a missing tuple must succeed, got %v", err)
	}
}

func TestIdentityManager_UnregisterPropagatesBackendError(t *testing.T) {
	h := newIdentityHarness(t)
	h.authz.deleteErr = errors.New("openfga: connection refused")
	if err := h.mgr.Unregister(context.Background(), uuid.NewString(), uuid.NewString()); err == nil {
		t.Fatal("Unregister must propagate a real FGA failure — the delete route MUST keep the rows")
	}
}

// ---------------------------------------------------------------------------
// Mint
// ---------------------------------------------------------------------------

// TestIdentityManager_MintSubjectIsAppJWTSubject decodes the minted JWT and
// pins `sub`, the audience, the kind and the 60 s TTL.
func TestIdentityManager_MintSubjectIsAppJWTSubject(t *testing.T) {
	h := newIdentityHarness(t)
	appID := uuid.NewString()

	tok, err := h.mgr.Mint(context.Background(), appID, uuid.NewString())
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	claims := decodeClaims(t, tok)

	if got, _ := claims["sub"].(string); got != apps.AppJWTSubject(appID) {
		t.Errorf("sub = %q, want AppJWTSubject %q", got, apps.AppJWTSubject(appID))
	}
	if got, _ := claims["sub"].(string); strings.Contains(got, "user:") || strings.Contains(got, "oidc~") {
		t.Errorf("sub %q must be the BARE form — lakekeeper composes the prefixes", got)
	}
	if got, _ := claims["aud"].(string); got != tokens.TableTokenAudience {
		t.Errorf("aud = %q, want %q", got, tokens.TableTokenAudience)
	}
	if got, _ := claims["token_kind"].(string); got != tokens.TokenKindApp {
		t.Errorf("token_kind = %q, want %q", got, tokens.TokenKindApp)
	}
	if got, _ := claims["project_id"].(string); got != testLakekeeperPID {
		t.Errorf("project_id = %q, want the lakekeeper project id %q", got, testLakekeeperPID)
	}
	iat, _ := claims["iat"].(float64)
	exp, _ := claims["exp"].(float64)
	if int64(exp-iat) != 60 {
		t.Errorf("TTL = %ds, want 60s (spec §5.4)", int64(exp-iat))
	}
}

// TestIdentityManager_MintSubjectNormalisesToTupleUser is the load-bearing
// both-halves assertion: run the minted JWT `sub` through the same
// normalisation lakekeeper applies (`user:oidc~<sub>`) and it must land
// EXACTLY on the FGA user string Register wrote the `viewer` tuple for.
// If either helper is misused, this fails.
func TestIdentityManager_MintSubjectNormalisesToTupleUser(t *testing.T) {
	h := newIdentityHarness(t)
	appID := uuid.NewString()
	projectID := uuid.NewString()
	ctx := context.Background()

	if err := h.mgr.Register(ctx, appID, projectID); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tok, err := h.mgr.Mint(ctx, appID, projectID)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	sub, _ := decodeClaims(t, tok)["sub"].(string)

	// lakekeeper's normalisation of a JWT subject into an FGA user object.
	normalised := authz.UserObject(sub).String()
	if normalised != apps.AppFGASubject(appID) {
		t.Fatalf("normalised JWT sub = %q, want the tuple's user %q", normalised, apps.AppFGASubject(appID))
	}
	if strings.Count(normalised, "oidc~") != 1 {
		t.Errorf("normalised subject is double-prefixed: %q", normalised)
	}
	// The authorization check that results must be allowed by the tuple
	// Register wrote — i.e. the mint and the tuple agree end to end.
	allowed, err := h.authz.Check(ctx, normalised, authz.RelationViewer,
		authz.ProjectObject(testLakekeeperPID))
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !allowed {
		t.Errorf("FGA check for the minted subject %q was DENIED — the mint and the tuple disagree", normalised)
	}
}

// TestIdentityManager_MintIsFreshPerCall: no caching anywhere (spec §5.4).
func TestIdentityManager_MintIsFreshPerCall(t *testing.T) {
	h := newIdentityHarness(t)
	appID := uuid.NewString()
	projectID := uuid.NewString()

	tok1, err := h.mgr.Mint(context.Background(), appID, projectID)
	if err != nil {
		t.Fatalf("mint 1: %v", err)
	}
	tok2, err := h.mgr.Mint(context.Background(), appID, projectID)
	if err != nil {
		t.Fatalf("mint 2: %v", err)
	}
	if tok1 == tok2 {
		t.Fatal("two mints returned identical tokens — a cached credential (spec §5.4 violation)")
	}
	jti1, _ := decodeClaims(t, tok1)["jti"].(string)
	jti2, _ := decodeClaims(t, tok2)["jti"].(string)
	if jti1 == "" || jti1 == jti2 {
		t.Errorf("jti reused across renders: %q vs %q", jti1, jti2)
	}
}

// TestIdentityManager_MintEmitsAudit asserts impersonation_minted{app_id, jti}
// — and that the record's jti is the minted token's real jti.
func TestIdentityManager_MintEmitsAudit(t *testing.T) {
	h := newIdentityHarness(t)
	cap := captureAudit(t)
	appID := uuid.NewString()

	tok, err := h.mgr.Mint(context.Background(), appID, uuid.NewString())
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	rec := cap.findAction(t, "impersonation_minted")
	if got, _ := rec["app_id"].(string); got != appID {
		t.Errorf("audit app_id = %q, want %q", got, appID)
	}
	wantJTI, _ := decodeClaims(t, tok)["jti"].(string)
	if got, _ := rec["jti"].(string); got == "" || got != wantJTI {
		t.Errorf("audit jti = %q, want the token's jti %q", got, wantJTI)
	}
}

// TestIdentityManager_MintNeverLogsTheToken is the security assertion: the
// audit trail carries app_id + jti and NOTHING that reconstructs the
// credential. Greps the whole captured log for the token, each of its three
// JWT segments, and the JWT header marker.
func TestIdentityManager_MintNeverLogsTheToken(t *testing.T) {
	h := newIdentityHarness(t)
	cap := captureAudit(t)
	appID := uuid.NewString()
	projectID := uuid.NewString()
	ctx := context.Background()

	if err := h.mgr.Register(ctx, appID, projectID); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tok, err := h.mgr.Mint(ctx, appID, projectID)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if err := h.mgr.Unregister(ctx, appID, projectID); err != nil {
		t.Fatalf("Unregister: %v", err)
	}

	logged := cap.raw()
	if logged == "" {
		t.Fatal("no audit output captured — the capture harness is broken, so this test proves nothing")
	}
	if strings.Contains(logged, tok) {
		t.Error("the minted token appears verbatim in a log line")
	}
	for i, seg := range strings.Split(tok, ".") {
		if seg == "" {
			continue
		}
		if strings.Contains(logged, seg) {
			t.Errorf("JWT segment %d appears in a log line", i)
		}
	}
	if strings.Contains(logged, "eyJ") {
		t.Error("a base64 JWT header marker (\"eyJ\") appears in a log line")
	}
}

// TestIdentityManager_MintFailsClosedOnUnknownProject: an app whose project
// row is gone, or whose lakekeeper project is not provisioned yet, must NOT
// receive a credential — the FGA tuple it would authorize against cannot
// exist. Also covers the unknown/empty app case.
func TestIdentityManager_MintFailsClosedOnUnknownProject(t *testing.T) {
	appID := uuid.NewString()
	projectID := uuid.NewString()

	t.Run("project not found", func(t *testing.T) {
		h := newIdentityHarness(t)
		h.projects.err = apps.ErrNotFound
		if _, err := h.mgr.Mint(context.Background(), appID, projectID); err == nil {
			t.Fatal("Mint must fail for an unknown project")
		}
	})
	t.Run("lakekeeper project unprovisioned", func(t *testing.T) {
		h := newIdentityHarness(t)
		h.projects.lakekeeperID = ""
		if _, err := h.mgr.Mint(context.Background(), appID, projectID); err == nil {
			t.Fatal("Mint must fail closed when the lakekeeper project id is empty")
		}
	})
	t.Run("register on unprovisioned project", func(t *testing.T) {
		h := newIdentityHarness(t)
		h.projects.lakekeeperID = ""
		if err := h.mgr.Register(context.Background(), appID, projectID); err == nil {
			t.Fatal("Register must fail closed rather than write a tuple on project:<empty>")
		}
		if ops := h.authz.opsSnapshot(); len(ops) != 0 {
			t.Errorf("wrote FGA ops %v despite an unresolvable project", ops)
		}
	})
}

// TestIdentityManager_RejectsEmptyArguments guards the "unknown app" input
// shape at the manager boundary (the HTTP routes 404 an unknown app id before
// they ever get here — see internal.go's mustGetApp).
func TestIdentityManager_RejectsEmptyArguments(t *testing.T) {
	h := newIdentityHarness(t)
	ctx := context.Background()
	if err := h.mgr.Register(ctx, "", uuid.NewString()); err == nil {
		t.Error("Register with an empty appID must fail")
	}
	if err := h.mgr.Unregister(ctx, "", uuid.NewString()); err == nil {
		t.Error("Unregister with an empty appID must fail")
	}
	if _, err := h.mgr.Mint(ctx, "", uuid.NewString()); err == nil {
		t.Error("Mint with an empty appID must fail")
	}
	if _, err := h.mgr.Mint(ctx, uuid.NewString(), "not-a-uuid"); err == nil {
		t.Error("Mint with a malformed projectID must fail")
	}
	if ops := h.authz.opsSnapshot(); len(ops) != 0 {
		t.Errorf("rejected calls still touched FGA: %v", ops)
	}
}

// TestIdentityManager_MintRequiresSigner: a deployment without a signing key
// must fail the mint rather than return an empty credential.
func TestIdentityManager_MintRequiresSigner(t *testing.T) {
	ra := newRecorderAuthz()
	mgr := apps.NewIdentityManager(ra, nil, &fakeProjects{lakekeeperID: testLakekeeperPID})
	if _, err := mgr.Mint(context.Background(), uuid.NewString(), uuid.NewString()); err == nil {
		t.Fatal("Mint without a signer must fail")
	}
}

func decodeClaims(t *testing.T, token string) jwt.MapClaims {
	t.Helper()
	parsed, _, err := jwt.NewParser().ParseUnverified(token, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("parse minted token: %v", err)
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		t.Fatalf("unexpected claims type %T", parsed.Claims)
	}
	return claims
}
