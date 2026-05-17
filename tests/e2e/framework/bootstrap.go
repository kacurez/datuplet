// Package framework — FGA-aware bootstrap for the e2e harness.
//
// Brings up enough state for every K8s-tier scenario to share a single
// fixture: looks up the OpenFGA store + model that `make local-up` /
// `deploy-local.sh` provisioned, calls lakekeeper's management API to
// allocate (or reuse) a per-test Datuplet project + warehouse, and
// seeds FGA tuples for the TestUsers grant matrix.
//
// The bootstrap is **idempotent** by design: re-running an e2e suite
// against an already-seeded store returns the existing IDs and writes
// no duplicate tuples. The cost of "did this already run?" is one
// /stores list + one /management/v1/project-list.
package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/lakekeeper"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// FGAHarness holds the IDs the bootstrap layer resolved at fixture
// init. Every K8s-tier scenario reads from this struct rather than
// re-resolving — the constants are stable across the whole `go test`
// process.
//
// Zero value is invalid; SetupFGABootstrap returns a fully populated
// pointer or an error.
type FGAHarness struct {
	// OpenFGAStoreID is the ULID of the "datuplet" store in the live
	// OpenFGA instance. Tests that want to write tuples directly use
	// this with NewOpenFGAAuthorizer.
	OpenFGAStoreID string

	// OpenFGAModelID is the ULID of the collaboration-4.4 model
	// (Datuplet's extended version). Required by NewOpenFGAAuthorizer.
	OpenFGAModelID string

	// LakekeeperProjectID is the UUID of the per-e2e-suite lakekeeper
	// Project we created (or reused). Forwarded as `x-project-id` on
	// catalog requests; carried as `project_id` claim on minted tokens.
	LakekeeperProjectID string

	// LakekeeperProjectName is the human-readable name we used.
	LakekeeperProjectName string

	// WarehouseName is the lakekeeper warehouse inside the project.
	WarehouseName string

	// Authorizer is a live OpenFGAAuthorizer wired to OpenFGAStoreID +
	// OpenFGAModelID. Used by SeedFGAGrants and scenarios that need to
	// assert FGA tuples directly.
	Authorizer *authz.OpenFGAAuthorizer

	// LakekeeperManager is a live management-API client scoped to the
	// service token minter (see boot()).
	LakekeeperManager *lakekeeper.Manager

	// Signer is the loaded pipeline-api signing key. Used to mint
	// per-test impersonation/run tokens.
	Signer *tokens.Signer

	// LakekeeperBaseURL is the URL the harness uses for management calls
	// and that scenarios feed into catalogwriter for LoadTable lookups.
	// Includes the `/catalog` suffix when querying the iceberg REST
	// surface; omits it for management calls.
	LakekeeperBaseURL string
}

// FGABootstrapConfig drives SetupFGABootstrap. All fields are
// optional — sensible local-mode defaults are filled in by Defaults()
// when zero.
type FGABootstrapConfig struct {
	// OpenFGAURL is the HTTP base of the OpenFGA API
	// (e.g. http://localhost:8180). Empty → env DATUPLET_OPENFGA_URL or
	// the local default.
	OpenFGAURL string

	// OpenFGAStoreName is the named store to look up (created by
	// `pipeline-api admin authz-bootstrap` at install time).
	OpenFGAStoreName string

	// OpenFGAModelVersion is the collaboration model version label
	// (e.g. "4.4"). The bootstrap reads the version-pin tuple to
	// resolve this to a model ID. Default kept in lockstep with the
	// Helm chart's CONFIGURED_MODEL_VERSION.
	OpenFGAModelVersion string

	// LakekeeperURL is the management base URL
	// (e.g. http://localhost:8181). Empty → env DATUPLET_LAKEKEEPER_URL
	// or the local default. May include the `/catalog` suffix; the
	// Manager strips it for management calls.
	LakekeeperURL string

	// LakekeeperProjectName is the per-suite lakekeeper Project name to
	// create-or-reuse. Empty → "datuplet-e2e".
	LakekeeperProjectName string

	// WarehouseName is the lakekeeper warehouse inside the project.
	// Empty → "datuplet" (matches the production single-warehouse shape).
	WarehouseName string

	// SigningKeyPath is the path to the pipeline-api private key PEM.
	// Empty → discovered from the cluster's pipeline-api-signing-key
	// Secret (cluster mode) or the local-mode dir's
	// signing-key.pem (local mode). See LoadSigner.
	SigningKeyPath string

	// SigningKeyID matches the SIGNING_KEY_ID pipeline-api uses to
	// label the JWK in JWKS. Lakekeeper looks up keys by `kid` so this
	// MUST match what the live JWKS endpoint publishes. Empty →
	// "key-1".
	SigningKeyID string

	// S3 warehouse profile. Used when EnsureWarehouseInProject has to
	// create a fresh warehouse. All four fields are required if the
	// warehouse doesn't exist yet; ignored when EnsureWarehouseInProject
	// finds an existing one.
	S3Bucket    string
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
}

// Defaults returns a config with local-mode defaults filled in for any
// zero-valued field. Mirrors `make local-up` / `deploy-local.sh`
// conventions.
func (c FGABootstrapConfig) Defaults() FGABootstrapConfig {
	out := c
	if out.OpenFGAURL == "" {
		if env := os.Getenv("DATUPLET_OPENFGA_URL"); env != "" {
			out.OpenFGAURL = env
		} else {
			out.OpenFGAURL = "http://localhost:8180"
		}
	}
	if out.OpenFGAStoreName == "" {
		out.OpenFGAStoreName = "datuplet"
	}
	if out.OpenFGAModelVersion == "" {
		// Default "4.4" matches the chart's
		// LAKEKEEPER__OPENFGA__CONFIGURED_MODEL_VERSION pin. Override via
		// the test config if a future bump lands.
		out.OpenFGAModelVersion = "4.4"
	}
	if out.LakekeeperURL == "" {
		if env := os.Getenv("DATUPLET_LAKEKEEPER_URL"); env != "" {
			out.LakekeeperURL = env
		} else {
			out.LakekeeperURL = "http://localhost:8181"
		}
	}
	if out.LakekeeperProjectName == "" {
		out.LakekeeperProjectName = "datuplet-e2e"
	}
	if out.WarehouseName == "" {
		out.WarehouseName = "datuplet"
	}
	if out.SigningKeyID == "" {
		out.SigningKeyID = "key-1"
	}
	if out.S3Bucket == "" {
		out.S3Bucket = "datuplet"
	}
	if out.S3Endpoint == "" {
		// Cluster MinIO endpoint reachable from the lakekeeper Pod.
		// Default matches deploy-local.sh's namespace (datuplet-e2e);
		// override via DATUPLET_E2E_NAMESPACE for non-default installs.
		ns := os.Getenv("DATUPLET_E2E_NAMESPACE")
		if ns == "" {
			ns = "datuplet-e2e"
		}
		out.S3Endpoint = "http://minio." + ns + ".svc.cluster.local:9000"
	}
	if out.S3AccessKey == "" {
		out.S3AccessKey = "minioadmin"
	}
	if out.S3SecretKey == "" {
		out.S3SecretKey = "minioadmin"
	}
	return out
}

// SetupFGABootstrap is the entry point for fixture init.
// Idempotent: re-running on an already-seeded environment returns the
// existing IDs and writes no duplicate tuples.
//
// Soft-degrade contract: every "infrastructure not reachable" failure
// surfaces a clear error mentioning what's missing. Tests skip
// individually (via PreCheck) rather than hard-failing TestMain so a
// developer running the suite against a half-up local stack still gets
// the per-tier-skip messages.
func SetupFGABootstrap(ctx context.Context, cfg FGABootstrapConfig) (*FGAHarness, error) {
	cfg = cfg.Defaults()

	storeID, err := lookupFGAStore(ctx, cfg.OpenFGAURL, cfg.OpenFGAStoreName)
	if err != nil {
		return nil, fmt.Errorf("lookup OpenFGA store %q: %w", cfg.OpenFGAStoreName, err)
	}
	modelID, err := lookupFGAModel(ctx, cfg.OpenFGAURL, storeID, cfg.OpenFGAModelVersion)
	if err != nil {
		return nil, fmt.Errorf("lookup OpenFGA model collaboration-%s: %w", cfg.OpenFGAModelVersion, err)
	}

	authzr, err := authz.NewOpenFGAAuthorizer(cfg.OpenFGAURL, storeID, modelID, os.Getenv("OPENFGA_API_KEY"), 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("new OpenFGAAuthorizer: %w", err)
	}

	signer, err := LoadSigner(ctx, cfg.SigningKeyPath, cfg.SigningKeyID)
	if err != nil {
		return nil, fmt.Errorf("load signer: %w", err)
	}

	mgr, err := lakekeeper.New(cfg.LakekeeperURL, serviceTokenMinter(signer), 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("new lakekeeper manager: %w", err)
	}

	projectID, err := mgr.FindProjectIDByName(ctx, cfg.LakekeeperProjectName)
	if err != nil {
		return nil, fmt.Errorf("find lakekeeper project: %w", err)
	}
	if projectID == "" {
		projectID, err = mgr.CreateProject(ctx, cfg.LakekeeperProjectName)
		if err != nil {
			return nil, fmt.Errorf("create lakekeeper project: %w", err)
		}
	}

	if err := mgr.EnsureWarehouseInProject(ctx, projectID, cfg.WarehouseName, lakekeeper.S3WarehouseProfile{
		Bucket:    cfg.S3Bucket,
		Endpoint:  cfg.S3Endpoint,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
	}); err != nil {
		return nil, fmt.Errorf("ensure warehouse %q in project %q: %w", cfg.WarehouseName, projectID, err)
	}

	h := &FGAHarness{
		OpenFGAStoreID:        storeID,
		OpenFGAModelID:        modelID,
		LakekeeperProjectID:   projectID,
		LakekeeperProjectName: cfg.LakekeeperProjectName,
		WarehouseName:         cfg.WarehouseName,
		Authorizer:            authzr,
		LakekeeperManager:     mgr,
		Signer:                signer,
		LakekeeperBaseURL:     cfg.LakekeeperURL,
	}

	if err := SeedFGAGrants(ctx, h); err != nil {
		return nil, fmt.Errorf("seed FGA grants: %w", err)
	}
	return h, nil
}

// SeedFGAGrants writes one project-relation tuple per TestUser whose
// Relation is non-empty. Idempotent: tuples that already exist
// (re-running the suite) are silently skipped via the
// already-exists-error pattern from cmd/pipeline-api/admin_authz.go.
func SeedFGAGrants(ctx context.Context, h *FGAHarness) error {
	if h == nil || h.Authorizer == nil {
		return errors.New("SeedFGAGrants: harness not initialised")
	}
	projectObj := authz.ProjectObject(h.LakekeeperProjectID)

	var tuples []authz.Tuple
	for _, u := range TestUsers {
		if u.Relation == "" {
			continue
		}
		tuples = append(tuples, authz.Tuple{
			User:     authz.UserObject(u.ID.String()).String(),
			Relation: u.Relation,
			Object:   projectObj,
		})
	}
	if len(tuples) == 0 {
		return nil
	}

	// OpenFGA's Write rejects the whole batch on any duplicate. Try the
	// batch first; on any error, fall through to single-tuple writes
	// that swallow already-exists. This matches the bootstrap pattern
	// in cmd/pipeline-api/admin_authz.go::fgaWriteModelVersionTuples.
	if err := h.Authorizer.WriteTuples(ctx, tuples); err == nil {
		return nil
	}
	for _, t := range tuples {
		one := []authz.Tuple{t}
		if err := h.Authorizer.WriteTuples(ctx, one); err != nil {
			if isAlreadyExistsErr(err) {
				continue
			}
			return fmt.Errorf("write tuple %s %s %s: %w",
				t.User, t.Relation, t.Object, err)
		}
	}
	return nil
}

// serviceTokenMinter returns a TokenMinter that mints fresh service
// tokens for every lakekeeper management call. 5-minute lifetime
// matches the production bootstrap pattern.
//
// Subject "admin" matches the --admin-user default of `pipeline-api
// admin authz-bootstrap` (cmd/pipeline-api/admin_authz.go). That
// command writes an FGA tuple `user:oidc~admin admin server:<id>`,
// granting the JWT subject lakekeeper-server admin rights — including
// can_create_project, which SetupFGABootstrap needs when the named
// project doesn't yet exist. Using a distinct subject like
// "e2e-bootstrap" would require a separate FGA grant per cluster
// bring-up; reusing the existing admin tuple keeps the harness in
// lockstep with the production bootstrap shape.
func serviceTokenMinter(signer *tokens.Signer) lakekeeper.TokenMinter {
	return func() (string, error) {
		return tokens.MintServiceToken(signer, tokens.ServiceTokenSpec{
			Subject:  "admin",
			Lifetime: 5 * time.Minute,
		})
	}
}

// LoadSigner returns a tokens.Signer suitable for minting per-test
// JWTs against pipeline-api's live JWKS. Resolution order:
//
//  1. explicit path: cfg.SigningKeyPath set → load that PEM directly.
//  2. local-mode dir: env DATUPLET_LOCAL_DIR set → look for
//     <dir>/signing-key.pem (the path runServeLocal writes).
//  3. cluster mode: kubectl-fetch the
//     `pipeline-api-signing-key` Secret from the `datuplet`
//     namespace, write to a tempfile, and load.
//
// The returned signer matches whatever pipeline-api is currently
// publishing on /api/v1/auth/jwks.json; lakekeeper validates against
// that JWKS so signature mismatch surfaces as a 401 from lakekeeper,
// not a silent test pass.
//
// Cluster-mode fetch happens via shell-out to kubectl so the framework
// doesn't need a real K8s client. If kubectl isn't reachable, the
// error includes a concrete next step.
func LoadSigner(ctx context.Context, path, keyID string) (*tokens.Signer, error) {
	if keyID == "" {
		keyID = "key-1"
	}
	if path != "" {
		return tokens.LoadPrivateKeyFromPEMFile(path, keyID)
	}
	if dir := os.Getenv("DATUPLET_LOCAL_DIR"); dir != "" {
		local := dir + "/signing-key.pem"
		if _, err := os.Stat(local); err == nil {
			return tokens.LoadPrivateKeyFromPEMFile(local, keyID)
		}
	}
	return loadClusterSigner(ctx, keyID)
}

// lookupFGAStore returns the ULID of the named store, or an error if
// it doesn't exist. The store must already exist (created by the
// install-time `pipeline-api admin authz-bootstrap` job).
func lookupFGAStore(ctx context.Context, baseURL, name string) (string, error) {
	c := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(baseURL, "/")+"/stores", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", fmt.Errorf("GET /stores: %w (is OpenFGA reachable at %s?)", err, baseURL)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("list stores HTTP %d: %s", resp.StatusCode, string(b))
	}
	var list struct {
		Stores []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"stores"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return "", fmt.Errorf("decode stores: %w", err)
	}
	for _, s := range list.Stores {
		if s.Name == name {
			return s.ID, nil
		}
	}
	// TODO: pagination — /stores response includes a continuation_token
	// when the deployment has more stores than the default page size
	// (~25); revisit if the test environment grows past a single store.
	return "", fmt.Errorf("store %q not found in OpenFGA — run `pipeline-api admin authz-bootstrap` first", name)
}

// lookupFGAModel reads the model_version:collaboration-<version> /
// openfga_id tuple to resolve the active model ID. Mirrors
// fgaFindVersionPinnedModelID in cmd/pipeline-api/admin_authz.go.
func lookupFGAModel(ctx context.Context, baseURL, storeID, version string) (string, error) {
	c := &http.Client{Timeout: 10 * time.Second}
	url := strings.TrimRight(baseURL, "/") + "/stores/" + storeID + "/read"
	body, _ := json.Marshal(map[string]any{
		"tuple_key": map[string]string{
			"object":   "model_version:collaboration-" + version,
			"relation": "openfga_id",
		},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST /read: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("read tuples HTTP %d: %s", resp.StatusCode, string(b))
	}
	var result struct {
		Tuples []struct {
			Key struct {
				User string `json:"user"`
			} `json:"key"`
		} `json:"tuples"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode read tuples: %w", err)
	}
	for _, t := range result.Tuples {
		if id, ok := strings.CutPrefix(t.Key.User, "auth_model_id:"); ok {
			return id, nil
		}
	}
	return "", fmt.Errorf("no model pinned for version %q — run `pipeline-api admin authz-bootstrap` first", version)
}

// isAlreadyExistsErr returns true when the OpenFGA write came back
// with the canonical "tuple already exists" shape. Mirrors
// cmd/pipeline-api/admin_authz.go::isTupleAlreadyExistsError.
func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "cannot write a tuple") ||
		strings.Contains(s, "already exists") ||
		strings.Contains(s, "write_failed_due_to_invalid_input") ||
		strings.Contains(s, "invalid_argument")
}
