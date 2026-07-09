package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// defaultLakekeeperProjectUUID is Lakekeeper's built-in default project ID.
// Warehouses created against this project don't need an explicit project-scoped
// FGA grant for the bootstrap service identity (the server-admin tuple is
// sufficient). Non-default projects (allocated by `pipeline-api admin
// create-project`) DO need the service identity to hold `project_admin` on the
// project before lakekeeper's warehouse-create endpoint will accept the POST.
const defaultLakekeeperProjectUUID = "00000000-0000-0000-0000-000000000000"

// bootstrapServiceSubject is the synthetic identity used by
// `pipeline-api admin lakekeeper-bootstrap`: it appears both as the `sub`
// claim on the short-lived bootstrap JWT minted via MintServiceToken AND
// as the FGA user-subject on the project_admin tuple written for
// non-default lakekeeper projects. Both sites MUST use this same value —
// if they drift, the tuple grants project_admin to one identity while
// the JWT presents another, and lakekeeper's warehouse-create silently
// 403s.
const bootstrapServiceSubject = "pipeline-api-bootstrap"

// warehouseSpec describes the inputs to buildWarehouseBody. The Type
// field selects which sibling spec (S3 or GCS) is consulted.
//
// LakekeeperProjectID is the project the warehouse will be created in.
// Lakekeeper's default project is 00000000-0000-0000-0000-000000000000;
// use a different ID to target a project provisioned by
// `pipeline-api admin create-project`. Datuplet's projects table stores
// the lakekeeper_project_id allocated for each Datuplet project — pass
// that value here so the warehouse lives where the Datuplet project
// expects to find it (pipeline-api looks warehouses up by the calling
// project's lakekeeper_project_id; mismatched IDs surface as
// "no warehouses registered for project ..." at run-trigger time).
type warehouseSpec struct {
	WarehouseName       string
	LakekeeperProjectID string
	Type                string // "s3" | "gcs"
	S3                  *s3Spec
	GCS                 *gcsSpec
}

// s3Spec captures the S3-flavoured warehouse parameters that the chart
// (D4) passes via flags.
type s3Spec struct {
	Bucket          string
	Region          string
	Endpoint        string
	PathStyleAccess bool
	StsEnabled      bool
	AccessKey       string
	SecretKey       string
}

// gcsSpec captures the GCS-flavoured warehouse parameters. The
// ServiceAccountKeyJSON field carries the contents of a Google IAM
// service-account key (the bytes mounted at --gcs-sa-key-file).
// CredentialType selects the authentication mode:
//   - "" or "system-identity": Workload Identity Federation (no key file).
//   - "service-account-key": static SA JSON key (requires ServiceAccountKeyJSON).
type gcsSpec struct {
	Bucket                string
	KeyPrefix             string
	StsEnabled            bool
	ServiceAccountKeyJSON string
	CredentialType        string // "service-account-key" | "system-identity"
}

// Validate checks that the gcsSpec's CredentialType and ServiceAccountKeyJSON
// are self-consistent. Returns an error for mutual-exclusion violations or
// unknown credential types.
func (s *gcsSpec) Validate() error {
	if s.Bucket == "" {
		return fmt.Errorf("--gcs-bucket required")
	}
	switch s.CredentialType {
	case "", "system-identity":
		if s.ServiceAccountKeyJSON != "" {
			return fmt.Errorf("--gcs-credential-type=system-identity cannot be combined with --gcs-sa-key-file/GCS_SA_KEY_FILE")
		}
		if !s.StsEnabled {
			return fmt.Errorf("--gcs-credential-type=system-identity requires --sts-enabled=true (Lakekeeper has no static credentials to return without STS downscoping)")
		}
	case "service-account-key":
		if s.ServiceAccountKeyJSON == "" {
			return fmt.Errorf("--gcs-credential-type=service-account-key requires --gcs-sa-key-file (or GCS_SA_KEY_FILE)")
		}
	default:
		return fmt.Errorf("unknown --gcs-credential-type %q (want system-identity or service-account-key)", s.CredentialType)
	}
	return nil
}

// buildWarehouseBody assembles the request body for
// `POST /management/v1/warehouse`. Returns an error for unknown types,
// missing per-type sub-spec, or malformed GCS service-account JSON.
//
// Security note: when the GCS service-account JSON is malformed we
// surface a generic "invalid GCS service account key JSON" error;
// the JSON content (which may include the private key) is NEVER echoed
// into the error message or any log line.
func buildWarehouseBody(spec warehouseSpec) (map[string]any, error) {
	switch spec.Type {
	case "s3":
		if spec.S3 == nil {
			return nil, fmt.Errorf("type=s3 requires non-nil S3 spec")
		}
		return map[string]any{
			"warehouse-name": spec.WarehouseName,
			"project-id":     spec.LakekeeperProjectID,
			"storage-profile": map[string]any{
				"type":              "s3",
				"bucket":            spec.S3.Bucket,
				"region":            spec.S3.Region,
				"sts-enabled":       spec.S3.StsEnabled,
				"flavor":            "s3-compat",
				"endpoint":          spec.S3.Endpoint,
				"path-style-access": spec.S3.PathStyleAccess,
			},
			"storage-credential": map[string]any{
				"type":                  "s3",
				"credential-type":       "access-key",
				"aws-access-key-id":     spec.S3.AccessKey,
				"aws-secret-access-key": spec.S3.SecretKey,
			},
			"delete-profile": map[string]any{"type": "hard"},
		}, nil
	case "gcs":
		if spec.GCS == nil {
			return nil, fmt.Errorf("type=gcs requires non-nil GCS spec")
		}
		if err := spec.GCS.Validate(); err != nil {
			return nil, err
		}
		profile := map[string]any{
			"type":        "gcs",
			"bucket":      spec.GCS.Bucket,
			"sts-enabled": spec.GCS.StsEnabled,
		}
		if spec.GCS.KeyPrefix != "" {
			profile["key-prefix"] = spec.GCS.KeyPrefix
		}
		var storageCred map[string]any
		switch spec.GCS.CredentialType {
		case "", "system-identity":
			storageCred = map[string]any{
				"type":            "gcs",
				"credential-type": "gcp-system-identity",
			}
		case "service-account-key":
			var keyObj map[string]any
			if err := json.Unmarshal([]byte(spec.GCS.ServiceAccountKeyJSON), &keyObj); err != nil {
				// Do NOT echo the JSON content (could leak the private key
				// into logs); surface only a generic invalidity message.
				// The underlying json.Unmarshal error is intentionally
				// dropped — its message can quote fragments of the input.
				return nil, errors.New("invalid GCS service account key JSON")
			}
			storageCred = map[string]any{
				"type":            "gcs",
				"credential-type": "service-account-key",
				"key":             keyObj,
			}
		}
		return map[string]any{
			"warehouse-name":     spec.WarehouseName,
			"project-id":         spec.LakekeeperProjectID,
			"storage-profile":    profile,
			"storage-credential": storageCred,
			"delete-profile":     map[string]any{"type": "hard"},
		}, nil
	default:
		return nil, fmt.Errorf("unknown warehouse type %q (want s3 or gcs)", spec.Type)
	}
}

// adminLakekeeperBootstrap drives the one-time `POST /management/v1/bootstrap`
// + warehouse-creation flow against a lakekeeper instance. Every management
// endpoint requires a valid JWT, so this subcommand:
//
//   1. Loads pipeline-api's signing key (same one that signs run-tokens).
//   2. Mints a short-lived service JWT. Lakekeeper's `allowall` authz means
//      signature + audience + expiry are the only checks that matter.
//   3. Calls /management/v1/info to check `bootstrapped`. Skips if true.
//   4. Otherwise POSTs /management/v1/bootstrap.
//   5. Calls /management/v1/warehouse to check the named warehouse exists.
//      Skips if true.
//   6. Otherwise POSTs the warehouse spec (S3 or GCS).
//
// All steps are idempotent via the probe-first pattern; safe to re-run.
func adminLakekeeperBootstrap(args []string) error {
	fs := flag.NewFlagSet("lakekeeper-bootstrap", flag.ExitOnError)
	lkURL := fs.String("lakekeeper-url", "http://lakekeeper.lakekeeper.svc.cluster.local:8181", "Lakekeeper base URL")
	warehouseName := fs.String("warehouse-name", "datuplet", "Warehouse to create")
	whType := fs.String("type", "s3", "Warehouse storage type (s3 | gcs)")
	// Lakekeeper project the warehouse lands in. The default lakekeeper
	// project ID is "00000000-0000-0000-0000-000000000000" — sufficient for
	// single-project deployments where the Datuplet project's
	// lakekeeper_project_id is left at that default. For deployments that
	// run `pipeline-api admin create-project` (which allocates a fresh
	// lakekeeper project), pass that project's ID here so the warehouse
	// lives where the Datuplet project will look for it. Mismatched IDs
	// surface at run-trigger time as
	// "no warehouses registered for project ...".
	lakekeeperProjectID := fs.String("lakekeeper-project-id", "00000000-0000-0000-0000-000000000000",
		"Lakekeeper project the warehouse will be created in. Use 00000000-...0 for the default project, or pass a Datuplet project's lakekeeper_project_id from `SELECT lakekeeper_project_id FROM projects;` for multi-project deployments.")

	// S3 flags.
	bucket := fs.String("bucket", "datuplet", "S3 bucket holding the warehouse")
	s3Region := fs.String("s3-region", "local-01", "S3 region")
	s3Endpoint := fs.String("s3-endpoint", "", "S3 endpoint URL (default from S3_ENDPOINT env, with http:// prefix added if missing)")
	s3PathStyle := fs.Bool("path-style", true, "S3 path-style addressing")
	s3StsEnabled := fs.Bool("sts-enabled", true, "Enable STS-vended credentials for the warehouse")
	s3AccessKey := fs.String("s3-access-key", "", "S3 access key (default from S3_ACCESS_KEY env)")
	s3SecretKey := fs.String("s3-secret-key", "", "S3 secret key (default from S3_SECRET_KEY env)")

	// GCS flags.
	gcsBucket := fs.String("gcs-bucket", "", "GCS bucket name (required when --type=gcs)")
	gcsKeyPrefix := fs.String("gcs-key-prefix", "", "GCS key prefix")
	gcsSAKeyFile := fs.String("gcs-sa-key-file", "", "Path to a Google service-account key JSON file (default from GCS_SA_KEY_FILE env)")
	gcsCredType := fs.String("gcs-credential-type", "system-identity",
		"GCS credential type: 'system-identity' (default; Workload Identity Federation, no key file) or 'service-account-key' (static SA JSON key; needs --gcs-sa-key-file)")

	// Signing.
	keyFile := fs.String("signing-key-file", "", "Path to the RS256 PEM private key (default from SIGNING_KEY_FILE env)")
	keyID := fs.String("key-id", "", "JWK kid (default from SIGNING_KEY_ID env, then 'key-1')")
	audience := fs.String("audience", tokens.TableTokenAudience, "JWT aud claim (must match LAKEKEEPER__OPENID_AUDIENCE)")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), `Usage: pipeline-api admin lakekeeper-bootstrap [flags]

Bootstrap a Lakekeeper warehouse. Supports both S3 and GCS; for GCS,
both static-key and Workload-Identity (WIF) credentials are supported:

  # GCS + Workload Identity (recommended on GKE)
  pipeline-api admin lakekeeper-bootstrap \
    --type=gcs --gcs-bucket=$BUCKET \
    --gcs-credential-type=system-identity \
    --lakekeeper-project-id=$LK_PROJECT_ID \
    --lakekeeper-url=$LK_URL \
    --signing-key-file=$SIGNING_KEY

  # GCS + static service-account key (works anywhere)
  pipeline-api admin lakekeeper-bootstrap \
    --type=gcs --gcs-bucket=$BUCKET \
    --gcs-credential-type=service-account-key \
    --gcs-sa-key-file=/tmp/datuplet-sa.json \
    --lakekeeper-project-id=$LK_PROJECT_ID \
    --lakekeeper-url=$LK_URL \
    --signing-key-file=$SIGNING_KEY

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(args)

	if *keyFile == "" {
		*keyFile = os.Getenv("SIGNING_KEY_FILE")
	}
	if *keyFile == "" {
		return fmt.Errorf("--signing-key-file is required (or set SIGNING_KEY_FILE)")
	}
	if *keyID == "" {
		*keyID = os.Getenv("SIGNING_KEY_ID")
	}
	if *keyID == "" {
		*keyID = "key-1"
	}

	// Build the per-type spec, validating required inputs.
	spec := warehouseSpec{
		WarehouseName:       *warehouseName,
		LakekeeperProjectID: *lakekeeperProjectID,
		Type:                *whType,
	}
	switch *whType {
	case "s3":
		if *s3Endpoint == "" {
			*s3Endpoint = os.Getenv("S3_ENDPOINT")
		}
		if *s3AccessKey == "" {
			*s3AccessKey = os.Getenv("S3_ACCESS_KEY")
		}
		if *s3SecretKey == "" {
			*s3SecretKey = os.Getenv("S3_SECRET_KEY")
		}
		if *s3Endpoint == "" || *s3AccessKey == "" || *s3SecretKey == "" {
			return fmt.Errorf("S3 endpoint/access-key/secret-key are all required (flags or env)")
		}
		// MinIO secret in datuplet ships the endpoint as "minio.datuplet...:9000"
		// (no scheme); lakekeeper requires a fully-qualified URL.
		if !strings.HasPrefix(*s3Endpoint, "http://") && !strings.HasPrefix(*s3Endpoint, "https://") {
			*s3Endpoint = "http://" + *s3Endpoint
		}
		spec.S3 = &s3Spec{
			Bucket:          *bucket,
			Region:          *s3Region,
			Endpoint:        *s3Endpoint,
			PathStyleAccess: *s3PathStyle,
			StsEnabled:      *s3StsEnabled,
			AccessKey:       *s3AccessKey,
			SecretKey:       *s3SecretKey,
		}
	case "gcs":
		if *gcsBucket == "" {
			return fmt.Errorf("--gcs-bucket is required when --type=gcs")
		}
		switch *gcsCredType {
		case "", "system-identity":
			if *gcsSAKeyFile != "" || os.Getenv("GCS_SA_KEY_FILE") != "" {
				return fmt.Errorf("--gcs-credential-type=system-identity cannot be combined with --gcs-sa-key-file/GCS_SA_KEY_FILE")
			}
			spec.GCS = &gcsSpec{
				Bucket:         *gcsBucket,
				KeyPrefix:      *gcsKeyPrefix,
				StsEnabled:     *s3StsEnabled, // shared --sts-enabled flag
				CredentialType: "system-identity",
			}
		case "service-account-key":
			if *gcsSAKeyFile == "" {
				*gcsSAKeyFile = os.Getenv("GCS_SA_KEY_FILE")
			}
			if *gcsSAKeyFile == "" {
				return fmt.Errorf("--gcs-credential-type=service-account-key requires --gcs-sa-key-file (or GCS_SA_KEY_FILE)")
			}
			// Read the SA-key JSON from disk. Don't echo the path's
			// content into the error — rely on os.ReadFile's error.
			saBytes, err := os.ReadFile(*gcsSAKeyFile)
			if err != nil {
				return fmt.Errorf("read GCS service account key file: %w", err)
			}
			spec.GCS = &gcsSpec{
				Bucket:                *gcsBucket,
				KeyPrefix:             *gcsKeyPrefix,
				StsEnabled:            *s3StsEnabled, // shared --sts-enabled flag
				CredentialType:        "service-account-key",
				ServiceAccountKeyJSON: string(saBytes),
			}
		default:
			return fmt.Errorf("unknown --gcs-credential-type %q (want system-identity or service-account-key)", *gcsCredType)
		}
	default:
		return fmt.Errorf("unknown --type %q (want s3 or gcs)", *whType)
	}

	signer, err := tokens.LoadPrivateKeyFromPEMFile(*keyFile, *keyID)
	if err != nil {
		return fmt.Errorf("load signing key: %w", err)
	}

	// Mint a 5-minute service JWT (token_use="service") so a data-plane
	// verifier can reject it if it ever leaks onto a per-table RPC path.
	// Lakekeeper's `allowall` authz only checks signature+audience+expiry
	// for management calls, so the service shape is sufficient.
	jwt, err := tokens.MintServiceToken(signer, tokens.ServiceTokenSpec{
		Subject:  bootstrapServiceSubject,
		Audience: *audience,
		Lifetime: 5 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("mint service token: %w", err)
	}

	base := strings.TrimRight(*lkURL, "/")
	httpc := &http.Client{Timeout: 30 * time.Second}

	// Step 1: bootstrapped probe.
	bootstrapped, err := lakekeeperBootstrapped(httpc, base, jwt)
	if err != nil {
		return fmt.Errorf("probe bootstrapped: %w", err)
	}
	if bootstrapped {
		fmt.Println("  bootstrap: already done")
	} else {
		fmt.Println("Bootstrapping...")
		if err := postJSON(httpc, base+"/management/v1/bootstrap", jwt, map[string]any{
			"accept-terms-of-use": true,
		}, http.StatusNoContent, false); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
		fmt.Println("  bootstrap: 204 (OK)")
	}

	// Step 2: FGA tuples (server-admin + non-default-project project_admin).
	//
	// Both grants land BEFORE the warehouse-exists probe + create POST,
	// because (a) lakekeeper's warehouse-create endpoint enforces
	// project_admin on non-default projects, and (b) a future change that
	// scopes the warehouse-exists probe by x-project-id will need the
	// project_admin tuple in place for that probe too.
	fgaURL := os.Getenv("OPENFGA_URL")
	if fgaURL == "" {
		fmt.Println("  server-admin tuple: skipping (OPENFGA_URL not set)")
	} else {
		apiKey := os.Getenv("OPENFGA_API_KEY")
		storeName := os.Getenv("OPENFGA_STORE_NAME")
		if storeName == "" {
			storeName = "datuplet"
		}
		storeID, err := resolveStoreIDByName(context.Background(), fgaURL, apiKey, storeName)
		if err != nil {
			return fmt.Errorf("resolve store_id by name: %w", err)
		}
		if err := writeServerAdminTuple(context.Background(), fgaURL, apiKey, storeID); err != nil {
			return fmt.Errorf("write server-admin tuple: %w", err)
		}
		fmt.Println("  server-admin tuple: written (or already existed)")

		// Grant the bootstrap service identity project_admin on a non-default
		// lakekeeper project. The default project is implicitly covered by
		// the server-admin tuple; non-default projects need an explicit
		// project-scoped grant or lakekeeper's warehouse-create POST returns 403.
		if err := grantServiceIdentityProjectAdminIfMissing(context.Background(),
			fgaURL, apiKey, storeID, bootstrapServiceSubject, *lakekeeperProjectID); err != nil {
			return err
		}
	}

	// Step 3: warehouse exists probe. Scoped by --lakekeeper-project-id so a
	// warehouse of the same name in the default project doesn't false-positive
	// the probe for a non-default-project bootstrap.
	exists, err := lakekeeperWarehouseExists(httpc, base, jwt, *lakekeeperProjectID, *warehouseName)
	if err != nil {
		return fmt.Errorf("probe warehouse: %w", err)
	}
	if exists {
		fmt.Printf("  warehouse %q: already exists\n", *warehouseName)
	} else {
		fmt.Printf("Creating warehouse %q (type=%s)...\n", *warehouseName, *whType)
		body, err := buildWarehouseBody(spec)
		if err != nil {
			return fmt.Errorf("build warehouse body: %w", err)
		}
		if err := postJSON(httpc, base+"/management/v1/warehouse", jwt, body, http.StatusCreated, true); err != nil {
			return fmt.Errorf("create warehouse: %w", err)
		}
		fmt.Println("  warehouse: created")
	}

	fmt.Println("Bootstrap complete.")
	return nil
}

// lakekeeperBootstrapped probes /management/v1/info; returns true iff the
// catalog is already bootstrapped.
func lakekeeperBootstrapped(c *http.Client, base, jwt string) (bool, error) {
	req, err := http.NewRequest("GET", base+"/management/v1/info", nil)
	if err != nil {
		return false, fmt.Errorf("build info request: %w", err)
	}
	req.Header.Set("authorization", "Bearer "+jwt)
	resp, err := c.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	var info struct {
		Bootstrapped bool `json:"bootstrapped"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return false, err
	}
	return info.Bootstrapped, nil
}

// lakekeeperWarehouseExists lists warehouses in the given lakekeeper project
// and returns true iff one with the given name is registered there.
//
// The `x-project-id` header is REQUIRED: without it lakekeeper lists
// warehouses in the default project regardless of caller intent, which
// produces a false-positive "already exists" when re-bootstrapping against
// a non-default project (RFC 019 v0.2.1 fix).
func lakekeeperWarehouseExists(c *http.Client, base, jwt, projectID, name string) (bool, error) {
	req, err := http.NewRequest("GET", base+"/management/v1/warehouse", nil)
	if err != nil {
		return false, fmt.Errorf("build warehouse request: %w", err)
	}
	req.Header.Set("authorization", "Bearer "+jwt)
	req.Header.Set("x-project-id", projectID)
	resp, err := c.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("project %s: HTTP %d: %s", projectID, resp.StatusCode, body)
	}
	var list struct {
		Warehouses []struct {
			Name string `json:"name"`
		} `json:"warehouses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return false, fmt.Errorf("project %s: decode warehouses: %w", projectID, err)
	}
	for _, w := range list.Warehouses {
		if w.Name == name {
			return true, nil
		}
	}
	return false, nil
}

// writeServerAdminTuple writes (user:oidc~admin, admin, server:<uuid>) to the
// FGA store after lakekeeper bootstrap. Discovers server:<uuid> via /changes
// pagination (lakekeeper writes a single server:<uuid> tuple on first bootstrap).
// Idempotent: re-write of an existing tuple returns 400 "already exists" and is
// treated as success.
func writeServerAdminTuple(ctx context.Context, fgaURL, apiKey, storeID string) error {
	c := &http.Client{Timeout: 30 * time.Second}

	serverObj, err := authz.DiscoverServerObject(ctx, fgaURL, apiKey, storeID)
	if err != nil {
		return fmt.Errorf("discover server object: %w", err)
	}

	body, _ := json.Marshal(map[string]any{
		"writes": map[string]any{
			"tuple_keys": []map[string]string{
				{"user": "user:oidc~admin", "relation": "admin", "object": serverObj},
			},
		},
	})
	req, _ := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/stores/%s/write", strings.TrimRight(fgaURL, "/"), storeID),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 400 && bytes.Contains(b, []byte("already exists")) {
		return nil
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
}

// grantServiceIdentityProjectAdminIfMissing ensures the bootstrap service
// identity (user:oidc~<userSub>) holds project_admin on project:<projectID>.
// It uses a check-then-write pattern so re-runs don't spam blind writes:
//
//   - Skip entirely when projectID is the default lakekeeper project UUID
//     (the server-admin tuple already covers it) or when fgaURL is empty
//     (local-mode-without-FGA test path).
//   - Issue an FGA Check. If allowed=true, return.
//   - Otherwise Write the tuple. "Already exists" errors are tolerated
//     (concurrent bootstraps, partial prior runs).
//
// This unblocks `pipeline-api admin lakekeeper-bootstrap --lakekeeper-project-id=<non-default>`
// flows where the warehouse-create POST against a fresh project would
// otherwise 403 because the service JWT (sub=pipeline-api-bootstrap) has no
// project-scoped FGA tuples.
func grantServiceIdentityProjectAdminIfMissing(ctx context.Context, fgaURL, apiKey, storeID, userSub, projectID string) error {
	if fgaURL == "" {
		return nil
	}
	if projectID == defaultLakekeeperProjectUUID {
		return nil
	}
	has, err := checkProjectAdminTuple(ctx, fgaURL, apiKey, storeID, userSub, projectID)
	if err != nil {
		return fmt.Errorf("check service-identity project_admin on project:%s: %w", projectID, err)
	}
	fmt.Printf("  service project_admin: target=project:%s already=%v\n", projectID, has)
	if has {
		return nil
	}
	if err := writeProjectAdminTuple(ctx, fgaURL, apiKey, storeID, userSub, projectID); err != nil {
		return fmt.Errorf("grant service-identity project_admin on project:%s: %w", projectID, err)
	}
	fmt.Printf("  service project_admin: written on project:%s\n", projectID)
	return nil
}

// checkProjectAdminTuple issues an FGA Check for
// (user:oidc~<userSub>, project_admin, project:<projectID>) via the raw
// OpenFGA REST endpoint. Returns the boolean `allowed` from the response.
func checkProjectAdminTuple(ctx context.Context, fgaURL, apiKey, storeID, userSub, projectID string) (bool, error) {
	c := &http.Client{Timeout: 30 * time.Second}
	body, _ := json.Marshal(map[string]any{
		"tuple_key": map[string]string{
			"user":     "user:oidc~" + userSub,
			"relation": "project_admin",
			"object":   "project:" + projectID,
		},
	})
	req, _ := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/stores/%s/check", strings.TrimRight(fgaURL, "/"), storeID),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := c.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
	}
	var out struct {
		Allowed bool `json:"allowed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, err
	}
	return out.Allowed, nil
}

// writeProjectAdminTuple writes (user:oidc~<userSub>, project_admin,
// project:<projectID>) via the raw OpenFGA REST Write endpoint. A 400
// "already exists" response is treated as success (idempotent).
func writeProjectAdminTuple(ctx context.Context, fgaURL, apiKey, storeID, userSub, projectID string) error {
	c := &http.Client{Timeout: 30 * time.Second}
	body, _ := json.Marshal(map[string]any{
		"writes": map[string]any{
			"tuple_keys": []map[string]string{
				{
					"user":     "user:oidc~" + userSub,
					"relation": "project_admin",
					"object":   "project:" + projectID,
				},
			},
		},
	})
	req, _ := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/stores/%s/write", strings.TrimRight(fgaURL, "/"), storeID),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 400 && bytes.Contains(b, []byte("already exists")) {
		return nil
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
}

func resolveStoreIDByName(ctx context.Context, fgaURL, apiKey, storeName string) (string, error) {
	c := &http.Client{Timeout: 10 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(fgaURL, "/")+"/stores", nil)
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var sl struct {
		Stores []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"stores"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&sl); err != nil {
		return "", err
	}
	for _, s := range sl.Stores {
		if s.Name == storeName {
			return s.ID, nil
		}
	}
	return "", fmt.Errorf("store %q not found", storeName)
}

// postJSON POSTs body as JSON with bearer auth and asserts a specific
// success status. On any other status, returns an error.
//
// When redactBodyOnError is true, the response body is not included in the
// error message (only its length is shown). Set this for requests whose body
// carries sensitive material (e.g. GCS SA keys) — lakekeeper's REST handlers
// may echo the request payload in 4xx responses, which would leak credentials
// into logs.
func postJSON(c *http.Client, u, jwt string, body any, expect int, redactBodyOnError bool) error {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		return err
	}
	req, err := http.NewRequest("POST", u, buf)
	if err != nil {
		return fmt.Errorf("build POST request for %q: %w", u, err)
	}
	req.Header.Set("authorization", "Bearer "+jwt)
	req.Header.Set("content-type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != expect {
		b, _ := io.ReadAll(resp.Body)
		if redactBodyOnError {
			return fmt.Errorf("HTTP %d (want %d): <response body redacted: %d bytes>", resp.StatusCode, expect, len(b))
		}
		return fmt.Errorf("HTTP %d (want %d): %s", resp.StatusCode, expect, b)
	}
	return nil
}
