package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// warehouseSpec describes the inputs to buildWarehouseBody. The Type
// field selects which sibling spec (S3 or GCS) is consulted.
type warehouseSpec struct {
	WarehouseName string
	Type          string // "s3" | "gcs"
	S3            *s3Spec
	GCS           *gcsSpec
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
type gcsSpec struct {
	Bucket                string
	KeyPrefix             string
	StsEnabled            bool
	ServiceAccountKeyJSON string
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
			"project-id":     "00000000-0000-0000-0000-000000000000",
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
		var keyObj map[string]any
		if err := json.Unmarshal([]byte(spec.GCS.ServiceAccountKeyJSON), &keyObj); err != nil {
			// Do NOT echo the JSON content (could leak the private key
			// into logs); surface only a generic invalidity message.
			return nil, fmt.Errorf("invalid GCS service account key JSON: %s", err)
		}
		profile := map[string]any{
			"type":        "gcs",
			"bucket":      spec.GCS.Bucket,
			"sts-enabled": spec.GCS.StsEnabled,
		}
		if spec.GCS.KeyPrefix != "" {
			profile["key-prefix"] = spec.GCS.KeyPrefix
		}
		return map[string]any{
			"warehouse-name": spec.WarehouseName,
			"project-id":     "00000000-0000-0000-0000-000000000000",
			"storage-profile": profile,
			"storage-credential": map[string]any{
				"type":            "gcs",
				"credential-type": "service-account-key",
				"key":             keyObj,
			},
			"delete-profile": map[string]any{"type": "hard"},
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

	// Signing.
	keyFile := fs.String("signing-key-file", "", "Path to the RS256 PEM private key (default from SIGNING_KEY_FILE env)")
	keyID := fs.String("key-id", "", "JWK kid (default from SIGNING_KEY_ID env, then 'key-1')")
	audience := fs.String("audience", tokens.TableTokenAudience, "JWT aud claim (must match LAKEKEEPER__OPENID_AUDIENCE)")
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
		WarehouseName: *warehouseName,
		Type:          *whType,
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
		if *gcsSAKeyFile == "" {
			*gcsSAKeyFile = os.Getenv("GCS_SA_KEY_FILE")
		}
		if *gcsSAKeyFile == "" {
			return fmt.Errorf("--gcs-sa-key-file is required when --type=gcs (or set GCS_SA_KEY_FILE)")
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
			ServiceAccountKeyJSON: string(saBytes),
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
		Subject:  "pipeline-api-bootstrap",
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

	// Step 2: warehouse exists probe.
	exists, err := lakekeeperWarehouseExists(httpc, base, jwt, *warehouseName)
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
	// Write the user:oidc~admin admin server:<uuid> tuple for the lakekeeper server admin.
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

// lakekeeperWarehouseExists lists warehouses and returns true iff one with
// the given name is registered.
func lakekeeperWarehouseExists(c *http.Client, base, jwt, name string) (bool, error) {
	req, err := http.NewRequest("GET", base+"/management/v1/warehouse", nil)
	if err != nil {
		return false, fmt.Errorf("build warehouse request: %w", err)
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
	var list struct {
		Warehouses []struct {
			Name string `json:"name"`
		} `json:"warehouses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return false, err
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

	serverObj, err := discoverServerObject(ctx, c, fgaURL, apiKey, storeID)
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

func discoverServerObject(ctx context.Context, c *http.Client, fgaURL, apiKey, storeID string) (string, error) {
	token := ""
	pattern := regexp.MustCompile(`^server:[0-9a-f-]+$`)
	for i := 0; i < 100; i++ {
		u := fmt.Sprintf("%s/stores/%s/changes?page_size=100", strings.TrimRight(fgaURL, "/"), storeID)
		if token != "" {
			u += "&continuation_token=" + token
		}
		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := c.Do(req)
		if err != nil {
			return "", err
		}
		var page struct {
			Changes []struct {
				TupleKey struct{ Object string } `json:"tuple_key"`
			} `json:"changes"`
			ContinuationToken string `json:"continuation_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return "", err
		}
		resp.Body.Close()
		for _, ch := range page.Changes {
			if pattern.MatchString(ch.TupleKey.Object) {
				return ch.TupleKey.Object, nil
			}
		}
		if page.ContinuationToken == "" {
			break
		}
		token = page.ContinuationToken
	}
	return "", fmt.Errorf("no server:<uuid> tuple found")
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
