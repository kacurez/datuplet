package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/lakekeeper"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// adminAttachWarehouse associates an existing Datuplet project with an
// existing lakekeeper warehouse by calling EnsureS3WarehouseInProject or
// EnsureGCSWarehouseInProject. The warehouse must already exist in
// lakekeeper (created by `admin lakekeeper-bootstrap` first) OR will be
// created inside the per-project lakekeeper Project if it doesn't exist yet.
//
// This is the single-purpose interface for wiring a project to a warehouse.
//
// The pool and env arguments come from runAdmin's shared setup (pool from
// pipelineapidb.Open; env from dialProjectProvisioning). Both may be nil
// when testing flag-parsing logic; the function returns early with an error
// before dereferencing them.
func adminAttachWarehouse(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("attach-warehouse", flag.ContinueOnError)

	projectName := fs.String("project", "", "Datuplet project name (required)")
	warehouseName := fs.String("warehouse", "", "Lakekeeper warehouse name (required)")

	// Warehouse-type flag — mirrors lakekeeper-bootstrap.
	whType := fs.String("type", "s3", "Warehouse storage type (s3 | gcs)")

	// S3 flags — same names as lakekeeper-bootstrap for parity.
	bucket := fs.String("bucket", "datuplet", "S3 bucket holding the warehouse")
	s3Region := fs.String("s3-region", "local-01", "S3 region")
	s3Endpoint := fs.String("s3-endpoint", "", "S3 endpoint URL (default from S3_ENDPOINT env)")
	s3PathStyle := fs.Bool("path-style", true, "S3 path-style addressing")
	s3StsEnabled := fs.Bool("sts-enabled", true, "Enable STS-vended credentials")
	s3AccessKey := fs.String("s3-access-key", "", "S3 access key (default from S3_ACCESS_KEY env)")
	s3SecretKey := fs.String("s3-secret-key", "", "S3 secret key (default from S3_SECRET_KEY env)")

	// GCS flags — mirror lakekeeper-bootstrap (admin_lakekeeper.go).
	gcsBucket := fs.String("gcs-bucket", "", "GCS bucket name (required when --type=gcs)")
	gcsKeyPrefix := fs.String("gcs-key-prefix", "", "GCS key prefix")
	gcsSAKeyFile := fs.String("gcs-sa-key-file", "", "Path to a Google service-account key JSON file (default from GCS_SA_KEY_FILE env)")
	gcsCredType := fs.String("gcs-credential-type", "system-identity",
		"GCS credential type: 'system-identity' (default; Workload Identity Federation, no key file) or 'service-account-key' (static SA JSON key; needs --gcs-sa-key-file)")

	// Pass our own FlagSet through dialProjectProvisioning so it adds its
	// signing-key / lakekeeper-url / openfga flags on the same set. One
	// fs.Parse(args) inside dialProjectProvisioning handles everything;
	// avoids the previous bug where local parse failed on --signing-key-file
	// because it wasn't yet declared. (Mirrors adminCreateProject's pattern.)
	env, err := dialProjectProvisioning(ctx, pool, fs, args)
	if err != nil {
		return fmt.Errorf("dial project provisioning: %w", err)
	}

	if *projectName == "" || *warehouseName == "" {
		return fmt.Errorf("--project and --warehouse are required")
	}

	// pool may be nil in flag-only tests; error before reaching here.
	if pool == nil {
		return fmt.Errorf("database pool is required")
	}

	// Look up the Datuplet project by name to retrieve lakekeeper_project_id.
	proj, err := store.GetProjectByName(ctx, pool, *projectName)
	if err != nil {
		return fmt.Errorf("project %q not found: %w", *projectName, err)
	}
	if proj.LakekeeperProjectID == "" {
		return fmt.Errorf("project %q has no lakekeeper_project_id — was it created via `admin create-project`?", *projectName)
	}

	if err := attachWarehouse(ctx, env.lkManager, proj.LakekeeperProjectID, *warehouseName, *whType,
		attachWarehouseS3Opts{
			bucket:     *bucket,
			region:     *s3Region,
			endpoint:   *s3Endpoint,
			pathStyle:  *s3PathStyle,
			stsEnabled: *s3StsEnabled,
			accessKey:  *s3AccessKey,
			secretKey:  *s3SecretKey,
		},
		attachWarehouseGCSOpts{
			bucket:    *gcsBucket,
			keyPrefix: *gcsKeyPrefix,
			saKeyFile: *gcsSAKeyFile,
			credType:  *gcsCredType,
			// shared --sts-enabled flag (mirrors lakekeeper-bootstrap behaviour)
			stsEnabled: *s3StsEnabled,
		}); err != nil {
		return fmt.Errorf("attach warehouse: %w", err)
	}
	fmt.Printf("Project %s (lakekeeper-id=%s) attached to warehouse %s.\n",
		*projectName, proj.LakekeeperProjectID, *warehouseName)
	return nil
}

type attachWarehouseS3Opts struct {
	bucket, region, endpoint, accessKey, secretKey string
	pathStyle, stsEnabled                          bool
}

type attachWarehouseGCSOpts struct {
	bucket, keyPrefix, saKeyFile, credType string
	stsEnabled                             bool
}

// warehouseEnsurer is the subset of lakekeeper.Manager used by
// attachWarehouse. The seam keeps the function testable without standing
// up an httptest server for the CLI-layer unit tests.
type warehouseEnsurer interface {
	EnsureS3WarehouseInProject(ctx context.Context, projectID, warehouseName string, profile lakekeeper.S3WarehouseProfile) error
	EnsureGCSWarehouseInProject(ctx context.Context, projectID, warehouseName string, profile lakekeeper.GCSWarehouseProfile) error
}

// attachWarehouse builds the per-type warehouse profile from the parsed
// flags and dispatches to the matching EnsureXxxWarehouseInProject on the
// lakekeeper Manager. Splitting profile-build + Ensure into one
// type-dispatched function keeps the caller flat and avoids leaking the
// per-flavor profile struct types out of this file.
//
// GCS env fallbacks mirror admin_lakekeeper.go (GCS_SA_KEY_FILE,
// GCS_CREDENTIAL_TYPE). S3 fallbacks (S3_ENDPOINT, S3_ACCESS_KEY,
// S3_SECRET_KEY) are unchanged.
func attachWarehouse(
	ctx context.Context,
	mgr warehouseEnsurer,
	projectID, warehouseName, whType string,
	s3 attachWarehouseS3Opts,
	gcs attachWarehouseGCSOpts,
) error {
	switch whType {
	case "s3":
		if s3.endpoint == "" {
			s3.endpoint = os.Getenv("S3_ENDPOINT")
		}
		if s3.accessKey == "" {
			s3.accessKey = os.Getenv("S3_ACCESS_KEY")
		}
		if s3.secretKey == "" {
			s3.secretKey = os.Getenv("S3_SECRET_KEY")
		}
		if s3.endpoint == "" || s3.accessKey == "" || s3.secretKey == "" {
			return fmt.Errorf("S3 endpoint/access-key/secret-key are all required (flags or env)")
		}
		if !strings.HasPrefix(s3.endpoint, "http://") && !strings.HasPrefix(s3.endpoint, "https://") {
			s3.endpoint = "http://" + s3.endpoint
		}
		pathStyle := s3.pathStyle
		profile := lakekeeper.S3WarehouseProfile{
			Bucket:    s3.bucket,
			Region:    s3.region,
			Endpoint:  s3.endpoint,
			AccessKey: s3.accessKey,
			SecretKey: s3.secretKey,
			PathStyle: &pathStyle,
		}
		return mgr.EnsureS3WarehouseInProject(ctx, projectID, warehouseName, profile)

	case "gcs":
		if gcs.bucket == "" {
			return fmt.Errorf("--gcs-bucket is required when --type=gcs")
		}
		credType := gcs.credType
		if credType == "" {
			credType = os.Getenv("GCS_CREDENTIAL_TYPE")
		}
		if credType == "" {
			credType = "system-identity"
		}
		profile := lakekeeper.GCSWarehouseProfile{
			Bucket:         gcs.bucket,
			KeyPrefix:      gcs.keyPrefix,
			StsEnabled:     gcs.stsEnabled,
			CredentialType: credType,
		}
		switch credType {
		case "", "system-identity":
			if gcs.saKeyFile != "" || os.Getenv("GCS_SA_KEY_FILE") != "" {
				return fmt.Errorf("--gcs-credential-type=system-identity cannot be combined with --gcs-sa-key-file/GCS_SA_KEY_FILE")
			}
		case "service-account-key":
			path := gcs.saKeyFile
			if path == "" {
				path = os.Getenv("GCS_SA_KEY_FILE")
			}
			if path == "" {
				return fmt.Errorf("--gcs-credential-type=service-account-key requires --gcs-sa-key-file (or GCS_SA_KEY_FILE)")
			}
			saBytes, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("read GCS service account key file: %w", err)
			}
			profile.ServiceAccountKeyJSON = string(saBytes)
		default:
			return fmt.Errorf("unknown --gcs-credential-type %q (want system-identity or service-account-key)", credType)
		}
		return mgr.EnsureGCSWarehouseInProject(ctx, projectID, warehouseName, profile)

	default:
		return fmt.Errorf("unknown --type %q (want s3 or gcs)", whType)
	}
}
