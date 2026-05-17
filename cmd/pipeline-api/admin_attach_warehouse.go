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
// existing lakekeeper warehouse by calling EnsureWarehouseInProject. The
// warehouse must already exist in lakekeeper (created by
// `admin lakekeeper-bootstrap` first) OR will be created inside the
// per-project lakekeeper Project if it doesn't exist yet.
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

	// GCS flags.
	gcsBucket := fs.String("gcs-bucket", "", "GCS bucket name (required when --type=gcs)")
	gcsKeyPrefix := fs.String("gcs-key-prefix", "", "GCS key prefix")
	gcsSAKeyFile := fs.String("gcs-sa-key-file", "", "Path to a Google service-account key JSON file (default from GCS_SA_KEY_FILE env)")

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

	// Build the warehouse profile from flags / env vars.
	profile, err := attachWarehouseProfile(*whType, attachWarehouseS3Opts{
		bucket:      *bucket,
		region:      *s3Region,
		endpoint:    *s3Endpoint,
		pathStyle:   *s3PathStyle,
		stsEnabled:  *s3StsEnabled,
		accessKey:   *s3AccessKey,
		secretKey:   *s3SecretKey,
	}, attachWarehouseGCSOpts{
		bucket:    *gcsBucket,
		keyPrefix: *gcsKeyPrefix,
		saKeyFile: *gcsSAKeyFile,
		// shared --sts-enabled flag (mirrors lakekeeper-bootstrap behaviour)
		stsEnabled: *s3StsEnabled,
	})
	if err != nil {
		return fmt.Errorf("warehouse profile: %w", err)
	}

	if err := env.lkManager.EnsureWarehouseInProject(ctx, proj.LakekeeperProjectID, *warehouseName, profile); err != nil {
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
	bucket, keyPrefix, saKeyFile string
	stsEnabled                   bool
}

// attachWarehouseProfile constructs the lakekeeper.S3WarehouseProfile from
// the parsed flags. Only S3 is supported today (EnsureWarehouseInProject
// accepts S3WarehouseProfile); GCS support via EnsureWarehouseInProject is
// a future slice.
func attachWarehouseProfile(whType string, s3 attachWarehouseS3Opts, gcs attachWarehouseGCSOpts) (lakekeeper.S3WarehouseProfile, error) {
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
			return lakekeeper.S3WarehouseProfile{}, fmt.Errorf("S3 endpoint/access-key/secret-key are all required (flags or env)")
		}
		if !strings.HasPrefix(s3.endpoint, "http://") && !strings.HasPrefix(s3.endpoint, "https://") {
			s3.endpoint = "http://" + s3.endpoint
		}
		pathStyle := s3.pathStyle
		return lakekeeper.S3WarehouseProfile{
			Bucket:    s3.bucket,
			Region:    s3.region,
			Endpoint:  s3.endpoint,
			AccessKey: s3.accessKey,
			SecretKey: s3.secretKey,
			PathStyle: &pathStyle,
		}, nil
	case "gcs":
		// EnsureWarehouseInProject only accepts S3WarehouseProfile today.
		// GCS attach-warehouse is deferred; surface a clear error so the
		// operator uses lakekeeper-bootstrap for GCS warehouses and calls
		// this subcommand only for S3.
		_ = gcs
		return lakekeeper.S3WarehouseProfile{}, fmt.Errorf("--type=gcs is not yet supported by attach-warehouse (use lakekeeper-bootstrap for GCS warehouses)")
	default:
		return lakekeeper.S3WarehouseProfile{}, fmt.Errorf("unknown --type %q (want s3 or gcs)", whType)
	}
}
