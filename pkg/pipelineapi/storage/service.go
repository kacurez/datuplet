package storage

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// Service holds everything the four /api/v1/storage handlers need at
// request time. Two distinct production shapes:
//
//   - Catalog-backed: LakekeeperURL + WarehouseName + Minter are set;
//     the handlers proxy List/Load/Schema through a
//     `pkg/catalogwriter.Client` and read parquet via the iceberg-go
//     REST-catalog table (which carries lakekeeper-vended STS credentials
//     inside the table's FS closure — no long-lived S3 creds needed on
//     pipeline-api). S3Props is nil; WarehouseURI and OrgName are empty
//     in this shape.
//   - Filesystem-fixture-backed (tests + legacy local mode): only
//     WarehouseURI + OrgName + AllowLocal are set; LakekeeperURL is
//     empty and handlers fall back to the directory walker.
//
// AllowLocal is true iff WarehouseURI is a file:// URI; the handlers
// use it to gate the symlink-rejection step (which only applies to
// local paths).
//
// For production use, prefer NewForLakekeeper over NewFromEnv.
type Service struct {
	WarehouseURI string            // e.g. "s3://datuplet" or "file:///abs/path"; empty in lakekeeper mode
	OrgName      string            // warehouse org segment — "myorg" default; unused in lakekeeper mode
	S3Props      map[string]string // iceberg-go property map for s3:// warehouses; nil in lakekeeper mode and file:// mode
	GCSProps     map[string]string // iceberg-go property map for gs:// warehouses; nil in lakekeeper mode and s3:// mode
	AllowLocal   bool              // true when WarehouseURI is file://

	// LakekeeperURL is the catalog REST base URL the handlers proxy
	// against. Empty disables the proxy path (handlers fall back to the
	// fixture walker).
	LakekeeperURL string

	// WarehouseName is a deployment-time default lakekeeper warehouse name
	// (legacy field, used by tests + the per-request fallback when
	// WarehouseResolver is nil). When WarehouseResolver is set, callers
	// resolve per-request and ignore this field.
	WarehouseName string

	// WarehouseResolver resolves the lakekeeper warehouse name for a given
	// lakekeeper project UUID at request time. Pipeline-api asks lakekeeper
	// for the warehouses attached to the user's project and picks the first.
	// An explicit per-project warehouse selector is not yet implemented.
	//
	// nil → falls back to WarehouseName (legacy / tests).
	WarehouseResolver func(ctx context.Context, lakekeeperProjectID string) (string, error)

	// Minter mints an impersonation JWT for the authenticated user in ctx.
	// Called on every catalog operation so the token always carries the
	// current request's identity and is never stale.
	// nil is OK when LakekeeperURL is empty (tests / filesystem walker).
	Minter func(ctx context.Context) (tokens.ImpersonationToken, error)

	// LakekeeperProjectIDFor maps a Datuplet project UUID to the lakekeeper
	// Project UUID that should be sent as x-project-id on catalog requests.
	// Cluster mode: pgx lookup in the projects table.
	// Local mode: closure over localmode.LoadProjectState(dir).LakekeeperProjectID.
	// nil or a func that returns "" is safe — callers omit the header.
	// Required for per-project authz in lakekeeper.
	LakekeeperProjectIDFor func(ctx context.Context, datupletProjectID uuid.UUID) (string, error)
}

// NewForLakekeeper constructs a Service backed exclusively by lakekeeper.
// Pipeline-api holds no long-lived S3 credentials — the iceberg-go REST
// catalog table returned by LoadTable carries lakekeeper-vended STS
// credentials inside its FS closure, so every read (Preview, TableInfo
// manifests) uses per-request STS creds scoped to that table's S3 prefix.
//
// url must be non-empty; if blank the function returns (nil, nil) so the
// caller can wire a 503 soft-degrade route instead of failing the whole
// pipeline-api boot.
//
// The Minter + LakekeeperProjectIDFor + WarehouseResolver fields are
// wired separately by the caller via WithLakekeeper after the signing
// key is loaded — they require pipeline-api's signer + project store,
// which this constructor can't see.
func NewForLakekeeper(url string) (*Service, error) {
	if url == "" {
		return nil, nil
	}
	return &Service{
		LakekeeperURL: url,
	}, nil
}

// NewFromEnv reads DATUPLET_STORAGE_TYPE + DATUPLET_STORAGE_ROOT and
// returns a Service backed by the local filesystem. It returns (nil, nil)
// when either env var is absent so the caller can wire a 503 soft-degrade
// route instead of failing boot.
//
// Supported DATUPLET_STORAGE_TYPE values:
//
//   - "filesystem": DATUPLET_STORAGE_ROOT must be an absolute path.
//     WarehouseURI becomes "file://" + the path; S3Props is nil;
//     AllowLocal is true.
//
// DATUPLET_ORG overrides the default org segment ("myorg").
//
// This constructor is kept for tests and local-mode pipelines that use a
// filesystem warehouse. For production (lakekeeper-backed) use, prefer
// NewForLakekeeper — it requires no S3 credentials.
func NewFromEnv() (*Service, error) {
	sType := os.Getenv("DATUPLET_STORAGE_TYPE")
	sRoot := os.Getenv("DATUPLET_STORAGE_ROOT")
	if sType == "" || sRoot == "" {
		return nil, nil
	}
	switch sType {
	case "filesystem":
		if !strings.HasPrefix(sRoot, "/") {
			return nil, fmt.Errorf("DATUPLET_STORAGE_ROOT must be absolute, got %q", sRoot)
		}
		return &Service{
			WarehouseURI: "file://" + sRoot,
			OrgName:      orgFromEnvOrDefault(),
			AllowLocal:   true,
		}, nil
	default:
		return nil, fmt.Errorf("DATUPLET_STORAGE_TYPE=%q not supported (production deployments use lakekeeper vended creds via NewForLakekeeper)", sType)
	}
}

// WithLakekeeper wires the catalog-proxy fields onto a constructed
// Service. Called from cmd/pipeline-api after the signer is loaded.
// Empty url leaves the fields zero-valued (handlers fall back to the
// fixture walker).
//
// warehouseResolver is the per-request "warehouse for this project"
// lookup that replaces the deployment-time DATUPLET_LAKEKEEPER_WAREHOUSE
// env. Empty / nil is OK in tests; production wiring always supplies it.
func (s *Service) WithLakekeeper(
	url string,
	minter func(ctx context.Context) (tokens.ImpersonationToken, error),
	projectIDFor func(ctx context.Context, datupletProjectID uuid.UUID) (string, error),
	warehouseResolver func(ctx context.Context, lakekeeperProjectID string) (string, error),
) *Service {
	s.LakekeeperURL = url
	s.Minter = minter
	s.LakekeeperProjectIDFor = projectIDFor
	s.WarehouseResolver = warehouseResolver
	return s
}

// orgFromEnvOrDefault honours DATUPLET_ORG but falls back to "myorg"
// for consistency with the testdata fixtures and the examples.
func orgFromEnvOrDefault() string {
	if s := os.Getenv("DATUPLET_ORG"); s != "" {
		return s
	}
	return "myorg"
}
