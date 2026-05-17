package storage

import (
	"fmt"
	"strings"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/datuplet/datuplet/pkg/lib/datalake"
)

// dataLakeFor returns a datalake.DataLake backed by the Service's
// WarehouseURI + S3Props. For s3:// URIs we build a MinIODataLake
// (works for both MinIO and real AWS S3). For file:// we use
// FilesystemDataLake rooted at the host path. Unsupported schemes
// return an error.
//
// The returned DataLake uses paths RELATIVE to the warehouse root —
// e.g. passing "orgs/myorg/projects/<uuid>/tables" to List.
//
// This is the second S3/filesystem scheme dispatch in the package —
// LoadFS() in fsys.go is the first, and it hands off to iceberg-go's
// io.IO registry. Both need to stay aligned on what S3Props looks like
// (see S3Props in fsys.go for the iceberg-go key set).
func (s *Service) dataLakeFor() (datalake.DataLake, error) {
	switch {
	case strings.HasPrefix(s.WarehouseURI, "file://"):
		root := strings.TrimPrefix(s.WarehouseURI, "file://")
		return datalake.NewFilesystemDataLake(root), nil
	case strings.HasPrefix(s.WarehouseURI, "s3://"):
		bucket := strings.TrimPrefix(s.WarehouseURI, "s3://")
		endpoint := s.S3Props[iceio.S3EndpointURL]
		// UsePathStyle is the NEGATION of iceberg-go's
		// S3ForceVirtualAddressing key — S3Props() wrote that key as
		// boolStr(!pathStyle), so we flip it back here.
		usePathStyle := s.S3Props[iceio.S3ForceVirtualAddressing] != "true"
		region := s.S3Props[iceio.S3Region]
		var hostOnly string
		var useSSL bool
		if endpoint == "" {
			// AWS-native deploys omit S3_ENDPOINT and rely on the SDK
			// endpoint resolver. minio-go has no equivalent auto-resolve,
			// so synthesize the standard regional S3 endpoint. Always
			// HTTPS + virtual-hosted unless path-style was requested.
			if region == "" {
				region = "us-east-1"
			}
			hostOnly = "s3." + region + ".amazonaws.com"
			useSSL = true
		} else {
			// UseSSL follows the scheme S3Props() preserved via
			// ensureHTTPScheme. minio-go parses Endpoint as host[:port],
			// not a URL — strip the scheme we carried around in
			// iceberg-go's property bag.
			useSSL = strings.HasPrefix(endpoint, "https://")
			hostOnly = strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
		}
		return datalake.NewMinIODataLake(datalake.Config{
			Type:         "minio",
			Endpoint:     hostOnly,
			Bucket:       bucket,
			AccessKey:    s.S3Props[iceio.S3AccessKeyID],
			SecretKey:    s.S3Props[iceio.S3SecretAccessKey],
			Region:       region,
			UseSSL:       useSSL,
			UsePathStyle: usePathStyle,
		})
	default:
		return nil, fmt.Errorf("unsupported warehouse URI scheme: %q", s.WarehouseURI)
	}
}
