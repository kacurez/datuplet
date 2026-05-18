package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	// Blank-import the centralised iceberg-go IO scheme registration
	// package. It transitively registers the s3:// scheme via the
	// upstream gocloud subpackage and additionally overrides the gs://
	// scheme with the Datuplet refreshing-TokenSource factory. The
	// file:// scheme is registered by iceberg-go/io's package init, so
	// no import is needed for local paths. See pkg/datupleticeio/doc.go
	// and RFC 019 §4.5.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

// LoadFS returns an iceberg-go-compatible filesystem for the given URI.
// Supported schemes: file://, s3://, gs://.
//
// props (optional) supplies the credential + endpoint properties
// iceberg-go expects for the relevant backend; can be nil for local paths.
// Use S3Props to build the s3:// property map and GCSProps for gs://.
//
// This is the single entry-point every other file in the package uses
// to obtain a filesystem. Do not call iceberg-go's io package directly
// elsewhere — go through here so scheme gating stays in one place.
//
// The returned value is an iceberg-go io.IO (Open + Remove at minimum;
// the concrete impl also supports ReadFileIO for efficient reads).
// Call sites pass it into iceberg-go helpers like table.NewFromLocation
// via a FSysF closure: `func(ctx) (io.IO, error) { return LoadFS(ctx, uri, props) }`.
func LoadFS(ctx context.Context, uri string, s3Props map[string]string) (iceio.IO, error) {
	props := map[string]string{}
	for k, v := range s3Props {
		props[k] = v
	}
	switch {
	case strings.HasPrefix(uri, "file://"):
		// local: no props needed, LocalFS is registered by iceberg-go/io init.
	case strings.HasPrefix(uri, "s3://"):
		// props already carry S3 creds; gocloud subpackage registers the scheme.
	case strings.HasPrefix(uri, "gs://"):
		// GCS IO factory registered by pkg/datupleticeio blank-import above.
		// props must carry gcs.oauth2.token (from GCSProps); forwarded verbatim.
	default:
		return nil, fmt.Errorf("unsupported URI scheme: %q", uri)
	}
	return iceio.LoadFS(ctx, props, uri)
}

// GCSProps builds the props map that pkg/datupleticeio's gs:// factory
// consumes. Mirrors S3Props's shape; expects a lakekeeper-vended OAuth
// bearer token and its absolute expiry time.
//
// The returned map uses the two keys the datupletGCSFactory reads:
//
//	"gcs.oauth2.token"             — the bearer token itself
//	"gcs.oauth2.token-expires-at"  — absolute expiry as Unix milliseconds
//
// The caller passes this map directly to LoadFS for a gs:// URI.
func GCSProps(oauthToken string, expiresAt time.Time) map[string]string {
	return map[string]string{
		"gcs.oauth2.token":            oauthToken,
		"gcs.oauth2.token-expires-at": strconv.FormatInt(expiresAt.UnixMilli(), 10),
	}
}

// S3Props normalizes our DATUPLET_* / S3_* env vars into the property
// keys iceberg-go's S3 backend recognizes. Values come from the caller;
// this helper just fixes the keys.
//
// pathStyle=true (MinIO, localstack, any S3-compatible non-AWS store) maps
// to S3ForceVirtualAddressing=false. pathStyle=false (real AWS) maps to
// S3ForceVirtualAddressing=true so requests use virtual-hosted addressing
// (host is "<bucket>.s3.<region>.amazonaws.com").
//
// An empty region defaults to "us-east-1" (our MinIO/OrbStack convention);
// any non-empty region is passed through so real AWS deployments outside
// us-east-1 sign correctly. An empty endpoint is omitted entirely so
// iceberg-go / aws-sdk-go-v2 falls back to default endpoint resolution
// (the AWS native case); a non-empty endpoint keeps the existing
// ensureHTTPScheme behaviour for MinIO-style "host:port" strings.
func S3Props(endpoint, bucket, accessKey, secretKey, region string, pathStyle bool) map[string]string {
	_ = bucket // accepted for symmetry with callers; the bucket lives in the URI, not the props.
	if region == "" {
		region = "us-east-1"
	}
	props := map[string]string{
		iceio.S3AccessKeyID:            accessKey,
		iceio.S3SecretAccessKey:        secretKey,
		iceio.S3Region:                 region,
		iceio.S3ForceVirtualAddressing: boolStr(!pathStyle),
	}
	if endpoint != "" {
		props[iceio.S3EndpointURL] = ensureHTTPScheme(endpoint)
	}
	return props
}

func ensureHTTPScheme(s string) string {
	if strings.Contains(s, "://") {
		return s
	}
	return "http://" + s
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
