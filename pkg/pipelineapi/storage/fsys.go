package storage

import (
	"context"
	"fmt"
	"strings"

	iceio "github.com/apache/iceberg-go/io"
	// Blank-import the gocloud subpackage to register the s3:// scheme
	// factory with iceberg-go's IO registry. The file:// scheme is
	// registered by iceberg-go/io's package init, so no import is needed
	// for local paths. Keep this the ONLY place that imports iceberg-go's
	// io packages so scheme gating stays centralised.
	_ "github.com/apache/iceberg-go/io/gocloud"
)

// LoadFS returns an iceberg-go-compatible filesystem for the given URI.
// Supported schemes: file://, s3://. s3Props (optional) supplies the
// S3 credential + endpoint properties iceberg-go expects; can be nil
// for local paths.
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
	default:
		return nil, fmt.Errorf("unsupported URI scheme: %q", uri)
	}
	return iceio.LoadFS(ctx, props, uri)
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
