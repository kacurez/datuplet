package datalake

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIODataLake implements DataLake interface using MinIO/S3-compatible storage.
type MinIODataLake struct {
	client *minio.Client
	bucket string
}

// toObjectKey converts a storage path (full URL or relative) to MinIO object key.
// Handles s3:// URLs by stripping the scheme and bucket prefix.
// Examples:
//   - "s3://bucket/path/to/file" -> "path/to/file"
//   - "path/to/file" -> "path/to/file" (unchanged)
func (m *MinIODataLake) toObjectKey(storagePath string) string {
	if strings.HasPrefix(storagePath, "s3://") {
		// Remove s3://bucket/ prefix
		withoutScheme := strings.TrimPrefix(storagePath, "s3://")
		parts := strings.SplitN(withoutScheme, "/", 2)
		if len(parts) == 2 {
			return parts[1]
		}
		return ""
	}
	return storagePath
}

// NewMinIODataLake creates a new MinIO data lake client.
func NewMinIODataLake(cfg Config) (*MinIODataLake, error) {
	// Determine bucket lookup style based on config
	bucketLookup := minio.BucketLookupDNS // AWS S3 default (virtual-hosted)
	if cfg.UsePathStyle {
		bucketLookup = minio.BucketLookupPath // MinIO default (path-style)
	}

	// minio-go's New() rejects fully-qualified URLs with
	//   "Endpoint url cannot have fully qualified paths."
	// — it expects a bare host:port. Lakekeeper's warehouse config
	// (forwarded to us via S3_ENDPOINT) carries the URL with scheme
	// because lakekeeper itself wants it that way; the chart's
	// host.docker.internal default is also scheme-ful. Normalize here
	// so callers can pass either shape: strip http:// or https:// and
	// flip UseSSL accordingly. Any explicit cfg.UseSSL=true wins
	// (defensive for the bare-host case where the caller knows TLS is
	// in use). Mirrors splitEndpoint in pkg/datagateway/backend/minio.go.
	endpoint, useSSL := normalizeMinIOEndpoint(cfg.Endpoint, cfg.UseSSL)

	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       useSSL,
		Region:       cfg.Region,
		BucketLookup: bucketLookup,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return &MinIODataLake{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

// normalizeMinIOEndpoint accepts either a bare host:port or a fully-
// qualified http(s):// URL and returns the host:port + the implied SSL
// flag. The cfgUseSSL value wins when the input is bare (no scheme to
// derive from); when a scheme IS present, the scheme is authoritative
// and overrides cfgUseSSL — passing https://… with UseSSL=false is a
// caller mistake we'd rather correct than honor.
func normalizeMinIOEndpoint(endpoint string, cfgUseSSL bool) (string, bool) {
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		return strings.TrimPrefix(endpoint, "https://"), true
	case strings.HasPrefix(endpoint, "http://"):
		return strings.TrimPrefix(endpoint, "http://"), false
	default:
		return endpoint, cfgUseSSL
	}
}

// EnsureBucket creates the bucket if it doesn't exist.
func (m *MinIODataLake) EnsureBucket(ctx context.Context) error {
	exists, err := m.client.BucketExists(ctx, m.bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		err = m.client.MakeBucket(ctx, m.bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}
	return nil
}

// Write writes data to the specified path.
// Path can be a full S3 URL (s3://bucket/path) or relative path.
func (m *MinIODataLake) Write(ctx context.Context, objectPath string, reader io.Reader, size int64) error {
	key := m.toObjectKey(objectPath)
	_, err := m.client.PutObject(ctx, m.bucket, key, reader, size, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to write object %s: %w", objectPath, err)
	}
	return nil
}

// Read reads data from the specified path.
// Path can be a full S3 URL (s3://bucket/path) or relative path.
func (m *MinIODataLake) Read(ctx context.Context, objectPath string, offset, length int64) (io.ReadCloser, error) {
	key := m.toObjectKey(objectPath)
	opts := minio.GetObjectOptions{}

	// Set range if specified
	if offset >= 0 && length > 0 {
		err := opts.SetRange(offset, offset+length-1)
		if err != nil {
			return nil, fmt.Errorf("failed to set range: %w", err)
		}
	}

	obj, err := m.client.GetObject(ctx, m.bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to read object %s: %w", objectPath, err)
	}

	return obj, nil
}

// List returns objects under the given prefix.
// Prefix can be a full S3 URL (s3://bucket/path) or relative path.
func (m *MinIODataLake) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	key := m.toObjectKey(prefix)
	var objects []ObjectInfo

	objectsCh := m.client.ListObjects(ctx, m.bucket, minio.ListObjectsOptions{
		Prefix:    key,
		Recursive: true,
	})

	for obj := range objectsCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", obj.Err)
		}

		objects = append(objects, ObjectInfo{
			Path:         obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified,
			IsDir:        strings.HasSuffix(obj.Key, "/"),
		})
	}

	return objects, nil
}

// Exists checks if an object exists at the specified path.
// Path can be a full S3 URL (s3://bucket/path) or relative path.
func (m *MinIODataLake) Exists(ctx context.Context, path string) (bool, error) {
	key := m.toObjectKey(path)
	_, err := m.client.StatObject(ctx, m.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		// Check if it's a "not found" error
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat object %s: %w", path, err)
	}
	return true, nil
}

// PresignGet returns a pre-signed URL for reading an object, valid for the given duration.
func (m *MinIODataLake) PresignGet(ctx context.Context, objectPath string, expiry time.Duration) (string, error) {
	key := m.toObjectKey(objectPath)
	u, err := m.client.PresignedGetObject(ctx, m.bucket, key, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("failed to presign GET for %s: %w", objectPath, err)
	}
	return u.String(), nil
}

// Ensure MinIODataLake implements PresignableDataLake (which embeds DataLake)
var _ PresignableDataLake = (*MinIODataLake)(nil)
