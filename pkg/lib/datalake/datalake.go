// Package datalake provides an abstraction for data lake storage operations.
package datalake

import (
	"context"
	"io"
	"time"
)

// PresignableDataLake extends DataLake with pre-signed URL generation.
type PresignableDataLake interface {
	DataLake

	// PresignGet returns a pre-signed URL for reading an object, valid for the given duration.
	PresignGet(ctx context.Context, path string, expiry time.Duration) (string, error)
}

// ObjectInfo contains metadata about an object in the data lake.
type ObjectInfo struct {
	Path         string
	Size         int64
	LastModified time.Time
	IsDir        bool
}

// DataLake defines the interface for data lake storage operations.
type DataLake interface {
	// Write writes data to the specified path.
	Write(ctx context.Context, path string, reader io.Reader, size int64) error

	// Read reads data from the specified path.
	// If offset >= 0 and length > 0, performs a range read.
	// Use offset=-1 and length=-1 to read the entire object.
	Read(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)

	// List returns objects under the given prefix.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)

	// Exists checks if an object exists at the specified path.
	Exists(ctx context.Context, path string) (bool, error)
}

// Config holds configuration for data lake connections.
// All fields are treated as opaque config.
type Config struct {
	Type         string // minio, s3, duckdb
	Endpoint     string
	Bucket       string
	AccessKey    string
	SecretKey    string
	Region       string
	UseSSL       bool // Use HTTPS (transport security)
	UsePathStyle bool // Use path-style URLs (for MinIO)
}

