//go:build !duckdb_arrow

// Package duckplane is the DuckDB implementation of datagateway.DataPlane. It
// is CGO-only: build with CGO_ENABLED=1 and -tags duckdb_arrow. This file keeps
// the package non-empty for untagged builds (go vet ./...).
package duckplane
