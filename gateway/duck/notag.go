//go:build !duckdb_arrow

// Package duck is the gateway's thin layer over duckdb-go-bindings. It is
// CGO-only: build with CGO_ENABLED=1 and -tags duckdb_arrow. This file keeps
// the package non-empty for untagged builds (go vet ./...).
package duck
