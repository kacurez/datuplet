//go:build !duckdb_arrow

// This file is the stand-in main package when the binary is built WITHOUT
// the `duckdb_arrow` build tag: the DuckDB data plane (gateway/duckplane)
// is CGO-only. The image build always passes -tags=duckdb_arrow; this stub
// turns an untagged build into a clear error instead of a missing symbol.

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "datagateway: built without duckdb_arrow; the DuckDB data plane is unavailable")
	fmt.Fprintln(os.Stderr, "rebuild with: CGO_ENABLED=1 go build -tags=duckdb_arrow ./cmd/datagateway")
	os.Exit(20)
}
