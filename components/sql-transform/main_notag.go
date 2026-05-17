//go:build !duckdb_arrow

// This file is the stand-in main package when the binary is built WITHOUT
// the `duckdb_arrow` build tag. The sql-transform component requires
// duckdb-go/v2's Arrow interface (NewArrowFromConn + Arrow.RegisterView),
// which is itself gated on `//go:build duckdb_arrow` (see duckdb-go/v2/arrow.go).
//
// The component's only production build path is `docker build` driven by
// components/sql-transform/Dockerfile, which always passes
// `-tags=duckdb_arrow`. This stub exists so that an inadvertent
// `go build .` from this directory fails with a clear, actionable error
// instead of "undefined: duckdb.Arrow".

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "sql-transform: built without -tags=duckdb_arrow; arrow_scan path is unavailable")
	fmt.Fprintln(os.Stderr, "rebuild with: go build -tags=duckdb_arrow .")
	os.Exit(20)
}
