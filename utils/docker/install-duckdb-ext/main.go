//go:build duckdb_arrow

// install-duckdb-ext is a build-time-only throwaway program that opens an
// embedded DuckDB and installs the iceberg and httpfs extensions.  It is run
// during the Docker image build (builder stage) while the network is available
// and the local FS is writable.  The resulting ~/.duckdb/extensions/ directory
// is COPY-ed into the runtime image so that `INSTALL iceberg` / `INSTALL httpfs`
// inside the query-worker resolve the locally-cached copy — treated as a no-op
// network download — instead of hitting the network at runtime.
//
// This binary is NEVER shipped in the final image; it lives only in the builder
// stage.
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	for _, stmt := range []string{
		"INSTALL iceberg",
		"LOAD iceberg",
		"INSTALL httpfs",
		"LOAD httpfs",
	} {
		if _, err := db.Exec(stmt); err != nil {
			log.Fatalf("%s: %v", stmt, err)
		}
		fmt.Printf("OK: %s\n", stmt)
	}
	fmt.Println("DuckDB extensions baked successfully.")
}
