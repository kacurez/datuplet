//go:build ignore

// gen_duckdb writes shredded_duckdb.parquet: id int and v VARIANT, written by
// DuckDB 2.0's COPY ... TO parquet, which shreds VARIANT (typed_value) by
// default. It needs duckdb-go v2.20000.0 (DuckDB v2.0.0), the version
// components/queryengine pins, which the root module does not require, so
// run it from a throwaway module:
//
//	mkdir /tmp/duckgen && cp pkg/datagateway/testdata/variant/gen_duckdb.go /tmp/duckgen/main.go
//	cp components/queryengine/go.sum /tmp/duckgen/
//	cd /tmp/duckgen && printf 'module duckgen\n\ngo 1.25\n\nrequire github.com/duckdb/duckdb-go/v2 v2.20000.0\n' > go.mod
//	sed -i.bak 's|^//go:build ignore||' main.go && go mod tidy
//	go run . <repo>/pkg/datagateway/testdata/variant/shredded_duckdb.parquet
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	var ver string
	if err := db.QueryRow("SELECT version()").Scan(&ver); err != nil {
		log.Fatal(err)
	}
	fmt.Println("duckdb", ver)
	q := fmt.Sprintf(`COPY (
  SELECT id, v::VARIANT AS v FROM (VALUES
    (1, '{"a":1,"b":"x"}'::JSON),
    (2, '{"a":2,"b":"y"}'::JSON),
    (3, '{"a":9007199254740993,"b":"z","c":[1,2]}'::JSON),
    (4, NULL),
    (5, '"hello"'::JSON)
  ) t(id, v) ORDER BY id
) TO '%s' (FORMAT parquet)`, os.Args[1])
	if _, err := db.Exec(q); err != nil {
		log.Fatal(err)
	}
}
