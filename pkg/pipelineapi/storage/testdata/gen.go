//go:build ignore

// gen.go regenerates the committed Iceberg fixtures under
// ./warehouse/orgs/myorg/projects/.../tables/ for human inspection.
// Tests do NOT use this — they call testdata.GenerateAll(t, t.TempDir())
// directly via the regular Go package in testdata.go.
//
// Usage (from the repo root):
//
//	go run ./pkg/pipelineapi/storage/testdata/gen.go
//
// Iceberg embeds absolute paths in metadata.json ("location" + data-file
// paths) and in the Avro manifest/manifest-list files, so the committed
// fixtures are only guaranteed to resolve on the exact checkout that
// generated them. That's fine for human inspection and for smoke.go;
// tests regenerate into t.TempDir() for portability.

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/datuplet/datuplet/pkg/pipelineapi/storage/testdata"
)

func main() {
	log.SetFlags(0)

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("runtime.Caller failed")
	}
	testdataDir := filepath.Dir(thisFile)
	warehouseDir := filepath.Join(testdataDir, "warehouse")

	if err := os.RemoveAll(warehouseDir); err != nil {
		log.Fatalf("remove warehouse: %v", err)
	}
	if err := testdata.GenerateAllErr(warehouseDir); err != nil {
		log.Fatalf("generate fixtures: %v", err)
	}
	fmt.Printf("wrote fixtures under %s\n", warehouseDir)
}
