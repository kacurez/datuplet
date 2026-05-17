//go:build ignore

// smoke.go is a throwaway verification tool for the warehouse fixtures.
// It opens public/simple/metadata/v11.metadata.json via iceberg-go and
// prints the resolved schema + current snapshot. A successful run proves
// the fixtures are valid Iceberg tables that iceberg-go can parse.
//
// Usage (from the repo root):
//
//	go run ./pkg/pipelineapi/storage/testdata/smoke.go

package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"runtime"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

func main() {
	log.SetFlags(0)

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("runtime.Caller failed")
	}
	testdataDir := filepath.Dir(thisFile)

	metaPath := filepath.Join(
		testdataDir,
		"warehouse",
		"orgs", "myorg",
		"projects", "00000000-0000-0000-0000-000000000002",
		"tables",
		"public", "simple",
		"metadata", "v11.metadata.json",
	)
	loc := "file://" + metaPath

	ctx := context.Background()
	tbl, err := table.NewFromLocation(
		ctx,
		table.Identifier{"simple"},
		loc,
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LoadFS(ctx, nil, loc)
		},
		nil,
	)
	if err != nil {
		log.Fatalf("NewFromLocation(%s): %v", loc, err)
	}

	fmt.Printf("schema:\n%s\n", tbl.Schema())
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		log.Fatal("CurrentSnapshot() returned nil — expected a snapshot on v1+")
	}
	fmt.Printf("snapshot: id=%d seq=%d manifest-list=%s\n",
		snap.SnapshotID, snap.SequenceNumber, snap.ManifestList)
	fmt.Printf("metadata-log entries: %d\n", len(metadataLog(tbl)))
	fmt.Printf("properties: %v\n", tbl.Metadata().Properties())
	fmt.Println("OK")
}

// metadataLog is a tiny helper that iterates PreviousFiles() into a slice
// so we can report a count without pulling in the iter package.
func metadataLog(t *table.Table) []table.MetadataLogEntry {
	var out []table.MetadataLogEntry
	for e := range t.Metadata().PreviousFiles() {
		out = append(out, e)
	}
	return out
}
