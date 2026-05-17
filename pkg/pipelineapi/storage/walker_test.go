package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/datuplet/datuplet/pkg/lib/datalake"
	"github.com/datuplet/datuplet/pkg/pipelineapi/storage/testdata"
)

const fixturePID = "00000000-0000-0000-0000-000000000002"
const fixtureOrg = "myorg"

func genWarehouse(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	testdata.GenerateAll(t, filepath.Join(dir, "warehouse"))
	return filepath.Join(dir, "warehouse")
}

func TestResolveCurrentMetadata_VersionHint(t *testing.T) {
	warehouse := genWarehouse(t)
	dl := datalake.NewFilesystemDataLake(warehouse)
	tblPrefix := filepath.Join(testdata.ProjectRoot(), "public", "simple")

	got, err := ResolveCurrentMetadata(context.Background(), dl, "file://"+warehouse, tblPrefix, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "file://" + filepath.Join(warehouse, testdata.ProjectRoot(), "public", "simple", "metadata", "v11.metadata.json")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestResolveCurrentMetadata_NumericFallback_SkipsOrphan(t *testing.T) {
	// Build a one-off table dir: no version-hint.text, two metadata files
	// (v11 good — copied from the simple fixture — and v99 malformed).
	// Walker should fall back to numeric sort and pick v11 after
	// load-verifying each candidate.
	warehouse := genWarehouse(t)
	srcGood := filepath.Join(warehouse, testdata.ProjectRoot(), "public", "simple", "metadata", "v11.metadata.json")
	goodBytes, err := os.ReadFile(srcGood)
	if err != nil {
		t.Fatal(err)
	}

	// Place the "tbl" directly under a fresh warehouse root so the
	// tablePrefix is a simple "tbl" segment; metadata/ sits beneath it.
	dir := t.TempDir()
	tableSub := filepath.Join(dir, "tbl")
	metaDir := filepath.Join(tableSub, "metadata")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(metaDir, "v11.metadata.json"), goodBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(metaDir, "v99.metadata.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	// NOTE: v11's metadata bakes in an absolute location path of the
	// *original* warehouse, so iceberg-go may not be able to open data
	// files — but the walker's `loadable` check should only validate
	// that metadata.json itself parses, not that data files exist.

	dl := datalake.NewFilesystemDataLake(dir)
	got, err := ResolveCurrentMetadata(context.Background(), dl, "file://"+dir, "tbl", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "file://" + filepath.Join(metaDir, "v11.metadata.json")
	if got != want {
		t.Errorf("expected fallback to v11 (skipping orphan v99), got %q", got)
	}
}

func TestResolveCurrentMetadata_EmptyDir(t *testing.T) {
	warehouse := genWarehouse(t)
	dl := datalake.NewFilesystemDataLake(warehouse)
	tblPrefix := filepath.Join(testdata.ProjectRoot(), "public", "empty")

	_, err := ResolveCurrentMetadata(context.Background(), dl, "file://"+warehouse, tblPrefix, nil)
	if err == nil {
		t.Fatal("want error, got nil")
	}
}

func TestListTables(t *testing.T) {
	warehouse := genWarehouse(t)
	dl := datalake.NewFilesystemDataLake(warehouse)

	got, err := ListTables(context.Background(), dl, "file://"+warehouse, fixtureOrg, fixturePID, nil)
	if err != nil {
		t.Fatal(err)
	}
	// public/simple should be present; public/empty (no metadata) and
	// public/orphan (only malformed metadata) should be omitted.
	if len(got) != 1 {
		t.Fatalf("expected 1 table, got %d: %+v", len(got), got)
	}
	if got[0].Namespace != "public" || got[0].Name != "simple" {
		t.Errorf("unexpected table %+v", got[0])
	}
}
