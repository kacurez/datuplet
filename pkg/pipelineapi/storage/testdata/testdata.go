// Package testdata is test-support code for pkg/pipelineapi/storage.
//
// It exposes helpers that (re)generate the Iceberg warehouse fixtures
// used by the walker / handler / serializer tests into an arbitrary
// directory — typically t.TempDir(). The reason the helpers take a path
// instead of relying on the committed testdata/warehouse/ tree is that
// Iceberg metadata.json bakes absolute file:// paths into "location",
// "metadata-log" entries, snapshot "manifest-list" URIs, and per-file
// data paths. The committed tree therefore only resolves on the exact
// checkout that generated it. Regenerating into t.TempDir() makes the
// fixtures portable across contributor machines and CI.
//
// The gen.go sibling file keeps //go:build ignore and wraps these
// helpers with a main() so contributors can still `go run` a one-off
// regeneration of the committed tree for human inspection.
//
// Rule: production code never imports this package.
package testdata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

const (
	fixtureProjectID = "00000000-0000-0000-0000-000000000002"
	fixtureOrgID     = "myorg"
	fixtureSchema    = "public"
)

// ProjectRoot returns the relative path segment under warehouse/ that
// tests should use to reach the tables/ directory for the fixture
// project. Tests compose absolute paths as:
//
//	filepath.Join(warehouse, testdata.ProjectRoot(), "public", "simple")
func ProjectRoot() string {
	return filepath.Join("orgs", fixtureOrgID, "projects", fixtureProjectID, "tables")
}

// GenerateAll writes the full fixture tree (simple, orphan, empty)
// into warehouse. warehouse must be absolute — Iceberg bakes absolute
// paths into metadata.json.
func GenerateAll(t testing.TB, warehouse string) {
	t.Helper()
	if err := GenerateAllErr(warehouse); err != nil {
		t.Fatalf("testdata.GenerateAll: %v", err)
	}
}


// GenerateAllErr is the error-returning counterpart to GenerateAll,
// intended for use by the //go:build ignore gen.go tool so it can
// reuse the fixture-writing logic without depending on testing.TB
// (which has unexported methods).
func GenerateAllErr(warehouse string) error {
	if err := checkAbsWarehouse(warehouse); err != nil {
		return err
	}
	if err := GenerateSimpleErr(warehouse); err != nil {
		return err
	}
	if err := GenerateOrphanErr(warehouse); err != nil {
		return err
	}
	return GenerateEmptyErr(warehouse)
}

// GenerateSimpleErr is the error-returning counterpart to GenerateSimple.
func GenerateSimpleErr(warehouse string) error {
	if err := checkAbsWarehouse(warehouse); err != nil {
		return err
	}
	return writeSimple(filepath.Join(warehouse, ProjectRoot(), fixtureSchema, "simple"))
}

// GenerateOrphanErr is the error-returning counterpart to GenerateOrphan.
func GenerateOrphanErr(warehouse string) error {
	if err := checkAbsWarehouse(warehouse); err != nil {
		return err
	}
	return writeOrphan(filepath.Join(warehouse, ProjectRoot(), fixtureSchema, "orphan"))
}

// GenerateEmptyErr is the error-returning counterpart to GenerateEmpty.
func GenerateEmptyErr(warehouse string) error {
	if err := checkAbsWarehouse(warehouse); err != nil {
		return err
	}
	return writeEmpty(filepath.Join(warehouse, ProjectRoot(), fixtureSchema, "empty"))
}

// checkAbsWarehouse guards every public entrypoint against a relative
// warehouse path. Iceberg bakes absolute file:// URIs into metadata.json
// (location, metadata-log, snapshot manifest-list, per-file data paths),
// so a relative path would silently produce malformed fixtures that
// iceberg-go fails on later in non-obvious ways.
func checkAbsWarehouse(warehouse string) error {
	if !filepath.IsAbs(warehouse) {
		return fmt.Errorf("testdata: warehouse path must be absolute, got %q", warehouse)
	}
	return nil
}

// ----- Implementation below. Mirrors gen.go but returns errors
// instead of calling log.Fatal so the testing.TB wrappers above can
// report failures cleanly.

func writeSimple(tableDir string) error {
	metadataDir := filepath.Join(tableDir, "metadata")
	dataDir := filepath.Join(tableDir, "data")
	if err := os.MkdirAll(metadataDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}

	parquetName := "part-00001.parquet"
	parquetPath := filepath.Join(dataDir, parquetName)
	rowCount, sizeBytes, err := writeParquet(parquetPath)
	if err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	iceSchema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	partSpec := iceberg.UnpartitionedSpec

	parquetURI := "file://" + parquetPath
	tableLocation := "file://" + tableDir
	metadataBase := "file://" + metadataDir + "/"

	now := time.Now().UTC()
	snapshotID := now.UnixNano() / int64(time.Millisecond)

	dfBuilder, err := iceberg.NewDataFileBuilder(
		*partSpec,
		iceberg.EntryContentData,
		parquetURI,
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		rowCount,
		sizeBytes,
	)
	if err != nil {
		return fmt.Errorf("data file builder: %w", err)
	}
	dataFile := dfBuilder.Build()

	entry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED,
		&snapshotID,
		nil,
		nil,
		dataFile,
	)

	manifestFileName := fmt.Sprintf("%s-m0.avro", uuid.New().String())
	manifestURI := metadataBase + manifestFileName
	var manifestBuf bytes.Buffer
	manifestFile, err := iceberg.WriteManifest(
		manifestURI,
		&manifestBuf,
		2,
		*partSpec,
		iceSchema,
		snapshotID,
		[]iceberg.ManifestEntry{entry},
	)
	if err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(metadataDir, manifestFileName), manifestBuf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write manifest file: %w", err)
	}

	manifestListName := fmt.Sprintf("snap-%d-%s.avro", snapshotID, uuid.New().String()[:8])
	manifestListURI := metadataBase + manifestListName
	seqNum := int64(1)
	var manifestListBuf bytes.Buffer
	if err := iceberg.WriteManifestList(
		2,
		&manifestListBuf,
		snapshotID,
		nil,
		&seqNum,
		0,
		[]iceberg.ManifestFile{manifestFile},
	); err != nil {
		return fmt.Errorf("write manifest list: %w", err)
	}
	if err := os.WriteFile(filepath.Join(metadataDir, manifestListName), manifestListBuf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write manifest list file: %w", err)
	}

	v1Name := "v1.metadata.json"
	v1URI := metadataBase + v1Name

	base, err := table.NewMetadataWithUUID(
		iceSchema,
		partSpec,
		table.UnsortedSortOrder,
		tableLocation,
		iceberg.Properties{},
		uuid.New(),
	)
	if err != nil {
		return fmt.Errorf("new metadata: %w", err)
	}

	snapshot := table.Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: seqNum,
		TimestampMs:    now.UnixMilli(),
		ManifestList:   manifestListURI,
		Summary: &table.Summary{
			Operation: table.OpAppend,
			Properties: map[string]string{
				"added-data-files": "1",
				"added-records":    fmt.Sprintf("%d", rowCount),
				"added-files-size": fmt.Sprintf("%d", sizeBytes),
				"total-data-files": "1",
				"total-records":    fmt.Sprintf("%d", rowCount),
				"total-files-size": fmt.Sprintf("%d", sizeBytes),
			},
		},
		SchemaID: intPtr(base.CurrentSchema().ID),
	}

	// Include a default name mapping so iceberg-go's Parquet reader
	// can link plain Parquet columns (written without field_id metadata
	// by the pqarrow writer below) to the Iceberg field IDs at scan
	// time. Without this the data scan fails with "no leaf column
	// readers matched col indices"; metadata-only reads are unaffected.
	nameMappingJSON := `[{"field-id":1,"names":["id"]},{"field-id":2,"names":["name"]}]`

	v1Meta, err := table.UpdateTableMetadata(
		base,
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{
				"schema.name-mapping.default": nameMappingJSON,
			}),
			table.NewAddSnapshotUpdate(&snapshot),
			table.NewSetSnapshotRefUpdate("main", snapshotID, table.BranchRef, 0, 0, 0),
		},
		v1URI,
	)
	if err != nil {
		return fmt.Errorf("update v1: %w", err)
	}
	if err := writeMetadataJSON(filepath.Join(metadataDir, v1Name), v1Meta); err != nil {
		return err
	}

	prev := v1Meta
	for i := 2; i <= 11; i++ {
		name := fmt.Sprintf("v%d.metadata.json", i)
		locURI := metadataBase + name
		updates := []table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{
				"datuplet.testdata.version": fmt.Sprintf("%d", i),
			}),
		}
		next, err := table.UpdateTableMetadata(prev, updates, locURI)
		if err != nil {
			return fmt.Errorf("update %s: %w", name, err)
		}
		if err := writeMetadataJSON(filepath.Join(metadataDir, name), next); err != nil {
			return err
		}
		prev = next
	}

	if err := os.WriteFile(filepath.Join(metadataDir, "version-hint.text"), []byte("11"), 0o644); err != nil {
		return fmt.Errorf("write version-hint: %w", err)
	}
	return nil
}

func writeOrphan(tableDir string) error {
	metadataDir := filepath.Join(tableDir, "metadata")
	if err := os.MkdirAll(metadataDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(
		filepath.Join(metadataDir, "v99.metadata.json"),
		[]byte("{not json"),
		0o644,
	)
}

func writeEmpty(tableDir string) error {
	metadataDir := filepath.Join(tableDir, "metadata")
	if err := os.MkdirAll(metadataDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(metadataDir, ".gitkeep"), []byte(""), 0o644)
}

func writeMetadataJSON(path string, meta table.Metadata) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

// writeParquet writes a 3-row parquet file with (id int64, name string)
// and returns (rowCount, fileSizeBytes).
func writeParquet(path string) (int64, int64, error) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	idBuilder := array.NewInt64Builder(alloc)
	defer idBuilder.Release()
	nameBuilder := array.NewStringBuilder(alloc)
	defer nameBuilder.Release()

	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	nameBuilder.AppendValues([]string{"a", "b", "c"}, nil)

	idArr := idBuilder.NewArray()
	defer idArr.Release()
	nameArr := nameBuilder.NewArray()
	defer nameArr.Release()

	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 3)
	defer rec.Release()

	out, err := os.Create(path)
	if err != nil {
		return 0, 0, err
	}
	defer out.Close()

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithAllocator(alloc),
		pqarrow.WithStoreSchema(),
	)
	pw, err := pqarrow.NewFileWriter(schema, out, writerProps, arrowProps)
	if err != nil {
		return 0, 0, err
	}
	if err := pw.Write(rec); err != nil {
		_ = pw.Close()
		return 0, 0, err
	}
	if err := pw.Close(); err != nil {
		return 0, 0, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	return 3, fi.Size(), nil
}

func intPtr(v int) *int { return &v }
