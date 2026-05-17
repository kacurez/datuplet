package datagateway

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	dgschema "github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// patchParquetFieldIDs injects Iceberg field IDs into Parquet files that lack them.
//
// This is ONLY needed for external files (e.g., DuckDB COPY TO).
// Files written through DataGateway buffering already have correct field IDs.
//
// For each external file:
// 1. Check if field_id is present in schema
// 2. If missing, rewrite Parquet with field IDs from Iceberg schema
// 3. Preserve all row group data (rewrite with new schema only)
// 4. Atomically replace original file
//
// Fails fast if any file cannot be patched.
func (s *ServerV2) patchParquetFieldIDs(ctx context.Context, ws *writerState) (err error) {
	// Recover from panics to provide better error messages
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during Parquet patching: %v", r)
			log.Printf("ERROR: Parquet patching panic for %s.%s: %v", ws.bucket, ws.table, r)
		}
	}()

	if len(ws.externalFiles) == 0 {
		return nil // Nothing to patch
	}

	if ws.schema == nil {
		return fmt.Errorf("cannot patch Parquet files: schema is nil")
	}

	// Use per-writer backend if available, otherwise shared backend
	backendForPatching := s.backend
	if ws.writerBackend != nil {
		backendForPatching = ws.writerBackend
	}

	// Build field ID map from schema
	fieldIDMap := buildFieldIDMap(ws.schema)

	// Patch each file
	patchedCount := 0
	for _, fileInfo := range ws.externalFiles {
		needsPatching, err := checkParquetNeedsFieldIDs(ctx, backendForPatching, fileInfo.Path)
		if err != nil {
			return fmt.Errorf("failed to check file %s: %w", fileInfo.Path, err)
		}

		if needsPatching {
			if err := patchParquetFile(ctx, backendForPatching, fileInfo.Path, fieldIDMap); err != nil {
				return fmt.Errorf("failed to patch file %s: %w", fileInfo.Path, err)
			}
			patchedCount++
		}
	}

	if patchedCount > 0 {
		log.Printf("Patched %d/%d Parquet files with Iceberg field IDs for %s.%s",
			patchedCount, len(ws.externalFiles), ws.bucket, ws.table)
	}

	return nil
}

// buildFieldIDMap creates a mapping from column name to Iceberg field ID.
// Auto-assigns IDs (starting from 1) if not set in schema.
func buildFieldIDMap(schema *dgschema.Schema) map[string]int32 {
	fieldIDMap := make(map[string]int32)
	for i, col := range schema.Columns() {
		fieldID := col.FieldID
		if fieldID == 0 {
			// Auto-assign field IDs starting from 1 (Iceberg convention)
			fieldID = int32(i + 1)
		}
		fieldIDMap[col.Name] = fieldID
	}
	return fieldIDMap
}

// checkParquetNeedsFieldIDs checks if a Parquet file has Iceberg field IDs.
// Returns true if field IDs are missing and patching is needed.
func checkParquetNeedsFieldIDs(ctx context.Context, storageBackend backend.StorageBackend, filePath string) (bool, error) {
	// Download file
	data, err := storageBackend.GetObject(ctx, filePath)
	if err != nil {
		return false, fmt.Errorf("failed to download file: %w", err)
	}

	// Write to temp file for Parquet reading
	tmpFile, err := os.CreateTemp("", "parquet-check-*.parquet")
	if err != nil {
		return false, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return false, fmt.Errorf("failed to write temp file: %w", err)
	}
	tmpFile.Close()

	// Open Parquet file and check for field IDs
	parquetReader, err := file.OpenParquetFile(tmpPath, false)
	if err != nil {
		return false, fmt.Errorf("failed to read Parquet metadata: %w", err)
	}
	defer parquetReader.Close()

	// Get schema from metadata
	metadata := parquetReader.MetaData()
	schema := metadata.Schema

	// Check each column for field_id
	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		node := col.SchemaNode()

		// Check if field_id is set (-1 means not set in Arrow)
		if node.FieldID() == -1 {
			return true, nil
		}
	}

	// All columns have field IDs
	return false, nil
}

// patchParquetFile rewrites a Parquet file with Iceberg field IDs injected.
// This reads the file, rewrites it with updated schema (including field IDs), and uploads.
func patchParquetFile(ctx context.Context, storageBackend backend.StorageBackend, filePath string, fieldIDMap map[string]int32) error {
	// Step 1: Download original file
	originalData, err := storageBackend.GetObject(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to download original file: %w", err)
	}

	// Step 2: Write to temp file
	tmpOriginal, err := os.CreateTemp("", "parquet-original-*.parquet")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpOriginalPath := tmpOriginal.Name()
	defer os.Remove(tmpOriginalPath)

	if _, err := tmpOriginal.Write(originalData); err != nil {
		tmpOriginal.Close()
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	tmpOriginal.Close()

	// Step 3: Read original Parquet file
	srcReader, err := file.OpenParquetFile(tmpOriginalPath, false)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcReader.Close()

	srcMetadata := srcReader.MetaData()
	srcSchema := srcMetadata.Schema

	// Step 4: Validate schema matches field ID map
	if err := validateSchemaMatchesFieldIDs(srcSchema, fieldIDMap); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	// Step 5: Create temp file for the patched output.
	tmpPatched, err := os.CreateTemp("", "parquet-patched-*.parquet")
	if err != nil {
		return fmt.Errorf("failed to create patched temp file: %w", err)
	}
	tmpPatchedPath := tmpPatched.Name()
	tmpPatched.Close()
	defer os.Remove(tmpPatchedPath)

	// Step 6: Rewrite Parquet with new schema (including field IDs)
	if err := rewriteParquetWithFieldIDs(ctx, tmpOriginalPath, tmpPatchedPath, fieldIDMap); err != nil {
		return fmt.Errorf("failed to rewrite Parquet: %w", err)
	}

	// Step 7: Upload patched file (atomic replacement)
	patchedData, err := os.ReadFile(tmpPatchedPath)
	if err != nil {
		return fmt.Errorf("failed to read patched file: %w", err)
	}

	if err := storageBackend.PutObject(ctx, filePath, patchedData); err != nil {
		return fmt.Errorf("failed to upload patched file: %w", err)
	}

	return nil
}

// validateSchemaMatchesFieldIDs ensures Parquet schema columns match the field ID map.
func validateSchemaMatchesFieldIDs(parquetSchema *schema.Schema, fieldIDMap map[string]int32) error {
	numCols := parquetSchema.NumColumns()
	if numCols != len(fieldIDMap) {
		return fmt.Errorf("column count mismatch: Parquet has %d, field map has %d",
			numCols, len(fieldIDMap))
	}

	for i := 0; i < numCols; i++ {
		col := parquetSchema.Column(i)
		colName := col.Name()

		if _, exists := fieldIDMap[colName]; !exists {
			return fmt.Errorf("column %s not found in field ID map", colName)
		}
	}

	return nil
}

// rewriteParquetWithFieldIDs reads a Parquet file at srcPath, rewrites it with
// field IDs injected into the Arrow schema, and writes the result to dstPath.
//
// Previously this function contained a buggy outer loop that called
// GetRecordReader once per row group — each call returned a reader covering ALL
// row groups, so data was duplicated N times (N = number of row groups). The fix
// delegates to backend.StampParquetStream which uses a single GetRecordReader
// call and streams all batches in one pass, eliminating the duplication.
func rewriteParquetWithFieldIDs(ctx context.Context, srcPath, dstPath string, fieldIDMap map[string]int32) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer dstFile.Close()

	// Use writeOnly to prevent StampParquetStream's fw.Close() from closing
	// dstFile under us — we hold the file lifecycle here (defer dstFile.Close
	// above), same pattern as LocalCopier.copyOneStamped.
	return backend.StampParquetStream(ctx, srcFile, writeOnly{dstFile}, fieldIDMap)
}

// writeOnly wraps an io.Writer to strip io.Closer so that pqarrow's
// FileWriter.Close() does not close the underlying *os.File prematurely.
// The caller retains ownership of the file lifecycle.
type writeOnly struct {
	io.Writer
}

// injectFieldIDsIntoArrowSchema has moved to
// pkg/datagateway/backend/parquet_stamp.go (exported as
// backend.InjectFieldIDsIntoArrowSchema) so the S3Copier streaming-stamp
// path can reuse the same field-id metadata logic.
