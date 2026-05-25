//go:build integration

package backend

// Integration test for the GCS range-read adapter against a real
// fake-gcs-server container (the same harness used by
// gcs_integration_test.go).
//
// Run with:
//   go test -v -tags=integration ./pkg/datagateway/backend/... -run TestGCSRangeReader
//
// Requires Docker. Excluded from the default `go test ./...` run by the
// `integration` build tag.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// TestGCSRangeReaderFooterFirst exercises the Parquet footer-first read
// pattern against a real fake-gcs-server. Write a 1 MiB blob, then read
// the last 8 bytes via the *gcsBackend.NewRangeReader path. EOF on the
// tail read is expected and acceptable; what matters is that the bytes
// match.
func TestGCSRangeReaderFooterFirst(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-rangeread")
	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()

	ctx := context.Background()
	blob := make([]byte, 1<<20)
	for i := range blob {
		blob[i] = byte(i % 256)
	}
	if err := be.PutObject(ctx, "big.bin", blob); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	ra, err := be.NewRangeReader(ctx, "big.bin")
	if err != nil {
		t.Fatalf("NewRangeReader: %v", err)
	}
	if got := ra.Size(); got != int64(len(blob)) {
		t.Errorf("Size() = %d, want %d", got, len(blob))
	}

	tail := make([]byte, 8)
	n, err := ra.ReadAt(tail, int64(len(blob))-8)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 8 {
		t.Fatalf("ReadAt = %d, want 8", n)
	}
	for i := 0; i < 8; i++ {
		want := byte((len(blob) - 8 + i) % 256)
		if tail[i] != want {
			t.Fatalf("tail[%d] = %d, want %d", i, tail[i], want)
		}
	}
}

// TestGCSBackendOpenStreamingArrowReader_ByteBoundChunks exercises the GCS
// streaming-reader path end-to-end against fake-gcs. Writes a wide-row parquet
// blob (4096 × 8 KiB strings = ~32 MiB in one row group), then reads via
// OpenStreamingArrowReader and asserts every emitted DataChunk is bounded by
// targetChunkBytes. Without this method, sql-transform's gRPC ReadChunk
// against a GCS-backed table fell through to gcsReader.readParquetFile (whole
// file io.Copy → bytes.Buffer) and busted the SDK's 64 MiB MaxRecvMsgSize.
func TestGCSBackendOpenStreamingArrowReader_ByteBoundChunks(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-streaming-arrow")
	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()
	ctx := context.Background()

	// Build a wide-row parquet on disk, upload to fake-gcs.
	dir := t.TempDir()
	localFile := filepath.Join(dir, "wide.parquet")
	const totalRows = 4096
	const rowBytes = 8 * 1024
	payload := bytes.Repeat([]byte{'x'}, rowBytes)
	values := make([]string, totalRows)
	for i := range values {
		values[i] = string(payload)
	}
	writeIntegrationWideStringParquet(t, localFile, values, totalRows)
	blob, err := os.ReadFile(localFile)
	if err != nil {
		t.Fatalf("read local parquet: %v", err)
	}
	if err := be.PutObject(ctx, "wide.parquet", blob); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Same-cap path the gateway uses post-lakekeeper.
	schema := &SchemaInfo{Columns: []ColumnInfo{{Name: "blob", Type: "string", Nullable: false}}}
	r, err := be.OpenStreamingArrowReader(ctx, []string{"wide.parquet"}, schema)
	if err != nil {
		t.Fatalf("OpenStreamingArrowReader: %v", err)
	}
	defer r.Close()

	var totalSeen int64
	var maxChunkBytes int
	chunkCount := 0
	for {
		chunk, err := r.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		if len(chunk.Data) > targetChunkBytes {
			t.Errorf("chunk #%d carries %d bytes; want <= %d (targetChunkBytes cap)",
				chunkCount, len(chunk.Data), targetChunkBytes)
		}
		if len(chunk.Data) > maxChunkBytes {
			maxChunkBytes = len(chunk.Data)
		}
		totalSeen += chunk.RowsInChunk
		chunkCount++
	}
	if totalSeen != totalRows {
		t.Errorf("totalSeen = %d, want %d", totalSeen, totalRows)
	}
	// 32 MiB / 16 MiB cap = 2 — splitting must engage, otherwise the GCS
	// streaming path silently regresses to whole-record emission.
	if chunkCount < 2 {
		t.Errorf("chunkCount = %d, want >= 2 (proves byte-bound slicing engaged over GCS range reads); maxChunkBytes=%d", chunkCount, maxChunkBytes)
	}
}

// writeIntegrationWideStringParquet is the integration-test fixture writer.
// Separate from parquet_arrow_reader_test.go's helper because integration
// tests are in a different build-tag scope (//go:build integration).
func writeIntegrationWideStringParquet(t *testing.T, path string, values []string, rowsPerGroup int) {
	t.Helper()
	pool := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "blob", Type: arrow.BinaryTypes.String, Nullable: false}}, nil)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create fixture: %v", err)
	}
	defer f.Close()
	props := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(rowsPerGroup)))
	pqw, err := pqarrow.NewFileWriter(arrowSchema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("pqarrow writer: %v", err)
	}
	defer pqw.Close()
	builder := array.NewStringBuilder(pool)
	for _, v := range values {
		builder.Append(v)
	}
	arr := builder.NewArray()
	defer arr.Release()
	rec := array.NewRecord(arrowSchema, []arrow.Array{arr}, int64(len(values)))
	defer rec.Release()
	if err := pqw.Write(rec); err != nil {
		t.Fatalf("write record: %v", err)
	}
}
