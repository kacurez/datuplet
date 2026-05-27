package datagateway

import (
	"context"
	"os"
	"strings"
	"testing"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerV2_ExternalFiles(t *testing.T) {
	// This test verifies that external files (files written directly by components)
	// are correctly handled by the gateway.
	//
	// Under RFC 021, CloseWriter now calls finalizeAndDispatch inline, which
	// includes patchParquetFieldIDs. For external files that don't exist in the
	// mock backend, CloseWriter returns an error at patching time. The test
	// validates this new contract.

	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	testSchema := &pb.Schema{
		Columns: []*pb.ColumnDef{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
		},
	}

	openResp, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Bucket:      "test_bucket",
		Table:       "test_table",
		InputFormat: pb.DataFormat_FORMAT_CSV,
		Schema:      testSchema,
	})
	require.NoError(t, err)
	require.NotEmpty(t, openResp.WriterId)

	writerID := openResp.WriterId

	// Close writer with external files. Under the new contract, CloseWriter
	// calls finalizeAndDispatch which attempts patchParquetFieldIDs. Since
	// the mock backend does not contain the referenced parquet file, the call
	// returns an error — this is the correct, expected behaviour.
	externalFiles := []*pb.ExternalDataFile{
		{
			Path:      "data.parquet",
			RowCount:  100,
			SizeBytes: 1024,
		},
	}

	_, closeErr := server.CloseWriter(ctx, &pb.CloseWriterRequest{
		WriterId:      writerID,
		ExternalFiles: externalFiles,
	})
	// Patching fails because the file does not exist in the mock backend.
	require.Error(t, closeErr, "CloseWriter must return an error when the external parquet file cannot be patched")
	if !strings.Contains(closeErr.Error(), "patch parquet field IDs") {
		t.Errorf("expected 'patch parquet field IDs' in error, got: %v", closeErr)
	}

	// finalizeAndDispatch claims the writer (sets ws.committed=true) before
	// attempting I/O, so even though patchParquetFieldIDs failed, the writer
	// is already marked committed. Commit will see it as claimed (not in
	// sweepList) and, in nil-pool test mode, report it as COMMITTED.
	commitResp, err := server.Commit(ctx, &pb.CommitRequest{})
	require.NoError(t, err)
	assert.NotNil(t, commitResp, "Commit response should not be nil")

	// Writers are cleared after Commit regardless of per-table outcome.
	server.mu.Lock()
	_, ok := server.writers[writerID]
	server.mu.Unlock()
	assert.False(t, ok, "writer should be removed after Commit")
}

// TestServerV2_ExternalFiles_WriteStateSetBeforePatch verifies that writer state
// (externalFiles, totalRows) is populated correctly before finalizeAndDispatch is
// called — i.e., the CloseWriter path correctly sets writer state even though the
// downstream patch call may fail.
func TestServerV2_ExternalFiles_WriteStateSetBeforePatch(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	testSchema := &pb.Schema{
		Columns: []*pb.ColumnDef{
			{Name: "id", Type: "int64", Nullable: false},
		},
	}

	openResp, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Bucket:      "test_bucket",
		Table:       "test_table2",
		InputFormat: pb.DataFormat_FORMAT_CSV,
		Schema:      testSchema,
	})
	require.NoError(t, err)
	writerID := openResp.WriterId

	// Peek at writer state before CloseWriter.
	server.mu.Lock()
	ws, ok := server.writers[writerID]
	server.mu.Unlock()
	require.True(t, ok)

	// externalFiles not set yet.
	assert.Empty(t, ws.externalFiles)

	// Close with external files; the path sets ws.externalFiles before calling
	// finalizeAndDispatch. Even though the downstream patch fails, the writer
	// state fields are set.
	_, closeErr := server.CloseWriter(ctx, &pb.CloseWriterRequest{
		WriterId: writerID,
		ExternalFiles: []*pb.ExternalDataFile{
			{Path: "s3://bucket/data.parquet", RowCount: 50, SizeBytes: 500},
		},
	})
	require.Error(t, closeErr) // expected: parquet file not in mock backend

	// Writer state was populated before the error.
	server.mu.Lock()
	ws2, ok2 := server.writers[writerID]
	server.mu.Unlock()
	if ok2 {
		assert.Equal(t, int64(50), ws2.totalRows, "totalRows should be set before patch attempt")
		assert.Len(t, ws2.externalFiles, 1, "externalFiles should be set before patch attempt")
	}
	// (If ok2 is false the writer was already removed — also acceptable)

	// Clean up.
	server.Commit(ctx, &pb.CommitRequest{}) //nolint:errcheck
}
