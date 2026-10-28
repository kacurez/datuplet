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

	// Peek at writer state before CloseWriter: no external files yet.
	server.mu.Lock()
	ws, ok := server.writers[writerID]
	server.mu.Unlock()
	require.True(t, ok)
	assert.Empty(t, ws.externalFiles)

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

	// The CloseWriter path populates writer state (externalFiles, totalRows)
	// before finalizeAndDispatch, so it is set even though the patch failed.
	// The writer is still registered until Commit.
	server.mu.Lock()
	ws2, ok2 := server.writers[writerID]
	server.mu.Unlock()
	require.True(t, ok2, "writer must still be registered after a failed CloseWriter")
	assert.Equal(t, int64(100), ws2.totalRows, "totalRows should be set before patch attempt")
	assert.Len(t, ws2.externalFiles, 1, "externalFiles should be set before patch attempt")

	// finalizeAndDispatch claims the writer (sets ws.committed=true) before
	// attempting I/O, so even though patchParquetFieldIDs failed, the writer
	// is already marked committed. Commit will see it as claimed (not in
	// sweepList) and, in nil-pool test mode, report it as COMMITTED.
	commitResp, err := server.Commit(ctx, &pb.CommitRequest{})
	require.NoError(t, err)
	assert.NotNil(t, commitResp, "Commit response should not be nil")

	// Writers are cleared after Commit regardless of per-table outcome.
	server.mu.Lock()
	_, stillThere := server.writers[writerID]
	server.mu.Unlock()
	assert.False(t, stillThere, "writer should be removed after Commit")
}
