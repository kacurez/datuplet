package datagateway

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway/buffer"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// setCommitPoolForTest injects a CommitPool for testing purposes.
// Must only be used in test code.
func (s *ServerV2) setCommitPoolForTest(p *CommitPool) { s.commitPool = p }

// TestCloseWriter_DispatchesToPool verifies that CloseWriter dispatches exactly
// one commit per writer to the pool, and that the Commit barrier returns
// STATUS_COMMITTED for that writer.
func TestCloseWriter_DispatchesToPool(t *testing.T) {
	server, _ := createTestServerV2WithOutputTable(t)
	ctx := context.Background()

	var commitCalled int
	pool := NewCommitPool(CommitPoolConfig{
		Workers:      2,
		MaxQueueSize: 16,
		CatalogFn:    func(context.Context) (catalog.Catalog, error) { return nil, nil },
		CommitFn: func(_ context.Context, _ catalog.Catalog, _ icebergtable.Identifier,
			paths []string, _ icebergjob.WriteMode, _ string) (*icebergjob.CommitResult, error) {
			commitCalled++
			return &icebergjob.CommitResult{DataFilesAdded: len(paths)}, nil
		},
	})
	server.setCommitPoolForTest(pool)

	// Open, write, close.
	openResp, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Table:       "orders",
		Bucket:      "raw",
		InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	if _, err := server.WriteChunk(ctx, &pb.WriteChunkRequest{
		WriterId: openResp.WriterId,
		Data:     []byte("id,name\n1,Alice\n"),
	}); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	if _, err := server.CloseWriter(ctx, &pb.CloseWriterRequest{
		WriterId: openResp.WriterId,
	}); err != nil {
		t.Fatalf("CloseWriter: %v", err)
	}

	// Exactly one commit must have been dispatched.
	// (The pool runs it asynchronously; Wait will drain it.)

	// Commit is the barrier.
	commitResp, err := server.Commit(ctx, &pb.CommitRequest{})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !commitResp.Success {
		t.Errorf("Commit.Success = false")
	}

	// Verify exactly one commit was dispatched and executed.
	if commitCalled != 1 {
		t.Errorf("commitCalled = %d, want 1", commitCalled)
	}

	// The table result must be COMMITTED.
	var tableResult *pb.TableCommitResult
	for _, b := range commitResp.Buckets {
		for _, tbl := range b.Tables {
			if tbl.Table == "orders" {
				tableResult = tbl
			}
		}
	}
	if tableResult == nil {
		t.Fatal("no result for table 'orders'")
	}
	if tableResult.Status != pb.TableCommitResult_STATUS_COMMITTED {
		t.Errorf("table status = %v, want COMMITTED", tableResult.Status)
	}
	if tableResult.FilesAdded == 0 {
		t.Error("FilesAdded should be > 0")
	}

	// Writers map must be cleared.
	server.mu.RLock()
	n := len(server.writers)
	server.mu.RUnlock()
	if n != 0 {
		t.Errorf("writers map should be empty after Commit, got %d", n)
	}
}

// TestCloseWriter_PoolError propagates commit errors to the Commit response.
func TestCloseWriter_PoolError(t *testing.T) {
	server, _ := createTestServerV2WithOutputTable(t)
	ctx := context.Background()

	pool := NewCommitPool(CommitPoolConfig{
		Workers: 1, MaxQueueSize: 8,
		CatalogFn: func(context.Context) (catalog.Catalog, error) { return nil, nil },
		CommitFn: func(context.Context, catalog.Catalog, icebergtable.Identifier,
			[]string, icebergjob.WriteMode, string) (*icebergjob.CommitResult, error) {
			return nil, errors.New("synthetic commit error")
		},
	})
	server.setCommitPoolForTest(pool)

	openResp, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Table: "orders", Bucket: "raw", InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	server.WriteChunk(ctx, &pb.WriteChunkRequest{ //nolint:errcheck
		WriterId: openResp.WriterId,
		Data:     []byte("id,name\n1,Bob\n"),
	})
	if _, err := server.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: openResp.WriterId}); err != nil {
		t.Fatalf("CloseWriter: %v", err)
	}

	commitResp, err := server.Commit(ctx, &pb.CommitRequest{})
	if err != nil {
		t.Fatalf("Commit must not return an error (pool errors go in response): %v", err)
	}
	if commitResp.Success {
		t.Error("Commit.Success must be false when pool commit failed")
	}
	for _, b := range commitResp.Buckets {
		for _, tbl := range b.Tables {
			if tbl.Table == "orders" && tbl.Status != pb.TableCommitResult_STATUS_FAILED {
				t.Errorf("orders status = %v, want FAILED", tbl.Status)
			}
		}
	}
}

// TestWriteModeForTable verifies write-mode resolution. Two tables with the
// same name in different buckets resolve to their own configured modes —
// FULL_LOAD must not bleed across buckets — and no match yields "" (the caller
// defaults to APPEND). The defaultBucket form (defaultBucket +
// defaultWriteMode=FULL_LOAD, no explicit OutputTables) must replace data
// (ReplaceDataFiles) on a second run, not accumulate duplicates (AddFiles).
func TestWriteModeForTable(t *testing.T) {
	type probe struct {
		bucket, table string
		want          icebergjob.WriteMode
	}
	tests := []struct {
		name   string
		config *Config
		probes []probe
	}{
		{
			name: "bucket scoped",
			config: &Config{
				RunID: "r",
				OutputTables: []OutputTableConfig{
					{Bucket: "raw", Name: "events", WriteMode: "APPEND"},
					{Bucket: "staging", Name: "events", WriteMode: "FULL_LOAD"},
				},
			},
			probes: []probe{
				{"raw", "events", icebergjob.WriteModeAppend},
				{"staging", "events", icebergjob.WriteModeFullLoad},
				{"unknown", "events", ""}, // unknown bucket
				{"raw", "nope", ""},       // unknown table
			},
		},
		{
			name: "table name matches but bucket differs",
			config: &Config{
				RunID: "r",
				OutputTables: []OutputTableConfig{
					{Bucket: "raw", Name: "orders", WriteMode: "FULL_LOAD"},
				},
			},
			probes: []probe{{"staging", "orders", ""}},
		},
		{
			name: "defaultBucket FULL_LOAD without OutputTables",
			config: &Config{
				RunID:            "r",
				DefaultBucket:    "out",
				DefaultWriteMode: "FULL_LOAD",
			},
			probes: []probe{
				{"out", "orders", icebergjob.WriteModeFullLoad},
				{"out", "users", icebergjob.WriteModeFullLoad},
				{"other", "orders", ""}, // different bucket: no default-bucket match
			},
		},
		{
			name: "explicit OutputTables entry overrides the default write mode",
			config: &Config{
				RunID:            "r",
				DefaultBucket:    "out",
				DefaultWriteMode: "FULL_LOAD",
				OutputTables: []OutputTableConfig{
					{Bucket: "out", Name: "archive", WriteMode: "APPEND"},
				},
			},
			probes: []probe{
				{"out", "archive", icebergjob.WriteModeAppend},
				{"out", "orders", icebergjob.WriteModeFullLoad},
			},
		},
		{
			name: "no default bucket and no match",
			config: &Config{
				RunID: "r",
				OutputTables: []OutputTableConfig{
					{Bucket: "raw", Name: "events", WriteMode: "APPEND"},
				},
			},
			probes: []probe{{"unknown", "events", ""}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &ServerV2{config: tc.config}
			for _, p := range tc.probes {
				if got := s.writeModeForTable(p.bucket, p.table); got != p.want {
					t.Errorf("%s.%s = %q, want %q", p.bucket, p.table, got, p.want)
				}
			}
		})
	}
}

// TestCommit_NoPool_WriterWithFiles verifies that in nil-pool (test) mode,
// a writer that produced files reports STATUS_COMMITTED.
func TestCommit_NoPool_WriterWithFiles(t *testing.T) {
	ctx := context.Background()
	mb := newMockBackend()
	cfg := &Config{RunID: "r", DefaultBucket: "raw"}
	server := &ServerV2{
		backend:       mb,
		config:        cfg,
		writers:       make(map[string]*writerState),
		filesManifest: NewFilesManifest("r"),
	}

	sch, _ := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64},
	})
	ws := &writerState{
		writerID: "w1", bucket: "raw", table: "t1",
		externalFiles: []buffer.FileInfo{
			{Path: "s3://b/f.parquet", RowCount: 1, SizeBytes: 100},
		},
		schema:    sch,
		committed: true, // already claimed (simulates CloseWriter path)
	}
	server.writers["w1"] = ws

	resp, err := server.Commit(ctx, &pb.CommitRequest{})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !resp.Success {
		t.Error("Success must be true in nil-pool mode")
	}
	found := false
	for _, b := range resp.Buckets {
		for _, tbl := range b.Tables {
			if tbl.Table == "t1" {
				found = true
				if tbl.Status != pb.TableCommitResult_STATUS_COMMITTED {
					t.Errorf("t1 status = %v, want COMMITTED", tbl.Status)
				}
			}
		}
	}
	if !found {
		t.Error("table t1 not found in response")
	}
}

// TestCommit_NoPool_WriterWithNoFiles verifies that in nil-pool mode,
// a writer that produced no files reports STATUS_SKIPPED.
func TestCommit_NoPool_WriterWithNoFiles(t *testing.T) {
	ctx := context.Background()
	mb := newMockBackend()
	cfg := &Config{RunID: "r", DefaultBucket: "raw"}
	server := &ServerV2{
		backend:       mb,
		config:        cfg,
		writers:       make(map[string]*writerState),
		filesManifest: NewFilesManifest("r"),
	}

	ws := &writerState{
		writerID:  "w1",
		bucket:    "raw",
		table:     "empty_table",
		committed: true,
		// no bufferMgr, no partitionRouter, no externalFiles → no files
	}
	server.writers["w1"] = ws

	resp, err := server.Commit(ctx, &pb.CommitRequest{})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !resp.Success {
		t.Error("Success must be true when all tables are skipped")
	}
	for _, b := range resp.Buckets {
		for _, tbl := range b.Tables {
			if tbl.Table == "empty_table" && tbl.Status != pb.TableCommitResult_STATUS_SKIPPED {
				t.Errorf("empty_table status = %v, want SKIPPED", tbl.Status)
			}
		}
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// createTestServerV2WithOutputTable returns a server with a configured
// output table ("raw.orders") so writeModeForTable and the pool path can match it.
func createTestServerV2WithOutputTable(t *testing.T) (*ServerV2, string) {
	t.Helper()
	cfg := &Config{
		RunID:         "test-pool-exec",
		ComponentName: "test-component",
		DefaultBucket: "raw",
		Backend:       newMockBackend(),
		OutputTables: []OutputTableConfig{
			{Name: "orders", Bucket: "raw", WriteMode: "APPEND"},
		},
	}
	return NewServerV2(cfg), t.TempDir()
}
