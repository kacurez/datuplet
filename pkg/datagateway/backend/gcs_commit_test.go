package backend

import (
	"context"
	"testing"
)

// TestGCSBackend_Commit_ReportsWriterStats verifies that Commit assembles
// one TableCommitResult per gcsWriter with stats copied through.
// This is a structural test — no real GCS access is needed because Commit
// only reads from gcsWriter fields. The end-to-end iceberg-commit flow
// lands in Slice D's pkg/datupleticeio (gs:// iceberg-go factory).
func TestGCSBackend_Commit_ReportsWriterStats(t *testing.T) {
	t.Parallel()

	// gcsBackend with a nil bucket is fine — Commit never touches it.
	g := &gcsBackend{}

	w1 := &gcsWriter{
		tablePath:    "tables/orders",
		outputName:   "orders",
		format:       "csv",
		partNum:      2,
		bytesWritten: 1024,
		rowsWritten:  42,
		filePaths:    []string{"tables/orders/part-00000.csv", "tables/orders/part-00001.csv"},
	}
	w2 := &gcsWriter{
		tablePath:    "tables/customers",
		outputName:   "customers",
		format:       "csv",
		partNum:      1,
		bytesWritten: 512,
		rowsWritten:  17,
		filePaths:    []string{"tables/customers/part-00000.csv"},
	}

	res, err := g.Commit(context.Background(), []Writer{w1, w2})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(res.Tables) != 2 {
		t.Fatalf("Tables = %d, want 2", len(res.Tables))
	}

	if res.Tables[0].OutputName != "orders" {
		t.Errorf("Tables[0].OutputName = %q, want orders", res.Tables[0].OutputName)
	}
	if res.Tables[0].FilesAdded != 2 {
		t.Errorf("Tables[0].FilesAdded = %d, want 2", res.Tables[0].FilesAdded)
	}
	if res.Tables[0].RowsAdded != 42 {
		t.Errorf("Tables[0].RowsAdded = %d, want 42", res.Tables[0].RowsAdded)
	}
	if res.Tables[0].Status != CommitStatusCommitted {
		t.Errorf("Tables[0].Status = %v, want CommitStatusCommitted", res.Tables[0].Status)
	}

	if res.Tables[1].OutputName != "customers" {
		t.Errorf("Tables[1].OutputName = %q, want customers", res.Tables[1].OutputName)
	}
	if res.Tables[1].RowsAdded != 17 {
		t.Errorf("Tables[1].RowsAdded = %d, want 17", res.Tables[1].RowsAdded)
	}
}

// TestGCSBackend_Commit_SkipsNonGCSWriters verifies that writers from
// another backend (e.g. *minioWriter) are silently skipped — Commit only
// reports on its own writers. The skipped entry stays as the zero
// TableCommitResult, matching MinIOBackend.Commit's contract.
func TestGCSBackend_Commit_SkipsNonGCSWriters(t *testing.T) {
	t.Parallel()

	g := &gcsBackend{}
	mw := &minioWriter{tablePath: "alien/path", outputName: "alien"}
	gw := &gcsWriter{tablePath: "tables/x", outputName: "x", rowsWritten: 3}

	res, err := g.Commit(context.Background(), []Writer{mw, gw})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(res.Tables) != 2 {
		t.Fatalf("Tables = %d, want 2", len(res.Tables))
	}
	// First entry: zero (alien writer was skipped).
	if res.Tables[0].OutputName != "" {
		t.Errorf("Tables[0].OutputName = %q, want \"\"", res.Tables[0].OutputName)
	}
	// Second entry: populated from gw.
	if res.Tables[1].OutputName != "x" {
		t.Errorf("Tables[1].OutputName = %q, want x", res.Tables[1].OutputName)
	}
	if res.Tables[1].RowsAdded != 3 {
		t.Errorf("Tables[1].RowsAdded = %d, want 3", res.Tables[1].RowsAdded)
	}
}
