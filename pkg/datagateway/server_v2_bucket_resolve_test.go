package datagateway

import (
	"context"
	"os"
	"strings"
	"testing"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// When a component writes a table that isn't declared in outputs.tables, the
// old message was "bucket is required (…)", which sent operators looking for a
// missing bucket when the actual fault was a table-name drift between the
// component config and outputs.tables. The error must name the undeclared
// table AND the declared ones so the diff is obvious.
func TestOpenWriter_UndeclaredTable_NamesDeclaredTables(t *testing.T) {
	s := &ServerV2{
		config: &Config{
			RunID: "r",
			// Deliberate: no DefaultBucket, so resolution must fail.
			OutputTables: []OutputTableConfig{
				{Bucket: "raw", Name: "gbif_occurrences_sk2"},
				{Bucket: "raw", Name: "other_table"},
			},
		},
		writers: make(map[string]*writerState),
	}

	_, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{
		Table: "gbif_occurrences_sk", // note: no trailing "2"
	})
	if err == nil {
		t.Fatal("OpenWriter with an undeclared table and no defaultBucket: got nil error, want failure")
	}
	msg := err.Error()

	for _, want := range []string{
		"gbif_occurrences_sk",  // the table that was asked for
		"is not declared",      // the actual diagnosis
		"gbif_occurrences_sk2", // the declared names, so the typo is visible
		"other_table",
		"defaultBucket", // the alternative remedy
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q\ngot: %s", want, msg)
		}
	}
	// The old misleading phrasing must be gone.
	if strings.Contains(msg, "bucket is required") {
		t.Errorf("error still uses the misleading \"bucket is required\" phrasing\ngot: %s", msg)
	}
}

// With no OutputTables at all there is nothing to diff against, so the message
// should say plainly that no bucket could be determined from any source rather
// than printing an empty declared-tables list.
func TestOpenWriter_NoOutputTablesAndNoDefaultBucket(t *testing.T) {
	s := &ServerV2{
		config:  &Config{RunID: "r"},
		writers: make(map[string]*writerState),
	}

	_, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders"})
	if err == nil {
		t.Fatal("OpenWriter with no bucket source at all: got nil error, want failure")
	}
	msg := err.Error()
	if !strings.Contains(msg, "orders") {
		t.Errorf("error should name the table; got: %s", msg)
	}
	if !strings.Contains(msg, "no outputs.tables entries") {
		t.Errorf("error should state that outputs.tables is empty; got: %s", msg)
	}
	// Must not claim a table is "not declared" when nothing at all was declared.
	if strings.Contains(msg, "is not declared in outputs.tables") {
		t.Errorf("wrong branch taken: no OutputTables configured, so the declared-list message is misleading\ngot: %s", msg)
	}
}

// A declared table must still resolve its bucket from outputs.tables — the new
// error branches must not shadow the happy path. DefaultBucket is cleared so
// the resolution can only come from the OutputTables lookup.
func TestOpenWriter_DeclaredTableResolvesBucket(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	server.config.DefaultBucket = ""
	server.config.OutputTables = []OutputTableConfig{{Bucket: "declared", Name: "orders"}}

	resp, err := server.OpenWriter(context.Background(), &pb.OpenWriterRequest{
		Table:       "orders",
		InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenWriter for a declared table: unexpected error: %v", err)
	}
	if resp.WriterId == "" {
		t.Fatal("expected a writer id for a successfully opened writer")
	}

	server.mu.RLock()
	ws, ok := server.writers[resp.WriterId]
	server.mu.RUnlock()
	if !ok {
		t.Fatal("writer should be stored")
	}
	if ws.bucket != "declared" {
		t.Errorf("bucket = %q, want %q (resolved from outputs.tables, not defaultBucket)", ws.bucket, "declared")
	}
}
