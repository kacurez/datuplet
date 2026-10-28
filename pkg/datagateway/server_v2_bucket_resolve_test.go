package datagateway

import (
	"context"
	"strings"
	"testing"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// When a component writes a table that isn't declared in outputs.tables, the
// old message was "bucket is required (…)", which sent operators looking for a
// missing bucket when the actual fault was a table-name drift between the
// component config and outputs.tables. The error must name the undeclared
// table AND the declared ones so the diff is obvious.
//
// With no OutputTables at all there is nothing to diff against, so the message
// should say plainly that no bucket could be determined from any source rather
// than printing an empty declared-tables list.
func TestOpenWriter_UnresolvableBucket_ErrorMessage(t *testing.T) {
	tests := []struct {
		name    string
		tables  []OutputTableConfig
		table   string
		want    []string
		notWant []string
	}{
		{
			name: "undeclared table names the declared tables (gbif typo incident)",
			tables: []OutputTableConfig{
				{Bucket: "raw", Name: "gbif_occurrences_sk2"},
				{Bucket: "raw", Name: "other_table"},
			},
			table: "gbif_occurrences_sk", // note: no trailing "2"
			want: []string{
				"gbif_occurrences_sk",  // the table that was asked for
				"is not declared",      // the actual diagnosis
				"gbif_occurrences_sk2", // the declared names, so the typo is visible
				"other_table",
				"defaultBucket", // the alternative remedy
			},
			// The old misleading phrasing must be gone.
			notWant: []string{"bucket is required"},
		},
		{
			name:  "no outputs.tables and no defaultBucket",
			table: "orders",
			want:  []string{"orders", "no outputs.tables entries"},
			// Must not claim a table is "not declared" when nothing at all was declared.
			notWant: []string{"is not declared in outputs.tables"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &ServerV2{
				// Deliberate: no DefaultBucket, so resolution must fail.
				config:  &Config{RunID: "r", OutputTables: tc.tables},
				writers: make(map[string]*writerState),
			}

			_, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: tc.table})
			if err == nil {
				t.Fatal("OpenWriter with no resolvable bucket: got nil error, want failure")
			}
			msg := err.Error()
			for _, want := range tc.want {
				if !strings.Contains(msg, want) {
					t.Errorf("error message missing %q\ngot: %s", want, msg)
				}
			}
			for _, bad := range tc.notWant {
				if strings.Contains(msg, bad) {
					t.Errorf("error message must not contain %q\ngot: %s", bad, msg)
				}
			}
		})
	}
}
