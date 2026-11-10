package icebergjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
)

func TestObjectKey(t *testing.T) {
	for in, want := range map[string]string{
		"s3://b/k":                        "k",
		"gs://b/k":                        "k",
		"s3a://b/k":                       "k",
		"k":                               "k",
		"/k":                              "k",
		"s3://bucket/wh/t/data/a.parquet": "wh/t/data/a.parquet",
		"wh/t/data/a.parquet":             "wh/t/data/a.parquet",
	} {
		if got := objectKey(in); got != want {
			t.Errorf("objectKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLiveFileGuard_BareKeyListing(t *testing.T) {
	live := map[string]struct{}{objectKey("s3://bucket/wh/t/data/a.parquet"): {}}
	err := liveFileGuard([]string{"wh/t/data/orphan.parquet", "wh/t/data/a.parquet"}, live)
	if !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), "wh/t/data/a.parquet") {
		t.Fatalf("err = %v, want ErrRefused naming the live file", err)
	}
}

func TestLiveFileGuard_SchemeMismatchListing(t *testing.T) {
	live := map[string]struct{}{objectKey("s3://bucket/wh/t/metadata/00001.metadata.json"): {}}
	err := liveFileGuard([]string{"s3a://bucket/wh/t/metadata/00001.metadata.json"}, live)
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("err = %v, want ErrRefused", err)
	}
}

func TestLiveFileGuard_CleanPlan(t *testing.T) {
	live := map[string]struct{}{objectKey("s3://bucket/wh/t/data/a.parquet"): {}}
	if err := liveFileGuard([]string{"s3://bucket/wh/t/data/orphan.parquet"}, live); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestPrefixCheck(t *testing.T) {
	const loc = "gs://b/wh/t"
	for _, tc := range []struct {
		path string
		ok   bool
	}{
		{"gs://b/wh/t/data/a.parquet", true},
		{"gs://b/wh/t/metadata/00001.metadata.json", true},
		{"wh/t/data/a.parquet", false},            // bare key
		{"b/wh/t/data/a.parquet", false},          // bucket/key
		{"s3a://b/wh/t/data/a.parquet", false},    // other scheme
		{"gs://b/wh/t_v2/data/a.parquet", false},  // sibling table
		{"gs://b/wh/t", false},                    // the location itself
		{"gs://b/wh/t%2Fdata/a.parquet", false},   // percent-encoded separator
		{"gs://b/wh//t/data/a.parquet", false},    // interior //
		{"gs://other/wh/t/data/a.parquet", false}, // other bucket
	} {
		for _, l := range []string{loc, loc + "/"} {
			err := prefixCheck([]string{"gs://b/wh/t/data/ok.parquet", tc.path}, l)
			if tc.ok && err != nil {
				t.Errorf("prefixCheck(%q, loc %q) = %v, want nil", tc.path, l, err)
			}
			if !tc.ok && (!errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), tc.path)) {
				t.Errorf("prefixCheck(%q, loc %q) = %v, want ErrRefused naming it", tc.path, l, err)
			}
		}
	}
}

func TestCapErrors(t *testing.T) {
	var errs []error
	for i := range 25 {
		errs = append(errs, fmt.Errorf("delete f%d: boom", i))
	}
	err := capErrors(fmt.Errorf("failed to delete orphan files: %w", errors.Join(errs...)), 10)
	msg := err.Error()
	if !strings.Contains(msg, "f9: boom") || strings.Contains(msg, "f10: boom") || !strings.Contains(msg, "and 15 more errors") {
		t.Fatalf("capped = %q", msg)
	}
	short := fmt.Errorf("x: %w", errors.Join(errs[:3]...))
	if capErrors(short, 10) != short {
		t.Fatal("short error changed")
	}
}

// TestLiveFiles_RetainedSnapshotsAndMetadataLog: after a compaction the
// current snapshot references only new files, but a retained older snapshot
// still references the pre-compaction data file, and the metadata log and
// statistics name older metadata files. All of them are live.
func TestLiveFiles_RetainedSnapshotsAndMetadataLog(t *testing.T) {
	f := newManifestFixture(t)
	F := f.dataFile("mem://t/data/F.parquet", 100)
	G := f.dataFile("mem://t/data/G.parquet", 50)
	m1 := f.manifest("mem://t/metadata/m1.avro", 1, entry(iceberg.EntryStatusADDED, 1, F))
	m2 := f.manifest("mem://t/metadata/m2.avro", 2, entry(iceberg.EntryStatusDELETED, 2, F), entry(iceberg.EntryStatusADDED, 2, G))
	f.snapshot(1, "mem://t/metadata/snap-1.avro", m1)
	f.snapshot(2, "mem://t/metadata/snap-2.avro", m2)

	snap := func(id int64) map[string]any {
		s := map[string]any{"snapshot-id": id, "sequence-number": id, "timestamp-ms": testNow,
			"manifest-list": fmt.Sprintf("mem://t/metadata/snap-%d.avro", id), "summary": map[string]string{"operation": "append"}}
		if id > 1 {
			s["parent-snapshot-id"] = id - 1
		}
		return s
	}
	doc := map[string]any{
		"format-version": 2, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "location": "mem://t",
		"last-sequence-number": 2, "last-updated-ms": testNow, "last-column-id": 1,
		"current-schema-id": 0, "schemas": []any{map[string]any{"type": "struct", "schema-id": 0,
			"fields": []any{map[string]any{"id": 1, "name": "id", "required": false, "type": "long"}}}},
		"default-spec-id": 0, "partition-specs": []any{map[string]any{"spec-id": 0, "fields": []any{}}},
		"last-partition-id": 999, "default-sort-order-id": 0, "sort-orders": []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties": map[string]string{}, "current-snapshot-id": 2, "snapshots": []any{snap(1), snap(2)},
		"refs":         map[string]any{icebergtable.MainBranch: map[string]any{"snapshot-id": 2, "type": "branch"}},
		"metadata-log": []any{map[string]any{"metadata-file": "mem://t/metadata/v1.metadata.json", "timestamp-ms": testNow}},
		"statistics": []any{map[string]any{"snapshot-id": 2, "statistics-path": "mem://t/metadata/stats.puffin",
			"file-size-in-bytes": 1, "file-footer-size-in-bytes": 1, "blob-metadata": []any{}}},
		"partition-statistics": []any{map[string]any{"snapshot-id": 2, "statistics-path": "mem://t/metadata/pstats.parquet",
			"file-size-in-bytes": 1}},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	md, err := icebergtable.ParseMetadataBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	tbl := icebergtable.New(icebergtable.Identifier{"ns", "t"}, md, "mem://t/metadata/v2.metadata.json",
		func(context.Context) (iceio.IO, error) { return f.fs, nil }, nil)

	live, err := liveFiles(t.Context(), tbl)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		"mem://t/data/F.parquet", // retained snapshot 1's data file, DELETED in the current one
		"mem://t/data/G.parquet",
		"mem://t/metadata/m1.avro",
		"mem://t/metadata/snap-1.avro",
		"mem://t/metadata/v1.metadata.json",
		"mem://t/metadata/v2.metadata.json",
		"mem://t/metadata/stats.puffin",
		"mem://t/metadata/pstats.parquet",
	} {
		if err := liveFileGuard([]string{"mem://t/data/orphan.parquet", p}, live); !errors.Is(err, ErrRefused) {
			t.Errorf("guard on %s = %v, want ErrRefused", p, err)
		}
	}
	if err := liveFileGuard([]string{"mem://t/data/orphan.parquet"}, live); err != nil {
		t.Errorf("guard on a real orphan = %v", err)
	}
}
