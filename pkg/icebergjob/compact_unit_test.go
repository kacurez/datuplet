package icebergjob

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
)

func TestCompactOperationKey(t *testing.T) {
	k := CompactOperationKey("run", "comp", "raw", "events")
	if len(k) != 64 || k != CompactOperationKey("run", "comp", "raw", "events") {
		t.Fatalf("key %q not a stable sha256 hex", k)
	}
	for _, other := range []string{
		CompactOperationKey("run", "comp-b", "raw", "events"),
		CompactOperationKey("run", "comp", "raw", "events2"),
		CompactOperationKey("run", "compr", "aw", "events"),
	} {
		if other == k {
			t.Fatalf("distinct inputs share key %q", k)
		}
	}
}

func TestPureAppendSnapshot(t *testing.T) {
	const id = 7
	appendSnap := icebergtable.Snapshot{SnapshotID: id, Summary: &icebergtable.Summary{Operation: icebergtable.OpAppend}}
	data := iceberg.NewManifestFile(2, "m-data.avro", 1, 0, id).Build()
	inheritedDeletes := iceberg.NewManifestFile(2, "m-old-del.avro", 1, 0, 3).Content(iceberg.ManifestContentDeletes).Build()
	ownDeletes := iceberg.NewManifestFile(2, "m-del.avro", 1, 0, id).Content(iceberg.ManifestContentDeletes).Build()

	cases := []struct {
		name string
		snap icebergtable.Snapshot
		mfs  []iceberg.ManifestFile
		want bool
	}{
		{"append with data manifests", appendSnap, []iceberg.ManifestFile{data, inheritedDeletes}, true},
		{"append adding a deletes manifest", appendSnap, []iceberg.ManifestFile{data, ownDeletes}, false},
		{"overwrite", icebergtable.Snapshot{SnapshotID: id, Summary: &icebergtable.Summary{Operation: icebergtable.OpOverwrite}}, []iceberg.ManifestFile{data}, false},
		{"no summary", icebergtable.Snapshot{SnapshotID: id}, []iceberg.ManifestFile{data}, false},
	}
	for _, c := range cases {
		if got := pureAppend(c.snap, c.mfs); got != c.want {
			t.Errorf("%s: pureAppend = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestCompactClassifyUnsupported(t *testing.T) {
	orc := fmt.Errorf("read data file: %w", fmt.Errorf("orc: %w", iceberg.ErrNotImplemented))
	got := classifyUnsupported(orc)
	if !errors.Is(got, ErrUnsupportedFile) || !errors.Is(got, iceberg.ErrNotImplemented) {
		t.Fatalf("classifyUnsupported(%v) = %v, want ErrUnsupportedFile wrapping ErrNotImplemented", orc, got)
	}
	other := errors.New("boom")
	if got := classifyUnsupported(other); errors.Is(got, ErrUnsupportedFile) {
		t.Fatalf("classifyUnsupported(%v) = %v, want unchanged", other, got)
	}
}
