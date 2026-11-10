package icebergjob

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
)

const hourMs = int64(time.Hour / time.Millisecond)

// testNow is the fixture clock; snapshot times are relative to it.
var testNow = time.Now().UnixMilli()

// chainMetadata parses metadata whose main branch is a linear chain of
// snapshots 1..n with the given commit times (oldest first), plus tags
// (name → snapshot id). Snapshots in drop are left out, as an expiry would.
func chainMetadata(t *testing.T, ts []int64, tags map[string]int64, drop ...int64) icebergtable.Metadata {
	t.Helper()
	dropped := map[int64]bool{}
	for _, id := range drop {
		dropped[id] = true
	}
	var snaps []map[string]any
	for i, tm := range ts {
		id := int64(i + 1)
		if dropped[id] {
			continue
		}
		s := map[string]any{"snapshot-id": id, "sequence-number": id, "timestamp-ms": tm,
			"manifest-list": fmt.Sprintf("mem://t/metadata/snap-%d.avro", id), "summary": map[string]string{"operation": "append"}}
		if i > 0 {
			s["parent-snapshot-id"] = id - 1
		}
		snaps = append(snaps, s)
	}
	head := int64(len(ts))
	refs := map[string]any{icebergtable.MainBranch: map[string]any{"snapshot-id": head, "type": "branch"}}
	for name, id := range tags {
		refs[name] = map[string]any{"snapshot-id": id, "type": "tag"}
	}
	doc := map[string]any{
		"format-version": 2, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "location": "mem://t",
		"last-sequence-number": head, "last-updated-ms": ts[len(ts)-1], "last-column-id": 1,
		"current-schema-id": 0, "schemas": []any{map[string]any{"type": "struct", "schema-id": 0,
			"fields": []any{map[string]any{"id": 1, "name": "id", "required": false, "type": "long"}}}},
		"default-spec-id": 0, "partition-specs": []any{map[string]any{"spec-id": 0, "fields": []any{}}},
		"last-partition-id": 999, "default-sort-order-id": 0, "sort-orders": []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties": map[string]string{}, "current-snapshot-id": head, "snapshots": snaps, "refs": refs,
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	md, err := icebergtable.ParseMetadataBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	return md
}

func ids(set map[int64]struct{}) []int64 {
	var out []int64
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestEffectiveRetainLast_SupersededRecently(t *testing.T) {
	now := testNow
	// Parent committed 30 days ago, head 1 h ago: the parent was superseded
	// 1 h ago, so a reader may still be on it.
	md := chainMetadata(t, []int64{now - 30*24*hourMs, now - hourMs}, nil)
	if got := effectiveRetainLast(md, now-24*hourMs, 1); got != 2 {
		t.Fatalf("effectiveRetainLast = %d, want 2", got)
	}
}

func TestEffectiveRetainLast_AllOld(t *testing.T) {
	now := testNow
	md := chainMetadata(t, []int64{now - 30*24*hourMs, now - 20*24*hourMs, now - 10*24*hourMs}, nil)
	for _, retain := range []int{1, 2, 10} {
		if got := effectiveRetainLast(md, now-24*hourMs, retain); got != retain {
			t.Errorf("retain %d: effectiveRetainLast = %d", retain, got)
		}
	}
}

func TestEffectiveRetainLast_ManyRecent(t *testing.T) {
	now := testNow
	// 1 and 2 old; 3, 4, 5 committed in the last 24 h. Snapshot 2 was
	// superseded by 3 after the cutoff, so 5,4,3,2 are protected.
	md := chainMetadata(t, []int64{now - 30*24*hourMs, now - 20*24*hourMs, now - 3*hourMs, now - 2*hourMs, now - hourMs}, nil)
	if got := effectiveRetainLast(md, now-24*hourMs, 1); got != 4 {
		t.Fatalf("effectiveRetainLast = %d, want 4", got)
	}
}

func TestKeepSet_IncludesOtherRefs(t *testing.T) {
	now := testNow
	md := chainMetadata(t, []int64{now - 40*24*hourMs, now - 30*24*hourMs, now - 20*24*hourMs, now - 10*24*hourMs},
		map[string]int64{"audit": 1})
	got := ids(keepSet(md, now-24*hourMs, 2))
	want := []int64{1, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("keepSet = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("keepSet = %v, want %v", got, want)
		}
	}
}

func TestGuardKeepSet(t *testing.T) {
	now := testNow
	base := chainMetadata(t, []int64{now - 3*hourMs, now - 2*hourMs, now - hourMs}, nil)
	ks := keepSet(base, now-24*hourMs, effectiveRetainLast(base, now-24*hourMs, 1))

	// A staged view that drops snapshot 1, as a foreign per-ref
	// min-snapshots-to-keep=1 / max-snapshot-age-ms=1 would make iceberg-go do.
	staged := chainMetadata(t, []int64{now - 3*hourMs, now - 2*hourMs, now - hourMs}, nil, 1)
	removed := removedSnapshotIDs(base, staged)
	if len(removed) != 1 || removed[0] != 1 {
		t.Fatalf("removed = %v, want [1]", removed)
	}
	if err := guardKeepSet(removed, ks); !errors.Is(err, ErrRefused) {
		t.Fatalf("guardKeepSet = %v, want ErrRefused", err)
	}
	if err := guardKeepSet([]int64{99}, ks); err != nil {
		t.Fatalf("guardKeepSet outside keep-set = %v", err)
	}
}

// manifestFixture writes manifests and manifest lists into a MemFS.
type manifestFixture struct {
	t   *testing.T
	fs  *iceio.MemFS
	sch *iceberg.Schema
}

func newManifestFixture(t *testing.T) *manifestFixture {
	return &manifestFixture{t: t, fs: iceio.NewMemFS(),
		sch: iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})}
}

func (f *manifestFixture) dataFile(path string, size int64) iceberg.DataFile {
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.ParquetFile, nil, nil, nil, 1, size)
	if err != nil {
		f.t.Fatal(err)
	}
	return b.Build()
}

func (f *manifestFixture) manifest(path string, snap int64, entries ...iceberg.ManifestEntry) iceberg.ManifestFile {
	var buf bytes.Buffer
	mf, err := iceberg.WriteManifest(path, &buf, 2, *iceberg.UnpartitionedSpec, f.sch, snap, entries)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.fs.WriteFile(path, buf.Bytes()); err != nil {
		f.t.Fatal(err)
	}
	return mf
}

func (f *manifestFixture) snapshot(id int64, list string, mfs ...iceberg.ManifestFile) icebergtable.Snapshot {
	var buf bytes.Buffer
	seq := id
	if err := iceberg.WriteManifestList(2, &buf, id, nil, &seq, 0, mfs); err != nil {
		f.t.Fatal(err)
	}
	if err := f.fs.WriteFile(list, buf.Bytes()); err != nil {
		f.t.Fatal(err)
	}
	return icebergtable.Snapshot{SnapshotID: id, SequenceNumber: id, ManifestList: list}
}

func entry(status iceberg.ManifestEntryStatus, snap int64, df iceberg.DataFile) iceberg.ManifestEntry {
	seq := snap
	return iceberg.NewManifestEntry(status, &snap, &seq, &seq, df)
}

func TestExpireDeletionSet_DeletedEntriesAreCandidates(t *testing.T) {
	f := newManifestFixture(t)
	ctx := t.Context()
	F := f.dataFile("mem://t/data/F.parquet", 100)
	G := f.dataFile("mem://t/data/G.parquet", 50)
	m1 := f.manifest("mem://t/metadata/m1.avro", 1, entry(iceberg.EntryStatusADDED, 1, F))
	// Snapshot 2 (a compaction) replaces F with G: F is DELETED in m2.
	m2 := f.manifest("mem://t/metadata/m2.avro", 2, entry(iceberg.EntryStatusDELETED, 2, F), entry(iceberg.EntryStatusADDED, 2, G))
	s1 := f.snapshot(1, "mem://t/metadata/snap-1.avro", m1)
	s2 := f.snapshot(2, "mem://t/metadata/snap-2.avro", m2)

	paths, sizes, err := expireDeletionSet(ctx, f.fs, []icebergtable.Snapshot{s2}, []icebergtable.Snapshot{s1})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	want := []string{"mem://t/data/F.parquet", "mem://t/metadata/m1.avro", "mem://t/metadata/snap-1.avro"}
	if len(paths) != len(want) {
		t.Fatalf("paths = %v, want %v", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("paths = %v, want %v", paths, want)
		}
	}
	if sizes["mem://t/data/F.parquet"] != 100 || sizes["mem://t/metadata/m1.avro"] != m1.Length() {
		t.Fatalf("sizes = %v", sizes)
	}
}

func TestExpireDeletionSet_LiveFileKept(t *testing.T) {
	f := newManifestFixture(t)
	ctx := t.Context()
	F := f.dataFile("mem://t/data/F.parquet", 100)
	H := f.dataFile("mem://t/data/H.parquet", 10)
	m1 := f.manifest("mem://t/metadata/m1.avro", 1, entry(iceberg.EntryStatusADDED, 1, F))
	// m1 is shared with the retained snapshot; m1b is rewritten and carries
	// F forward as EXISTING. Neither F nor m1 may be deleted.
	shared := f.manifest("mem://t/metadata/shared.avro", 1, entry(iceberg.EntryStatusADDED, 1, H))
	m1b := f.manifest("mem://t/metadata/m1b.avro", 2, entry(iceberg.EntryStatusEXISTING, 1, F))
	s1 := f.snapshot(1, "mem://t/metadata/snap-1.avro", m1, shared)
	// Reuse shared as snapshot 1's list recorded it (sequence number assigned).
	mfs, err := s1.Manifests(f.fs)
	if err != nil {
		t.Fatal(err)
	}
	s2 := f.snapshot(2, "mem://t/metadata/snap-2.avro", m1b, mfs[1])

	paths, _, err := expireDeletionSet(ctx, f.fs, []icebergtable.Snapshot{s2}, []icebergtable.Snapshot{s1})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	want := []string{"mem://t/metadata/m1.avro", "mem://t/metadata/snap-1.avro"}
	if len(paths) != len(want) || paths[0] != want[0] || paths[1] != want[1] {
		t.Fatalf("paths = %v, want %v", paths, want)
	}
}
