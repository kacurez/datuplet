package datupleticeio_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"gocloud.dev/blob/memblob"

	"github.com/datuplet/datuplet/pkg/datupleticeio"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// memCatalog is a minimal table.CatalogIO that writes each new metadata file
// through the table IO, so a gs:// table lives entirely on gcsIO.
type memCatalog struct {
	fsys    iceio.IO
	tbl     *table.Table
	version int
}

func (c *memCatalog) fsF(context.Context) (iceio.IO, error) { return c.fsys, nil }

func (c *memCatalog) writeMetadata(md table.Metadata) (string, error) {
	loc := fmt.Sprintf("%s/metadata/%05d-%s.metadata.json", md.Location(), c.version, uuid.NewString())
	c.version++
	b, err := json.Marshal(md)
	if err != nil {
		return "", err
	}
	return loc, c.fsys.(iceio.WriteFileIO).WriteFile(loc, b)
}

func (c *memCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return c.tbl, nil
}

func (c *memCatalog) CommitTable(_ context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	for _, r := range reqs {
		if err := r.Validate(c.tbl.Metadata()); err != nil {
			return nil, "", err
		}
	}
	md, err := table.UpdateTableMetadata(c.tbl.Metadata(), updates, c.tbl.MetadataLocation())
	if err != nil {
		return nil, "", err
	}
	loc, err := c.writeMetadata(md)
	if err != nil {
		return nil, "", err
	}
	c.tbl = table.New(ident, md, loc, c.fsF, c)
	return md, loc, nil
}

func appendRows(t *testing.T, cat *memCatalog, ids ...int64) { writeRows(t, cat, false, ids...) }

// writeRows appends ids, or with overwrite replaces every row with ids in one
// snapshot (every current file new, as after a compaction).
func writeRows(t *testing.T, cat *memCatalog, overwrite bool, ids ...int64) {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(ids, nil)
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(sc, []arrow.Array{col}, int64(len(ids)))
	defer rec.Release()
	tbl := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer tbl.Release()
	txn := cat.tbl.NewTransaction()
	write := txn.AppendTable
	if overwrite {
		write = func(ctx context.Context, tbl arrow.Table, batch int64, props iceberg.Properties) error {
			return txn.OverwriteTable(ctx, tbl, batch, props)
		}
	}
	if err := write(context.Background(), tbl, 1024, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func listAll(t *testing.T, fsys iceio.IO, root string) []string {
	t.Helper()
	var out []string
	err := fsys.(iceio.ListableIO).WalkDir(root, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		out = append(out, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

// TestGCSOrphans_DeletesOnlyUncommitted is the positive GCS case: a correct
// gcsIO listing passes the live-file guard and exactly the uncommitted file
// under the table location is deleted; a sibling table's file survives.
func TestGCSOrphans_DeletesOnlyUncommitted(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	fsys := datupleticeio.NewGCSIOForTest(bucket, "b")
	const loc = "gs://b/wh/t"

	sch := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	md, err := table.NewMetadata(sch, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, loc, nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &memCatalog{fsys: fsys}
	mdLoc, err := cat.writeMetadata(md)
	if err != nil {
		t.Fatal(err)
	}
	ident := table.Identifier{"wh", "t"}
	cat.tbl = table.New(ident, md, mdLoc, cat.fsF, cat)

	appendRows(t, cat, 1, 2)
	appendRows(t, cat, 3)

	const orphan = loc + "/data/uncommitted.parquet"
	const sibling = "gs://b/wh/t_v2/data/uncommitted.parquet"
	wf := fsys.(iceio.WriteFileIO)
	for _, p := range []string{orphan, sibling} {
		if err := wf.WriteFile(p, []byte("not committed")); err != nil {
			t.Fatal(err)
		}
	}
	before := listAll(t, fsys, loc)
	if !slices.Contains(before, orphan) {
		t.Fatalf("listing %v lacks the orphan", before)
	}

	res, err := icebergjob.DeleteOrphansOnTable(ctx, cat.tbl, icebergjob.OrphanOpts{OlderThan: 1, MaxFiles: 10})
	if err != nil {
		t.Fatalf("DeleteOrphansOnTable: %v", err)
	}
	if res.Status != "DELETED" || res.OrphanFiles != 1 || res.FilesDeleted != 1 || res.OrphanBytes != int64(len("not committed")) {
		t.Fatalf("result = %+v", res)
	}

	after := listAll(t, fsys, loc)
	want := slices.DeleteFunc(slices.Clone(before), func(p string) bool { return p == orphan })
	if !slices.Equal(after, want) {
		t.Fatalf("after cleanup:\n%v\nwant:\n%v", after, want)
	}
	if ok, _ := bucket.Exists(ctx, strings.TrimPrefix(sibling, "gs://b/")); !ok {
		t.Fatal("sibling table's file was deleted")
	}

	_, itr, err := cat.tbl.Scan().ToArrowRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rows int64
	for rec, err := range itr {
		if err != nil {
			t.Fatal(err)
		}
		rows += rec.NumRows()
		rec.Release()
	}
	if rows != 3 {
		t.Fatalf("rows = %d, want 3", rows)
	}

	res, err = icebergjob.DeleteOrphansOnTable(ctx, cat.tbl, icebergjob.OrphanOpts{OlderThan: 1, MaxFiles: 10})
	if err != nil || res.Status != "NOTHING_TO_DO" {
		t.Fatalf("rerun = %+v, %v", res, err)
	}
}

// respellIO lists through the wrapped IO but reports each path rewritten by
// spell, simulating a listing whose path spelling differs from metadata.
type respellIO struct {
	iceio.IO
	spell func(string) string
}

func (r respellIO) WalkDir(root string, fn fs.WalkDirFunc) error {
	return r.IO.(iceio.ListableIO).WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		return fn(r.spell(p), d, err)
	})
}

// TestGCSOrphans_MisspelledListing_Refused: a listing that drops the gs://b/
// prefix makes every live file look orphaned to iceberg-go's planner; the
// prefix check must refuse and delete nothing.
func TestGCSOrphans_MisspelledListing_Refused(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	fsys := datupleticeio.NewGCSIOForTest(bucket, "b")
	const loc = "gs://b/wh/t"

	sch := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	md, err := table.NewMetadata(sch, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, loc, nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &memCatalog{fsys: fsys}
	mdLoc, err := cat.writeMetadata(md)
	if err != nil {
		t.Fatal(err)
	}
	ident := table.Identifier{"wh", "t"}
	cat.tbl = table.New(ident, md, mdLoc, cat.fsF, cat)
	appendRows(t, cat, 1, 2)
	before := listAll(t, fsys, loc)

	bare := respellIO{IO: fsys, spell: func(p string) string { return strings.TrimPrefix(p, "gs://b/") }}
	tbl := table.New(ident, cat.tbl.Metadata(), cat.tbl.MetadataLocation(),
		func(context.Context) (iceio.IO, error) { return bare, nil }, cat)
	res, err := icebergjob.DeleteOrphansOnTable(ctx, tbl, icebergjob.OrphanOpts{OlderThan: 1, MaxFiles: 100})
	if !errors.Is(err, icebergjob.ErrRefused) || !strings.Contains(err.Error(), "prefix check") {
		t.Fatalf("DeleteOrphansOnTable = %+v, %v; want prefix check refusal", res, err)
	}
	if after := listAll(t, fsys, loc); !slices.Equal(after, before) {
		t.Fatalf("files changed despite refusal:\n%v\nwant:\n%v", after, before)
	}
}

// TestGCSOrphans_CompactedHistory_NothingDeleted: after an overwrite every
// current file is new, while retained older snapshots reference files older
// than the cutoff (OlderThan 1 ns makes every file old). A listing whose
// spellings match nothing is refused; a correct listing finds no orphans.
func TestGCSOrphans_CompactedHistory_NothingDeleted(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	fsys := datupleticeio.NewGCSIOForTest(bucket, "b")
	const loc = "gs://b/wh/t"

	sch := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	md, err := table.NewMetadata(sch, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, loc, nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &memCatalog{fsys: fsys}
	mdLoc, err := cat.writeMetadata(md)
	if err != nil {
		t.Fatal(err)
	}
	ident := table.Identifier{"wh", "t"}
	cat.tbl = table.New(ident, md, mdLoc, cat.fsF, cat)
	appendRows(t, cat, 1, 2)
	appendRows(t, cat, 3)
	writeRows(t, cat, true, 1, 2, 3)
	if n := len(cat.tbl.Metadata().Snapshots()); n != 3 {
		t.Fatalf("%d snapshots, want 3", n)
	}
	before := listAll(t, fsys, loc)
	opts := icebergjob.OrphanOpts{OlderThan: 1, MaxFiles: 100}

	odd := respellIO{IO: fsys, spell: func(p string) string { return strings.TrimPrefix(p, "gs://") }}
	tbl := table.New(ident, cat.tbl.Metadata(), cat.tbl.MetadataLocation(),
		func(context.Context) (iceio.IO, error) { return odd, nil }, cat)
	if res, err := icebergjob.DeleteOrphansOnTable(ctx, tbl, opts); !errors.Is(err, icebergjob.ErrRefused) || !strings.Contains(err.Error(), "prefix check") {
		t.Fatalf("misspelled listing = %+v, %v; want ErrRefused", res, err)
	}
	if after := listAll(t, fsys, loc); !slices.Equal(after, before) {
		t.Fatalf("files changed despite refusal:\n%v\nwant:\n%v", after, before)
	}

	res, err := icebergjob.DeleteOrphansOnTable(ctx, cat.tbl, opts)
	if err != nil || res.Status != "NOTHING_TO_DO" {
		t.Fatalf("correct listing = %+v, %v", res, err)
	}
	if after := listAll(t, fsys, loc); !slices.Equal(after, before) {
		t.Fatalf("files changed:\n%v\nwant:\n%v", after, before)
	}
}
