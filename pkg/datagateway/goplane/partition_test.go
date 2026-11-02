package goplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	dgschema "github.com/datuplet/datuplet/pkg/datagateway/schema"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// Partitioned-write fixture: a Hadoop catalog table created through
// lakekeeper.LoadOrCreateTable (the gateway's create path), written with the
// goplane writer and committed with icebergjob.CommitTableFiles.

var tsUTC = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}

// eventSchema is (id long, name string, ts timestamptz).
var eventSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "ts", Type: tsUTC, Nullable: true},
}, nil)

type ptab struct {
	t     *testing.T
	cat   *hadoop.Catalog
	ident table.Identifier
	sch   *arrow.Schema
	p     *Plane
	tbl   *table.Table
	warn  string
	n     int
}

func newPartTable(t *testing.T, sch *arrow.Schema, fields ...lakekeeper.PartitionField) *ptab {
	t.Helper()
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pt := &ptab{t: t, cat: cat, ident: catalog.ToIdentifier("ns", "tbl"), sch: sch}
	pt.p = New(Config{})
	t.Cleanup(func() { _ = pt.p.Close() })
	pt.resolve(fields...)
	return pt
}

// resolve runs the gateway's load-or-create with fields.
func (pt *ptab) resolve(fields ...lakekeeper.PartitionField) {
	pt.t.Helper()
	gw, err := dgschema.NewSchemaFromArrow(pt.sch)
	if err != nil {
		pt.t.Fatal(err)
	}
	pt.tbl, pt.warn, err = lakekeeper.LoadOrCreateTable(context.Background(), pt.cat, "ns", "tbl",
		func(context.Context) (*dgschema.Schema, error) { return gw, nil }, fields)
	if err != nil {
		pt.t.Fatalf("LoadOrCreateTable: %v", err)
	}
}

func (pt *ptab) reload() *table.Table {
	pt.t.Helper()
	tb, err := pt.cat.LoadTable(context.Background(), pt.ident)
	if err != nil {
		pt.t.Fatal(err)
	}
	return tb
}

func (pt *ptab) spec() datagateway.WriterSpec {
	spec := pt.tbl.Spec()
	pt.n++
	return datagateway.WriterSpec{
		DataPrefix:    strings.TrimRight(pt.tbl.Location(), "/") + "/data/",
		FilePrefix:    fmt.Sprintf("part-unit-w%d", pt.n),
		Schema:        pt.sch,
		Table:         "ns.tbl",
		PartitionSpec: &spec,
		TableSchema:   pt.tbl.Schema(),
	}
}

func (pt *ptab) write(spec datagateway.WriterSpec, recs ...arrow.RecordBatch) []datagateway.WrittenFile {
	pt.t.Helper()
	w := openWriter(pt.t, pt.p, spec)
	for _, rec := range recs {
		write(pt.t, w, rec)
	}
	files, err := w.Close(context.Background())
	if err != nil {
		pt.t.Fatalf("Close: %v", err)
	}
	return files
}

func (pt *ptab) commit(files []datagateway.WrittenFile, mode icebergjob.WriteMode) {
	pt.t.Helper()
	var paths []string
	part := &icebergjob.PartitionedFiles{SpecID: files[0].SpecID}
	for _, f := range files {
		paths = append(paths, f.Path)
		part.Tuples = append(part.Tuples, f.Partition)
	}
	key := fmt.Sprint(time.Now().UnixNano())
	if _, err := icebergjob.CommitTableFiles(context.Background(), pt.cat, pt.ident, paths, part, mode, nil, key); err != nil {
		pt.t.Fatalf("CommitTableFiles: %v", err)
	}
}

// scan returns the planned file count and the ids read for filter.
func (pt *ptab) scan(filter iceberg.BooleanExpression) (int, []int64) {
	pt.t.Helper()
	opts := []table.ScanOption{}
	if filter != nil {
		opts = append(opts, table.WithRowFilter(filter))
	}
	sc := pt.reload().Scan(opts...)
	tasks, err := sc.PlanFiles(context.Background())
	if err != nil {
		pt.t.Fatalf("PlanFiles: %v", err)
	}
	tb, err := sc.ToArrowTable(context.Background())
	if err != nil {
		pt.t.Fatalf("ToArrowTable: %v", err)
	}
	defer tb.Release()
	var ids []int64
	for _, ch := range tb.Column(0).Data().Chunks() {
		a := ch.(*array.Int64)
		for i := 0; i < a.Len(); i++ {
			ids = append(ids, a.Value(i))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return len(tasks), ids
}

// row of eventSchema; name "" is null, ts in micros.
type evRow struct {
	id   int64
	name string
	ts   int64
}

func events(rows ...evRow) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, eventSchema)
	defer b.Release()
	for _, r := range rows {
		b.Field(0).(*array.Int64Builder).Append(r.id)
		if r.name == "" {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append(r.name)
		}
		b.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(r.ts))
	}
	return b.NewRecordBatch()
}

const dayMicros = int64(24 * time.Hour / time.Microsecond)

// day0 is 2024-01-01T00:00:00Z in micros.
var day0 = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()

func identityDay() []lakekeeper.PartitionField {
	return []lakekeeper.PartitionField{{SourceColumn: "name", Transform: "identity"}, {SourceColumn: "ts", Transform: "day"}}
}

// expectedTuple is the (name, day) tuple of a row computed without
// iceberg-go: the identity value and days since the epoch.
func expectedTuple(r evRow) string {
	return fmt.Sprintf("%s|%d", r.name, int32(math.Floor(float64(r.ts)/float64(dayMicros))))
}

func tupleString(tb *table.Table, part map[int]any) string {
	spec := tb.Spec()
	name := part[spec.Field(0).FieldID]
	day := part[spec.Field(1).FieldID]
	n := ""
	if name != nil {
		n = name.(string)
	}
	return fmt.Sprintf("%s|%d", n, int32(day.(iceberg.Date)))
}

func identityDayRows() ([]evRow, []evRow) {
	b1 := []evRow{
		{1, "a", day0 + 10}, {2, "b", day0 + 20}, {3, "a", day0 + dayMicros + 5}, {4, "a", day0 + 30},
	}
	b2 := []evRow{
		{5, "b", day0 + 40}, {6, "a", day0 + dayMicros + 6}, {7, "c", day0 - 1}, // the day before (floor)
	}
	return b1, b2
}

func TestPartitionedWriteIdentityAndDay(t *testing.T) {
	pt := newPartTable(t, eventSchema, identityDay()...)
	b1, b2 := identityDayRows()
	files := pt.write(pt.spec(), events(b1...), events(b2...))

	want := map[string][]int64{} // tuple -> ids
	for _, r := range append(append([]evRow{}, b1...), b2...) {
		want[expectedTuple(r)] = append(want[expectedTuple(r)], r.id)
	}
	if len(files) != len(want) {
		t.Fatalf("files = %d, want one per tuple (%d): %+v", len(files), len(want), files)
	}
	for _, f := range files {
		ids := readIDs(t, f.Path)
		key := tupleString(pt.tbl, f.Partition)
		if fmt.Sprint(ids) != fmt.Sprint(want[key]) {
			t.Errorf("file %s (tuple %s) ids %v, want %v", f.Path, key, ids, want[key])
		}
		if f.Rows != int64(len(ids)) {
			t.Errorf("file %s Rows %d, read %d", f.Path, f.Rows, len(ids))
		}
	}
	if !strings.Contains(files[0].Path, "/data/name=a/ts_day=2024-01-01/part-unit-w1-00000.parquet") {
		t.Errorf("first file path %s, want .../data/name=a/ts_day=2024-01-01/part-unit-w1-00000.parquet", files[0].Path)
	}
}

func TestPartitionedCommitTuplesMatchRows(t *testing.T) {
	pt := newPartTable(t, eventSchema, identityDay()...)
	b1, b2 := identityDayRows()
	pt.commit(pt.write(pt.spec(), events(b1...), events(b2...)), icebergjob.WriteModeAppend)

	byID := map[int64]evRow{}
	for _, r := range append(append([]evRow{}, b1...), b2...) {
		byID[r.id] = r
	}
	tb := pt.reload()
	tasks, err := tb.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, task := range tasks {
		got := tupleString(tb, task.File.Partition())
		for _, id := range readIDs(t, task.File.FilePath()) {
			seen++
			if want := expectedTuple(byID[id]); got != want {
				t.Errorf("file %s: committed tuple %s, row %d has %s", task.File.FilePath(), got, id, want)
			}
		}
		if want := tb.Metadata().DefaultPartitionSpec(); int(task.File.SpecID()) != want {
			t.Errorf("file spec id %d, table spec %d", task.File.SpecID(), want)
		}
	}
	if seen != len(byID) {
		t.Fatalf("committed %d rows, want %d", seen, len(byID))
	}
}

func TestPartitionedScanPrunesByFilter(t *testing.T) {
	pt := newPartTable(t, eventSchema, identityDay()...)
	b1, b2 := identityDayRows()
	pt.commit(pt.write(pt.spec(), events(b1...), events(b2...)), icebergjob.WriteModeAppend)

	all, allIDs := pt.scan(nil)
	n, ids := pt.scan(iceberg.EqualTo(iceberg.Reference("name"), "a"))
	if n >= all || n != 2 {
		t.Fatalf("filtered scan planned %d of %d files, want the 2 files of name=a", n, all)
	}
	if fmt.Sprint(ids) != "[1 3 4 6]" || len(allIDs) != 7 {
		t.Fatalf("filtered ids %v (all %v), want [1 3 4 6]", ids, allIDs)
	}
	n, ids = pt.scan(iceberg.LessThan(iceberg.Reference("ts"), iceberg.Timestamp(day0)))
	if n != 1 || fmt.Sprint(ids) != "[7]" {
		t.Fatalf("ts filter planned %d files ids %v, want 1 file [7]", n, ids)
	}
}

// Iceberg spec appendix test vectors: bucket hash of int 34 is 2017239379,
// of "iceberg" 1210000089; bucket = (hash & MaxInt32) % N.
func TestBucketTruncateSpecVectors(t *testing.T) {
	bucket := func(hash uint32, n int32) int32 { return int32(hash&math.MaxInt32) % n }
	long := "abcdefghijklmnopqrstuvwxyz" // > 16 chars
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	pt := newPartTable(t, sch,
		lakekeeper.PartitionField{SourceColumn: "id", Transform: "bucket[16]"},
		lakekeeper.PartitionField{SourceColumn: "s", Transform: "bucket[16]"},
		lakekeeper.PartitionField{SourceColumn: "s", Transform: "truncate[4]"},
	)
	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, sch)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{34, 34}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"iceberg", long}, nil)
	files := pt.write(pt.spec(), b.NewRecordBatch())
	if len(files) != 2 {
		t.Fatalf("files = %+v, want 2", files)
	}
	spec := pt.tbl.Spec()
	idB, sB, sT := spec.Field(0).FieldID, spec.Field(1).FieldID, spec.Field(2).FieldID
	if names := []string{spec.Field(0).Name, spec.Field(1).Name, spec.Field(2).Name}; strings.Join(names, ",") != "id_bucket_16,s_bucket_16,s_trunc_4" {
		t.Errorf("partition field names = %v", names)
	}
	f0, f1 := files[0].Partition, files[1].Partition
	if f0[idB] != bucket(2017239379, 16) || f1[idB] != bucket(2017239379, 16) {
		t.Errorf("bucket[16](34) = %v, %v; want %d", f0[idB], f1[idB], bucket(2017239379, 16))
	}
	if f0[sB] != bucket(1210000089, 16) {
		t.Errorf("bucket[16](iceberg) = %v, want %d", f0[sB], bucket(1210000089, 16))
	}
	if f0[sT] != "iceb" || f1[sT] != "abcd" {
		t.Errorf("truncate[4] = %q, %q; want iceb, abcd", f0[sT], f1[sT])
	}
	pt.commit(files, icebergjob.WriteModeAppend)
	if n, ids := pt.scan(iceberg.EqualTo(iceberg.Reference("s"), long)); n != 1 || len(ids) != 1 {
		t.Fatalf("scan s=long planned %d files, %d rows; want 1, 1", n, len(ids))
	}
}

// splitOne resolves spec over (sc, arrow schema) and returns the tuple of
// row 0 of rec.
func splitOne(t *testing.T, spec iceberg.PartitionSpec, sc *iceberg.Schema, rec arrow.RecordBatch) map[int]any {
	t.Helper()
	defer rec.Release()
	p, err := newPartitioner(spec, sc, rec.Schema())
	if err != nil {
		t.Fatalf("newPartitioner: %v", err)
	}
	groups, err := p.split(rec)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	return groups[0].tuple
}

func oneColumn(dt arrow.DataType, appendFn func(array.Builder)) arrow.RecordBatch {
	sch := arrow.NewSchema([]arrow.Field{{Name: "c", Type: dt, Nullable: true}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), sch)
	defer b.Release()
	appendFn(b.Field(0))
	return b.NewRecordBatch()
}

func specOn(t *testing.T, it iceberg.Type, transforms ...iceberg.Transform) (iceberg.PartitionSpec, *iceberg.Schema) {
	t.Helper()
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "c", Type: it})
	var opts []iceberg.PartitionOption
	for i, tr := range transforms {
		id := 1000 + i
		opts = append(opts, iceberg.AddPartitionFieldByName("c", fmt.Sprintf("p%d", i), tr, sc, &id))
	}
	spec, err := iceberg.NewPartitionSpecOpts(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return spec, sc
}

func TestPartitionTypeConversions(t *testing.T) {
	ident, day, hour := iceberg.IdentityTransform{}, iceberg.DayTransform{}, iceberg.HourTransform{}
	tsms := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
	tsns := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}

	t.Run("date32", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.Date, ident)
		got := splitOne(t, spec, sc, oneColumn(arrow.FixedWidthTypes.Date32, func(b array.Builder) {
			b.(*array.Date32Builder).Append(19000)
		}))
		if got[1000] != iceberg.Date(19000) {
			t.Fatalf("identity(date32 19000) = %#v", got[1000])
		}
	})
	t.Run("date64", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.Date, ident)
		got := splitOne(t, spec, sc, oneColumn(arrow.FixedWidthTypes.Date64, func(b array.Builder) {
			b.(*array.Date64Builder).Append(-1) // 1 ms before the epoch: day -1 (floor)
		}))
		if got[1000] != iceberg.Date(-1) {
			t.Fatalf("identity(date64 -1ms) = %#v, want Date(-1)", got[1000])
		}
	})
	t.Run("timestamp[ms] into timestamptz", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.TimestampTz, ident, day)
		got := splitOne(t, spec, sc, oneColumn(tsms, func(b array.Builder) {
			b.(*array.TimestampBuilder).Append(-1) // -1 ms
		}))
		if got[1000] != iceberg.Timestamp(-1000) || got[1001] != iceberg.Date(-1) {
			t.Fatalf("tuple of -1ms = %#v", got)
		}
	})
	t.Run("timestamp[ns] into timestamp", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.Timestamp, ident, hour)
		got := splitOne(t, spec, sc, oneColumn(tsns, func(b array.Builder) {
			b.(*array.TimestampBuilder).Append(-1) // -1 ns floors to -1 µs
		}))
		if got[1000] != iceberg.Timestamp(-1) || got[1001] != int32(-1) {
			t.Fatalf("identity(-1ns) = %#v, want Timestamp(-1)", got[1000])
		}
	})
	t.Run("timestamptz table column path", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.TimestampTz, ident)
		rec := oneColumn(tsUTC, func(b array.Builder) { b.(*array.TimestampBuilder).Append(arrow.Timestamp(day0)) })
		defer rec.Release()
		p, err := newPartitioner(spec, sc, rec.Schema())
		if err != nil {
			t.Fatal(err)
		}
		groups, _ := p.split(rec)
		if path := p.path(groups[0].tuple); path != "p0=2024-01-01T00%3A00%3A00%2B00%3A00" {
			t.Fatalf("path = %s", path)
		}
	})
	t.Run("int16 into int", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.Int32, ident)
		got := splitOne(t, spec, sc, oneColumn(arrow.PrimitiveTypes.Int16, func(b array.Builder) {
			b.(*array.Int16Builder).Append(-7)
		}))
		if got[1000] != int32(-7) {
			t.Fatalf("identity(int16 -7) = %#v", got[1000])
		}
	})
	t.Run("uint16 on a new table is long", func(t *testing.T) {
		sch := arrow.NewSchema([]arrow.Field{{Name: "c", Type: arrow.PrimitiveTypes.Uint16, Nullable: true}}, nil)
		pt := newPartTable(t, sch,
			lakekeeper.PartitionField{SourceColumn: "c", Transform: "bucket[4]"},
			lakekeeper.PartitionField{SourceColumn: "c", Transform: "truncate[10]"})
		if _, ok := pt.tbl.Schema().Field(0).Type.(iceberg.Int64Type); !ok {
			t.Fatalf("table column type %s, want long", pt.tbl.Schema().Field(0).Type)
		}
		spec := pt.tbl.Spec()
		got := splitOne(t, spec, pt.tbl.Schema(), oneColumn(arrow.PrimitiveTypes.Uint16, func(b array.Builder) {
			b.(*array.Uint16Builder).Append(1234)
		}))
		want := iceberg.BucketTransform{NumBuckets: 4}.Transformer(iceberg.PrimitiveTypes.Int64)(int64(1234)).Val
		if got[spec.Field(0).FieldID] != want || got[spec.Field(1).FieldID] != int64(1230) {
			t.Fatalf("tuple = %#v, want bucket %d, truncate 1230", got, want)
		}
	})
	t.Run("int32 into an existing long column", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.Int64, ident, iceberg.BucketTransform{NumBuckets: 16})
		got := splitOne(t, spec, sc, oneColumn(arrow.PrimitiveTypes.Int32, func(b array.Builder) {
			b.(*array.Int32Builder).Append(34)
		}))
		if got[1000] != int64(34) || got[1001] != int32(2017239379&math.MaxInt32)%16 {
			t.Fatalf("tuple = %#v", got)
		}
	})
	t.Run("identity on float64", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.Float64, ident)
		rec := oneColumn(arrow.PrimitiveTypes.Float64, func(b array.Builder) {
			b.(*array.Float64Builder).AppendValues([]float64{1.5, math.NaN(), 1.5, math.Copysign(math.NaN(), -1)}, nil)
		})
		defer rec.Release()
		p, err := newPartitioner(spec, sc, rec.Schema())
		if err != nil {
			t.Fatal(err)
		}
		groups, err := p.split(rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(groups) != 2 || groups[0].tuple[1000] != 1.5 || len(groups[1].rows) != 2 {
			t.Fatalf("groups = %+v, want {1.5} and one NaN group of 2", groups)
		}
	})
	t.Run("uint64 above MaxInt64 is a write error", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.Int64, ident)
		rec := oneColumn(arrow.PrimitiveTypes.Uint64, func(b array.Builder) { b.(*array.Uint64Builder).Append(math.MaxUint64) })
		defer rec.Release()
		p, err := newPartitioner(spec, sc, rec.Schema())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.split(rec); err == nil || !strings.Contains(err.Error(), "does not fit") {
			t.Fatalf("split err = %v", err)
		}
	})
	t.Run("unsupported pair is InvalidArgument at resolve", func(t *testing.T) {
		spec, sc := specOn(t, iceberg.PrimitiveTypes.String, ident)
		rec := oneColumn(arrow.PrimitiveTypes.Int64, func(b array.Builder) { b.(*array.Int64Builder).Append(1) })
		defer rec.Release()
		_, err := newPartitioner(spec, sc, rec.Schema())
		if !errors.Is(err, datagateway.ErrInvalidPartition) || !strings.Contains(err.Error(), `column "c" has Arrow type int64`) {
			t.Fatalf("err = %v, want ErrInvalidPartition naming the column and types", err)
		}
		p := New(Config{})
		defer p.Close()
		_, err = p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
			DataPrefix: "file:///unused/", FilePrefix: "x", Schema: rec.Schema(), PartitionSpec: &spec, TableSchema: sc,
		})
		if !errors.Is(err, datagateway.ErrInvalidPartition) {
			t.Fatalf("OpenTableWriter err = %v, want ErrInvalidPartition", err)
		}
	})
}

func TestPartitionNullAndVoidRoundTrip(t *testing.T) {
	pt := newPartTable(t, eventSchema,
		lakekeeper.PartitionField{SourceColumn: "name", Transform: "identity"},
		lakekeeper.PartitionField{SourceColumn: "id", Transform: "void"})
	files := pt.write(pt.spec(), events(evRow{1, "a", day0}, evRow{2, "", day0}, evRow{3, "", day0}))
	if len(files) != 2 {
		t.Fatalf("files = %+v, want 2", files)
	}
	spec := pt.tbl.Spec()
	nameID, voidID := spec.Field(0).FieldID, spec.Field(1).FieldID
	for _, f := range files {
		if v, ok := f.Partition[voidID]; !ok || v != nil {
			t.Fatalf("void field %d: %v present %v; want a nil entry", voidID, v, ok)
		}
		if _, ok := f.Partition[nameID]; !ok {
			t.Fatalf("tuple %v has no entry for field %d", f.Partition, nameID)
		}
	}
	if !strings.Contains(files[1].Path, "/name=null/id_null=null/") {
		t.Errorf("null tuple path %s", files[1].Path)
	}
	pt.commit(files, icebergjob.WriteModeAppend)

	if _, ids := pt.scan(nil); fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("scan ids %v, want [1 2 3]", ids)
	}
	n, ids := pt.scan(iceberg.IsNull(iceberg.Reference("name")))
	if n != 1 || fmt.Sprint(ids) != "[2 3]" {
		t.Fatalf("IsNull(name) planned %d files, ids %v; want 1 file, [2 3]", n, ids)
	}
	tasks, err := pt.reload().Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		part := task.File.Partition()
		if v, ok := part[voidID]; !ok || v != nil {
			t.Fatalf("committed void value %v (present %v)", v, ok)
		}
	}
}

func TestExistingTableSpecWins(t *testing.T) {
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := cat.CreateNamespace(ctx, catalog.ToIdentifier("ns"), nil); err != nil {
		t.Fatal(err)
	}
	// Created outside the gateway with ids the catalog reassigns.
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 10, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 11, Name: "name", Type: iceberg.PrimitiveTypes.String})
	fid := 5000
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("name", "name", iceberg.IdentityTransform{}, sc, &fid))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable(ctx, catalog.ToIdentifier("ns", "tbl"), sc, catalog.WithPartitionSpec(&spec)); err != nil {
		t.Fatal(err)
	}
	pt := &ptab{t: t, cat: cat, ident: catalog.ToIdentifier("ns", "tbl"), sch: testSchema, p: New(Config{})}
	t.Cleanup(func() { _ = pt.p.Close() })
	pt.resolve(lakekeeper.PartitionField{SourceColumn: "id", Transform: "bucket[4]"})

	if !strings.Contains(pt.warn, "[bucket[4](id)]") || !strings.Contains(pt.warn, "[identity(name)]") {
		t.Fatalf("warning = %q, want both the configured and the table spec", pt.warn)
	}
	tspec := pt.tbl.Spec()
	if tspec.NumFields() != 1 {
		t.Fatalf("table spec = %v", tspec)
	}
	tableFieldID := tspec.Field(0).FieldID
	files := pt.write(pt.spec(), batch(testSchema, 0, 3))
	if len(files) != 3 {
		t.Fatalf("files = %d, want 3 (identity(name), not bucket(id))", len(files))
	}
	for _, f := range files {
		if len(f.Partition) != 1 || f.Partition[tableFieldID] == nil || f.SpecID != tspec.ID() {
			t.Fatalf("file tuple %v spec %d, want field %d of spec %d", f.Partition, f.SpecID, tableFieldID, tspec.ID())
		}
	}
	pt.commit(files, icebergjob.WriteModeAppend)
	if _, ids := pt.scan(iceberg.EqualTo(iceberg.Reference("name"), "row-1-x")); fmt.Sprint(ids) != "[1]" {
		t.Fatalf("scan name=row-1-x ids %v", ids)
	}

	// Configured fields equal to the table's: no warning.
	pt.resolve(lakekeeper.PartitionField{SourceColumn: "name", Transform: "identity"})
	if pt.warn != "" {
		t.Fatalf("warning for matching fields: %q", pt.warn)
	}
}

// nameBatch builds testSchema rows with the given names (ids from start).
func nameBatch(start int, names ...string) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.NewGoAllocator(), testSchema)
	defer b.Release()
	for i, n := range names {
		b.Field(0).(*array.Int64Builder).Append(int64(start + i))
		b.Field(1).(*array.StringBuilder).Append(n)
	}
	return b.NewRecordBatch()
}

func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return buf
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestOpenPartitionCapEvicts(t *testing.T) {
	logs := captureLog(t)
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pt := &ptab{t: t, cat: cat, ident: catalog.ToIdentifier("ns", "tbl"), sch: testSchema, p: New(Config{MaxOpenPartitions: 2})}
	t.Cleanup(func() { _ = pt.p.Close() })
	pt.resolve(lakekeeper.PartitionField{SourceColumn: "name", Transform: "identity"})

	files := pt.write(pt.spec(),
		nameBatch(0, "a", "b"),
		nameBatch(2, "c", "a"),
		nameBatch(4, "b", "c"),
		nameBatch(6, "a", "b", "c"))
	if got := strings.Count(logs.String(), "partition files open"); got != 1 {
		t.Fatalf("eviction warnings = %d, want 1; log:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "table ns.tbl: 2 partition files open") || !strings.Contains(logs.String(), "after 3 distinct partition tuples") {
		t.Errorf("warning lacks table, cap or tuple count: %s", logs.String())
	}
	if len(files) <= 3 {
		t.Fatalf("files = %d, want more than one per tuple after evictions", len(files))
	}
	pt.commit(files, icebergjob.WriteModeAppend)
	if _, ids := pt.scan(nil); fmt.Sprint(ids) != "[0 1 2 3 4 5 6 7 8]" {
		t.Fatalf("committed ids %v, want each of 0..8 exactly once", ids)
	}
	spec := pt.tbl.Spec()
	for _, f := range files {
		name := f.Partition[spec.Field(0).FieldID].(string)
		for _, id := range readIDs(t, f.Path) {
			if want := string("abc"[id%3]); want != name {
				t.Errorf("row %d (name %s) in the file of %s", id, want, name)
			}
		}
	}
}

func TestPartitionedWriterUniqueFileNames(t *testing.T) {
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pt := &ptab{t: t, cat: cat, ident: catalog.ToIdentifier("ns", "tbl"), sch: testSchema, p: New(Config{MaxOpenPartitions: 2})}
	t.Cleanup(func() { _ = pt.p.Close() })
	// One void tuple: every file shares one directory.
	pt.resolve(lakekeeper.PartitionField{SourceColumn: "name", Transform: "void"})
	spec := pt.spec()
	spec.TargetFileBytes = 1 // every sub-batch rotates
	var recs []arrow.RecordBatch
	for i := 0; i < 5; i++ {
		recs = append(recs, nameBatch(i*3, "a", "b", "c"))
	}
	files := pt.write(spec, recs...)
	if len(files) != 5 {
		t.Fatalf("files = %d, want 5 (one rotation per batch, all one void tuple)", len(files))
	}
	paths, bases := map[string]bool{}, map[string]bool{}
	for _, f := range files {
		if paths[f.Path] || bases[filepath.Base(f.Path)] {
			t.Fatalf("duplicate file name %s in %v", f.Path, files)
		}
		paths[f.Path], bases[filepath.Base(f.Path)] = true, true
	}

	// Identity tuples with rotation and eviction: one writer-wide sequence.
	pt2 := newPartTable(t, testSchema, lakekeeper.PartitionField{SourceColumn: "name", Transform: "identity"})
	pt2.p.cfg.MaxOpenPartitions = 2
	spec2 := pt2.spec()
	spec2.TargetFileBytes = 1
	files = pt2.write(spec2, nameBatch(0, "a", "b", "c", "a"), nameBatch(4, "c", "b", "a"))
	seqs := map[string]bool{}
	for _, f := range files {
		seq := filepath.Base(f.Path)
		if seqs[seq] {
			t.Fatalf("file name %s used twice: %v", seq, files)
		}
		seqs[seq] = true
	}
	if len(files) != 6 {
		t.Fatalf("files = %d, want 6 (one per sub-batch)", len(files))
	}
}

// abortSink accepts writes; its abort (Close with a cancelled upload
// context) waits until `want` aborts are in flight at once, or 2 s.
type abortSink struct {
	ctx        context.Context
	inFlight   *atomic.Int32
	want       int32
	closes     atomic.Int32
	cancelled  atomic.Bool
	concurrent atomic.Bool
}

func (s *abortSink) Write(p []byte) (int, error) { return len(p), nil }

func (s *abortSink) Close() error {
	s.closes.Add(1)
	if s.ctx.Err() == nil {
		return nil
	}
	s.cancelled.Store(true)
	s.inFlight.Add(1)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.inFlight.Load() >= s.want {
			s.concurrent.Store(true)
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

func TestPartitionedAbortCleansAllOpenFiles(t *testing.T) {
	sc := readSchema
	id := 1000
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("name", "name", iceberg.IdentityTransform{}, sc, &id))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		stop func(*Plane, datagateway.TableWriter)
	}{
		{"Abort", func(_ *Plane, w datagateway.TableWriter) { w.Abort() }},
		{"Interrupt", func(p *Plane, w datagateway.TableWriter) { p.Interrupt(); <-w.(*writer).done }},
		{"PlaneClose", func(p *Plane, _ datagateway.TableWriter) { _ = p.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := runtime.NumGoroutine()
			p := New(Config{})
			var inFlight atomic.Int32
			var mu sync.Mutex
			var sinks []*abortSink
			p.openSink = func(ctx context.Context, _ map[string]string, _ string) (io.WriteCloser, error) {
				s := &abortSink{ctx: ctx, inFlight: &inFlight, want: 3}
				mu.Lock()
				sinks = append(sinks, s)
				mu.Unlock()
				return s, nil
			}
			w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: "file:///unused/", PartitionSpec: &spec, TableSchema: sc})
			write(t, w, nameBatch(0, "a", "b", "c"))
			deadline := time.Now().Add(5 * time.Second)
			for {
				mu.Lock()
				n := len(sinks)
				mu.Unlock()
				if n == 3 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("writer opened %d files, want 3", n)
				}
				time.Sleep(5 * time.Millisecond)
			}
			tc.stop(p, w)
			for i, s := range sinks {
				if s.closes.Load() != 1 || !s.cancelled.Load() {
					t.Errorf("sink %d: closes %d, aborted %v; want one abort", i, s.closes.Load(), s.cancelled.Load())
				}
				if !s.concurrent.Load() {
					t.Errorf("sink %d: abort did not overlap the others (aborts ran sequentially)", i)
				}
			}
			if _, err := w.Close(context.Background()); err == nil {
				t.Error("Close after the stop succeeded")
			}
			w.Abort()
			_ = p.Close()
			waitGoroutines(t, base)
		})
	}

	t.Run("no local partial objects", func(t *testing.T) {
		p := New(Config{})
		defer p.Close()
		prefix := localPrefix(t)
		w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: prefix, RowGroupRows: 2, PartitionSpec: &spec, TableSchema: sc})
		write(t, w, nameBatch(0, "a", "b", "c", "a", "b", "c", "a", "b", "c"))
		deadline := time.Now().Add(5 * time.Second)
		for {
			m, _ := filepath.Glob(localPath(prefix) + "name=*/*.parquet")
			if len(m) == 3 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("partial files %v, want 3", m)
			}
			time.Sleep(5 * time.Millisecond)
		}
		w.Abort()
		if m, _ := filepath.Glob(localPath(prefix) + "name=*/*.parquet"); len(m) != 0 {
			t.Fatalf("files left after Abort: %v", m)
		}
	})
}

// partSink fails every Write after the header when fail is set.
type partSink struct {
	ctx       context.Context
	fail      bool
	writes    int
	closes    atomic.Int32
	cancelled atomic.Bool
}

func (s *partSink) Write(p []byte) (int, error) {
	if s.writes++; s.fail && s.writes > 1 {
		return 0, errors.New("injected upload failure")
	}
	return len(p), nil
}

func (s *partSink) Close() error {
	s.closes.Add(1)
	s.cancelled.Store(s.ctx.Err() != nil)
	return nil
}

func TestPartitionedOneUploadErrorAbortsAll(t *testing.T) {
	base := runtime.NumGoroutine()
	sc := readSchema
	id := 1000
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("name", "name", iceberg.IdentityTransform{}, sc, &id))
	if err != nil {
		t.Fatal(err)
	}
	p := New(Config{})
	var mu sync.Mutex
	sinks := map[string]*partSink{}
	p.openSink = func(ctx context.Context, _ map[string]string, path string) (io.WriteCloser, error) {
		s := &partSink{ctx: ctx, fail: strings.Contains(path, "/name=b/")}
		mu.Lock()
		sinks[path] = s
		mu.Unlock()
		return s, nil
	}
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: "file:///unused/", RowGroupRows: 2, PartitionSpec: &spec, TableSchema: sc})
	var werr error
	for i := 0; i < 20 && werr == nil; i++ {
		rec := nameBatch(i*6, "a", "a", "b", "b", "c", "c")
		werr = w.Write(context.Background(), rec)
		rec.Release()
	}
	files, cerr := w.Close(context.Background())
	if cerr == nil || !strings.Contains(cerr.Error(), "injected upload failure") || files != nil {
		t.Fatalf("Close = %v, %v; want the upload failure", files, cerr)
	}
	if len(sinks) != 3 {
		t.Fatalf("sinks = %d, want 3", len(sinks))
	}
	for path, s := range sinks {
		if s.closes.Load() != 1 || !s.cancelled.Load() {
			t.Errorf("%s: closes %d, aborted %v; want aborted once", path, s.closes.Load(), s.cancelled.Load())
		}
	}
	w.Abort()
	_ = p.Close()
	waitGoroutines(t, base)
}

func TestPartitionedRotationBytes(t *testing.T) {
	sc := readSchema
	id := 1000
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("name", "name", iceberg.IdentityTransform{}, sc, &id))
	if err != nil {
		t.Fatal(err)
	}
	const half = 50
	mk := func() arrow.RecordBatch {
		names := make([]string, 2*half)
		for i := range names {
			names[i] = map[bool]string{true: "a", false: "b"}[i < half] // contiguous halves
		}
		return nameBatch(0, names...)
	}
	probe := mk()
	parent := recordBytes(probe)
	probe.Release()
	// Each half counts parent/2; a file rotates once 4 x target >= 3 halves.
	target := (3 * parent / 2) / rotationFactor

	p := New(Config{})
	defer p.Close()
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), TargetFileBytes: target, PartitionSpec: &spec, TableSchema: sc})
	for i := 0; i < 6; i++ {
		write(t, w, mk())
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 4 {
		t.Fatalf("files = %d, want 4 (two per tuple)", len(files))
	}
	for _, f := range files {
		if f.Rows != 3*half {
			t.Errorf("file %s has %d rows, want %d (rotation after 3 half batches)", f.Path, f.Rows, 3*half)
		}
	}
}

func TestMaxOpenPartitionsEnv(t *testing.T) {
	for _, tc := range []struct {
		val  string
		set  bool
		want int
		warn bool
	}{
		{"", false, 16, false},
		{"2", true, 2, false},
		{"1024", true, 1024, false},
		{"0", true, 16, true},
		{"1025", true, 16, true},
		{"abc", true, 16, true},
	} {
		t.Run(tc.val, func(t *testing.T) {
			logs := captureLog(t)
			if tc.set {
				t.Setenv(maxOpenPartitionsEnv, tc.val)
			} else {
				t.Setenv(maxOpenPartitionsEnv, "")
			}
			dp, err := Factory(context.Background(), datagateway.BootInfo{})
			if err != nil {
				t.Fatal(err)
			}
			defer dp.Close()
			if got := dp.(*Plane).cfg.MaxOpenPartitions; got != tc.want {
				t.Errorf("MaxOpenPartitions = %d, want %d", got, tc.want)
			}
			if got := strings.Contains(logs.String(), maxOpenPartitionsEnv); got != tc.warn {
				t.Errorf("warning logged = %v, want %v (%q)", got, tc.warn, logs.String())
			}
		})
	}
}

func TestPartitionPathTooLong(t *testing.T) {
	pt := newPartTable(t, testSchema, lakekeeper.PartitionField{SourceColumn: "name", Transform: "identity"})
	w := openWriter(t, pt.p, pt.spec())
	rec := nameBatch(0, strings.Repeat("x", 1100))
	werr := w.Write(context.Background(), rec)
	rec.Release()
	_, cerr := w.Close(context.Background())
	err := errors.Join(werr, cerr)
	if err == nil || !strings.Contains(err.Error(), "1024-byte limit") || !strings.Contains(err.Error(), "partition field name") {
		t.Fatalf("err = %v, want the object key limit naming the field", err)
	}
	if m, _ := filepath.Glob(localPath(pt.spec().DataPrefix) + "*/*"); len(m) != 0 {
		t.Fatalf("files written: %v", m)
	}
}

// A spec field sourced from a nested column is never bound to a same-named
// top-level writer column.
func TestPartitionNestedSourceNotBound(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "meta", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		}}})
	fid := 1000
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("meta.id", "meta_id", iceberg.IdentityTransform{}, sc, &fid))
	if err != nil {
		t.Fatal(err)
	}
	writer := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	_, err = newPartitioner(spec, sc, writer)
	if !errors.Is(err, datagateway.ErrInvalidPartition) || !strings.Contains(err.Error(), `"meta.id"`) {
		t.Fatalf("err = %v, want ErrInvalidPartition naming meta.id", err)
	}
}
