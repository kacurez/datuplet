package icebergjob

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
)

var metricsArrow = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
}, nil)

var metricsIceberg = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 3, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
)

// writeGroups writes one row group per entry of groups (an empty entry is
// a 0-row row group) and returns the local path. Row id: name null when
// id%5 == 0, score NaN when id%7 == 0.
func writeGroups(t *testing.T, name string, groups ...[]int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	sink, err := (iceio.LocalFS{}).Create(path)
	if err != nil {
		t.Fatal(err)
	}
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd), parquet.WithStats(true))
	fw, err := pqarrow.NewFileWriter(metricsArrow, sink, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		t.Fatal(err)
	}
	for gi, ids := range groups {
		if gi > 0 || len(ids) == 0 {
			if err := fw.NewBufferedRowGroupChecked(); err != nil {
				t.Fatal(err)
			}
		}
		if len(ids) == 0 {
			continue
		}
		b := array.NewRecordBuilder(memory.NewGoAllocator(), metricsArrow)
		for _, id := range ids {
			b.Field(0).(*array.Int64Builder).Append(id)
			if id%5 == 0 {
				b.Field(1).AppendNull()
			} else {
				b.Field(1).(*array.StringBuilder).Append("name-" + string(rune('a'+id%26)))
			}
			if id%7 == 0 {
				b.Field(2).(*array.Float64Builder).Append(math.NaN())
			} else {
				b.Field(2).(*array.Float64Builder).Append(float64(id) / 3)
			}
		}
		rec := b.NewRecordBatch()
		b.Release()
		if err := fw.WriteBuffered(rec); err != nil {
			t.Fatal(err)
		}
		rec.Release()
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func rowGroupRows(t *testing.T, path string) []int64 {
	t.Helper()
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	var out []int64
	for i := 0; i < rdr.NumRowGroups(); i++ {
		out = append(out, rdr.MetaData().RowGroup(i).NumRows())
	}
	return out
}

func metricsProps(t *testing.T) iceberg.Properties {
	t.Helper()
	m, err := json.Marshal(metricsIceberg.NameMapping())
	if err != nil {
		t.Fatal(err)
	}
	return iceberg.Properties{icebergtable.DefaultNameMappingKey: string(m)}
}

func fileToDataFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()
	df, err := icebergtable.FileToDataFile(context.Background(), iceio.LocalFS{}, path, metricsIceberg, *iceberg.UnpartitionedSpec, 0, metricsProps(t))
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func idRange(lo, hi int64) []int64 {
	var out []int64
	for i := lo; i < hi; i++ {
		out = append(out, i)
	}
	return out
}

// sameMetrics compares the column metrics and row count of got and want.
func sameMetrics(t *testing.T, got, want iceberg.DataFile) {
	t.Helper()
	for _, c := range []struct {
		name      string
		got, want any
	}{
		{"count", got.Count(), want.Count()},
		{"lower bounds", got.LowerBoundValues(), want.LowerBoundValues()},
		{"upper bounds", got.UpperBoundValues(), want.UpperBoundValues()},
		{"null counts", got.NullValueCounts(), want.NullValueCounts()},
		{"value counts", got.ValueCounts(), want.ValueCounts()},
		{"NaN counts", got.NaNValueCounts(), want.NaNValueCounts()},
		{"column sizes", got.ColumnSizes(), want.ColumnSizes()},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestFooterMetricsSkipEmptyRowGroups(t *testing.T) {
	rows := idRange(0, 500)
	ref := fileToDataFile(t, writeGroups(t, "ref.parquet", rows))
	if len(ref.LowerBoundValues()) != 3 || len(ref.NullValueCounts()) != 3 {
		t.Fatalf("reference metrics incomplete: bounds %v, nulls %v, NaN %v", ref.LowerBoundValues(), ref.NullValueCounts(), ref.NaNValueCounts())
	}
	for _, tc := range []struct {
		name   string
		groups [][]int64
		shape  string
	}{
		{"trailing empty", [][]int64{rows, nil}, "[500 0]"},
		{"leading empty", [][]int64{nil, rows}, "[0 500]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeGroups(t, "f.parquet", tc.groups...)
			if got := rowGroupRows(t, path); fmt.Sprint(got) != tc.shape {
				t.Fatalf("row groups %v, want %s", got, tc.shape)
			}
			raw := fileToDataFile(t, path)
			t.Logf("%s: FileToDataFile on the whole footer keeps %d lower bounds, %d null counts", tc.shape, len(raw.LowerBoundValues()), len(raw.NullValueCounts()))
			got, size, err := footerMetrics(context.Background(), iceio.LocalFS{}, path, metricsIceberg, metricsProps(t))
			if err != nil {
				t.Fatal(err)
			}
			sameMetrics(t, got, ref)
			if want := fileToDataFile(t, path).FileSizeBytes(); size != want {
				t.Errorf("size = %d, want the file's %d", size, want)
			}
			if n := len(got.SplitOffsets()); n != 1 {
				t.Errorf("split offsets %v, want one (the non-empty row group)", got.SplitOffsets())
			}
		})
	}

	t.Run("no empty groups matches FileToDataFile", func(t *testing.T) {
		path := writeGroups(t, "full.parquet", idRange(0, 300), idRange(300, 500))
		want := fileToDataFile(t, path)
		got, size, err := footerMetrics(context.Background(), iceio.LocalFS{}, path, metricsIceberg, metricsProps(t))
		if err != nil {
			t.Fatal(err)
		}
		sameMetrics(t, got, want)
		if size != want.FileSizeBytes() {
			t.Errorf("size = %d, want %d", size, want.FileSizeBytes())
		}
		if !reflect.DeepEqual(got.SplitOffsets(), want.SplitOffsets()) {
			t.Errorf("split offsets %v, want %v", got.SplitOffsets(), want.SplitOffsets())
		}
	})
}
