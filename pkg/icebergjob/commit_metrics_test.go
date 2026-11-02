package icebergjob

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	iofs "io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
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
	return writeGroupsWith(t, name, nil, groups...)
}

func writeGroupsWith(t *testing.T, name string, extra []parquet.WriterProperty, groups ...[]int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	sink, err := (iceio.LocalFS{}).Create(path)
	if err != nil {
		t.Fatal(err)
	}
	props := parquet.NewWriterProperties(append([]parquet.WriterProperty{parquet.WithCompression(compress.Codecs.Zstd), parquet.WithStats(true)}, extra...)...)
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

// retainedSplitOffsets is the split offset (as iceberg-go computes it) of
// each non-empty row group of the file at path.
func retainedSplitOffsets(t *testing.T, path string) []int64 {
	t.Helper()
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	var out []int64
	for i := 0; i < rdr.NumRowGroups(); i++ {
		rg := rdr.MetaData().RowGroup(i)
		if rg.NumRows() == 0 {
			continue
		}
		cc, err := rg.ColumnChunk(0)
		if err != nil {
			t.Fatal(err)
		}
		if cc.HasDictionaryPage() && cc.DictionaryPageOffset() < cc.DataPageOffset() {
			out = append(out, cc.DictionaryPageOffset())
		} else {
			out = append(out, cc.DataPageOffset())
		}
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
			if want := retainedSplitOffsets(t, path); len(want) != 1 || !reflect.DeepEqual(got.SplitOffsets(), want) {
				t.Errorf("split offsets %v, want the non-empty row group's %v", got.SplitOffsets(), want)
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

// An encrypted file whose footer is plaintext opens without keys; its
// metrics are refused with an error, not a panic.
func TestFooterMetricsRejectsEncryptedPlaintextFooter(t *testing.T) {
	enc := parquet.NewFileEncryptionProperties("0123456789012345", parquet.WithPlaintextFooter())
	path := writeGroupsWith(t, "enc.parquet", []parquet.WriterProperty{parquet.WithEncryptionProperties(enc)}, idRange(0, 100), nil)
	_, _, err := footerMetrics(context.Background(), iceio.LocalFS{}, path, metricsIceberg, metricsProps(t))
	if err == nil || !strings.Contains(err.Error(), "unsupported encrypted parquet file") {
		t.Fatalf("err = %v, want unsupported encrypted parquet file", err)
	}
}

// footerMetrics turns a panic while reading a footer into an error.
func TestFooterMetricsRecoversPanic(t *testing.T) {
	_, _, err := footerMetrics(context.Background(), panicIO{}, "x.parquet", metricsIceberg, metricsProps(t))
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want the recovered panic", err)
	}
}

type panicIO struct{}

func (panicIO) Open(string) (iceio.File, error) { panic("boom") }
func (panicIO) Remove(string) error             { return nil }

// negativeChildrenFooter is a Parquet file whose footer (Thrift compact)
// declares a root schema element with num_children = -1; arrow-go's reader
// constructor panics on it.
func negativeChildrenFooter() []byte {
	meta := []byte{
		0x15, 0x02, // 1: version = 1
		0x19, 0x1c, // 2: schema, list of 1 struct
		0x48, 0x04, 'r', 'o', 'o', 't', // 4: name = "root"
		0x15, 0x01, // 5: num_children = -1
		0x00,       // end SchemaElement
		0x16, 0x00, // 3: num_rows = 0
		0x19, 0x0c, // 4: row_groups, empty list
		0x00, // end FileMetaData
	}
	out := append([]byte("PAR1"), meta...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(meta)))
	return append(out, "PAR1"...)
}

// countingIO serves data as every file and counts closes.
type countingIO struct {
	data   []byte
	closes *atomic.Int32
}

func (c countingIO) Open(name string) (iceio.File, error) {
	return &countingFile{Reader: bytes.NewReader(c.data), closes: c.closes, size: int64(len(c.data))}, nil
}
func (countingIO) Remove(string) error { return nil }

type countingFile struct {
	*bytes.Reader
	closes *atomic.Int32
	size   int64
}

func (f *countingFile) Stat() (iofs.FileInfo, error) { return footerInfo{"f.parquet", f.size}, nil }
func (f *countingFile) Close() error                 { f.closes.Add(1); return nil }

// A reader constructor panic is recovered into an error and the opened file
// is closed exactly once; a valid file is closed exactly once too.
func TestFooterMetricsClosesFileOnce(t *testing.T) {
	var closes atomic.Int32
	df, size, err := footerMetrics(context.Background(), countingIO{data: negativeChildrenFooter(), closes: &closes}, "f.parquet", metricsIceberg, metricsProps(t))
	if err == nil || !strings.Contains(err.Error(), "parquet footer:") || df != nil || size != 0 {
		t.Fatalf("footerMetrics = %v, %d, %v; want nil, 0 and the recovered constructor panic", df, size, err)
	}
	if n := closes.Load(); n != 1 {
		t.Fatalf("file closed %d times, want 1", n)
	}

	valid, err := os.ReadFile(writeGroups(t, "ok.parquet", idRange(0, 10)))
	if err != nil {
		t.Fatal(err)
	}
	closes.Store(0)
	if _, _, err := footerMetrics(context.Background(), countingIO{data: valid, closes: &closes}, "f.parquet", metricsIceberg, metricsProps(t)); err != nil {
		t.Fatal(err)
	}
	if n := closes.Load(); n != 1 {
		t.Fatalf("valid file closed %d times, want 1", n)
	}
}
