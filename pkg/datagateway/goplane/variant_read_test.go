package goplane

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"

	"github.com/datuplet/datuplet/pkg/datagateway"
)

var variantReadSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.VariantType{}},
)

var variantArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "v", Type: extensions.NewDefaultVariantType(), Nullable: true},
}, nil)

func variantRowsBatch(t *testing.T, start, n int) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()
	ib := array.NewInt64Builder(mem)
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer ib.Release()
	defer vb.Release()
	for i := start; i < start+n; i++ {
		ib.Append(int64(i))
		v, err := variant.ParseJSON(`{"i":`+string(rune('0'+i%10))+`,"s":"x"}`, false)
		if err != nil {
			t.Fatal(err)
		}
		vb.Append(v)
	}
	ia, va := ib.NewArray(), vb.NewArray()
	defer ia.Release()
	defer va.Release()
	return array.NewRecordBatch(variantArrowSchema, []arrow.Array{ia, va}, int64(n))
}

// A deletion vector on one v3 file forces that file through the decoded
// path while the other is served raw; the canonical variant column survives
// on both.
func TestReadVariantRawAndDecodedWithDeletionVector(t *testing.T) {
	f := newTable(t, variantReadSchema, morProps("3"))
	var paths []string
	for i := 0; i < 2; i++ {
		f.files++
		w := openWriter(t, f.p, datagateway.WriterSpec{
			DataPrefix: f.dataPrefix(), FilePrefix: "part-variant-w" + string(rune('1'+i)), Schema: variantArrowSchema,
		})
		write(t, w, variantRowsBatch(t, i*10, 5))
		files, err := w.Close(context.Background())
		if err != nil || len(files) != 1 {
			t.Fatalf("writer Close = %+v, %v", files, err)
		}
		paths = append(paths, files[0].Path)
	}
	f.addFiles(paths...)
	f.deleteWhere(iceberg.EqualTo(iceberg.Reference("id"), int64(2)))
	if task := f.taskFor(paths[0]); len(task.DeletionVectorFiles) != 1 {
		t.Fatalf("task for %s has %d deletion vectors, want 1", paths[0], len(task.DeletionVectorFiles))
	}

	r := f.open(true)
	var rawFiles, decodedFiles int
	ids := map[int64]bool{}
	check := func(recs []arrow.RecordBatch, sch *arrow.Schema) {
		if !arrow.TypeEqual(sch.Field(1).Type, extensions.NewDefaultVariantType()) {
			t.Errorf("v type %s, want the canonical variant", sch.Field(1).Type)
		}
		for _, rec := range recs {
			va, ok := rec.Column(1).(*extensions.VariantArray)
			if !ok {
				t.Fatalf("v is %T", rec.Column(1))
			}
			for i := 0; i < int(rec.NumRows()); i++ {
				id := rec.Column(0).(*array.Int64).Value(i)
				ids[id] = true
				v, err := va.Value(i)
				if err != nil {
					t.Fatal(err)
				}
				js, _ := v.MarshalJSON()
				if want := `{"i":` + string(rune('0'+id%10)) + `,"s":"x"}`; string(js) != want {
					t.Errorf("id %d v = %s, want %s", id, js, want)
				}
			}
			rec.Release()
		}
	}
	for {
		part, err := r.NextPart(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if part.Raw != nil {
			rawFiles++
			rc, err := part.Raw.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			sch, recs := readParquetBytes(t, b)
			check(recs, sch)
			continue
		}
		decodedFiles++
		var recs []arrow.RecordBatch
		for {
			rec, err := part.Records.Next(context.Background())
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			recs = append(recs, rec)
		}
		check(recs, part.Records.Schema())
		part.Records.Close()
	}
	if rawFiles != 1 || decodedFiles != 1 {
		t.Errorf("%d raw and %d decoded files, want 1 and 1", rawFiles, decodedFiles)
	}
	want := map[int64]bool{0: true, 1: true, 3: true, 4: true, 10: true, 11: true, 12: true, 13: true, 14: true}
	if !reflect.DeepEqual(ids, want) {
		t.Errorf("ids read %v, want %v (id 2 deleted)", ids, want)
	}
}
