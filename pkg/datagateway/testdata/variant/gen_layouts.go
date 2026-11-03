//go:build ignore

// gen_layouts writes the unshredded, non-canonical variant fixtures as Arrow
// IPC streams (Parquet does not keep these storage layouts on read):
//
//	large_binary.arrow:  struct<metadata: large_binary, value: large_binary>
//	dict_metadata.arrow: struct<metadata: dictionary<int32, binary>, value: binary>
//
// Both hold id int64 and v variant with the rows of variantFixtureRows.
//
// Regenerate from the repository root:
//
//	go run ./pkg/datagateway/testdata/variant/gen_layouts.go pkg/datagateway/testdata/variant
package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
)

// rows; "" is SQL NULL.
var rows = []string{`{"a":1,"b":"x","n":9007199254740993}`, `[1,"two",3.5]`, `"hello"`, ``}

func write(path string, storage arrow.DataType) {
	vt, err := extensions.NewVariantType(storage)
	if err != nil {
		log.Fatal(err)
	}
	mem := memory.DefaultAllocator
	vb := extensions.NewVariantBuilder(mem, vt)
	ib := array.NewInt64Builder(mem)
	for i, js := range rows {
		ib.Append(int64(i + 1))
		if js == "" {
			vb.AppendNull()
			continue
		}
		v, err := variant.ParseJSON(js, false)
		if err != nil {
			log.Fatal(err)
		}
		vb.Append(v)
	}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: vt, Nullable: true},
	}, nil)
	rec := array.NewRecordBatch(sc, []arrow.Array{ib.NewArray(), vb.NewArray()}, int64(len(rows)))
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	w := ipc.NewWriter(f, ipc.WithSchema(sc))
	if err := w.Write(rec); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	dir := os.Args[1]
	write(filepath.Join(dir, "large_binary.arrow"), arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.LargeBinary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.LargeBinary}))
	write(filepath.Join(dir, "dict_metadata.arrow"), arrow.StructOf(
		arrow.Field{Name: "metadata", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary}},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary}))
}
