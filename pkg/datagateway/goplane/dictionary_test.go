package goplane

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"

	"github.com/datuplet/datuplet/pkg/datagateway"
)

// Dictionary page limit of partitioned writers (spec decision 9, rev 6).

// cardSchema: partition key, high-cardinality float64 / int64 / string and a
// 16-value string.
var cardSchema = arrow.NewSchema([]arrow.Field{
	{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "f", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "i", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "cat", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

var cardIceberg = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "f", Type: iceberg.PrimitiveTypes.Float64},
	iceberg.NestedField{ID: 3, Name: "i", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 4, Name: "s", Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 5, Name: "cat", Type: iceberg.PrimitiveTypes.String},
)

func cardSpec(t *testing.T) iceberg.PartitionSpec {
	t.Helper()
	id := 1000
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("k", "k", iceberg.IdentityTransform{}, cardIceberg, &id))
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

// cardBatch is perTuple rows of each of tuples keys, interleaved, with
// random high-cardinality values.
func cardBatch(rng *rand.Rand, tuples, perTuple int) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.NewGoAllocator(), cardSchema)
	defer b.Release()
	for r := 0; r < perTuple; r++ {
		for k := 0; k < tuples; k++ {
			b.Field(0).(*array.Int64Builder).Append(int64(k))
			b.Field(1).(*array.Float64Builder).Append(rng.Float64())
			b.Field(2).(*array.Int64Builder).Append(rng.Int63())
			b.Field(3).(*array.StringBuilder).Append(fmt.Sprintf("%016x", rng.Uint64()))
			b.Field(4).(*array.StringBuilder).Append(fmt.Sprintf("category-%02d", rng.Intn(16)))
		}
	}
	return b.NewRecordBatch()
}

// plainDataPages reports, per column name, whether any data page of the
// file is PLAIN-encoded (a dictionary fallback), and which columns have a
// dictionary page in every row group.
func plainDataPages(t *testing.T, path string) (plain, dict map[string]bool) {
	t.Helper()
	rdr, err := file.OpenParquetFile(localPath(path), false)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	plain, dict = map[string]bool{}, map[string]bool{}
	for g := 0; g < rdr.NumRowGroups(); g++ {
		rg := rdr.MetaData().RowGroup(g)
		for c := 0; c < rg.NumColumns(); c++ {
			cc, err := rg.ColumnChunk(c)
			if err != nil {
				t.Fatal(err)
			}
			name := cc.PathInSchema().String()
			if g == 0 {
				dict[name] = cc.HasDictionaryPage()
			} else {
				dict[name] = dict[name] && cc.HasDictionaryPage()
			}
			stats := cc.EncodingStats()
			if len(stats) == 0 {
				t.Fatalf("%s: column %s has no page encoding stats", path, name)
			}
			for _, s := range stats {
				// format.PageType: DATA_PAGE 0, DATA_PAGE_V2 3.
				if (s.PageType == 0 || s.PageType == 3) && s.Encoding == parquet.Encodings.Plain {
					plain[name] = true
				}
			}
		}
	}
	return plain, dict
}

func TestPartitionedWriterDictionaryLimit(t *testing.T) {
	spec := cardSpec(t)
	// 2 tuples x 20 000 rows: 160 KB of distinct float64 / int64 per
	// partition file (over 64 KiB), 320 KB in the unpartitioned file (under
	// 1 MiB).
	for _, tc := range []struct {
		name  string
		ws    datagateway.WriterSpec
		plain map[string]bool
	}{
		{"partitioned", datagateway.WriterSpec{PartitionSpec: &spec, TableSchema: cardIceberg},
			map[string]bool{"f": true, "i": true, "s": true}},
		{"unpartitioned", datagateway.WriterSpec{}, map[string]bool{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(Config{})
			defer p.Close()
			ws := tc.ws
			ws.DataPrefix, ws.Schema = localPrefix(t), cardSchema
			w := openWriter(t, p, ws)
			rng := rand.New(rand.NewSource(1))
			for b := 0; b < 4; b++ {
				write(t, w, cardBatch(rng, 2, 5000))
			}
			files, err := w.Close(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range files {
				plain, dict := plainDataPages(t, f.Path)
				if fmt.Sprint(plain) != fmt.Sprint(tc.plain) {
					t.Errorf("%s: columns with PLAIN data pages %v, want %v", f.Path, plain, tc.plain)
				}
				for _, c := range []string{"k", "f", "i", "s", "cat"} {
					if !tc.plain[c] && !dict[c] {
						t.Errorf("%s: column %s has no dictionary page; want it dictionary-encoded", f.Path, c)
					}
				}
			}
		})
	}
}

func TestPartitionedWriterHeapBounded(t *testing.T) {
	spec := cardSpec(t)
	const tuples, perTuple, batches = 16, 1000, 50 // 50 000 rows per partition file
	peak := func(limit int64) uint64 {
		saved := partitionDictionaryPageBytes
		partitionDictionaryPageBytes = limit
		defer func() { partitionDictionaryPageBytes = saved }()
		p := New(Config{})
		defer p.Close()
		var mu sync.Mutex
		var max uint64
		events := 0
		p.budgetHook = func(ev budgetEvent) {
			if ev.site != "enforced" {
				return
			}
			if events++; events%64 != 0 {
				return
			}
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			mu.Lock()
			if ms.HeapInuse > max {
				max = ms.HeapInuse
			}
			mu.Unlock()
		}
		w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Schema: cardSchema, PartitionSpec: &spec, TableSchema: cardIceberg})
		rng := rand.New(rand.NewSource(2))
		for b := 0; b < batches; b++ {
			write(t, w, cardBatch(rng, tuples, perTuple))
		}
		files, err := w.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != tuples {
			t.Fatalf("files = %d, want %d", len(files), tuples)
		}
		mu.Lock()
		defer mu.Unlock()
		return max
	}
	runtime.GC()
	def := peak(parquet.DefaultDictionaryPageSizeLimit)
	capped := peak(64 << 10)
	t.Logf("peak live heap: default dictionary limit %d MiB, 64 KiB limit %d MiB", def>>20, capped>>20)
	if def == 0 || capped == 0 {
		t.Fatal("no heap samples")
	}
	if float64(capped) > 0.6*float64(def) {
		t.Fatalf("64 KiB limit peak heap %d bytes is over 60%% of the default limit's %d", capped, def)
	}
}
