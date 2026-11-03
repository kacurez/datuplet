package datagateway

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icebergtable "github.com/apache/iceberg-go/table"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
	dglakekeeper "github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// variantJSON renders a variant column row by row: "NULL" for SQL NULL,
// otherwise the value's JSON.
func variantJSON(t *testing.T, col arrow.Array) []string {
	t.Helper()
	va, ok := col.(*extensions.VariantArray)
	if !ok {
		t.Fatalf("column is %T (%s), want a variant array", col, col.DataType())
	}
	out := make([]string, va.Len())
	for i := range out {
		if va.IsNull(i) {
			out[i] = "NULL"
			continue
		}
		v, err := va.Value(i)
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		b, err := v.MarshalJSON()
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		out[i] = string(b)
	}
	return out
}

// textBatch is (id int64, v utf8) with v the given texts; "" is null.
func textBatch(t *testing.T, texts ...string) arrow.RecordBatch {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer b.Release()
	for i, s := range texts {
		b.Field(0).(*array.Int64Builder).Append(int64(i + 1))
		if s == "" {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append(s)
		}
	}
	return b.NewRecordBatch()
}

func variantColumnsConfig(cols ...ColumnConfig) *Config {
	return &Config{
		DefaultBucket: "raw",
		OutputTables:  []OutputTableConfig{{Name: "orders", Bucket: "raw", Columns: cols}},
	}
}

// writtenBatches returns the batches of the only plane writer.
func writtenBatches(t *testing.T, p *fakeDataPlane) (*fakeTableWriter, []arrow.RecordBatch) {
	t.Helper()
	if len(p.writers) != 1 {
		t.Fatalf("plane writers = %d, want 1", len(p.writers))
	}
	w := p.writers[0]
	return w, w.recorded()
}

func mustOK(t *testing.T, s *ServerV2, writerID string, body []byte) {
	t.Helper()
	if resp := httpWrite(s, writerID, body); resp.Code != http.StatusOK {
		t.Fatalf("write = %d %s", resp.Code, resp.Body.String())
	}
}

func checkCanonical(t *testing.T, sch *arrow.Schema, col int) {
	t.Helper()
	if !arrow.TypeEqual(sch.Field(col).Type, extensions.NewDefaultVariantType()) {
		t.Fatalf("column %q is %s, want the canonical variant", sch.Field(col).Name, sch.Field(col).Type)
	}
}

// A declared variant column (component schema, C3a, or output columns, C3c)
// is in the schema of a table created eagerly at OpenWriter, before any
// chunk; JSON-native values are then written as they are.
func TestVariantDeclaredBeforeEagerCreate(t *testing.T) {
	t.Run("component schema", func(t *testing.T) {
		s, p := newWriteTestServer(t, &Config{DefaultBucket: "raw"})
		fr := &fakeResolver{}
		s.lakekeeperResolver = fr
		open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_JSON,
			Schema: &pb.Schema{Columns: []*pb.ColumnDef{{Name: "id", Type: "int64", Nullable: true}, {Name: "v", Type: "variant", Nullable: true}}}})
		if err != nil {
			t.Fatal(err)
		}
		if len(fr.creates) != 1 || fr.creates[0].Column(1).Type != schema.TypeVariant {
			t.Fatalf("eager creates = %v, want one with v variant", fr.createdColumns())
		}
		mustOK(t, s, open.WriterId, []byte(`[{"id":1,"v":"hello"},{"id":2,"v":{"a":[1,2]}}]`))
		w, batches := writtenBatches(t, p)
		checkCanonical(t, w.spec.Schema, 1)
		if got, want := variantJSON(t, batches[0].Column(1)), []string{`"hello"`, `{"a":[1,2]}`}; !reflect.DeepEqual(got, want) {
			t.Fatalf("values %v, want %v", got, want)
		}
	})
	t.Run("output columns over a string declaration", func(t *testing.T) {
		s, p := newWriteTestServer(t, variantColumnsConfig(ColumnConfig{Name: "v", Type: "variant"}))
		fr := &fakeResolver{}
		s.lakekeeperResolver = fr
		open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_ARROW_IPC,
			Schema: &pb.Schema{Columns: []*pb.ColumnDef{{Name: "id", Type: "int64", Nullable: true}, {Name: "v", Type: "string", Nullable: true}}}})
		if err != nil {
			t.Fatal(err)
		}
		if len(fr.creates) != 1 || fr.creates[0].Column(1).Type != schema.TypeVariant {
			t.Fatalf("eager creates = %v, want one with v variant", fr.createdColumns())
		}
		rec := textBatch(t, `{"a":1}`, `"x"`)
		defer rec.Release()
		mustOK(t, s, open.WriterId, ipcPayload(t, rec))
		_, batches := writtenBatches(t, p)
		if got, want := variantJSON(t, batches[0].Column(1)), []string{`{"a":1}`, `"x"`}; !reflect.DeepEqual(got, want) {
			t.Fatalf("values %v, want %v", got, want)
		}
	})
}

// Output columns declared variant apply before the deferred create on the
// first chunk; a JSON first chunk is parsed with them, so JSON-native
// strings stay strings (buffered JSON and streaming JSONL alike).
func TestVariantDeclaredBeforeDeferredCreate(t *testing.T) {
	for _, tc := range []struct {
		name string
		fmt  pb.DataFormat
		body string
	}{
		{"json", pb.DataFormat_FORMAT_JSON, `[{"id":1,"v":"hello"},{"id":2,"v":{"n":9007199254740993}},{"id":3,"v":"42"}]`},
		{"jsonl", pb.DataFormat_FORMAT_JSONL, "{\"id\":1,\"v\":\"hello\"}\n{\"id\":2,\"v\":{\"n\":9007199254740993}}\n{\"id\":3,\"v\":\"42\"}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p := newWriteTestServer(t, variantColumnsConfig(ColumnConfig{Name: "v", Type: "variant"}))
			fr := &fakeResolver{}
			s.lakekeeperResolver = fr
			open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: tc.fmt})
			if err != nil {
				t.Fatal(err)
			}
			if len(fr.creates) != 0 {
				t.Fatalf("table created at OpenWriter without a schema")
			}
			mustOK(t, s, open.WriterId, []byte(tc.body))
			if len(fr.creates) != 1 || fr.creates[0].ColumnByName("v").Type != schema.TypeVariant {
				t.Fatalf("deferred creates = %v, want one with v variant", fr.createdColumns())
			}
			w, batches := writtenBatches(t, p)
			vi := w.spec.Schema.FieldIndices("v")[0]
			checkCanonical(t, w.spec.Schema, vi)
			if got, want := variantJSON(t, batches[0].Column(vi)), []string{`"hello"`, `{"n":9007199254740993}`, `"42"`}; !reflect.DeepEqual(got, want) {
				t.Fatalf("values %v, want %v", got, want)
			}
		})
	}
}

// Text input (Arrow utf8, CSV) is parsed as JSON text; invalid JSON fails
// the write naming the row and the column.
func TestVariantTextInput(t *testing.T) {
	cfg := variantColumnsConfig(ColumnConfig{Name: "v", Type: "variant"})
	t.Run("arrow utf8", func(t *testing.T) {
		s, p := newWriteTestServer(t, cfg)
		s.lakekeeperResolver = &fakeResolver{}
		open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
		if err != nil {
			t.Fatal(err)
		}
		rec := textBatch(t, `{"a":1}`, `"\"hello\""`, `"hello"`, "")
		defer rec.Release()
		mustOK(t, s, open.WriterId, ipcPayload(t, rec))
		_, batches := writtenBatches(t, p)
		if got, want := variantJSON(t, batches[0].Column(1)), []string{`{"a":1}`, `"\"hello\""`, `"hello"`, "NULL"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("values %v, want %v", got, want)
		}
		bad := textBatch(t, `1`, `hello`)
		defer bad.Release()
		resp := httpWrite(s, open.WriterId, ipcPayload(t, bad))
		if e := decodeHTTPError(t, resp); resp.Code == http.StatusOK || !strings.Contains(e.Error, `column "v": row 1: invalid JSON`) {
			t.Fatalf("invalid text = %d %+v, want a write error naming row 1 of column v", resp.Code, e)
		}
	})
	t.Run("csv", func(t *testing.T) {
		s, p := newWriteTestServer(t, cfg)
		s.lakekeeperResolver = &fakeResolver{}
		open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV})
		if err != nil {
			t.Fatal(err)
		}
		mustOK(t, s, open.WriterId, []byte("id,v\n1,1\n2,\"{\"\"a\"\":1}\"\n3,\"\"\"hello\"\"\"\n"))
		_, batches := writtenBatches(t, p)
		if got, want := variantJSON(t, batches[0].Column(1)), []string{`1`, `{"a":1}`, `"hello"`}; !reflect.DeepEqual(got, want) {
			t.Fatalf("values %v, want %v", got, want)
		}
		resp := httpWrite(s, open.WriterId, []byte("id,v\n1,1\n2,hello\n"))
		if e := decodeHTTPError(t, resp); resp.Code == http.StatusOK || !strings.Contains(e.Error, `row 1, column "v"`) || !strings.Contains(e.Error, "invalid JSON") {
			t.Fatalf("invalid cell = %d %+v, want an invalid JSON error naming row 1 and column v", resp.Code, e)
		}
	})
}

// catalogServer is a write test server resolving through a local Hadoop
// catalog.
func catalogServer(t *testing.T, cfg *Config) (*ServerV2, *fakeDataPlane, *hadoop.Catalog) {
	t.Helper()
	s, p := newWriteTestServer(t, cfg)
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.lakekeeperResolver = &catalogResolver{cat: cat}
	return s, p, cat
}

// createOrders creates raw.orders with fields at formatVersion.
func createOrders(t *testing.T, cat catalog.Catalog, formatVersion string, fields ...iceberg.NestedField) {
	t.Helper()
	ctx := context.Background()
	if err := cat.CreateNamespace(ctx, catalog.ToIdentifier("raw"), nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable(ctx, catalog.ToIdentifier("raw", "orders"), iceberg.NewSchema(0, fields...),
		catalog.WithProperties(iceberg.Properties{icebergtable.PropertyFormatVersion: formatVersion})); err != nil {
		t.Fatalf("create raw.orders v%s: %v", formatVersion, err)
	}
}

var (
	idField      = iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}
	variantField = iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.VariantType{}}
	stringField  = iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String}
)

// An existing table's variant column is the destination whatever the input
// says (C3b); a declaration conflicting with the table's type is an error.
func TestVariantExistingTableColumnWins(t *testing.T) {
	ctx := context.Background()
	t.Run("text input", func(t *testing.T) {
		s, p, cat := catalogServer(t, &Config{DefaultBucket: "raw"})
		createOrders(t, cat, "3", idField, variantField)
		open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
		if err != nil {
			t.Fatal(err)
		}
		rec := textBatch(t, `{"a":1}`, `[true]`)
		defer rec.Release()
		mustOK(t, s, open.WriterId, ipcPayload(t, rec))
		w, batches := writtenBatches(t, p)
		checkCanonical(t, w.spec.Schema, 1)
		if got, want := variantJSON(t, batches[0].Column(1)), []string{`{"a":1}`, `[true]`}; !reflect.DeepEqual(got, want) {
			t.Fatalf("values %v, want %v", got, want)
		}
	})
	t.Run("json input keeps native strings", func(t *testing.T) {
		s, p, cat := catalogServer(t, &Config{DefaultBucket: "raw"})
		createOrders(t, cat, "3", idField, variantField)
		open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_JSONL})
		if err != nil {
			t.Fatal(err)
		}
		mustOK(t, s, open.WriterId, []byte("{\"id\":1,\"v\":\"hello\"}\n"))
		_, batches := writtenBatches(t, p)
		if got := variantJSON(t, batches[0].Column(1)); !reflect.DeepEqual(got, []string{`"hello"`}) {
			t.Fatalf("values %v, want [\"hello\"]", got)
		}
	})
	t.Run("conflicting declarations", func(t *testing.T) {
		for name, cfg := range map[string]*Config{
			"component schema": {DefaultBucket: "raw"},
			"output columns":   variantColumnsConfig(ColumnConfig{Name: "v", Type: "variant"}),
		} {
			s, p, cat := catalogServer(t, cfg)
			createOrders(t, cat, "3", idField, stringField)
			req := &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_JSON}
			if name == "component schema" {
				req.Schema = &pb.Schema{Columns: []*pb.ColumnDef{{Name: "id", Type: "int64", Nullable: true}, {Name: "v", Type: "variant", Nullable: true}}}
			}
			_, err := s.OpenWriter(ctx, req)
			if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), `column "v" is declared variant`) {
				t.Fatalf("%s: OpenWriter err = %v, want InvalidArgument naming v", name, err)
			}
			if len(p.writers) != 0 {
				t.Fatalf("%s: plane writers opened", name)
			}
		}
	})
}

// raceCatalog misses the table on its first load, as if another writer
// created it between that load and this writer's create.
type raceCatalog struct {
	*hadoop.Catalog
	other func()
	once  sync.Once
}

func (c *raceCatalog) LoadTable(ctx context.Context, ident icebergtable.Identifier) (*icebergtable.Table, error) {
	missed := false
	c.once.Do(func() { missed = true })
	if missed {
		c.other()
		return nil, catalog.ErrNoSuchTable
	}
	return c.Catalog.LoadTable(ctx, ident)
}

// A concurrent create that wins the race with a variant column this writer
// did not declare: the table is re-loaded and the destination re-resolved,
// and the chunk fails (InvalidArgument naming the column) instead of
// reinterpreting values parsed without the variant hint (a JSON "42" would
// become the number 42). Nothing is written or committed.
func TestVariantConcurrentCreateRace(t *testing.T) {
	jsonl := "{\"id\":1,\"v\":\"42\"}\n"
	for _, tc := range []struct {
		name string
		fmt  pb.DataFormat
		body func(t *testing.T) []byte
	}{
		{"json", pb.DataFormat_FORMAT_JSON, func(*testing.T) []byte { return []byte(`[{"id":1,"v":"42"}]`) }},
		{"jsonl", pb.DataFormat_FORMAT_JSONL, func(*testing.T) []byte { return []byte(jsonl) }},
		{"arrow utf8", pb.DataFormat_FORMAT_ARROW_IPC, func(t *testing.T) []byte {
			rec := textBatch(t, `42`)
			defer rec.Release()
			return ipcPayload(t, rec)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p := newWriteTestServer(t, &Config{DefaultBucket: "raw"})
			cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			rc := &raceCatalog{Catalog: cat}
			rc.other = func() { createOrders(t, cat, "3", idField, variantField) }
			s.lakekeeperResolver = &raceResolver{cat: rc}
			ctx := context.Background()
			open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: tc.fmt})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ { // the writer stays unopened: every retry fails the same way
				resp := httpWrite(s, open.WriterId, tc.body(t))
				if e := decodeHTTPError(t, resp); resp.Code != http.StatusBadRequest || e.Code != "INVALID_ARGUMENT" ||
					!strings.Contains(e.Error, `column "v"`) || !strings.Contains(e.Error, "did not declare it variant") {
					t.Fatalf("chunk %d = %d %+v, want 400 INVALID_ARGUMENT naming v", i, resp.Code, e)
				}
			}
			_, err = s.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: tc.body(t)})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("gRPC WriteChunk err = %v, want InvalidArgument", err)
			}
			if len(p.writers) != 0 {
				t.Fatalf("plane writers opened: %d", len(p.writers))
			}
			if _, err := s.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: open.WriterId}); err != nil {
				t.Fatalf("CloseWriter: %v", err)
			}
			tb, err := cat.LoadTable(ctx, catalog.ToIdentifier("raw", "orders"))
			if err != nil {
				t.Fatal(err)
			}
			if f, _ := tb.Schema().FindFieldByName("v"); !isIcebergVariant(f.Type) {
				t.Fatalf("table v is %v, want the racing writer's variant", f.Type)
			}
			if snap := tb.CurrentSnapshot(); snap != nil {
				t.Fatalf("table has snapshot %d, want nothing committed", snap.SnapshotID)
			}
		})
	}
}

// A declared variant column survives the same race: the chunk is written.
func TestVariantConcurrentCreateRaceDeclared(t *testing.T) {
	s, p := newWriteTestServer(t, variantColumnsConfig(ColumnConfig{Name: "v", Type: "variant"}))
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	rc := &raceCatalog{Catalog: cat}
	rc.other = func() { createOrders(t, cat, "3", idField, variantField) }
	s.lakekeeperResolver = &raceResolver{cat: rc}
	open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_JSON})
	if err != nil {
		t.Fatal(err)
	}
	mustOK(t, s, open.WriterId, []byte(`[{"id":1,"v":"42"}]`))
	w, batches := writtenBatches(t, p)
	vi := w.spec.Schema.FieldIndices("v")[0]
	checkCanonical(t, w.spec.Schema, vi)
	if got := variantJSON(t, batches[0].Column(vi)); !reflect.DeepEqual(got, []string{`"42"`}) {
		t.Fatalf("values %v, want [\"42\"]", got)
	}
}

// raceResolver is catalogResolver over any catalog.
type raceResolver struct{ cat catalog.Catalog }

func (r *raceResolver) LoadOrCreateForWrite(ctx context.Context, ns, tbl string, sp dglakekeeper.SchemaProvider, fields []dglakekeeper.PartitionField) (*dglakekeeper.WriterTarget, error) {
	t, warning, err := dglakekeeper.LoadOrCreateTable(ctx, r.cat, ns, tbl, sp, fields)
	if err != nil {
		return nil, err
	}
	return &dglakekeeper.WriterTarget{
		BasePath: strings.TrimRight(t.Location(), "/") + "/data/", Spec: t.Spec(), Schema: t.Schema(),
		FormatVersion: t.Metadata().Version(), SpecWarning: warning,
	}, nil
}

func (r *raceResolver) Catalog(context.Context) (catalog.Catalog, error) { return r.cat, nil }
func (r *raceResolver) Close() error                                     { return nil }

// Every chunk after the first is normalized too: text and other variant
// layouts all reach the plane in the canonical layout.
func TestVariantSubsequentChunksNormalized(t *testing.T) {
	s, p := newWriteTestServer(t, variantColumnsConfig(ColumnConfig{Name: "v", Type: "variant"}))
	s.lakekeeperResolver = &fakeResolver{}
	open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if err != nil {
		t.Fatal(err)
	}
	for i, texts := range [][]string{{`{"a":1}`}, {`[2]`, `"three"`}, {`4`}} {
		rec := textBatch(t, texts...)
		mustOK(t, s, open.WriterId, ipcPayload(t, rec))
		rec.Release()
		_, batches := writtenBatches(t, p)
		if len(batches) != i+1 {
			t.Fatalf("chunk %d: %d batches written", i, len(batches))
		}
		checkCanonical(t, batches[i].Schema(), 1)
		if got := variantJSON(t, batches[i].Column(1)); !reflect.DeepEqual(got, texts) {
			t.Fatalf("chunk %d values %v, want %v", i, got, texts)
		}
	}
}

// shreddedVariantBatch is (id int64, v variant) with v in a shredded
// layout (typed_value int64).
func shreddedVariantBatch(t *testing.T, values ...string) arrow.RecordBatch {
	t.Helper()
	vt := extensions.NewShreddedVariantType(arrow.PrimitiveTypes.Int64)
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: vt, Nullable: true},
	}, nil)
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	defer ib.Release()
	vb := extensions.NewVariantBuilder(memory.DefaultAllocator, vt)
	defer vb.Release()
	for i, js := range values {
		ib.Append(int64(i + 1))
		v, err := variant.ParseJSON(js, false)
		if err != nil {
			t.Fatal(err)
		}
		vb.Append(v)
	}
	ids, vs := ib.NewArray(), vb.NewArray()
	defer ids.Release()
	defer vs.Release()
	return array.NewRecordBatch(sch, []arrow.Array{ids, vs}, int64(len(values)))
}

// A variant column bound for an existing format v2 table fails writer
// resolution with InvalidArgument, before any plane writer is opened.
func TestVariantToV2TableFailsBeforeUpload(t *testing.T) {
	ctx := context.Background()
	t.Run("component schema", func(t *testing.T) {
		s, p, cat := catalogServer(t, &Config{DefaultBucket: "raw"})
		createOrders(t, cat, "2", idField)
		_, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_JSON,
			Schema: &pb.Schema{Columns: []*pb.ColumnDef{{Name: "id", Type: "int64", Nullable: true}, {Name: "v", Type: "variant", Nullable: true}}}})
		if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "format v3") {
			t.Fatalf("OpenWriter err = %v, want InvalidArgument (format v3)", err)
		}
		if len(p.writers) != 0 {
			t.Fatal("plane writer opened")
		}
	})
	t.Run("output columns", func(t *testing.T) {
		s, _, cat := catalogServer(t, variantColumnsConfig(ColumnConfig{Name: "v", Type: "variant"}))
		createOrders(t, cat, "2", idField)
		_, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
		if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "format v3") {
			t.Fatalf("OpenWriter err = %v, want InvalidArgument (format v3)", err)
		}
	})
	t.Run("variant in the data", func(t *testing.T) {
		s, p, cat := catalogServer(t, &Config{DefaultBucket: "raw"})
		createOrders(t, cat, "2", idField)
		open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
		if err != nil {
			t.Fatal(err)
		}
		rec := shreddedVariantBatch(t, `1`, `{"a":2}`)
		defer rec.Release()
		resp := httpWrite(s, open.WriterId, ipcPayload(t, rec))
		if e := decodeHTTPError(t, resp); resp.Code != http.StatusBadRequest || e.Code != "INVALID_ARGUMENT" || !strings.Contains(e.Error, "format v3") {
			t.Fatalf("first chunk = %d %+v, want 400 INVALID_ARGUMENT (format v3)", resp.Code, e)
		}
		if len(p.writers) != 0 {
			t.Fatal("plane writer opened")
		}
	})
}

// A shredded Arrow variant written to a new table is created as variant
// and reaches the plane in the canonical layout.
func TestVariantArrowLayoutNormalized(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{DefaultBucket: "raw"})
	fr := &fakeResolver{}
	s.lakekeeperResolver = fr
	open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if err != nil {
		t.Fatal(err)
	}
	rec := shreddedVariantBatch(t, `1`, `{"a":2}`, `"s"`)
	defer rec.Release()
	mustOK(t, s, open.WriterId, ipcPayload(t, rec))
	if len(fr.creates) != 1 || fr.creates[0].Column(1).Type != schema.TypeVariant || !format.IsCanonicalVariant(fr.creates[0].ArrowSchema().Field(1).Type) {
		t.Fatalf("creates = %v, want v canonical variant", fr.createdColumns())
	}
	_, batches := writtenBatches(t, p)
	checkCanonical(t, batches[0].Schema(), 1)
	if got, want := variantJSON(t, batches[0].Column(1)), []string{`1`, `{"a":2}`, `"s"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("values %v, want %v", got, want)
	}
}

// Partitioning by a variant column is rejected with InvalidArgument,
// carrying iceberg-go's own error.
func TestVariantPartitionSourceRejected(t *testing.T) {
	sc := iceberg.NewSchema(0, idField, variantField)
	for _, tr := range []string{"identity", "void", "bucket[4]"} {
		_, err := dglakekeeper.BuildPartitionSpec([]dglakekeeper.PartitionField{{SourceColumn: "v", Transform: tr}}, sc)
		if !errors.Is(err, ErrInvalidPartition) || !errors.Is(err, iceberg.ErrInvalidArgument) || !strings.Contains(err.Error(), "cannot partition by variant source field") {
			t.Fatalf("%s(v): err = %v, want ErrInvalidPartition wrapping iceberg-go's variant rejection", tr, err)
		}
	}

	s, p, _ := partitionedServer(t, PartitionFieldConfig{SourceColumn: "v", Transform: "identity"})
	_, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Bucket: "raw", Table: "orders", InputFormat: pb.DataFormat_FORMAT_JSON,
		Schema: &pb.Schema{Columns: []*pb.ColumnDef{{Name: "id", Type: "int64", Nullable: true}, {Name: "v", Type: "variant", Nullable: true}}}})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "cannot partition by variant source field") {
		t.Fatalf("OpenWriter err = %v, want InvalidArgument with iceberg-go's error", err)
	}
	if len(p.writers) != 0 {
		t.Fatal("plane writer opened")
	}
}
