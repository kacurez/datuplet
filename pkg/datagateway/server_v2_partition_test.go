package datagateway

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dglakekeeper "github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// catalogResolver resolves write targets through the gateway's
// load-or-create against a local Hadoop catalog.
type catalogResolver struct{ cat *hadoop.Catalog }

func (r *catalogResolver) LoadOrCreateForWrite(ctx context.Context, ns, tbl string, sp dglakekeeper.SchemaProvider, fields []dglakekeeper.PartitionField) (*dglakekeeper.WriterTarget, error) {
	t, warning, err := dglakekeeper.LoadOrCreateTable(ctx, r.cat, ns, tbl, sp, fields)
	if err != nil {
		return nil, err
	}
	return &dglakekeeper.WriterTarget{
		BasePath: strings.TrimRight(t.Location(), "/") + "/data/", Spec: t.Spec(), Schema: t.Schema(),
		FormatVersion: t.Metadata().Version(), SpecWarning: warning,
	}, nil
}

func (r *catalogResolver) Catalog(context.Context) (catalog.Catalog, error) { return r.cat, nil }
func (r *catalogResolver) Close() error                                     { return nil }

func partitionedServer(t *testing.T, fields ...PartitionFieldConfig) (*ServerV2, *fakeDataPlane, *hadoop.Catalog) {
	t.Helper()
	s, p := newWriteTestServer(t, &Config{
		DefaultBucket: "raw",
		OutputTables:  []OutputTableConfig{{Name: "orders", Bucket: "raw", PartitionFields: fields}},
	})
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.lakekeeperResolver = &catalogResolver{cat: cat}
	return s, p, cat
}

// With an inferred schema the partition spec is resolved on the first
// chunk: an invalid one fails that chunk with HTTP 400 (gRPC
// InvalidArgument), and no table is created.
func TestInferredSchemaPartitionErrorOnFirstChunk(t *testing.T) {
	s, p, cat := partitionedServer(t, PartitionFieldConfig{SourceColumn: "missing", Transform: "identity"})
	ctx := context.Background()
	open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Bucket: "raw", Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	resp := httpWrite(s, open.WriterId, []byte("id,name\n1,a\n"))
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("first chunk = %d %s, want 400", resp.Code, resp.Body.String())
	}
	if e := decodeHTTPError(t, resp); e.Code != "INVALID_ARGUMENT" || !strings.Contains(e.Error, `source column "missing"`) {
		t.Fatalf("error = %+v, want INVALID_ARGUMENT naming the column", e)
	}
	_, err = s.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: []byte("id,name\n1,a\n")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("gRPC WriteChunk err = %v (code %s), want InvalidArgument", err, status.Code(err))
	}
	if _, err := cat.LoadTable(ctx, catalog.ToIdentifier("raw", "orders")); !errors.Is(err, catalog.ErrNoSuchTable) {
		t.Fatalf("table created despite the invalid spec (LoadTable err %v)", err)
	}
	if len(p.writers) != 0 {
		t.Fatalf("plane writers opened: %d", len(p.writers))
	}

	// A component-supplied schema fails OpenWriter itself.
	_, err = s.OpenWriter(ctx, &pb.OpenWriterRequest{Bucket: "raw", Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV,
		Schema: &pb.Schema{Columns: []*pb.ColumnDef{{Name: "id", Type: "int64", Nullable: true}}}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("OpenWriter with schema err = %v, want InvalidArgument", err)
	}
}

// A valid spec reaches the plane writer: the writer spec carries the
// catalog's spec and schema, and no "ignored" warning remains.
func TestPartitionSpecReachesWriter(t *testing.T) {
	s, p, _ := partitionedServer(t, PartitionFieldConfig{SourceColumn: "name", Transform: "identity"})
	ctx := context.Background()
	open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Bucket: "raw", Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if _, err := s.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: []byte("id,name\n1,a\n")}); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	spec := p.writers[0].spec
	if spec.PartitionSpec == nil || spec.PartitionSpec.NumFields() != 1 || spec.TableSchema == nil || spec.Table != "raw.orders" {
		t.Fatalf("writer spec partition %v schema %v table %q", spec.PartitionSpec, spec.TableSchema, spec.Table)
	}
	if f := spec.PartitionSpec.Field(0); f.Name != "name" {
		t.Fatalf("partition field = %v", f)
	}
}
