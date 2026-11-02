package lakekeeper

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icebergtable "github.com/apache/iceberg-go/table"

	dgschema "github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestPartitionValidationErrors(t *testing.T) {
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "id_bucket_4", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ok", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
	gw, err := dgschema.NewSchemaFromArrow(sch)
	if err != nil {
		t.Fatal(err)
	}
	sp := func(context.Context) (*dgschema.Schema, error) { return gw, nil }
	pf := func(pairs ...string) []PartitionField {
		var out []PartitionField
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, PartitionField{SourceColumn: pairs[i], Transform: pairs[i+1]})
		}
		return out
	}

	for _, tc := range []struct {
		name   string
		fields []PartitionField
		want   string
	}{
		{"unknown transform days", pf("ts", "days"), `unknown transform "days"`},
		{"bucket[-1]", pf("id", "bucket[-1]"), `unknown transform "bucket[-1]"`},
		{"bucket[0]", pf("id", "bucket[0]"), "bucket[0]"},
		{"year and day on one column", pf("ts", "year", "ts", "day"), "redundant partition field"},
		{"name collision", pf("id", "bucket[4]"), "partition name id_bucket_4 matches schema column"},
		{"unknown column", pf("nope", "identity"), `source column "nope" is not in the table schema`},
		{"transform invalid for type", pf("ok", "bucket[4]"), `cannot be applied to column "ok" of type boolean`},
		{"year on a string", pf("id_bucket_4", "year"), `cannot be applied to column "id_bucket_4"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = LoadOrCreateTable(context.Background(), cat, "ns", "tbl", sp, tc.fields)
			if !errors.Is(err, ErrInvalidPartition) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ErrInvalidPartition containing %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "table ns.tbl") {
				t.Errorf("err = %v, want the table named", err)
			}
			if _, err := cat.LoadTable(context.Background(), catalog.ToIdentifier("ns", "tbl")); !errors.Is(err, catalog.ErrNoSuchTable) {
				t.Fatalf("table exists after a rejected spec (LoadTable err %v)", err)
			}
		})
	}

	t.Run("partitioned v3 table", func(t *testing.T) {
		cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := cat.CreateNamespace(ctx, catalog.ToIdentifier("ns"), nil); err != nil {
			t.Fatal(err)
		}
		sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
		id := iceberg.PartitionDataIDStart
		spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("id", "id", iceberg.IdentityTransform{}, sc, &id))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cat.CreateTable(ctx, catalog.ToIdentifier("ns", "tbl"), sc, catalog.WithPartitionSpec(&spec),
			catalog.WithProperties(iceberg.Properties{icebergtable.PropertyFormatVersion: "3"})); err != nil {
			t.Fatal(err)
		}
		_, _, err = LoadOrCreateTable(ctx, cat, "ns", "tbl", sp, nil)
		if !errors.Is(err, ErrInvalidPartition) || !strings.Contains(err.Error(), "format-version 3") {
			t.Fatalf("err = %v, want the v3 rejection", err)
		}

		// An unpartitioned v3 table keeps the AddFiles path.
		if _, err := cat.CreateTable(ctx, catalog.ToIdentifier("ns", "flat"), sc,
			catalog.WithProperties(iceberg.Properties{icebergtable.PropertyFormatVersion: "3"})); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadOrCreateTable(ctx, cat, "ns", "flat", sp, nil); err != nil {
			t.Fatalf("unpartitioned v3: %v", err)
		}
	})
}

// createCapture records the options of every CreateTable.
type createCapture struct {
	catalog.Catalog
	cfgs []catalog.CreateTableCfg
}

func (c *createCapture) CreateTable(ctx context.Context, ident icebergtable.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*icebergtable.Table, error) {
	cfg := catalog.NewCreateTableCfg()
	for _, o := range opts {
		o(&cfg)
	}
	c.cfgs = append(c.cfgs, cfg)
	return c.Catalog.CreateTable(ctx, ident, sc, opts...)
}

// A partitioned table is created as format version 2, so a catalog default of
// v3 cannot create a table CheckWriteSpec then rejects; an unpartitioned one
// keeps the catalog default.
func TestPartitionedCreatePinsFormatV2(t *testing.T) {
	sch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	gw, err := dgschema.NewSchemaFromArrow(sch)
	if err != nil {
		t.Fatal(err)
	}
	sp := func(context.Context) (*dgschema.Schema, error) { return gw, nil }
	hc, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &createCapture{Catalog: hc}
	tb, _, err := LoadOrCreateTable(context.Background(), cat, "ns", "part", sp, []PartitionField{{SourceColumn: "id", Transform: "bucket[4]"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := cat.cfgs[0].Properties[icebergtable.PropertyFormatVersion]; got != "2" || tb.Metadata().Version() != 2 {
		t.Fatalf("partitioned create: format-version property %q, table version %d; want 2", got, tb.Metadata().Version())
	}
	if _, _, err := LoadOrCreateTable(context.Background(), cat, "ns", "flat", sp, nil); err != nil {
		t.Fatal(err)
	}
	if _, set := cat.cfgs[1].Properties[icebergtable.PropertyFormatVersion]; set {
		t.Fatalf("unpartitioned create pinned a format version: %v", cat.cfgs[1].Properties)
	}
}

// A spec sourced from a nested field is rejected, never bound to a same-named
// top-level column.
func TestPartitionNestedSourceRejected(t *testing.T) {
	ctx := context.Background()
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, catalog.ToIdentifier("ns"), nil); err != nil {
		t.Fatal(err)
	}
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "meta", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		}}})
	fid := iceberg.PartitionDataIDStart
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("meta.id", "meta_id", iceberg.IdentityTransform{}, sc, &fid))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable(ctx, catalog.ToIdentifier("ns", "tbl"), sc, catalog.WithPartitionSpec(&spec)); err != nil {
		t.Fatal(err)
	}
	_, _, err = LoadOrCreateTable(ctx, cat, "ns", "tbl", nil, nil)
	if !errors.Is(err, ErrInvalidPartition) || !strings.Contains(err.Error(), `"meta.id" is a nested field`) {
		t.Fatalf("existing table: err = %v, want the nested-field rejection", err)
	}
	if _, err := BuildPartitionSpec([]PartitionField{{SourceColumn: "meta.id", Transform: "identity"}}, sc); !errors.Is(err, ErrInvalidPartition) {
		t.Fatalf("BuildPartitionSpec(meta.id) err = %v, want ErrInvalidPartition", err)
	}
}
