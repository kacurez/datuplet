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

func idSchemaProvider(t *testing.T) SchemaProvider {
	t.Helper()
	gw, err := dgschema.NewSchemaFromArrow(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	return func(context.Context) (*dgschema.Schema, error) { return gw, nil }
}

func idPartitionSpec(t *testing.T, sc *iceberg.Schema) iceberg.PartitionSpec {
	t.Helper()
	id := iceberg.PartitionDataIDStart
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("id", "id", iceberg.IdentityTransform{}, sc, &id))
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

// createdTables creates ns.part (bucket[4](id)) and ns.flat (unpartitioned)
// through LoadOrCreateTable and returns them with the captured create options.
func createdTables(t *testing.T) (part, flat *icebergtable.Table, cfgs []catalog.CreateTableCfg) {
	t.Helper()
	sp := idSchemaProvider(t)
	hc, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &createCapture{Catalog: hc}
	part, _, err = LoadOrCreateTable(context.Background(), cat, "ns", "part", sp, []PartitionField{{SourceColumn: "id", Transform: "bucket[4]"}})
	if err != nil {
		t.Fatal(err)
	}
	flat, _, err = LoadOrCreateTable(context.Background(), cat, "ns", "flat", sp, nil)
	if err != nil {
		t.Fatal(err)
	}
	return part, flat, cat.cfgs
}

// Every table the gateway creates is format version 3, partitioned or not.
func TestCreateTableIsV3(t *testing.T) {
	part, flat, cfgs := createdTables(t)
	for i, tb := range []*icebergtable.Table{part, flat} {
		if got := cfgs[i].Properties[icebergtable.PropertyFormatVersion]; got != "3" || tb.Metadata().Version() != 3 {
			t.Errorf("%s: format-version property %q, table version %d; want 3", tb.Identifier(), got, tb.Metadata().Version())
		}
	}
	if ps, fs := part.Spec(), flat.Spec(); ps.NumFields() != 1 || fs.NumFields() != 0 {
		t.Fatalf("specs: part %v, flat %v", ps, fs)
	}
}

// Every created table disables iceberg-go's internal commit retry, which would
// re-base the snapshot's first-row-id but not explicit per-file row ids.
func TestCreateTableDisablesInternalRetry(t *testing.T) {
	part, flat, cfgs := createdTables(t)
	for i, tb := range []*icebergtable.Table{part, flat} {
		if got := cfgs[i].Properties[icebergtable.CommitNumRetriesKey]; got != "0" {
			t.Errorf("%s: create property %s = %q, want \"0\"", tb.Identifier(), icebergtable.CommitNumRetriesKey, got)
		}
		if got := tb.Properties()[icebergtable.CommitNumRetriesKey]; got != "0" {
			t.Errorf("%s: table property %s = %q, want \"0\"", tb.Identifier(), icebergtable.CommitNumRetriesKey, got)
		}
	}
}

// An existing partitioned v3 table whose commit.retry.num-retries is above 0
// is rejected at resolve, naming the property; 0 or unset is writable, and an
// unpartitioned table (manifest-inherited row ids) is never rejected.
func TestPartitionedV3RejectsInternalRetriesAtResolve(t *testing.T) {
	ctx := context.Background()
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, catalog.ToIdentifier("ns"), nil); err != nil {
		t.Fatal(err)
	}
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	spec := idPartitionSpec(t, sc)
	for _, tc := range []struct {
		name        string
		partitioned bool
		version     string
		retries     string // "" = unset
		wantErr     bool
	}{
		{"part_v3_retries_3", true, "3", "3", true},
		{"part_v3_retries_1", true, "3", "1", true},
		{"part_v3_retries_0", true, "3", "0", false},
		{"part_v3_unset", true, "3", "", false},
		{"part_v2_retries_3", true, "2", "3", false},
		{"flat_v3_retries_3", false, "3", "3", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			props := iceberg.Properties{icebergtable.PropertyFormatVersion: tc.version}
			if tc.retries != "" {
				props[icebergtable.CommitNumRetriesKey] = tc.retries
			}
			opts := []catalog.CreateTableOpt{catalog.WithProperties(props)}
			if tc.partitioned {
				opts = append(opts, catalog.WithPartitionSpec(&spec))
			}
			if _, err := cat.CreateTable(ctx, catalog.ToIdentifier("ns", tc.name), sc, opts...); err != nil {
				t.Fatal(err)
			}
			_, _, err := LoadOrCreateTable(ctx, cat, "ns", tc.name, nil, nil)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("err = %v, want writable", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidPartition) || !strings.Contains(err.Error(), "commit.retry.num-retries") ||
				!strings.Contains(err.Error(), "table ns."+tc.name) {
				t.Fatalf("err = %v, want ErrInvalidPartition naming commit.retry.num-retries and the table", err)
			}
		})
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
