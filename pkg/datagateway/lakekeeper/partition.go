package lakekeeper

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway/manifest"
)

// ErrInvalidPartition marks a partition_fields or table partition spec the
// gateway cannot write (spec decision 3); the server returns it as gRPC
// InvalidArgument (HTTP 400).
var ErrInvalidPartition = errors.New("invalid partitioning")

// PartitionField is one configured partition field
// (OutputTableConfig.PartitionFields).
type PartitionField struct {
	SourceColumn string
	Transform    string
}

func (f PartitionField) String() string { return f.Transform + "(" + f.SourceColumn + ")" }

func invalidPartition(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidPartition}, args...)...)
}

// LoadOrCreateTable loads (ns, tbl) from cat or, when it is missing, creates
// it from the schema provider's (post-drop) schema partitioned by fields. It
// returns the table as the catalog has it (the catalog may reassign field
// ids) and checks that table's current spec for writing. warning is non-empty
// when fields differ from an existing table's spec (the table's spec wins).
// Spec decisions 3–5.
func LoadOrCreateTable(ctx context.Context, cat catalog.Catalog, ns, tbl string, sp SchemaProvider, fields []PartitionField) (t *icebergtable.Table, warning string, err error) {
	ident := catalog.ToIdentifier(ns, tbl)
	t, err = cat.LoadTable(ctx, ident)
	created := false
	if err != nil {
		if !errors.Is(err, catalog.ErrNoSuchTable) {
			return nil, "", fmt.Errorf("lakekeeper: load %s.%s: %w", ns, tbl, err)
		}
		if sp == nil {
			return nil, "", fmt.Errorf("lakekeeper: table %s.%s missing and no schema available to create it", ns, tbl)
		}
		gwSchema, schErr := sp(ctx)
		if schErr != nil {
			return nil, "", fmt.Errorf("lakekeeper: schema provider for %s.%s: %w", ns, tbl, schErr)
		}
		if gwSchema == nil {
			return nil, "", fmt.Errorf("lakekeeper: schema provider returned nil for %s.%s", ns, tbl)
		}
		icSchema := manifest.SchemaToIceberg(gwSchema)
		if icSchema == nil {
			return nil, "", fmt.Errorf("lakekeeper: cannot convert gateway schema for %s.%s", ns, tbl)
		}
		spec, specErr := BuildPartitionSpec(fields, icSchema)
		if specErr != nil {
			return nil, "", fmt.Errorf("table %s.%s: %w", ns, tbl, specErr)
		}
		if err := ensureNamespace(ctx, cat, ns); err != nil {
			return nil, "", err
		}
		t, err = cat.CreateTable(ctx, ident, icSchema, catalog.WithPartitionSpec(&spec))
		switch {
		case err == nil:
			created = true
		case errors.Is(err, catalog.ErrTableAlreadyExists):
			// Concurrent committer race: the existing table's spec wins.
			if t, err = cat.LoadTable(ctx, ident); err != nil {
				return nil, "", fmt.Errorf("lakekeeper: load %s.%s after race: %w", ns, tbl, err)
			}
		default:
			return nil, "", fmt.Errorf("lakekeeper: create %s.%s: %w", ns, tbl, err)
		}
	}
	if err := CheckWriteSpec(t); err != nil {
		return nil, "", fmt.Errorf("table %s.%s: %w", ns, tbl, err)
	}
	if !created {
		warning = specDiffers(t, fields)
	}
	return t, warning, nil
}

// BuildPartitionSpec builds the spec a new table is created with from the
// configured fields, bound to sc (the post-drop schema). Field names follow
// iceberg.GeneratePartitionFieldName; partition field ids start at 1000 as
// Iceberg assigns them. Every error wraps ErrInvalidPartition.
func BuildPartitionSpec(fields []PartitionField, sc *iceberg.Schema) (iceberg.PartitionSpec, error) {
	opts := make([]iceberg.PartitionOption, 0, len(fields))
	for i, f := range fields {
		src, ok := sc.FindFieldByName(f.SourceColumn)
		if !ok {
			return iceberg.PartitionSpec{}, invalidPartition("partition field %s: source column %q is not in the table schema", f, f.SourceColumn)
		}
		tr, err := parseTransform(f.Transform)
		if err != nil {
			return iceberg.PartitionSpec{}, invalidPartition("partition field %s: %v", f, err)
		}
		if !tr.CanTransform(src.Type) {
			return iceberg.PartitionSpec{}, invalidPartition("partition field %s: transform %s cannot be applied to column %q of type %s", f, tr, src.Name, src.Type)
		}
		name, err := iceberg.GeneratePartitionFieldName(sc, iceberg.PartitionField{SourceIDs: []int{src.ID}, Transform: tr})
		if err != nil {
			return iceberg.PartitionSpec{}, invalidPartition("partition field %s: %v", f, err)
		}
		id := iceberg.PartitionDataIDStart + i
		opts = append(opts, iceberg.AddPartitionFieldByName(src.Name, name, tr, sc, &id))
	}
	spec, err := iceberg.NewPartitionSpecOpts(opts...)
	if err != nil {
		return iceberg.PartitionSpec{}, invalidPartition("partition_fields %v: %v", fields, err)
	}
	return spec, nil
}

// parseTransform parses a configured transform string; an unknown transform
// (which iceberg.ParseTransform returns without an error, e.g. "days" or
// "bucket[-1]") is an error.
func parseTransform(s string) (iceberg.Transform, error) {
	tr, err := iceberg.ParseTransform(s)
	if err != nil {
		return nil, err
	}
	if _, ok := tr.(iceberg.UnknownTransform); ok {
		return nil, fmt.Errorf("unknown transform %q (want identity, year, month, day, hour, bucket[N], truncate[W] or void)", s)
	}
	return tr, nil
}

// CheckWriteSpec checks that the gateway can write t partitioned by its
// current spec: no unknown transform, every source column in the table schema,
// and a format version below 3 for a partitioned table (iceberg-go's
// AddDataFiles requires first_row_id on v3). Errors wrap ErrInvalidPartition.
func CheckWriteSpec(t *icebergtable.Table) error {
	spec := t.Spec()
	if spec.NumFields() == 0 {
		return nil
	}
	if v := t.Metadata().Version(); v >= 3 {
		return invalidPartition("partitioned writes to format-version %d tables are not supported", v)
	}
	sc := t.Schema()
	for _, f := range spec.Fields() {
		if _, ok := f.Transform.(iceberg.UnknownTransform); ok {
			return invalidPartition("partition field %s uses unknown transform %s", f.Name, f.Transform)
		}
		if _, isVoid := f.Transform.(iceberg.VoidTransform); isVoid {
			continue
		}
		if _, ok := sc.FindFieldByID(f.SourceID()); !ok {
			return invalidPartition("partition field %s: source field id %d is not in the table schema", f.Name, f.SourceID())
		}
	}
	return nil
}

// specDiffers returns the warning for an existing table whose current spec
// is not the configured fields as (source column, transform) pairs in order,
// or "" when they match.
func specDiffers(t *icebergtable.Table, fields []PartitionField) string {
	spec := t.Spec()
	sc := t.Schema()
	have := make([]string, 0, spec.NumFields())
	same := spec.NumFields() == len(fields)
	i := 0
	for _, f := range spec.Fields() {
		col, _ := sc.FindColumnName(f.SourceID())
		have = append(have, f.Transform.String()+"("+col+")")
		if same {
			tr, err := parseTransform(fields[i].Transform)
			same = err == nil && col == fields[i].SourceColumn && tr.Equals(f.Transform)
		}
		i++
	}
	if same {
		return ""
	}
	want := make([]string, len(fields))
	for i, f := range fields {
		want[i] = f.String()
	}
	return fmt.Sprintf("partition_fields [%s] differ from the existing table's partition spec [%s]; the table's spec is used (drop the table to change it)",
		strings.Join(want, ", "), strings.Join(have, ", "))
}
