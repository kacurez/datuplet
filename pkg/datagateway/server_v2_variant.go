package datagateway

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// ErrInvalidVariant marks a variant column the gateway cannot write: a
// declaration conflicting with the table's column type, or a variant column
// bound for a table below format v3. The server returns it as gRPC
// InvalidArgument (HTTP 400), before anything is uploaded (spec C2, C4).
var ErrInvalidVariant = errors.New("invalid variant column")

func invalidVariant(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidVariant}, args...)...)
}

// configuredVariantColumns returns the output columns of (bucket, table)
// declared with type variant (spec C3c), matching both as
// partitionFieldsFor does.
func (s *ServerV2) configuredVariantColumns(bucket, table string) map[string]bool {
	cols := map[string]bool{}
	for _, t := range s.config.OutputTables {
		if t.Name == table && t.Bucket == bucket {
			for _, c := range t.Columns {
				if strings.EqualFold(c.Type, "variant") {
					cols[c.Name] = true
				}
			}
		}
	}
	return cols
}

// tableColumn returns the top-level column of sc named name.
func tableColumn(sc *iceberg.Schema, name string) (iceberg.NestedField, bool) {
	if sc == nil {
		return iceberg.NestedField{}, false
	}
	for _, f := range sc.Fields() {
		if f.Name == name {
			return f, true
		}
	}
	return iceberg.NestedField{}, false
}

func isIcebergVariant(t iceberg.Type) bool {
	_, ok := t.(iceberg.VariantType)
	return ok
}

// addTableVariantColumns adds sc's top-level variant columns to cols.
func addTableVariantColumns(cols map[string]bool, sc *iceberg.Schema) {
	if sc == nil {
		return
	}
	for _, f := range sc.Fields() {
		if isIcebergVariant(f.Type) {
			cols[f.Name] = true
		}
	}
}

// checkVariantTarget checks variant columns names against the resolved
// table: a column the table already has must be variant there (the table
// wins; a conflicting declaration is an error), and a variant column needs a
// format v3 table (formatVersion 0 = unknown, not checked).
func checkVariantTarget(names []string, table *iceberg.Schema, formatVersion int) error {
	for _, n := range names {
		if f, ok := tableColumn(table, n); ok && !isIcebergVariant(f.Type) {
			return invalidVariant("column %q is declared variant but the existing table has it as %s", n, f.Type)
		}
	}
	if len(names) > 0 && formatVersion > 0 && formatVersion < 3 {
		return invalidVariant("variant column(s) %s need an Iceberg format v3 table; the existing table is format v%d", strings.Join(names, ", "), formatVersion)
	}
	return nil
}

// variantDestination returns the destination schema of the post-drop input
// schema out (spec C4): a column is variant when out has it as variant
// (C3a), it is in declared (C3c) or the table has it as variant (C3b). Every
// other column keeps its input type. Variant columns use the canonical
// layout.
func variantDestination(out *schema.Schema, declared map[string]bool, table *iceberg.Schema, formatVersion int) (*schema.Schema, error) {
	var names []string
	fields := out.ArrowSchema().Fields()
	changed := false
	for i, f := range fields {
		col := out.Column(i)
		tf, inTable := tableColumn(table, col.Name)
		if !(col.Type == schema.TypeVariant || declared[col.Name] || (inTable && isIcebergVariant(tf.Type))) {
			continue
		}
		names = append(names, col.Name)
		if !format.IsCanonicalVariant(f.Type) {
			fields[i].Type = extensions.NewDefaultVariantType()
			changed = true
		}
	}
	if err := checkVariantTarget(names, table, formatVersion); err != nil {
		return nil, err
	}
	if !changed {
		return out, nil
	}
	md := out.ArrowSchema().Metadata()
	return schema.NewSchemaFromArrow(arrow.NewSchema(fields, &md))
}

// isTextFormat reports whether f's adapter builds records from a gateway
// schema (CSV, JSON, JSONL), so a variant column can be parsed as variant.
func isTextFormat(f format.DataFormat) bool {
	return f == format.FormatCSV || f == format.FormatJSON || f == format.FormatJSONL
}

// overlayVariant returns s with the columns in cols typed variant. changed is
// false (and s returned) when s already has them so.
func overlayVariant(s *schema.Schema, cols map[string]bool) (*schema.Schema, bool, error) {
	if len(cols) == 0 {
		return s, false, nil
	}
	defs := s.Columns()
	changed := false
	for i := range defs {
		if cols[defs[i].Name] && defs[i].Type != schema.TypeVariant {
			defs[i].Type = schema.TypeVariant
			changed = true
		}
	}
	if !changed {
		return s, false, nil
	}
	over, err := schema.NewSchema(defs)
	if err != nil {
		return nil, false, err
	}
	return over, true, nil
}

// sortedNames returns the keys of cols in order.
func sortedNames(cols map[string]bool) []string {
	names := make([]string, 0, len(cols))
	for n := range cols {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// normalizedSchema is the schema normalizeRecord turns a record of schema
// in into for dest. A variant destination column whose input cannot be
// converted is an error; any other mismatch is left to acceptRecordTypes.
func normalizedSchema(in, dest *arrow.Schema) (*arrow.Schema, error) {
	if in.NumFields() != dest.NumFields() {
		return in, nil
	}
	fields := in.Fields()
	changed := false
	for i, f := range fields {
		dt := dest.Field(i).Type
		if !format.IsCanonicalVariant(dt) || format.IsCanonicalVariant(f.Type) {
			continue
		}
		if !format.CanConvertToVariant(f.Type) {
			return nil, fmt.Errorf("column %q: cannot write %s into a variant column", f.Name, f.Type)
		}
		fields[i].Type = dt
		changed = true
	}
	if !changed {
		return in, nil
	}
	md := in.Metadata()
	return arrow.NewSchema(fields, &md), nil
}

// normalizeRecord converts rec's variant destination columns to the
// canonical variant layout (spec C4): other variant layouts are rebuilt and
// text is parsed as JSON text. Other columns are left as they are. The caller
// releases the result.
func normalizeRecord(rec arrow.RecordBatch, dest *arrow.Schema, mem memory.Allocator) (arrow.RecordBatch, error) {
	sch, err := normalizedSchema(rec.Schema(), dest)
	if err != nil {
		return nil, err
	}
	if sch == rec.Schema() {
		rec.Retain()
		return rec, nil
	}
	cols := make([]arrow.Array, rec.NumCols())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	for i := range cols {
		if arrow.TypeEqual(sch.Field(i).Type, rec.Column(i).DataType()) {
			cols[i] = rec.Column(i)
			cols[i].Retain()
			continue
		}
		if cols[i], err = format.ToCanonicalVariant(rec.Column(i), mem); err != nil {
			return nil, fmt.Errorf("column %q: %w", sch.Field(i).Name, err)
		}
	}
	return array.NewRecordBatch(sch, cols, rec.NumRows()), nil
}
