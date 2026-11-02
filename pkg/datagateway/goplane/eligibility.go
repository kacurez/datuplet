package goplane

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// rawEligible reports whether task may be served as the data file's bytes
// (spec, Read path 2): a whole-file Parquet task without delete files whose
// footer schema equals the table's current schema in names, Arrow types and
// field identity. The footer is read through IO built from current
// credentials.
func (r *reader) rawEligible(ctx context.Context, task table.FileScanTask) (bool, error) {
	if !wholeFileTask(task) || hasDeletes(task) || task.File.FileFormat() != iceberg.ParquetFile {
		return false, nil
	}
	fileSchema, err := r.footerSchema(ctx, task.File.FilePath())
	if err != nil {
		return false, err
	}
	return fileMatchesTable(fileSchema, r.schema, r.tableSchema, r.nameMapping), nil
}

func wholeFileTask(t table.FileScanTask) bool {
	return t.Start == 0 && t.Length == t.File.FileSizeBytes()
}

func hasDeletes(t table.FileScanTask) bool {
	return len(t.DeleteFiles) > 0 || len(t.EqualityDeleteFiles) > 0 || len(t.DeletionVectorFiles) > 0
}

// footerSchema reads a Parquet footer and converts it with pqarrow (field
// ids, when the file carries them, become PARQUET:field_id metadata).
func (r *reader) footerSchema(ctx context.Context, path string) (*arrow.Schema, error) {
	fsys, err := r.openIO(ctx, path)
	if err != nil {
		return nil, err
	}
	f, err := fsys.Open(path)
	if err != nil {
		return nil, fmt.Errorf("goplane: open %s: %w", path, err)
	}
	defer f.Close()
	pr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("goplane: footer of %s: %w", path, err)
	}
	defer pr.Close()
	md := pr.MetaData()
	sch, err := pqarrow.FromParquet(md.Schema, &pqarrow.ArrowReadProperties{}, md.KeyValueMetadata())
	if err != nil {
		return nil, fmt.Errorf("goplane: arrow schema of %s: %w", path, err)
	}
	return sch, nil
}

// fileMatchesTable is the schema part of raw eligibility: the file's Arrow
// fields equal the table's in order (names and Arrow types; nullability and
// metadata ignored), and every file column resolves to the same Iceberg
// field id as the table column — through the file's field ids when it has
// them, else through the table's name mapping, which must be complete and
// unambiguous.
func fileMatchesTable(fileSchema, tableArrow *arrow.Schema, tableSchema *iceberg.Schema, nm iceberg.NameMapping) bool {
	if !sameNamesAndTypes(fileSchema, tableArrow) {
		return false
	}
	opts := table.ArrowToIcebergOptions{}
	if !allFieldIDs(fileSchema.Fields()) {
		if nm == nil || !mappingUsable(nm) {
			return false
		}
		opts.NameMapping = nm
	}
	resolved, ok := resolveIDs(fileSchema, opts)
	return ok && sameFieldIDs(resolved.Fields(), tableSchema.Fields())
}

func sameNamesAndTypes(a, b *arrow.Schema) bool {
	if a.NumFields() != b.NumFields() {
		return false
	}
	for i := range a.NumFields() {
		fa, fb := a.Field(i), b.Field(i)
		if fa.Name != fb.Name || !arrow.TypeEqual(fa.Type, fb.Type) {
			return false
		}
	}
	return true
}

// allFieldIDs reports whether every field, nested ones included, carries a
// positive Parquet field id — the condition under which iceberg-go's reader
// uses the file's ids instead of the name mapping (pqarrow reports a missing
// id as -1).
func allFieldIDs(fields []arrow.Field) bool {
	for _, f := range fields {
		if !hasFieldID(f) {
			return false
		}
		switch t := f.Type.(type) {
		case *arrow.StructType:
			if !allFieldIDs(t.Fields()) {
				return false
			}
		case *arrow.MapType:
			if !allFieldIDs([]arrow.Field{t.KeyField(), t.ItemField()}) {
				return false
			}
		case arrow.ListLikeType:
			if !allFieldIDs([]arrow.Field{t.ElemField()}) {
				return false
			}
		}
	}
	return true
}

func hasFieldID(f arrow.Field) bool {
	v, ok := f.Metadata.GetValue("PARQUET:field_id")
	if !ok {
		return false
	}
	id, err := strconv.Atoi(v)
	return err == nil && id > 0
}

// resolveIDs converts the file schema to Iceberg the way iceberg-go's reader
// does; any failure (a column missing from the mapping, a partial set of
// field ids) means "not raw".
func resolveIDs(s *arrow.Schema, opts table.ArrowToIcebergOptions) (out *iceberg.Schema, ok bool) {
	defer func() {
		if recover() != nil {
			out, ok = nil, false
		}
	}()
	sc, err := table.ArrowSchemaToIcebergWithOptions(s, opts)
	return sc, err == nil && sc != nil
}

// mappingUsable rejects a name mapping with an entry without a field id or
// a name mapped twice on one level (ambiguous).
func mappingUsable(fields []iceberg.MappedField) bool {
	seen := map[string]bool{}
	for _, f := range fields {
		if f.FieldID == nil {
			return false
		}
		for _, n := range f.Names {
			if seen[n] {
				return false
			}
			seen[n] = true
		}
		if !mappingUsable(f.Fields) {
			return false
		}
	}
	return true
}

func sameFieldIDs(a, b []iceberg.NestedField) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || !sameNestedIDs(a[i].Type, b[i].Type) {
			return false
		}
	}
	return true
}

func sameNestedIDs(a, b iceberg.Type) bool {
	switch at := a.(type) {
	case *iceberg.StructType:
		bt, ok := b.(*iceberg.StructType)
		return ok && sameFieldIDs(at.FieldList, bt.FieldList)
	case *iceberg.ListType:
		bt, ok := b.(*iceberg.ListType)
		return ok && at.ElementID == bt.ElementID && sameNestedIDs(at.Element, bt.Element)
	case *iceberg.MapType:
		bt, ok := b.(*iceberg.MapType)
		return ok && at.KeyID == bt.KeyID && at.ValueID == bt.ValueID &&
			sameNestedIDs(at.KeyType, bt.KeyType) && sameNestedIDs(at.ValueType, bt.ValueType)
	}
	switch b.(type) {
	case *iceberg.StructType, *iceberg.ListType, *iceberg.MapType:
		return false
	}
	return true
}
