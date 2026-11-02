package goplane

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// int64Pair is a two-long-column schema (a=1, b=2) for field-identity cases.
var int64PairSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.Int64},
)

// pairRecord is a=[1,2,3], b=[10,20,30], with the given field ids (nil: none).
func pairRecord(ids []string) arrow.RecordBatch {
	fields := []arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}
	for i := range fields {
		if ids != nil {
			fields[i].Metadata = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{ids[i]})
		}
	}
	mem := memory.NewGoAllocator()
	a, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[1,2,3]`))
	b, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[10,20,30]`))
	defer a.Release()
	defer b.Release()
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), []arrow.Array{a, b}, 3)
}

func setNameMapping(t *testing.T, f *tfx, nm iceberg.NameMapping) {
	t.Helper()
	b, err := json.Marshal(nm)
	if err != nil {
		t.Fatal(err)
	}
	js := string(b)
	f.commit(func(txn *table.Transaction) error {
		return txn.SetProperties(iceberg.Properties{table.DefaultNameMappingKey: js})
	})
}

func intp(i int) *int { return &i }

func TestEligibilityRules(t *testing.T) {
	ctx := context.Background()

	t.Run("clean whole-file task is raw", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("2"))
		path := f.writeFile(0, 50, 0)
		f.addFiles(path)
		parts := readParts(t, f.open(true))
		if kinds(parts) != "raw" || parts[0].path != path || !equalIDs(allIDs(parts), idRange(0, 50)) {
			t.Fatalf("parts %s %+v", kinds(parts), parts)
		}
	})

	t.Run("position deletes are decoded", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("2"))
		path := f.writeFile(0, 50, 0)
		f.addFiles(path)
		f.deleteWhere(iceberg.EqualTo(iceberg.Reference("id"), int64(7)))
		if task := f.taskFor(path); len(task.DeleteFiles) == 0 {
			t.Fatalf("planned task has no position delete files: %+v", task)
		}
		parts := readParts(t, f.open(true))
		if kinds(parts) != "decoded" || !equalIDs(allIDs(parts), idRange(0, 50, 7)) {
			t.Fatalf("parts %s ids %v", kinds(parts), allIDs(parts))
		}
	})

	t.Run("equality deletes are decoded", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("2"))
		path := f.writeFile(0, 50, 0)
		f.addFiles(path)
		delSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"})}}, nil)
		arr, _, _ := array.FromJSON(memory.NewGoAllocator(), arrow.PrimitiveTypes.Int64, strings.NewReader(`[3, 4]`))
		rec := array.NewRecordBatch(delSchema, []arrow.Array{arr}, 2)
		arr.Release()
		f.commit(func(txn *table.Transaction) error {
			return txn.CommitEqualityDeletes(ctx, []int{1}, oneBatch(rec), nil)
		})
		if task := f.taskFor(path); len(task.EqualityDeleteFiles) == 0 {
			t.Fatalf("planned task has no equality delete files: %+v", task)
		}
		parts := readParts(t, f.open(true))
		if kinds(parts) != "decoded" || !equalIDs(allIDs(parts), idRange(0, 50, 3, 4)) {
			t.Fatalf("parts %s ids %v", kinds(parts), allIDs(parts))
		}
	})

	t.Run("deletion vector is decoded", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("3"))
		path := f.writeFile(0, 50, 0)
		f.addFiles(path)
		f.deleteWhere(iceberg.EqualTo(iceberg.Reference("id"), int64(11)))
		if task := f.taskFor(path); len(task.DeletionVectorFiles) == 0 {
			t.Fatalf("planned task has no deletion vector: %+v", task)
		}
		parts := readParts(t, f.open(true))
		if kinds(parts) != "decoded" || !equalIDs(allIDs(parts), idRange(0, 50, 11)) {
			t.Fatalf("parts %s ids %v", kinds(parts), allIDs(parts))
		}
	})

	t.Run("partial task that cannot be coalesced is decoded", func(t *testing.T) {
		f := splitTable(t)
		tasks := f.plan()
		r := f.open(true)
		lone := coalesceSplits(tasks[1:2])
		if len(lone) != 1 || wholeFileTask(lone[0]) {
			t.Fatalf("a lone split was coalesced: %+v", lone)
		}
		if raw, err := r.rawEligible(ctx, lone[0]); err != nil || raw {
			t.Fatalf("partial task rawEligible = %v, %v", raw, err)
		}
	})

	t.Run("column added after the file is decoded", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("2"))
		path := f.writeFile(0, 5, 0)
		f.addFiles(path)
		f.commit(func(txn *table.Transaction) error {
			return txn.UpdateSchema(true, false, table.WithNameMapping(f.load().NameMapping())).
				AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.String, "", false, nil).Commit()
		})
		r := f.open(true)
		parts := readParts(t, r)
		if kinds(parts) != "decoded" || r.Schema().NumFields() != 3 {
			t.Fatalf("parts %s schema %s", kinds(parts), r.Schema())
		}
		for _, v := range parts[0].cols[2] {
			if v != nil {
				t.Fatalf("added column not null: %v", parts[0].cols[2])
			}
		}
	})

	t.Run("renamed column is decoded", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("2"))
		path := f.writeFile(0, 5, 0)
		f.addFiles(path)
		f.commit(func(txn *table.Transaction) error {
			return txn.UpdateSchema(true, false, table.WithNameMapping(f.load().NameMapping())).
				RenameColumn([]string{"name"}, "label").Commit()
		})
		r := f.open(true)
		parts := readParts(t, r)
		if kinds(parts) != "decoded" || r.Schema().Field(1).Name != "label" || parts[0].cols[1][0] == nil {
			t.Fatalf("parts %s schema %s cols %v", kinds(parts), r.Schema(), parts[0].cols)
		}
	})

	t.Run("int8 file under an int column is decoded", func(t *testing.T) {
		sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "v", Type: iceberg.PrimitiveTypes.Int32})
		f := newTable(t, sc, morProps("2"))
		arr, _, _ := array.FromJSON(memory.NewGoAllocator(), arrow.PrimitiveTypes.Int8, strings.NewReader(`[1, -2, 127]`))
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int8, Nullable: true}}, nil),
			[]arrow.Array{arr}, 3)
		arr.Release()
		path := f.writeArrow(rec)
		rec.Release()
		f.addFiles(path)
		r := f.open(true)
		parts := readParts(t, r)
		if kinds(parts) != "decoded" || !arrow.TypeEqual(r.Schema().Field(0).Type, arrow.PrimitiveTypes.Int32) ||
			fmt.Sprint(parts[0].cols[0]) != "[1 -2 127]" {
			t.Fatalf("parts %s cols %v", kinds(parts), parts[0].cols)
		}
	})

	t.Run("matching field ids are raw", func(t *testing.T) {
		f := newTable(t, int64PairSchema, morProps("2"))
		rec := pairRecord([]string{"1", "2"})
		path := f.writeArrow(rec)
		rec.Release()
		f.addFiles(path)
		parts := readParts(t, f.open(true))
		if kinds(parts) != "raw" {
			t.Fatalf("parts %s", kinds(parts))
		}
	})

	t.Run("mismatching field id is decoded", func(t *testing.T) {
		f := newTable(t, int64PairSchema, morProps("2"))
		rec := pairRecord([]string{"2", "1"}) // file a carries field 2, b carries field 1
		path := f.writeArrow(rec)
		rec.Release()
		f.addFiles(path)
		parts := readParts(t, f.open(true))
		if kinds(parts) != "decoded" || fmt.Sprint(parts[0].cols[0]) != "[10 20 30]" {
			t.Fatalf("parts %s cols %v (field-id projection should swap the columns)", kinds(parts), parts[0].cols)
		}
	})

	t.Run("name mapping to a different field id is decoded", func(t *testing.T) {
		f := newTable(t, int64PairSchema, morProps("2"))
		rec := pairRecord(nil)
		path := f.writeArrow(rec)
		rec.Release()
		setNameMapping(t, f, iceberg.NameMapping{
			{Names: []string{"a"}, FieldID: intp(2)},
			{Names: []string{"b"}, FieldID: intp(1)},
		})
		f.addFiles(path)
		parts := readParts(t, f.open(true))
		if kinds(parts) != "decoded" || fmt.Sprint(parts[0].cols[0]) != "[10 20 30]" {
			t.Fatalf("parts %s cols %v (name mapping should swap the columns)", kinds(parts), parts[0].cols)
		}
	})

	t.Run("no name mapping and no file field ids is not raw", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("2"))
		path := f.writeFile(0, 5, 0)
		f.addFiles(path) // AddFiles installs a name mapping; remove it
		f.commit(func(txn *table.Transaction) error {
			return txn.RemoveProperties([]string{table.DefaultNameMappingKey})
		})
		if f.load().NameMapping() != nil {
			t.Fatal("name mapping still set")
		}
		r := f.open(true)
		// The decoded read itself fails in iceberg-go (no ids, no mapping), so
		// eligibility is asserted directly.
		if raw, err := r.rawEligible(ctx, f.taskFor(path)); err != nil || raw {
			t.Fatalf("rawEligible = %v, %v", raw, err)
		}
	})

	t.Run("ambiguous or incomplete name mapping is not raw", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("2"))
		path := f.writeFile(0, 5, 0)
		f.addFiles(path)
		r := f.open(true)
		fs, err := r.footerSchema(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		for name, nm := range map[string]iceberg.NameMapping{
			"ambiguous": {{Names: []string{"id"}, FieldID: intp(1)}, {Names: []string{"name", "id"}, FieldID: intp(2)}},
			"no id":     {{Names: []string{"id"}, FieldID: intp(1)}, {Names: []string{"name"}}},
			"missing":   {{Names: []string{"id"}, FieldID: intp(1)}},
		} {
			if fileMatchesTable(fs, r.schema, r.tableSchema, nm) {
				t.Errorf("%s mapping accepted", name)
			}
		}
		if !fileMatchesTable(fs, r.schema, r.tableSchema, r.nameMapping) {
			t.Error("the table's own mapping rejected")
		}
	})

	t.Run("RawParquet false is one Records part", func(t *testing.T) {
		f := newTable(t, readSchema, morProps("2"))
		f.addFiles(f.writeFile(0, 10, 0), f.writeFile(10, 10, 0))
		parts := readParts(t, f.open(false))
		if kinds(parts) != "decoded" || !equalIDs(allIDs(parts), idRange(0, 20)) {
			t.Fatalf("parts %s ids %v", kinds(parts), allIDs(parts))
		}
	})
}

func oneBatch(rec arrow.RecordBatch) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}
}
