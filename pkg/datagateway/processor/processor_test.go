package processor

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func makeTestRecord(allocator memory.Allocator) arrow.Record {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()

	// Row 0: id=1, name="Apple", price=1.50, quantity=10
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("Apple")
	builder.Field(2).(*array.Float64Builder).Append(1.50)
	builder.Field(3).(*array.Int64Builder).Append(10)

	// Row 1: id=2, name="Banana", price=0.75, quantity=20
	builder.Field(0).(*array.Int64Builder).Append(2)
	builder.Field(1).(*array.StringBuilder).Append("Banana")
	builder.Field(2).(*array.Float64Builder).Append(0.75)
	builder.Field(3).(*array.Int64Builder).Append(20)

	// Row 2: id=3, name="Cherry", price=2.00, quantity=5
	builder.Field(0).(*array.Int64Builder).Append(3)
	builder.Field(1).(*array.StringBuilder).Append("Cherry")
	builder.Field(2).(*array.Float64Builder).Append(2.00)
	builder.Field(3).(*array.Int64Builder).Append(5)

	return builder.NewRecord()
}

func makeTestSchema() *schema.Schema {
	s, _ := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "price", Type: schema.TypeFloat64, Nullable: true},
		{Name: "quantity", Type: schema.TypeInt64, Nullable: false},
	})
	return s
}

func TestDropOp(t *testing.T) {
	allocator := memory.NewGoAllocator()
	record := makeTestRecord(allocator)
	defer record.Release()

	op := NewDropOp([]string{"price"})

	result, err := op.Apply(record, allocator)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	defer result.Release()

	if result.NumCols() != 3 {
		t.Errorf("NumCols() = %d, want 3", result.NumCols())
	}

	// Verify price column is gone
	for i := 0; i < int(result.NumCols()); i++ {
		if result.Schema().Field(i).Name == "price" {
			t.Error("price column should be dropped")
		}
	}

	// Verify schema transform
	inputSchema := makeTestSchema()
	outputSchema, err := op.TransformSchema(inputSchema)
	if err != nil {
		t.Fatalf("TransformSchema error: %v", err)
	}
	if outputSchema.NumColumns() != 3 {
		t.Errorf("schema NumColumns() = %d, want 3", outputSchema.NumColumns())
	}
}

func TestDropOpMultipleColumns(t *testing.T) {
	allocator := memory.NewGoAllocator()
	record := makeTestRecord(allocator)
	defer record.Release()

	op := NewDropOp([]string{"price", "quantity"})

	result, err := op.Apply(record, allocator)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	defer result.Release()

	if result.NumCols() != 2 {
		t.Errorf("NumCols() = %d, want 2", result.NumCols())
	}

	// Should only have id and name
	if result.Schema().Field(0).Name != "id" {
		t.Errorf("column 0 = %s, want id", result.Schema().Field(0).Name)
	}
	if result.Schema().Field(1).Name != "name" {
		t.Errorf("column 1 = %s, want name", result.Schema().Field(1).Name)
	}
}

func TestDropOpAllColumns(t *testing.T) {
	allocator := memory.NewGoAllocator()
	record := makeTestRecord(allocator)
	defer record.Release()

	op := NewDropOp([]string{"id", "name", "price", "quantity"})

	_, err := op.Apply(record, allocator)
	if err == nil {
		t.Error("expected error when dropping all columns")
	}
}

func TestDropOpString(t *testing.T) {
	op := NewDropOp([]string{"price", "quantity"})
	str := op.String()
	if str == "" {
		t.Error("String() should not be empty")
	}
}

func TestPipeline(t *testing.T) {
	allocator := memory.NewGoAllocator()
	record := makeTestRecord(allocator)
	defer record.Release()

	pipeline := NewPipeline()
	pipeline.Add(NewDropOp([]string{"price", "quantity"}))

	result, err := pipeline.Apply(record, allocator)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	defer result.Release()

	// Should have 2 columns (id, name)
	if result.NumCols() != 2 {
		t.Errorf("NumCols() = %d, want 2", result.NumCols())
	}

	// Row count should be unchanged
	if result.NumRows() != 3 {
		t.Errorf("NumRows() = %d, want 3", result.NumRows())
	}
}

func TestPipelineEmpty(t *testing.T) {
	allocator := memory.NewGoAllocator()
	record := makeTestRecord(allocator)
	defer record.Release()

	pipeline := NewPipeline()

	result, err := pipeline.Apply(record, allocator)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	defer result.Release()

	// Empty pipeline should return same data
	if result.NumRows() != record.NumRows() {
		t.Errorf("NumRows() = %d, want %d", result.NumRows(), record.NumRows())
	}
	if result.NumCols() != record.NumCols() {
		t.Errorf("NumCols() = %d, want %d", result.NumCols(), record.NumCols())
	}
}

func TestPipelineTransformSchema(t *testing.T) {
	inputSchema := makeTestSchema()

	pipeline := NewPipeline()
	pipeline.Add(NewDropOp([]string{"price"}))

	outputSchema, err := pipeline.TransformSchema(inputSchema)
	if err != nil {
		t.Fatalf("TransformSchema error: %v", err)
	}

	if outputSchema.NumColumns() != 3 {
		t.Errorf("NumColumns() = %d, want 3", outputSchema.NumColumns())
	}
}

func TestDropOpSchemaError(t *testing.T) {
	inputSchema := makeTestSchema()

	// Try to drop all columns
	op := NewDropOp([]string{"id", "name", "price", "quantity"})
	_, err := op.TransformSchema(inputSchema)
	if err == nil {
		t.Error("expected error for dropping all columns")
	}
}
