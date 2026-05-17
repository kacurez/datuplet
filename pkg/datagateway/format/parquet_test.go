package format

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestParquetAdapterFormat(t *testing.T) {
	adapter := NewParquetAdapter(nil, nil)
	if adapter.Format() != FormatParquet {
		t.Errorf("Format() = %v, want FormatParquet", adapter.Format())
	}
}

func TestParquetAdapterSerializeNotSupported(t *testing.T) {
	adapter := NewParquetAdapter(nil, nil)
	_, err := adapter.Serialize(nil)
	if err == nil {
		t.Error("Serialize() should return error (not supported)")
	}
}

func TestParquetAdapterParse(t *testing.T) {
	// First create a Parquet file
	allocator := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	// Build test data
	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()

	for i := 0; i < 100; i++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		builder.Field(1).(*array.StringBuilder).Append("item_" + string(rune('A'+i%26)))
		builder.Field(2).(*array.Float64Builder).Append(float64(i) * 9.99)
	}

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet
	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(allocator))

	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, writerProps, arrowProps)
	if err != nil {
		t.Fatalf("Failed to create Parquet writer: %v", err)
	}

	if err := writer.WriteBuffered(record); err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Now test the adapter
	adapter := NewParquetAdapter(allocator, nil)
	parsedRecord, parsedSchema, err := adapter.Parse(buf.Bytes(), nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer parsedRecord.Release()

	// Verify
	if parsedRecord.NumRows() != 100 {
		t.Errorf("NumRows() = %d, want 100", parsedRecord.NumRows())
	}

	if parsedSchema.NumColumns() != 3 {
		t.Errorf("Schema has %d columns, want 3", parsedSchema.NumColumns())
	}

	// Check column types
	if parsedSchema.Column(0).Type != schema.TypeInt64 {
		t.Errorf("Column 0 type = %v, want int64", parsedSchema.Column(0).Type)
	}
	if parsedSchema.Column(1).Type != schema.TypeString {
		t.Errorf("Column 1 type = %v, want string", parsedSchema.Column(1).Type)
	}
	if parsedSchema.Column(2).Type != schema.TypeFloat64 {
		t.Errorf("Column 2 type = %v, want float64", parsedSchema.Column(2).Type)
	}
}

func TestParquetAdapterParseInvalid(t *testing.T) {
	adapter := NewParquetAdapter(nil, nil)

	// Invalid data
	_, _, err := adapter.Parse([]byte("not parquet data"), nil)
	if err == nil {
		t.Error("Parse() should return error for invalid data")
	}
}
