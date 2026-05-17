package processor

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// DropOp removes specific columns from a record.
type DropOp struct {
	columns map[string]bool
}

// NewDropOp creates a new drop operation.
func NewDropOp(columns []string) *DropOp {
	colMap := make(map[string]bool, len(columns))
	for _, col := range columns {
		colMap[col] = true
	}
	return &DropOp{columns: colMap}
}

// Apply applies the drop operation to a record batch.
func (d *DropOp) Apply(record arrow.Record, allocator memory.Allocator) (arrow.Record, error) {
	arrowSchema := record.Schema()

	// Build new schema and collect columns (excluding dropped ones)
	fields := make([]arrow.Field, 0, arrowSchema.NumFields())
	cols := make([]arrow.Array, 0, arrowSchema.NumFields())

	for i, field := range arrowSchema.Fields() {
		if d.columns[field.Name] {
			continue
		}
		fields = append(fields, field)
		col := record.Column(i)
		col.Retain()
		cols = append(cols, col)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("cannot drop all columns")
	}

	newSchema := arrow.NewSchema(fields, nil)
	return array.NewRecord(newSchema, cols, record.NumRows()), nil
}

// TransformSchema returns the schema without dropped columns.
func (d *DropOp) TransformSchema(input *schema.Schema) (*schema.Schema, error) {
	columns := make([]schema.ColumnDef, 0, input.NumColumns())
	for i := 0; i < input.NumColumns(); i++ {
		col := input.Column(i)
		if d.columns[col.Name] {
			continue
		}
		columns = append(columns, col)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("cannot drop all columns")
	}

	return schema.NewSchema(columns)
}

// String returns a string representation of the drop operation.
func (d *DropOp) String() string {
	cols := make([]string, 0, len(d.columns))
	for col := range d.columns {
		cols = append(cols, col)
	}
	return fmt.Sprintf("Drop(%s)", strings.Join(cols, ", "))
}
