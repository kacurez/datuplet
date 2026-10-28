package format

import (
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestCSVAdapterParseInferredTypes(t *testing.T) {
	tests := []struct {
		name      string
		csv       string
		wantTypes map[int]schema.DataType // column index -> inferred type
		wantCols  int
		wantRows  int64
	}{
		{
			name:      "simple int/string/float",
			csv:       "id,name,price\n1,Widget,9.99\n2,Gadget,19.99",
			wantTypes: map[int]schema.DataType{0: schema.TypeInt64, 1: schema.TypeString, 2: schema.TypeFloat64},
			wantCols:  3,
			wantRows:  2,
		},
		{
			name:      "timestamps",
			csv:       "id,created_at\n1,2024-01-15T10:30:00Z\n2,2024-02-20T15:45:00Z",
			wantTypes: map[int]schema.DataType{1: schema.TypeTimestamp},
			wantCols:  2,
			wantRows:  2,
		},
		{
			name:      "dates",
			csv:       "id,birth_date\n1,2000-01-15\n2,1995-06-20",
			wantTypes: map[int]schema.DataType{1: schema.TypeDate},
			wantCols:  2,
			wantRows:  2,
		},
		{
			name:      "booleans",
			csv:       "id,active\n1,true\n2,false\n3,yes\n4,no",
			wantTypes: map[int]schema.DataType{1: schema.TypeBool},
			wantCols:  2,
			wantRows:  4,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record, s, err := NewCSVAdapter(nil, nil).Parse([]byte(tc.csv), nil)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			defer record.Release()

			if s.NumColumns() != tc.wantCols {
				t.Errorf("Schema has %d columns, want %d", s.NumColumns(), tc.wantCols)
			}
			for i, want := range tc.wantTypes {
				if got := s.Column(i).Type; got != want {
					t.Errorf("Column %d (%s) type = %v, want %v", i, s.Column(i).Name, got, want)
				}
			}
			if record.NumRows() != tc.wantRows {
				t.Errorf("NumRows() = %d, want %d", record.NumRows(), tc.wantRows)
			}
		})
	}
}

func TestCSVAdapterParseOptions(t *testing.T) {
	type cell struct{ col, row int }
	tests := []struct {
		name        string
		opts        *ParseOptions
		csv         string
		wantCols    int
		wantNames   []string // when non-nil, the leading column names
		wantRows    int64
		wantNull    []cell
		wantNotNull []cell
	}{
		{
			name:      "no header generates col0..colN names",
			opts:      &ParseOptions{HasHeader: false, Delimiter: ','},
			csv:       "1,Widget,9.99\n2,Gadget,19.99",
			wantCols:  3,
			wantNames: []string{"col0", "col1", "col2"},
			wantRows:  2,
		},
		{
			name:     "custom delimiter",
			opts:     &ParseOptions{HasHeader: true, Delimiter: ';'},
			csv:      "id;name;price\n1;Widget;9.99\n2;Gadget;19.99",
			wantCols: 3,
			wantRows: 2,
		},
		{
			name:     "header only has schema but zero rows",
			csv:      "id,name,price",
			wantCols: 3,
			wantRows: 0,
		},
		{
			name:        "empty fields become nulls",
			csv:         "id,name,price\n1,Widget,9.99\n2,,\n3,Gadget,",
			wantCols:    3,
			wantRows:    3,
			wantNull:    []cell{{1, 1}, {2, 1}, {2, 2}},
			wantNotNull: []cell{{1, 0}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record, s, err := NewCSVAdapter(nil, tc.opts).Parse([]byte(tc.csv), nil)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			defer record.Release()

			if s.NumColumns() != tc.wantCols {
				t.Errorf("Schema has %d columns, want %d", s.NumColumns(), tc.wantCols)
			}
			for i, want := range tc.wantNames {
				if got := s.Column(i).Name; got != want {
					t.Errorf("Column %d name = %q, want %q", i, got, want)
				}
			}
			if record.NumRows() != tc.wantRows {
				t.Errorf("NumRows() = %d, want %d", record.NumRows(), tc.wantRows)
			}
			for _, c := range tc.wantNull {
				if !record.Column(c.col).IsNull(c.row) {
					t.Errorf("row %d, column %d should be null", c.row, c.col)
				}
			}
			for _, c := range tc.wantNotNull {
				if record.Column(c.col).IsNull(c.row) {
					t.Errorf("row %d, column %d should not be null", c.row, c.col)
				}
			}
		})
	}
}

func TestCSVAdapterParseEmpty(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	_, _, err := adapter.Parse([]byte(""), nil)
	if err == nil {
		t.Error("Parse() should return error for empty data")
	}
}

func TestCSVAdapterRoundTrip(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	original := []byte("id,name,active,score\n1,Alice,true,95.5\n2,Bob,false,87.3\n3,Charlie,true,91.0")

	// Parse
	record, s, err := adapter.Parse(original, nil)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	defer record.Release()

	// Serialize
	output, err := adapter.Serialize(record)
	if err != nil {
		t.Fatalf("Serialize() error: %v", err)
	}

	// Serialized output carries the header and the data
	outputStr := string(output)
	if !strings.Contains(outputStr, "id,name,active,score") {
		t.Errorf("Output missing header: %s", outputStr)
	}
	if !strings.Contains(outputStr, "Alice") {
		t.Errorf("Output missing data: %s", outputStr)
	}

	// Parse again
	record2, s2, err := adapter.Parse(output, nil)
	if err != nil {
		t.Fatalf("Second Parse() error: %v", err)
	}
	defer record2.Release()

	// Compare
	if s.NumColumns() != s2.NumColumns() {
		t.Errorf("Schema columns: %d vs %d", s.NumColumns(), s2.NumColumns())
	}
	if record.NumRows() != record2.NumRows() {
		t.Errorf("Row counts: %d vs %d", record.NumRows(), record2.NumRows())
	}
}

func TestCSVAdapterColumnMismatch(t *testing.T) {
	adapter := NewCSVAdapter(nil, nil)

	// Schema has 2 columns
	columns := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
	}
	s, _ := schema.NewSchema(columns)

	// CSV has 3 columns
	csvData := []byte("id,name,price\n1,Widget,9.99")
	_, _, err := adapter.Parse(csvData, s)
	if err == nil {
		t.Error("Parse() should return error for column count mismatch")
	}
}
