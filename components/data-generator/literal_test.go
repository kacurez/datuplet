package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestInferLiteralSchema verifies JSON primitives map to the expected type
// names, null is skipped until a non-null value is found, and multi-column
// inference works.
func TestInferLiteralSchema(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"int", `[[42]]`, []string{"int"}},
		{"double", `[[3.14]]`, []string{"double"}},
		{"string", `[["hello"]]`, []string{"string"}},
		{"boolean_true", `[[true]]`, []string{"boolean"}},
		{"boolean_false", `[[false]]`, []string{"boolean"}},
		{"null_skipped", `[[null],[null],["alice"]]`, []string{"string"}},
		{"multiple_columns", `[[1,"alice",true],[2,"bob",false]]`, []string{"int", "string", "boolean"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			types := InferLiteralSchema(decodeLiteralRows(t, tc.raw))
			if len(types) != len(tc.want) {
				t.Fatalf("expected %d types, got %d: %v", len(tc.want), len(types), types)
			}
			for i, w := range tc.want {
				if types[i] != w {
					t.Errorf("col[%d]: got %q, want %q", i, types[i], w)
				}
			}
		})
	}
}

// TestValidateLiteral drives literal/random column validation through
// ParseAndValidate. wantErr "" means the config must validate.
func TestValidateLiteral(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name:    "mixed types rejected",
			raw:     `{"tables":[{"name":"t","literal":{"columns":["a","b"],"rows":[[1,"hello"],[2,3]]}}]}`,
			wantErr: "mixed types",
		},
		{
			name:    "all-null column rejected",
			raw:     `{"tables":[{"name":"t","literal":{"columns":["a","b"],"rows":[[1,null],[2,null],[3,null]]}}]}`,
			wantErr: "all-null",
		},
		{
			name:    "row arity mismatch rejected",
			raw:     `{"tables":[{"name":"t","literal":{"columns":["a","b","c"],"rows":[[1,2,3],[4,5]]}}]}`,
			wantErr: "arity",
		},
		{
			name: "valid rows",
			raw:  `{"tables":[{"name":"users","literal":{"columns":["id","name"],"rows":[[1,"alice"],[2,"bob"]]}}]}`,
		},
		{
			name: "single row",
			raw:  `{"tables":[{"name":"t","literal":{"columns":["a","b","c"],"rows":[[42,"x",true]]}}]}`,
		},
		{
			// Mixed types are a config error (no int->double widening).
			name:    "int and double in one column rejected",
			raw:     `{"tables":[{"name":"t","literal":{"columns":["a"],"rows":[[1],[3.14]]}}]}`,
			wantErr: "mixed types",
		},
		{
			// Names count must match row arity.
			name:    "columns arity mismatch rejected",
			raw:     `{"tables":[{"name":"t","literal":{"columns":["a","b","c"],"rows":[[1,"x"]]}}]}`,
			wantErr: "lengths must match",
		},
		{
			name:    "duplicate column names rejected",
			raw:     `{"tables":[{"name":"t","literal":{"columns":["id","id"],"rows":[[1,"x"]]}}]}`,
			wantErr: "duplicated",
		},
		{
			name:    "invalid column name rejected",
			raw:     `{"tables":[{"name":"t","literal":{"columns":["1bad"],"rows":[[1]]}}]}`,
			wantErr: "not a valid column name",
		},
		{
			// Dots are not allowed in column names.
			name:    "column name with dot rejected",
			raw:     `{"tables":[{"name":"t","literal":{"columns":["foo.bar"],"rows":[[1]]}}]}`,
			wantErr: "not a valid column name",
		},
		{
			name:    "random schema invalid column name rejected",
			raw:     `{"tables":[{"name":"t","random":{"schema":{"1bad":"int"},"limit":{"rowsCount":1}}}]}`,
			wantErr: "not a valid column name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := decodeConfig(t, tc.raw)
			err := ParseAndValidate(&cfg)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

// TestJsonLiteralType verifies the type classification helper.
func TestJsonLiteralType(t *testing.T) {
	cases := []struct {
		input    any
		wantType string
		wantErr  bool
	}{
		{json.Number("42"), "int", false},
		{json.Number("3.14"), "double", false},
		{json.Number("1e10"), "double", false},
		{"hello", "string", false},
		{true, "boolean", false},
		{false, "boolean", false},
		{float64(1.5), "double", false},
		{[]any{1, 2}, "", true}, // unsupported type
	}

	for _, tc := range cases {
		typ, err := jsonLiteralType(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("input %v: expected error, got nil", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("input %v: unexpected error: %v", tc.input, err)
			continue
		}
		if typ != tc.wantType {
			t.Errorf("input %v: got %q, want %q", tc.input, typ, tc.wantType)
		}
	}
}

// ---- helpers ----

func decodeLiteralRows(t *testing.T, raw string) [][]any {
	t.Helper()
	var rows [][]any
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&rows); err != nil {
		t.Fatalf("decode rows: %v", err)
	}
	return rows
}
