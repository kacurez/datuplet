package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestInferLiteralSchema_PrimitiveTypes verifies each JSON primitive maps to
// the expected type name.
func TestInferLiteralSchema_PrimitiveTypes(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantType string
	}{
		{"int", `[[42]]`, "int"},
		{"double", `[[3.14]]`, "double"},
		{"string", `[["hello"]]`, "string"},
		{"boolean_true", `[[true]]`, "boolean"},
		{"boolean_false", `[[false]]`, "boolean"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := decodeLiteralRows(t, tc.raw)
			types := InferLiteralSchema(rows)
			if len(types) != 1 {
				t.Fatalf("expected 1 column type, got %d", len(types))
			}
			if types[0] != tc.wantType {
				t.Errorf("got type %q, want %q", types[0], tc.wantType)
			}
		})
	}
}

// TestInferLiteralSchema_NullSkipped verifies null is skipped until a non-null
// value is found.
func TestInferLiteralSchema_NullSkipped(t *testing.T) {
	raw := `[[null],[null],["alice"]]`
	rows := decodeLiteralRows(t, raw)
	types := InferLiteralSchema(rows)
	if types[0] != "string" {
		t.Errorf("expected string after nulls, got %q", types[0])
	}
}

// TestInferLiteralSchema_MultipleColumns verifies multi-column inference.
func TestInferLiteralSchema_MultipleColumns(t *testing.T) {
	raw := `[[1,"alice",true],[2,"bob",false]]`
	rows := decodeLiteralRows(t, raw)
	types := InferLiteralSchema(rows)
	want := []string{"int", "string", "boolean"}
	if len(types) != len(want) {
		t.Fatalf("expected %d types, got %d: %v", len(want), len(types), types)
	}
	for i, w := range want {
		if types[i] != w {
			t.Errorf("col[%d]: got %q, want %q", i, types[i], w)
		}
	}
}

// TestValidateLiteral_MixedTypes verifies mixed int/string column is rejected.
func TestValidateLiteral_MixedTypes(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a","b"],"rows":[[1,"hello"],[2,3]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "mixed types") {
		t.Fatalf("expected mixed-type error, got: %v", err)
	}
}

// TestValidateLiteral_AllNullColumn verifies all-null column is rejected.
func TestValidateLiteral_AllNullColumn(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a","b"],"rows":[[1,null],[2,null],[3,null]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "all-null") {
		t.Fatalf("expected all-null error, got: %v", err)
	}
}

// TestValidateLiteral_ArityMismatch verifies row arity mismatch is rejected.
func TestValidateLiteral_ArityMismatch(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a","b","c"],"rows":[[1,2,3],[4,5]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "arity") {
		t.Fatalf("expected arity error, got: %v", err)
	}
}

// TestValidateLiteral_ValidRows verifies a well-formed literal table passes.
func TestValidateLiteral_ValidRows(t *testing.T) {
	raw := `{"tables":[{"name":"users","literal":{"columns":["id","name"],"rows":[[1,"alice"],[2,"bob"]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	if err := ParseAndValidate(&cfg); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

// TestValidateLiteral_SingleRow verifies a single-row literal is accepted.
func TestValidateLiteral_SingleRow(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a","b","c"],"rows":[[42,"x",true]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	if err := ParseAndValidate(&cfg); err != nil {
		t.Fatalf("expected no error for single row, got: %v", err)
	}
}

// TestValidateLiteral_IntAndDouble — int + double in same column → mixed types.
// Mixed types are a config error (no int→double widening).
func TestValidateLiteral_IntAndDouble(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a"],"rows":[[1],[3.14]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "mixed types") {
		t.Fatalf("expected mixed int/double error, got: %v", err)
	}
}

// TestValidateLiteral_ColumnsArityMismatch — names count must match row arity.
func TestValidateLiteral_ColumnsArityMismatch(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a","b","c"],"rows":[[1,"x"]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "lengths must match") {
		t.Fatalf("expected arity mismatch error, got: %v", err)
	}
}

// TestValidateLiteral_ColumnsDuplicate — duplicate names are rejected.
func TestValidateLiteral_ColumnsDuplicate(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["id","id"],"rows":[[1,"x"]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("expected duplicate error, got: %v", err)
	}
}

// TestValidateLiteral_ColumnsInvalidName — names must be valid identifiers.
func TestValidateLiteral_ColumnsInvalidName(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["1bad"],"rows":[[1]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "not a valid column name") {
		t.Fatalf("expected invalid-name error, got: %v", err)
	}
}

// TestValidateLiteral_ColumnsWithDot — dots are not allowed in column names.
func TestValidateLiteral_ColumnsWithDot(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["foo.bar"],"rows":[[1]]}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "not a valid column name") {
		t.Fatalf("expected dot-rejection, got: %v", err)
	}
}

// TestValidateRandom_InvalidColumnName — schema column names must be valid.
func TestValidateRandom_InvalidColumnName(t *testing.T) {
	raw := `{"tables":[{"name":"t","random":{"schema":{"1bad":"int"},"limit":{"rowsCount":1}}}]}`
	cfg := decodeCfgWithNumber(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "not a valid column name") {
		t.Fatalf("expected invalid-column-name error, got: %v", err)
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

func decodeCfgWithNumber(t *testing.T, raw string) Config {
	t.Helper()
	var cfg Config
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	return cfg
}
