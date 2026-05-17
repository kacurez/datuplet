package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// decodeConfig is a helper that decodes JSON into Config using
// json.Decoder with UseNumber so json.Number is preserved for literal rows.
func decodeConfig(t *testing.T, raw string) Config {
	t.Helper()
	var cfg Config
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&cfg); err != nil {
		t.Fatalf("json decode failed: %v", err)
	}
	return cfg
}

func TestParseAndValidate_EmptyTables(t *testing.T) {
	cfg := Config{}
	if err := ParseAndValidate(&cfg); err == nil {
		t.Fatal("expected error for empty tables, got nil")
	}
}

func TestParseAndValidate_MissingName(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Random: &RandomSpec{Schema: map[string]string{"id": "int"}, Limit: &Limit{RowsCount: 1}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("expected 'name' error, got: %v", err)
	}
}

func TestParseAndValidate_InvalidIdentifier(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "123bad", Random: &RandomSpec{Schema: map[string]string{"id": "int"}, Limit: &Limit{RowsCount: 1}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "valid identifier") {
		t.Fatalf("expected identifier error, got: %v", err)
	}
}

func TestParseAndValidate_BothModesSet(t *testing.T) {
	raw := `{"tables":[{"name":"t","random":{"schema":{"id":"int"},"limit":{"rowsCount":1}},"literal":{"columns":["id"],"rows":[[1]]}}]}`
	cfg := decodeConfig(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "choose exactly one mode") {
		t.Fatalf("expected mode conflict error, got: %v", err)
	}
}

func TestParseAndValidate_NeitherModeSet(t *testing.T) {
	cfg := Config{Tables: []Table{{Name: "t"}}}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "neither") {
		t.Fatalf("expected neither-mode error, got: %v", err)
	}
}

func TestParseAndValidate_UnknownColumnType(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "t", Random: &RandomSpec{Schema: map[string]string{"id": "decimal"}, Limit: &Limit{RowsCount: 1}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "decimal") {
		t.Fatalf("expected unknown-type error for decimal, got: %v", err)
	}
}

func TestParseAndValidate_AllKnownTypes(t *testing.T) {
	cols := map[string]string{
		"a": "string", "b": "int", "c": "long", "d": "float",
		"e": "double", "f": "boolean", "g": "date", "h": "timestamp",
		"i": "now", "j": "uuid",
	}
	cfg := Config{
		Tables: []Table{
			{Name: "t", Random: &RandomSpec{Schema: cols, Limit: &Limit{RowsCount: 1}}},
		},
	}
	if err := ParseAndValidate(&cfg); err != nil {
		t.Fatalf("expected no error for all known types, got: %v", err)
	}
}

func TestParseAndValidate_RandomSchemaMissing(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "t", Random: &RandomSpec{Limit: &Limit{RowsCount: 1}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("expected schema error, got: %v", err)
	}
}

func TestParseAndValidate_LimitMissing(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "t", Random: &RandomSpec{Schema: map[string]string{"id": "int"}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("expected limit error, got: %v", err)
	}
}

func TestParseAndValidate_LimitAllZero(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "t", Random: &RandomSpec{Schema: map[string]string{"id": "int"}, Limit: &Limit{}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("expected all-zero limit error, got: %v", err)
	}
}

func TestParseAndValidate_NegativeLimitRowsCount(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "t", Random: &RandomSpec{Schema: map[string]string{"id": "int"}, Limit: &Limit{RowsCount: -1}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "rowsCount must be >= 0") {
		t.Fatalf("expected negative-rowsCount error, got: %v", err)
	}
}

func TestParseAndValidate_NegativeLimitSizeInBytes(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "t", Random: &RandomSpec{Schema: map[string]string{"id": "int"}, Limit: &Limit{SizeInBytes: -1}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "sizeInBytes must be >= 0") {
		t.Fatalf("expected negative-sizeInBytes error, got: %v", err)
	}
}

func TestParseAndValidate_NegativeLimitTimeoutInSeconds(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "t", Random: &RandomSpec{Schema: map[string]string{"id": "int"}, Limit: &Limit{TimeoutInSeconds: -1}}},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "timeoutInSeconds must be >= 0") {
		t.Fatalf("expected negative-timeoutInSeconds error, got: %v", err)
	}
}

func TestParseAndValidate_NegativeRowInsertSpeed(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{
				Name:           "t",
				RowInsertSpeed: -1,
				Random:         &RandomSpec{Schema: map[string]string{"id": "int"}, Limit: &Limit{RowsCount: 1}},
			},
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "rowInsertSpeed") {
		t.Fatalf("expected rowInsertSpeed error, got: %v", err)
	}
}

func TestParseAndValidate_ValidRandomMode(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{
				Name:   "products",
				Random: &RandomSpec{Schema: map[string]string{"id": "int", "name": "string"}, Limit: &Limit{RowsCount: 100}},
			},
		},
	}
	if err := ParseAndValidate(&cfg); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestParseAndValidate_LiteralEmptyRows(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["id"],"rows":[]}}]}`
	cfg := decodeConfig(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "rows") {
		t.Fatalf("expected empty-rows error, got: %v", err)
	}
}

func TestParseAndValidate_LiteralMixedTypes(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a","b"],"rows":[[1,"hello"],[2,3]]}}]}`
	cfg := decodeConfig(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "mixed types") {
		t.Fatalf("expected mixed-type error, got: %v", err)
	}
}

func TestParseAndValidate_LiteralArityMismatch(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a","b"],"rows":[[1,2],[3]]}}]}`
	cfg := decodeConfig(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "arity") {
		t.Fatalf("expected arity error, got: %v", err)
	}
}

func TestParseAndValidate_LiteralAllNullColumn(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["a","b"],"rows":[[1,null],[2,null]]}}]}`
	cfg := decodeConfig(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "all-null") {
		t.Fatalf("expected all-null error, got: %v", err)
	}
}

func TestParseAndValidate_LiteralValidWithNullable(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"columns":["id","name"],"rows":[[1,"alice"],[2,null]]}}]}`
	cfg := decodeConfig(t, raw)
	if err := ParseAndValidate(&cfg); err != nil {
		t.Fatalf("expected no error for nullable column, got: %v", err)
	}
}

func TestParseAndValidate_LiteralColumnsRequired(t *testing.T) {
	raw := `{"tables":[{"name":"t","literal":{"rows":[[1,"alice"]]}}]}`
	cfg := decodeConfig(t, raw)
	err := ParseAndValidate(&cfg)
	if err == nil || !strings.Contains(err.Error(), "requires 'columns'") {
		t.Fatalf("expected columns-required error, got: %v", err)
	}
}

func TestParseAndValidate_MultipleTableErrors(t *testing.T) {
	cfg := Config{
		Tables: []Table{
			{Name: "t1"}, // neither mode
			{Name: "t2"}, // neither mode
		},
	}
	err := ParseAndValidate(&cfg)
	if err == nil {
		t.Fatal("expected errors for two bad tables, got nil")
	}
	if !strings.Contains(err.Error(), "t1") || !strings.Contains(err.Error(), "t2") {
		t.Fatalf("expected both table names in error, got: %v", err)
	}
}
