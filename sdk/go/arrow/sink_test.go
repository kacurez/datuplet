package arrow

import (
	"encoding/json"
	"testing"
)

func TestStringifyValue(t *testing.T) {
	cases := []struct {
		name     string
		in       any
		want     string
		wantNull bool
	}{
		{"nil", nil, "", true},
		{"string", "hello", "hello", false},
		{"json_number_int", json.Number("5938028332"), "5938028332", false},
		{"json_number_frac", json.Number("1.50"), "1.50", false},
		{"bool", true, "true", false},
		{"float64_defensive", float64(2.5), "2.5", false},
		{"nested_object", map[string]any{"a": json.Number("1")}, `{"a":1}`, false},
		{"nested_array", []any{json.Number("1"), "x"}, `[1,"x"]`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, null := stringifyValue(tc.in)
			if null != tc.wantNull || got != tc.want {
				t.Fatalf("stringifyValue(%#v) = (%q,%v), want (%q,%v)", tc.in, got, null, tc.want, tc.wantNull)
			}
		})
	}
}

func TestPlanFromBatch(t *testing.T) {
	batch := []map[string]any{
		{"b": 1, "a": 2},
		{"c": 3}, // later-only key must still become a column (union)
	}
	p := planFromBatch(batch)
	if len(p.names) != 3 || p.names[0] != "a" || p.names[1] != "b" || p.names[2] != "c" {
		t.Fatalf("names = %v, want sorted union [a b c]", p.names)
	}
	if v := p.extract(batch[1], 2); v != 3 {
		t.Fatalf("extract c = %v", v)
	}
	if v := p.extract(batch[1], 0); v != nil {
		t.Fatalf("absent key should be nil, got %v", v)
	}
}
