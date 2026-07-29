package main

import "testing"

func TestFieldColumns(t *testing.T) {
	names, extract := fieldColumns([]FieldMapping{
		{Path: "country.value", Name: "entity"},
		{Path: "iso", Name: "iso3"},
	})
	if len(names) != 2 || names[0] != "entity" || names[1] != "iso3" {
		t.Fatalf("names = %v (declared order required)", names)
	}
	rec := map[string]any{"country": map[string]any{"value": "Africa"}, "iso": "AFE"}
	if v := extract(rec, 0); v != "Africa" {
		t.Fatalf("extract nested = %v", v)
	}
	if v := extract(rec, 1); v != "AFE" {
		t.Fatalf("extract flat = %v", v)
	}
	if v := extract(map[string]any{}, 0); v != nil {
		t.Fatalf("missing path should be nil, got %v", v)
	}
}
