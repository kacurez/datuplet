package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"testing"
)

func TestResolveOutputTable(t *testing.T) {
	cases := []struct{ table, array, want string }{
		{"t", "a", "t"},
		{"", "a", "a"},
		{"", "", "data"},
	}
	for _, c := range cases {
		if got := resolveOutputTable(c.table, c.array); got != c.want {
			t.Fatalf("resolveOutputTable(%q,%q)=%q want %q", c.table, c.array, got, c.want)
		}
	}
}

func TestEncodeJSONL_LinesAndConcatSafety(t *testing.T) {
	a := []map[string]any{{"id": 1}, {"id": 2}}
	b := []map[string]any{{"id": 3}}

	ba, err := encodeJSONL(a)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := encodeJSONL(b)
	if err != nil {
		t.Fatal(err)
	}

	// Concatenating two encodeJSONL outputs (simulating coalesced pages in one
	// gateway POST) must still be valid, line-by-line JSONL: 3 parseable lines.
	joined := append(append([]byte{}, ba...), bb...)
	sc := bufio.NewScanner(bytes.NewReader(joined))
	n := 0
	for sc.Scan() {
		var obj map[string]any
		if err := json.Unmarshal(sc.Bytes(), &obj); err != nil {
			t.Fatalf("line %d not valid JSON: %v", n, err)
		}
		n++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("got %d JSONL lines, want 3", n)
	}
}

func TestEncodeJSONL_NoHTMLEscape(t *testing.T) {
	out, err := encodeJSONL([]map[string]any{{"u": "a&b<c>"}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("a&b<c>")) {
		t.Fatalf("value was HTML-escaped: %s", out)
	}
}

func TestProjectRecords(t *testing.T) {
	recs := []map[string]any{
		{"country": map[string]any{"id": "ZH", "value": "Africa"}, "iso": "AFE", "value": 5.0},
		{"country": "not-an-object", "iso": "XXX"}, // intermediate not object, missing value
	}
	fields := []FieldMapping{
		{Path: "country.value", Name: "entity"},
		{Path: "iso", Name: "iso3"},
		{Path: "value", Name: "population"},
	}
	out := projectRecords(recs, fields)
	if len(out) != 2 {
		t.Fatalf("got %d rows, want 2", len(out))
	}
	// row 0: all resolved
	if out[0]["entity"] != "Africa" || out[0]["iso3"] != "AFE" || out[0]["population"] != 5.0 {
		t.Fatalf("row0 wrong: %v", out[0])
	}
	// only projected keys present
	if len(out[0]) != 3 {
		t.Fatalf("row0 should have exactly 3 keys, got %v", out[0])
	}
	// row 1: non-object intermediate -> nil; missing -> nil
	if out[1]["entity"] != nil || out[1]["population"] != nil || out[1]["iso3"] != "XXX" {
		t.Fatalf("row1 wrong: %v", out[1])
	}
}

func TestProjectRecords_Identity(t *testing.T) {
	recs := []map[string]any{{"a": 1, "b": 2}}
	// nil / empty fields -> unchanged slice
	if got := projectRecords(recs, nil); len(got) != 1 || len(got[0]) != 2 {
		t.Fatalf("identity failed: %v", got)
	}
}
