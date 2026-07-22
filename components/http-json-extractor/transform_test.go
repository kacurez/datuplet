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
