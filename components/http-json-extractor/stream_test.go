package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func collect(t *testing.T, body, arrayPath string) ([]map[string]any, int, error) {
	t.Helper()
	var got []map[string]any
	n, err := decodeRecords(strings.NewReader(body), arrayPath, func(rec map[string]any) error {
		got = append(got, rec)
		return nil
	})
	return got, n, err
}

func TestDecodeRecords_Shapes(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		arrayPath string
		wantLen   int
		wantKey   string
	}{
		{"bare_array", `[{"id":1},{"id":2}]`, "", 2, "id"},
		{"worldbank_positional", `[{"page":1,"pages":53},[{"countryiso3code":"SVK","value":5},{"countryiso3code":"CZE","value":10}]]`, "", 2, "countryiso3code"},
		{"object_arraypath", `{"offset":0,"results":[{"key":1},{"key":2},{"key":3}]}`, "results", 3, "key"},
		{"object_autodetect", `{"offset":0,"results":[{"key":1},{"key":2}]}`, "", 2, "key"},
		{"empty_bare_array", `[]`, "", 0, ""},
		{"empty_wrapped_array", `{"results":[]}`, "results", 0, ""},
		{"skips_non_objects", `[{"id":1},"noise",42,{"id":2}]`, "", 2, "id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, n, err := collect(t, tc.body, tc.arrayPath)
			if err != nil {
				t.Fatalf("decodeRecords error: %v", err)
			}
			if n != tc.wantLen || len(got) != tc.wantLen {
				t.Fatalf("got n=%d len=%d, want %d", n, len(got), tc.wantLen)
			}
			if tc.wantLen > 0 {
				if _, ok := got[0][tc.wantKey]; !ok {
					t.Fatalf("first record missing key %q: %v", tc.wantKey, got[0])
				}
			}
		})
	}
}

func TestDecodeRecords_UseNumber(t *testing.T) {
	got, _, err := collect(t, `[{"big":5938028332,"f":1.50}]`, "")
	if err != nil {
		t.Fatal(err)
	}
	n, ok := got[0]["big"].(json.Number)
	if !ok || n.String() != "5938028332" {
		t.Fatalf("big = %#v, want json.Number 5938028332", got[0]["big"])
	}
	f, ok := got[0]["f"].(json.Number)
	if !ok || f.String() != "1.50" {
		t.Fatalf("f = %#v, want json.Number 1.50 (exact source text)", got[0]["f"])
	}
}

func TestDecodeRecords_Errors(t *testing.T) {
	if _, _, err := collect(t, `not json`, ""); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// Exact error-text compatibility with parseJSON (main.go): a set array_path
// that is missing or non-array must say "field '<k>' is not an array"; the
// generic message is reserved for auto-detect finding nothing.
func TestDecodeRecords_ErrorText(t *testing.T) {
	_, _, err := collect(t, `{"results":"not-an-array"}`, "results")
	if err == nil || err.Error() != "field 'results' is not an array" {
		t.Fatalf("non-array array_path: got %v", err)
	}
	_, _, err = collect(t, `{"other":[{"x":1}]}`, "results") // key absent entirely
	if err == nil || err.Error() != "field 'results' is not an array" {
		t.Fatalf("absent array_path key: got %v", err)
	}
	_, _, err = collect(t, `{"no":"array here"}`, "")
	if err == nil || err.Error() != "no array found in JSON response, specify array_path in config" {
		t.Fatalf("auto-detect no array: got %v", err)
	}
}

// parseJSON unmarshalled the whole body, so a document that turns malformed
// AFTER the record array must still fail on the streaming path (drainToEOF).
func TestDecodeRecords_MalformedRemainder(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		arrayPath string
	}{
		{"wrapped_truncated_after_array", `{"results":[{"x":1}],`, "results"},
		{"positional_unclosed_outer", `[{"meta":1},[{"x":1}]`, ""},
		{"bare_unclosed", `[{"x":1}`, ""},
		{"bare_trailing_garbage", `[{"x":1}] garbage`, ""},
		{"trailing_second_json_value", `[{"x":1}] 42`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := collect(t, tc.body, tc.arrayPath); err == nil {
				t.Fatal("expected parse error for malformed document remainder")
			}
		})
	}
}

func TestDecodeRecords_FnErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	_, err := decodeRecords(strings.NewReader(`[{"id":1},{"id":2}]`), "", func(map[string]any) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("fn error not propagated: %v", err)
	}
}
