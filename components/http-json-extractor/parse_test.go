package main

import "testing"

func TestParseJSON_Shapes(t *testing.T) {
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recs, err := parseJSON([]byte(tc.body), tc.arrayPath)
			if err != nil {
				t.Fatalf("parseJSON error: %v", err)
			}
			if len(recs) != tc.wantLen {
				t.Fatalf("got %d records, want %d", len(recs), tc.wantLen)
			}
			if _, ok := recs[0][tc.wantKey]; !ok {
				t.Fatalf("first record missing key %q: %v", tc.wantKey, recs[0])
			}
		})
	}
}

func TestParseJSON_Invalid(t *testing.T) {
	if _, err := parseJSON([]byte(`{"no":"array here"}`), ""); err == nil {
		t.Fatal("expected error for object with no array field")
	}
}
