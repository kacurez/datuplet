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

func TestParseAndValidate(t *testing.T) {
	intSchema := map[string]string{"id": "int"}
	cases := []struct {
		name string
		cfg  Config
		raw  string // when set, decoded into cfg via decodeConfig
		// wantErr false means the config must validate. wantSubstr entries must
		// all appear in the error.
		wantErr    bool
		wantSubstr []string
	}{
		{name: "empty tables", cfg: Config{}, wantErr: true},
		{
			name:       "missing name",
			cfg:        Config{Tables: []Table{{Random: &RandomSpec{Schema: intSchema, Limit: &Limit{RowsCount: 1}}}}},
			wantErr:    true,
			wantSubstr: []string{"name"},
		},
		{
			name:       "invalid identifier",
			cfg:        Config{Tables: []Table{{Name: "123bad", Random: &RandomSpec{Schema: intSchema, Limit: &Limit{RowsCount: 1}}}}},
			wantErr:    true,
			wantSubstr: []string{"valid identifier"},
		},
		{
			name:       "both modes set",
			raw:        `{"tables":[{"name":"t","random":{"schema":{"id":"int"},"limit":{"rowsCount":1}},"literal":{"columns":["id"],"rows":[[1]]}}]}`,
			wantErr:    true,
			wantSubstr: []string{"choose exactly one mode"},
		},
		{
			name:       "neither mode set",
			cfg:        Config{Tables: []Table{{Name: "t"}}},
			wantErr:    true,
			wantSubstr: []string{"neither"},
		},
		{
			name:       "unknown column type",
			cfg:        Config{Tables: []Table{{Name: "t", Random: &RandomSpec{Schema: map[string]string{"id": "decimal"}, Limit: &Limit{RowsCount: 1}}}}},
			wantErr:    true,
			wantSubstr: []string{"decimal"},
		},
		{
			name: "all known types",
			cfg: Config{Tables: []Table{{Name: "t", Random: &RandomSpec{
				Schema: map[string]string{
					"a": "string", "b": "int", "c": "long", "d": "float",
					"e": "double", "f": "boolean", "g": "date", "h": "timestamp",
					"i": "now", "j": "uuid",
				},
				Limit: &Limit{RowsCount: 1},
			}}}},
		},
		{
			name:       "random schema missing",
			cfg:        Config{Tables: []Table{{Name: "t", Random: &RandomSpec{Limit: &Limit{RowsCount: 1}}}}},
			wantErr:    true,
			wantSubstr: []string{"schema"},
		},
		{
			name:       "limit missing",
			cfg:        Config{Tables: []Table{{Name: "t", Random: &RandomSpec{Schema: intSchema}}}},
			wantErr:    true,
			wantSubstr: []string{"limit"},
		},
		{
			name:       "limit all zero",
			cfg:        Config{Tables: []Table{{Name: "t", Random: &RandomSpec{Schema: intSchema, Limit: &Limit{}}}}},
			wantErr:    true,
			wantSubstr: []string{"at least one"},
		},
		{
			name:       "negative limit rowsCount",
			cfg:        Config{Tables: []Table{{Name: "t", Random: &RandomSpec{Schema: intSchema, Limit: &Limit{RowsCount: -1}}}}},
			wantErr:    true,
			wantSubstr: []string{"rowsCount must be >= 0"},
		},
		{
			name:       "negative limit sizeInBytes",
			cfg:        Config{Tables: []Table{{Name: "t", Random: &RandomSpec{Schema: intSchema, Limit: &Limit{SizeInBytes: -1}}}}},
			wantErr:    true,
			wantSubstr: []string{"sizeInBytes must be >= 0"},
		},
		{
			name:       "negative limit timeoutInSeconds",
			cfg:        Config{Tables: []Table{{Name: "t", Random: &RandomSpec{Schema: intSchema, Limit: &Limit{TimeoutInSeconds: -1}}}}},
			wantErr:    true,
			wantSubstr: []string{"timeoutInSeconds must be >= 0"},
		},
		{
			name: "negative rowInsertSpeed",
			cfg: Config{Tables: []Table{{
				Name:           "t",
				RowInsertSpeed: -1,
				Random:         &RandomSpec{Schema: intSchema, Limit: &Limit{RowsCount: 1}},
			}}},
			wantErr:    true,
			wantSubstr: []string{"rowInsertSpeed"},
		},
		{
			name: "valid random mode",
			cfg: Config{Tables: []Table{{
				Name:   "products",
				Random: &RandomSpec{Schema: map[string]string{"id": "int", "name": "string"}, Limit: &Limit{RowsCount: 100}},
			}}},
		},
		{
			name:       "literal empty rows",
			raw:        `{"tables":[{"name":"t","literal":{"columns":["id"],"rows":[]}}]}`,
			wantErr:    true,
			wantSubstr: []string{"rows"},
		},
		{
			// Restores the deleted LiteralValidWithNullable: only test that a
			// column with SOME nulls (but not all) passes ParseAndValidate.
			name: "nullable column with some nulls accepted",
			raw:  `{"tables":[{"name":"t","literal":{"columns":["id","name"],"rows":[[1,"alice"],[2,null]]}}]}`,
		},
		{
			name:       "literal columns required",
			raw:        `{"tables":[{"name":"t","literal":{"rows":[[1,"alice"]]}}]}`,
			wantErr:    true,
			wantSubstr: []string{"requires 'columns'"},
		},
		{
			name:       "multiple table errors name every table",
			cfg:        Config{Tables: []Table{{Name: "t1"}, {Name: "t2"}}},
			wantErr:    true,
			wantSubstr: []string{"t1", "t2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			if tc.raw != "" {
				cfg = decodeConfig(t, tc.raw)
			}
			err := ParseAndValidate(&cfg)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			for _, sub := range tc.wantSubstr {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error should contain %q, got: %v", sub, err)
				}
			}
		})
	}
}
