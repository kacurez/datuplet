package main

import "testing"

func TestParseAndValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"ok_no_fields", Config{URL: "https://x"}, false},
		{"ok_fields", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a.b", Name: "ab"}, {Path: "c", Name: "c_1"}}}, false},
		{"empty_url", Config{}, true},
		{"empty_path", Config{URL: "https://x", Fields: []FieldMapping{{Path: "", Name: "n"}}}, true},
		{"bad_name_dot", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a", Name: "a.b"}}}, true},
		{"bad_name_space", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a", Name: "a b"}}}, true},
		{"bad_name_empty", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a", Name: ""}}}, true},
		{"dup_name", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a", Name: "n"}, {Path: "b", Name: "n"}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ParseAndValidate(&tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseAndValidate() err=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}
