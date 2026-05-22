package main

import "testing"

func TestParseNsTable(t *testing.T) {
	cases := []struct {
		in      string
		ns, tbl string
		wantErr bool
	}{
		{"raw.products", "raw", "products", false},
		{"my_ns.my_table", "my_ns", "my_table", false},
		{"noseparator", "", "", true},
		{"too.many.dots", "", "", true},
		{".missingNs", "", "", true},
		{"missingTbl.", "", "", true},
	}
	for _, tc := range cases {
		ns, tbl, err := parseNsTable(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseNsTable(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseNsTable(%q) unexpected error: %v", tc.in, err)
		}
		if ns != tc.ns || tbl != tc.tbl {
			t.Errorf("parseNsTable(%q) = (%q,%q), want (%q,%q)", tc.in, ns, tbl, tc.ns, tc.tbl)
		}
	}
}
