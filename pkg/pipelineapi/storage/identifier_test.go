package storage

import (
	"strings"
	"testing"
)

func TestValidIdentifier(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"public", true},
		{"my_table", true},
		{"a.b-c_d", true},
		{"A1", true},
		{strings.Repeat("a", 128), true}, // boundary: 128 chars exactly
		{"", false},
		{"_starts_with_underscore", false},
		{"-dash-first", false},
		{"..", false},
		{"with/slash", false},
		{"with space", false},
		{"with%2e", false},
		{"with\x00null", false},
		{strings.Repeat("a", 129), false}, // boundary + 1
	}
	for _, tc := range cases {
		if got := ValidIdentifier(tc.in); got != tc.want {
			t.Errorf("ValidIdentifier(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
