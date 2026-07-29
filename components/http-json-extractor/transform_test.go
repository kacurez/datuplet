package main

import (
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
