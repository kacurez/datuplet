//go:build duckdb_arrow

package queryengine

import "testing"

func TestEscapeSQL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"plain unchanged", "abc_def-123", "abc_def-123"},
		{"single quote doubled", "a'b", "a''b"},
		{"two quotes doubled twice", "a''b", "a''''b"},
		{"backslash passes through", `a\b`, `a\b`},
		{"newline passes through", "a\nb", "a\nb"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := escapeSQL(tc.in); got != tc.want {
				t.Fatalf("escapeSQL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestEscapeSQLIdent(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"plain unchanged", "schema_name-1", "schema_name-1"},
		{"double quote doubled", `a"b`, `a""b`},
		{"two double quotes doubled twice", `a""b`, `a""""b`},
		{"single quote unchanged", "a'b", "a'b"},
		{"semicolon unchanged", "a;b", "a;b"},
		{"space unchanged", "a b", "a b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := escapeSQLIdent(tc.in); got != tc.want {
				t.Fatalf("escapeSQLIdent(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
