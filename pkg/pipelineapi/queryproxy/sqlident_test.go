package queryproxy

import "testing"

func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"users":        `"users"`,
		"my-ns":        `"my-ns"`,
		"a.b":          `"a.b"`,
		`we"ird`:       `"we""ird"`,
		"":             `""`,
	}
	for in, want := range cases {
		if got := QuoteIdent(in); got != want {
			t.Errorf("QuoteIdent(%q) = %s, want %s", in, got, want)
		}
	}
}
