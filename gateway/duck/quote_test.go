//go:build duckdb_arrow

package duck

import (
	"context"
	"testing"
)

var hostile = []string{
	`a"b`,
	`x'y`,
	`;DROP TABLE t;--`,
	"line1\nline2",
	"żluťoučký kůň 🐎",
	`""`,
	`''`,
	`\`,
	`"; SELECT 1; --`,
	`'); ATTACH 'x' AS y; --`,
}

func TestIdentLiteralRoundTrip(t *testing.T) {
	_, c := openConn(t)
	for _, s := range hostile {
		// The outer SELECT resolves the column by Ident(s): proves the quoted
		// identifier names exactly s.
		sql := "SELECT " + Ident(s) + " FROM (SELECT " + Literal(s) + " AS " + Ident(s) + ")"
		rows, err := c.QueryRows(context.Background(), sql)
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		if len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatalf("%q: want 1x1, got %v", s, rows)
		}
		if got, ok := rows[0][0].(string); !ok || got != s {
			t.Fatalf("round trip %q: got %T(%q)", s, rows[0][0], rows[0][0])
		}
	}
}

func TestQuoteShape(t *testing.T) {
	if got := Ident(`a"b`); got != `"a""b"` {
		t.Fatalf("Ident = %s", got)
	}
	if got := Literal(`x'y`); got != `'x''y'` {
		t.Fatalf("Literal = %s", got)
	}
}

func TestLiteralCheckedRejectsNUL(t *testing.T) {
	if _, err := LiteralChecked("a\x00b"); err == nil {
		t.Fatal("LiteralChecked with NUL: want error")
	}
	if got, err := LiteralChecked("ok'"); err != nil || got != `'ok'''` {
		t.Fatalf("LiteralChecked = %q, %v", got, err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Literal with NUL: want panic")
		}
	}()
	_ = Literal("a\x00b")
}
