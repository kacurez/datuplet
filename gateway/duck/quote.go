//go:build duckdb_arrow

package duck

import (
	"errors"
	"strings"
)

// ErrNUL is returned by LiteralChecked for a string containing a NUL byte:
// the C API truncates SQL text at NUL, so such a value cannot be expressed.
var ErrNUL = errors.New("duck: string literal contains a NUL byte")

// Ident quotes s as a SQL identifier: wrapped in " with every " doubled. It is
// the only way an identifier may enter SQL text. A NUL byte truncates the SQL
// at the C boundary inside the open quote, which DuckDB rejects as a parse error.
func Ident(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Literal quotes s as a SQL string literal: wrapped in ' with every ' doubled.
// It panics if s contains a NUL byte; use LiteralChecked for untrusted input.
func Literal(s string) string {
	q, err := LiteralChecked(s)
	if err != nil {
		panic(err)
	}
	return q
}

// LiteralChecked is Literal returning ErrNUL instead of panicking.
func LiteralChecked(s string) (string, error) {
	if strings.IndexByte(s, 0) >= 0 {
		return "", ErrNUL
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
}
