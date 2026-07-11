package queryproxy

import "strings"

// QuoteIdent wraps s in double quotes for embedding in DuckDB SQL,
// doubling embedded double quotes. Callers must still validate s
// (storage.ValidIdentifier) — quoting is defense in depth plus
// disambiguation for dot/hyphen names. Mirrors escapeSQLIdent in
// components/queryengine/attach.go; the two sit on opposite sides of a
// Go-module + build-tag boundary, so the lines are duplicated by design.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
