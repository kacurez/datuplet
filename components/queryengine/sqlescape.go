//go:build duckdb_arrow

package queryengine

// escapeSQL escapes a value for embedding into a DuckDB single-quoted string
// literal. DuckDB uses standard SQL: a single quote is doubled.
//
// Contract: single-quote doubling ONLY. DuckDB plain '...' literals treat a
// backslash as a literal backslash — there are no C-style escape sequences
// without the E'...' prefix — so doubling the single quote is sufficient to
// neutralize the only metacharacter inside a plain literal. Values containing
// a newline or NUL are not specially handled here; they pass through verbatim
// and fail at DuckDB parse time, which is safe (no smuggling, just an error).
func escapeSQL(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'', '\'')
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}
