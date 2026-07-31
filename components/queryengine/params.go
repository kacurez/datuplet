package queryengine

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// maxNameLen is the placeholder-name limit from the RFC 028 §6.1 grammar
// $([A-Za-z_][A-Za-z0-9_]{0,63}) — one leading character plus up to 63 more.
const maxNameLen = 64

// maxSafeInteger is Number.MAX_SAFE_INTEGER (2^53−1). Spec §6.1 binds integral
// numbers within ±maxSafeInteger as BIGINT and rejects everything outside it.
const maxSafeInteger = int64(9007199254740991)

// maxSafeRat is maxSafeInteger as an exact rational, for magnitude comparison.
var maxSafeRat = new(big.Rat).SetInt64(maxSafeInteger)

// ParamError is returned when parameter validation fails.
type ParamError struct {
	Msg string
}

func (e *ParamError) Error() string {
	return e.Msg
}

// span represents a placeholder location and name in SQL.
type span struct {
	offset int
	name   string
}

// ValidateParams validates SQL and bound parameters according to RFC 028 §6.1.
// It returns the ordered distinct placeholder names if successful, or a ParamError if validation fails.
//
// The function:
//  1. Scans the SQL for placeholder names matching $([A-Za-z_][A-Za-z0-9_]{0,63})
//  2. Ignores placeholders inside quoted strings, dollar-quoted strings and comments
//  3. Validates that every placeholder has a corresponding parameter and vice versa
//  4. Validates parameter types (scalar only; arrays/maps rejected)
//  5. Validates numeric values (integral values must satisfy |n| ≤ 2^53−1)
func ValidateParams(sql string, params map[string]any) ([]string, error) {
	// Scan for all placeholder spans
	spans := placeholderSpans(sql)

	// Build ordered distinct list of referenced placeholder names, enforcing the
	// grammar's length limit. An over-long run of identifier characters after $
	// is a malformed placeholder, never a 64-character prefix plus stray text.
	seen := make(map[string]bool)
	var orderedNames []string
	for _, s := range spans {
		if len(s.name) > maxNameLen {
			return nil, &ParamError{Msg: fmt.Sprintf(
				"placeholder name %q… (%d characters) exceeds the %d-character limit",
				s.name[:maxNameLen], len(s.name), maxNameLen)}
		}
		if !seen[s.name] {
			orderedNames = append(orderedNames, s.name)
			seen[s.name] = true
		}
	}

	// Check both-ways reference:
	// 1. Every referenced placeholder must be in params
	for _, name := range orderedNames {
		if _, ok := params[name]; !ok {
			return nil, &ParamError{Msg: fmt.Sprintf("missing required parameter %q", name)}
		}
	}

	// 2. Every param key must be referenced
	for key := range params {
		if !seen[key] {
			return nil, &ParamError{Msg: fmt.Sprintf("unreferenced parameter %q", key)}
		}
	}

	// Validate parameter types
	for _, name := range orderedNames {
		if err := validateParamValue(params[name]); err != nil {
			return nil, err
		}
	}

	return orderedNames, nil
}

// placeholderSpans scans SQL and returns all placeholder spans (location and
// name) that appear in executable SQL text. A placeholder is a `$` followed by
// a run of identifier characters starting with a letter or underscore.
//
// The scan skips the spans in which a `$` is not a placeholder:
//   - single- and double-quoted strings, with doubled-quote (” and "") escapes
//   - dollar-quoted string literals, `$$…$$` and `$tag$…$tag$`
//   - line comments (`-- …` to end of line)
//   - block comments (`/* … */`, which DuckDB nests)
//
// The returned name is the full identifier run: the §6.1 length limit is
// enforced by ValidateParams, so an over-long name surfaces as an error rather
// than being silently truncated to a valid-looking prefix.
func placeholderSpans(sql string) []span {
	var result []span
	i := 0
	for i < len(sql) {
		switch c := sql[i]; {
		case c == '\'' || c == '"':
			i = skipQuoted(sql, i)
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			i = skipLineComment(sql, i)
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			i = skipBlockComment(sql, i)
		case c == '$':
			next, s := scanDollar(sql, i)
			if s != nil {
				result = append(result, *s)
			}
			i = next
		default:
			i++
		}
	}

	return result
}

// scanDollar interprets the `$` at sql[i]. It returns the offset to resume
// scanning from and, when the `$` starts a placeholder, that placeholder's span.
//
// `$tag$` and `$$` open a dollar-quoted string literal, which is skipped whole.
// `$tag` followed by anything else is a placeholder. The two syntaxes share the
// `$` sigil, so the trailing character decides between them; a tag that does not
// start with a letter or underscore (e.g. `$1$`) is neither.
func scanDollar(sql string, i int) (int, *span) {
	j := i + 1
	for j < len(sql) && isNameChar(sql[j]) {
		j++
	}
	tag := sql[i+1 : j]
	validTag := tag == "" || isNameStart(tag[0])

	if validTag && j < len(sql) && sql[j] == '$' {
		return skipDollarQuoted(sql, i, j+1), nil
	}
	if tag != "" && isNameStart(tag[0]) {
		return j, &span{offset: i + 1, name: tag}
	}
	return i + 1, nil
}

// skipQuoted skips the quoted string opening at sql[i] and returns the offset
// just past its closing quote (or the end of input if it is unterminated).
// A doubled quote inside the string is an escape, not a terminator.
func skipQuoted(sql string, i int) int {
	q := sql[i]
	for i++; i < len(sql); i++ {
		if sql[i] != q {
			continue
		}
		if i+1 < len(sql) && sql[i+1] == q {
			i++ // doubled quote: escaped, keep scanning
			continue
		}
		return i + 1
	}
	return len(sql)
}

// skipDollarQuoted skips a dollar-quoted string whose opening delimiter is
// sql[start:bodyStart] (e.g. `$$` or `$tag$`) and whose body begins at
// bodyStart. It returns the offset just past the closing delimiter, or the end
// of input if the literal is unterminated.
func skipDollarQuoted(sql string, start, bodyStart int) int {
	delim := sql[start:bodyStart]
	if k := strings.Index(sql[bodyStart:], delim); k >= 0 {
		return bodyStart + k + len(delim)
	}
	return len(sql)
}

// skipLineComment skips a `--` comment and returns the offset of the newline
// that ends it (left unconsumed) or the end of input.
func skipLineComment(sql string, i int) int {
	for i += 2; i < len(sql); i++ {
		if sql[i] == '\n' {
			return i
		}
	}
	return len(sql)
}

// skipBlockComment skips a `/* … */` comment and returns the offset just past
// its close, or the end of input if unterminated. DuckDB nests block comments
// (verified against DuckDB v1.4.1), so nesting depth is tracked.
func skipBlockComment(sql string, i int) int {
	depth := 1
	for i += 2; i+1 < len(sql); {
		switch {
		case sql[i] == '/' && sql[i+1] == '*':
			depth++
			i += 2
		case sql[i] == '*' && sql[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(sql)
}

// validateParamValue checks a parameter value for type validity.
// Allowed types: string, bool, number (per the §6.1 bind-type table), null.
// Rejected: maps, slices, and any other non-scalar.
func validateParamValue(v any) *ParamError {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case string:
		return nil
	case bool:
		return nil
	case int:
		return validateNumber(new(big.Rat).SetInt64(int64(val)), strconv.FormatInt(int64(val), 10))
	case int32:
		return validateNumber(new(big.Rat).SetInt64(int64(val)), strconv.FormatInt(int64(val), 10))
	case int64:
		return validateNumber(new(big.Rat).SetInt64(val), strconv.FormatInt(val, 10))
	case float64:
		return validateFloat(val)
	case json.Number:
		// json.Number is used when the decoder has UseNumber() set — the worker's
		// production path.
		return validateJSONNumber(val)
	default:
		// Anything else (map, slice, etc.) is invalid
		return &ParamError{Msg: fmt.Sprintf("invalid parameter type: %T", val)}
	}
}

// validateNumber applies the single §6.1 precision rule to an exact numeric
// value, whatever Go type or lexical form it arrived in: an *integral* value
// must satisfy |n| ≤ 2^53−1 (bound as BIGINT); a non-integral value binds as
// DOUBLE and is unbounded. display is the value as it should appear in the
// error message.
func validateNumber(r *big.Rat, display string) *ParamError {
	if !r.IsInt() {
		return nil
	}
	if new(big.Rat).Abs(r).Cmp(maxSafeRat) > 0 {
		return &ParamError{Msg: fmt.Sprintf(
			"numeric value %s exceeds MAX_SAFE_INTEGER (%d); pass it as a string with an explicit CAST",
			display, maxSafeInteger)}
	}
	return nil
}

// validateFloat classifies a float64 by its value. Note that a float64 carries
// no memory of the literal it came from: a value that is integral *after* the
// inevitable rounding (e.g. 2^53−0.5, which is not representable and rounds to
// 2^53) is integral for binding purposes and is bounded accordingly.
func validateFloat(f float64) *ParamError {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return &ParamError{Msg: fmt.Sprintf(
			"invalid parameter value: %s is not a finite number", strconv.FormatFloat(f, 'g', -1, 64))}
	}
	return validateNumber(new(big.Rat).SetFloat64(f), strconv.FormatFloat(f, 'g', -1, 64))
}

// validateJSONNumber validates a json.Number. The literal is parsed exactly
// (big.Rat, not float64) so that integralness is decided by the mathematical
// value and not by the lexical form: "9007199254740992.0" and "1e20" are
// integral and therefore rejected, while "1.5e3" (= 1500) is integral and
// accepted, and "12345678901234567890.5" is non-integral and binds as DOUBLE.
func validateJSONNumber(jn json.Number) *ParamError {
	s := string(jn)
	if !isJSONNumberLiteral(s) {
		return &ParamError{Msg: fmt.Sprintf("invalid parameter type: json.Number(%q) is not numeric", s)}
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return &ParamError{Msg: fmt.Sprintf("invalid parameter type: json.Number(%q) is not numeric", s)}
	}
	return validateNumber(r, s)
}

// isJSONNumberLiteral reports whether s is a JSON number per RFC 8259:
// -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)? — a gate in front of big.Rat,
// whose own syntax is wider (fractions, hex floats, digit separators).
func isJSONNumberLiteral(s string) bool {
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
	}
	// Integer part: a single 0, or a non-zero digit followed by digits.
	start := i
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	if i == start || (i-start > 1 && s[start] == '0') {
		return false
	}
	// Optional fraction.
	if i < len(s) && s[i] == '.' {
		i++
		start = i
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		if i == start {
			return false
		}
	}
	// Optional exponent.
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		start = i
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(s)
}

// isDigit returns true if c is an ASCII digit.
func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// isNameStart returns true if c may start a placeholder name or dollar-quote tag.
func isNameStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

// isNameChar returns true if c may continue a placeholder name or dollar-quote tag.
func isNameChar(c byte) bool {
	return isNameStart(c) || isDigit(c)
}
