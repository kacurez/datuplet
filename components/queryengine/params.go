package queryengine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"unicode"
)

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
// 1. Scans the SQL for placeholder names matching $([A-Za-z_][A-Za-z0-9_]{0,63})
// 2. Ignores placeholders inside single-quoted or double-quoted strings (with doubled-quote escapes)
// 3. Validates that every placeholder has a corresponding parameter and vice versa
// 4. Validates parameter types (scalar only; arrays/maps rejected)
// 5. Validates numeric boundaries (integral: |n| ≤ 2^53−1)
func ValidateParams(sql string, params map[string]any) ([]string, error) {
	// Scan for all placeholder spans
	spans := placeholderSpans(sql)

	// Build ordered distinct list of referenced placeholder names
	seen := make(map[string]bool)
	var orderedNames []string
	for _, s := range spans {
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

// placeholderSpans scans SQL and returns all placeholder spans (location and name)
// outside of quoted strings. A placeholder must match $([A-Za-z_][A-Za-z0-9_]{0,63}).
// Placeholders inside single or double quoted strings are ignored.
// Doubled quotes (' ' and "") are escape sequences and do not terminate strings.
func placeholderSpans(sql string) []span {
	var result []span
	i := 0
	for i < len(sql) {
		ch := sql[i]

		// Handle single-quoted strings
		if ch == '\'' {
			i++
			// Scan until end of string, handling '' escape
			for i < len(sql) {
				if sql[i] == '\'' {
					i++
					// Check for escaped quote ''
					if i < len(sql) && sql[i] == '\'' {
						i++ // Skip the second quote
						continue
					}
					// End of string
					break
				}
				i++
			}
			continue
		}

		// Handle double-quoted strings
		if ch == '"' {
			i++
			// Scan until end of string, handling "" escape
			for i < len(sql) {
				if sql[i] == '"' {
					i++
					// Check for escaped quote ""
					if i < len(sql) && sql[i] == '"' {
						i++ // Skip the second quote
						continue
					}
					// End of string
					break
				}
				i++
			}
			continue
		}

		// Look for $ that starts a placeholder
		if ch == '$' && i+1 < len(sql) {
			firstChar := sql[i+1]
			// First character must be letter or underscore
			if isLetter(firstChar) || firstChar == '_' {
				// Scan the name (up to 64 chars: 1 first + 63 more)
				nameStart := i + 1
				nameLen := 1
				j := i + 2
				for j < len(sql) && nameLen < 64 {
					c := sql[j]
					if isLetter(c) || unicode.IsDigit(rune(c)) || c == '_' {
						nameLen++
						j++
					} else {
						break
					}
				}

				// If nameLen is still 64 and we could continue, that's an error
				// But we only capture up to 64, which is the valid limit
				name := sql[nameStart : nameStart+nameLen]
				result = append(result, span{offset: nameStart, name: name})
				i = j
				continue
			}
		}

		i++
	}

	return result
}

// validateParamValue checks a parameter value for type validity.
// Allowed types: string, bool, integer (within safe range), float, null.
// Rejected: maps, slices, and integers outside safe range.
func validateParamValue(v any) *ParamError {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case string:
		return nil
	case bool:
		return nil
	case float64:
		// Float64 values are always accepted, regardless of magnitude or
		// whether they're mathematically integral. The safe-integer check
		// only applies to actual integer types in Go, not float types.
		// (When json.Number is used for JSON-decoded numerics, integral
		// floats in JSON are checked in validateJSONNumber.)
		return nil
	case int:
		return validateIntegral(int64(val))
	case int32:
		return validateIntegral(int64(val))
	case int64:
		return validateIntegral(val)
	case json.Number:
		// json.Number is used when decoder has UseNumber() set
		return validateJSONNumber(val)
	default:
		// Anything else (map, slice, etc.) is invalid
		return &ParamError{Msg: fmt.Sprintf("invalid parameter type: %T", val)}
	}
}

// validateIntegral checks if an int64 is within the safe-integer range [-(2^53-1), 2^53-1].
func validateIntegral(n int64) *ParamError {
	// Check against 2^53 - 1 = 9007199254740991
	const maxSafeInt = int64(9007199254740991)
	const minSafeInt = int64(-9007199254740991)

	if n > maxSafeInt || n < minSafeInt {
		return &ParamError{Msg: fmt.Sprintf("numeric value %d exceeds MAX_SAFE_INTEGER", n)}
	}
	return nil
}

// validateJSONNumber validates a json.Number from a decoded value.
func validateJSONNumber(jn json.Number) *ParamError {
	s := string(jn)

	// Try to parse as integer first (no decimal point or exponent)
	if intVal, err := strconv.ParseInt(s, 10, 64); err == nil {
		// Successfully parsed as int64, check bounds
		return validateIntegral(intVal)
	}

	// Try to parse as float (may have decimal point or exponent)
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		// Successfully parsed as float - accept it regardless of value
		// (floats in JSON are not subject to the safe-integer boundary,
		// per RFC 028 §6.1 bind-type table)
		return nil
	}

	// Neither integer nor float
	return &ParamError{Msg: fmt.Sprintf("invalid parameter type: json.Number(%q) is not numeric", s)}
}

// isLetter returns true if c is an ASCII letter (a-z or A-Z).
func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
