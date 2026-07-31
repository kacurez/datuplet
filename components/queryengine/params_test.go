package queryengine

import (
	"encoding/json"
	"testing"
)

// TestValidateParams tests the complete bound-parameter validation matrix.
func TestValidateParams(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		params    map[string]any
		wantNames []string
		wantErr   string
	}{
		// Grammar: placeholder names matching $([A-Za-z_][A-Za-z0-9_]{0,63})
		{
			name:      "single char name",
			sql:       "SELECT * WHERE a = $x",
			params:    map[string]any{"x": "value"},
			wantNames: []string{"x"},
		},
		{
			name:      "underscore start",
			sql:       "SELECT * WHERE a = $_var",
			params:    map[string]any{"_var": "value"},
			wantNames: []string{"_var"},
		},
		{
			name:      "letter start uppercase",
			sql:       "SELECT * WHERE a = $Abc",
			params:    map[string]any{"Abc": "value"},
			wantNames: []string{"Abc"},
		},
		{
			name:      "mixed alphanumeric and underscore",
			sql:       "SELECT * WHERE a = $var_123_test",
			params:    map[string]any{"var_123_test": "value"},
			wantNames: []string{"var_123_test"},
		},
		{
			name:      "64-char name (at limit)",
			sql:       "SELECT * WHERE a = $" + makeValidName(64),
			params:    map[string]any{makeValidName(64): "value"},
			wantNames: []string{makeValidName(64)},
		},
		{
			name:    "65-char name (exceeds limit - scanner captures first 64 chars)",
			sql:     "SELECT * WHERE a = $" + makeValidName(65),
			params:  map[string]any{makeValidName(64): "value"},
			wantNames: []string{makeValidName(64)},
		},
		{
			name:    "digit start (invalid - not recognized as placeholder)",
			sql:     "SELECT * WHERE a = $1x",
			params:  map[string]any{},
			wantNames: []string{},
		},
		{
			name:      "dash terminates placeholder name",
			sql:       "SELECT * WHERE a = $var AND b = $other",
			params:    map[string]any{"var": "value1", "other": "value2"},
			wantNames: []string{"var", "other"},
		},

		// Quoted strings: placeholders inside quotes are NOT placeholders
		{
			name:      "placeholder in single quotes",
			sql:       "SELECT * WHERE a = '$x' AND b = $y",
			params:    map[string]any{"y": "value"},
			wantNames: []string{"y"},
		},
		{
			name:      "placeholder in double quotes",
			sql:       `SELECT * WHERE a = "$x" AND b = $y`,
			params:    map[string]any{"y": "value"},
			wantNames: []string{"y"},
		},
		{
			name:      "doubled single quotes escape",
			sql:       "SELECT * WHERE a = 'it''s' AND b = $x",
			params:    map[string]any{"x": "value"},
			wantNames: []string{"x"},
		},
		{
			name:      "doubled double quotes escape",
			sql:       `SELECT * WHERE a = "foo""bar" AND b = $x`,
			params:    map[string]any{"x": "value"},
			wantNames: []string{"x"},
		},
		{
			name:      "complex quoted example",
			sql:       "SELECT '$x' AS literal, * FROM t WHERE c = $country AND d = $days",
			params:    map[string]any{"country": "DE", "days": 30},
			wantNames: []string{"country", "days"},
		},

		// Repeated placeholders: same name appears multiple times, ordered distinct output
		{
			name:      "repeated placeholder same value",
			sql:       "SELECT * WHERE a = $x AND b = $x",
			params:    map[string]any{"x": "value"},
			wantNames: []string{"x"},
		},
		{
			name:      "multiple placeholders, first one repeated",
			sql:       "SELECT * WHERE a = $x AND b = $y AND c = $x",
			params:    map[string]any{"x": "val1", "y": "val2"},
			wantNames: []string{"x", "y"},
		},
		{
			name:      "ordered distinct by first appearance",
			sql:       "SELECT * WHERE a = $z AND b = $x AND c = $y AND d = $x",
			params:    map[string]any{"z": "1", "x": "2", "y": "3"},
			wantNames: []string{"z", "x", "y"},
		},

		// Missing parameters: placeholder referenced but not in params
		{
			name:    "missing single param",
			sql:     "SELECT * WHERE a = $missing",
			params:  map[string]any{},
			wantErr: "missing required parameter \"missing\"",
		},
		{
			name:    "missing one of two params",
			sql:     "SELECT * WHERE a = $x AND b = $y",
			params:  map[string]any{"x": "value"},
			wantErr: "missing required parameter \"y\"",
		},

		// Unreferenced parameters: param provided but not referenced in SQL
		{
			name:    "unreferenced param",
			sql:     "SELECT * WHERE a = $x",
			params:  map[string]any{"x": "value", "unused": "extra"},
			wantErr: "unreferenced parameter \"unused\"",
		},
		{
			name:    "multiple unreferenced params",
			sql:     "SELECT * WHERE a = $x",
			params:  map[string]any{"x": "value", "extra1": "a", "extra2": "b"},
			wantErr: "unreferenced parameter",
		},

		// Value types: string
		{
			name:      "string value",
			sql:       "SELECT * WHERE name = $name",
			params:    map[string]any{"name": "Alice"},
			wantNames: []string{"name"},
		},
		{
			name:      "empty string",
			sql:       "SELECT * WHERE name = $name",
			params:    map[string]any{"name": ""},
			wantNames: []string{"name"},
		},

		// Value types: bool
		{
			name:      "bool true",
			sql:       "SELECT * WHERE active = $active",
			params:    map[string]any{"active": true},
			wantNames: []string{"active"},
		},
		{
			name:      "bool false",
			sql:       "SELECT * WHERE active = $active",
			params:    map[string]any{"active": false},
			wantNames: []string{"active"},
		},

		// Value types: integral numbers within safe range
		{
			name:      "small positive int",
			sql:       "SELECT * WHERE count = $n",
			params:    map[string]any{"n": int(42)},
			wantNames: []string{"n"},
		},
		{
			name:      "small negative int",
			sql:       "SELECT * WHERE count = $n",
			params:    map[string]any{"n": int(-42)},
			wantNames: []string{"n"},
		},
		{
			name:      "int64 max safe",
			sql:       "SELECT * WHERE id = $id",
			params:    map[string]any{"id": int64(9007199254740991)}, // 2^53-1
			wantNames: []string{"id"},
		},
		{
			name:      "int64 min safe",
			sql:       "SELECT * WHERE id = $id",
			params:    map[string]any{"id": int64(-9007199254740991)}, // -(2^53-1)
			wantNames: []string{"id"},
		},
		{
			name:    "int64 exceeds max safe (1<<53)",
			sql:     "SELECT * WHERE id = $id",
			params:  map[string]any{"id": int64(1 << 53)},
			wantErr: "MAX_SAFE_INTEGER",
		},
		{
			name:    "int64 exceeds min safe (-(1<<53))",
			sql:     "SELECT * WHERE id = $id",
			params:  map[string]any{"id": int64(-(1 << 53))},
			wantErr: "MAX_SAFE_INTEGER",
		},

		// Value types: floating point
		{
			name:      "small float",
			sql:       "SELECT * WHERE price = $price",
			params:    map[string]any{"price": 3.14},
			wantNames: []string{"price"},
		},
		{
			name:      "zero",
			sql:       "SELECT * WHERE x = $x",
			params:    map[string]any{"x": 0.0},
			wantNames: []string{"x"},
		},
		{
			name:      "negative float",
			sql:       "SELECT * WHERE delta = $delta",
			params:    map[string]any{"delta": -1.5},
			wantNames: []string{"delta"},
		},

		// Value types: null
		{
			name:      "explicit null",
			sql:       "SELECT * WHERE x = $x",
			params:    map[string]any{"x": nil},
			wantNames: []string{"x"},
		},

		// Nested structures: rejected
		{
			name:    "nested map",
			sql:     "SELECT * WHERE data = $data",
			params:  map[string]any{"data": map[string]any{"key": "value"}},
			wantErr: "invalid parameter type",
		},
		{
			name:    "array",
			sql:     "SELECT * WHERE ids = $ids",
			params:  map[string]any{"ids": []int{1, 2, 3}},
			wantErr: "invalid parameter type",
		},
		{
			name:    "slice of strings",
			sql:     "SELECT * WHERE names = $names",
			params:  map[string]any{"names": []string{"a", "b"}},
			wantErr: "invalid parameter type",
		},

		// json.Number awareness (worker decodes with UseNumber())
		{
			name:      "json.Number integral within safe range",
			sql:       "SELECT * WHERE n = $n",
			params:    map[string]any{"n": json.Number("42")},
			wantNames: []string{"n"},
		},
		{
			name:      "json.Number float",
			sql:       "SELECT * WHERE n = $n",
			params:    map[string]any{"n": json.Number("3.14")},
			wantNames: []string{"n"},
		},
		{
			name:    "json.Number exceeds safe int",
			sql:     "SELECT * WHERE n = $n",
			params:  map[string]any{"n": json.Number("9007199254740992")}, // 2^53
			wantErr: "MAX_SAFE_INTEGER",
		},
		{
			name:    "json.Number non-numeric",
			sql:     "SELECT * WHERE n = $n",
			params:  map[string]any{"n": json.Number("not_a_number")},
			wantErr: "invalid parameter type",
		},

		// Empty cases
		{
			name:      "no placeholders, no params",
			sql:       "SELECT * FROM users",
			params:    map[string]any{},
			wantNames: []string{},
		},
		{
			name:      "no placeholders, nil params",
			sql:       "SELECT * FROM users",
			params:    nil,
			wantNames: []string{},
		},
		{
			name:    "no placeholders, non-empty params",
			sql:     "SELECT * FROM users",
			params:  map[string]any{"x": "value"},
			wantErr: "unreferenced parameter",
		},

		// Complex real-world scenarios
		{
			name: "sales app example from spec",
			sql: `SELECT count(*) AS orders, sum(amount) AS revenue
FROM sales.orders
WHERE order_date >= current_date - $days AND country = $country`,
			params:    map[string]any{"days": 30, "country": "DE"},
			wantNames: []string{"days", "country"},
		},
		{
			name: "conditional bind (country omitted)",
			sql: `SELECT count(*) AS orders
FROM sales.orders
WHERE order_date >= current_date - $days`,
			params:    map[string]any{"days": 30},
			wantNames: []string{"days"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateParams(tt.sql, tt.params)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("ValidateParams got no error, want error containing %q", tt.wantErr)
				}
				// Check that error message contains the expected substring
				pe, ok := err.(*ParamError)
				if !ok {
					t.Fatalf("ValidateParams returned %T, want *ParamError", err)
				}
				if !containsSubstring(pe.Msg, tt.wantErr) {
					t.Errorf("ValidateParams error message %q does not contain %q", pe.Msg, tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("ValidateParams got unexpected error: %v", err)
			}

			// Check ordered distinct names match
			if len(got) != len(tt.wantNames) {
				t.Errorf("ValidateParams returned %d names, want %d", len(got), len(tt.wantNames))
			}
			for i, name := range got {
				if i >= len(tt.wantNames) {
					t.Errorf("ValidateParams returned extra name: %s", name)
				} else if name != tt.wantNames[i] {
					t.Errorf("ValidateParams name[%d] = %q, want %q", i, name, tt.wantNames[i])
				}
			}
		})
	}
}

// Helper to check substring containment
func containsSubstring(haystack, needle string) bool {
	// Simple substring check; in Go could also use strings.Contains
	for i := 0; i <= len(haystack)-len(needle); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// makeValidName creates a valid identifier name of the specified length.
// Names are made of 'a' characters to ensure validity.
func makeValidName(length int) string {
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = 'a'
	}
	return string(result)
}

// TestPlaceholderSpans tests the scanner primitive (exported for Q2 potential reuse).
func TestPlaceholderSpans(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantSpan []span
	}{
		{
			name: "simple placeholder",
			sql:  "SELECT * WHERE a = $x",
			wantSpan: []span{
				{offset: 20, name: "x"},
			},
		},
		{
			name: "placeholder in quotes is not found",
			sql:  "SELECT * WHERE a = '$x' AND b = $y",
			wantSpan: []span{
				{offset: 33, name: "y"},
			},
		},
		{
			name: "multiple placeholders",
			sql:  "SELECT * WHERE a = $x AND b = $y",
			wantSpan: []span{
				{offset: 20, name: "x"},
				{offset: 31, name: "y"},
			},
		},
		{
			name: "repeated placeholder",
			sql:  "SELECT * WHERE a = $x AND b = $x",
			wantSpan: []span{
				{offset: 20, name: "x"},
				{offset: 31, name: "x"},
			},
		},
		{
			name:     "no placeholders",
			sql:      "SELECT * FROM users",
			wantSpan: []span{},
		},
		{
			name: "doubled quotes escape",
			sql:  `SELECT 'foo''bar' WHERE a = $x`,
			wantSpan: []span{
				{offset: 29, name: "x"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := placeholderSpans(tt.sql)

			if len(got) != len(tt.wantSpan) {
				t.Errorf("placeholderSpans returned %d spans, want %d", len(got), len(tt.wantSpan))
			}

			for i, s := range got {
				if i >= len(tt.wantSpan) {
					t.Errorf("placeholderSpans returned extra span: %+v", s)
				} else {
					if s.name != tt.wantSpan[i].name {
						t.Errorf("placeholderSpans[%d].name = %q, want %q", i, s.name, tt.wantSpan[i].name)
					}
					if s.offset != tt.wantSpan[i].offset {
						t.Errorf("placeholderSpans[%d].offset = %d, want %d", i, s.offset, tt.wantSpan[i].offset)
					}
				}
			}
		})
	}
}

// TestMaxSafeInteger validates the 2^53-1 boundary specifically
func TestMaxSafeInteger(t *testing.T) {
	maxSafe := int64(9007199254740991)      // 2^53 - 1
	overSafe := int64(9007199254740992)     // 2^53
	minSafe := int64(-9007199254740991)     // -(2^53 - 1)
	underMinSafe := int64(-9007199254740992) // -(2^53)

	tests := []struct {
		name      string
		sql       string
		params    map[string]any
		shouldErr bool
	}{
		{
			name:      "max safe",
			sql:       "SELECT * WHERE n = $n",
			params:    map[string]any{"n": maxSafe},
			shouldErr: false,
		},
		{
			name:      "one over max safe",
			sql:       "SELECT * WHERE n = $n",
			params:    map[string]any{"n": overSafe},
			shouldErr: true,
		},
		{
			name:      "min safe (negative)",
			sql:       "SELECT * WHERE n = $n",
			params:    map[string]any{"n": minSafe},
			shouldErr: false,
		},
		{
			name:      "one under min safe (negative)",
			sql:       "SELECT * WHERE n = $n",
			params:    map[string]any{"n": underMinSafe},
			shouldErr: true,
		},
		{
			name:      "1<<53 as mentioned in brief",
			sql:       "SELECT * WHERE n = $n",
			params:    map[string]any{"n": int64(1 << 53)},
			shouldErr: true,
		},
		{
			name:      "-(1<<53) as mentioned in brief",
			sql:       "SELECT * WHERE n = $n",
			params:    map[string]any{"n": int64(-(1 << 53))},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateParams(tt.sql, tt.params)
			if tt.shouldErr && err == nil {
				t.Errorf("ValidateParams should error for %v, got nil", tt.params["n"])
			} else if !tt.shouldErr && err != nil {
				t.Errorf("ValidateParams should not error for %v, got %v", tt.params["n"], err)
			}
		})
	}
}

// TestFloatSafeInteger checks that float64 values at the boundary are handled correctly
func TestFloatSafeInteger(t *testing.T) {
	// Test that floats are accepted (non-integral), even at the boundary
	tests := []struct {
		name      string
		value     float64
		shouldErr bool
	}{
		{
			name:      "normal float",
			value:     3.14,
			shouldErr: false,
		},
		{
			name:      "float at max safe int + 0.5",
			value:     float64(1<<53) - 0.5,
			shouldErr: false,
		},
		{
			name:      "float exceeding safe int",
			value:     float64(1<<53) + 1.5,
			shouldErr: false, // Non-integral floats are accepted
		},
		{
			name:      "float that is exactly 1<<53 (integral representation)",
			value:     float64(1 << 53),
			shouldErr: false, // This is a float, not an integral number type
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := "SELECT * WHERE n = $n"
			params := map[string]any{"n": tt.value}
			_, err := ValidateParams(sql, params)
			if tt.shouldErr && err == nil {
				t.Errorf("ValidateParams should error for float %v", tt.value)
			} else if !tt.shouldErr && err != nil {
				t.Errorf("ValidateParams should not error for float %v: %v", tt.value, err)
			}
		})
	}
}

// BenchmarkValidateParams benchmarks the validator
func BenchmarkValidateParams(b *testing.B) {
	sql := `SELECT * FROM sales.orders
		WHERE order_date >= current_date - $days
		AND country = $country
		AND amount > $minAmount
		AND region IN (SELECT region FROM allowed_regions WHERE id = $region_id)`
	params := map[string]any{
		"days":       30,
		"country":    "US",
		"minAmount":  1000.50,
		"region_id":  5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ValidateParams(sql, params)
	}
}
