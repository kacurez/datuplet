package schema

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// InferenceConfig configures the type inference behavior.
type InferenceConfig struct {
	// MinSampleSize is the minimum number of non-null values needed for inference.
	// Default: 1
	MinSampleSize int

	// NullStrings are values treated as null.
	// Default: ["", "null", "NULL", "NA", "N/A", "\\N", "nil", "NIL"]
	NullStrings []string

	// BoolTrueStrings are values treated as boolean true.
	// Default: ["true", "TRUE", "True", "1", "yes", "YES", "Yes", "y", "Y"]
	BoolTrueStrings []string

	// BoolFalseStrings are values treated as boolean false.
	// Default: ["false", "FALSE", "False", "0", "no", "NO", "No", "n", "N"]
	BoolFalseStrings []string

	// DateFormats are date formats to try.
	// Default: ["2006-01-02", "01/02/2006", "02/01/2006", "2006/01/02"]
	DateFormats []string

	// TimestampFormats are timestamp formats to try.
	// Default: RFC3339, common ISO formats
	TimestampFormats []string
}

// DefaultInferenceConfig returns the default inference configuration.
func DefaultInferenceConfig() *InferenceConfig {
	return &InferenceConfig{
		MinSampleSize: 1,
		NullStrings:   []string{"", "null", "NULL", "NA", "N/A", "\\N", "nil", "NIL", "none", "None", "NONE"},
		BoolTrueStrings: []string{
			"true", "TRUE", "True", "1", "yes", "YES", "Yes", "y", "Y",
		},
		BoolFalseStrings: []string{
			"false", "FALSE", "False", "0", "no", "NO", "No", "n", "N",
		},
		DateFormats: []string{
			"2006-01-02",
			"01/02/2006",
			"02/01/2006",
			"2006/01/02",
			"2006-1-2",
			"1/2/2006",
		},
		TimestampFormats: []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05.000",
			"2006-01-02 15:04:05.000",
			"2006-01-02T15:04:05.000000",
			"2006-01-02 15:04:05.000000",
			"01/02/2006 15:04:05",
			"02/01/2006 15:04:05",
			"2006/01/02 15:04:05",
		},
	}
}

// TypeInferrer infers data types from string values.
type TypeInferrer struct {
	config       *InferenceConfig
	nullSet      map[string]bool
	boolTrueSet  map[string]bool
	boolFalseSet map[string]bool
	intRegex     *regexp.Regexp
	floatRegex   *regexp.Regexp
}

// NewTypeInferrer creates a new TypeInferrer with the given configuration.
func NewTypeInferrer(config *InferenceConfig) *TypeInferrer {
	if config == nil {
		config = DefaultInferenceConfig()
	}

	nullSet := make(map[string]bool)
	for _, s := range config.NullStrings {
		nullSet[s] = true
	}

	boolTrueSet := make(map[string]bool)
	for _, s := range config.BoolTrueStrings {
		boolTrueSet[s] = true
	}

	boolFalseSet := make(map[string]bool)
	for _, s := range config.BoolFalseStrings {
		boolFalseSet[s] = true
	}

	return &TypeInferrer{
		config:       config,
		nullSet:      nullSet,
		boolTrueSet:  boolTrueSet,
		boolFalseSet: boolFalseSet,
		intRegex:     regexp.MustCompile(`^-?\d+$`),
		floatRegex:   regexp.MustCompile(`^-?\d*\.?\d+([eE][+-]?\d+)?$`),
	}
}

// IsNull checks if a string value represents null.
func (ti *TypeInferrer) IsNull(value string) bool {
	return ti.nullSet[value]
}

// InferValueType infers the most specific type for a single string value.
// Returns TypeUnknown for null values (they don't constrain type).
// Returns TypeString if no more specific type can be determined.
func (ti *TypeInferrer) InferValueType(value string) DataType {
	// Trim whitespace
	value = strings.TrimSpace(value)

	// Check for null
	if ti.IsNull(value) {
		return TypeUnknown // Null values don't constrain type
	}

	// Check for boolean (but not "1"/"0" which could be integers)
	// We exclude "1" and "0" from initial bool check to prefer int detection
	if ti.boolTrueSet[value] || ti.boolFalseSet[value] {
		// "1" and "0" should be detected as integers, not booleans
		if value == "1" || value == "0" {
			return TypeInt64
		}
		return TypeBool
	}

	// Check for integer (must come before float check)
	if ti.intRegex.MatchString(value) {
		// Check if it fits in int64
		if _, err := strconv.ParseInt(value, 10, 64); err == nil {
			return TypeInt64
		}
		// Too large for int64, treat as string
		return TypeString
	}

	// Check for float
	if ti.floatRegex.MatchString(value) {
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			return TypeFloat64
		}
	}

	// Check for timestamp (before date, as timestamp is more specific)
	for _, format := range ti.config.TimestampFormats {
		if _, err := time.Parse(format, value); err == nil {
			return TypeTimestamp
		}
	}

	// Check for date
	for _, format := range ti.config.DateFormats {
		if _, err := time.Parse(format, value); err == nil {
			return TypeDate
		}
	}

	// Default to string
	return TypeString
}

// columnInferenceState tracks inference state for a single column.
type columnInferenceState struct {
	name         string
	hasNull      bool
	typeCounts   map[DataType]int
	totalNonNull int
}

// InferSchema infers a schema from CSV headers and sample rows.
// The sampleRows should NOT include the header row.
func InferSchema(headers []string, sampleRows [][]string) (*Schema, error) {
	return InferSchemaWithConfig(headers, sampleRows, nil)
}

// InferSchemaWithConfig infers a schema with custom configuration.
func InferSchemaWithConfig(headers []string, sampleRows [][]string, config *InferenceConfig) (*Schema, error) {
	if len(headers) == 0 {
		return nil, fmt.Errorf("headers cannot be empty")
	}

	ti := NewTypeInferrer(config)

	// Initialize column state
	states := make([]*columnInferenceState, len(headers))
	for i, h := range headers {
		states[i] = &columnInferenceState{
			name:       h,
			typeCounts: make(map[DataType]int),
		}
	}

	// Process each row
	for _, row := range sampleRows {
		for i := 0; i < len(headers) && i < len(row); i++ {
			value := row[i]
			state := states[i]

			if ti.IsNull(value) {
				state.hasNull = true
				continue
			}

			inferredType := ti.InferValueType(value)
			if inferredType != TypeUnknown {
				state.typeCounts[inferredType]++
				state.totalNonNull++
			}
		}
	}

	// Determine final type for each column
	columns := make([]ColumnDef, len(headers))
	for i, state := range states {
		columns[i] = ColumnDef{
			Name:     state.name,
			Type:     resolveColumnType(state),
			Nullable: state.hasNull || state.totalNonNull == 0,
		}
	}

	return NewSchema(columns)
}

// resolveColumnType determines the final type for a column based on inference state.
// Uses type promotion rules:
//   - int64 + float64 → float64
//   - bool + numeric → string (ambiguous)
//   - timestamp/date + other → string
//   - any + string → string
func resolveColumnType(state *columnInferenceState) DataType {
	if state.totalNonNull == 0 {
		return TypeString // No data, default to string
	}

	counts := state.typeCounts

	// If all values are the same type, use that type
	if len(counts) == 1 {
		for t := range counts {
			return t
		}
	}

	// Check for timestamp/date (if ALL non-null values are timestamp/date, use it)
	if counts[TypeTimestamp] > 0 && counts[TypeTimestamp] == state.totalNonNull {
		return TypeTimestamp
	}
	if counts[TypeDate] > 0 && counts[TypeDate] == state.totalNonNull {
		return TypeDate
	}

	// Type promotion rules:

	// If any string values exist, result is string
	if counts[TypeString] > 0 {
		return TypeString
	}

	// If timestamp/date mixed with other types, use string
	if counts[TypeTimestamp] > 0 || counts[TypeDate] > 0 {
		return TypeString
	}

	// If we have bool mixed with numeric types, use string (ambiguous)
	if counts[TypeBool] > 0 && (counts[TypeInt64] > 0 || counts[TypeFloat64] > 0) {
		return TypeString
	}

	// If we have int64 and float64, promote to float64
	if counts[TypeInt64] > 0 && counts[TypeFloat64] > 0 {
		return TypeFloat64
	}

	// If we have int32 and int64, use int64
	if counts[TypeInt32] > 0 && counts[TypeInt64] > 0 {
		return TypeInt64
	}

	// If we have float32 and float64, use float64
	if counts[TypeFloat32] > 0 && counts[TypeFloat64] > 0 {
		return TypeFloat64
	}

	// Return the most common type
	maxCount := 0
	result := TypeString
	for t, c := range counts {
		if c > maxCount {
			maxCount = c
			result = t
		}
	}

	return result
}

// InferSchemaFromJSON infers a schema from JSON objects.
// Takes a slice of maps representing JSON objects.
func InferSchemaFromJSON(objects []map[string]interface{}) (*Schema, error) {
	return InferSchemaFromJSONWithConfig(objects, nil)
}

// jsonValueToString renders a decoded JSON value as the string the inferrer sees.
// Nested objects/arrays round-trip through json.Marshal so inference observes valid
// JSON text (e.g. `{"a":1}`) instead of Go's `map[a:1]` fmt output — matching what
// appendStringFromInterface will actually store in the string column.
func jsonValueToString(val any) string {
	switch v := val.(type) {
	case map[string]any, []any, []map[string]any:
		data, err := json.Marshal(v)
		if err == nil {
			return string(data)
		}
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// InferSchemaFromJSONWithConfig infers a schema from JSON objects with custom configuration.
func InferSchemaFromJSONWithConfig(objects []map[string]interface{}, config *InferenceConfig) (*Schema, error) {
	if len(objects) == 0 {
		return nil, fmt.Errorf("no data to infer schema from")
	}

	// Collect all field names from all objects
	fieldNames := make(map[string]bool)
	for _, obj := range objects {
		for key := range obj {
			fieldNames[key] = true
		}
	}

	// Sort field names for deterministic order
	headers := make([]string, 0, len(fieldNames))
	for name := range fieldNames {
		headers = append(headers, name)
	}
	sort.Strings(headers)

	// Convert to string rows for inference
	sampleRows := make([][]string, len(objects))
	for i, obj := range objects {
		row := make([]string, len(headers))
		for j, name := range headers {
			if val, ok := obj[name]; ok {
				if val == nil {
					row[j] = "" // nil = null
				} else {
					row[j] = jsonValueToString(val)
				}
			} else {
				row[j] = "" // Missing field = null
			}
		}
		sampleRows[i] = row
	}

	return InferSchemaWithConfig(headers, sampleRows, config)
}
