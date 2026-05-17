// Package main is the data-generator component: generates data inline from a
// pipeline-YAML config (random or literal mode) and writes to the data lake
// via the DataGateway SDK.
package main

import (
	"encoding/json"
	"fmt"
	"regexp"
)

// validIdentifier matches table/column names (ASCII letters, digits, underscores,
// hyphens, dots — must start with a letter or digit).
var validIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)

// validColumnName matches column names. Slightly tighter than table names
// — column names are referenced by SQL engines and shouldn't include dots
// or hyphens. Letters, digits, underscores; must start with a letter or
// underscore; max 128 chars.
var validColumnName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

// allowedTypes is the full set of supported column types for random mode.
var allowedTypes = map[string]bool{
	"string":    true,
	"int":       true,
	"long":      true,
	"float":     true,
	"double":    true,
	"boolean":   true,
	"date":      true,
	"timestamp": true,
	"now":       true,
	"uuid":      true,
}

// Config is the top-level component configuration decoded from
// `components[].config` in the pipeline YAML.
type Config struct {
	Tables []Table `json:"tables"`
}

// Table represents a single output table. Each table uses exactly one of
// `random` or `literal` — both-set or neither-set is a config error.
type Table struct {
	Name string `json:"name"`

	// RowInsertSpeed is honoured by both modes: ms to sleep between rows. 0 = no sleep.
	RowInsertSpeed int `json:"rowInsertSpeed,omitempty"`

	// Random is set for random-data mode (schema + limit).
	Random *RandomSpec `json:"random,omitempty"`

	// Literal is set for literal-data mode (explicit columns + rows).
	Literal *LiteralSpec `json:"literal,omitempty"`
}

// RandomSpec configures random-data generation: schema (column → type) plus
// limits and optional error injection.
type RandomSpec struct {
	// Schema maps column name → type. At least one column required.
	Schema map[string]string `json:"schema"`

	// Limit controls when generation stops. At least one limit field must be non-zero.
	Limit *Limit `json:"limit"`

	// UserErrorMessage, if set, fires a user-error exit (code 1) at a random
	// row in [0, limit.rowsCount), emitting `DUPLET_STATUS_MESSAGE: <msg>`.
	// Random-mode only.
	UserErrorMessage string `json:"userErrorMessage,omitempty"`
}

// LiteralSpec configures literal-data emission: explicit row values plus
// matching column names. Schema is inferred from the first non-null value
// per column.
type LiteralSpec struct {
	// Columns names the columns in `rows`. Required; arity must match the
	// row width. No synthesised default.
	Columns []string `json:"columns"`

	// Rows is the literal data. At least one row required; all rows share arity.
	Rows [][]any `json:"rows"`
}

// Limit controls when a random-mode table stops emitting rows.
// At least one field must be non-zero. OR semantics: the first limit to trip wins.
type Limit struct {
	RowsCount        int `json:"rowsCount,omitempty"`
	SizeInBytes      int `json:"sizeInBytes,omitempty"`
	TimeoutInSeconds int `json:"timeoutInSeconds,omitempty"`
}

// ParseAndValidate validates the config after JSON decoding. All table errors
// are collected and reported together.
func ParseAndValidate(cfg *Config) error {
	if len(cfg.Tables) == 0 {
		return fmt.Errorf("config: 'tables' must contain at least one entry")
	}

	var errs []string
	for i, t := range cfg.Tables {
		prefix := fmt.Sprintf("table[%d]", i)
		if t.Name != "" {
			prefix = fmt.Sprintf("table[%d] %q", i, t.Name)
		}

		if t.Name == "" {
			errs = append(errs, fmt.Sprintf("%s: 'name' is required", prefix))
			continue
		}
		if !validIdentifier.MatchString(t.Name) {
			errs = append(errs, fmt.Sprintf("%s: name %q is not a valid identifier (must match ^[A-Za-z][A-Za-z0-9_.-]{0,127}$)", prefix, t.Name))
		}

		hasRandom := t.Random != nil
		hasLiteral := t.Literal != nil

		switch {
		case hasRandom && hasLiteral:
			errs = append(errs, fmt.Sprintf("%s: both 'random' and 'literal' set; choose exactly one mode", prefix))
		case !hasRandom && !hasLiteral:
			errs = append(errs, fmt.Sprintf("%s: neither 'random' nor 'literal' set; choose exactly one mode", prefix))
		case hasRandom:
			if es := validateRandom(prefix, t.Random); len(es) > 0 {
				errs = append(errs, es...)
			}
		case hasLiteral:
			if es := validateLiteral(prefix, t.Literal); len(es) > 0 {
				errs = append(errs, es...)
			}
		}

		if t.RowInsertSpeed < 0 {
			errs = append(errs, fmt.Sprintf("%s: rowInsertSpeed must be >= 0, got %d", prefix, t.RowInsertSpeed))
		}
	}

	if len(errs) > 0 {
		msg := "config validation failed:"
		for _, e := range errs {
			msg += "\n  - " + e
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// validateRandom validates a random-mode table.
func validateRandom(prefix string, r *RandomSpec) []string {
	var errs []string

	if len(r.Schema) == 0 {
		errs = append(errs, fmt.Sprintf("%s: random mode requires 'schema' (non-empty map of column → type)", prefix))
	} else {
		for name, typ := range r.Schema {
			if !validColumnName.MatchString(name) {
				errs = append(errs, fmt.Sprintf("%s: schema column %q is not a valid column name (must match ^[A-Za-z_][A-Za-z0-9_]{0,127}$)", prefix, name))
			}
			if !allowedTypes[typ] {
				errs = append(errs, fmt.Sprintf("%s: schema column %q has unknown type %q; allowed: string, int, long, float, double, boolean, date, timestamp, now, uuid", prefix, name, typ))
			}
		}
	}

	if r.Limit == nil {
		errs = append(errs, fmt.Sprintf("%s: random mode requires 'limit' (at least one of rowsCount/sizeInBytes/timeoutInSeconds)", prefix))
	} else {
		l := r.Limit
		if l.RowsCount == 0 && l.SizeInBytes == 0 && l.TimeoutInSeconds == 0 {
			errs = append(errs, fmt.Sprintf("%s: 'limit' must have at least one of rowsCount/sizeInBytes/timeoutInSeconds non-zero", prefix))
		}
		if l.RowsCount < 0 {
			errs = append(errs, fmt.Sprintf("%s: limit.rowsCount must be >= 0, got %d", prefix, l.RowsCount))
		}
		if l.SizeInBytes < 0 {
			errs = append(errs, fmt.Sprintf("%s: limit.sizeInBytes must be >= 0, got %d", prefix, l.SizeInBytes))
		}
		if l.TimeoutInSeconds < 0 {
			errs = append(errs, fmt.Sprintf("%s: limit.timeoutInSeconds must be >= 0, got %d", prefix, l.TimeoutInSeconds))
		}
	}

	return errs
}

// validateLiteral validates a literal-mode table, including per-column type
// inference over all rows. Returns errors for mixed types, all-null columns,
// arity mismatches, and unsupported value types.
func validateLiteral(prefix string, l *LiteralSpec) []string {
	var errs []string

	if len(l.Rows) == 0 {
		errs = append(errs, fmt.Sprintf("%s: literal mode requires 'rows' with at least one row", prefix))
		return errs
	}

	// Check arity consistency: all rows must have the same number of columns.
	arity := len(l.Rows[0])
	for rowIdx, row := range l.Rows {
		if len(row) != arity {
			errs = append(errs, fmt.Sprintf("%s: rows[%d] has %d columns but rows[0] has %d; all rows must have the same arity", prefix, rowIdx, len(row), arity))
		}
	}
	if len(errs) > 0 {
		// Can't do column-level inference with inconsistent arities.
		return errs
	}

	// columns is required; must match arity; each name valid + unique.
	if len(l.Columns) == 0 {
		errs = append(errs, fmt.Sprintf("%s: literal mode requires 'columns' (non-empty list of names matching the row arity)", prefix))
		return errs
	}
	if len(l.Columns) != arity {
		errs = append(errs, fmt.Sprintf("%s: 'columns' has %d entries but rows have %d columns; lengths must match", prefix, len(l.Columns), arity))
		return errs
	}
	seen := make(map[string]struct{}, arity)
	for i, name := range l.Columns {
		if !validColumnName.MatchString(name) {
			errs = append(errs, fmt.Sprintf("%s: columns[%d] %q is not a valid column name (must match ^[A-Za-z_][A-Za-z0-9_]{0,127}$)", prefix, i, name))
			continue
		}
		if _, dup := seen[name]; dup {
			errs = append(errs, fmt.Sprintf("%s: columns[%d] %q is duplicated", prefix, i, name))
			continue
		}
		seen[name] = struct{}{}
	}
	if len(errs) > 0 {
		return errs
	}

	// Per-column type inference.
	for colIdx := 0; colIdx < arity; colIdx++ {
		inferredType := ""
		allNull := true

		for rowIdx, row := range l.Rows {
			val := row[colIdx]
			if val == nil {
				continue
			}
			allNull = false

			typeName, err := jsonLiteralType(val)
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s: rows[%d] col[%d]: %v", prefix, rowIdx, colIdx, err))
				break
			}

			if inferredType == "" {
				inferredType = typeName
			} else if inferredType != typeName {
				errs = append(errs, fmt.Sprintf("%s: col[%d] has mixed types (%q and %q); all non-null values in a column must have the same type", prefix, colIdx, inferredType, typeName))
				break
			}
		}

		if allNull {
			errs = append(errs, fmt.Sprintf("%s: col[%d] has all-null values; cannot infer schema for column with no non-null values", prefix, colIdx))
		}
	}

	return errs
}

// jsonLiteralType returns a canonical type name for a JSON-decoded value.
// JSON numbers are decoded as json.Number; strings are strings; bools are bool;
// nil is null. Any other Go type is an error.
func jsonLiteralType(v any) (string, error) {
	switch val := v.(type) {
	case json.Number:
		s := val.String()
		for _, ch := range s {
			if ch == '.' || ch == 'e' || ch == 'E' {
				return "double", nil
			}
		}
		return "int", nil
	case float64:
		return "double", nil
	case string:
		return "string", nil
	case bool:
		return "boolean", nil
	default:
		return "", fmt.Errorf("unsupported value type %T (only int/double/string/bool/null allowed in literal rows)", v)
	}
}

// InferLiteralSchema infers column types from the first non-null value per column.
// Returns a slice of type names (length == number of columns).
// Assumes validateLiteral has already been called and returned no errors.
func InferLiteralSchema(rows [][]any) []string {
	if len(rows) == 0 {
		return nil
	}
	arity := len(rows[0])
	types := make([]string, arity)

	for colIdx := 0; colIdx < arity; colIdx++ {
		for _, row := range rows {
			val := row[colIdx]
			if val == nil {
				continue
			}
			t, err := jsonLiteralType(val)
			if err == nil {
				types[colIdx] = t
				break
			}
		}
	}
	return types
}
