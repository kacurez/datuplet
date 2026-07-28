package arrow

import (
	"encoding/json"
	"sort"
	"strconv"
)

// stringifyValue renders one decoded JSON value as an Arrow String cell.
// Returns (cell, isNull). json.Number keeps its exact source text; nested
// objects/arrays become compact JSON.
func stringifyValue(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", true
	case string:
		return x, false
	case json.Number:
		return x.String(), false
	case bool:
		return strconv.FormatBool(x), false
	case float64: // defensive: non-UseNumber callers
		return strconv.FormatFloat(x, 'g', -1, 64), false
	default: // map[string]any, []any
		b, err := json.Marshal(x)
		if err != nil {
			return "", true
		}
		return string(b), false
	}
}

// columnPlan fixes the output column names and how to pull each column's
// value out of a decoded record.
type columnPlan struct {
	names   []string
	extract func(rec map[string]any, i int) any
}

// planFromBatch derives the plan when no explicit columns are given: the
// sorted union of top-level keys across the first batch (matching the JSONL
// path's gateway inference, which collected field names from all objects in
// the first chunk).
func planFromBatch(batch []map[string]any) *columnPlan {
	set := make(map[string]bool)
	for _, rec := range batch {
		for k := range rec {
			set[k] = true
		}
	}
	names := make([]string, 0, len(set))
	for k := range set {
		names = append(names, k)
	}
	sort.Strings(names)
	return &columnPlan{
		names: names,
		extract: func(rec map[string]any, i int) any {
			return rec[names[i]]
		},
	}
}
