package validate

import "fmt"

// walkStrings recurses through a decoded generic value (map[string]any /
// []any / string) and invokes fn for every string leaf with its dotted /
// indexed path relative to the supplied base path. Non-string scalars
// (numbers, bools, nil) are ignored. It is used to apply the whole-scalar
// secret-reference check to every string in a component's ConfigMap() output,
// nested config included.
func walkStrings(v any, path string, fn func(path, s string)) {
	switch node := v.(type) {
	case map[string]any:
		for k, child := range node {
			walkStrings(child, joinKey(path, k), fn)
		}
	case []any:
		for i, child := range node {
			walkStrings(child, fmt.Sprintf("%s[%d]", path, i), fn)
		}
	case string:
		fn(path, node)
	}
}

func joinKey(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
