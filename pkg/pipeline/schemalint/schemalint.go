// Package schemalint enforces the RFC 027 "Form Subset" of JSON Schema (spec
// §4.2) on component config schemas. A schema that lints clean is guaranteed to
// be fully form-editable by the UI, never falling back to the raw JSON editor.
//
// The linter is a plain recursive map[string]any traversal — no JSON-Schema
// library is required for these rules.
package schemalint

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Issue is a single Form-Subset violation. Path is a JSON-pointer-style
// location ("" is the schema root); Rule is one of the Rule* constants;
// Message is a human-readable explanation.
type Issue struct {
	Path    string
	Rule    string
	Message string
}

// Rule identifiers, one per Form-Subset check (spec §4.2).
const (
	RuleInvalidJSON         = "invalid-json"          // rule 1: must parse as a JSON object
	RuleMissingType         = "missing-type"          // rule 1: `type` present (string or []string) on every node
	RuleForbiddenKeyword    = "forbidden-keyword"     // rule 2: out-of-subset composition keywords
	RuleMissingDescription  = "missing-description"   // rule 3: every property has a non-empty description
	RuleRequiredWithDefault = "required-with-default" // rule 4: required + default on the same property
	RuleUnknownAnnotation   = "unknown-annotation"    // rule 5: unknown x-datuplet-* key
	RuleProducesNotRoot     = "produces-not-root"     // rule 5: x-datuplet-produces only at the root
)

// forbiddenKeywords are the out-of-subset JSON-Schema constructs: composition,
// references, conditionals, pattern-keyed properties, and const. Note that
// nested/recursive arrays and the native multi-value `type` keyword are NOT
// forbidden — only these named keywords are.
var forbiddenKeywords = map[string]struct{}{
	"oneOf":             {},
	"anyOf":             {},
	"allOf":             {},
	"not":               {},
	"$ref":              {},
	"$defs":             {},
	"if":                {},
	"then":              {},
	"else":              {},
	"patternProperties": {},
	"const":             {},
}

// knownAnnotations are the five contract-defined x-datuplet-* keys.
// x-datuplet-produces is additionally constrained to the schema root.
var knownAnnotations = map[string]struct{}{
	"x-datuplet-secret":    {},
	"x-datuplet-multiline": {},
	"x-datuplet-advanced":  {},
	"x-datuplet-doc":       {},
	"x-datuplet-produces":  {},
}

// Lint parses the schema and returns every Form-Subset violation found. An
// empty result means the schema is entirely within the Form Subset.
func Lint(schema []byte) []Issue {
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		return []Issue{{Path: rootPath, Rule: RuleInvalidJSON, Message: fmt.Sprintf("schema is not valid JSON: %v", err)}}
	}
	node, ok := root.(map[string]any)
	if !ok {
		return []Issue{{Path: rootPath, Rule: RuleInvalidJSON, Message: "schema root must be a JSON object"}}
	}
	var issues []Issue
	lintNode(node, rootPath, true, &issues)
	return issues
}

const rootPath = "(root)"

// lintNode applies every per-node rule to one schema node and recurses into its
// subschemas (properties, items, additionalProperties).
func lintNode(node map[string]any, path string, isRoot bool, issues *[]Issue) {
	// Rule 1: `type` present, and either a string or a non-empty array of strings.
	checkType(node, path, issues)

	// Rule 2: no forbidden composition keywords anywhere.
	for _, key := range sortedKeys(node) {
		if _, bad := forbiddenKeywords[key]; bad {
			*issues = append(*issues, Issue{
				Path:    path,
				Rule:    RuleForbiddenKeyword,
				Message: fmt.Sprintf("out-of-subset keyword %q is not allowed", key),
			})
		}
	}

	// Rule 5: x-datuplet-* annotations must be known; produces only at root.
	for _, key := range sortedKeys(node) {
		if !strings.HasPrefix(key, "x-datuplet-") {
			continue
		}
		if _, known := knownAnnotations[key]; !known {
			*issues = append(*issues, Issue{
				Path:    path,
				Rule:    RuleUnknownAnnotation,
				Message: fmt.Sprintf("unknown annotation %q", key),
			})
			continue
		}
		if key == "x-datuplet-produces" && !isRoot {
			*issues = append(*issues, Issue{
				Path:    path,
				Rule:    RuleProducesNotRoot,
				Message: "x-datuplet-produces is only allowed at the schema root",
			})
		}
	}

	// Rule 4: a property must not be both required and carry a default.
	checkRequiredWithDefault(node, path, issues)

	// Rule 3 + recursion into `properties`.
	if props, ok := node["properties"].(map[string]any); ok {
		for _, name := range sortedKeys(props) {
			sub, ok := props[name].(map[string]any)
			childPath := path + "/properties/" + name
			if !ok {
				// A non-object subschema can't be form-rendered; flag missing type.
				*issues = append(*issues, Issue{
					Path:    childPath,
					Rule:    RuleMissingType,
					Message: "property schema must be a JSON object",
				})
				continue
			}
			// Rule 3: every property has a non-empty description.
			if desc, _ := sub["description"].(string); strings.TrimSpace(desc) == "" {
				*issues = append(*issues, Issue{
					Path:    childPath,
					Rule:    RuleMissingDescription,
					Message: "property must have a non-empty description",
				})
			}
			lintNode(sub, childPath, false, issues)
		}
	}

	// Recurse into `items` (single subschema; draft tuple-form array handled too).
	switch items := node["items"].(type) {
	case map[string]any:
		lintNode(items, path+"/items", false, issues)
	case []any:
		for idx, it := range items {
			if sub, ok := it.(map[string]any); ok {
				lintNode(sub, fmt.Sprintf("%s/items/%d", path, idx), false, issues)
			}
		}
	}

	// Recurse into `additionalProperties` when it is a subschema (an object),
	// not when it is a boolean (e.g. additionalProperties: false).
	if ap, ok := node["additionalProperties"].(map[string]any); ok {
		lintNode(ap, path+"/additionalProperties", false, issues)
	}
}

// checkType enforces rule 1's per-node `type` requirement.
func checkType(node map[string]any, path string, issues *[]Issue) {
	raw, present := node["type"]
	if !present {
		*issues = append(*issues, Issue{
			Path:    path,
			Rule:    RuleMissingType,
			Message: "schema node is missing a `type`",
		})
		return
	}
	switch v := raw.(type) {
	case string:
		if v == "" {
			*issues = append(*issues, Issue{Path: path, Rule: RuleMissingType, Message: "`type` must not be empty"})
		}
	case []any:
		if len(v) == 0 {
			*issues = append(*issues, Issue{Path: path, Rule: RuleMissingType, Message: "`type` array must not be empty"})
			return
		}
		for _, elem := range v {
			if _, ok := elem.(string); !ok {
				*issues = append(*issues, Issue{Path: path, Rule: RuleMissingType, Message: "`type` array must contain only strings"})
				return
			}
		}
	default:
		*issues = append(*issues, Issue{Path: path, Rule: RuleMissingType, Message: "`type` must be a string or an array of strings"})
	}
}

// checkRequiredWithDefault enforces rule 4.
func checkRequiredWithDefault(node map[string]any, path string, issues *[]Issue) {
	reqRaw, ok := node["required"].([]any)
	if !ok {
		return
	}
	props, ok := node["properties"].(map[string]any)
	if !ok {
		return
	}
	for _, r := range reqRaw {
		name, ok := r.(string)
		if !ok {
			continue
		}
		sub, ok := props[name].(map[string]any)
		if !ok {
			continue
		}
		if _, hasDefault := sub["default"]; hasDefault {
			*issues = append(*issues, Issue{
				Path:    path + "/properties/" + name,
				Rule:    RuleRequiredWithDefault,
				Message: fmt.Sprintf("property %q is both required and has a default", name),
			})
		}
	}
}

// sortedKeys returns a node's keys in a stable order so issues are deterministic.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
