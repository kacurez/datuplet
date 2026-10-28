package schemalint

import (
	"strings"
	"testing"
)

// hasRule reports whether any issue has the given Rule.
func hasRule(issues []Issue, rule string) bool {
	for _, i := range issues {
		if i.Rule == rule {
			return true
		}
	}
	return false
}

// passingSchema exercises every allowed Form-Subset construct, including the
// two maintainer-accepted exceptions (array-of-arrays and multi-type `type`).
// It must lint completely clean.
const passingSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Everything",
  "description": "A schema exercising every allowed construct.",
  "type": "object",
  "additionalProperties": false,
  "required": ["req_scalar"],
  "x-datuplet-produces": "req_scalar",
  "properties": {
    "req_scalar": {
      "type": "string",
      "description": "A required scalar (no default, so rule 4 stays satisfied)."
    },
    "nested_object": {
      "type": "object",
      "additionalProperties": false,
      "required": ["inner"],
      "description": "A nested field group.",
      "properties": {
        "inner": {
          "type": "integer",
          "minimum": 0,
          "description": "An inner scalar."
        }
      }
    },
    "array_of_objects": {
      "type": "array",
      "minItems": 1,
      "description": "Repeater cards.",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "description": "One card.",
        "properties": {
          "field": {
            "type": "string",
            "description": "A card field."
          }
        }
      }
    },
    "scalar_array": {
      "type": "array",
      "description": "A list of scalars.",
      "items": {
        "type": "string",
        "description": "A scalar item."
      }
    },
    "enum_map": {
      "type": "object",
      "description": "A key->value map with enum-constrained values.",
      "additionalProperties": {
        "type": "string",
        "enum": ["a", "b", "c"],
        "description": "The mapped value."
      }
    },
    "tristate_bool": {
      "type": "boolean",
      "default": true,
      "description": "A tri-state boolean (default present, but not required)."
    },
    "secret_field": {
      "type": "string",
      "x-datuplet-secret": true,
      "description": "A secret."
    },
    "code_field": {
      "type": "string",
      "x-datuplet-multiline": "sql",
      "description": "A multiline code field."
    },
    "advanced_field": {
      "type": "integer",
      "minimum": 1,
      "x-datuplet-advanced": true,
      "description": "An advanced knob.",
      "x-datuplet-doc": "Extended documentation string."
    },
    "array_of_arrays": {
      "type": "array",
      "description": "Exception 1: array whose items are themselves arrays.",
      "items": {
        "type": "array",
        "description": "One inner row of heterogeneous cells (deliberately no nested items)."
      }
    },
    "multi_type": {
      "type": ["string", "number", "boolean", "array"],
      "description": "Exception 2: JSON Schema native multi-type 'type' array."
    }
  }
}`

// TestLint covers every Form-Subset rule: each row lints one schema and asserts
// the rule that must fire, the rules that must not, or a fully clean result.
// Whatever the row, every issue must carry a human-readable path and message.
func TestLint(t *testing.T) {
	cases := []struct {
		name      string
		schema    string
		wantRule  string   // an issue with this rule must be reported
		forbid    []string // no issue may carry any of these rules
		wantClean bool     // zero issues expected
	}{
		{name: "passing fixture is clean", schema: passingSchema, wantClean: true},
		{name: "invalid JSON", schema: `{ not json `, wantRule: RuleInvalidJSON},
		{
			// The `bad` property has no `type`.
			name: "rule1 missing type",
			schema: `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": { "description": "no type here" }
  }
}`,
			wantRule: RuleMissingType,
		},
		{
			// A multi-type array must NOT trigger a missing/invalid-type issue.
			name: "rule1 multi-type accepted",
			schema: `{
  "type": "object",
  "description": "root",
  "properties": {
    "ok": { "type": ["string", "number"], "description": "multi-type" }
  }
}`,
			forbid: []string{RuleMissingType},
		},
		{
			name: "rule2 forbidden keyword",
			schema: `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": {
      "type": "string",
      "description": "d",
      "oneOf": [{"const": "x"}]
    }
  }
}`,
			wantRule: RuleForbiddenKeyword,
		},
		{
			name: "rule2 array of arrays accepted",
			schema: `{
  "type": "object",
  "description": "root",
  "properties": {
    "rows": {
      "type": "array",
      "description": "d",
      "items": { "type": "array", "description": "inner" }
    }
  }
}`,
			wantClean: true,
		},
		{
			name: "rule3 missing description",
			schema: `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": { "type": "string" }
  }
}`,
			wantRule: RuleMissingDescription,
		},
		{
			name: "rule3 whitespace-only description",
			schema: `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": { "type": "string", "description": "   " }
  }
}`,
			wantRule: RuleMissingDescription,
		},
		{
			name: "rule4 required with default",
			schema: `{
  "type": "object",
  "description": "root",
  "required": ["k"],
  "properties": {
    "k": { "type": "string", "default": "x", "description": "d" }
  }
}`,
			wantRule: RuleRequiredWithDefault,
		},
		{
			name: "rule5 unknown annotation",
			schema: `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": { "type": "string", "description": "d", "x-datuplet-bogus": true }
  }
}`,
			wantRule: RuleUnknownAnnotation,
		},
		{
			name: "rule5 produces not at root",
			schema: `{
  "type": "object",
  "description": "root",
  "properties": {
    "child": {
      "type": "object",
      "description": "d",
      "x-datuplet-produces": "foo",
      "properties": {
        "inner": { "type": "string", "description": "d" }
      }
    }
  }
}`,
			wantRule: RuleProducesNotRoot,
		},
		{
			name: "rule5 produces at root accepted",
			schema: `{
  "type": "object",
  "description": "root",
  "x-datuplet-produces": "foo",
  "properties": {
    "foo": { "type": "string", "description": "d" }
  }
}`,
			forbid: []string{RuleProducesNotRoot, RuleUnknownAnnotation},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issues := Lint([]byte(tc.schema))
			if tc.wantClean && len(issues) != 0 {
				t.Fatalf("expected zero issues, got %d: %+v", len(issues), issues)
			}
			if tc.wantRule != "" && !hasRule(issues, tc.wantRule) {
				t.Fatalf("expected %q issue, got %+v", tc.wantRule, issues)
			}
			for _, rule := range tc.forbid {
				if hasRule(issues, rule) {
					t.Fatalf("did not want a %q issue, got %+v", rule, issues)
				}
			}
			for _, i := range issues {
				if strings.TrimSpace(i.Path) == "" {
					t.Fatalf("issue has empty path: %+v", i)
				}
				if strings.TrimSpace(i.Message) == "" {
					t.Fatalf("issue has empty message: %+v", i)
				}
			}
		})
	}
}
