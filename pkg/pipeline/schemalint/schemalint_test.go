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

func TestLint_PassingFixture(t *testing.T) {
	issues := Lint([]byte(passingSchema))
	if len(issues) != 0 {
		t.Fatalf("expected zero issues on passing fixture, got %d: %+v", len(issues), issues)
	}
}

func TestLint_InvalidJSON(t *testing.T) {
	issues := Lint([]byte(`{ not json `))
	if !hasRule(issues, RuleInvalidJSON) {
		t.Fatalf("expected %q issue, got %+v", RuleInvalidJSON, issues)
	}
}

func TestLint_Rule1_MissingType(t *testing.T) {
	// The `bad` property has no `type`.
	schema := `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": { "description": "no type here" }
  }
}`
	issues := Lint([]byte(schema))
	if !hasRule(issues, RuleMissingType) {
		t.Fatalf("expected %q issue, got %+v", RuleMissingType, issues)
	}
}

func TestLint_Rule1_MultiTypeAccepted(t *testing.T) {
	// A multi-type array must NOT trigger a missing/invalid-type issue.
	schema := `{
  "type": "object",
  "description": "root",
  "properties": {
    "ok": { "type": ["string", "number"], "description": "multi-type" }
  }
}`
	issues := Lint([]byte(schema))
	if hasRule(issues, RuleMissingType) {
		t.Fatalf("multi-type array must be accepted, got %+v", issues)
	}
}

func TestLint_Rule2_ForbiddenKeyword(t *testing.T) {
	schema := `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": {
      "type": "string",
      "description": "d",
      "oneOf": [{"const": "x"}]
    }
  }
}`
	issues := Lint([]byte(schema))
	if !hasRule(issues, RuleForbiddenKeyword) {
		t.Fatalf("expected %q issue, got %+v", RuleForbiddenKeyword, issues)
	}
}

func TestLint_Rule2_ArrayOfArraysAccepted(t *testing.T) {
	schema := `{
  "type": "object",
  "description": "root",
  "properties": {
    "rows": {
      "type": "array",
      "description": "d",
      "items": { "type": "array", "description": "inner" }
    }
  }
}`
	issues := Lint([]byte(schema))
	if len(issues) != 0 {
		t.Fatalf("array-of-arrays must lint clean, got %+v", issues)
	}
}

func TestLint_Rule3_MissingDescription(t *testing.T) {
	schema := `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": { "type": "string" }
  }
}`
	issues := Lint([]byte(schema))
	if !hasRule(issues, RuleMissingDescription) {
		t.Fatalf("expected %q issue, got %+v", RuleMissingDescription, issues)
	}
}

func TestLint_Rule3_EmptyDescription(t *testing.T) {
	schema := `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": { "type": "string", "description": "   " }
  }
}`
	issues := Lint([]byte(schema))
	if !hasRule(issues, RuleMissingDescription) {
		t.Fatalf("expected %q issue for whitespace-only description, got %+v", RuleMissingDescription, issues)
	}
}

func TestLint_Rule4_RequiredWithDefault(t *testing.T) {
	schema := `{
  "type": "object",
  "description": "root",
  "required": ["k"],
  "properties": {
    "k": { "type": "string", "default": "x", "description": "d" }
  }
}`
	issues := Lint([]byte(schema))
	if !hasRule(issues, RuleRequiredWithDefault) {
		t.Fatalf("expected %q issue, got %+v", RuleRequiredWithDefault, issues)
	}
}

func TestLint_Rule5_UnknownAnnotation(t *testing.T) {
	schema := `{
  "type": "object",
  "description": "root",
  "properties": {
    "bad": { "type": "string", "description": "d", "x-datuplet-bogus": true }
  }
}`
	issues := Lint([]byte(schema))
	if !hasRule(issues, RuleUnknownAnnotation) {
		t.Fatalf("expected %q issue, got %+v", RuleUnknownAnnotation, issues)
	}
}

func TestLint_Rule5_ProducesNotRoot(t *testing.T) {
	schema := `{
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
}`
	issues := Lint([]byte(schema))
	if !hasRule(issues, RuleProducesNotRoot) {
		t.Fatalf("expected %q issue, got %+v", RuleProducesNotRoot, issues)
	}
}

func TestLint_Rule5_ProducesAtRootAccepted(t *testing.T) {
	schema := `{
  "type": "object",
  "description": "root",
  "x-datuplet-produces": "foo",
  "properties": {
    "foo": { "type": "string", "description": "d" }
  }
}`
	issues := Lint([]byte(schema))
	if hasRule(issues, RuleProducesNotRoot) || hasRule(issues, RuleUnknownAnnotation) {
		t.Fatalf("x-datuplet-produces at root must be accepted, got %+v", issues)
	}
}

// Sanity: Issue paths are non-empty and human-readable.
func TestLint_IssuePathPresent(t *testing.T) {
	schema := `{ "type": "object", "description": "root", "properties": { "bad": { "type": "string" } } }`
	issues := Lint([]byte(schema))
	if len(issues) == 0 {
		t.Fatal("expected at least one issue")
	}
	for _, i := range issues {
		if strings.TrimSpace(i.Path) == "" {
			t.Fatalf("issue has empty path: %+v", i)
		}
		if strings.TrimSpace(i.Message) == "" {
			t.Fatalf("issue has empty message: %+v", i)
		}
	}
}
