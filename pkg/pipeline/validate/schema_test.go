package validate

import (
	"encoding/json"
	"strings"
	"testing"
)

// schemaWithSecret exercises every branch of the algorithm: a required
// string, an integer field, a typeless enum field, an additionalProperties
// gate, and an x-datuplet-secret string field.
const schemaWithSecret = `{
  "type": "object",
  "additionalProperties": false,
  "required": ["name"],
  "properties": {
    "name":   { "type": "string" },
    "count":  { "type": "integer" },
    "mode":   { "enum": ["a", "b"] },
    "apiKey": { "type": "string", "x-datuplet-secret": true }
  }
}`

func mustCfg(t *testing.T, jsonStr string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		t.Fatalf("bad test config JSON: %v", err)
	}
	return m
}

func hasFindingMsg(findings []Finding, substr string) bool {
	for _, f := range findings {
		if strings.Contains(f.Message, substr) {
			return true
		}
	}
	return false
}

func findingAtPath(findings []Finding, path string) *Finding {
	for i := range findings {
		if findings[i].Path == path {
			return &findings[i]
		}
	}
	return nil
}

func TestCompileSchema_InvalidSchemaString(t *testing.T) {
	if _, err := CompileSchema("{ this is not json"); err == nil {
		t.Fatal("expected error compiling invalid schema string, got nil")
	}
}

func TestCompileSchema_IgnoresXDatupletSecret(t *testing.T) {
	// The unknown x-datuplet-secret keyword must not break compilation.
	if _, err := CompileSchema(schemaWithSecret); err != nil {
		t.Fatalf("CompileSchema rejected x-datuplet-secret keyword: %v", err)
	}
}

func TestValidateConfig_RequiredMissing(t *testing.T) {
	sch, err := CompileSchema(schemaWithSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg := mustCfg(t, `{}`)
	findings := ValidateConfig(sch, schemaWithSecret, cfg, "config")
	if len(findings) == 0 {
		t.Fatal("expected a finding for missing required field, got none")
	}
	if !hasFindingMsg(findings, "name") {
		t.Fatalf("expected finding to mention missing 'name', got %+v", findings)
	}
}

func TestValidateConfig_WrongType(t *testing.T) {
	sch, err := CompileSchema(schemaWithSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg := mustCfg(t, `{"name": 5}`)
	findings := ValidateConfig(sch, schemaWithSecret, cfg, "config")
	f := findingAtPath(findings, "config.name")
	if f == nil {
		t.Fatalf("expected a type finding at config.name, got %+v", findings)
	}
	if !strings.Contains(f.Message, "string") {
		t.Fatalf("expected type finding to mention string, got %q", f.Message)
	}
}

func TestValidateConfig_EnumFieldWithRef_NoFinding(t *testing.T) {
	sch, err := CompileSchema(schemaWithSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// mode has enum ["a","b"]; a whole-scalar ref must NOT be evaluated
	// against the enum (content assertion at the ref path is dropped).
	cfg := mustCfg(t, `{"name": "ok", "mode": "$[chosen_mode]"}`)
	findings := ValidateConfig(sch, schemaWithSecret, cfg, "config")
	if len(findings) != 0 {
		t.Fatalf("expected no findings for enum field holding a ref, got %+v", findings)
	}
}

func TestValidateConfig_IntegerFieldWithRef_NonStringFinding(t *testing.T) {
	sch, err := CompileSchema(schemaWithSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg := mustCfg(t, `{"name": "ok", "count": "$[c]"}`)
	findings := ValidateConfig(sch, schemaWithSecret, cfg, "config")
	f := findingAtPath(findings, "config.count")
	if f == nil {
		t.Fatalf("expected a non-string finding at config.count, got %+v", findings)
	}
	if !strings.Contains(f.Message, "non-string") {
		t.Fatalf("expected non-string finding, got %q", f.Message)
	}
	// The library type error against the "x" placeholder must be dropped, so
	// there is exactly one finding for count.
	if len(findings) != 1 {
		t.Fatalf("expected exactly one finding, got %+v", findings)
	}
}

func TestValidateConfig_SecretFieldWithPlaintext_Finding(t *testing.T) {
	sch, err := CompileSchema(schemaWithSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg := mustCfg(t, `{"name": "ok", "apiKey": "literal-secret"}`)
	findings := ValidateConfig(sch, schemaWithSecret, cfg, "config")
	f := findingAtPath(findings, "config.apiKey")
	if f == nil {
		t.Fatalf("expected a secret-ref-required finding at config.apiKey, got %+v", findings)
	}
	if !strings.Contains(f.Message, "requires a $[secret] reference") {
		t.Fatalf("unexpected message: %q", f.Message)
	}
}

func TestValidateConfig_SecretFieldWithRef_NoFinding(t *testing.T) {
	sch, err := CompileSchema(schemaWithSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg := mustCfg(t, `{"name": "ok", "apiKey": "$[api_key]"}`)
	findings := ValidateConfig(sch, schemaWithSecret, cfg, "config")
	if len(findings) != 0 {
		t.Fatalf("expected no findings for secret field holding a ref, got %+v", findings)
	}
}

// schemaMapSecret is a map whose dynamic (additionalProperties) values are each
// an x-datuplet-secret string — exercising secret enforcement on dynamic keys.
const schemaMapSecret = `{
  "type": "object",
  "additionalProperties": { "type": "string", "x-datuplet-secret": true }
}`

func TestValidateConfig_AdditionalPropsSecret_Plaintext_Finding(t *testing.T) {
	sch, err := CompileSchema(schemaMapSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// A plaintext map value under a schema-form additionalProperties marked
	// x-datuplet-secret must be flagged at the dynamic key's path.
	cfg := mustCfg(t, `{"a": "plain"}`)
	findings := ValidateConfig(sch, schemaMapSecret, cfg, "config")
	f := findingAtPath(findings, "config.a")
	if f == nil {
		t.Fatalf("expected a secret-ref-required finding at config.a, got %+v", findings)
	}
	if !strings.Contains(f.Message, "requires a $[secret] reference") {
		t.Fatalf("unexpected message: %q", f.Message)
	}
}

func TestValidateConfig_AdditionalPropsSecret_Ref_NoFinding(t *testing.T) {
	sch, err := CompileSchema(schemaMapSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// A whole-scalar ref satisfies the secret requirement on the dynamic key.
	cfg := mustCfg(t, `{"a": "$[tok]"}`)
	findings := ValidateConfig(sch, schemaMapSecret, cfg, "config")
	if len(findings) != 0 {
		t.Fatalf("expected no findings for dynamic-key value holding a ref, got %+v", findings)
	}
}

func TestValidateConfig_AdditionalPropsInteger_Ref_NonStringFinding(t *testing.T) {
	// step 2: a ref landing on a dynamic-key value whose additionalProperties
	// subschema type excludes string is a non-string finding.
	const schemaMapInt = `{
	  "type": "object",
	  "additionalProperties": { "type": "integer" }
	}`
	sch, err := CompileSchema(schemaMapInt)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg := mustCfg(t, `{"a": "$[c]"}`)
	findings := ValidateConfig(sch, schemaMapInt, cfg, "config")
	f := findingAtPath(findings, "config.a")
	if f == nil {
		t.Fatalf("expected a non-string finding at config.a, got %+v", findings)
	}
	if !strings.Contains(f.Message, "non-string") {
		t.Fatalf("expected non-string finding, got %q", f.Message)
	}
	// The library type error against the "x" placeholder must be dropped, so
	// there is exactly one finding.
	if len(findings) != 1 {
		t.Fatalf("expected exactly one finding, got %+v", findings)
	}
}

func TestValidateConfig_UnknownProperty_Finding(t *testing.T) {
	sch, err := CompileSchema(schemaWithSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg := mustCfg(t, `{"name": "ok", "bogus": "y"}`)
	findings := ValidateConfig(sch, schemaWithSecret, cfg, "config")
	if len(findings) == 0 {
		t.Fatalf("expected a finding for unknown property under additionalProperties:false, got none")
	}
}

func TestValidateConfig_Valid_NoFindings(t *testing.T) {
	sch, err := CompileSchema(schemaWithSecret)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg := mustCfg(t, `{"name": "ok", "count": 3, "mode": "a", "apiKey": "$[api_key]"}`)
	findings := ValidateConfig(sch, schemaWithSecret, cfg, "config")
	if len(findings) != 0 {
		t.Fatalf("expected no findings for a valid config, got %+v", findings)
	}
}
