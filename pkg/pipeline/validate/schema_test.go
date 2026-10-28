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

// schemaMapSecret is a map whose dynamic (additionalProperties) values are each
// an x-datuplet-secret string — exercising secret enforcement on dynamic keys.
const schemaMapSecret = `{
  "type": "object",
  "additionalProperties": { "type": "string", "x-datuplet-secret": true }
}`

// schemaMapInt is a map whose dynamic values must be integers: a whole-scalar
// ref landing on one is a non-string finding.
const schemaMapInt = `{
  "type": "object",
  "additionalProperties": { "type": "integer" }
}`

func TestCompileSchema(t *testing.T) {
	cases := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{"invalid schema string", "{ this is not json", true},
		// The unknown x-datuplet-secret keyword must not break compilation.
		{"ignores x-datuplet-secret", schemaWithSecret, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CompileSchema(tc.schema)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CompileSchema err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	const anyFindings = -1
	cases := []struct {
		name         string
		schema       string
		cfg          string
		wantFindings int    // 0: none; anyFindings: at least one; n: exactly n
		wantPath     string // when set, a finding must sit at this path...
		wantSubstr   string // ...(or, with no path, anywhere) and mention this
	}{
		{
			name: "required missing", schema: schemaWithSecret, cfg: `{}`,
			wantFindings: anyFindings, wantSubstr: "name",
		},
		{
			name: "wrong type", schema: schemaWithSecret, cfg: `{"name": 5}`,
			wantFindings: anyFindings, wantPath: "config.name", wantSubstr: "string",
		},
		{
			// mode has enum ["a","b"]; a whole-scalar ref must NOT be evaluated
			// against the enum (content assertion at the ref path is dropped).
			name: "enum field with ref no finding", schema: schemaWithSecret,
			cfg: `{"name": "ok", "mode": "$[chosen_mode]"}`,
		},
		{
			// The library type error against the "x" placeholder must be
			// dropped, so there is exactly one finding for count.
			name: "integer field with ref non-string finding", schema: schemaWithSecret,
			cfg:          `{"name": "ok", "count": "$[c]"}`,
			wantFindings: 1, wantPath: "config.count", wantSubstr: "non-string",
		},
		{
			name: "secret field with plaintext finding", schema: schemaWithSecret,
			cfg:          `{"name": "ok", "apiKey": "literal-secret"}`,
			wantFindings: anyFindings, wantPath: "config.apiKey", wantSubstr: "requires a $[secret] reference",
		},
		{
			name: "secret field with ref no finding", schema: schemaWithSecret,
			cfg: `{"name": "ok", "apiKey": "$[api_key]"}`,
		},
		{
			// A plaintext map value under a schema-form additionalProperties
			// marked x-datuplet-secret is flagged at the dynamic key's path.
			name: "additionalProperties secret plaintext finding", schema: schemaMapSecret,
			cfg:          `{"a": "plain"}`,
			wantFindings: anyFindings, wantPath: "config.a", wantSubstr: "requires a $[secret] reference",
		},
		{
			// A whole-scalar ref satisfies the secret requirement on the dynamic key.
			name: "additionalProperties secret ref no finding", schema: schemaMapSecret,
			cfg: `{"a": "$[tok]"}`,
		},
		{
			// A ref landing on a dynamic-key value whose additionalProperties
			// subschema type excludes string is a non-string finding; the
			// placeholder type error is dropped, so exactly one finding.
			name: "additionalProperties integer ref non-string finding", schema: schemaMapInt,
			cfg:          `{"a": "$[c]"}`,
			wantFindings: 1, wantPath: "config.a", wantSubstr: "non-string",
		},
		{
			name: "unknown property finding", schema: schemaWithSecret,
			cfg:          `{"name": "ok", "bogus": "y"}`,
			wantFindings: anyFindings,
		},
		{
			name: "valid no findings", schema: schemaWithSecret,
			cfg: `{"name": "ok", "count": 3, "mode": "a", "apiKey": "$[api_key]"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sch, err := CompileSchema(tc.schema)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			findings := ValidateConfig(sch, tc.schema, mustCfg(t, tc.cfg), "config")
			switch {
			case tc.wantFindings == 0 && len(findings) != 0:
				t.Fatalf("expected no findings, got %+v", findings)
			case tc.wantFindings == anyFindings && len(findings) == 0:
				t.Fatal("expected at least one finding, got none")
			case tc.wantFindings > 0 && len(findings) != tc.wantFindings:
				t.Fatalf("expected exactly %d finding(s), got %+v", tc.wantFindings, findings)
			}
			if tc.wantPath != "" {
				f := findingAtPath(findings, tc.wantPath)
				if f == nil {
					t.Fatalf("expected a finding at %s, got %+v", tc.wantPath, findings)
				}
				if !strings.Contains(f.Message, tc.wantSubstr) {
					t.Fatalf("finding at %s = %q, want it to mention %q", tc.wantPath, f.Message, tc.wantSubstr)
				}
			} else if tc.wantSubstr != "" && !hasFindingMsg(findings, tc.wantSubstr) {
				t.Fatalf("expected a finding mentioning %q, got %+v", tc.wantSubstr, findings)
			}
		})
	}
}
