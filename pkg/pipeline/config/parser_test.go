package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseAcceptsSecretRef(t *testing.T) {
	yaml := `
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: secret-ok
spec:
  stages:
    - name: s1
      components:
        - name: c1
          image: foo:latest
          config:
            password: $[db_password]
            user: alice
          outputs:
            defaultBucket: raw
`
	if _, err := Parse([]byte(yaml)); err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestParseRejectsMalformedSecretRef(t *testing.T) {
	yaml := `
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: secret-bad
spec:
  stages:
    - name: s1
      components:
        - name: c1
          image: foo:latest
          config:
            password: "prefix $[db_password] suffix"
          outputs:
            defaultBucket: raw
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for mid-string ref")
	}
	// The single validation source now reports structured paths; the error
	// must locate the offending component positionally.
	if !strings.Contains(err.Error(), "components[0]") {
		t.Errorf("error does not locate the component: %v", err)
	}
}

func TestParseAcceptsEscape(t *testing.T) {
	yaml := `
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: secret-escape
spec:
  stages:
    - name: s1
      components:
        - name: c1
          image: foo:latest
          config:
            literal: $$[not_a_secret]
          outputs:
            defaultBucket: raw
`
	if _, err := Parse([]byte(yaml)); err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

// TestParseNestedConfig proves nested component config survives the
// validate -> FromCRD bridge: a nested YAML list lands in the runtime
// Config map as a []any.
func TestParseNestedConfig(t *testing.T) {
	yaml := `
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: nested-ok
spec:
  stages:
    - name: s1
      components:
        - name: c1
          image: foo:latest
          config:
            tables:
              - name: t1
                columns: [a, b]
          outputs:
            defaultBucket: raw
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := cfg.Spec.Stages[0].Components[0].Config["tables"]
	if _, ok := got.([]any); !ok {
		t.Fatalf("Config[\"tables\"] = %#v, want []any", got)
	}
}

// TestParseRejectsConfigJSON proves the decode is strict: the deleted
// configJSON field is now an unknown-field error, not silently dropped.
func TestParseRejectsConfigJSON(t *testing.T) {
	yaml := `
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: legacy
spec:
  stages:
    - name: s1
      components:
        - name: c1
          image: foo:latest
          configJSON: "{}"
          outputs:
            defaultBucket: raw
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected unknown-field error for configJSON")
	}
	if !strings.Contains(err.Error(), "configJSON") {
		t.Errorf("error does not mention the unknown field: %v", err)
	}
}

func TestParseSinceDuration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		// Day suffix
		{"1d", 24 * time.Hour, false},
		{"3d", 3 * 24 * time.Hour, false},
		{"30d", 30 * 24 * time.Hour, false},

		// Week suffix
		{"1w", 7 * 24 * time.Hour, false},
		{"2w", 14 * 24 * time.Hour, false},

		// Standard Go durations
		{"30m", 30 * time.Minute, false},
		{"12h", 12 * time.Hour, false},
		{"1h30m", 90 * time.Minute, false},

		// Errors
		{"", 0, true},           // empty
		{"0d", 0, true},         // zero days
		{"-1d", 0, true},        // negative days
		{"0w", 0, true},         // zero weeks
		{"-1w", 0, true},        // negative weeks
		{"abcd", 0, true},       // invalid
		{"-30m", 0, true},       // negative standard duration
		{"0s", 0, true},         // zero standard duration
		{"1.5d", 0, true},       // fractional days not supported
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseSinceDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSinceDuration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseSinceDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
