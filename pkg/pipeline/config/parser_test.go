package config

import (
	"strings"
	"testing"
	"time"
)

// TestParseNestedConfig proves nested component config survives the strict
// decode: a nested YAML list lands in the runtime Config map as a []any.
func TestParseNestedConfig(t *testing.T) {
	yaml := `
name: nested-ok
stages:
  - name: s1
    components:
      - name: c1
        component: foo:latest
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
	got := cfg.Stages[0].Components[0].Config["tables"]
	if _, ok := got.([]any); !ok {
		t.Fatalf("Config[\"tables\"] = %#v, want []any", got)
	}
}

// TestParseRejectsUnknownField proves the decode is strict at every level: an
// unknown field (nested component field or top level) is an error naming the
// key, not a silently-dropped value.
func TestParseRejectsUnknownField(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantKey string
	}{
		{
			name: "component field",
			yaml: `
name: legacy
stages:
  - name: s1
    components:
      - name: c1
        component: foo:latest
        configJSON: "{}"
        outputs:
          defaultBucket: raw
`,
			wantKey: "configJSON",
		},
		{
			name:    "top-level field",
			yaml:    "name: x\nbogus: 1\nstages: []\n",
			wantKey: "bogus",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected unknown-field error for %s", tc.wantKey)
			}
			if !strings.Contains(err.Error(), tc.wantKey) {
				t.Errorf("error does not mention the unknown field %q: %v", tc.wantKey, err)
			}
		})
	}
}

func TestParseRejectsLegacyEnvelope(t *testing.T) {
	body := []byte("apiVersion: datuplet.io/v1\nkind: Pipeline\nmetadata:\n  name: x\nspec:\n  stages: []\n")
	_, err := Parse(body)
	if err == nil || !strings.Contains(err.Error(), "legacy Kubernetes CR format") {
		t.Fatalf("want legacy-format error, got %v", err)
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
		{"", 0, true},     // empty
		{"0d", 0, true},   // zero days
		{"-1d", 0, true},  // negative days
		{"0w", 0, true},   // zero weeks
		{"-1w", 0, true},  // negative weeks
		{"abcd", 0, true}, // invalid
		{"-30m", 0, true}, // negative standard duration
		{"0s", 0, true},   // zero standard duration
		{"1.5d", 0, true}, // fractional days not supported
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
