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

// TestParseRejectsUnknownComponentField proves the decode is strict at nested
// levels too: an unknown component field is an error naming the key, not a
// silently-dropped value.
func TestParseRejectsUnknownComponentField(t *testing.T) {
	yaml := `
name: legacy
stages:
  - name: s1
    components:
      - name: c1
        component: foo:latest
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

func TestParseRejectsLegacyEnvelope(t *testing.T) {
	body := []byte("apiVersion: datuplet.io/v1\nkind: Pipeline\nmetadata:\n  name: x\nspec:\n  stages: []\n")
	_, err := Parse(body)
	if err == nil || !strings.Contains(err.Error(), "legacy Kubernetes CR format") {
		t.Fatalf("want legacy-format error, got %v", err)
	}
}

func TestParseEnvelopeFreeDoc(t *testing.T) {
	body := []byte("name: events-etl\nstages:\n  - name: s1\n    components:\n      - name: c1\n        component: data-generator\n        config: {tables: [{name: events}]}\n        outputs: {defaultBucket: raw}\n")
	p, err := Parse(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if p.Name != "events-etl" || len(p.Stages) != 1 {
		t.Fatalf("bad doc: %+v", p)
	}
}

func TestParseRejectsUnknownTopLevelField(t *testing.T) {
	_, err := Parse([]byte("name: x\nbogus: 1\nstages: []\n"))
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("want unknown-field error naming the key, got %v", err)
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
