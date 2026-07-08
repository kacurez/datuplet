package validate

import (
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// validYAML is a minimal, fully-valid pipeline used as the baseline for the
// table cases. It exercises nested component config and a whole-scalar secret
// reference deep inside that config.
const validYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          config:
            url: "https://example.com"
            nested:
              token: "$[api_token]"
          outputs:
            defaultBucket: raw
`

func TestValidatePipeline_TableCases(t *testing.T) {
	cases := []struct {
		name        string
		yaml        string
		wantZero    bool
		msgContains string
	}{
		{
			name:     "valid nested config",
			yaml:     validYAML,
			wantZero: true,
		},
		{
			name: "valid whole-scalar secret ref deep in nested config",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          config:
            a:
              b:
                c:
                  token: "$[api_token]"
          outputs:
            defaultBucket: raw
`,
			wantZero: true,
		},
		{
			name: "unknown field writeMod",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          outputs:
            tables:
              - name: summary
                bucket: curated
                writeMod: APPEND
`,
			msgContains: "writeMod",
		},
		{
			name: "deleted field configJSON",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          configJSON: "{}"
          outputs:
            defaultBucket: raw
`,
			msgContains: "configJSON",
		},
		{
			name: "bad bucket name RAW!",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          outputs:
            defaultBucket: "RAW!"
`,
			msgContains: "RAW!",
		},
		{
			name: "defaultBucket combined with outputs.tables",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          outputs:
            defaultBucket: raw
            tables:
              - name: summary
                bucket: curated
`,
			msgContains: "exclusive",
		},
		{
			name: "mid-string secret ref inside nested config",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          config:
            nested:
              url: "x-$[a]-y"
          outputs:
            defaultBucket: raw
`,
			msgContains: "whole-scalar",
		},
		{
			name: "empty metadata.name",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: ""
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          outputs:
            defaultBucket: raw
`,
			msgContains: "metadata.name is required",
		},
		{
			name: "invalid writeMode UPSERT",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          outputs:
            tables:
              - name: summary
                bucket: curated
                writeMode: UPSERT
`,
			msgContains: "UPSERT",
		},
		{
			name: "invalid processor type keep",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          outputs:
            defaultBucket: raw
            processors:
              - type: keep
                columns: [a]
`,
			msgContains: "keep",
		},
		{
			name: "invalid partition transform week",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          outputs:
            tables:
              - name: summary
                bucket: curated
                partitionFields:
                  - sourceColumn: created
                    transform: week
`,
			msgContains: "week",
		},
		{
			name: "input since and sinceSnapshot mutually exclusive",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          inputs:
            tables:
              - bucket: raw
                table: orders
                since: "3d"
                sinceSnapshot: 12345
`,
			msgContains: "mutually exclusive",
		},
		{
			name: "input invalid since duration",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          inputs:
            tables:
              - bucket: raw
                table: orders
                since: "invalid"
`,
			msgContains: "invalid since duration",
		},
		{
			name: "input valid since",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          inputs:
            tables:
              - bucket: raw
                table: orders
                since: "3d"
`,
			wantZero: true,
		},
		{
			name: "input sinceDays not positive",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          inputs:
            tables:
              - bucket: raw
                table: orders
                sinceDays: 0
`,
			msgContains: "sinceDays must be positive",
		},
		{
			name: "input valid sinceDays",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: extractor
          image: extractor:latest
          inputs:
            tables:
              - bucket: raw
                table: orders
                sinceDays: 7
`,
			wantZero: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, findings, err := ValidatePipeline([]byte(c.yaml), nil)
			if err != nil {
				t.Fatalf("ValidatePipeline returned unexpected error: %v", err)
			}
			if c.wantZero {
				if len(findings) != 0 {
					t.Fatalf("want 0 findings, got %d: %+v", len(findings), findings)
				}
				return
			}
			if len(findings) == 0 {
				t.Fatalf("want >=1 finding, got 0")
			}
			for _, f := range findings {
				if f.Severity != "error" {
					t.Errorf("want severity %q, got %q (finding %+v)", "error", f.Severity, f)
				}
			}
			if c.msgContains != "" {
				found := false
				for _, f := range findings {
					if strings.Contains(f.Message, c.msgContains) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("no finding message contains %q; findings=%+v", c.msgContains, findings)
				}
			}
		})
	}
}

// TestValidatePipeline_ParsesNestedConfig proves the returned typed object
// carries the nested component config through the decode.
func TestValidatePipeline_ParsesNestedConfig(t *testing.T) {
	p, findings, err := ValidatePipeline([]byte(validYAML), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("want 0 findings, got %+v", findings)
	}
	cfg, err := p.Spec.Stages[0].Components[0].ConfigMap()
	if err != nil {
		t.Fatalf("ConfigMap: %v", err)
	}
	nested, ok := cfg["nested"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map in config, got %#v", cfg["nested"])
	}
	if got := nested["token"]; got != "$[api_token]" {
		t.Fatalf("expected nested.token=$[api_token], got %#v", got)
	}
}

// TestValidateTyped_Direct proves controllers can call ValidateTyped on an
// already-decoded object (no YAML round-trip).
func TestValidateTyped_Direct(t *testing.T) {
	p := &datupletv1.Pipeline{
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:    "extractor",
					Image:   "extractor:latest",
					Outputs: &datupletv1.OutputSpec{DefaultBucket: "raw"},
				}},
			}},
		},
	}
	// Empty metadata.name is the only violation.
	findings := ValidateTyped(p, nil)
	if len(findings) != 1 {
		t.Fatalf("want exactly 1 finding, got %d: %+v", len(findings), findings)
	}
	if !strings.Contains(findings[0].Message, "metadata.name is required") {
		t.Fatalf("unexpected finding: %+v", findings[0])
	}
	if findings[0].Severity != "error" {
		t.Fatalf("want severity error, got %q", findings[0].Severity)
	}

	// Filling the name yields zero findings.
	p.Name = "ok"
	if findings := ValidateTyped(p, nil); len(findings) != 0 {
		t.Fatalf("want 0 findings for valid typed pipeline, got %+v", findings)
	}

	t.Run("nil pipeline", func(t *testing.T) {
		findings := ValidateTyped(nil, nil)
		if len(findings) == 0 {
			t.Fatal("want a non-empty findings slice for a nil pipeline, got none")
		}
	})
}
