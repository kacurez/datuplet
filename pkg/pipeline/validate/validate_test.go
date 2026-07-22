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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
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
          component: extractor
          inputs:
            tables:
              - bucket: raw
                table: orders
                sinceDays: 7
`,
			wantZero: true,
		},
		{
			name: "duplicate component name in same stage",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: c1
          component: extractor
          outputs:
            defaultBucket: raw
        - name: c1
          component: extractor
          outputs:
            defaultBucket: raw2
`,
			msgContains: "not unique",
		},
		{
			name: "duplicate component name across stages",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: c1
          component: extractor
          outputs:
            defaultBucket: raw
    - name: transform
      components:
        - name: c1
          component: transformer
          inputs:
            buckets: [raw]
          outputs:
            defaultBucket: curated
`,
			msgContains: "not unique",
		},
		{
			name: "all component names unique",
			yaml: `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: c1
          component: extractor
          outputs:
            defaultBucket: raw
        - name: c2
          component: extractor
          outputs:
            defaultBucket: raw2
    - name: transform
      components:
        - name: c3
          component: transformer
          inputs:
            buckets: [raw]
          outputs:
            defaultBucket: curated
`,
			wantZero: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, findings, err := ValidatePipeline([]byte(c.yaml), nil, nil)
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
	p, findings, err := ValidatePipeline([]byte(validYAML), nil, nil)
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
					Name:      "extractor",
					Component: "extractor",
					Outputs:   &datupletv1.OutputSpec{DefaultBucket: "raw"},
				}},
			}},
		},
	}
	// Empty metadata.name is the only violation.
	findings := ValidateTyped(p, nil, nil)
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
	if findings := ValidateTyped(p, nil, nil); len(findings) != 0 {
		t.Fatalf("want 0 findings for valid typed pipeline, got %+v", findings)
	}

	t.Run("nil pipeline", func(t *testing.T) {
		findings := ValidateTyped(nil, nil, nil)
		if len(findings) == 0 {
			t.Fatal("want a non-empty findings slice for a nil pipeline, got none")
		}
	})
}

func TestValidatePipelineDocEnvelopeRejected(t *testing.T) {
	_, fs := ValidatePipelineDoc([]byte("apiVersion: v1\nkind: Pipeline\n"), "x", nil, nil)
	requireFinding(t, fs, "error", "legacy Kubernetes CR format")
}

func TestValidatePipelineDocNameContext(t *testing.T) {
	body := []byte("name: other\nstages: []\n")
	_, fs := ValidatePipelineDoc(body, "route-name", nil, nil)
	requireFinding(t, fs, "error", `name "other" does not match`)
	// Empty context (POST /validate, no name): equality check skipped.
	_, fs = ValidatePipelineDoc(body, "", nil, nil)
	forbidFinding(t, fs, "does not match")
}

// TestValidatePipelineDocEffectiveName is a regression test for the bug where
// effectiveName (contextName when the body omits name) was used only for the
// DNS-1123 check and never written back onto doc.Name before config.DocToCR,
// so a body-omits-name PUT/validate/trigger request produced a CR with an
// empty ObjectMeta.Name and spuriously failed the "metadata.name is required"
// check in ValidateTyped.
func TestValidatePipelineDocEffectiveName(t *testing.T) {
	validBody := func(nameLine string) []byte {
		return []byte(nameLine + `stages:
  - name: a
    components:
      - {name: c1, component: x, outputs: {defaultBucket: raw}}
`)
	}

	t.Run("name absent, contextName supplies it", func(t *testing.T) {
		cr, fs := ValidatePipelineDoc(validBody(""), "my-pipeline", nil, nil)
		forbidFinding(t, fs, "metadata.name is required")
		if cr == nil || cr.Name != "my-pipeline" {
			t.Fatalf("want cr.Name == %q, got %+v", "my-pipeline", cr)
		}
	})

	t.Run("name present and matches contextName", func(t *testing.T) {
		cr, fs := ValidatePipelineDoc(validBody("name: my-pipeline\n"), "my-pipeline", nil, nil)
		forbidFinding(t, fs, "metadata.name is required")
		forbidFinding(t, fs, "does not match")
		if cr == nil || cr.Name != "my-pipeline" {
			t.Fatalf("want cr.Name == %q, got %+v", "my-pipeline", cr)
		}
	})

	t.Run("name present and mismatches contextName still flagged", func(t *testing.T) {
		_, fs := ValidatePipelineDoc(validBody("name: other\n"), "my-pipeline", nil, nil)
		requireFinding(t, fs, "error", `name "other" does not match`)
	})
}

func TestUpstreamInputDemotedToWarning(t *testing.T) {
	body := []byte(`name: p
stages:
  - name: a
    components:
      - {name: c1, component: x, outputs: {defaultBucket: raw}}
  - name: b
    components:
      - name: c2
        component: y
        inputs: {tables: [{bucket: staging, table: preexisting}]}
        outputs: {defaultBucket: out}
`)
	_, fs := ValidatePipelineDoc(body, "p", nil, nil)
	f := findByPath(t, fs, "stages[1].components[0].inputs.tables[0]")
	if f.Severity != "warning" || !strings.Contains(f.Message, "assumed to pre-exist in storage") {
		t.Fatalf("want warning about pre-existing storage table, got %+v", f)
	}
}

// requireFinding fails unless fs contains a finding with the given severity
// whose message contains substr.
func requireFinding(t *testing.T, fs []Finding, severity, substr string) {
	t.Helper()
	for _, f := range fs {
		if f.Severity == severity && strings.Contains(f.Message, substr) {
			return
		}
	}
	t.Fatalf("want %s finding containing %q, got %+v", severity, substr, fs)
}

// forbidFinding fails if any finding's message contains substr.
func forbidFinding(t *testing.T, fs []Finding, substr string) {
	t.Helper()
	for _, f := range fs {
		if strings.Contains(f.Message, substr) {
			t.Fatalf("did not want a finding containing %q, got %+v", substr, f)
		}
	}
}

// findByPath returns the first finding at the exact path, failing if none.
func findByPath(t *testing.T, fs []Finding, path string) Finding {
	t.Helper()
	for _, f := range fs {
		if f.Path == path {
			return f
		}
	}
	t.Fatalf("no finding at path %q, got %+v", path, fs)
	return Finding{}
}
