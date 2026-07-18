package config

import (
	"bytes"
	"os"
	"testing"
)

// exampleDoc builds the validated example PipelineDoc from spec §3.
func exampleDoc() *Pipeline {
	return &Pipeline{
		Name:        "events-etl",
		Description: "Generate demo events and aggregate revenue per country.",
		Gateway: GatewayConfig{
			ChunkSize: 33554432,
		},
		Stages: []Stage{
			{
				Name: "generate",
				Components: []Component{
					{
						Name:      "gen",
						Component: "data-generator",
						Version:   "0.9.1",
						Config: map[string]any{
							"tables": []any{
								map[string]any{
									"name":           "events",
									"rowInsertSpeed": 1000,
									"random": map[string]any{
										"schema": map[string]any{
											"id":      "uuid",
											"created": "timestamp",
											"amount":  "double",
											"country": "string",
										},
										"limit": map[string]any{
											"rowsCount": 100000,
										},
									},
								},
							},
						},
						Outputs: &OutputSpec{
							DefaultBucket:    "raw",
							DefaultWriteMode: "APPEND",
						},
					},
				},
			},
			{
				Name: "transform",
				Components: []Component{
					{
						Name:      "daily-summary",
						Component: "sql-transform",
						Inputs: &InputSpec{
							Tables: []InputTableSpec{
								{Bucket: "raw", Table: "events"},
							},
						},
						Config: map[string]any{
							"sql": "CREATE TABLE daily_summary AS\nSELECT country, count(*) AS events, sum(amount) AS revenue\nFROM events GROUP BY country;\n",
						},
						Outputs: &OutputSpec{
							Tables: []OutputTableSpec{
								{Name: "daily_summary", Bucket: "curated", WriteMode: "FULL_LOAD"},
							},
						},
					},
				},
			},
		},
	}
}

// TestRenderYAMLGolden pins the exact byte output of RenderYAML against a
// checked-in golden file. Config map keys are sorted (id/created/amount/country
// -> amount/country/created/id) — this is expected yaml.v3 behavior, not a bug.
func TestRenderYAMLGolden(t *testing.T) {
	got, err := RenderYAML(exampleDoc())
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/render_golden.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("golden mismatch:\n%s", got)
	}
}

// TestRenderYAMLDeterministic proves two renders of the same doc are
// byte-for-byte identical — required since the API (S6) and CLI serve
// RenderYAML output directly and must not flap between requests.
func TestRenderYAMLDeterministic(t *testing.T) {
	a, err := RenderYAML(exampleDoc())
	if err != nil {
		t.Fatal(err)
	}
	b, err := RenderYAML(exampleDoc())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("nondeterministic render")
	}
}

// TestRenderParseRoundTrip proves rendered YAML re-parses through Parse (S1)
// without loss of the top-level shape.
func TestRenderParseRoundTrip(t *testing.T) {
	y, err := RenderYAML(exampleDoc())
	if err != nil {
		t.Fatal(err)
	}
	p, err := Parse(y)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if p.Name != exampleDoc().Name || len(p.Stages) != len(exampleDoc().Stages) {
		t.Fatal("lossy render")
	}
}
