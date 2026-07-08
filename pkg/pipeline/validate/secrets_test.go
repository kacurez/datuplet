package validate

import (
	"reflect"
	"testing"
)

// TestReferencedSecrets_TableCases exercises the whole-scalar $[name]
// extraction across every component's ConfigMap() output.
func TestReferencedSecrets_TableCases(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want []string
	}{
		{
			name: "nested config with duplicate and distinct refs",
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
              token: "$[a]"
              other: "$[b]"
          outputs:
            defaultBucket: raw
    - name: load
      components:
        - name: loader
          image: loader:latest
          config:
            again: "$[a]"
          inputs:
            buckets: [raw]
`,
			want: []string{"a", "b"},
		},
		{
			name: "no refs",
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
            url: "https://example.com"
          outputs:
            defaultBucket: raw
`,
			want: nil,
		},
		{
			name: "escaped ref is not a ref",
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
            url: "$$[x]"
          outputs:
            defaultBucket: raw
`,
			want: nil,
		},
		{
			name: "mid-string ref is not a whole-scalar ref",
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
			want: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, _, err := ValidatePipeline([]byte(c.yaml))
			if err != nil {
				t.Fatalf("ValidatePipeline returned unexpected error: %v", err)
			}
			got := ReferencedSecrets(p)
			if len(got) == 0 && len(c.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("ReferencedSecrets() = %#v, want %#v", got, c.want)
			}
		})
	}
}

// TestReferencedSecrets_NilPipeline proves the helper is nil-safe.
func TestReferencedSecrets_NilPipeline(t *testing.T) {
	if got := ReferencedSecrets(nil); len(got) != 0 {
		t.Fatalf("want empty result for nil pipeline, got %#v", got)
	}
}
