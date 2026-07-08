package examples_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pipelineconfig "github.com/datuplet/datuplet/pkg/pipeline/config"
	"sigs.k8s.io/yaml"
)

// Guards RFC 026 §4.8: every example must strict-decode into the CRD
// types (typos fail) and pass the same semantic validation pipeline-api
// runs at save time. Examples can no longer rot silently.
func TestExamplesAreValid(t *testing.T) {
	files, err := filepath.Glob("pipelines/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no example YAMLs found under examples/pipelines/")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for i, doc := range splitDocs(string(raw)) {
				var probe struct {
					Kind string `json:"kind"`
				}
				if err := yaml.Unmarshal([]byte(doc), &probe); err != nil {
					t.Fatalf("doc %d: %v", i, err)
				}
				switch probe.Kind {
				case "Pipeline":
					var p datupletv1.Pipeline
					if err := yaml.UnmarshalStrict([]byte(doc), &p); err != nil {
						t.Errorf("doc %d strict decode: %v", i, err)
					}
					if _, err := pipelineconfig.Parse([]byte(doc)); err != nil {
						t.Errorf("doc %d semantic validation: %v", i, err)
					}
				case "PipelineRun":
					var pr datupletv1.PipelineRun
					if err := yaml.UnmarshalStrict([]byte(doc), &pr); err != nil {
						t.Errorf("doc %d strict decode: %v", i, err)
					}
				default:
					t.Errorf("doc %d: unexpected kind %q", i, probe.Kind)
				}
			}
		})
	}
}

// splitDocs splits a multi-doc YAML stream on top-level "---" separators
// and drops empty/comment-only documents.
func splitDocs(s string) []string {
	var docs []string
	for _, d := range strings.Split(s, "\n---") {
		trimmed := strings.TrimSpace(strings.TrimPrefix(d, "---"))
		hasContent := false
		for _, line := range strings.Split(trimmed, "\n") {
			l := strings.TrimSpace(line)
			if l != "" && !strings.HasPrefix(l, "#") {
				hasContent = true
				break
			}
		}
		if hasContent {
			docs = append(docs, trimmed)
		}
	}
	return docs
}
