package examples_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
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
			pipelineNames := map[string]bool{}
			var pipelineRefs []string
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
					// Examples stay in the K8s CRD envelope shape (kubectl
					// apply-able); validate.ValidatePipeline (not
					// config.Parse, which post-RFC-027 is the envelope-free
					// PipelineDoc front door) runs the same semantic checks
					// pipeline-api's save path runs.
					if cr, findings, err := validate.ValidatePipeline([]byte(doc), nil, nil); err != nil || cr == nil {
						t.Errorf("doc %d semantic validation: %v (findings=%+v)", i, err, findings)
					} else {
						for _, f := range findings {
							if f.Severity == "error" {
								t.Errorf("doc %d semantic validation: %s: %s", i, f.Path, f.Message)
							}
						}
					}
					pipelineNames[p.Name] = true
				case "PipelineRun":
					var pr datupletv1.PipelineRun
					if err := yaml.UnmarshalStrict([]byte(doc), &pr); err != nil {
						t.Errorf("doc %d strict decode: %v", i, err)
					}
					pipelineRefs = append(pipelineRefs, pr.Spec.PipelineRef.Name)
				default:
					t.Errorf("doc %d: unexpected kind %q", i, probe.Kind)
				}
			}
			if len(pipelineNames) == 0 {
				t.Errorf("no Pipeline doc found in %s", f)
			}
			if len(pipelineRefs) == 0 {
				t.Errorf("no PipelineRun doc found in %s", f)
			}
			for _, ref := range pipelineRefs {
				if !pipelineNames[ref] {
					t.Errorf("PipelineRun.spec.pipelineRef.name %q does not match any Pipeline metadata.name in %s", ref, f)
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
