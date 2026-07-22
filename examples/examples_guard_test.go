package examples_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

// Guards RFC 027 §3/E1: every example is an envelope-free PipelineDoc that
// parses through config.Parse — the same front door pipeline-api's PUT
// handler and the CLI's `pipeline put` use. A legacy Kubernetes CR body
// (apiVersion/kind/metadata) would be rejected by config.Parse, so this also
// guards against examples silently drifting back to the old envelope shape.
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
			p, err := config.Parse(raw)
			if err != nil {
				t.Fatalf("config.Parse: %v", err)
			}
			if p == nil {
				t.Fatal("config.Parse returned a nil pipeline with no error")
			}
			if p.Name == "" {
				t.Errorf("%s: pipeline doc has no top-level name", f)
			}
			if len(p.Stages) == 0 {
				t.Errorf("%s: pipeline doc has no stages", f)
			}
		})
	}
}
