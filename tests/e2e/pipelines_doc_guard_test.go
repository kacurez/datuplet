package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

// TestK8sFixturesAreValidDocs is the offline oracle for RFC 027 E2: every
// K8s-tier pipeline fixture must be an envelope-free PipelineDoc that parses
// through config.Parse — the exact front door pipeline-api's PUT handler and
// the harness's RunPipeline now use. A legacy Kubernetes CR body
// (apiVersion/kind/metadata) is rejected by config.Parse, so this also guards
// against a fixture silently drifting back to the old envelope shape.
//
// It needs no cluster (E2E_K8S unset): fixtures still carry their
// {{.RunPrefix}} template placeholders, but those are quoted scalars, so the
// raw bytes parse as valid YAML without rendering.
func TestK8sFixturesAreValidDocs(t *testing.T) {
	files, err := filepath.Glob("pipelines/k8s/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no fixtures found under pipelines/k8s/")
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
