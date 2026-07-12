package framework

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const sampleMultiDoc = `---
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: myrun-http-json-extract
  namespace: datuplet
spec:
  stages:
    - name: extract
      components:
        - name: json-extractor
          component: http-json-extractor
          version: dev
          config:
            url: "http://e2e-http-fixture.datuplet-e2e.svc.cluster.local/posts"
          outputs:
            defaultBucket: myrun-api
            defaultWriteMode: FULL_LOAD
---
apiVersion: datuplet.io/v1
kind: PipelineRun
metadata:
  name: myrun-http-json-extract-run
  namespace: datuplet
spec:
  pipelineRef:
    name: myrun-http-json-extract
`

func TestRewriteYAMLWithRunToken_InjectsRunIDAndSecretRef(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pipeline.yaml")
	if err := os.WriteFile(path, []byte(sampleMultiDoc), 0o644); err != nil {
		t.Fatalf("write sample: %v", err)
	}

	runID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	secretName := "myrun-http-json-extract-run-token"
	namespace := "datuplet-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	lakekeeperProjectID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	if err := rewriteYAMLWithRunToken(path, runID, secretName, namespace, lakekeeperProjectID); err != nil {
		t.Fatalf("rewriteYAMLWithRunToken: %v", err)
	}

	out, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}

	// Decode every doc; verify Pipeline untouched, PipelineRun has runId
	// + runTokenRef.name.
	dec := yaml.NewDecoder(bytes.NewReader(out))
	seenPipeline, seenPipelineRun := false, false
	for {
		var raw map[string]any
		if err := dec.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode result: %v", err)
		}
		if raw == nil {
			continue
		}
		kind, _ := raw["kind"].(string)
		switch kind {
		case "Pipeline":
			seenPipeline = true
			if _, hasSpec := raw["spec"]; !hasSpec {
				t.Error("Pipeline doc missing spec after rewrite")
			}
			// No spec.runId should have been injected on Pipeline.
			spec, _ := raw["spec"].(map[string]any)
			if _, hasRunID := spec["runId"]; hasRunID {
				t.Error("Pipeline.spec.runId was injected — should only be on PipelineRun")
			}
			// Namespace should be rewritten to the per-project ns.
			md, _ := raw["metadata"].(map[string]any)
			if got := md["namespace"]; got != namespace {
				t.Errorf("Pipeline.metadata.namespace = %v, want %s", got, namespace)
			}
		case "PipelineRun":
			seenPipelineRun = true
			spec, _ := raw["spec"].(map[string]any)
			if spec == nil {
				t.Fatal("PipelineRun.spec missing after rewrite")
			}
			if got := spec["runId"]; got != runID.String() {
				t.Errorf("spec.runId = %v, want %s", got, runID)
			}
			ref, _ := spec["runTokenRef"].(map[string]any)
			if ref == nil {
				t.Fatal("spec.runTokenRef missing")
			}
			if got := ref["name"]; got != secretName {
				t.Errorf("spec.runTokenRef.name = %v, want %s", got, secretName)
			}
			// Namespace should be rewritten to the per-project ns.
			md, _ := raw["metadata"].(map[string]any)
			if got := md["namespace"]; got != namespace {
				t.Errorf("PipelineRun.metadata.namespace = %v, want %s", got, namespace)
			}
			// Preserved fields.
			if _, ok := spec["pipelineRef"]; !ok {
				t.Error("spec.pipelineRef lost during rewrite")
			}
		}
	}
	if !seenPipeline {
		t.Error("Pipeline doc lost during rewrite")
	}
	if !seenPipelineRun {
		t.Error("PipelineRun doc lost during rewrite")
	}

	// Additional sanity: the rewritten YAML must still `apiVersion: datuplet.io/v1` — catches cases
	// where the yaml.v3 round-trip might drop scalar tags.
	if !strings.Contains(string(out), "apiVersion: datuplet.io/v1") {
		t.Error("apiVersion not preserved round-trip:\n" + string(out))
	}
}
