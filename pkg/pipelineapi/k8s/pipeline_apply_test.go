package k8s_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

// testPipelineDoc is envelope-free PipelineDoc content (RFC 027 §3); ApplyPipelineCRD
// renders it into a datupletv1.Pipeline via config.DocToCR.
const testPipelineDoc = `{
  "name": "etl",
  "stages": [
    {
      "name": "extract",
      "components": [
        {"name": "c1", "component": "datuplet/test:latest", "outputs": {"defaultBucket": "raw"}}
      ]
    }
  ]
}`

func mustParseDoc(t *testing.T, raw string) *config.Pipeline {
	t.Helper()
	doc, err := config.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("parse doc: %v", err)
	}
	return doc
}

func TestApplyPipelineCRD_CreatesWhenMissing(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	pid := uuid.New()
	ns := pkg8s.NamespaceForProject(pid)

	if err := pkg8s.ApplyPipelineCRD(context.Background(), c, ns, mustParseDoc(t, testPipelineDoc)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got := &datupletv1.Pipeline{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "etl", Namespace: ns}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Spec.Stages) != 1 || got.Spec.Stages[0].Name != "extract" {
		t.Errorf("spec not applied: %+v", got.Spec)
	}
}

func TestApplyPipelineCRD_UpdatesWhenPresent(t *testing.T) {
	pid := uuid.New()
	ns := pkg8s.NamespaceForProject(pid)

	// Pre-seed an older version.
	existing := &datupletv1.Pipeline{}
	existing.Name = "etl"
	existing.Namespace = ns
	existing.Spec.Stages = []datupletv1.StageSpec{{Name: "old", Components: []datupletv1.ComponentSpec{{Name: "c", Component: "old"}}}}

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(existing).Build()
	if err := pkg8s.ApplyPipelineCRD(context.Background(), c, ns, mustParseDoc(t, testPipelineDoc)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got := &datupletv1.Pipeline{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "etl", Namespace: ns}, got)
	if got.Spec.Stages[0].Name != "extract" {
		t.Errorf("update didn't replace spec: %+v", got.Spec)
	}
}

func TestApplyPipelineCRD_RejectsNilDoc(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	if err := pkg8s.ApplyPipelineCRD(context.Background(), c, "ns", nil); err == nil {
		t.Error("expected error for nil doc")
	}
}

func TestApplyPipelineCRD_RejectsUnnamedDoc(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	if err := pkg8s.ApplyPipelineCRD(context.Background(), c, "ns", &config.Pipeline{}); err == nil {
		t.Error("expected error for doc with no name")
	}
}
