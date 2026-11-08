package k8s_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

func TestCreateRunResources_CreatesSecretAndPipelineRun(t *testing.T) {
	pid := uuid.New()
	ns := pkg8s.NamespaceForProject(pid)
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()

	runID := uuid.New()
	opts := pkg8s.CreateRunOpts{
		Namespace:    ns,
		PipelineName: "etl",
		RunID:        runID,
		Token:        "eyJ-the-single-run-token",
	}
	if err := pkg8s.CreateRunResources(context.Background(), c, opts); err != nil {
		t.Fatalf("CreateRunResources: %v", err)
	}

	pr := &datupletv1.PipelineRun{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pkg8s.PipelineRunName(runID), Namespace: ns}, pr); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}
	if pr.Spec.PipelineRef.Name != "etl" || pr.Spec.RunID != runID.String() {
		t.Errorf("unexpected PipelineRun spec: %+v", pr.Spec)
	}
	if pr.Spec.RunTokenRef == nil || pr.Spec.RunTokenRef.Name != opts.SecretName() {
		t.Errorf("PipelineRun is missing RunTokenRef pointing at %s (got %+v)", opts.SecretName(), pr.Spec.RunTokenRef)
	}

	// Secret has the single per-run token under the singular `token` key.
	sec := &corev1.Secret{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: opts.SecretName(), Namespace: ns}, sec); err != nil {
		t.Fatalf("Get Secret: %v", err)
	}
	raw, ok := sec.Data[pkg8s.RunTokenSecretKey]
	if !ok {
		t.Fatalf("Secret missing %q key; have %v", pkg8s.RunTokenSecretKey, keysOf(sec.Data))
	}
	if string(raw) != "eyJ-the-single-run-token" {
		t.Errorf("Secret.token = %q, want eyJ-the-single-run-token", string(raw))
	}
	if len(sec.OwnerReferences) != 1 || sec.OwnerReferences[0].Kind != "PipelineRun" {
		t.Errorf("Secret missing owner reference to PipelineRun: %+v", sec.OwnerReferences)
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestPipelineRunName_IgnoresPipelineName(t *testing.T) {
	id := uuid.New()
	got := pkg8s.PipelineRunName(id)
	if got != "run-"+id.String() {
		t.Errorf("PipelineRunName = %q, want run-%s", got, id)
	}
	if errs := validation.IsDNS1123Label(got); len(errs) != 0 {
		t.Errorf("not a DNS-1123 label: %v", errs)
	}
}

func TestCreateRunResources_LongPipelineNameInAnnotation(t *testing.T) {
	ns := pkg8s.NamespaceForProject(uuid.New())
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	runID := uuid.New()
	long := strings.Repeat("a", 60) + ".b." + strings.Repeat("c", 100)
	opts := pkg8s.CreateRunOpts{Namespace: ns, PipelineName: long, RunID: runID, Token: "t"}
	if err := pkg8s.CreateRunResources(context.Background(), c, opts); err != nil {
		t.Fatalf("CreateRunResources: %v", err)
	}
	pr := &datupletv1.PipelineRun{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "run-" + runID.String(), Namespace: ns}, pr); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}
	if got := pr.Annotations["datuplet.io/pipeline-name"]; got != long {
		t.Errorf("annotation = %q, want full pipeline name", got)
	}
	if pr.Spec.PipelineRef.Name != long {
		t.Errorf("pipelineRef.name = %q, want full pipeline name", pr.Spec.PipelineRef.Name)
	}
}
