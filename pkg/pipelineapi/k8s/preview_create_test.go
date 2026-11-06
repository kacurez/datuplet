package k8s_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

func TestCreatePreviewResources_Labels(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	id := uuid.New()
	const ns = "datuplet-proj"
	spec := datupletv1.PipelineSpec{Stages: []datupletv1.StageSpec{{Name: "extract"}}}
	pv := datupletv1.PreviewSpec{ID: id.String(), RowLimit: 10, TimeoutSeconds: 60}

	if err := pkg8s.CreatePreviewResources(ctx, c, pkg8s.CreatePreviewOpts{
		Namespace: ns, PreviewID: id, Token: "tok", Spec: spec, Preview: pv,
	}); err != nil {
		t.Fatalf("CreatePreviewResources: %v", err)
	}

	name := pkg8s.PreviewPipelineRunName(id)
	if name != "preview-"+id.String() {
		t.Fatalf("PipelineRun name = %q", name)
	}
	pr := &datupletv1.PipelineRun{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, pr); err != nil {
		t.Fatalf("get PipelineRun: %v", err)
	}
	if got := pr.Labels["datuplet.io/preview-id"]; got != id.String() {
		t.Errorf("preview-id label = %q", got)
	}
	if _, ok := pr.Labels["datuplet.io/run-id"]; ok {
		t.Errorf("PipelineRun must not carry datuplet.io/run-id: %v", pr.Labels)
	}
	if pr.Spec.RunID != id.String() {
		t.Errorf("Spec.RunID = %q, want %s", pr.Spec.RunID, id)
	}
	if pr.Spec.RunTokenRef == nil || pr.Spec.RunTokenRef.Name != "runtoken-"+id.String() {
		t.Errorf("RunTokenRef = %+v", pr.Spec.RunTokenRef)
	}
	if pr.Spec.PipelineSpec == nil || !reflect.DeepEqual(*pr.Spec.PipelineSpec, spec) {
		t.Errorf("PipelineSpec = %+v", pr.Spec.PipelineSpec)
	}
	if pr.Spec.Preview == nil || *pr.Spec.Preview != pv {
		t.Errorf("Preview = %+v", pr.Spec.Preview)
	}

	sec := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: "runtoken-" + id.String(), Namespace: ns}, sec); err != nil {
		t.Fatalf("get Secret: %v", err)
	}
	if got := sec.Labels["datuplet.io/preview-id"]; got != id.String() {
		t.Errorf("Secret preview-id label = %q", got)
	}
	if string(sec.Data[pkg8s.RunTokenSecretKey]) != "tok" {
		t.Errorf("Secret token = %q", sec.Data[pkg8s.RunTokenSecretKey])
	}
	if len(sec.OwnerReferences) != 1 || sec.OwnerReferences[0].Kind != "PipelineRun" || sec.OwnerReferences[0].Name != name {
		t.Errorf("Secret owner refs = %+v", sec.OwnerReferences)
	}
}

func TestEnsureSecretsRBAC_PodLogsRole(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "datuplet-install")
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	projectID := uuid.New()
	if err := pkg8s.EnsureProjectNamespace(context.Background(), c, projectID); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	ns := "datuplet-" + projectID.String()

	role := getRole(t, c, ns, pkg8s.PodLogsRoleName)
	if pkg8s.PodLogsRoleName != "datuplet-pod-logs" {
		t.Fatalf("PodLogsRoleName = %q", pkg8s.PodLogsRoleName)
	}
	if len(role.Rules) != 2 {
		t.Fatalf("want 2 rules, got %+v", role.Rules)
	}
	want := map[string][]string{"pods": {"get", "list"}, "pods/log": {"get"}}
	for _, r := range role.Rules {
		if len(r.Resources) != 1 || !reflect.DeepEqual(r.APIGroups, []string{""}) {
			t.Fatalf("unexpected rule %+v", r)
		}
		if !reflect.DeepEqual(r.Verbs, want[r.Resources[0]]) {
			t.Errorf("%s verbs = %v, want %v", r.Resources[0], r.Verbs, want[r.Resources[0]])
		}
	}

	rb := getRoleBinding(t, c, ns, pkg8s.PodLogsRoleName)
	if rb.RoleRef.Kind != "Role" || rb.RoleRef.Name != pkg8s.PodLogsRoleName {
		t.Errorf("roleRef = %+v", rb.RoleRef)
	}
	if len(rb.Subjects) != 1 || rb.Subjects[0].Name != "pipeline-api" || rb.Subjects[0].Namespace != "datuplet-install" {
		t.Errorf("subjects = %+v", rb.Subjects)
	}
}
