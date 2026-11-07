package k8s_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func TestReconcileOne_PreviewRouting(t *testing.T) {
	id := uuid.New()
	mk := func(name, label string) *datupletv1.PipelineRun {
		return &datupletv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "ns",
				Labels:          map[string]string{label: id.String()},
				ResourceVersion: "7",
			},
			Status: datupletv1.PipelineRunStatus{Phase: "Running"},
		}
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).
		WithObjects(mk("preview-"+id.String(), "datuplet.io/preview-id"), mk("etl-run", "datuplet.io/run-id")).Build()

	runs, previews := &mockUpdater{}, &mockUpdater{}
	u := &pkg8s.RoutingUpdater{Runs: runs, Previews: previews}
	reconcileFor(t, c, u, "ns", "preview-"+id.String())
	if len(previews.calls) != 1 || len(runs.calls) != 0 {
		t.Fatalf("preview routed wrong: previews=%d runs=%d", len(previews.calls), len(runs.calls))
	}
	if got := previews.calls[0]; got.PreviewID != id || got.RunID != id {
		t.Errorf("status ids = %+v", got)
	}
	reconcileFor(t, c, u, "ns", "etl-run")
	if len(previews.calls) != 1 || len(runs.calls) != 1 || runs.calls[0].PreviewID != uuid.Nil {
		t.Fatalf("run routed wrong: previews=%d runs=%d", len(previews.calls), len(runs.calls))
	}
}

func TestDBPreviewUpdater_RejectsDecoyName(t *testing.T) {
	pool, cleanup := testReapDB(t)
	defer cleanup()
	ctx := context.Background()
	proj, err := store.CreateProject(ctx, pool, "pv-project")
	if err != nil {
		t.Fatal(err)
	}
	p := &store.Preview{
		ID: uuid.New(), ProjectID: proj.ID, Workspace: "default", Doc: []byte(`{}`),
		RowLimit: 10, TimeoutSeconds: 60, TTLSeconds: 60, K8sNamespace: "proj-ns",
	}
	if err := store.InsertPreview(ctx, pool, p, store.PreviewCaps{}); err != nil {
		t.Fatal(err)
	}
	u := pkg8s.NewDBPreviewUpdater(pool)
	base := pkg8s.RunStatus{
		RunID: p.ID, PreviewID: p.ID, Namespace: "proj-ns",
		PipelineRunName: "preview-" + p.ID.String(), Phase: "Running", ResourceVersion: 5,
	}

	decoyName := base
	decoyName.PipelineRunName = "preview-decoy"
	decoyNS := base
	decoyNS.Namespace = "other-ns"
	for name, s := range map[string]pkg8s.RunStatus{"name": decoyName, "namespace": decoyNS} {
		if applied, err := u.Update(ctx, s); err != nil || applied {
			t.Errorf("decoy %s: applied=%v err=%v, want false,nil", name, applied, err)
		}
	}
	if got, _ := store.GetPreviewByID(ctx, pool, p.ID); got.Phase != "Pending" {
		t.Errorf("decoy mutated row: phase=%q", got.Phase)
	}
	if applied, err := u.Update(ctx, base); err != nil || !applied {
		t.Fatalf("genuine: applied=%v err=%v", applied, err)
	}
	if got, _ := store.GetPreviewByID(ctx, pool, p.ID); got.Phase != "Running" {
		t.Errorf("phase=%q, want Running", got.Phase)
	}
}
