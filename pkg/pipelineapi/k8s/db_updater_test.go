package k8s_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func TestDBRunUpdater_RejectsDecoyName(t *testing.T) {
	pool, done := testReapDB(t)
	defer done()
	ctx := context.Background()

	projectID, pipelineID := seedProjectPipeline(t, pool)
	runID := uuid.New()
	if _, err := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: runID, ProjectID: projectID, PipelineID: pipelineID,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	proj, err := store.GetProjectByID(ctx, pool, projectID)
	if err != nil {
		t.Fatal(err)
	}

	u := pkg8s.NewDBRunUpdater(pool)
	base := pkg8s.RunStatus{
		RunID: runID, Namespace: proj.K8sNamespace,
		PipelineRunName: pkg8s.PipelineRunName(runID), Phase: "Running", ResourceVersion: 5,
	}
	decoyName := base
	decoyName.PipelineRunName = "p-" + runID.String()
	decoyNS := base
	decoyNS.Namespace = "other-ns"
	for name, s := range map[string]pkg8s.RunStatus{"name": decoyName, "namespace": decoyNS} {
		if applied, err := u.Update(ctx, s); err != nil || applied {
			t.Errorf("decoy %s: applied=%v err=%v, want false,nil", name, applied, err)
		}
	}
	if applied, err := u.Update(ctx, base); err != nil || !applied {
		t.Fatalf("genuine: applied=%v err=%v", applied, err)
	}
}
