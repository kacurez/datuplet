package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

const minimalYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: etl
spec:
  stages: []
`

func TestCreatePipeline_Duplicate(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	if _, err := store.CreatePipeline(ctx, pool, p.ID, "etl", minimalYAML); err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err := store.CreatePipeline(ctx, pool, p.ID, "etl", minimalYAML)
	if !errors.Is(err, store.ErrPipelineAlreadyExists) {
		t.Errorf("expected ErrPipelineAlreadyExists, got: %v", err)
	}
}

func TestGetPipelineByName_OK(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	created, _ := store.CreatePipeline(ctx, pool, p.ID, "etl", minimalYAML)

	got, err := store.GetPipelineByName(ctx, pool, p.ID, "etl")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != created.ID || got.Name != "etl" || got.YAML != minimalYAML {
		t.Errorf("mismatch: %+v", got)
	}
}

func TestGetPipelineByName_Missing(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	if _, err := store.GetPipelineByName(ctx, pool, p.ID, "nope"); !errors.Is(err, store.ErrPipelineNotFound) {
		t.Errorf("expected ErrPipelineNotFound, got %v", err)
	}
}

func TestListPipelines(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "b", minimalYAML)
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "a", minimalYAML)
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "c", minimalYAML)

	got, err := store.ListPipelines(ctx, pool, p.ID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Name != "a" || got[1].Name != "b" || got[2].Name != "c" {
		t.Errorf("not sorted by name: %v", []string{got[0].Name, got[1].Name, got[2].Name})
	}
}

func TestUpdatePipelineYAML(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "etl", minimalYAML)

	newYAML := minimalYAML + "# updated\n"
	if err := store.UpdatePipelineYAML(ctx, pool, p.ID, "etl", newYAML); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ := store.GetPipelineByName(ctx, pool, p.ID, "etl")
	if got.YAML != newYAML {
		t.Errorf("YAML not updated")
	}
}

func TestDeletePipeline(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "etl", minimalYAML)
	if err := store.DeletePipeline(ctx, pool, p.ID, "etl"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.GetPipelineByName(ctx, pool, p.ID, "etl"); !errors.Is(err, store.ErrPipelineNotFound) {
		t.Errorf("expected ErrPipelineNotFound after delete, got %v", err)
	}
}
