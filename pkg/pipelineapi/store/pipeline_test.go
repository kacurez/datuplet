package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// minimalDoc is a minimal envelope-free PipelineDoc, JSON-encoded (the
// canonical wire/storage shape post RFC 027 — JSONB requires valid JSON,
// unlike the old free-form YAML column).
var minimalDoc = []byte(`{"name":"x","stages":[]}`)

// docEqual compares two JSON byte slices by value rather than by exact
// bytes: JSONB round-trips through Postgres re-serialize and may reorder
// object keys, so a byte-for-byte comparison would be flaky.
func docEqual(t *testing.T, got, want []byte) bool {
	t.Helper()
	var gotVal, wantVal any
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("unmarshal got doc: %v", err)
	}
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("unmarshal want doc: %v", err)
	}
	return reflect.DeepEqual(gotVal, wantVal)
}

func TestCreatePipeline_Duplicate(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	if _, err := store.CreatePipeline(ctx, pool, p.ID, "etl", "d", minimalDoc); err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err := store.CreatePipeline(ctx, pool, p.ID, "etl", "d", minimalDoc)
	if !errors.Is(err, store.ErrPipelineAlreadyExists) {
		t.Errorf("expected ErrPipelineAlreadyExists, got: %v", err)
	}
}

func TestGetPipelineByName_OK(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	created, _ := store.CreatePipeline(ctx, pool, p.ID, "etl", "d", minimalDoc)

	got, err := store.GetPipelineByName(ctx, pool, p.ID, "etl")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != created.ID || got.Name != "etl" || got.Description != "d" {
		t.Errorf("mismatch: %+v", got)
	}
	if !docEqual(t, got.Doc, minimalDoc) {
		t.Errorf("Doc mismatch: got %s, want %s", got.Doc, minimalDoc)
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
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "b", "d", minimalDoc)
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "a", "d", minimalDoc)
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "c", "d", minimalDoc)

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
	for _, pp := range got {
		if pp.Description != "d" {
			t.Errorf("description = %q, want %q", pp.Description, "d")
		}
	}
}

func TestUpdatePipeline(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "etl", "d", minimalDoc)

	newDoc := []byte(`{"name":"x","stages":[],"description":"updated"}`)
	if err := store.UpdatePipeline(ctx, pool, p.ID, "etl", "d2", newDoc); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ := store.GetPipelineByName(ctx, pool, p.ID, "etl")
	if got.Description != "d2" {
		t.Errorf("Description not updated: got %q", got.Description)
	}
	if !docEqual(t, got.Doc, newDoc) {
		t.Errorf("Doc not updated: got %s, want %s", got.Doc, newDoc)
	}
}

func TestGetDocByID(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	created, _ := store.CreatePipeline(ctx, pool, p.ID, "etl", "d", minimalDoc)

	doc, err := store.GetDocByID(ctx, pool, created.ID)
	if err != nil {
		t.Fatalf("GetDocByID: %v", err)
	}
	if !docEqual(t, doc, minimalDoc) {
		t.Errorf("Doc mismatch: got %s, want %s", doc, minimalDoc)
	}
}

func TestDeletePipeline(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	p, _ := store.CreateProject(ctx, pool, "proj")
	_, _ = store.CreatePipeline(ctx, pool, p.ID, "etl", "d", minimalDoc)
	if err := store.DeletePipeline(ctx, pool, p.ID, "etl"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.GetPipelineByName(ctx, pool, p.ID, "etl"); !errors.Is(err, store.ErrPipelineNotFound) {
		t.Errorf("expected ErrPipelineNotFound after delete, got %v", err)
	}
}
