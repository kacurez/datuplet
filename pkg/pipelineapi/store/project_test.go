package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func TestCreateProject_SetsNamespaceConvention(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()

	p, err := store.CreateProject(context.Background(), pool, "acme")
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	want := "datuplet-" + p.ID.String()
	if p.K8sNamespace != want {
		t.Errorf("K8sNamespace = %q, want %q", p.K8sNamespace, want)
	}
}

func TestCreateProject_Duplicate(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()

	if _, err := store.CreateProject(context.Background(), pool, "acme"); err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err := store.CreateProject(context.Background(), pool, "acme")
	if !errors.Is(err, store.ErrProjectAlreadyExists) {
		t.Errorf("expected ErrProjectAlreadyExists, got: %v", err)
	}
}

func TestGetProjectByID_Missing(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()

	_, err := store.GetProjectByID(context.Background(), pool, uuid.MustParse("00000000-0000-0000-0000-000000000000"))
	if !errors.Is(err, store.ErrProjectNotFound) {
		t.Errorf("expected ErrProjectNotFound, got: %v", err)
	}
}

