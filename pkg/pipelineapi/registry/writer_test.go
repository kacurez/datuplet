package registry

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

const putYAML = `apiVersion: datuplet.io/v1
kind: ComponentDefinition
metadata:
  name: http-fetch
spec:
  displayName: HTTP Fetch
  defaultVersion: v1.0.0
  versions:
    - version: v1.0.0
      image: datuplet/http-fetch:v1.0.0
`

func TestWriterPut_CreatesWhenMissing(t *testing.T) {
	c := newFakeClient()
	wr := NewWriter(c)

	if err := wr.Put(context.Background(), "http-fetch", []byte(putYAML)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got := &datupletv1.ComponentDefinition{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "http-fetch"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Spec.Versions) != 1 || got.Spec.Versions[0].Version != "v1.0.0" {
		t.Fatalf("unexpected spec: %+v", got.Spec)
	}
}

func TestWriterPut_UpdatesWhenPresent(t *testing.T) {
	existing := componentDef("http-fetch", "v0.1.0", "")
	existing.Spec.DisplayName = "stale"
	c := newFakeClient(existing)
	wr := NewWriter(c)

	if err := wr.Put(context.Background(), "http-fetch", []byte(putYAML)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got := &datupletv1.ComponentDefinition{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "http-fetch"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.DisplayName != "HTTP Fetch" {
		t.Fatalf("DisplayName = %q, want the re-applied value", got.Spec.DisplayName)
	}
	if len(got.Spec.Versions) != 1 || got.Spec.Versions[0].Version != "v1.0.0" {
		t.Fatalf("update did not take effect: %+v", got.Spec)
	}
}

func TestWriterPut_RejectsNameMismatch(t *testing.T) {
	c := newFakeClient()
	wr := NewWriter(c)

	err := wr.Put(context.Background(), "different-name", []byte(putYAML))
	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("err = %v, want ErrInvalidDefinition", err)
	}
}

func TestWriterPut_RejectsMissingName(t *testing.T) {
	c := newFakeClient()
	wr := NewWriter(c)

	err := wr.Put(context.Background(), "", []byte("apiVersion: datuplet.io/v1\nkind: ComponentDefinition\n"))
	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("err = %v, want ErrInvalidDefinition", err)
	}
}

func TestWriterPut_RejectsUnparseableYAML(t *testing.T) {
	c := newFakeClient()
	wr := NewWriter(c)

	err := wr.Put(context.Background(), "http-fetch", []byte("\tnot: [valid"))
	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("err = %v, want ErrInvalidDefinition", err)
	}
}

func TestWriterDelete_RemovesExisting(t *testing.T) {
	c := newFakeClient(componentDef("http-fetch", "v1.0.0", ""))
	wr := NewWriter(c)

	if err := wr.Delete(context.Background(), "http-fetch"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := &datupletv1.ComponentDefinition{}
	err := c.Get(context.Background(), types.NamespacedName{Name: "http-fetch"}, got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Get after delete: err = %v, want NotFound", err)
	}
}

func TestWriterDelete_NotFound(t *testing.T) {
	c := newFakeClient()
	wr := NewWriter(c)

	err := wr.Delete(context.Background(), "does-not-exist")
	if !apierrors.IsNotFound(err) {
		t.Fatalf("err = %v, want an apierrors.IsNotFound error the handler maps to 404", err)
	}
}
