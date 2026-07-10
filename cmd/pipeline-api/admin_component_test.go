package main

import (
	"bytes"
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

const testComponentYAML = `apiVersion: datuplet.io/v1
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

func TestApplyComponentDefinition_CreatesWhenMissing(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()

	def, err := applyComponentDefinition(context.Background(), c, []byte(testComponentYAML))
	if err != nil {
		t.Fatalf("applyComponentDefinition: %v", err)
	}
	if def.Name != "http-fetch" {
		t.Fatalf("name = %q, want http-fetch", def.Name)
	}

	got := &datupletv1.ComponentDefinition{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "http-fetch"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Spec.Versions) != 1 || got.Spec.Versions[0].Version != "v1.0.0" {
		t.Fatalf("unexpected spec: %+v", got.Spec)
	}
}

func TestApplyComponentDefinition_UpdatesWhenPresent(t *testing.T) {
	existing := &datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "http-fetch"},
		Spec: datupletv1.ComponentDefinitionSpec{
			DisplayName: "stale",
			Versions:    []datupletv1.VersionSpec{{Version: "v0.1.0", Image: "datuplet/http-fetch:v0.1.0"}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(existing).Build()

	def, err := applyComponentDefinition(context.Background(), c, []byte(testComponentYAML))
	if err != nil {
		t.Fatalf("applyComponentDefinition: %v", err)
	}
	if def.Spec.DisplayName != "HTTP Fetch" {
		t.Fatalf("DisplayName = %q, want the re-applied value", def.Spec.DisplayName)
	}

	got := &datupletv1.ComponentDefinition{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "http-fetch"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Spec.Versions) != 1 || got.Spec.Versions[0].Version != "v1.0.0" {
		t.Fatalf("update did not take effect: %+v", got.Spec)
	}
}

func TestApplyComponentDefinition_RejectsMissingName(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	_, err := applyComponentDefinition(context.Background(), c, []byte("apiVersion: datuplet.io/v1\nkind: ComponentDefinition\n"))
	if err == nil {
		t.Fatal("expected an error for a manifest with no metadata.name")
	}
}

func TestListComponentDefinitions_EmptyAndPopulated(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()

	var buf bytes.Buffer
	if err := listComponentDefinitions(context.Background(), c, &buf); err != nil {
		t.Fatalf("list (empty): %v", err)
	}
	if buf.String() == "" {
		t.Fatal("expected a 'no components' message for an empty registry")
	}

	def := &datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "http-fetch"},
		Spec: datupletv1.ComponentDefinitionSpec{
			DefaultVersion: "v1.0.0",
			Versions:       []datupletv1.VersionSpec{{Version: "v1.0.0", Image: "datuplet/http-fetch:v1.0.0"}},
		},
		Status: datupletv1.ComponentDefinitionStatus{Phase: "Valid"},
	}
	if err := c.Create(context.Background(), def); err != nil {
		t.Fatalf("seed create: %v", err)
	}

	buf.Reset()
	if err := listComponentDefinitions(context.Background(), c, &buf); err != nil {
		t.Fatalf("list (populated): %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("http-fetch")) {
		t.Fatalf("output missing component name: %q", buf.String())
	}
	if !bytes.Contains(buf.Bytes(), []byte("phase=Valid")) {
		t.Fatalf("output missing phase: %q", buf.String())
	}
}

func TestDeprecateComponentDefinition(t *testing.T) {
	def := &datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "http-fetch"},
		Spec: datupletv1.ComponentDefinitionSpec{
			Versions: []datupletv1.VersionSpec{{Version: "v1.0.0", Image: "datuplet/http-fetch:v1.0.0"}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(def).Build()

	var buf bytes.Buffer
	if err := deprecateComponentDefinition(context.Background(), c, "http-fetch", &buf); err != nil {
		t.Fatalf("deprecate: %v", err)
	}

	got := &datupletv1.ComponentDefinition{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "http-fetch"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Spec.Deprecated {
		t.Fatal("expected Spec.Deprecated = true")
	}

	// Re-running is a no-op (already-deprecated message, not an error).
	buf.Reset()
	if err := deprecateComponentDefinition(context.Background(), c, "http-fetch", &buf); err != nil {
		t.Fatalf("re-deprecate: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("already deprecated")) {
		t.Fatalf("expected an 'already deprecated' message, got %q", buf.String())
	}
}

func TestDeprecateComponentDefinition_NotFound(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	var buf bytes.Buffer
	if err := deprecateComponentDefinition(context.Background(), c, "does-not-exist", &buf); err == nil {
		t.Fatal("expected an error for an unknown component")
	}
}
