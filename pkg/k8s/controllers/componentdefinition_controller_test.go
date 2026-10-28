package controllers

// RFC 026 Task R8: registration-time validation of ComponentDefinition CRs.
// A valid definition reconciles to status.phase=Valid; each structural defect
// below must surface as status.phase=Invalid with a message identifying the
// specific cause, so a component author never has to guess why registration
// failed.

import (
	"context"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// validComponentDefinition returns a minimal ComponentDefinition that passes
// every registration-time check; each test mutates a copy to introduce
// exactly one defect.
func validComponentDefinition(name string) *datupletv1.ComponentDefinition {
	return &datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 3},
		Spec: datupletv1.ComponentDefinitionSpec{
			Versions: []datupletv1.VersionSpec{
				{Version: "v1.0.0", Image: "img:v1.0.0"},
			},
		},
	}
}

// reconcileComponentDefinition runs the reconciler once against def on a fake
// client and returns the persisted object.
func reconcileComponentDefinition(t *testing.T, def *datupletv1.ComponentDefinition) *datupletv1.ComponentDefinition {
	t.Helper()
	scheme := newTestScheme(t)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(def).
		WithStatusSubresource(def).
		Build()

	r := &ComponentDefinitionReconciler{Client: fakeClient, Scheme: scheme}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: def.Name},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &datupletv1.ComponentDefinition{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: def.Name}, got); err != nil {
		t.Fatalf("Get ComponentDefinition: %v", err)
	}
	return got
}

// assertInvalid fails the test unless got is Invalid with a message
// containing wantSubstr.
func assertInvalid(t *testing.T, got *datupletv1.ComponentDefinition, wantSubstr string) {
	t.Helper()
	if got.Status.Phase != "Invalid" {
		t.Fatalf("phase = %q, want Invalid (message: %q)", got.Status.Phase, got.Status.Message)
	}
	if !strings.Contains(got.Status.Message, wantSubstr) {
		t.Errorf("message = %q, want it to contain %q", got.Status.Message, wantSubstr)
	}
}
func TestComponentDefinitionReconcile(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(*datupletv1.ComponentDefinition)
		wantSubstr string // empty: the definition must reconcile to Valid
	}{
		{name: "valid sets Valid phase", mutate: func(*datupletv1.ComponentDefinition) {}},
		{
			name: "config schema does not compile",
			mutate: func(d *datupletv1.ComponentDefinition) {
				d.Spec.Versions[0].ConfigSchema = "{ this is not json"
			},
			wantSubstr: "v1.0.0",
		},
		{
			name: "stable version bad format",
			mutate: func(d *datupletv1.ComponentDefinition) {
				d.Spec.Versions[0].Version = "1.0" // missing leading "v" and patch
			},
			wantSubstr: "1.0",
		},
		{
			name: "duplicate versions",
			mutate: func(d *datupletv1.ComponentDefinition) {
				d.Spec.Versions = append(d.Spec.Versions, datupletv1.VersionSpec{
					Version: "v1.0.0",
					Image:   "img:v1.0.0-other",
				})
			},
			wantSubstr: "v1.0.0",
		},
		{
			name: "default version not registered",
			mutate: func(d *datupletv1.ComponentDefinition) {
				d.Spec.DefaultVersion = "v9.9.9"
			},
			wantSubstr: "v9.9.9",
		},
		{
			name:       "image missing tag",
			mutate:     func(d *datupletv1.ComponentDefinition) { d.Spec.Versions[0].Image = "img" },
			wantSubstr: "img",
		},
		{
			name:       "image latest tag",
			mutate:     func(d *datupletv1.ComponentDefinition) { d.Spec.Versions[0].Image = "img:latest" },
			wantSubstr: "img:latest",
		},
		{
			name: "image registry port no tag",
			mutate: func(d *datupletv1.ComponentDefinition) {
				d.Spec.Versions[0].Image = "registry.example.com:5000/component"
			},
			wantSubstr: "registry.example.com:5000/component",
		},
		{
			name: "image registry port with tag is valid",
			mutate: func(d *datupletv1.ComponentDefinition) {
				d.Spec.Versions[0].Image = "registry.example.com:5000/component:v1.2.3"
			},
		},
		{
			name: "prerelease skips version and image checks",
			mutate: func(d *datupletv1.ComponentDefinition) {
				d.Spec.Versions[0].Version = "dev"
				d.Spec.Versions[0].Image = "img:latest"
				d.Spec.Versions[0].Prerelease = true
			},
		},
		{
			name: "resources default exceeds max",
			mutate: func(d *datupletv1.ComponentDefinition) {
				d.Spec.Versions[0].Resources = &datupletv1.ComponentResources{
					Default: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
					},
					Max: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
				}
			},
			wantSubstr: string(corev1.ResourceCPU),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := validComponentDefinition("comp-a")
			tc.mutate(def)
			got := reconcileComponentDefinition(t, def)
			if tc.wantSubstr != "" {
				assertInvalid(t, got, tc.wantSubstr)
				return
			}
			if got.Status.Phase != "Valid" {
				t.Fatalf("phase = %q, want Valid (message: %q)", got.Status.Phase, got.Status.Message)
			}
			if got.Status.ObservedGeneration != def.Generation {
				t.Fatalf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, def.Generation)
			}
		})
	}
}
