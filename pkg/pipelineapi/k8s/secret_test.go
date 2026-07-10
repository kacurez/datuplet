package k8s_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

func TestEnsureProjectSecret_CreatesIfMissing(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	const ns = "datuplet-test-project"

	if err := pkg8s.EnsureProjectSecret(context.Background(), c, ns); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	sec := &corev1.Secret{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pkg8s.ProjectSecretsName, Namespace: ns}, sec); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(sec.Data) != 0 {
		t.Errorf("Data = %v, want empty", sec.Data)
	}
}

func TestEnsureProjectSecret_Idempotent(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	const ns = "datuplet-test-project"

	if err := pkg8s.EnsureProjectSecret(context.Background(), c, ns); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Seed a key so we can prove the second Ensure call is a no-op Get
	// (not a re-Create that would clobber existing data).
	sec := &corev1.Secret{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pkg8s.ProjectSecretsName, Namespace: ns}, sec); err != nil {
		t.Fatalf("get after first: %v", err)
	}
	sec.Data = map[string][]byte{"api_token": []byte("shh")}
	if err := c.Update(context.Background(), sec); err != nil {
		t.Fatalf("seed data: %v", err)
	}

	if err := pkg8s.EnsureProjectSecret(context.Background(), c, ns); err != nil {
		t.Fatalf("second: %v", err)
	}

	got := &corev1.Secret{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pkg8s.ProjectSecretsName, Namespace: ns}, got); err != nil {
		t.Fatalf("get after second: %v", err)
	}
	if string(got.Data["api_token"]) != "shh" {
		t.Errorf("data was clobbered: %v", got.Data)
	}
}
