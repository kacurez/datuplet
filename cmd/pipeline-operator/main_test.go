package main

import (
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestLoadRuntimeTolerationsEmptyEnv(t *testing.T) {
	os.Unsetenv("DATUPLET_RUN_TOLERATIONS_JSON")
	got, err := loadRuntimeTolerations()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("got %v, want nil", got)
	}
}

func TestLoadRuntimeTolerationsHappy(t *testing.T) {
	t.Setenv("DATUPLET_RUN_TOLERATIONS_JSON",
		`[{"key":"kubernetes.io/arch","operator":"Equal","value":"arm64","effect":"NoSchedule"}]`)
	got, err := loadRuntimeTolerations()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	if got[0].Key != "kubernetes.io/arch" {
		t.Fatalf("key = %q", got[0].Key)
	}
}

func TestLoadRuntimeTolerationsInvalidJSON(t *testing.T) {
	t.Setenv("DATUPLET_RUN_TOLERATIONS_JSON", `not-json`)
	_, err := loadRuntimeTolerations()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadRuntimeTolerationsRejectsUnknownField(t *testing.T) {
	t.Setenv("DATUPLET_RUN_TOLERATIONS_JSON",
		`[{"key":"k","Effekt":"NoSchedule"}]`) // typo: Effekt
	_, err := loadRuntimeTolerations()
	if err == nil {
		t.Fatal("expected unknown-field error")
	}
}

func TestValidateTolerationEachBranch(t *testing.T) {
	cases := []struct {
		name string
		tol  corev1.Toleration
		ok   bool
	}{
		{"OK Equal", corev1.Toleration{Key: "k", Operator: "Equal", Value: "v", Effect: "NoSchedule"}, true},
		{"OK Exists", corev1.Toleration{Key: "k", Operator: "Exists", Effect: "NoSchedule"}, true},
		{"bad operator", corev1.Toleration{Operator: "Nope"}, false},
		{"bad effect", corev1.Toleration{Operator: "Exists", Effect: "Bogus"}, false},
		{"Exists with value", corev1.Toleration{Operator: "Exists", Value: "x"}, false},
		{"Equal with empty key", corev1.Toleration{Operator: "Equal", Key: ""}, false},
		{"tolerationSeconds with wrong effect", corev1.Toleration{Operator: "Exists", Effect: "NoSchedule", TolerationSeconds: int64Ptr(60)}, false},
	}
	for _, c := range cases {
		err := validateToleration(c.tol)
		if c.ok && err != nil {
			t.Errorf("%s: unexpected error %v", c.name, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%s: expected error", c.name)
		}
	}
}

func int64Ptr(v int64) *int64 { return &v }
