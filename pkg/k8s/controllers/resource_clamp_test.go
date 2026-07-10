package controllers

// RFC 026 P3 (Task T7): effective resources at admission — registry Default
// when the spec is nil, clamp each requests/limits entry to Max, strip names
// absent from Max, and keep every request <= its (clamped) limit.

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func qty(s string) resource.Quantity { return resource.MustParse(s) }

// eq reports whether the quantity stored under name equals want.
func eq(t *testing.T, list corev1.ResourceList, name corev1.ResourceName, want string) {
	t.Helper()
	got, ok := list[name]
	if !ok {
		t.Fatalf("%s absent from list %v", name, list)
	}
	w := qty(want)
	if got.Cmp(w) != 0 {
		t.Errorf("%s = %s, want %s", name, got.String(), want)
	}
}

// (1) nil spec → Default verbatim, empty Note.
func TestApplyDefaultsThenClamp_NilSpec_ReturnsDefault(t *testing.T) {
	def := corev1.ResourceRequirements{
		Limits:   corev1.ResourceList{corev1.ResourceCPU: qty("2"), corev1.ResourceMemory: qty("1Gi")},
		Requests: corev1.ResourceList{corev1.ResourceCPU: qty("1")},
	}
	res := applyDefaultsThenClamp(nil, def, corev1.ResourceList{corev1.ResourceCPU: qty("2")})
	if res.Note != "" {
		t.Errorf("Note = %q, want empty (pure default application)", res.Note)
	}
	eq(t, res.Resources.Limits, corev1.ResourceCPU, "2")
	eq(t, res.Resources.Limits, corev1.ResourceMemory, "1Gi")
	eq(t, res.Resources.Requests, corev1.ResourceCPU, "1")
}

// (2) limits.cpu 4, Max cpu 2 → clamped to 2, Note mentions cpu.
func TestApplyDefaultsThenClamp_LimitsCPUOverMax(t *testing.T) {
	spec := &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: qty("4")}}
	res := applyDefaultsThenClamp(spec, corev1.ResourceRequirements{}, corev1.ResourceList{corev1.ResourceCPU: qty("2")})
	eq(t, res.Resources.Limits, corev1.ResourceCPU, "2")
	if !strings.Contains(res.Note, "cpu") {
		t.Errorf("Note = %q, want it to mention cpu", res.Note)
	}
}

// (3) limits nvidia.com/gpu, Max lacks it → stripped, Note says dropped.
func TestApplyDefaultsThenClamp_UnlistedNameStripped(t *testing.T) {
	const gpu = corev1.ResourceName("nvidia.com/gpu")
	spec := &corev1.ResourceRequirements{Limits: corev1.ResourceList{
		gpu:                qty("1"),
		corev1.ResourceCPU: qty("1"),
	}}
	res := applyDefaultsThenClamp(spec, corev1.ResourceRequirements{}, corev1.ResourceList{corev1.ResourceCPU: qty("2")})
	if _, ok := res.Resources.Limits[gpu]; ok {
		t.Errorf("nvidia.com/gpu should be stripped, still present: %v", res.Resources.Limits)
	}
	eq(t, res.Resources.Limits, corev1.ResourceCPU, "1")
	if !strings.Contains(res.Note, "dropped nvidia.com/gpu") {
		t.Errorf("Note = %q, want it to mention 'dropped nvidia.com/gpu'", res.Note)
	}
}

// (4) requests.memory 3Gi clamped to Max 2Gi, limits.memory unset.
func TestApplyDefaultsThenClamp_RequestsClampedToMax(t *testing.T) {
	spec := &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: qty("3Gi")}}
	res := applyDefaultsThenClamp(spec, corev1.ResourceRequirements{}, corev1.ResourceList{corev1.ResourceMemory: qty("2Gi")})
	eq(t, res.Resources.Requests, corev1.ResourceMemory, "2Gi")
	if !strings.Contains(res.Note, "memory") {
		t.Errorf("Note = %q, want it to mention memory", res.Note)
	}
}

// (4b) request above its (clamped) limit is pulled down to the limit even when
// neither exceeds Max — the request<=limit invariant.
func TestApplyDefaultsThenClamp_RequestPulledDownToLimit(t *testing.T) {
	spec := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: qty("3")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: qty("2")},
	}
	res := applyDefaultsThenClamp(spec, corev1.ResourceRequirements{}, corev1.ResourceList{corev1.ResourceCPU: qty("4")})
	eq(t, res.Resources.Limits, corev1.ResourceCPU, "2")
	eq(t, res.Resources.Requests, corev1.ResourceCPU, "2") // pulled down to the limit
}

// (5) spec within Max → unchanged, empty Note.
func TestApplyDefaultsThenClamp_WithinMax_Unchanged(t *testing.T) {
	spec := &corev1.ResourceRequirements{
		Limits:   corev1.ResourceList{corev1.ResourceCPU: qty("1")},
		Requests: corev1.ResourceList{corev1.ResourceCPU: qty("1")},
	}
	res := applyDefaultsThenClamp(spec, corev1.ResourceRequirements{}, corev1.ResourceList{corev1.ResourceCPU: qty("2")})
	if res.Note != "" {
		t.Errorf("Note = %q, want empty (nothing clamped or stripped)", res.Note)
	}
	eq(t, res.Resources.Limits, corev1.ResourceCPU, "1")
	eq(t, res.Resources.Requests, corev1.ResourceCPU, "1")
}

// (6) ephemeral-storage over Max → clamped (full ResourceList, extended names).
func TestApplyDefaultsThenClamp_EphemeralStorageOverMax(t *testing.T) {
	spec := &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceEphemeralStorage: qty("10Gi")}}
	res := applyDefaultsThenClamp(spec, corev1.ResourceRequirements{}, corev1.ResourceList{corev1.ResourceEphemeralStorage: qty("5Gi")})
	eq(t, res.Resources.Limits, corev1.ResourceEphemeralStorage, "5Gi")
	if !strings.Contains(res.Note, "ephemeral-storage") {
		t.Errorf("Note = %q, want it to mention ephemeral-storage", res.Note)
	}
}
