package validate

import (
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestPolicy_ResourcesAgainstMax exercises the resource-reject rules (RFC 026
// §4.4): unset resources produce nothing; every requests/limits entry is
// checked against the registry Max across the full ResourceList (cpu, memory,
// ephemeral-storage, extended resources), with unlisted names and over-max
// quantities each yielding an error finding.
func TestPolicy_ResourcesAgainstMax(t *testing.T) {
	q := resource.MustParse
	tests := []struct {
		name       string
		rr         *corev1.ResourceRequirements
		max        corev1.ResourceList
		wantCount  int
		wantSubstr string
	}{
		{
			name:      "no resources set yields no findings",
			rr:        nil,
			max:       corev1.ResourceList{corev1.ResourceCPU: q("2")},
			wantCount: 0,
		},
		{
			name:       "limits cpu exceeds max",
			rr:         &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: q("4")}},
			max:        corev1.ResourceList{corev1.ResourceCPU: q("2")},
			wantCount:  1,
			wantSubstr: "exceeds registry max",
		},
		{
			name:       "requests memory exceeds max",
			rr:         &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: q("3Gi")}},
			max:        corev1.ResourceList{corev1.ResourceMemory: q("2Gi")},
			wantCount:  1,
			wantSubstr: "exceeds registry max",
		},
		{
			name:       "ephemeral-storage exceeds max proves full list",
			rr:         &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceEphemeralStorage: q("20Gi")}},
			max:        corev1.ResourceList{corev1.ResourceEphemeralStorage: q("10Gi")},
			wantCount:  1,
			wantSubstr: "exceeds registry max",
		},
		{
			name:       "extended resource not listed in max",
			rr:         &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceName("nvidia.com/gpu"): q("1")}},
			max:        corev1.ResourceList{corev1.ResourceCPU: q("2")},
			wantCount:  1,
			wantSubstr: "not listed in registry max",
		},
		{
			name:      "under max passes",
			rr:        &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: q("1")}},
			max:       corev1.ResourceList{corev1.ResourceCPU: q("2")},
			wantCount: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := checkResourcesAgainstMax(tc.rr, tc.max, "stages[0].components[0]", "c")
			if len(got) != tc.wantCount {
				t.Fatalf("want %d findings, got %d: %+v", tc.wantCount, len(got), got)
			}
			if tc.wantSubstr != "" && !strings.Contains(got[0].Message, tc.wantSubstr) {
				t.Fatalf("want message containing %q, got %+v", tc.wantSubstr, got)
			}
			for _, f := range got {
				if f.Severity != severityError {
					t.Fatalf("want severity %q, got %+v", severityError, f)
				}
			}
		})
	}
}

// gatewayFindings filters findings down to gateway-bounds violations.
func gatewayFindings(findings []Finding) []Finding {
	var out []Finding
	for _, f := range findings {
		if strings.HasPrefix(f.Path, "gateway.") {
			out = append(out, f)
		}
	}
	return out
}

// TestPolicy_GatewayBounds exercises the gateway-bounds rules (RFC 026 §4.6)
// through ValidateTyped's pol wiring: a nil policy disables all bound checks, a
// zero bound is "no bound on that knob", and a set knob over a non-zero bound
// yields an error finding.
func TestPolicy_GatewayBounds(t *testing.T) {
	const mib int64 = 1024 * 1024
	const gib = 1024 * mib
	tests := []struct {
		name      string
		pol       *Policy
		gw        datupletv1.GatewaySpec
		wantCount int
	}{
		{
			name:      "nil policy ignores gateway knobs",
			pol:       nil,
			gw:        datupletv1.GatewaySpec{ChunkSize: 999 * gib, BufferSize: 999 * gib},
			wantCount: 0,
		},
		{
			name:      "bufferSize over bound",
			pol:       &Policy{Gateway: GatewayBounds{MaxBufferSize: 512 * mib}},
			gw:        datupletv1.GatewaySpec{BufferSize: 1 * gib},
			wantCount: 1,
		},
		{
			name:      "bufferSize under bound",
			pol:       &Policy{Gateway: GatewayBounds{MaxBufferSize: 512 * mib}},
			gw:        datupletv1.GatewaySpec{BufferSize: 256 * mib},
			wantCount: 0,
		},
		{
			name:      "zero bound disables check",
			pol:       &Policy{Gateway: GatewayBounds{MaxChunkSize: 0}},
			gw:        datupletv1.GatewaySpec{ChunkSize: 999 * gib},
			wantCount: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := pipelineWithComponent("c", nil)
			p.Spec.Gateway = tc.gw
			findings := ValidateTyped(p, nil, tc.pol)
			gw := gatewayFindings(findings)
			if len(gw) != tc.wantCount {
				t.Fatalf("want %d gateway findings, got %d: %+v", tc.wantCount, len(gw), gw)
			}
			for _, f := range gw {
				if f.Severity != severityError {
					t.Fatalf("want severity %q, got %+v", severityError, f)
				}
			}
		})
	}
}
