package validate

import (
	"fmt"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	corev1 "k8s.io/api/core/v1"
)

// Policy carries the chart-configured bounds a non-superadmin pipeline must
// satisfy. A nil *Policy passed to ValidateTyped disables ALL bound checks
// (used by callers that don't enforce policy, and by the kubectl path before
// the controller clamps). (RFC 026 §4.4, §4.6)
type Policy struct {
	Gateway GatewayBounds
}

// GatewayBounds are the per-pipeline gateway-knob ceilings. A zero value for
// any field means "no bound on that knob". The knobs map to the int64 fields
// on datupletv1.GatewaySpec: MaxChunkSize->ChunkSize, MaxBufferSize->BufferSize,
// MaxTargetFileSize->TargetFileSize.
type GatewayBounds struct {
	MaxChunkSize      int64
	MaxBufferSize     int64
	MaxTargetFileSize int64
}

// checkResourcesAgainstMax appends a Finding for every requests/limits entry
// whose resource name is absent from max, or whose quantity exceeds the max
// for that name. The full ResourceList is walked (cpu, memory,
// ephemeral-storage, extended resources). Returns findings; does not mutate.
// A nil-or-empty rr yields no findings (defaults handled by the controller).
func checkResourcesAgainstMax(rr *corev1.ResourceRequirements, max corev1.ResourceList, pathPrefix, component string) []Finding {
	if rr == nil {
		return nil
	}
	var out []Finding
	check := func(kind string, list corev1.ResourceList) {
		for name, q := range list {
			maxq, ok := max[name]
			if !ok {
				out = append(out, Finding{
					Path:     pathPrefix + ".resources",
					Message:  "resource \"" + string(name) + "\" is not allowed for component " + component + " (not listed in registry max)",
					Severity: severityError,
				})
				continue
			}
			if q.Cmp(maxq) > 0 {
				out = append(out, Finding{
					Path:     pathPrefix + ".resources." + kind + "." + string(name),
					Message:  "resources." + kind + "." + string(name) + "=" + q.String() + " exceeds registry max " + maxq.String(),
					Severity: severityError,
				})
			}
		}
	}
	check("limits", rr.Limits)
	check("requests", rr.Requests)
	return out
}

// checkGatewayBounds appends a Finding for each gateway knob that is set and
// exceeds its corresponding non-zero bound in pol.Gateway. A zero bound means
// "no bound on that knob" (RFC 026 §4.6). Callers only invoke this when
// pol != nil.
func checkGatewayBounds(p *datupletv1.Pipeline, pol *Policy) []Finding {
	if pol == nil {
		return nil
	}
	var out []Finding
	check := func(knob, bound int64, name string) {
		if bound != 0 && knob > bound {
			out = append(out, Finding{
				Path:     "gateway." + name,
				Message:  fmt.Sprintf("gateway.%s=%d exceeds policy max %d", name, knob, bound),
				Severity: severityError,
			})
		}
	}
	check(p.Spec.Gateway.ChunkSize, pol.Gateway.MaxChunkSize, "chunkSize")
	check(p.Spec.Gateway.BufferSize, pol.Gateway.MaxBufferSize, "bufferSize")
	check(p.Spec.Gateway.TargetFileSize, pol.Gateway.MaxTargetFileSize, "targetFileSize")
	return out
}
