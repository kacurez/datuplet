package controllers

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// clampResult carries the clamped requirements plus a human-readable note
// describing what changed (empty when nothing was clamped or stripped).
type clampResult struct {
	Resources corev1.ResourceRequirements
	Note      string // "" when unchanged; else e.g. "cpu 4→2, dropped nvidia.com/gpu"
}

// applyDefaultsThenClamp returns the resources to set on a component
// container. When spec is nil/empty it returns def (the registry default).
// Otherwise it clamps every requests/limits entry to max and strips names
// absent from max. Clamping is unconditional (RFC 026 §4.4 Layer 2 —
// defense-in-depth for the direct-kubectl path).
func applyDefaultsThenClamp(spec *corev1.ResourceRequirements, def corev1.ResourceRequirements, max corev1.ResourceList) clampResult {
	if spec == nil || (len(spec.Requests) == 0 && len(spec.Limits) == 0) {
		return clampResult{Resources: *def.DeepCopy()}
	}
	out := *spec.DeepCopy()
	var notes []string
	clampList := func(list corev1.ResourceList) corev1.ResourceList {
		if list == nil {
			return nil
		}
		res := corev1.ResourceList{}
		for name, q := range list {
			maxq, ok := max[name]
			if !ok {
				notes = append(notes, "dropped "+string(name))
				continue // strip unlisted
			}
			if q.Cmp(maxq) > 0 {
				notes = append(notes, string(name)+" "+q.String()+"→"+maxq.String())
				res[name] = maxq.DeepCopy()
			} else {
				res[name] = q
			}
		}
		return res
	}
	out.Limits = clampList(out.Limits)
	out.Requests = clampList(out.Requests)
	// A request above its (possibly clamped) limit is invalid — pull it down.
	for name, rq := range out.Requests {
		if lq, ok := out.Limits[name]; ok && rq.Cmp(lq) > 0 {
			out.Requests[name] = lq.DeepCopy()
		}
	}
	note := ""
	if len(notes) > 0 {
		note = strings.Join(notes, ", ")
	}
	return clampResult{Resources: out, Note: note}
}
