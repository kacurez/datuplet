package http

import (
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// resourcesModified reports whether any component's resources block differs
// between the old (stored) and new (incoming) pipeline specs. Components are
// matched by name. A component present in new with non-nil resources but
// absent from old counts as modified. Semantic quantity equality is used so
// "1" and "1000m" are treated as identical. (RFC 026 §4.4 diff-gate.)
func resourcesModified(oldP, newP *datupletv1.Pipeline) bool {
	oldRes := resourcesByComponent(oldP)
	for _, comp := range allComponents(newP) {
		if !apiequality.Semantic.DeepEqual(oldRes[comp.Name], comp.Resources) {
			return true
		}
	}
	// Also catch a component removed from new that HAD resources in old:
	// removing an admin-set resources block is itself a modification.
	newRes := resourcesByComponent(newP)
	for name, oldR := range oldRes {
		if oldR == nil {
			continue
		}
		if _, ok := newRes[name]; !ok {
			return true
		}
	}
	return false
}

// resourcesByComponent flattens a pipeline's components into a
// componentInstanceName -> resources map. A component with no resources block
// maps to a nil value (present-but-nil is distinct from absent).
func resourcesByComponent(p *datupletv1.Pipeline) map[string]*corev1.ResourceRequirements {
	out := make(map[string]*corev1.ResourceRequirements)
	for _, c := range allComponents(p) {
		out[c.Name] = c.Resources
	}
	return out
}

// allComponents flattens every stage's components into a single slice.
func allComponents(p *datupletv1.Pipeline) []datupletv1.ComponentSpec {
	if p == nil {
		return nil
	}
	var out []datupletv1.ComponentSpec
	for i := range p.Spec.Stages {
		out = append(out, p.Spec.Stages[i].Components...)
	}
	return out
}
