package http

import (
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// resourcesModified reports whether any component's resources block differs
// between the old (stored) and new (incoming) pipeline specs. Component
// instances are matched by their (stage, name) identity — NOT bare name — so a
// component named "c1" in stage1 never collapses onto a "c1" in stage2 (which
// would let a non-superadmin smuggle in a resources block that another stage's
// same-named instance already carries). A component present in new with non-nil
// resources but absent from old counts as modified. Semantic quantity equality
// is used so "1" and "1000m" are treated as identical. (RFC 026 §4.4 diff-gate.)
//
// Same-stage duplicate instance names remain a degenerate case (later wins in
// the map); the real per-instance identity is stage+name and there is no
// uniqueness validation to lean on.
func resourcesModified(oldP, newP *datupletv1.Pipeline) bool {
	oldRes := resourcesByComponent(oldP)
	newRes := resourcesByComponent(newP)
	for key, newR := range newRes {
		if !apiequality.Semantic.DeepEqual(oldRes[key], newR) {
			return true
		}
	}
	// Also catch a component removed from new that HAD resources in old:
	// removing an admin-set resources block is itself a modification.
	for key, oldR := range oldRes {
		if oldR == nil {
			continue
		}
		if _, ok := newRes[key]; !ok {
			return true
		}
	}
	return false
}

// resourcesByComponent flattens a pipeline's component instances into a
// (stage, name) -> resources map. A component with no resources block maps to a
// nil value (present-but-nil is distinct from absent). The key joins stage and
// component name with a NUL separator so distinct stages never collide.
func resourcesByComponent(p *datupletv1.Pipeline) map[string]*corev1.ResourceRequirements {
	out := make(map[string]*corev1.ResourceRequirements)
	if p == nil {
		return out
	}
	for i := range p.Spec.Stages {
		stage := &p.Spec.Stages[i]
		for j := range stage.Components {
			c := &stage.Components[j]
			out[stage.Name+"\x00"+c.Name] = c.Resources
		}
	}
	return out
}
