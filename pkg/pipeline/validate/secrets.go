package validate

import (
	"sort"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/lib/secrets"
)

// ReferencedSecrets returns the sorted, deduplicated set of $[name] secret
// keys referenced across every component's ConfigMap() string values. Only
// whole-scalar refs count (a value that is exactly "$[name]"); mid-string
// occurrences and the escaped "$$[name]" form are not refs. This mirrors the
// rule enforced by validateSecretRefs, reusing the same walker (walker.go)
// and the pkg/lib/secrets whole-scalar matcher.
func ReferencedSecrets(p *datupletv1.Pipeline) []string {
	if p == nil {
		return nil
	}

	refs := map[string]struct{}{}
	for i := range p.Spec.Stages {
		stage := &p.Spec.Stages[i]
		for j := range stage.Components {
			cfg, err := stage.Components[j].ConfigMap()
			if err != nil || cfg == nil {
				continue
			}
			walkStrings(cfg, "", func(_, s string) {
				names, _ := secrets.Validate(s)
				for _, name := range names {
					refs[name] = struct{}{}
				}
			})
		}
	}

	if len(refs) == 0 {
		return nil
	}
	out := make([]string, 0, len(refs))
	for name := range refs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
