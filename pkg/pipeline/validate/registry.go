package validate

import (
	"fmt"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// RegistryView resolves a component reference (name + optional version) to a
// concrete registered version. Resolution problems come back as Findings whose
// Path is relative to the component ("component" or "version"); ValidateTyped
// prefixes them with the component's stages[i].components[j] path.
//
// Phase 3 appends a *Policy argument to Resolve.
type RegistryView interface {
	// Resolve resolves component at the given version. An empty version
	// resolves to the definition default (its DefaultVersion, or the highest
	// registered stable semver). A non-nil ResolvedComponent may be returned
	// together with warning findings — a deprecated component still resolves.
	Resolve(component, version string) (*ResolvedComponent, []Finding)
}

// ResolvedComponent is a component reference resolved to a concrete version.
type ResolvedComponent struct {
	Component    string
	Version      string
	Image        string
	Prerelease   bool
	ConfigSchema *jsonschema.Schema
	Resources    datupletv1.ComponentResources

	// rawConfigSchema is the source text ConfigSchema was compiled from.
	// ValidateConfig needs it for its x-datuplet-secret / type introspection.
	// Only registry impls in this package (StaticRegistry) populate it; other
	// RegistryView impls should build a StaticRegistry from their definitions
	// to retain the full secret-ref config semantics.
	rawConfigSchema string
}

// StaticRegistry is an in-memory RegistryView keyed by component name, backed
// by ComponentDefinition CRs. It is the resolution implementation used by tests
// and by callers that hold a list of definitions (the pipeline-api view and the
// controller adapter build one from the definitions they fetch).
type StaticRegistry map[string]datupletv1.ComponentDefinition

// Resolve implements RegistryView. Hard resolution problems (unknown component,
// invalid definition, unresolvable version) return a nil ResolvedComponent and
// a single finding; a resolvable-but-deprecated component resolves and returns
// a warning finding alongside the ResolvedComponent.
func (r StaticRegistry) Resolve(component, version string) (*ResolvedComponent, []Finding) {
	def, ok := r[component]
	if !ok {
		return nil, []Finding{{
			Path:     "component",
			Message:  fmt.Sprintf("unknown component %q", component),
			Severity: severityError,
		}}
	}
	if def.Status.Phase == "Invalid" {
		msg := fmt.Sprintf("component %q definition is invalid", component)
		if def.Status.Message != "" {
			msg += ": " + def.Status.Message
		}
		return nil, []Finding{{Path: "component", Message: msg, Severity: severityError}}
	}

	spec := def.Spec
	ver, bad := resolveVersion(component, version, &spec)
	if bad != nil {
		return nil, bad
	}

	rc := &ResolvedComponent{
		Component:  component,
		Version:    ver.Version,
		Image:      ver.Image,
		Prerelease: ver.Prerelease,
	}
	if ver.Resources != nil {
		rc.Resources = *ver.Resources
	}

	var findings []Finding
	if spec.Deprecated {
		findings = append(findings, Finding{
			Path:     "component",
			Message:  fmt.Sprintf("component %q is deprecated", component),
			Severity: severityWarning,
		})
	}
	if ver.ConfigSchema != "" {
		schema, err := CompileSchema(ver.ConfigSchema)
		if err != nil {
			findings = append(findings, Finding{
				Path:     "component",
				Message:  fmt.Sprintf("component %q version %q has an invalid config schema: %v", component, ver.Version, err),
				Severity: severityError,
			})
		} else {
			rc.ConfigSchema = schema
			rc.rawConfigSchema = ver.ConfigSchema
		}
	}
	return rc, findings
}

// resolveVersion selects the concrete version for a (possibly empty) pin. An
// empty pin resolves to the definition's DefaultVersion, else the highest
// registered stable semver. It returns a single finding (path "version") when
// no version can be resolved.
func resolveVersion(component, version string, spec *datupletv1.ComponentDefinitionSpec) (datupletv1.VersionSpec, []Finding) {
	if version != "" {
		v, ok := spec.FindVersion(version)
		if !ok {
			return datupletv1.VersionSpec{}, []Finding{{
				Path:     "version",
				Message:  fmt.Sprintf("component %q has no version %q", component, version),
				Severity: severityError,
			}}
		}
		return v, nil
	}
	if spec.DefaultVersion != "" {
		v, ok := spec.FindVersion(spec.DefaultVersion)
		if !ok {
			return datupletv1.VersionSpec{}, []Finding{{
				Path:     "version",
				Message:  fmt.Sprintf("component %q default version %q is not registered", component, spec.DefaultVersion),
				Severity: severityError,
			}}
		}
		return v, nil
	}
	v, ok := spec.LatestStable()
	if !ok {
		return datupletv1.VersionSpec{}, []Finding{{
			Path:     "version",
			Message:  fmt.Sprintf("component %q has no stable version and no default; pin an explicit version", component),
			Severity: severityError,
		}}
	}
	return v, nil
}
