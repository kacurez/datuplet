package validate

import (
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

// stableV builds a stable VersionSpec.
func stableV(version, image string) datupletv1.VersionSpec {
	return datupletv1.VersionSpec{Version: version, Image: image}
}

// findingAt returns the first finding at the exact path, or a zero Finding.
func findingAt(findings []Finding, path string) (Finding, bool) {
	for _, f := range findings {
		if f.Path == path {
			return f, true
		}
	}
	return Finding{}, false
}

func TestStaticRegistry_Resolve_UnknownComponent(t *testing.T) {
	reg := StaticRegistry{
		"known": {Spec: datupletv1.ComponentDefinitionSpec{Versions: []datupletv1.VersionSpec{stableV("v0.1.0", "img:v0.1.0")}}},
	}
	rc, findings := reg.Resolve("ghost", "")
	if rc != nil {
		t.Fatalf("want nil ResolvedComponent for unknown component, got %+v", rc)
	}
	f, ok := findingAt(findings, "component")
	if !ok {
		t.Fatalf("want a finding at path %q, got %+v", "component", findings)
	}
	if f.Severity != severityError || !strings.Contains(f.Message, "unknown component") {
		t.Fatalf("unexpected finding: %+v", f)
	}
}

func TestStaticRegistry_Resolve_UnknownVersion(t *testing.T) {
	reg := StaticRegistry{
		"c": {Spec: datupletv1.ComponentDefinitionSpec{Versions: []datupletv1.VersionSpec{stableV("v0.1.0", "img:v0.1.0")}}},
	}
	rc, findings := reg.Resolve("c", "v9.9.9")
	if rc != nil {
		t.Fatalf("want nil ResolvedComponent for unknown version, got %+v", rc)
	}
	f, ok := findingAt(findings, "version")
	if !ok {
		t.Fatalf("want a finding at path %q, got %+v", "version", findings)
	}
	if f.Severity != severityError {
		t.Fatalf("want severity error, got %+v", f)
	}
}

func TestStaticRegistry_Resolve_NoStableNoPin(t *testing.T) {
	reg := StaticRegistry{
		"c": {Spec: datupletv1.ComponentDefinitionSpec{
			Versions: []datupletv1.VersionSpec{{Version: "dev", Image: "img:dev", Prerelease: true}},
		}},
	}
	rc, findings := reg.Resolve("c", "")
	if rc != nil {
		t.Fatalf("want nil ResolvedComponent when no default resolvable, got %+v", rc)
	}
	f, ok := findingAt(findings, "version")
	if !ok {
		t.Fatalf("want a finding at path %q, got %+v", "version", findings)
	}
	if f.Severity != severityError || !strings.Contains(f.Message, "no stable version") {
		t.Fatalf("unexpected finding: %+v", f)
	}
}

func TestStaticRegistry_Resolve_DefaultVersionNotRegistered(t *testing.T) {
	reg := StaticRegistry{
		"c": {Spec: datupletv1.ComponentDefinitionSpec{
			DefaultVersion: "v9.9.9",
			Versions:       []datupletv1.VersionSpec{stableV("v0.1.0", "img:v0.1.0")},
		}},
	}
	rc, findings := reg.Resolve("c", "")
	if rc != nil {
		t.Fatalf("want nil ResolvedComponent when defaultVersion is unresolvable, got %+v", rc)
	}
	f, ok := findingAt(findings, "version")
	if !ok {
		t.Fatalf("want a finding at path %q, got %+v", "version", findings)
	}
	if f.Severity != severityError || !strings.Contains(f.Message, "default version") || !strings.Contains(f.Message, "is not registered") {
		t.Fatalf("unexpected finding: %+v", f)
	}
}

func TestStaticRegistry_Resolve_InvalidPhase(t *testing.T) {
	reg := StaticRegistry{
		"c": {
			Spec:   datupletv1.ComponentDefinitionSpec{Versions: []datupletv1.VersionSpec{stableV("v0.1.0", "img:v0.1.0")}},
			Status: datupletv1.ComponentDefinitionStatus{Phase: "Invalid", Message: "config schema does not compile"},
		},
	}
	rc, findings := reg.Resolve("c", "")
	if rc != nil {
		t.Fatalf("want nil ResolvedComponent for an invalid definition, got %+v", rc)
	}
	f, ok := findingAt(findings, "component")
	if !ok {
		t.Fatalf("want a finding at path %q, got %+v", "component", findings)
	}
	if f.Severity != severityError || !strings.Contains(f.Message, "invalid") {
		t.Fatalf("unexpected finding: %+v", f)
	}
}

func TestStaticRegistry_Resolve_Deprecated_Warning(t *testing.T) {
	reg := StaticRegistry{
		"c": {Spec: datupletv1.ComponentDefinitionSpec{
			Deprecated: true,
			Versions:   []datupletv1.VersionSpec{stableV("v0.1.0", "img:v0.1.0")},
		}},
	}
	rc, findings := reg.Resolve("c", "")
	if rc == nil {
		t.Fatal("deprecated component must still resolve, got nil ResolvedComponent")
	}
	if rc.Version != "v0.1.0" || rc.Image != "img:v0.1.0" {
		t.Fatalf("unexpected resolved version/image: %+v", rc)
	}
	f, ok := findingAt(findings, "component")
	if !ok {
		t.Fatalf("want a deprecation finding at path %q, got %+v", "component", findings)
	}
	if f.Severity != severityWarning || !strings.Contains(f.Message, "deprecated") {
		t.Fatalf("want a warning-severity deprecation finding, got %+v", f)
	}
}

func TestStaticRegistry_Resolve_HappyPath(t *testing.T) {
	reg := StaticRegistry{
		"c": {Spec: datupletv1.ComponentDefinitionSpec{
			Versions: []datupletv1.VersionSpec{
				stableV("v0.1.0", "img:v0.1.0"),
				stableV("v0.2.0", "img:v0.2.0"),
				{Version: "dev", Image: "img:dev", Prerelease: true},
			},
		}},
	}

	t.Run("unpinned resolves highest stable", func(t *testing.T) {
		rc, findings := reg.Resolve("c", "")
		if len(findings) != 0 {
			t.Fatalf("want 0 findings, got %+v", findings)
		}
		if rc == nil || rc.Version != "v0.2.0" || rc.Image != "img:v0.2.0" || rc.Prerelease {
			t.Fatalf("unexpected resolution: %+v", rc)
		}
	})

	t.Run("pinned prerelease resolves", func(t *testing.T) {
		rc, findings := reg.Resolve("c", "dev")
		if len(findings) != 0 {
			t.Fatalf("want 0 findings, got %+v", findings)
		}
		if rc == nil || rc.Version != "dev" || rc.Image != "img:dev" || !rc.Prerelease {
			t.Fatalf("unexpected resolution: %+v", rc)
		}
	})

	t.Run("explicit defaultVersion overrides latest stable", func(t *testing.T) {
		reg := StaticRegistry{
			"c": {Spec: datupletv1.ComponentDefinitionSpec{
				DefaultVersion: "v0.1.0",
				Versions:       []datupletv1.VersionSpec{stableV("v0.1.0", "img:v0.1.0"), stableV("v0.2.0", "img:v0.2.0")},
			}},
		}
		rc, findings := reg.Resolve("c", "")
		if len(findings) != 0 {
			t.Fatalf("want 0 findings, got %+v", findings)
		}
		if rc == nil || rc.Version != "v0.1.0" {
			t.Fatalf("unexpected resolution: %+v", rc)
		}
	})
}

// pipelineWithComponent builds an otherwise-valid single-stage pipeline whose
// sole component carries the given name and (optional) raw config.
func pipelineWithComponent(name string, rawConfig []byte) *datupletv1.Pipeline {
	c := datupletv1.ComponentSpec{
		Name:      name,
		Component: name,
		Outputs:   &datupletv1.OutputSpec{DefaultBucket: "raw"},
	}
	if rawConfig != nil {
		c.Config = apiextensionsv1.JSON{Raw: rawConfig}
	}
	p := &datupletv1.Pipeline{
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{Name: "s0", Components: []datupletv1.ComponentSpec{c}}},
		},
	}
	p.Name = "p"
	return p
}

func TestValidateTyped_UnknownComponent_PathPrefixed(t *testing.T) {
	p := pipelineWithComponent("ghost", nil)
	findings := ValidateTyped(p, StaticRegistry{})
	f, ok := findingAt(findings, "stages[0].components[0].component")
	if !ok {
		t.Fatalf("want a finding at the component's full path, got %+v", findings)
	}
	if f.Severity != severityError || !strings.Contains(f.Message, "unknown component") {
		t.Fatalf("unexpected finding: %+v", f)
	}
}

func TestValidateTyped_ConfigSchemaViolation_FullPath(t *testing.T) {
	const schema = `{"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}`
	reg := StaticRegistry{
		"extractor": {Spec: datupletv1.ComponentDefinitionSpec{
			Versions: []datupletv1.VersionSpec{{Version: "v0.1.0", Image: "img:v0.1.0", ConfigSchema: schema}},
		}},
	}
	// url is a number, violating the string type constraint.
	p := pipelineWithComponent("extractor", []byte(`{"url":123}`))

	findings := ValidateTyped(p, reg)
	if _, ok := findingAt(findings, "stages[0].components[0].config.url"); !ok {
		t.Fatalf("want a config-schema finding at stages[0].components[0].config.url, got %+v", findings)
	}
}

func TestValidateTyped_NilRegistry_SkipsResolution(t *testing.T) {
	// "ghost" is in no registry, but a nil registry must skip resolution
	// entirely, so no "unknown component" finding is produced.
	p := pipelineWithComponent("ghost", nil)
	findings := ValidateTyped(p, nil)
	for _, f := range findings {
		if strings.Contains(f.Message, "unknown component") {
			t.Fatalf("nil registry must skip resolution, got %+v", findings)
		}
	}
}
