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

// TestStaticRegistry_Resolve_Errors: every unresolvable (component, version)
// returns a nil ResolvedComponent and an error finding at the right path.
func TestStaticRegistry_Resolve_Errors(t *testing.T) {
	one := []datupletv1.VersionSpec{stableV("v0.1.0", "img:v0.1.0")}
	cases := []struct {
		name       string
		reg        StaticRegistry
		component  string
		version    string
		wantPath   string
		wantSubstr []string
	}{
		{
			name:       "unknown component",
			reg:        StaticRegistry{"known": {Spec: datupletv1.ComponentDefinitionSpec{Versions: one}}},
			component:  "ghost",
			wantPath:   "component",
			wantSubstr: []string{"unknown component"},
		},
		{
			name:      "unknown version",
			reg:       StaticRegistry{"c": {Spec: datupletv1.ComponentDefinitionSpec{Versions: one}}},
			component: "c",
			version:   "v9.9.9",
			wantPath:  "version",
		},
		{
			name: "no stable version and no pin",
			reg: StaticRegistry{"c": {Spec: datupletv1.ComponentDefinitionSpec{
				Versions: []datupletv1.VersionSpec{{Version: "dev", Image: "img:dev", Prerelease: true}},
			}}},
			component:  "c",
			wantPath:   "version",
			wantSubstr: []string{"no stable version"},
		},
		{
			name: "default version not registered",
			reg: StaticRegistry{"c": {Spec: datupletv1.ComponentDefinitionSpec{
				DefaultVersion: "v9.9.9",
				Versions:       one,
			}}},
			component:  "c",
			wantPath:   "version",
			wantSubstr: []string{"default version", "is not registered"},
		},
		{
			name: "invalid phase",
			reg: StaticRegistry{"c": {
				Spec:   datupletv1.ComponentDefinitionSpec{Versions: one},
				Status: datupletv1.ComponentDefinitionStatus{Phase: "Invalid", Message: "config schema does not compile"},
			}},
			component:  "c",
			wantPath:   "component",
			wantSubstr: []string{"invalid"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc, findings := tc.reg.Resolve(tc.component, tc.version)
			if rc != nil {
				t.Fatalf("want nil ResolvedComponent, got %+v", rc)
			}
			f, ok := findingAt(findings, tc.wantPath)
			if !ok {
				t.Fatalf("want a finding at path %q, got %+v", tc.wantPath, findings)
			}
			if f.Severity != severityError {
				t.Fatalf("want severity error, got %+v", f)
			}
			for _, s := range tc.wantSubstr {
				if !strings.Contains(f.Message, s) {
					t.Fatalf("finding message %q, want it to contain %q", f.Message, s)
				}
			}
		})
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
	findings := ValidateTyped(p, StaticRegistry{}, nil)
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

	findings := ValidateTyped(p, reg, nil)
	if _, ok := findingAt(findings, "stages[0].components[0].config.url"); !ok {
		t.Fatalf("want a config-schema finding at stages[0].components[0].config.url, got %+v", findings)
	}
}

// pipelineWithComponentIO builds a single-stage pipeline whose sole component
// carries the given name plus explicit inputs/outputs presence, for exercising
// the IO-capability rules (RFC 027 §4.3).
func pipelineWithComponentIO(name string, withInputs, withOutputs bool) *datupletv1.Pipeline {
	c := datupletv1.ComponentSpec{Name: name, Component: name}
	if withInputs {
		c.Inputs = &datupletv1.InputSpec{Buckets: []string{"raw"}}
	}
	if withOutputs {
		c.Outputs = &datupletv1.OutputSpec{DefaultBucket: "processed"}
	}
	p := &datupletv1.Pipeline{
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{Name: "s0", Components: []datupletv1.ComponentSpec{c}}},
		},
	}
	p.Name = "p"
	return p
}

// TestValidateTyped_IOCapability: the registry's io declaration gates whether
// a component may / must declare inputs and outputs (RFC 027 §4.3). A
// violation is an error finding at the inputs/outputs path.
func TestValidateTyped_IOCapability(t *testing.T) {
	const inputsPath = "stages[0].components[0].inputs"
	const outputsPath = "stages[0].components[0].outputs"
	cases := []struct {
		name        string
		io          *datupletv1.ComponentIO // nil: no io declaration (optional/optional)
		withInputs  bool
		withOutputs bool
		wantPath    string // empty: no IO-capability finding at all
	}{
		{"inputs none rejects declared inputs", &datupletv1.ComponentIO{Inputs: "none", Outputs: "required"}, true, true, inputsPath},
		{"inputs required rejects missing inputs", &datupletv1.ComponentIO{Inputs: "required", Outputs: "required"}, false, true, inputsPath},
		{"outputs none rejects declared outputs", &datupletv1.ComponentIO{Inputs: "required", Outputs: "none"}, true, true, outputsPath},
		{"outputs required rejects missing outputs", &datupletv1.ComponentIO{Inputs: "required", Outputs: "required"}, true, false, outputsPath},
		// No io declaration defaults to optional/optional: any combination of
		// inputs/outputs is acceptable to the IO-capability rules.
		{"optional default no findings", nil, true, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := StaticRegistry{
				"sink": {Spec: datupletv1.ComponentDefinitionSpec{
					IO:       tc.io,
					Versions: []datupletv1.VersionSpec{stableV("v0.1.0", "img:v0.1.0")},
				}},
			}
			p := pipelineWithComponentIO("sink", tc.withInputs, tc.withOutputs)
			findings := ValidateTyped(p, reg, nil)
			if tc.wantPath == "" {
				for _, f := range findings {
					if strings.Contains(f.Message, "takes no") || strings.Contains(f.Message, "requires at least one") {
						t.Fatalf("no io declared must not produce IO-capability findings, got %+v", findings)
					}
				}
				return
			}
			f, ok := findingAt(findings, tc.wantPath)
			if !ok {
				t.Fatalf("want a finding at %s, got %+v", tc.wantPath, findings)
			}
			if f.Severity != severityError {
				t.Fatalf("want severity error, got %+v", f)
			}
		})
	}
}

func TestValidateTyped_NilRegistry_SkipsResolution(t *testing.T) {
	// "ghost" is in no registry, but a nil registry must skip resolution
	// entirely, so no "unknown component" finding is produced.
	p := pipelineWithComponent("ghost", nil)
	findings := ValidateTyped(p, nil, nil)
	for _, f := range findings {
		if strings.Contains(f.Message, "unknown component") {
			t.Fatalf("nil registry must skip resolution, got %+v", findings)
		}
	}
}
