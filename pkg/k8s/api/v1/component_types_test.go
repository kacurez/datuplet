package v1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

func TestComponentDefinitionStrictDecode(t *testing.T) {
	in := []byte(`
apiVersion: datuplet.io/v1
kind: ComponentDefinition
metadata:
  name: http-json-extractor
spec:
  displayName: HTTP JSON Extractor
  description: Fetches JSON from an HTTP endpoint into an Iceberg table.
  maintainer: datuplet
  deprecated: false
  defaultVersion: v0.1.0
  versions:
    - version: v0.1.0
      image: ghcr.io/kacurez/http-json-extractor:v0.1.0
      configSchema: |
        {
          "type": "object",
          "required": ["url"],
          "properties": {
            "url": {"type": "string", "format": "uri"}
          },
          "additionalProperties": false
        }
      resources:
        default:
          requests: {cpu: 100m, memory: 128Mi}
          limits: {cpu: "1", memory: 512Mi, ephemeral-storage: 1Gi}
        max:
          cpu: "2"
          memory: 2Gi
          ephemeral-storage: 10Gi
    - version: dev
      prerelease: true
      image: localhost:5000/http-json-extractor:dev
      configSchema: '{"type": "object"}'
`)
	var cd ComponentDefinition
	if err := yaml.UnmarshalStrict(in, &cd); err != nil {
		t.Fatalf("strict decode: %v", err)
	}
	if cd.Spec.DisplayName != "HTTP JSON Extractor" {
		t.Errorf("DisplayName = %q", cd.Spec.DisplayName)
	}
	if len(cd.Spec.Versions) != 2 {
		t.Fatalf("len(Versions) = %d, want 2", len(cd.Spec.Versions))
	}
	stable := cd.Spec.Versions[0]
	if stable.Version != "v0.1.0" || stable.Image != "ghcr.io/kacurez/http-json-extractor:v0.1.0" {
		t.Errorf("stable version decoded wrong: %+v", stable)
	}
	if stable.Resources == nil {
		t.Fatal("stable.Resources is nil")
	}
	if stable.Resources.Max.Cpu().String() != "2" {
		t.Errorf("Max cpu = %v", stable.Resources.Max.Cpu())
	}
	dev := cd.Spec.Versions[1]
	if !dev.Prerelease {
		t.Error("dev.Prerelease = false, want true")
	}
	if dev.Resources != nil {
		t.Errorf("dev.Resources = %+v, want nil", dev.Resources)
	}
}

func TestIsStableVersion(t *testing.T) {
	cases := map[string]bool{
		"v0.1.0":  true,
		"v1.2.3":  true,
		"dev":     false,
		"v1.2":    false,
		"1.2.3":   false,
		"v1.2.3a": false,
		"":        false,
	}
	for v, want := range cases {
		if got := IsStableVersion(v); got != want {
			t.Errorf("IsStableVersion(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestComponentDefinitionSpec_LatestStable(t *testing.T) {
	spec := &ComponentDefinitionSpec{
		Versions: []VersionSpec{
			{Version: "v0.1.9"},
			{Version: "dev", Prerelease: true},
			{Version: "v0.2.0"},
			{Version: "v0.1.10"},
		},
	}
	got, ok := spec.LatestStable()
	if !ok {
		t.Fatal("LatestStable() ok = false, want true")
	}
	if got.Version != "v0.2.0" {
		t.Errorf("LatestStable() = %q, want v0.2.0", got.Version)
	}
}

func TestComponentDefinitionSpec_LatestStable_OnlyPrerelease(t *testing.T) {
	spec := &ComponentDefinitionSpec{
		Versions: []VersionSpec{
			{Version: "dev", Prerelease: true},
		},
	}
	if _, ok := spec.LatestStable(); ok {
		t.Error("LatestStable() ok = true, want false when only prerelease versions exist")
	}
}

func TestComponentDefinitionSpec_FindVersion(t *testing.T) {
	spec := &ComponentDefinitionSpec{
		Versions: []VersionSpec{
			{Version: "v0.1.0", Image: "img:v0.1.0"},
			{Version: "dev", Prerelease: true, Image: "img:dev"},
		},
	}
	got, ok := spec.FindVersion("dev")
	if !ok {
		t.Fatal("FindVersion(dev) ok = false, want true")
	}
	if got.Image != "img:dev" {
		t.Errorf("FindVersion(dev).Image = %q", got.Image)
	}
	if _, ok := spec.FindVersion("missing"); ok {
		t.Error("FindVersion(missing) ok = true, want false")
	}
}

func TestComponentDefinition_DeepCopy_Isolation(t *testing.T) {
	orig := &ComponentDefinition{
		Spec: ComponentDefinitionSpec{
			Versions: []VersionSpec{
				{
					Version: "v0.1.0",
					Image:   "img:v0.1.0",
					Resources: &ComponentResources{
						Max: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
	}

	cp := orig.DeepCopy()

	// Mutate the copy's pointer field and its nested map; the original must
	// be unaffected.
	cp.Spec.Versions[0].Resources.Max[corev1.ResourceCPU] = resource.MustParse("4")
	cp.Spec.Versions[0].Version = "mutated"

	if orig.Spec.Versions[0].Resources.Max.Cpu().String() != "2" {
		t.Errorf("mutation on copy leaked into original: Max cpu = %v", orig.Spec.Versions[0].Resources.Max.Cpu())
	}
	if orig.Spec.Versions[0].Version != "v0.1.0" {
		t.Errorf("mutation on copy leaked into original: Version = %q", orig.Spec.Versions[0].Version)
	}
	if cp.Spec.Versions[0].Resources == orig.Spec.Versions[0].Resources {
		t.Error("cp.Resources aliases orig.Resources; expected distinct pointer")
	}
}

func TestPipelineRunStatus_DeepCopy_ResolvedSpec_Isolation(t *testing.T) {
	orig := &PipelineRun{
		Status: PipelineRunStatus{
			PipelineGeneration: 3,
			ResolvedSpec: &PipelineSpec{
				Stages: []StageSpec{
					{Name: "extract"},
				},
			},
			Components: []ResolvedComponentStatus{
				{Name: "extract-posts", Component: "http-json-extractor", Version: "v0.1.0", Image: "img:v0.1.0"},
			},
		},
	}

	cp := orig.DeepCopy()

	if cp.Status.ResolvedSpec == orig.Status.ResolvedSpec {
		t.Fatal("cp.Status.ResolvedSpec aliases orig; expected distinct pointer")
	}

	cp.Status.ResolvedSpec.Stages[0].Name = "mutated"
	cp.Status.Components[0].Image = "mutated"

	if orig.Status.ResolvedSpec.Stages[0].Name != "extract" {
		t.Errorf("mutation on copy leaked into original ResolvedSpec: Name = %q", orig.Status.ResolvedSpec.Stages[0].Name)
	}
	if orig.Status.Components[0].Image != "img:v0.1.0" {
		t.Errorf("mutation on copy leaked into original Components: Image = %q", orig.Status.Components[0].Image)
	}
}
