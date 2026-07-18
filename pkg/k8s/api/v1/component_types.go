package v1

import (
	"regexp"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// stableVersionPattern matches a stable semver version string (vMAJOR.MINOR.PATCH).
var stableVersionPattern = regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)$`)

// IsStableVersion reports whether v is a stable semver version string
// (vMAJOR.MINOR.PATCH). Prerelease tags such as "dev" are not stable.
func IsStableVersion(v string) bool {
	return stableVersionPattern.MatchString(v)
}

// parseStableVersion extracts the (major, minor, patch) components of a
// stable semver string. ok is false for anything IsStableVersion rejects.
func parseStableVersion(v string) (major, minor, patch int, ok bool) {
	m := stableVersionPattern.FindStringSubmatch(v)
	if m == nil {
		return 0, 0, 0, false
	}
	major, _ = strconv.Atoi(m[1])
	minor, _ = strconv.Atoi(m[2])
	patch, _ = strconv.Atoi(m[3])
	return major, minor, patch, true
}

// ComponentDefinitionSpec defines a registered component and its available versions.
type ComponentDefinitionSpec struct {
	// DisplayName is the human-readable component name.
	// +optional
	DisplayName string `json:"displayName,omitempty"`

	// Description explains what the component does.
	// +optional
	Description string `json:"description,omitempty"`

	// Maintainer identifies who owns this component definition.
	// +optional
	Maintainer string `json:"maintainer,omitempty"`

	// Deprecated marks the component as no longer recommended for new pipelines.
	// +optional
	Deprecated bool `json:"deprecated,omitempty"`

	// DefaultVersion is the version a pipeline resolves to when it omits
	// version. Empty resolves to the highest registered stable semver.
	// +optional
	DefaultVersion string `json:"defaultVersion,omitempty"`

	// Versions lists the available versions of this component.
	Versions []VersionSpec `json:"versions"`

	// IO declares this component's input/output capability. Nil means
	// optional/optional (the component may or may not read/write tables).
	// IO lives at the definition level, not per-version, by design.
	// +optional
	IO *ComponentIO `json:"io,omitempty"`
}

// ComponentIO declares a component's input/output capability. Valid mode
// values are "none", "optional", and "required"; empty is treated as
// "optional". Use InputsMode/OutputsMode to read the resolved value —
// consumers should never inspect the fields directly.
type ComponentIO struct {
	// Inputs is the component's input capability mode.
	// +optional
	Inputs string `json:"inputs,omitempty"`

	// Outputs is the component's output capability mode.
	// +optional
	Outputs string `json:"outputs,omitempty"`
}

// InputsMode returns the resolved input mode: the explicit value, or
// "optional" when io is nil or the field is empty.
func (io *ComponentIO) InputsMode() string {
	if io == nil || io.Inputs == "" {
		return "optional"
	}
	return io.Inputs
}

// OutputsMode returns the resolved output mode: the explicit value, or
// "optional" when io is nil or the field is empty.
func (io *ComponentIO) OutputsMode() string {
	if io == nil || io.Outputs == "" {
		return "optional"
	}
	return io.Outputs
}

// VersionSpec defines a single version of a component.
type VersionSpec struct {
	// Version is the version identifier (e.g. "v0.1.0", or a mutable tag
	// such as "dev" when Prerelease is true).
	Version string `json:"version"`

	// Image is the container image for this version.
	Image string `json:"image"`

	// Prerelease marks this version as excluded from default-version
	// resolution; prerelease versions may use mutable tags.
	// +optional
	Prerelease bool `json:"prerelease,omitempty"`

	// ConfigSchema is a JSON Schema (draft 2020-12) string blob validating
	// component config.
	// +optional
	ConfigSchema string `json:"configSchema,omitempty"`

	// Resources bounds the compute resources this version may use.
	// +optional
	Resources *ComponentResources `json:"resources,omitempty"`
}

// ComponentResources bounds the compute resources a component version may use.
type ComponentResources struct {
	// Default is applied when the pipeline sets no resources.
	// +optional
	Default corev1.ResourceRequirements `json:"default,omitempty"`

	// Max is the resource ceiling; pipelines cannot exceed it.
	// +optional
	Max corev1.ResourceList `json:"max,omitempty"`
}

// ComponentDefinitionStatus defines the observed state of ComponentDefinition.
type ComponentDefinitionStatus struct {
	// Phase is "Valid" or "Invalid".
	// +optional
	Phase string `json:"phase,omitempty"`

	// Message provides additional information about the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the last generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ComponentDefinition is the Schema for the componentdefinitions API. It is
// cluster-scoped: the component registry is shared across all pipelines.
type ComponentDefinition struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ComponentDefinitionSpec   `json:"spec,omitempty"`
	Status ComponentDefinitionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ComponentDefinitionList contains a list of ComponentDefinition.
type ComponentDefinitionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ComponentDefinition `json:"items"`
}

// LatestStable returns the highest-semver non-prerelease version, if any.
func (s *ComponentDefinitionSpec) LatestStable() (VersionSpec, bool) {
	var (
		best                            VersionSpec
		bestMajor, bestMinor, bestPatch int
		found                           bool
	)
	for _, v := range s.Versions {
		if v.Prerelease {
			continue
		}
		major, minor, patch, ok := parseStableVersion(v.Version)
		if !ok {
			continue
		}
		if !found ||
			major > bestMajor ||
			(major == bestMajor && minor > bestMinor) ||
			(major == bestMajor && minor == bestMinor && patch > bestPatch) {
			best, bestMajor, bestMinor, bestPatch, found = v, major, minor, patch, true
		}
	}
	return best, found
}

// FindVersion returns the named version, if registered.
func (s *ComponentDefinitionSpec) FindVersion(v string) (VersionSpec, bool) {
	for _, ver := range s.Versions {
		if ver.Version == v {
			return ver, true
		}
	}
	return VersionSpec{}, false
}

// DeepCopyInto writes a deep copy of the receiver into out
func (in *ComponentDefinition) DeepCopyInto(out *ComponentDefinition) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	out.Status = in.Status
}

// DeepCopy creates a deep copy of ComponentDefinition
func (in *ComponentDefinition) DeepCopy() *ComponentDefinition {
	if in == nil {
		return nil
	}
	out := new(ComponentDefinition)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy as runtime.Object
func (in *ComponentDefinition) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto writes a deep copy of ComponentDefinitionList into out
func (in *ComponentDefinitionList) DeepCopyInto(out *ComponentDefinitionList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ComponentDefinition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy creates a deep copy of ComponentDefinitionList
func (in *ComponentDefinitionList) DeepCopy() *ComponentDefinitionList {
	if in == nil {
		return nil
	}
	out := new(ComponentDefinitionList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy as runtime.Object
func (in *ComponentDefinitionList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto for ComponentDefinitionSpec
func (in *ComponentDefinitionSpec) DeepCopyInto(out *ComponentDefinitionSpec) {
	*out = *in
	if in.Versions != nil {
		in, out := &in.Versions, &out.Versions
		*out = make([]VersionSpec, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.IO != nil {
		in, out := &in.IO, &out.IO
		*out = new(ComponentIO)
		**out = **in
	}
}

// DeepCopy for ComponentDefinitionSpec
func (in *ComponentDefinitionSpec) DeepCopy() *ComponentDefinitionSpec {
	if in == nil {
		return nil
	}
	out := new(ComponentDefinitionSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for VersionSpec
func (in *VersionSpec) DeepCopyInto(out *VersionSpec) {
	*out = *in
	if in.Resources != nil {
		in, out := &in.Resources, &out.Resources
		*out = new(ComponentResources)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy for VersionSpec
func (in *VersionSpec) DeepCopy() *VersionSpec {
	if in == nil {
		return nil
	}
	out := new(VersionSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for ComponentResources
func (in *ComponentResources) DeepCopyInto(out *ComponentResources) {
	*out = *in
	in.Default.DeepCopyInto(&out.Default)
	if in.Max != nil {
		out.Max = in.Max.DeepCopy()
	}
}

// DeepCopy for ComponentResources
func (in *ComponentResources) DeepCopy() *ComponentResources {
	if in == nil {
		return nil
	}
	out := new(ComponentResources)
	in.DeepCopyInto(out)
	return out
}
