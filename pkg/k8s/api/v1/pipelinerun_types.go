package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PipelineRunPhase represents the phase of a PipelineRun
type PipelineRunPhase string

const (
	// PipelineRunPhasePending indicates the run is waiting to start
	PipelineRunPhasePending PipelineRunPhase = "Pending"
	// PipelineRunPhaseRunning indicates the run is executing
	PipelineRunPhaseRunning PipelineRunPhase = "Running"
	// PipelineRunPhaseSucceeded indicates the run completed successfully
	PipelineRunPhaseSucceeded PipelineRunPhase = "Succeeded"
	// PipelineRunPhaseFailedUser indicates the run failed due to a user error
	PipelineRunPhaseFailedUser PipelineRunPhase = "FailedUser"
	// PipelineRunPhaseFailedApplication indicates the run failed due to an application error
	PipelineRunPhaseFailedApplication PipelineRunPhase = "FailedApplication"
)

const (
	// PipelineRunSecretsResolved is the condition Type reported on PipelineRun.Status
	// to surface the result of the gateway sidecar's $[name] secret resolution.
	// True once the gateway has resolved all references; False with a Reason below
	// on failure. Absent until the pod has progressed far enough to observe either.
	PipelineRunSecretsResolved = "SecretsResolved"

	// PipelineRunReasonSecretsRefMissing: the Secret object referenced by
	// Pipeline.spec.secretsRef.Name does not exist (surfaced via FailedMount).
	PipelineRunReasonSecretsRefMissing = "SecretsRefMissing"

	// PipelineRunReasonSecretNotFound: the Secret mounts successfully but a
	// $[name] reference pointed at a missing key file at gateway boot.
	PipelineRunReasonSecretNotFound = "SecretNotFound"

	// PipelineRunReasonSecretsResolved: all references resolved at gateway boot.
	PipelineRunReasonSecretsResolved = "Resolved"
)

// PipelineRunSpec defines the desired state of PipelineRun
type PipelineRunSpec struct {
	// PipelineRef references the Pipeline to run
	PipelineRef PipelineRef `json:"pipelineRef"`

	// RunID optionally specifies the execution identifier. If empty, the controller generates one.
	// Must be a valid UUID.
	// +optional
	RunID string `json:"runId,omitempty"`

	// RunTokenRef names a Kubernetes Secret in the same namespace whose
	// `token` key holds the single per-run RS256 JWT the gateway sidecar
	// uses to authenticate with lakekeeper. Optional; when absent no token
	// is mounted and lakekeeper rejects every catalog/STS call.
	// +optional
	RunTokenRef *RunTokenRef `json:"runTokenRef,omitempty"`
}

// RunTokenRef is a reference to a Kubernetes Secret holding the run-scoped JWT.
// Mirrors the shape of SecretsRef in pipeline_types.go.
type RunTokenRef struct {
	// Name is the Secret's name in the same namespace as the PipelineRun.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
}

// PipelineRef references a Pipeline
type PipelineRef struct {
	// Name is the name of the Pipeline resource
	Name string `json:"name"`
}

// PipelineRunStatus defines the observed state of PipelineRun
type PipelineRunStatus struct {
	// Phase is the current phase of the PipelineRun
	// +optional
	Phase PipelineRunPhase `json:"phase,omitempty"`

	// RunID is the unique execution identifier for this pipeline run
	// +optional
	RunID string `json:"runID,omitempty"`

	// CurrentStage is the name of the currently executing stage
	// +optional
	CurrentStage string `json:"currentStage,omitempty"`

	// StageStatuses tracks the status of each stage
	// +optional
	StageStatuses []StageStatus `json:"stageStatuses,omitempty"`

	// TableCommits tracks the TableCommit resources created for this run
	// +optional
	TableCommits []TableCommitRef `json:"tableCommits,omitempty"`

	// StartTime is when the run started
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the run completed
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message provides additional information about the current phase
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the last generation observed by the controller
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the PipelineRun's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// StageStatus tracks the status of a pipeline stage
type StageStatus struct {
	// Name is the stage name
	Name string `json:"name"`

	// Phase is the current phase of the stage
	Phase StagePhase `json:"phase"`

	// ComponentStatuses tracks the status of each component in the stage
	// +optional
	ComponentStatuses []ComponentStatus `json:"componentStatuses,omitempty"`

	// StartTime is when the stage started
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the stage completed
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message provides additional information
	// +optional
	Message string `json:"message,omitempty"`
}

// StagePhase represents the phase of a stage
type StagePhase string

const (
	// StagePhasePending indicates the stage is waiting to start
	StagePhasePending StagePhase = "Pending"
	// StagePhaseRunning indicates the stage is executing
	StagePhaseRunning StagePhase = "Running"
	// StagePhaseCommitting indicates the stage is committing outputs
	StagePhaseCommitting StagePhase = "Committing"
	// StagePhaseSucceeded indicates the stage completed successfully
	StagePhaseSucceeded StagePhase = "Succeeded"
	// StagePhaseFailedUser indicates the stage failed due to a user error
	StagePhaseFailedUser StagePhase = "FailedUser"
	// StagePhaseFailedApplication indicates the stage failed due to an application error
	StagePhaseFailedApplication StagePhase = "FailedApplication"
)

// ComponentStatus tracks the status of a component execution
type ComponentStatus struct {
	// Name is the component name
	Name string `json:"name"`

	// Phase is the current phase of the component
	Phase ComponentPhase `json:"phase"`

	// ExitCode is the container's exit code (nil if not yet terminated)
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`

	// PodName is the name of the Pod running this component
	// +optional
	PodName string `json:"podName,omitempty"`

	// StartTime is when the component started
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the component completed
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message provides additional information
	// +optional
	Message string `json:"message,omitempty"`
}

// ComponentPhase represents the phase of a component
type ComponentPhase string

const (
	// ComponentPhasePending indicates the component is waiting to start
	ComponentPhasePending ComponentPhase = "Pending"
	// ComponentPhaseRunning indicates the component is executing
	ComponentPhaseRunning ComponentPhase = "Running"
	// ComponentPhaseSucceeded indicates the component completed successfully
	ComponentPhaseSucceeded ComponentPhase = "Succeeded"
	// ComponentPhaseFailedUser indicates the component failed due to a user error
	ComponentPhaseFailedUser ComponentPhase = "FailedUser"
	// ComponentPhaseFailedApplication indicates the component failed due to an application error
	ComponentPhaseFailedApplication ComponentPhase = "FailedApplication"
)

// TableCommitRef references a TableCommit resource
type TableCommitRef struct {
	// Name is the name of the TableCommit resource
	Name string `json:"name"`

	// Bucket is the bucket being committed
	Bucket string `json:"bucket"`

	// Phase is the current phase of the TableCommit
	Phase TableCommitPhase `json:"phase"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pipeline",type="string",JSONPath=".spec.pipelineRef.name"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Stage",type="string",JSONPath=".status.currentStage"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// PipelineRun is the Schema for the pipelineruns API
type PipelineRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineRunSpec   `json:"spec,omitempty"`
	Status PipelineRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelineRunList contains a list of PipelineRun
type PipelineRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelineRun `json:"items"`
}

// DeepCopyInto writes a deep copy of the receiver into out
func (in *PipelineRun) DeepCopyInto(out *PipelineRun) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy creates a deep copy of PipelineRun
func (in *PipelineRun) DeepCopy() *PipelineRun {
	if in == nil {
		return nil
	}
	out := new(PipelineRun)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy as runtime.Object
func (in *PipelineRun) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto writes a deep copy of PipelineRunList into out
func (in *PipelineRunList) DeepCopyInto(out *PipelineRunList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]PipelineRun, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy creates a deep copy of PipelineRunList
func (in *PipelineRunList) DeepCopy() *PipelineRunList {
	if in == nil {
		return nil
	}
	out := new(PipelineRunList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy as runtime.Object
func (in *PipelineRunList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto for PipelineRunSpec
func (in *PipelineRunSpec) DeepCopyInto(out *PipelineRunSpec) {
	*out = *in
	out.PipelineRef = in.PipelineRef
	if in.RunTokenRef != nil {
		in, out := &in.RunTokenRef, &out.RunTokenRef
		*out = new(RunTokenRef)
		**out = **in
	}
}

// DeepCopyInto for RunTokenRef
func (in *RunTokenRef) DeepCopyInto(out *RunTokenRef) {
	*out = *in
}

// DeepCopy creates a deep copy of RunTokenRef
func (in *RunTokenRef) DeepCopy() *RunTokenRef {
	if in == nil {
		return nil
	}
	out := new(RunTokenRef)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for PipelineRunStatus
func (in *PipelineRunStatus) DeepCopyInto(out *PipelineRunStatus) {
	*out = *in
	if in.StageStatuses != nil {
		in, out := &in.StageStatuses, &out.StageStatuses
		*out = make([]StageStatus, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.TableCommits != nil {
		in, out := &in.TableCommits, &out.TableCommits
		*out = make([]TableCommitRef, len(*in))
		copy(*out, *in)
	}
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
	if in.CompletionTime != nil {
		in, out := &in.CompletionTime, &out.CompletionTime
		*out = (*in).DeepCopy()
	}
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopyInto for StageStatus
func (in *StageStatus) DeepCopyInto(out *StageStatus) {
	*out = *in
	if in.ComponentStatuses != nil {
		in, out := &in.ComponentStatuses, &out.ComponentStatuses
		*out = make([]ComponentStatus, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
	if in.CompletionTime != nil {
		in, out := &in.CompletionTime, &out.CompletionTime
		*out = (*in).DeepCopy()
	}
}

// DeepCopyInto for ComponentStatus
func (in *ComponentStatus) DeepCopyInto(out *ComponentStatus) {
	*out = *in
	if in.ExitCode != nil {
		in, out := &in.ExitCode, &out.ExitCode
		*out = new(int32)
		**out = **in
	}
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
	if in.CompletionTime != nil {
		in, out := &in.CompletionTime, &out.CompletionTime
		*out = (*in).DeepCopy()
	}
}
