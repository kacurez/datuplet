package v1

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PipelinePhase represents the phase of a Pipeline
type PipelinePhase string

const (
	// PipelinePhasePending indicates the pipeline is being validated
	PipelinePhasePending PipelinePhase = "Pending"
	// PipelinePhaseReady indicates the pipeline is valid and ready to run
	PipelinePhaseReady PipelinePhase = "Ready"
	// PipelinePhaseInvalid indicates the pipeline has validation errors
	PipelinePhaseInvalid PipelinePhase = "Invalid"
)

// PipelineSpec defines the desired state of Pipeline
type PipelineSpec struct {
	// Gateway configures the data gateway settings
	// +optional
	Gateway GatewaySpec `json:"gateway,omitempty"`

	// SecretsRef names a Kubernetes Secret in the same namespace whose keys
	// are resolvable via $[name] references in component configs. Optional.
	// +optional
	SecretsRef *SecretsRef `json:"secretsRef,omitempty"`

	// Stages defines the pipeline stages
	// +kubebuilder:validation:MinItems=1
	Stages []StageSpec `json:"stages"`
}

// SecretsRef is a reference to a Kubernetes Secret whose keys back $[name]
// secret references in the pipeline's component configs.
type SecretsRef struct {
	// Name is the Secret's name in the same namespace as the PipelineRun.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
}

// GatewaySpec configures the data gateway sidecar
type GatewaySpec struct {
	// ChunkSize is the chunk size for component read/write operations
	// +kubebuilder:default=33554432
	// +optional
	ChunkSize int64 `json:"chunkSize,omitempty"`

	// BufferSize is the memory buffer size before flushing to disk
	// +kubebuilder:default=67108864
	// +optional
	BufferSize int64 `json:"bufferSize,omitempty"`

	// RowGroupSize is the target size for Parquet row groups
	// +kubebuilder:default=67108864
	// +optional
	RowGroupSize int64 `json:"rowGroupSize,omitempty"`

	// TargetFileSize is the target Parquet file size before rotation
	// +kubebuilder:default=134217728
	// +optional
	TargetFileSize int64 `json:"targetFileSize,omitempty"`
}

// StageSpec defines a pipeline stage
type StageSpec struct {
	// Name is the stage name
	Name string `json:"name"`

	// Components defines the components in this stage
	// +kubebuilder:validation:MinItems=1
	Components []ComponentSpec `json:"components"`
}

// InputSpec defines input configuration for a component.
// Components can read from:
//   - Specific tables (explicit bucket.table references)
//   - Entire buckets (access to all tables in bucket)
type InputSpec struct {
	// Buckets grants read access to all tables in these buckets.
	// +optional
	Buckets []string `json:"buckets,omitempty"`

	// Tables grants read access to specific tables.
	// +optional
	Tables []InputTableSpec `json:"tables,omitempty"`
}

// InputTableSpec defines a specific table input.
type InputTableSpec struct {
	// Bucket is the bucket name (e.g., "raw")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_-]*$`
	Bucket string `json:"bucket"`

	// Table is the table name (e.g., "orders")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_-]*$`
	Table string `json:"table"`

	// LogicalName is an optional override for the SQL identifier used to
	// reference this input table in SQL transform components. Defaults
	// to Table when not specified. sql-transform reads it via the SDK and
	// registers the DuckDB view under this alias.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-zA-Z_][a-zA-Z0-9_]*$`
	LogicalName string `json:"logicalName,omitempty"`

	// Since is a relative duration for incremental reads (e.g., "30m", "12h", "3d", "1w").
	// Mutually exclusive with SinceSnapshot.
	// +optional
	Since string `json:"since,omitempty"`

	// SinceSnapshot is an explicit Iceberg snapshot ID for incremental reads.
	// Mutually exclusive with Since.
	// +optional
	SinceSnapshot *int64 `json:"sinceSnapshot,omitempty"`

	// SinceDays is sugar over Since: when set, DG applies an incremental read
	// for rows where TimestampColumn >= NOW - SinceDays days. Mutually exclusive
	// with Since and SinceSnapshot. Defaults to no filter when nil.
	// +optional
	// +kubebuilder:validation:Minimum=1
	SinceDays *int `json:"sinceDays,omitempty"`

	// TimestampColumn is the iceberg column DG uses to apply SinceDays / Since
	// filters. Defaults to "created" when SinceDays/Since is set and this field
	// is empty.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-zA-Z_][a-zA-Z0-9_]*$`
	TimestampColumn string `json:"timestampColumn,omitempty"`
}

// ComponentSpec defines a pipeline component
type ComponentSpec struct {
	// Name is the component name
	Name string `json:"name"`

	// Image is the container image to run
	Image string `json:"image"`

	// Config contains component-specific configuration as an arbitrary
	// structured object, validated against the component's registry
	// schema (RFC 026). Nested YAML is first-class.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Config apiextensionsv1.JSON `json:"config,omitempty"`

	// Inputs defines input configuration
	// +optional
	Inputs *InputSpec `json:"inputs,omitempty"`

	// Outputs defines output configuration
	// +optional
	Outputs *OutputSpec `json:"outputs,omitempty"`

	// Resources defines resource requirements
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ConfigMap decodes Config into a generic map. Nil-safe: an unset
// config yields (nil, nil).
func (c *ComponentSpec) ConfigMap() (map[string]any, error) {
	if len(c.Config.Raw) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(c.Config.Raw, &m); err != nil {
		return nil, fmt.Errorf("component %q config: %w", c.Name, err)
	}
	return m, nil
}

// OutputSpec defines output configuration for a component.
// Two exclusive modes are supported:
//  1. DefaultBucket mode: All writes go to one bucket with dynamic table names
//  2. Explicit mode: Specific buckets and/or tables declared
type OutputSpec struct {
	// DefaultBucket mode (exclusive - cannot be combined with Buckets/Tables)
	// Enables dynamic table creation: SDK calls WriteChunk(table, data)
	// +optional
	DefaultBucket string `json:"defaultBucket,omitempty"`

	// DefaultWriteMode is the write mode for tables created under DefaultBucket
	// +kubebuilder:validation:Enum=APPEND;FULL_LOAD
	// +kubebuilder:default=FULL_LOAD
	// +optional
	DefaultWriteMode string `json:"defaultWriteMode,omitempty"`

	// Explicit mode: declare specific buckets and/or tables
	// Buckets enables writes to any table within these buckets
	// +optional
	Buckets []OutputBucketSpec `json:"buckets,omitempty"`

	// Tables enables writes to specific pre-declared tables
	// +optional
	Tables []OutputTableSpec `json:"tables,omitempty"`

	// Processors apply transformations to output data (e.g., drop columns)
	// Applied to all outputs from this component
	// +optional
	Processors []ProcessorSpec `json:"processors,omitempty"`
}

// OutputBucketSpec defines a bucket output with dynamic table creation.
type OutputBucketSpec struct {
	// Name is the bucket name (e.g., "raw")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_-]*$`
	Name string `json:"name"`

	// WriteMode is the write mode for tables in this bucket
	// +kubebuilder:validation:Enum=APPEND;FULL_LOAD
	// +kubebuilder:default=APPEND
	// +optional
	WriteMode string `json:"writeMode,omitempty"`
}

// OutputTableSpec defines a fixed table output.
type OutputTableSpec struct {
	// Name is the table name (e.g., "products")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_-]*$`
	Name string `json:"name"`

	// Bucket is the bucket name (e.g., "curated")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_-]*$`
	Bucket string `json:"bucket"`

	// LogicalName is an optional override for the SQL identifier used to
	// reference this output table in SQL transform components. Defaults
	// to Name when not specified. sql-transform reads it via the SDK so
	// users can write `CREATE TABLE <logicalName>` independent of the
	// physical iceberg table name.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-zA-Z_][a-zA-Z0-9_]*$`
	LogicalName string `json:"logicalName,omitempty"`

	// WriteMode is the write mode for this table
	// +kubebuilder:validation:Enum=APPEND;FULL_LOAD
	// +kubebuilder:default=APPEND
	// +optional
	WriteMode string `json:"writeMode,omitempty"`

	// PartitionFields defines the partition specification for this table.
	// +optional
	PartitionFields []PartitionFieldSpec `json:"partitionFields,omitempty"`
}

// PartitionFieldSpec defines a single partition field.
type PartitionFieldSpec struct {
	// SourceColumn is the column name in source data to partition by.
	// +kubebuilder:validation:MinLength=1
	SourceColumn string `json:"sourceColumn"`

	// Transform is the partition transform to apply.
	// +kubebuilder:validation:Enum=identity;day;month;year;hour
	Transform string `json:"transform"`
}

// ProcessorSpec defines a data processor operation
type ProcessorSpec struct {
	// Type is the processor type (drop)
	// +kubebuilder:validation:Enum=drop
	Type string `json:"type"`

	// Columns is the list of columns for the operation
	// +optional
	Columns []string `json:"columns,omitempty"`
}

// PipelineStatus defines the observed state of Pipeline
type PipelineStatus struct {
	// Phase is the current phase of the Pipeline
	// +optional
	Phase PipelinePhase `json:"phase,omitempty"`

	// Message provides additional information about the current phase
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the last generation observed by the controller
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the Pipeline's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Pipeline is the Schema for the pipelines API
type Pipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineSpec   `json:"spec,omitempty"`
	Status PipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelineList contains a list of Pipeline
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pipeline `json:"items"`
}

// DeepCopyInto writes a deep copy of the receiver into out
func (in *Pipeline) DeepCopyInto(out *Pipeline) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy creates a deep copy of Pipeline
func (in *Pipeline) DeepCopy() *Pipeline {
	if in == nil {
		return nil
	}
	out := new(Pipeline)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy as runtime.Object
func (in *Pipeline) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto writes a deep copy of PipelineList into out
func (in *PipelineList) DeepCopyInto(out *PipelineList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]Pipeline, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy creates a deep copy of PipelineList
func (in *PipelineList) DeepCopy() *PipelineList {
	if in == nil {
		return nil
	}
	out := new(PipelineList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy as runtime.Object
func (in *PipelineList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto for PipelineSpec
func (in *PipelineSpec) DeepCopyInto(out *PipelineSpec) {
	*out = *in
	out.Gateway = in.Gateway
	if in.SecretsRef != nil {
		in, out := &in.SecretsRef, &out.SecretsRef
		*out = new(SecretsRef)
		**out = **in
	}
	if in.Stages != nil {
		in, out := &in.Stages, &out.Stages
		*out = make([]StageSpec, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopyInto for SecretsRef
func (in *SecretsRef) DeepCopyInto(out *SecretsRef) {
	*out = *in
}

// DeepCopy creates a deep copy of SecretsRef
func (in *SecretsRef) DeepCopy() *SecretsRef {
	if in == nil {
		return nil
	}
	out := new(SecretsRef)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for StageSpec
func (in *StageSpec) DeepCopyInto(out *StageSpec) {
	*out = *in
	if in.Components != nil {
		in, out := &in.Components, &out.Components
		*out = make([]ComponentSpec, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopyInto for ComponentSpec
func (in *ComponentSpec) DeepCopyInto(out *ComponentSpec) {
	*out = *in
	in.Config.DeepCopyInto(&out.Config)
	if in.Inputs != nil {
		in, out := &in.Inputs, &out.Inputs
		*out = new(InputSpec)
		(*in).DeepCopyInto(*out)
	}
	if in.Outputs != nil {
		in, out := &in.Outputs, &out.Outputs
		*out = new(OutputSpec)
		(*in).DeepCopyInto(*out)
	}
	if in.Resources != nil {
		in, out := &in.Resources, &out.Resources
		*out = new(corev1.ResourceRequirements)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy for ComponentSpec
func (in *ComponentSpec) DeepCopy() *ComponentSpec {
	if in == nil {
		return nil
	}
	out := new(ComponentSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for InputSpec
func (in *InputSpec) DeepCopyInto(out *InputSpec) {
	*out = *in
	if in.Buckets != nil {
		in, out := &in.Buckets, &out.Buckets
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.Tables != nil {
		in, out := &in.Tables, &out.Tables
		*out = make([]InputTableSpec, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy for InputSpec
func (in *InputSpec) DeepCopy() *InputSpec {
	if in == nil {
		return nil
	}
	out := new(InputSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for InputTableSpec
func (in *InputTableSpec) DeepCopyInto(out *InputTableSpec) {
	*out = *in
	if in.SinceSnapshot != nil {
		in, out := &in.SinceSnapshot, &out.SinceSnapshot
		*out = new(int64)
		**out = **in
	}
	if in.SinceDays != nil {
		in, out := &in.SinceDays, &out.SinceDays
		*out = new(int)
		**out = **in
	}
}

// DeepCopy for InputTableSpec
func (in *InputTableSpec) DeepCopy() *InputTableSpec {
	if in == nil {
		return nil
	}
	out := new(InputTableSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for OutputSpec
func (in *OutputSpec) DeepCopyInto(out *OutputSpec) {
	*out = *in
	if in.Buckets != nil {
		in, out := &in.Buckets, &out.Buckets
		*out = make([]OutputBucketSpec, len(*in))
		copy(*out, *in)
	}
	if in.Tables != nil {
		in, out := &in.Tables, &out.Tables
		*out = make([]OutputTableSpec, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.Processors != nil {
		in, out := &in.Processors, &out.Processors
		*out = make([]ProcessorSpec, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopyInto for OutputBucketSpec
func (in *OutputBucketSpec) DeepCopyInto(out *OutputBucketSpec) {
	*out = *in
}

// DeepCopy for OutputBucketSpec
func (in *OutputBucketSpec) DeepCopy() *OutputBucketSpec {
	if in == nil {
		return nil
	}
	out := new(OutputBucketSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for OutputTableSpec
func (in *OutputTableSpec) DeepCopyInto(out *OutputTableSpec) {
	*out = *in
	if in.PartitionFields != nil {
		in, out := &in.PartitionFields, &out.PartitionFields
		*out = make([]PartitionFieldSpec, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy for OutputTableSpec
func (in *OutputTableSpec) DeepCopy() *OutputTableSpec {
	if in == nil {
		return nil
	}
	out := new(OutputTableSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopy for OutputSpec
func (in *OutputSpec) DeepCopy() *OutputSpec {
	if in == nil {
		return nil
	}
	out := new(OutputSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto for ProcessorSpec
func (in *ProcessorSpec) DeepCopyInto(out *ProcessorSpec) {
	*out = *in
	if in.Columns != nil {
		in, out := &in.Columns, &out.Columns
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

// DeepCopyInto for PipelineStatus
func (in *PipelineStatus) DeepCopyInto(out *PipelineStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}
