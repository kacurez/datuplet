package config

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// DocToCR and CRToDoc are the ONLY bridge between the flat, envelope-free
// PipelineDoc (RFC 027 §3) and the typed CRD-shaped datupletv1.Pipeline.
// They are pure structural conversions (no defaulting, no validation) and
// own the two field-name renames the CRD and the doc disagree on:
//   - InputTableSpec: doc "as"            <-> CRD "logicalName"
//   - PartitionFieldSpec: doc "source_column" <-> CRD "sourceColumn"
//
// PipelineSpec has no Description field (the CRD is unchanged by RFC 027 -
// see spec §3), so Description round-trips through the descriptionAnnotation
// ObjectMeta annotation instead of a spec field; this needs no CRD manifest
// change since `metadata` is left as a generic object in the CRD schema.
//
// Both directions are plain field-by-field struct copies (no reflection) so
// the round-trip test (convert_test.go) is a meaningful tripwire against
// silently dropped fields.

// descriptionAnnotation carries Pipeline.Description across the CR boundary
// since PipelineSpec has no Description field.
const descriptionAnnotation = "datuplet.io/description"

// DocToCR converts a PipelineDoc into the typed CRD representation, e.g. for
// admission or storage as a Pipeline custom resource.
func DocToCR(p *Pipeline) *datupletv1.Pipeline {
	cr := &datupletv1.Pipeline{
		TypeMeta:   metav1.TypeMeta{APIVersion: "datuplet.io/v1", Kind: "Pipeline"},
		ObjectMeta: metav1.ObjectMeta{Name: p.Name},
		Spec: datupletv1.PipelineSpec{
			Gateway: gatewayToCRD(p.Gateway),
		},
	}
	if p.Description != "" {
		cr.ObjectMeta.Annotations = map[string]string{descriptionAnnotation: p.Description}
	}
	for _, s := range p.Stages {
		cr.Spec.Stages = append(cr.Spec.Stages, stageToCRD(s))
	}
	return cr
}

// CRToDoc converts the typed CRD representation back into a PipelineDoc.
func CRToDoc(cr *datupletv1.Pipeline) *Pipeline {
	out := &Pipeline{
		Name:        cr.Name,
		Description: cr.ObjectMeta.Annotations[descriptionAnnotation],
		Gateway:     gatewayFromCRD(cr.Spec.Gateway),
	}
	for _, s := range cr.Spec.Stages {
		out.Stages = append(out.Stages, stageFromCRD(s))
	}
	return out
}

func gatewayToCRD(g GatewayConfig) datupletv1.GatewaySpec {
	return datupletv1.GatewaySpec{
		ChunkSize:      g.ChunkSize,
		BufferSize:     g.BufferSize,
		RowGroupSize:   g.RowGroupSize,
		TargetFileSize: g.TargetFileSize,
	}
}

func gatewayFromCRD(g datupletv1.GatewaySpec) GatewayConfig {
	return GatewayConfig{
		ChunkSize:      g.ChunkSize,
		BufferSize:     g.BufferSize,
		RowGroupSize:   g.RowGroupSize,
		TargetFileSize: g.TargetFileSize,
	}
}

func stageToCRD(s Stage) datupletv1.StageSpec {
	out := datupletv1.StageSpec{Name: s.Name}
	for _, c := range s.Components {
		out.Components = append(out.Components, componentToCRD(c))
	}
	return out
}

func stageFromCRD(s datupletv1.StageSpec) Stage {
	out := Stage{Name: s.Name}
	for _, c := range s.Components {
		out.Components = append(out.Components, componentFromCRD(c))
	}
	return out
}

func componentToCRD(c Component) datupletv1.ComponentSpec {
	return datupletv1.ComponentSpec{
		Name:      c.Name,
		Component: c.Component,
		Version:   c.Version,
		Config:    configToCRD(c.Config),
		Inputs:    inputsToCRD(c.Inputs),
		Outputs:   outputsToCRD(c.Outputs),
		Resources: resourcesToCRD(c.Resources),
	}
}

func componentFromCRD(c datupletv1.ComponentSpec) Component {
	return Component{
		Name:      c.Name,
		Component: c.Component,
		Version:   c.Version,
		Config:    configFromCRD(c.Config),
		Inputs:    inputsFromCRD(c.Inputs),
		Outputs:   outputsFromCRD(c.Outputs),
		Resources: resourcesFromCRD(c.Resources),
	}
}

// configToCRD marshals the runtime config map into the CRD's raw JSON blob.
// A nil/empty map yields a zero-value apiextensionsv1.JSON (no Raw bytes),
// mirroring configFromCRD's nil-safe unmarshal.
func configToCRD(m map[string]any) apiextensionsv1.JSON {
	if len(m) == 0 {
		return apiextensionsv1.JSON{}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		// Config is arbitrary user data validated elsewhere; a plain
		// map[string]any built from JSON/YAML decode always marshals.
		panic(err)
	}
	return apiextensionsv1.JSON{Raw: raw}
}

func configFromCRD(j apiextensionsv1.JSON) map[string]any {
	if len(j.Raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(j.Raw, &m); err != nil {
		// Same reasoning as configToCRD: Raw only ever came from a prior
		// marshal of a plain map[string]any.
		panic(err)
	}
	return m
}

func inputsToCRD(in *InputSpec) *datupletv1.InputSpec {
	if in == nil {
		return nil
	}
	out := &datupletv1.InputSpec{Buckets: in.Buckets}
	for _, t := range in.Tables {
		out.Tables = append(out.Tables, datupletv1.InputTableSpec{
			Bucket:          t.Bucket,
			Table:           t.Table,
			LogicalName:     t.As, // runtime "as" is the CRD logicalName.
			Since:           t.Since,
			SinceSnapshot:   t.SinceSnapshot,
			SinceDays:       t.SinceDays,
			TimestampColumn: t.TimestampColumn,
		})
	}
	return out
}

func inputsFromCRD(in *datupletv1.InputSpec) *InputSpec {
	if in == nil {
		return nil
	}
	out := &InputSpec{Buckets: in.Buckets}
	for _, t := range in.Tables {
		out.Tables = append(out.Tables, InputTableSpec{
			Bucket:          t.Bucket,
			Table:           t.Table,
			As:              t.LogicalName, // CRD logicalName is the runtime "as".
			Since:           t.Since,
			SinceSnapshot:   t.SinceSnapshot,
			SinceDays:       t.SinceDays,
			TimestampColumn: t.TimestampColumn,
		})
	}
	return out
}

func outputsToCRD(o *OutputSpec) *datupletv1.OutputSpec {
	if o == nil {
		return nil
	}
	out := &datupletv1.OutputSpec{
		DefaultBucket:    o.DefaultBucket,
		DefaultWriteMode: o.DefaultWriteMode,
	}
	for _, b := range o.Buckets {
		out.Buckets = append(out.Buckets, datupletv1.OutputBucketSpec{
			Name:      b.Name,
			WriteMode: b.WriteMode,
		})
	}
	for _, t := range o.Tables {
		ot := datupletv1.OutputTableSpec{
			Name:        t.Name,
			Bucket:      t.Bucket,
			WriteMode:   t.WriteMode,
			LogicalName: t.LogicalName,
		}
		for _, pf := range t.PartitionSpec {
			ot.PartitionFields = append(ot.PartitionFields, datupletv1.PartitionFieldSpec{
				SourceColumn: pf.SourceColumn,
				Transform:    pf.Transform,
			})
		}
		out.Tables = append(out.Tables, ot)
	}
	for _, p := range o.Processors {
		out.Processors = append(out.Processors, datupletv1.ProcessorSpec{
			Type:    p.Type,
			Columns: p.Columns,
		})
	}
	return out
}

func outputsFromCRD(o *datupletv1.OutputSpec) *OutputSpec {
	if o == nil {
		return nil
	}
	out := &OutputSpec{
		DefaultBucket:    o.DefaultBucket,
		DefaultWriteMode: o.DefaultWriteMode,
	}
	for _, b := range o.Buckets {
		out.Buckets = append(out.Buckets, OutputBucketSpec{
			Name:      b.Name,
			WriteMode: b.WriteMode,
		})
	}
	for _, t := range o.Tables {
		ot := OutputTableSpec{
			Name:        t.Name,
			Bucket:      t.Bucket,
			WriteMode:   t.WriteMode,
			LogicalName: t.LogicalName,
		}
		for _, pf := range t.PartitionFields {
			ot.PartitionSpec = append(ot.PartitionSpec, PartitionFieldSpec{
				SourceColumn: pf.SourceColumn,
				Transform:    pf.Transform,
			})
		}
		out.Tables = append(out.Tables, ot)
	}
	for _, p := range o.Processors {
		out.Processors = append(out.Processors, Processor{
			Type:    p.Type,
			Columns: p.Columns,
		})
	}
	return out
}

// resourcesToCRD expands the runtime's flat memory/cpu strings into the
// CRD's corev1.ResourceRequirements limits, only where set.
func resourcesToCRD(r *ResourceSpec) *corev1.ResourceRequirements {
	if r == nil || (r.Memory == "" && r.CPU == "") {
		return nil
	}
	limits := corev1.ResourceList{}
	if r.Memory != "" {
		limits[corev1.ResourceMemory] = resource.MustParse(r.Memory)
	}
	if r.CPU != "" {
		limits[corev1.ResourceCPU] = resource.MustParse(r.CPU)
	}
	return &corev1.ResourceRequirements{Limits: limits}
}

// resourcesFromCRD flattens the CRD's corev1.ResourceRequirements limits into
// the runtime's flat memory/cpu strings, only where set.
func resourcesFromCRD(r *corev1.ResourceRequirements) *ResourceSpec {
	if r == nil || len(r.Limits) == 0 {
		return nil
	}
	out := &ResourceSpec{}
	if q, ok := r.Limits[corev1.ResourceMemory]; ok {
		out.Memory = q.String()
	}
	if q, ok := r.Limits[corev1.ResourceCPU]; ok {
		out.CPU = q.String()
	}
	if out.Memory == "" && out.CPU == "" {
		return nil
	}
	return out
}
