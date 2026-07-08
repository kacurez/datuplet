package config

import (
	corev1 "k8s.io/api/core/v1"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// FromCRD converts the typed CRD pipeline (the single validated
// representation) into the runtime view used by the runner, capability
// derivation, and gateway-config generation. It is the ONLY bridge
// between the two shapes (RFC 026 §4.3: no third dialect survives).
//
// Defaulting is applied here so the runtime view is identical to what the
// old YAML-decode parser produced: kubebuilder defaults are not applied by
// Go decode (only by the API server), so FromCRD reproduces them.
func FromCRD(p *datupletv1.Pipeline) (*Pipeline, error) {
	out := &Pipeline{
		APIVersion: p.APIVersion,
		Kind:       p.Kind,
		Metadata: Metadata{
			Name:   p.Name,
			Labels: p.Labels,
		},
		Spec: Spec{
			Gateway: gatewayFromCRD(p.Spec.Gateway),
		},
	}

	// Defaults for identity fields (old applyDefaults).
	if out.APIVersion == "" {
		out.APIVersion = DefaultAPIVersion
	}
	if out.Kind == "" {
		out.Kind = DefaultKind
	}

	for i := range p.Spec.Stages {
		stage := &p.Spec.Stages[i]
		s := Stage{Name: stage.Name}
		for j := range stage.Components {
			comp, err := componentFromCRD(&stage.Components[j])
			if err != nil {
				return nil, err
			}
			s.Components = append(s.Components, comp)
		}
		out.Spec.Stages = append(out.Spec.Stages, s)
	}

	return out, nil
}

// gatewayFromCRD copies gateway settings and reproduces the old
// applyDefaults gateway defaulting.
func gatewayFromCRD(g datupletv1.GatewaySpec) GatewayConfig {
	out := GatewayConfig{
		ChunkSize:      g.ChunkSize,
		BufferSize:     g.BufferSize,
		RowGroupSize:   g.RowGroupSize,
		TargetFileSize: g.TargetFileSize,
	}
	if out.ChunkSize == 0 {
		out.ChunkSize = DefaultChunkSize
	}
	if out.BufferSize == 0 {
		out.BufferSize = DefaultBufferSize
	}
	if out.RowGroupSize == 0 {
		out.RowGroupSize = out.BufferSize // Default to buffer size.
	}
	if out.TargetFileSize == 0 {
		out.TargetFileSize = DefaultTargetFileSize
	}
	return out
}

func componentFromCRD(c *datupletv1.ComponentSpec) (Component, error) {
	cfg, err := c.ConfigMap()
	if err != nil {
		return Component{}, err
	}
	return Component{
		Name:      c.Name,
		Image:     c.Image,
		Config:    cfg,
		Inputs:    inputsFromCRD(c.Inputs),
		Outputs:   outputsFromCRD(c.Outputs),
		Resources: resourcesFromCRD(c.Resources),
	}, nil
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

// outputsFromCRD maps outputs and reproduces the old applyOutputDefaults
// write-mode defaulting (empty -> FULL_LOAD).
func outputsFromCRD(o *datupletv1.OutputSpec) *OutputSpec {
	if o == nil {
		return nil
	}
	out := &OutputSpec{
		DefaultBucket:    o.DefaultBucket,
		DefaultWriteMode: o.DefaultWriteMode,
	}
	if out.DefaultBucket != "" && out.DefaultWriteMode == "" {
		out.DefaultWriteMode = DefaultWriteMode
	}

	for _, b := range o.Buckets {
		mode := b.WriteMode
		if mode == "" {
			mode = DefaultWriteMode
		}
		out.Buckets = append(out.Buckets, OutputBucketSpec{
			Name:      b.Name,
			WriteMode: mode,
		})
	}

	for _, t := range o.Tables {
		mode := t.WriteMode
		if mode == "" {
			mode = DefaultWriteMode
		}
		ot := OutputTableSpec{
			Name:        t.Name,
			Bucket:      t.Bucket,
			WriteMode:   mode,
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
