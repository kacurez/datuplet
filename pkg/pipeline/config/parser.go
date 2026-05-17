package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/datuplet/datuplet/pkg/lib/secrets"
	"gopkg.in/yaml.v3"
)

// Validation patterns
var (
	// bucketNameRegex validates bucket names: lowercase alphanumeric and hyphens, no dots
	// Must start and end with alphanumeric, 3-63 characters (DNS-safe)
	bucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

	// tableNameRegex validates table names: alphanumeric and underscores
	tableNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// ParseFile parses a pipeline YAML file and returns the Pipeline struct.
func ParseFile(path string) (*Pipeline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read pipeline file: %w", err)
	}

	return Parse(data)
}

// Parse parses pipeline YAML data and returns the Pipeline struct.
func Parse(data []byte) (*Pipeline, error) {
	var pipeline Pipeline
	if err := yaml.Unmarshal(data, &pipeline); err != nil {
		return nil, fmt.Errorf("failed to parse pipeline YAML: %w", err)
	}

	// Apply defaults
	applyDefaults(&pipeline)

	// Validate
	if err := Validate(&pipeline); err != nil {
		return nil, err
	}

	return &pipeline, nil
}

// applyDefaults sets default values for optional fields.
func applyDefaults(p *Pipeline) {
	if p.APIVersion == "" {
		p.APIVersion = DefaultAPIVersion
	}
	if p.Kind == "" {
		p.Kind = DefaultKind
	}

	// Apply gateway defaults
	if p.Spec.Gateway.ChunkSize == 0 {
		p.Spec.Gateway.ChunkSize = DefaultChunkSize
	}
	if p.Spec.Gateway.BufferSize == 0 {
		p.Spec.Gateway.BufferSize = DefaultBufferSize
	}
	if p.Spec.Gateway.RowGroupSize == 0 {
		p.Spec.Gateway.RowGroupSize = p.Spec.Gateway.BufferSize // Default to buffer size
	}
	if p.Spec.Gateway.TargetFileSize == 0 {
		p.Spec.Gateway.TargetFileSize = DefaultTargetFileSize
	}

	// Apply output defaults for each component
	for i := range p.Spec.Stages {
		for j := range p.Spec.Stages[i].Components {
			comp := &p.Spec.Stages[i].Components[j]
			if comp.Outputs != nil {
				applyOutputDefaults(comp.Outputs)
			}
		}
	}
}

// applyOutputDefaults sets default write modes for outputs.
func applyOutputDefaults(out *OutputSpec) {
	// Default write mode for defaultBucket mode
	if out.DefaultBucket != "" && out.DefaultWriteMode == "" {
		out.DefaultWriteMode = DefaultWriteMode
	}

	// Default write mode for bucket outputs
	for i := range out.Buckets {
		if out.Buckets[i].WriteMode == "" {
			out.Buckets[i].WriteMode = DefaultWriteMode
		}
	}

	// Default write mode for table outputs
	for i := range out.Tables {
		if out.Tables[i].WriteMode == "" {
			out.Tables[i].WriteMode = DefaultWriteMode
		}
	}
}

// Validate checks that the pipeline configuration is valid.
func Validate(p *Pipeline) error {
	// Basic validation
	if p.Metadata.Name == "" {
		return fmt.Errorf("pipeline metadata.name is required")
	}

	// Stages validation
	if len(p.Spec.Stages) == 0 {
		return fmt.Errorf("pipeline must have at least one stage")
	}

	// Track available outputs for input validation
	availableTables := make(map[string]bool)  // bucket.table -> true
	availableBuckets := make(map[string]bool) // bucket -> true

	for i, stage := range p.Spec.Stages {
		if stage.Name == "" {
			return fmt.Errorf("stage %d: name is required", i)
		}

		if len(stage.Components) == 0 {
			return fmt.Errorf("stage %s: must have at least one component", stage.Name)
		}

		for j, comp := range stage.Components {
			if err := validateComponent(&comp, i, j, availableTables, availableBuckets); err != nil {
				return err
			}

			// Register this component's outputs for subsequent stages
			if comp.Outputs != nil {
				registerOutputs(comp.Outputs, availableTables, availableBuckets)
			}
		}
	}

	// Secret-reference syntax validation (pkg/lib/secrets). $[name] markers
	// are restricted to component.config — we walk only that subtree per
	// component and surface syntax errors with the component identity prefixed.
	for i, stage := range p.Spec.Stages {
		for j, comp := range stage.Components {
			if comp.Config == nil {
				continue
			}
			if _, err := secrets.Validate(comp.Config); err != nil {
				return fmt.Errorf("stage %d (%s), component %d (%s): %w",
					i, stage.Name, j, comp.Name, err)
			}
		}
	}

	return nil
}

// registerOutputs adds component outputs to available tables/buckets.
func registerOutputs(out *OutputSpec, tables map[string]bool, buckets map[string]bool) {
	// DefaultBucket mode: register bucket
	if out.DefaultBucket != "" {
		buckets[out.DefaultBucket] = true
		return
	}

	// Explicit bucket outputs
	for _, b := range out.Buckets {
		buckets[b.Name] = true
	}

	// Explicit table outputs
	for _, t := range out.Tables {
		buckets[t.Bucket] = true
		tables[t.Bucket+"."+t.Name] = true
	}
}

// validateComponent validates a single component configuration.
func validateComponent(c *Component, stageIdx, compIdx int, availableTables, availableBuckets map[string]bool) error {
	if c.Name == "" {
		return fmt.Errorf("stage %d, component %d: name is required", stageIdx, compIdx)
	}

	if c.Image == "" {
		return fmt.Errorf("component %s: image is required", c.Name)
	}

	// Validate inputs
	if c.Inputs != nil {
		if err := validateInputs(c.Name, c.Inputs, stageIdx, availableTables, availableBuckets); err != nil {
			return err
		}
	}

	// Validate outputs
	if c.Outputs != nil {
		if err := validateOutputs(c.Name, c.Outputs); err != nil {
			return err
		}
	}

	// At least inputs or outputs must be defined
	hasInputs := c.Inputs != nil && (len(c.Inputs.Buckets) > 0 || len(c.Inputs.Tables) > 0)
	hasOutputs := c.Outputs != nil && (c.Outputs.DefaultBucket != "" || len(c.Outputs.Buckets) > 0 || len(c.Outputs.Tables) > 0)

	if !hasInputs && !hasOutputs {
		return fmt.Errorf("component %s: must have at least inputs or outputs defined", c.Name)
	}

	return nil
}

// validateInputs validates component input configuration.
func validateInputs(compName string, in *InputSpec, stageIdx int, availableTables, availableBuckets map[string]bool) error {
	// Validate bucket names
	for _, bucket := range in.Buckets {
		if err := validateBucketName(bucket); err != nil {
			return fmt.Errorf("component %s, input bucket: %w", compName, err)
		}
		// Check bucket is available from previous stages (skip for stage 0)
		if stageIdx > 0 && !availableBuckets[bucket] {
			return fmt.Errorf("component %s: input bucket %q not available from previous stages", compName, bucket)
		}
	}

	// Validate table inputs
	for _, t := range in.Tables {
		if err := validateBucketName(t.Bucket); err != nil {
			return fmt.Errorf("component %s, input table bucket: %w", compName, err)
		}
		if err := validateTableName(t.Table); err != nil {
			return fmt.Errorf("component %s, input table: %w", compName, err)
		}
		// since, sinceSnapshot and sinceDays are mutually exclusive
		sinceCount := 0
		if t.Since != "" {
			sinceCount++
		}
		if t.SinceSnapshot != nil {
			sinceCount++
		}
		if t.SinceDays != nil {
			sinceCount++
		}
		if sinceCount > 1 {
			return fmt.Errorf("component %s, input table %s.%s: since, sinceSnapshot and sinceDays are mutually exclusive", compName, t.Bucket, t.Table)
		}
		// Validate since duration parses correctly
		if t.Since != "" {
			if _, err := ParseSinceDuration(t.Since); err != nil {
				return fmt.Errorf("component %s, input table %s.%s: %w", compName, t.Bucket, t.Table, err)
			}
		}
		// Validate sinceDays is positive
		if t.SinceDays != nil && *t.SinceDays <= 0 {
			return fmt.Errorf("component %s, input table %s.%s: sinceDays must be positive (got %d)", compName, t.Bucket, t.Table, *t.SinceDays)
		}
		// Check table or bucket is available from previous stages (skip for stage 0)
		if stageIdx > 0 {
			tableKey := t.Bucket + "." + t.Table
			if !availableTables[tableKey] && !availableBuckets[t.Bucket] {
				return fmt.Errorf("component %s: input table %q not available from previous stages", compName, tableKey)
			}
		}
	}

	return nil
}

// validateOutputs validates component output configuration.
func validateOutputs(compName string, out *OutputSpec) error {
	hasDefaultBucket := out.DefaultBucket != ""
	hasBuckets := len(out.Buckets) > 0
	hasTables := len(out.Tables) > 0

	// DefaultBucket mode is exclusive
	if hasDefaultBucket && (hasBuckets || hasTables) {
		return fmt.Errorf("component %s: defaultBucket is exclusive, cannot be combined with buckets or tables", compName)
	}

	// Validate defaultBucket mode
	if hasDefaultBucket {
		if err := validateBucketName(out.DefaultBucket); err != nil {
			return fmt.Errorf("component %s, defaultBucket: %w", compName, err)
		}
		if err := validateWriteMode(out.DefaultWriteMode); err != nil {
			return fmt.Errorf("component %s, defaultWriteMode: %w", compName, err)
		}
	}

	// Validate bucket outputs
	for i, b := range out.Buckets {
		if err := validateBucketName(b.Name); err != nil {
			return fmt.Errorf("component %s, output bucket %d: %w", compName, i, err)
		}
		if err := validateWriteMode(b.WriteMode); err != nil {
			return fmt.Errorf("component %s, output bucket %s: %w", compName, b.Name, err)
		}
	}

	// Validate table outputs
	for i, t := range out.Tables {
		if t.Name == "" {
			return fmt.Errorf("component %s, output table %d: name is required", compName, i)
		}
		if err := validateTableName(t.Name); err != nil {
			return fmt.Errorf("component %s, output table %d: %w", compName, i, err)
		}
		if err := validateBucketName(t.Bucket); err != nil {
			return fmt.Errorf("component %s, output table %s bucket: %w", compName, t.Name, err)
		}
		if err := validateWriteMode(t.WriteMode); err != nil {
			return fmt.Errorf("component %s, output table %s: %w", compName, t.Name, err)
		}
		if err := validatePartitionSpec(compName, t.Name, t.PartitionSpec); err != nil {
			return err
		}
	}

	// Validate processors
	if err := validateProcessors(compName, out.Processors); err != nil {
		return err
	}

	return nil
}

// validateBucketName validates a bucket name is DNS-safe.
func validateBucketName(name string) error {
	if name == "" {
		return fmt.Errorf("bucket name is required")
	}
	if strings.Contains(name, ".") {
		return fmt.Errorf("bucket name %q cannot contain dots", name)
	}
	if !bucketNameRegex.MatchString(name) {
		return fmt.Errorf("bucket name %q is invalid: must be lowercase alphanumeric with hyphens, 3-63 chars", name)
	}
	return nil
}

// validateTableName validates a table name.
func validateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("table name is required")
	}
	if !tableNameRegex.MatchString(name) {
		return fmt.Errorf("table name %q is invalid: must be alphanumeric with underscores, start with letter or underscore", name)
	}
	return nil
}

// validateWriteMode validates the write mode value.
func validateWriteMode(mode string) error {
	switch strings.ToUpper(mode) {
	case WriteModeAppend, WriteModeFullLoad, "":
		return nil
	default:
		return fmt.Errorf("invalid writeMode %q, must be APPEND or FULL_LOAD", mode)
	}
}

// validateProcessors validates processor configurations.
func validateProcessors(compName string, processors []Processor) error {
	validTypes := map[string]bool{"drop": true}

	for i, proc := range processors {
		if !validTypes[proc.Type] {
			return fmt.Errorf("component %s, processor %d: invalid type %q (supported: drop)",
				compName, i, proc.Type)
		}

		switch proc.Type {
		case "drop":
			if len(proc.Columns) == 0 {
				return fmt.Errorf("component %s, processor %d: drop requires columns",
					compName, i)
			}
		}
	}

	return nil
}

// validPartitionTransforms lists the allowed partition transforms (Phase 1).
var validPartitionTransforms = map[string]bool{
	"identity": true,
	"day":      true,
	"month":    true,
	"year":     true,
	"hour":     true,
}

// validatePartitionSpec validates the partition specification on an output table.
func validatePartitionSpec(compName, tableName string, spec []PartitionFieldSpec) error {
	if len(spec) == 0 {
		return nil
	}

	seen := make(map[string]bool)
	for i, field := range spec {
		if field.SourceColumn == "" {
			return fmt.Errorf("component %s, output table %s, partitionSpec[%d]: source_column is required", compName, tableName, i)
		}
		if field.Transform == "" {
			return fmt.Errorf("component %s, output table %s, partitionSpec[%d]: transform is required", compName, tableName, i)
		}
		if !validPartitionTransforms[field.Transform] {
			return fmt.Errorf("component %s, output table %s, partitionSpec[%d]: invalid transform %q (supported: identity, day, month, year, hour)", compName, tableName, i, field.Transform)
		}
		if seen[field.SourceColumn] {
			return fmt.Errorf("component %s, output table %s, partitionSpec: duplicate source_column %q", compName, tableName, field.SourceColumn)
		}
		seen[field.SourceColumn] = true
	}

	return nil
}

// ParseSinceDuration parses a "since" duration string.
// Supported formats: "Nd" (days), "Nw" (weeks), and anything time.ParseDuration accepts ("30m", "12h").
// N must be a positive integer for d/w suffixes.
func ParseSinceDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration string")
	}

	// Handle day/week suffixes
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid since duration %q: must be a positive integer followed by d", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	if strings.HasSuffix(s, "w") {
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid since duration %q: must be a positive integer followed by w", s)
		}
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	}

	// Fallback to standard Go duration parsing (e.g., "30m", "12h")
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid since duration %q: %w", s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("invalid since duration %q: must be positive", s)
	}
	return d, nil
}

// Helper functions for extracting bucket/table information

// GetOutputBuckets returns all bucket names this component can write to.
func (c *Component) GetOutputBuckets() []string {
	if c.Outputs == nil {
		return nil
	}

	buckets := make(map[string]bool)

	if c.Outputs.DefaultBucket != "" {
		buckets[c.Outputs.DefaultBucket] = true
	}

	for _, b := range c.Outputs.Buckets {
		buckets[b.Name] = true
	}

	for _, t := range c.Outputs.Tables {
		buckets[t.Bucket] = true
	}

	result := make([]string, 0, len(buckets))
	for b := range buckets {
		result = append(result, b)
	}
	return result
}

// GetInputBuckets returns all bucket names this component can read from.
func (c *Component) GetInputBuckets() []string {
	if c.Inputs == nil {
		return nil
	}

	buckets := make(map[string]bool)

	for _, b := range c.Inputs.Buckets {
		buckets[b] = true
	}

	for _, t := range c.Inputs.Tables {
		buckets[t.Bucket] = true
	}

	result := make([]string, 0, len(buckets))
	for b := range buckets {
		result = append(result, b)
	}
	return result
}

// IsDefaultBucketMode returns true if the component uses defaultBucket mode.
func (o *OutputSpec) IsDefaultBucketMode() bool {
	return o != nil && o.DefaultBucket != ""
}

// GetWriteModeForBucket returns the write mode for a given bucket.
func (o *OutputSpec) GetWriteModeForBucket(bucket string) string {
	if o == nil {
		return DefaultWriteMode
	}

	if o.DefaultBucket == bucket {
		if o.DefaultWriteMode != "" {
			return strings.ToUpper(o.DefaultWriteMode)
		}
		return DefaultWriteMode
	}

	for _, b := range o.Buckets {
		if b.Name == bucket {
			if b.WriteMode != "" {
				return strings.ToUpper(b.WriteMode)
			}
			return DefaultWriteMode
		}
	}

	return DefaultWriteMode
}

// GetWriteModeForTable returns the write mode for a given table.
func (o *OutputSpec) GetWriteModeForTable(tableName string) string {
	if o == nil {
		return DefaultWriteMode
	}

	for _, t := range o.Tables {
		if t.Name == tableName {
			if t.WriteMode != "" {
				return strings.ToUpper(t.WriteMode)
			}
			return DefaultWriteMode
		}
	}

	return DefaultWriteMode
}
