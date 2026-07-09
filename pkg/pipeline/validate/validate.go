// Package validate is the single source of pipeline validation. Both the
// pipeline-api save path and the kubectl/controller path decode into
// datupletv1.Pipeline and run these checks; violations are reported as
// structured Findings rather than a single joined error.
package validate

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/lib/secrets"
	"sigs.k8s.io/yaml"
)

// Finding is a single validation problem. The JSON tags are load-bearing:
// pipeline-api serializes these directly in a 400 response body. Only "error"
// is emitted in Phase 1; "warning" is reserved for Phase 1.5's secrets ladder.
type Finding struct {
	Path     string `json:"path"`
	Message  string `json:"message"`
	Severity string `json:"severity"` // "error" | "warning"
}

const (
	severityError   = "error"
	severityWarning = "warning"
)

// Validation patterns (ported verbatim from pkg/pipeline/config/parser.go).
var (
	// bucketNameRegex validates bucket names: lowercase alphanumeric and hyphens,
	// no dots. Must start and end with alphanumeric, 3-63 characters (DNS-safe).
	bucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

	// tableNameRegex validates table names: alphanumeric and underscores.
	tableNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// Write modes for table outputs (ported to avoid an import cycle with the
// config package, which now delegates to this package).
const (
	writeModeAppend   = "APPEND"
	writeModeFullLoad = "FULL_LOAD"
)

// validPartitionTransforms lists the allowed partition transforms (Phase 1).
var validPartitionTransforms = map[string]bool{
	"identity": true,
	"day":      true,
	"month":    true,
	"year":     true,
	"hour":     true,
}

// ValidatePipeline strict-decodes YAML into a datupletv1.Pipeline and runs the
// semantic checks. The returned error is reserved for non-YAML / IO-level
// problems; strict-decode failures AND semantic violations come back as
// Findings (a strict-decode failure is a single Finding carrying the decode
// error text with an empty Path).
//
// reg resolves each component against the component registry and validates its
// config against the resolved version's JSON Schema; a nil reg skips all
// registry resolution and schema checks (the non-registry semantic checks still
// run). Phase 3 extends this signature with a pol *Policy argument.
func ValidatePipeline(data []byte, reg RegistryView, pol *Policy) (*datupletv1.Pipeline, []Finding, error) {
	var p datupletv1.Pipeline
	if err := yaml.UnmarshalStrict(data, &p); err != nil {
		return nil, []Finding{{Path: "", Message: err.Error(), Severity: severityError}}, nil
	}
	return &p, ValidateTyped(&p, reg, pol), nil
}

// ValidateTyped runs the semantic checks on an already-decoded Pipeline.
// Controllers that hold typed CRs (not YAML) call this directly. A nil reg
// skips registry resolution and config-schema validation (see ValidatePipeline).
//
// pol is the Phase-3 extension of the Phase-2 signature: a non-nil *Policy adds
// gateway-bounds findings (a nil *Policy disables all bound checks). Resource
// reject rules (over-max / unlisted names) run whenever reg resolves a
// component, independent of pol (RFC 026 §4.4, §4.6).
func ValidateTyped(p *datupletv1.Pipeline, reg RegistryView, pol *Policy) []Finding {
	if p == nil {
		return []Finding{{Path: "", Message: "pipeline is nil", Severity: severityError}}
	}

	var findings []Finding

	if p.Name == "" {
		findings = append(findings, Finding{
			Path:     "metadata.name",
			Message:  "pipeline metadata.name is required",
			Severity: severityError,
		})
	}

	if len(p.Spec.Stages) == 0 {
		findings = append(findings, Finding{
			Path:     "spec.stages",
			Message:  "pipeline must have at least one stage",
			Severity: severityError,
		})
	}

	// Track available outputs for cross-stage input validation.
	availableTables := make(map[string]bool)  // bucket.table -> true
	availableBuckets := make(map[string]bool) // bucket -> true

	for i := range p.Spec.Stages {
		stage := &p.Spec.Stages[i]
		if stage.Name == "" {
			findings = append(findings, Finding{
				Path:     fmt.Sprintf("stages[%d].name", i),
				Message:  fmt.Sprintf("stage %d: name is required", i),
				Severity: severityError,
			})
		}
		if len(stage.Components) == 0 {
			findings = append(findings, Finding{
				Path:     fmt.Sprintf("stages[%d].components", i),
				Message:  fmt.Sprintf("stage %s: must have at least one component", stage.Name),
				Severity: severityError,
			})
		}

		for j := range stage.Components {
			comp := &stage.Components[j]
			findings = append(findings, validateComponent(comp, i, j, availableTables, availableBuckets)...)
			findings = append(findings, validateRegistry(comp, i, j, reg)...)

			// Register this component's outputs for subsequent stages.
			if comp.Outputs != nil {
				registerOutputs(comp.Outputs, availableTables, availableBuckets)
			}
		}
	}

	// Secret-reference syntax validation (pkg/lib/secrets). $[name] markers are
	// restricted to component.config — we walk only that subtree per component
	// and surface whole-scalar violations with the offending path.
	for i := range p.Spec.Stages {
		stage := &p.Spec.Stages[i]
		for j := range stage.Components {
			findings = append(findings, validateSecretRefs(&stage.Components[j], i, j)...)
		}
	}

	// Phase 3: gateway-knob bounds are enforced only when a policy is supplied.
	if pol != nil {
		findings = append(findings, checkGatewayBounds(p, pol)...)
	}

	return findings
}

// registerOutputs adds component outputs to available tables/buckets.
func registerOutputs(out *datupletv1.OutputSpec, tables, buckets map[string]bool) {
	if out.DefaultBucket != "" {
		buckets[out.DefaultBucket] = true
		return
	}
	for _, b := range out.Buckets {
		buckets[b.Name] = true
	}
	for _, t := range out.Tables {
		buckets[t.Bucket] = true
		tables[t.Bucket+"."+t.Name] = true
	}
}

// validateComponent validates a single component configuration.
func validateComponent(c *datupletv1.ComponentSpec, stageIdx, compIdx int, availableTables, availableBuckets map[string]bool) []Finding {
	var findings []Finding
	base := fmt.Sprintf("stages[%d].components[%d]", stageIdx, compIdx)

	if c.Name == "" {
		findings = append(findings, Finding{
			Path:     base + ".name",
			Message:  fmt.Sprintf("stage %d, component %d: name is required", stageIdx, compIdx),
			Severity: severityError,
		})
	}
	if c.Component == "" {
		findings = append(findings, Finding{
			Path:     base + ".component",
			Message:  fmt.Sprintf("component %s: component is required", c.Name),
			Severity: severityError,
		})
	}

	if c.Inputs != nil {
		findings = append(findings, validateInputs(c.Name, c.Inputs, stageIdx, compIdx, availableTables, availableBuckets)...)
	}
	if c.Outputs != nil {
		findings = append(findings, validateOutputs(c.Name, c.Outputs, stageIdx, compIdx)...)
	}

	hasInputs := c.Inputs != nil && (len(c.Inputs.Buckets) > 0 || len(c.Inputs.Tables) > 0)
	hasOutputs := c.Outputs != nil && (c.Outputs.DefaultBucket != "" || len(c.Outputs.Buckets) > 0 || len(c.Outputs.Tables) > 0)
	if !hasInputs && !hasOutputs {
		findings = append(findings, Finding{
			Path:     base,
			Message:  fmt.Sprintf("component %s: must have at least inputs or outputs defined", c.Name),
			Severity: severityError,
		})
	}

	return findings
}

// validateInputs validates component input configuration.
func validateInputs(compName string, in *datupletv1.InputSpec, stageIdx, compIdx int, availableTables, availableBuckets map[string]bool) []Finding {
	var findings []Finding
	base := fmt.Sprintf("stages[%d].components[%d].inputs", stageIdx, compIdx)

	// Validate bucket inputs.
	for k, bucket := range in.Buckets {
		if msg := bucketNameError(bucket); msg != "" {
			findings = append(findings, Finding{
				Path:     fmt.Sprintf("%s.buckets[%d]", base, k),
				Message:  fmt.Sprintf("component %s, input bucket: %s", compName, msg),
				Severity: severityError,
			})
		}
		// Check bucket is available from previous stages (skip for stage 0).
		if stageIdx > 0 && !availableBuckets[bucket] {
			findings = append(findings, Finding{
				Path:     fmt.Sprintf("%s.buckets[%d]", base, k),
				Message:  fmt.Sprintf("component %s: input bucket %q not available from previous stages", compName, bucket),
				Severity: severityError,
			})
		}
	}

	// Validate table inputs.
	for k, t := range in.Tables {
		tablePath := fmt.Sprintf("%s.tables[%d]", base, k)
		if msg := bucketNameError(t.Bucket); msg != "" {
			findings = append(findings, Finding{
				Path:     tablePath + ".bucket",
				Message:  fmt.Sprintf("component %s, input table bucket: %s", compName, msg),
				Severity: severityError,
			})
		}
		if msg := tableNameError(t.Table); msg != "" {
			findings = append(findings, Finding{
				Path:     tablePath + ".table",
				Message:  fmt.Sprintf("component %s, input table: %s", compName, msg),
				Severity: severityError,
			})
		}
		// since, sinceSnapshot and sinceDays are mutually exclusive.
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
			findings = append(findings, Finding{
				Path:     tablePath,
				Message:  fmt.Sprintf("component %s, input table %s.%s: since, sinceSnapshot and sinceDays are mutually exclusive", compName, t.Bucket, t.Table),
				Severity: severityError,
			})
		}
		// Validate since duration parses correctly.
		if t.Since != "" {
			if _, err := parseSinceDuration(t.Since); err != nil {
				findings = append(findings, Finding{
					Path:     tablePath + ".since",
					Message:  fmt.Sprintf("component %s, input table %s.%s: %s", compName, t.Bucket, t.Table, err.Error()),
					Severity: severityError,
				})
			}
		}
		// Validate sinceDays is positive.
		if t.SinceDays != nil && *t.SinceDays <= 0 {
			findings = append(findings, Finding{
				Path:     tablePath + ".sinceDays",
				Message:  fmt.Sprintf("component %s, input table %s.%s: sinceDays must be positive (got %d)", compName, t.Bucket, t.Table, *t.SinceDays),
				Severity: severityError,
			})
		}
		// Check table or bucket is available from previous stages (skip for stage 0).
		if stageIdx > 0 {
			tableKey := t.Bucket + "." + t.Table
			if !availableTables[tableKey] && !availableBuckets[t.Bucket] {
				findings = append(findings, Finding{
					Path:     tablePath,
					Message:  fmt.Sprintf("component %s: input table %q not available from previous stages", compName, tableKey),
					Severity: severityError,
				})
			}
		}
	}

	return findings
}

// validateOutputs validates component output configuration.
func validateOutputs(compName string, out *datupletv1.OutputSpec, stageIdx, compIdx int) []Finding {
	var findings []Finding
	base := fmt.Sprintf("stages[%d].components[%d].outputs", stageIdx, compIdx)

	hasDefaultBucket := out.DefaultBucket != ""
	hasBuckets := len(out.Buckets) > 0
	hasTables := len(out.Tables) > 0

	// DefaultBucket mode is exclusive.
	if hasDefaultBucket && (hasBuckets || hasTables) {
		findings = append(findings, Finding{
			Path:     base,
			Message:  fmt.Sprintf("component %s: defaultBucket is exclusive, cannot be combined with buckets or tables", compName),
			Severity: severityError,
		})
	}

	// Validate defaultBucket mode.
	if hasDefaultBucket {
		if msg := bucketNameError(out.DefaultBucket); msg != "" {
			findings = append(findings, Finding{
				Path:     base + ".defaultBucket",
				Message:  fmt.Sprintf("component %s, defaultBucket: %s", compName, msg),
				Severity: severityError,
			})
		}
		if msg := writeModeError(out.DefaultWriteMode); msg != "" {
			findings = append(findings, Finding{
				Path:     base + ".defaultWriteMode",
				Message:  fmt.Sprintf("component %s, defaultWriteMode: %s", compName, msg),
				Severity: severityError,
			})
		}
	}

	// Validate bucket outputs.
	for i, b := range out.Buckets {
		if msg := bucketNameError(b.Name); msg != "" {
			findings = append(findings, Finding{
				Path:     fmt.Sprintf("%s.buckets[%d].name", base, i),
				Message:  fmt.Sprintf("component %s, output bucket %d: %s", compName, i, msg),
				Severity: severityError,
			})
		}
		if msg := writeModeError(b.WriteMode); msg != "" {
			findings = append(findings, Finding{
				Path:     fmt.Sprintf("%s.buckets[%d].writeMode", base, i),
				Message:  fmt.Sprintf("component %s, output bucket %s: %s", compName, b.Name, msg),
				Severity: severityError,
			})
		}
	}

	// Validate table outputs.
	for i, t := range out.Tables {
		tablePath := fmt.Sprintf("%s.tables[%d]", base, i)
		if t.Name == "" {
			findings = append(findings, Finding{
				Path:     tablePath + ".name",
				Message:  fmt.Sprintf("component %s, output table %d: name is required", compName, i),
				Severity: severityError,
			})
		} else if msg := tableNameError(t.Name); msg != "" {
			findings = append(findings, Finding{
				Path:     tablePath + ".name",
				Message:  fmt.Sprintf("component %s, output table %d: %s", compName, i, msg),
				Severity: severityError,
			})
		}
		if msg := bucketNameError(t.Bucket); msg != "" {
			findings = append(findings, Finding{
				Path:     tablePath + ".bucket",
				Message:  fmt.Sprintf("component %s, output table %s bucket: %s", compName, t.Name, msg),
				Severity: severityError,
			})
		}
		if msg := writeModeError(t.WriteMode); msg != "" {
			findings = append(findings, Finding{
				Path:     tablePath + ".writeMode",
				Message:  fmt.Sprintf("component %s, output table %s: %s", compName, t.Name, msg),
				Severity: severityError,
			})
		}
		findings = append(findings, validatePartitionSpec(compName, t.Name, t.PartitionFields, tablePath)...)
	}

	// Validate processors.
	findings = append(findings, validateProcessors(compName, out.Processors, base)...)

	return findings
}

// bucketNameError returns the validation message for a bucket name, or "" if valid.
func bucketNameError(name string) string {
	if name == "" {
		return "bucket name is required"
	}
	if strings.Contains(name, ".") {
		return fmt.Sprintf("bucket name %q cannot contain dots", name)
	}
	if !bucketNameRegex.MatchString(name) {
		return fmt.Sprintf("bucket name %q is invalid: must be lowercase alphanumeric with hyphens, 3-63 chars", name)
	}
	return ""
}

// tableNameError returns the validation message for a table name, or "" if valid.
func tableNameError(name string) string {
	if name == "" {
		return "table name is required"
	}
	if !tableNameRegex.MatchString(name) {
		return fmt.Sprintf("table name %q is invalid: must be alphanumeric with underscores, start with letter or underscore", name)
	}
	return ""
}

// writeModeError returns the validation message for a write mode, or "" if valid.
func writeModeError(mode string) string {
	switch strings.ToUpper(mode) {
	case writeModeAppend, writeModeFullLoad, "":
		return ""
	default:
		return fmt.Sprintf("invalid writeMode %q, must be APPEND or FULL_LOAD", mode)
	}
}

// validateProcessors validates processor configurations.
func validateProcessors(compName string, processors []datupletv1.ProcessorSpec, base string) []Finding {
	var findings []Finding
	validTypes := map[string]bool{"drop": true}

	for i, proc := range processors {
		procPath := fmt.Sprintf("%s.processors[%d]", base, i)
		if !validTypes[proc.Type] {
			findings = append(findings, Finding{
				Path:     procPath + ".type",
				Message:  fmt.Sprintf("component %s, processor %d: invalid type %q (supported: drop)", compName, i, proc.Type),
				Severity: severityError,
			})
			continue
		}

		switch proc.Type {
		case "drop":
			if len(proc.Columns) == 0 {
				findings = append(findings, Finding{
					Path:     procPath + ".columns",
					Message:  fmt.Sprintf("component %s, processor %d: drop requires columns", compName, i),
					Severity: severityError,
				})
			}
		}
	}

	return findings
}

// validatePartitionSpec validates the partition specification on an output table.
func validatePartitionSpec(compName, tableName string, spec []datupletv1.PartitionFieldSpec, tablePath string) []Finding {
	if len(spec) == 0 {
		return nil
	}

	var findings []Finding
	seen := make(map[string]bool)
	for i, field := range spec {
		fieldPath := fmt.Sprintf("%s.partitionFields[%d]", tablePath, i)
		if field.SourceColumn == "" {
			findings = append(findings, Finding{
				Path:     fieldPath + ".sourceColumn",
				Message:  fmt.Sprintf("component %s, output table %s, partitionSpec[%d]: source_column is required", compName, tableName, i),
				Severity: severityError,
			})
		}
		if field.Transform == "" {
			findings = append(findings, Finding{
				Path:     fieldPath + ".transform",
				Message:  fmt.Sprintf("component %s, output table %s, partitionSpec[%d]: transform is required", compName, tableName, i),
				Severity: severityError,
			})
		} else if !validPartitionTransforms[field.Transform] {
			findings = append(findings, Finding{
				Path:     fieldPath + ".transform",
				Message:  fmt.Sprintf("component %s, output table %s, partitionSpec[%d]: invalid transform %q (supported: identity, day, month, year, hour)", compName, tableName, i, field.Transform),
				Severity: severityError,
			})
		}
		if field.SourceColumn != "" {
			if seen[field.SourceColumn] {
				findings = append(findings, Finding{
					Path:     fieldPath + ".sourceColumn",
					Message:  fmt.Sprintf("component %s, output table %s, partitionSpec: duplicate source_column %q", compName, tableName, field.SourceColumn),
					Severity: severityError,
				})
			}
			seen[field.SourceColumn] = true
		}
	}

	return findings
}

// validateSecretRefs enforces the whole-scalar $[name] secret-reference rule
// on every string leaf in a component's decoded config, reusing the shared
// pkg/lib/secrets validator.
func validateSecretRefs(c *datupletv1.ComponentSpec, stageIdx, compIdx int) []Finding {
	base := fmt.Sprintf("stages[%d].components[%d].config", stageIdx, compIdx)

	cfg, err := c.ConfigMap()
	if err != nil {
		return []Finding{{Path: base, Message: err.Error(), Severity: severityError}}
	}
	if cfg == nil {
		return nil
	}

	var findings []Finding
	walkStrings(cfg, base, func(path, s string) {
		if _, err := secrets.Validate(s); err != nil {
			findings = append(findings, Finding{Path: path, Message: err.Error(), Severity: severityError})
		}
	})
	return findings
}

// validateRegistry resolves a component against reg and validates its config
// against the resolved version's JSON Schema. A nil reg skips both (Phase-1
// callers during the R5 cutover; R6/R9 pass a real view). Resolution findings
// come back from reg.Resolve with component-relative paths ("component" /
// "version"), rewritten here under the component's stages[i].components[j] path.
func validateRegistry(c *datupletv1.ComponentSpec, stageIdx, compIdx int, reg RegistryView) []Finding {
	if reg == nil {
		return nil
	}
	base := fmt.Sprintf("stages[%d].components[%d]", stageIdx, compIdx)

	rc, resolveFindings := reg.Resolve(c.Component, c.Version)

	var findings []Finding
	for _, f := range resolveFindings {
		findings = append(findings, Finding{
			Path:     joinKey(base, f.Path),
			Message:  f.Message,
			Severity: f.Severity,
		})
	}

	if rc == nil {
		return findings
	}

	// Phase 3: reject resource requests/limits that exceed the resolved
	// version's registry Max, or name a resource not listed in Max. Checked
	// whenever the component resolves, independent of any config schema
	// (RFC 026 §4.4).
	findings = append(findings, checkResourcesAgainstMax(c.Resources, rc.Resources.Max, base, c.Component)...)

	if rc.ConfigSchema == nil {
		return findings
	}

	cfg, err := c.ConfigMap()
	if err != nil {
		// A malformed config map is already reported by validateSecretRefs.
		return findings
	}
	findings = append(findings, ValidateConfig(rc.ConfigSchema, rc.rawConfigSchema, cfg, base+".config")...)
	return findings
}

// parseSinceDuration parses a "since" duration string. Ported from
// pkg/pipeline/config to avoid an import cycle (config delegates to validate).
// Supported formats: "Nd" (days), "Nw" (weeks), and anything
// time.ParseDuration accepts ("30m", "12h"). N must be a positive integer for
// d/w suffixes.
func parseSinceDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration string")
	}

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

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid since duration %q: %w", s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("invalid since duration %q: must be positive", s)
	}
	return d, nil
}
