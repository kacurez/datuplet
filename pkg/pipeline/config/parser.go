// Package config provides types and parsing for pipeline YAML configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/datuplet/datuplet/pkg/pipeline/validate"
)

// ParseFile parses a pipeline YAML file and returns the Pipeline struct.
func ParseFile(path string) (*Pipeline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read pipeline file: %w", err)
	}

	return Parse(data)
}

// Parse strict-decodes and validates pipeline YAML through the single
// validation source (pkg/pipeline/validate), then converts the validated CRD
// object into the runtime Pipeline. Any validation finding yields a non-nil
// error (preserving the historical Parse error contract); there is no second
// dialect and no duplicated checks here.
func Parse(data []byte) (*Pipeline, error) {
	p, findings, err := validate.ValidatePipeline(data, nil)
	if err != nil {
		return nil, err
	}
	if len(findings) > 0 {
		return nil, findingsError(findings)
	}
	return FromCRD(p)
}

// findingsError joins validation findings into a single error, prefixing each
// message with its structured path so callers can still locate the problem.
func findingsError(findings []validate.Finding) error {
	msgs := make([]string, 0, len(findings))
	for _, f := range findings {
		if f.Path != "" {
			msgs = append(msgs, fmt.Sprintf("%s: %s", f.Path, f.Message))
		} else {
			msgs = append(msgs, f.Message)
		}
	}
	return errors.New(strings.Join(msgs, "; "))
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
