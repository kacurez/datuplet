// Package preview holds the pure pieces of pipeline preview mode.
package preview

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

const maxBucketLen = 63

// RewriteResult is a PipelineDoc rewritten onto per-preview scratch namespaces.
type RewriteResult struct {
	Doc        *config.Pipeline
	Namespaces []string          // scratch namespaces, sorted, unique
	Logical    map[string]string // scratch -> logical bucket
}

// ScratchBucket maps a logical bucket to its scratch namespace for a preview.
func ScratchBucket(previewID uuid.UUID, bucket string) string {
	s := "pv-" + previewID.String()[:8] + "-" + bucket
	if len(s) > maxBucketLen {
		s = s[:maxBucketLen]
	}
	return strings.TrimRight(s, "-")
}

// Rewrite deep-copies doc, drops stages after untilStage (when set) and maps
// every output bucket, plus inputs that read a bucket produced by an earlier
// stage, onto scratch namespaces. Table names are never touched.
func Rewrite(doc *config.Pipeline, previewID uuid.UUID, untilStage string) (*RewriteResult, error) {
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("copy pipeline: %w", err)
	}
	var out config.Pipeline
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("copy pipeline: %w", err)
	}

	if untilStage != "" {
		idx := -1
		for i := range out.Stages {
			if out.Stages[i].Name == untilStage {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("until_stage %q is not a stage of this pipeline", untilStage)
		}
		out.Stages = out.Stages[:idx+1]
	}
	out.Name = "preview-" + previewID.String()[:8]

	logicalOf := map[string]string{} // scratch -> logical
	scratchOf := func(bucket string) (string, error) {
		s := ScratchBucket(previewID, bucket)
		if prev, ok := logicalOf[s]; ok && prev != bucket {
			return "", fmt.Errorf("buckets %q and %q both map to %q", prev, bucket, s)
		}
		logicalOf[s] = bucket
		return s, nil
	}

	produced := map[string]bool{} // logical buckets produced by earlier stages
	for i := range out.Stages {
		stageOutputs := map[string]bool{}
		for j := range out.Stages[i].Components {
			c := &out.Stages[i].Components[j]
			if in := c.Inputs; in != nil {
				for k, b := range in.Buckets {
					if produced[b] {
						s, err := scratchOf(b)
						if err != nil {
							return nil, err
						}
						in.Buckets[k] = s
					}
				}
				for k := range in.Tables {
					if b := in.Tables[k].Bucket; produced[b] {
						s, err := scratchOf(b)
						if err != nil {
							return nil, err
						}
						in.Tables[k].Bucket = s
					}
				}
			}
			o := c.Outputs
			if o == nil {
				continue
			}
			if o.DefaultBucket != "" {
				stageOutputs[o.DefaultBucket] = true
				s, err := scratchOf(o.DefaultBucket)
				if err != nil {
					return nil, err
				}
				o.DefaultBucket = s
			}
			for k := range o.Buckets {
				stageOutputs[o.Buckets[k].Name] = true
				s, err := scratchOf(o.Buckets[k].Name)
				if err != nil {
					return nil, err
				}
				o.Buckets[k].Name = s
			}
			for k := range o.Tables {
				stageOutputs[o.Tables[k].Bucket] = true
				s, err := scratchOf(o.Tables[k].Bucket)
				if err != nil {
					return nil, err
				}
				o.Tables[k].Bucket = s
			}
		}
		for b := range stageOutputs {
			produced[b] = true
		}
	}

	ns := make([]string, 0, len(logicalOf))
	for s := range logicalOf {
		ns = append(ns, s)
	}
	sort.Strings(ns)
	return &RewriteResult{Doc: &out, Namespaces: ns, Logical: logicalOf}, nil
}
