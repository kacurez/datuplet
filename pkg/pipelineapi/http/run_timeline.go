package http

import (
	"encoding/json"
	"time"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

type timelineTable struct {
	Kind   string `json:"kind"` // "table" | "bucket"
	Bucket string `json:"bucket,omitempty"`
	Table  string `json:"table,omitempty"`
	Label  string `json:"label"`
}

type timelineStage struct {
	Name        string          `json:"name"`
	Phase       string          `json:"phase"`
	StartedAt   string          `json:"started_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	DurationMS  *int64          `json:"duration_ms,omitempty"`
	Message     string          `json:"message,omitempty"`
	Imported    []timelineTable `json:"imported"`
	Exported    []timelineTable `json:"exported"`
}

// buildTimeline reconstructs the per-stage timeline from the persisted
// StageStatuses JSON snapshot, annotating each stage with the imported/exported
// tables/buckets DECLARED in the (current) pipeline doc. A nil/empty/"null"/"[]"
// snapshot returns (nil, nil) — "no timeline recorded". A nil doc is non-fatal:
// stages render with empty imported/exported.
func buildTimeline(stageStatusesJSON []byte, doc *config.Pipeline) ([]timelineStage, error) {
	if len(stageStatusesJSON) == 0 || string(stageStatusesJSON) == "null" {
		return nil, nil
	}
	var statuses []datupletv1.StageStatus
	if err := json.Unmarshal(stageStatusesJSON, &statuses); err != nil {
		return nil, err
	}
	if len(statuses) == 0 {
		return nil, nil // JSON "[]" → no timeline recorded
	}

	imp := map[string][]timelineTable{}
	exp := map[string][]timelineTable{}
	if doc != nil {
		for i := range doc.Stages {
			st := &doc.Stages[i]
			for j := range st.Components {
				c := &st.Components[j]
				if c.Inputs != nil {
					for _, b := range c.Inputs.Buckets {
						imp[st.Name] = appendUniqTable(imp[st.Name], timelineTable{Kind: "bucket", Bucket: b, Label: b})
					}
					for _, tbl := range c.Inputs.Tables {
						imp[st.Name] = appendUniqTable(imp[st.Name], timelineTable{Kind: "table", Bucket: tbl.Bucket, Table: tbl.Table, Label: tbl.Bucket + "." + tbl.Table})
					}
				}
				if c.Outputs != nil {
					if c.Outputs.DefaultBucket != "" {
						exp[st.Name] = appendUniqTable(exp[st.Name], timelineTable{Kind: "bucket", Bucket: c.Outputs.DefaultBucket, Label: c.Outputs.DefaultBucket})
					}
					for _, b := range c.Outputs.Buckets {
						exp[st.Name] = appendUniqTable(exp[st.Name], timelineTable{Kind: "bucket", Bucket: b.Name, Label: b.Name})
					}
					for _, tbl := range c.Outputs.Tables {
						exp[st.Name] = appendUniqTable(exp[st.Name], timelineTable{Kind: "table", Bucket: tbl.Bucket, Table: tbl.Name, Label: tbl.Bucket + "." + tbl.Name})
					}
				}
			}
		}
	}

	out := make([]timelineStage, 0, len(statuses))
	for _, s := range statuses {
		ts := timelineStage{
			Name:     s.Name,
			Phase:    string(s.Phase),
			Message:  s.Message,
			Imported: imp[s.Name],
			Exported: exp[s.Name],
		}
		if ts.Imported == nil {
			ts.Imported = []timelineTable{}
		}
		if ts.Exported == nil {
			ts.Exported = []timelineTable{}
		}
		if s.StartTime != nil {
			ts.StartedAt = s.StartTime.Time.UTC().Format(time.RFC3339)
		}
		if s.CompletionTime != nil {
			ts.CompletedAt = s.CompletionTime.Time.UTC().Format(time.RFC3339)
		}
		if s.StartTime != nil && s.CompletionTime != nil {
			ms := s.CompletionTime.Time.Sub(s.StartTime.Time).Milliseconds()
			ts.DurationMS = &ms
		}
		out = append(out, ts)
	}
	return out, nil
}

func appendUniqTable(xs []timelineTable, x timelineTable) []timelineTable {
	for _, e := range xs {
		if e.Kind == x.Kind && e.Bucket == x.Bucket && e.Table == x.Table {
			return xs
		}
	}
	return append(xs, x)
}
