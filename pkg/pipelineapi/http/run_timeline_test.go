package http

import (
	"testing"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

const timelineDoc = `{
  "name": "daily-orders",
  "stages": [
    {
      "name": "extract",
      "components": [
        {
          "name": "api",
          "component": "x",
          "inputs": {"buckets": ["api"]},
          "outputs": {"tables": [{"name": "orders", "bucket": "raw", "writeMode": "FULL_LOAD"}]}
        }
      ]
    },
    {
      "name": "transform",
      "components": [
        {
          "name": "sql",
          "component": "y",
          "inputs": {"tables": [{"bucket": "raw", "table": "orders"}]},
          "outputs": {"tables": [{"name": "orders_enriched", "bucket": "processed", "writeMode": "FULL_LOAD"}]}
        }
      ]
    }
  ]
}`

func mustParseTimelineDoc(t *testing.T) *config.Pipeline {
	t.Helper()
	doc, err := config.Parse([]byte(timelineDoc))
	if err != nil {
		t.Fatalf("parse timeline doc: %v", err)
	}
	return doc
}

func TestBuildTimeline_StagesImportsExports(t *testing.T) {
	snap := []byte(`[
	  {"name":"extract","phase":"Succeeded","startTime":"2026-06-16T14:02:12Z","completionTime":"2026-06-16T14:03:40Z"},
	  {"name":"transform","phase":"Running","startTime":"2026-06-16T14:03:41Z"}
	]`)
	stages, err := buildTimeline(snap, mustParseTimelineDoc(t))
	if err != nil {
		t.Fatalf("buildTimeline: %v", err)
	}
	if len(stages) != 2 {
		t.Fatalf("got %d stages, want 2", len(stages))
	}
	ex := stages[0]
	if ex.Name != "extract" || ex.Phase != "Succeeded" {
		t.Errorf("stage0 = %+v", ex)
	}
	if ex.DurationMS == nil || *ex.DurationMS != 88000 {
		t.Errorf("extract duration = %v, want 88000ms", ex.DurationMS)
	}
	if len(ex.Imported) != 1 || ex.Imported[0].Kind != "bucket" || ex.Imported[0].Label != "api" {
		t.Errorf("extract imported = %+v", ex.Imported)
	}
	if len(ex.Exported) != 1 || ex.Exported[0].Kind != "table" || ex.Exported[0].Label != "raw.orders" {
		t.Errorf("extract exported = %+v", ex.Exported)
	}
	tr := stages[1]
	if tr.DurationMS != nil {
		t.Errorf("running stage must have nil duration, got %v", *tr.DurationMS)
	}
	if len(tr.Imported) != 1 || tr.Imported[0].Label != "raw.orders" || tr.Imported[0].Kind != "table" {
		t.Errorf("transform imported = %+v", tr.Imported)
	}
}

func TestBuildTimeline_EmptyOrNull(t *testing.T) {
	doc := mustParseTimelineDoc(t)
	for _, in := range [][]byte{nil, []byte("null"), []byte(""), []byte("[]")} {
		stages, err := buildTimeline(in, doc)
		if err != nil {
			t.Fatalf("buildTimeline(%q): %v", in, err)
		}
		if stages != nil {
			t.Errorf("buildTimeline(%q) = %v, want nil", in, stages)
		}
	}
}

// TestBuildTimeline_NilDoc confirms a nil doc is non-fatal: stages still render
// from the snapshot, just with empty imported/exported sets.
func TestBuildTimeline_NilDoc(t *testing.T) {
	snap := []byte(`[{"name":"extract","phase":"Succeeded"}]`)
	stages, err := buildTimeline(snap, nil)
	if err != nil {
		t.Fatalf("buildTimeline: %v", err)
	}
	if len(stages) != 1 || stages[0].Name != "extract" {
		t.Fatalf("stages = %+v, want 1 extract stage", stages)
	}
	if len(stages[0].Imported) != 0 || len(stages[0].Exported) != 0 {
		t.Errorf("nil doc must yield empty imported/exported, got imported=%+v exported=%+v",
			stages[0].Imported, stages[0].Exported)
	}
}
