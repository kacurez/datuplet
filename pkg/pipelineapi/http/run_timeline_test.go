package http

import (
	"testing"
)

const timelineYAML = `{
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

func TestBuildTimeline_StagesImportsExports(t *testing.T) {
	snap := []byte(`[
	  {"name":"extract","phase":"Succeeded","startTime":"2026-06-16T14:02:12Z","completionTime":"2026-06-16T14:03:40Z"},
	  {"name":"transform","phase":"Running","startTime":"2026-06-16T14:03:41Z"}
	]`)
	stages, err := buildTimeline(snap, timelineYAML)
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
	for _, in := range [][]byte{nil, []byte("null"), []byte(""), []byte("[]")} {
		stages, err := buildTimeline(in, timelineYAML)
		if err != nil {
			t.Fatalf("buildTimeline(%q): %v", in, err)
		}
		if stages != nil {
			t.Errorf("buildTimeline(%q) = %v, want nil", in, stages)
		}
	}
}
