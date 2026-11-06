package preview

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
)

var testID = uuid.MustParse("ab12cd34-0000-0000-0000-000000000000")

func twoStageDoc() *config.Pipeline {
	return &config.Pipeline{
		Name: "orig",
		Stages: []config.Stage{
			{Name: "extract", Components: []config.Component{{
				Name: "gen", Component: "data-generator",
				Outputs: &config.OutputSpec{DefaultBucket: "raw"},
			}}},
			{Name: "transform", Components: []config.Component{{
				Name: "sql", Component: "sql-transform",
				Inputs: &config.InputSpec{
					Buckets: []string{"raw"},
					Tables:  []config.InputTableSpec{{Bucket: "raw", Table: "data"}, {Bucket: "ref", Table: "countries"}},
				},
				Outputs: &config.OutputSpec{Tables: []config.OutputTableSpec{{Name: "s", Bucket: "curated", WriteMode: "FULL_LOAD"}}},
			}}},
		},
	}
}

func TestScratchBucket(t *testing.T) {
	if got := ScratchBucket(testID, "raw"); got != "pv-ab12cd34-raw" {
		t.Fatalf("got %q", got)
	}
	long := strings.Repeat("a", 59) + "-b"
	got := ScratchBucket(testID, long)
	if len(got) > 63 || !strings.HasPrefix(got, "pv-ab12cd34-") || strings.HasSuffix(got, "-") {
		t.Fatalf("bad scratch %q (len %d)", got, len(got))
	}
	long2 := strings.Repeat("a", 50) + "-" + strings.Repeat("b", 9)
	got = ScratchBucket(testID, long2)
	if len(got) != 62 || strings.HasSuffix(got, "-") {
		t.Fatalf("trailing dash not trimmed: %q (len %d)", got, len(got))
	}
}

func TestRewrite_ExplicitAndDynamic(t *testing.T) {
	res, err := Rewrite(twoStageDoc(), testID, "")
	if err != nil {
		t.Fatal(err)
	}
	d := res.Doc
	if d.Name != "preview-ab12cd34" {
		t.Fatalf("name %q", d.Name)
	}
	if got := d.Stages[0].Components[0].Outputs.DefaultBucket; got != "pv-ab12cd34-raw" {
		t.Fatalf("defaultBucket %q", got)
	}
	tb := d.Stages[1].Components[0].Outputs.Tables[0]
	if tb.Bucket != "pv-ab12cd34-curated" || tb.Name != "s" {
		t.Fatalf("table %+v", tb)
	}
	want := []string{"pv-ab12cd34-curated", "pv-ab12cd34-raw"}
	if strings.Join(res.Namespaces, ",") != strings.Join(want, ",") {
		t.Fatalf("namespaces %v", res.Namespaces)
	}
	if res.Logical["pv-ab12cd34-raw"] != "raw" || res.Logical["pv-ab12cd34-curated"] != "curated" {
		t.Fatalf("logical %v", res.Logical)
	}
}

func TestRewrite_ExplicitBuckets(t *testing.T) {
	doc := &config.Pipeline{Stages: []config.Stage{{Name: "a", Components: []config.Component{{
		Name: "c", Component: "x",
		Outputs: &config.OutputSpec{Buckets: []config.OutputBucketSpec{{Name: "b1", WriteMode: "APPEND"}}},
	}}}}}
	res, err := Rewrite(doc, testID, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Doc.Stages[0].Components[0].Outputs.Buckets[0]; got.Name != "pv-ab12cd34-b1" || got.WriteMode != "APPEND" {
		t.Fatalf("%+v", got)
	}
}

func TestRewrite_CrossStageInputs(t *testing.T) {
	res, err := Rewrite(twoStageDoc(), testID, "")
	if err != nil {
		t.Fatal(err)
	}
	in := res.Doc.Stages[1].Components[0].Inputs
	if in.Tables[0].Bucket != "pv-ab12cd34-raw" || in.Tables[0].Table != "data" {
		t.Fatalf("%+v", in.Tables[0])
	}
	if in.Tables[1].Bucket != "ref" || in.Tables[1].Table != "countries" {
		t.Fatalf("ref input must stay: %+v", in.Tables[1])
	}
	if len(in.Buckets) != 1 || in.Buckets[0] != "pv-ab12cd34-raw" {
		t.Fatalf("buckets %v", in.Buckets)
	}
	for _, ns := range res.Namespaces {
		if ns == "ref" || strings.Contains(ns, "ref") {
			t.Fatalf("ref leaked into namespaces: %v", res.Namespaces)
		}
	}
}

func TestRewrite_UntilStage(t *testing.T) {
	doc := twoStageDoc()
	doc.Stages = append(doc.Stages, config.Stage{Name: "load", Components: []config.Component{{
		Name: "w", Component: "x", Outputs: &config.OutputSpec{DefaultBucket: "final"},
	}}})
	res, err := Rewrite(doc, testID, "extract")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Doc.Stages) != 1 || res.Doc.Stages[0].Name != "extract" {
		t.Fatalf("stages %+v", res.Doc.Stages)
	}
	if len(res.Namespaces) != 1 || res.Namespaces[0] != "pv-ab12cd34-raw" {
		t.Fatalf("namespaces %v", res.Namespaces)
	}
	if _, err := Rewrite(doc, testID, "nope"); err == nil || !strings.Contains(err.Error(), `until_stage "nope" is not a stage of this pipeline`) {
		t.Fatalf("err %v", err)
	}
}

func TestRewrite_CollisionAfterTruncation(t *testing.T) {
	base := strings.Repeat("a", 55)
	b1, b2 := base+"1", base+"2"
	doc := &config.Pipeline{Stages: []config.Stage{{Name: "a", Components: []config.Component{{
		Name: "c", Component: "x",
		Outputs: &config.OutputSpec{Buckets: []config.OutputBucketSpec{{Name: b1}, {Name: b2}}},
	}}}}}
	_, err := Rewrite(doc, testID, "")
	if err == nil || !strings.Contains(err.Error(), b1) || !strings.Contains(err.Error(), b2) {
		t.Fatalf("err %v", err)
	}
}

func TestRewrite_DoesNotMutateInput(t *testing.T) {
	doc := twoStageDoc()
	before, _ := json.Marshal(doc)
	if _, err := Rewrite(doc, testID, "extract"); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(doc)
	if string(before) != string(after) {
		t.Fatalf("input mutated:\n%s\n%s", before, after)
	}
}

func TestRewrite_OutputValidates(t *testing.T) {
	res, err := Rewrite(twoStageDoc(), testID, "")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(res.Doc)
	if err != nil {
		t.Fatal(err)
	}
	_, findings := validate.ValidatePipelineDoc(raw, "preview-ab12cd34", nil, nil)
	for _, f := range findings {
		if f.Severity == "error" {
			t.Fatalf("finding: %+v", f)
		}
	}
}
