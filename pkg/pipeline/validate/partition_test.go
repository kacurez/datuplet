package validate

import (
	"os"
	"regexp"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

func TestValidatePartitionSpecTransforms(t *testing.T) {
	pf := func(pairs ...string) []datupletv1.PartitionFieldSpec {
		var out []datupletv1.PartitionFieldSpec
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, datupletv1.PartitionFieldSpec{SourceColumn: pairs[i], Transform: pairs[i+1]})
		}
		return out
	}
	for _, tc := range []struct {
		name   string
		fields []datupletv1.PartitionFieldSpec
		want   string // "" = valid
	}{
		{"identity", pf("c", "identity"), ""},
		{"year", pf("c", "year"), ""},
		{"month", pf("c", "month"), ""},
		{"day", pf("c", "day"), ""},
		{"hour", pf("c", "hour"), ""},
		{"void", pf("c", "void"), ""},
		{"bucket[16]", pf("c", "bucket[16]"), ""},
		{"truncate[4]", pf("c", "truncate[4]"), ""},
		{"bucket and truncate on one column", pf("c", "bucket[16]", "c", "truncate[4]"), ""},
		{"identity and day on one column", pf("c", "identity", "c", "day"), ""},
		{"bucket[0]", pf("c", "bucket[0]"), "invalid transform"},
		{"bucket[x]", pf("c", "bucket[x]"), "invalid transform"},
		{"bucket[-1]", pf("c", "bucket[-1]"), "invalid transform"},
		{"truncate[]", pf("c", "truncate[]"), "invalid transform"},
		{"unknown", pf("c", "week"), "invalid transform"},
		{"days", pf("c", "days"), "invalid transform"},
		{"non-canonical case", pf("c", "Day"), "invalid transform"},
		{"leading zero", pf("c", "bucket[04]"), "invalid transform"},
		{"year and day on one column", pf("c", "year", "c", "day"), "redundant"},
		{"exact duplicate", pf("c", "bucket[4]", "c", "bucket[4]"), "duplicate partition field bucket[4](c)"},
		{"year on two columns", pf("a", "year", "b", "year"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := validatePartitionSpec("comp", "tbl", tc.fields, "spec.tables[0]")
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("findings = %+v, want none", got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0].Message, tc.want) {
				t.Fatalf("findings = %+v, want one containing %q", got, tc.want)
			}
		})
	}
}

// The Pipeline CRD's transform pattern admits exactly what the validator
// accepts, so a PipelineDoc that passes PUT/validate is never rejected by
// the API server (and vice versa).
func TestPartitionTransformCRDPatternMatchesValidator(t *testing.T) {
	crd, err := os.ReadFile("../../../charts/datuplet-app/crds/datuplet.io_pipelines.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`pattern: '(\^\(identity[^']*)'`).FindSubmatch(crd)
	if m == nil {
		t.Fatal("no transform pattern in the Pipeline CRD")
	}
	re := regexp.MustCompile(string(m[1]))
	for _, s := range []string{
		"identity", "year", "month", "day", "hour", "void", "bucket[1]", "bucket[16]", "truncate[4]", "truncate[100]",
		"bucket[0]", "bucket[x]", "bucket[-1]", "truncate[]", "week", "days", "Day", "bucket[04]", "BUCKET[4]", " day", "",
	} {
		_, valid := parsePartitionTransform(s)
		if re.MatchString(s) != valid {
			t.Errorf("%q: CRD pattern match %v, validator %v", s, re.MatchString(s), valid)
		}
	}
}
