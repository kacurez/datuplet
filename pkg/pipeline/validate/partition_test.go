package validate

import (
	"math"
	"os"
	"regexp"
	"strconv"
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
		{"bucket[1]", pf("c", "bucket[1]"), ""},
		{"bucket[MaxInt32]", pf("c", "bucket[2147483647]"), ""},
		{"truncate[MaxInt32]", pf("c", "truncate[2147483647]"), ""},
		{"bucket[MaxInt32+1]", pf("c", "bucket[2147483648]"), "invalid transform"},
		{"truncate[MaxInt32+1]", pf("c", "truncate[2147483648]"), "invalid transform"},
		{"bucket[9999999999]", pf("c", "bucket[9999999999]"), "invalid transform"},
		{"11-digit width", pf("c", "truncate[10000000000]"), "invalid transform"},
		{"truncate[0]", pf("c", "truncate[0]"), "invalid transform"},
		{"truncate[01]", pf("c", "truncate[01]"), "invalid transform"},
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
	types, err := os.ReadFile("../../k8s/api/v1/pipeline_types.go")
	if err != nil {
		t.Fatal(err)
	}
	marker := regexp.MustCompile("Pattern=`(\\^\\(identity[^`]*)`").FindSubmatch(types)
	if marker == nil || string(marker[1]) != string(m[1]) {
		t.Fatalf("kubebuilder transform marker %q differs from the CRD pattern %q", marker, m[1])
	}
	for _, s := range []string{
		"identity", "year", "month", "day", "hour", "void", "bucket[1]", "bucket[16]", "truncate[4]", "truncate[100]",
		"bucket[0]", "bucket[x]", "bucket[-1]", "truncate[]", "week", "days", "Day", "bucket[04]", "BUCKET[4]", " day", "",
		"bucket[01]", "truncate[0]", "truncate[01]", "bucket[2147483647]", "truncate[2147483647]",
		"bucket[2147483648]", "truncate[2147483648]", "bucket[9999999999]", "truncate[10000000000]",
	} {
		_, valid := parsePartitionTransform(s)
		if re.MatchString(s) != valid {
			t.Errorf("%q: CRD pattern match %v, validator %v", s, re.MatchString(s), valid)
		}
	}
	for _, s := range []string{"bucket[2147483647]", "truncate[1]"} {
		if !re.MatchString(s) {
			t.Errorf("CRD pattern rejects %q", s)
		}
	}
	for _, s := range []string{"bucket[2147483648]", "bucket[9999999999]", "truncate[10000000000]", "bucket[0]", "truncate[01]"} {
		if re.MatchString(s) {
			t.Errorf("CRD pattern admits %q", s)
		}
	}

	// Widths across every digit count and around every bound of the
	// 10-digit alternatives: the pattern agrees with the validator, which
	// admits exactly 1..MaxInt32.
	widths := []int64{0, 1, 9, 10, 99, 100}
	for p := int64(1000); p <= 100_000_000_000; p *= 10 {
		widths = append(widths, p-1, p, p+1)
	}
	for _, b := range []int64{
		1_999_999_999, 2_000_000_000, 2_099_999_999, 2_100_000_000, 2_139_999_999, 2_140_000_000,
		2_146_999_999, 2_147_000_000, 2_147_399_999, 2_147_400_000, 2_147_479_999, 2_147_480_000,
		2_147_482_999, 2_147_483_000, 2_147_483_599, 2_147_483_600, 2_147_483_639, 2_147_483_640,
		2_147_483_646, 2_147_483_647, 2_147_483_648, 2_147_483_649, 2_147_483_650, 2_147_483_700,
		2_147_484_000, 2_147_490_000, 2_147_500_000, 2_148_000_000, 2_150_000_000, 2_200_000_000,
		3_000_000_000, 9_999_999_999,
	} {
		widths = append(widths, b)
	}
	for w := int64(1_000_000_000); w < 10_000_000_000; w += 7_919_993 {
		widths = append(widths, w)
	}
	for _, w := range widths {
		for _, tr := range []string{"bucket", "truncate"} {
			s := tr + "[" + strconv.FormatInt(w, 10) + "]"
			_, valid := parsePartitionTransform(s)
			if want := w >= 1 && w <= math.MaxInt32; valid != want {
				t.Errorf("%q: validator %v, want %v", s, valid, want)
			}
			if re.MatchString(s) != valid {
				t.Errorf("%q: CRD pattern match %v, validator %v", s, re.MatchString(s), valid)
			}
		}
	}
}
