package authz

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/language/pkg/go/transformer"
)

// fgaModelDir is the model the datuplet-app chart ships and the authz-bootstrap
// Job uploads.
const fgaModelDir = "../../../charts/datuplet-app/files/fga"

// loadChartFGAModel compiles the chart's modular model (fga.mod + components).
// It returns the compiled model and the component paths fga.mod lists.
func loadChartFGAModel(t *testing.T) (*openfgav1.AuthorizationModel, []string) {
	t.Helper()
	modRaw, err := os.ReadFile(filepath.Join(fgaModelDir, "fga.mod"))
	if err != nil {
		t.Fatalf("read fga.mod: %v", err)
	}
	mod, err := transformer.TransformModFile(string(modRaw))
	if err != nil {
		t.Fatalf("parse fga.mod: %v", err)
	}
	var files []transformer.ModuleFile
	var paths []string
	for _, c := range mod.Contents.Value {
		b, err := os.ReadFile(filepath.Join(fgaModelDir, c.Value))
		if err != nil {
			t.Fatalf("read %s: %v", c.Value, err)
		}
		files = append(files, transformer.ModuleFile{Name: c.Value, Contents: string(b)})
		paths = append(paths, c.Value)
	}
	model, err := transformer.TransformModuleFilesToModel(files, mod.Schema.Value)
	if err != nil {
		t.Fatalf("compile FGA model: %v", err)
	}
	return model, paths
}

func TestFGAModelCompiles(t *testing.T) {
	model, paths := loadChartFGAModel(t)
	if _, err := graph.NewAuthorizationModelGraph(model); err != nil {
		t.Fatalf("build model graph: %v", err)
	}

	t.Run("fga.mod lists every shipped component", func(t *testing.T) {
		onDisk, err := filepath.Glob(filepath.Join(fgaModelDir, "components", "*.fga"))
		if err != nil {
			t.Fatal(err)
		}
		var disk []string
		for _, p := range onDisk {
			disk = append(disk, "components/"+filepath.Base(p))
		}
		got := slices.Clone(paths)
		slices.Sort(got)
		slices.Sort(disk)
		if !slices.Equal(got, disk) {
			t.Fatalf("fga.mod contents %v != components on disk %v", got, disk)
		}
		if len(paths) != 11 {
			t.Fatalf("fga.mod lists %d components, want 11 (Lakekeeper 4.13)", len(paths))
		}
	})

	t.Run("bootstrap helper lists fga.mod components in order", func(t *testing.T) {
		helpers, err := os.ReadFile("../../../charts/datuplet-app/templates/_helpers.tpl")
		if err != nil {
			t.Fatal(err)
		}
		m := regexp.MustCompile(`(?s)define "datuplet-app\.fgaModelComponents" -}}\n(.*?)\n\{\{- end`).FindSubmatch(helpers)
		if m == nil {
			t.Fatal(`define "datuplet-app.fgaModelComponents" not found in _helpers.tpl`)
		}
		var want []string
		for _, p := range paths {
			want = append(want, strings.TrimPrefix(p, "components/"))
		}
		if got := strings.Split(string(m[1]), "\n"); !slices.Equal(got, want) {
			t.Fatalf("helper lists %v, fga.mod lists %v", got, want)
		}
	})

	t.Run("datuplet extensions", func(t *testing.T) {
		const (
			p   = "project:p"
			w   = "warehouse:w"
			n   = "namespace:n"
			tbl = "lakekeeper_table:t"
			sib = "lakekeeper_table:sibling"
		)
		e := newModelEvaluator(t, model, [][3]string{
			{p, "project", w}, {w, "warehouse", p},
			{w, "parent", n}, {n, "namespace", w},
			{n, "parent", tbl}, {tbl, "child", n},
			{n, "parent", sib}, {sib, "child", n},
			{"user:viewer", "viewer", p},
			{"user:editor", "editor", p},
			{"user:reader", "explicit_reader", tbl},
		})
		cases := []struct {
			user, rel, obj string
			want           bool
		}{
			{"user:viewer", "can_read_data", tbl, true},
			{"user:viewer", "can_write_data", tbl, false},
			{"user:viewer", "describe_effective", p, true},
			{"user:viewer", "data_admin", p, false},
			{"user:viewer", "datuplet_member", p, true},
			{"user:editor", "can_read_data", tbl, true},
			{"user:editor", "can_write_data", tbl, true},
			{"user:editor", "describe_effective", p, true},
			{"user:editor", "data_admin", p, true},
			{"user:editor", "datuplet_member", p, true},
			{"user:reader", "can_read_data", tbl, true},
			{"user:reader", "can_read_data", sib, false},
			{"user:reader", "can_write_data", tbl, false},
			{"user:reader", "can_get_metadata", n, true},
			{"user:reader", "can_use", w, true},
			{"user:reader", "can_get_config", w, true},
			{"user:reader", "describe_effective", p, false},
			{"user:reader", "datuplet_member", p, false},
			{"user:nobody", "can_read_data", tbl, false},
			{"user:nobody", "describe_effective", p, false},
		}
		for _, c := range cases {
			if got := e.check(c.user, c.rel, c.obj, 0); got != c.want {
				t.Errorf("%s %s %s = %v, want %v", c.user, c.rel, c.obj, got, c.want)
			}
		}
	})
}

// modelEvaluator is a minimal in-memory Check over a compiled model: direct
// user grants, computed usersets, tuple-to-userset, union, intersection and
// difference. Enough to prove the Datuplet extension chains; the integration
// test runs the same model on a real OpenFGA server.
type modelEvaluator struct {
	t      *testing.T
	types  map[string]*openfgav1.TypeDefinition
	tuples [][3]string // user, relation, object
}

func newModelEvaluator(t *testing.T, m *openfgav1.AuthorizationModel, tuples [][3]string) *modelEvaluator {
	e := &modelEvaluator{t: t, types: map[string]*openfgav1.TypeDefinition{}, tuples: tuples}
	for _, td := range m.GetTypeDefinitions() {
		e.types[td.GetType()] = td
	}
	return e
}

func objType(obj string) string { typ, _, _ := strings.Cut(obj, ":"); return typ }

func (e *modelEvaluator) hasRelation(obj, rel string) bool {
	_, ok := e.types[objType(obj)].GetRelations()[rel]
	return ok
}

func (e *modelEvaluator) check(user, rel, obj string, depth int) bool {
	if !e.hasRelation(obj, rel) {
		e.t.Fatalf("relation %s not defined on %s", rel, objType(obj))
	}
	if depth > 64 {
		return false
	}
	return e.eval(user, rel, obj, e.types[objType(obj)].GetRelations()[rel], depth+1)
}

func (e *modelEvaluator) eval(user, rel, obj string, us *openfgav1.Userset, depth int) bool {
	switch u := us.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return slices.Contains(e.tuples, [3]string{user, rel, obj})
	case *openfgav1.Userset_ComputedUserset:
		return e.check(user, u.ComputedUserset.GetRelation(), obj, depth)
	case *openfgav1.Userset_TupleToUserset:
		via := u.TupleToUserset.GetTupleset().GetRelation()
		target := u.TupleToUserset.GetComputedUserset().GetRelation()
		for _, tp := range e.tuples {
			if tp[1] == via && tp[2] == obj && e.hasRelation(tp[0], target) && e.check(user, target, tp[0], depth) {
				return true
			}
		}
		return false
	case *openfgav1.Userset_Union:
		for _, c := range u.Union.GetChild() {
			if e.eval(user, rel, obj, c, depth) {
				return true
			}
		}
		return false
	case *openfgav1.Userset_Intersection:
		for _, c := range u.Intersection.GetChild() {
			if !e.eval(user, rel, obj, c, depth) {
				return false
			}
		}
		return true
	case *openfgav1.Userset_Difference:
		return e.eval(user, rel, obj, u.Difference.GetBase(), depth) && !e.eval(user, rel, obj, u.Difference.GetSubtract(), depth)
	default:
		e.t.Fatalf("unsupported userset %T on %s#%s", u, objType(obj), rel)
		return false
	}
}
