// Package notokenlog flags calls to fmt-style formatters and logging APIs
// that pass bearer-credential-carrying types (GCSCreds, oauth2.Token,
// vendedTokenSource) as arguments. See RFC 019 §4.10. The seed type list is
// extensible — add new types to the seeds slice as new backends land.
//
// Design (Option A from the Slice B.6 plan): the analyzer flags ANY seed-type
// argument passed to a formatter/logger, regardless of verb. This is
// intentionally conservative — it forbids %T on seed values too, because
// parsing the printf format string to pair verbs with args is brittle. The
// escape hatch when a seed value MUST appear in an error is to format only
// the safe fields (.Type(), .IssuedAt()) or the literal type name as a
// string, never the value.
package notokenlog

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the singleton analyzer registered by the cmd entry point.
var Analyzer = &analysis.Analyzer{
	Name:     "notokenlog",
	Doc:      "flags fmt-verb / log-arg uses of bearer-credential types (RFC 019 §4.10)",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// seeds names the qualified types whose values must never be passed to a
// formatter/logger as an unredacted value. Listed types are the ones
// carrying live bearer-credential material (a single field, the OAuth
// token / bearer, leaking which compromises a session).
//
// S3Creds (in pkg/catalogwriter) is intentionally excluded: the S3 path
// predates RFC 019 and its secrets are guarded by separate scrubBody
// rules + the AWS SDK's own credential handling. Add S3Creds here if the
// S3 backend ever gains a new structured-log call site that handles
// S3Creds values directly.
//
// Each entry is "<package-path>.<type-name>". Pointer receivers are
// handled by typeMatchesSeed unwrapping *T (recursively) before
// comparison. Extend this list as new credential-carrying types land.
// Keep it sorted alphabetically by package path for review ergonomics.
var seeds = []string{
	"github.com/datuplet/datuplet/pkg/catalogwriter.GCSCreds",
	"github.com/datuplet/datuplet/pkg/datagateway/backend.vendedTokenSource",
	"github.com/datuplet/datuplet/pkg/datupleticeio.refreshingTokenSource",
	"golang.org/x/oauth2.Token",
}

// loggerSet is the set of qualified function names that fan-out their args
// into formatted output. Anything outside this set is ignored. The covered
// surface is stdlib fmt/log plus any project-internal structured-log
// helpers — extend as new helpers land. Investigation (grep for
// `slog\.|logger\.|log\.Printf|...`) at the time of writing showed:
//   - No `log/slog` usage in pkg/cmd.
//   - No project-internal log wrapper.
//   - Only stdlib fmt.* and a few stdlib log.* calls.
//   - K8s controller-runtime logr.Logger (`logger.Info / .Error`) is
//     keyword-style and uses %v internally inside controller-runtime; we
//     do NOT cover it here. If a seed value were ever passed as a logr
//     value, that's a separate concern and we'd add the helper here.
var loggerSet = map[string]bool{
	"fmt.Print":    true,
	"fmt.Printf":   true,
	"fmt.Println":  true,
	"fmt.Sprint":   true,
	"fmt.Sprintf":  true,
	"fmt.Sprintln": true,
	"fmt.Errorf":   true,
	"fmt.Fprint":   true,
	"fmt.Fprintf":  true,
	"fmt.Fprintln": true,
	"log.Print":    true,
	"log.Printf":   true,
	"log.Println":  true,
	"log.Fatal":    true,
	"log.Fatalf":   true,
	"log.Fatalln":  true,
	"log.Panic":    true,
	"log.Panicf":   true,
	"log.Panicln":  true,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	nodeFilter := []ast.Node{(*ast.CallExpr)(nil)}
	insp.Preorder(nodeFilter, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		if !isFormatterOrLogger(pass, call) {
			return
		}
		for _, arg := range call.Args {
			t := pass.TypesInfo.TypeOf(arg)
			if name, ok := typeMatchesSeed(t); ok {
				pass.Reportf(arg.Pos(),
					"notokenlog: bearer-credential type %s passed to formatter/logger; redact before formatting (RFC 019 §4.10)",
					name)
			}
		}
	})
	return nil, nil
}

// isFormatterOrLogger returns true iff call.Fun resolves to a function in
// loggerSet. Selector expressions (`fmt.Printf(...)`) are the common shape;
// bare identifiers (only possible via dot-imports, which we don't use)
// are not supported.
func isFormatterOrLogger(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	obj := pass.TypesInfo.ObjectOf(sel.Sel)
	if obj == nil {
		return false
	}
	fn, ok := obj.(*types.Func)
	if !ok {
		return false
	}
	if fn.Pkg() == nil {
		return false
	}
	return loggerSet[fn.Pkg().Path()+"."+fn.Name()]
}

// typeMatchesSeed unwraps pointer types and reports whether the resolved
// named type matches any entry in seeds. Returns the qualified name used
// for the diagnostic message.
func typeMatchesSeed(t types.Type) (string, bool) {
	if t == nil {
		return "", false
	}
	// Unwrap pointer(s): **T → *T → T. A single level is typical but
	// recursive unwrapping handles generated or table-driven code that
	// wraps values in double pointers.
	for {
		ptr, ok := t.(*types.Pointer)
		if !ok {
			break
		}
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok {
		return "", false
	}
	obj := named.Obj()
	if obj == nil || obj.Pkg() == nil {
		return "", false
	}
	qualified := obj.Pkg().Path() + "." + obj.Name()
	for _, s := range seeds {
		if qualified == s {
			return qualified, true
		}
	}
	return "", false
}
