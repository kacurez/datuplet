// Package backend is a fake stub used only by the notokenlog analyzer's
// testdata. The real package lives at
// github.com/datuplet/datuplet/pkg/datagateway/backend. The
// vendedTokenSource type is unexported in the real package; the analyzer
// matches the qualified name, so a same-named unexported type here is
// sufficient for an intra-package call exercise (see vts_use.go in this
// package — but kept here to expose the in-package type to other testdata
// files when the analyzer is run on this directory).
package backend

import "fmt"

// vendedTokenSource mirrors the shape of the real (unexported)
// vendedTokenSource. The analyzer must flag any formatter/logger call on
// values of this type even within the defining package.
type vendedTokenSource struct {
	token string
}

// inPackageBad demonstrates an in-package violation: the formatter receives
// a vendedTokenSource value, and the analyzer must flag it.
func inPackageBad(v vendedTokenSource) string {
	return fmt.Sprintf("ts=%v", v) // want `bearer-credential type`
}

// inPackagePointerBad demonstrates a pointer-receiver violation.
func inPackagePointerBad(v *vendedTokenSource) string {
	return fmt.Sprintf("ts=%+v", v) // want `bearer-credential type`
}
