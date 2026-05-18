package notokenlog

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestAnalyzer runs the analyzer against the GOPATH-style testdata tree
// under ./testdata/src. The fake `catalogwriter`, `oauth2`, and `backend`
// packages mirror the qualified names of the real seed types so the
// analyzer's type-matching (which uses pkg.Path() + "." + Name()) resolves
// correctly without any module/replace gymnastics.
func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), Analyzer,
		"a",
		"github.com/datuplet/datuplet/pkg/datagateway/backend",
	)
}
