package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

var runPrefix string

func TestMain(m *testing.M) {
	runPrefix = fmt.Sprintf("e2e-%04x", rand.Intn(0xFFFF))
	fmt.Printf("E2E run prefix: %s\n", runPrefix)

	// K8s is the only supported tier. When E2E_K8S=1 is set, run
	// SetupFGABootstrap to get a full harness (Authorizer +
	// LakekeeperManager + LakekeeperBaseURL + project ID + FGA
	// test-user tuples). Without it, SharedHarness() returns nil and
	// every scenario skips with a clear message.
	//
	// make e2e-k8s keeps port-forwards alive so DATUPLET_LAKEKEEPER_URL /
	// DATUPLET_OPENFGA_URL are reachable during the test run.
	if os.Getenv("E2E_K8S") == "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		h, err := framework.SetupFGABootstrap(ctx, framework.FGABootstrapConfig{})
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"E2E: FGA bootstrap failed (K8s scenarios will skip): %v\n", err)
		} else if err := framework.RegisterBuiltinComponents(ctx); err != nil {
			// Registration failing (e.g. ComponentDefinition CRD not installed on
			// an older cluster) must not silently let every scenario run against
			// an empty registry and fail FailedUser with a confusing "unknown
			// component" — skip cleanly instead, same as an FGA bootstrap failure.
			fmt.Fprintf(os.Stderr,
				"E2E: component registration failed (K8s scenarios will skip): %v\n", err)
		} else {
			framework.SetSharedHarness(h)
			fmt.Printf("E2E: bootstrap OK — lakekeeper project=%s, FGA store=%s\n",
				h.LakekeeperProjectID, h.OpenFGAStoreID)
		}
	}

	code := m.Run()
	os.Exit(code)
}
