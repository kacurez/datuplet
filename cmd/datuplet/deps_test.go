package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestCLIBinaryHasNoDataPlaneDeps(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	for _, banned := range []string{
		"arrow-go",
		"iceberg-go",
		"google.golang.org/grpc",
		"pkg/datupleticeio",
		"pkg/datagateway",
	} {
		for _, dep := range strings.Split(string(out), "\n") {
			if strings.Contains(dep, banned) {
				t.Errorf("CLI binary depends on %q (via %s); the gateway belongs in cmd/gateway", banned, dep)
				break
			}
		}
	}
}
