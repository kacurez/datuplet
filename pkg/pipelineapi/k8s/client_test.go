package k8s_test

import (
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

func TestNewClient_RejectsMissingKubeconfig(t *testing.T) {
	_, err := k8s.NewClient(k8s.ClientOpts{KubeconfigPath: "/nonexistent/kubeconfig", InCluster: false})
	if err == nil {
		t.Error("expected error for missing kubeconfig")
	}
}
