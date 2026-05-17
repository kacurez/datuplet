package k8s_test

import (
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

func TestNewClient_RejectsMissingKubeconfig(t *testing.T) {
	_, err := k8s.NewClient(k8s.ClientOpts{KubeconfigPath: "/nonexistent/kubeconfig", InCluster: false})
	if err == nil {
		t.Error("expected error for missing kubeconfig")
	}
}

func TestScheme_IncludesDatupletCRDs(t *testing.T) {
	sch := k8s.Scheme()
	gvks, _, err := sch.ObjectKinds(&datupletv1.Pipeline{})
	if err != nil {
		t.Fatalf("ObjectKinds: %v", err)
	}
	if len(gvks) == 0 {
		t.Error("Pipeline not registered in scheme")
	}
}
