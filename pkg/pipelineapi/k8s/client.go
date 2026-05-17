// Package k8s wraps Kubernetes client operations pipeline-api needs:
// constructing a client, provisioning project namespaces, materializing
// Pipeline CRDs, creating PipelineRun + run-token Secret, mirroring CRD
// status into the DB, and GC-reaping stale PipelineRuns.
package k8s

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// ClientOpts controls how NewClient constructs a client.Client.
type ClientOpts struct {
	// KubeconfigPath is a filesystem path to a kubeconfig. Used when InCluster
	// is false (the typical dev setup).
	KubeconfigPath string
	// InCluster is true when running inside a Pod with a ServiceAccount token.
	// Takes precedence over KubeconfigPath.
	InCluster bool
}

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(datupletv1.AddToScheme(scheme))
	// corev1 is already in clientgoscheme — explicit import keeps it obvious
	// in `go doc` that we need Secret/Namespace types.
	_ = corev1.AddToScheme
}

// Scheme returns the runtime.Scheme with the Datuplet CRDs registered.
// Exported for tests that want to construct a fake client.
func Scheme() *runtime.Scheme { return scheme }

// NewClient returns a controller-runtime client. Delegates rest.Config
// construction to NewRESTConfig (shared with the Observer's Manager).
func NewClient(opts ClientOpts) (client.Client, error) {
	cfg, err := NewRESTConfig(opts)
	if err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}
