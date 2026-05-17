package k8s

import (
	"errors"
	"fmt"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewRESTConfig builds a *rest.Config from the given ClientOpts. Used by
// NewClient (for the HTTP admin/create paths) and by NewObserver (which
// hands it to controller-runtime's manager).
func NewRESTConfig(opts ClientOpts) (*rest.Config, error) {
	if opts.InCluster {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster config: %w", err)
		}
		return cfg, nil
	}
	if opts.KubeconfigPath == "" {
		return nil, errors.New("KubeconfigPath is required when InCluster is false")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", opts.KubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("kubeconfig %s: %w", opts.KubeconfigPath, err)
	}
	return cfg, nil
}
