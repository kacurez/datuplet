// Package main is the entry point for the Pipeline operator.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/k8s/controllers"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	// Blank-import the centralised iceberg-go IO scheme registration
	// package so this binary's `gs://` factory is the Datuplet override.
	// See pkg/datupleticeio/doc.go and RFC 019 §4.5.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(datupletv1.AddToScheme(scheme))
}

// loadRuntimeTolerations reads DATUPLET_RUN_TOLERATIONS_JSON from the
// environment and returns the parsed slice. Returns nil (no error) when the
// env var is unset or empty. Fails fast on invalid JSON or unknown fields so
// operator startup surfaces misconfiguration immediately.
func loadRuntimeTolerations() ([]corev1.Toleration, error) {
	raw := os.Getenv("DATUPLET_RUN_TOLERATIONS_JSON")
	if raw == "" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var ts []corev1.Toleration
	if err := dec.Decode(&ts); err != nil {
		return nil, fmt.Errorf("DATUPLET_RUN_TOLERATIONS_JSON: invalid JSON: %w", err)
	}
	for i, t := range ts {
		if err := validateToleration(t); err != nil {
			return nil, fmt.Errorf("DATUPLET_RUN_TOLERATIONS_JSON[%d]: %w", i, err)
		}
	}
	return ts, nil
}

// validateToleration checks that a single Toleration has a supported
// operator, effect, and the correct key/value/tolerationSeconds constraints.
func validateToleration(t corev1.Toleration) error {
	switch t.Operator {
	case "", corev1.TolerationOpEqual, corev1.TolerationOpExists:
	default:
		return fmt.Errorf("invalid operator %q (want Equal or Exists)", t.Operator)
	}
	switch t.Effect {
	case "", corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
	default:
		return fmt.Errorf("invalid effect %q", t.Effect)
	}
	if t.Operator == corev1.TolerationOpExists && t.Value != "" {
		return fmt.Errorf("operator=Exists requires empty value, got %q", t.Value)
	}
	if t.Operator == corev1.TolerationOpEqual && t.Key == "" {
		return fmt.Errorf("operator=Equal requires non-empty key")
	}
	if t.TolerationSeconds != nil && t.Effect != corev1.TaintEffectNoExecute {
		return fmt.Errorf("tolerationSeconds is only valid with effect=NoExecute")
	}
	return nil
}

func main() {
	var probeAddr string
	var gatewayImage string
	// Commit-Job image flag (formerly on tablecommit-operator, now on this
	// operator). The image is named iceberg-job.
	var icebergJobImage string
	var lakekeeperURL string
	var pipelineAPIURL string

	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&gatewayImage, "gateway-image", "datuplet/gateway:latest", "The image to use for gateway sidecars.")
	flag.StringVar(&icebergJobImage, "iceberg-job-image", "datuplet/iceberg-job:latest",
		"The image to use for iceberg-job (table-commit) jobs scheduled directly by this operator.")
	flag.StringVar(&lakekeeperURL, "lakekeeper-url", "",
		"Catalog REST base URL the spawned commit container "+
			"uses (e.g. http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog). "+
			"Also LAKEKEEPER_URL env var.")
	flag.StringVar(&pipelineAPIURL, "pipeline-api-url", "",
		"Base URL of pipeline-api (e.g. http://pipeline-api.datuplet.svc.cluster.local:8081). "+
			"Spawned DG sidecars + commit Jobs use this to fetch the JWKS for run-token validation. "+
			"Also PIPELINE_API_URL env var.")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	// Env fallback: only fills in when the flag wasn't explicitly set.
	// flag.Visit enumerates flags that were set on the command line
	// (regardless of the value), which is what we want to distinguish.
	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	if !explicit["lakekeeper-url"] {
		if env := os.Getenv("LAKEKEEPER_URL"); env != "" {
			lakekeeperURL = env
		}
	}
	if !explicit["pipeline-api-url"] {
		if env := os.Getenv("PIPELINE_API_URL"); env != "" {
			pipelineAPIURL = env
		}
	}
	if !explicit["gateway-image"] {
		if env := os.Getenv("GATEWAY_IMAGE"); env != "" {
			gatewayImage = env
		}
	}
	if !explicit["iceberg-job-image"] {
		if env := os.Getenv("ICEBERG_JOB_IMAGE"); env != "" {
			icebergJobImage = env
		}
	}
	// S3_ENDPOINT / S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY /
	// S3_USE_PATH_STYLE are no longer read. Commit Job Pods carry zero long-
	// lived S3 credentials; all storage access uses lakekeeper-vended STS
	// credentials via the run-token JWT.

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	runTolerations, err := loadRuntimeTolerations()
	if err != nil {
		setupLog.Error(err, "invalid DATUPLET_RUN_TOLERATIONS_JSON")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Set up Pipeline controller
	if err = (&controllers.PipelineReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Pipeline")
		os.Exit(1)
	}

	// Create Kubernetes clientset for pod log access
	clientset, err := kubernetes.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		setupLog.Error(err, "unable to create kubernetes clientset")
		os.Exit(1)
	}

	// Set up PipelineRun controller — the sole reconciler for both
	// component Jobs and commit Jobs.
	if err = (&controllers.PipelineRunReconciler{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		GatewayImage:       gatewayImage,
		TableCommitImage:   icebergJobImage,
		LakekeeperURL:      lakekeeperURL,
		PipelineAPIURL:     pipelineAPIURL,
		Clientset:          clientset,
		RuntimeTolerations: runTolerations,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PipelineRun")
		os.Exit(1)
	}

	// Add health check endpoints
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting Pipeline operator")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
