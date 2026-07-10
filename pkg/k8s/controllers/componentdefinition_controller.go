package controllers

// RFC 026 Task R8: registration-time validation of ComponentDefinition CRs.
// This reconciler never touches Pipeline/PipelineRun state — it only decides
// whether a ComponentDefinition itself is well-formed, so that resolution
// paths (StaticRegistry / ComponentRegistry, both in this package's sibling
// files) can trust an Invalid-phase definition was already caught here.

import (
	"context"
	"fmt"
	"strings"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	componentDefinitionPhaseValid   = "Valid"
	componentDefinitionPhaseInvalid = "Invalid"
)

// ComponentDefinitionReconciler reconciles a ComponentDefinition object.
type ComponentDefinitionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=datuplet.io,resources=componentdefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=datuplet.io,resources=componentdefinitions/status,verbs=get;update;patch

// Reconcile handles ComponentDefinition reconciliation (validation).
func (r *ComponentDefinitionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	def := &datupletv1.ComponentDefinition{}
	if err := r.Get(ctx, req.NamespacedName, def); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get ComponentDefinition")
		return ctrl.Result{}, err
	}

	// Skip if already processed this generation.
	if def.Status.ObservedGeneration == def.Generation &&
		(def.Status.Phase == componentDefinitionPhaseValid ||
			def.Status.Phase == componentDefinitionPhaseInvalid) {
		return ctrl.Result{}, nil
	}

	problems := validateComponentDefinitionSpec(&def.Spec)

	def.Status.ObservedGeneration = def.Generation
	if len(problems) > 0 {
		def.Status.Phase = componentDefinitionPhaseInvalid
		def.Status.Message = strings.Join(problems, "; ")
		logger.Info("ComponentDefinition validation failed", "error", def.Status.Message)
	} else {
		def.Status.Phase = componentDefinitionPhaseValid
		def.Status.Message = ""
		logger.Info("ComponentDefinition validation passed")
	}

	if err := r.Status().Update(ctx, def); err != nil {
		logger.Error(err, "Failed to update ComponentDefinition status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// validateComponentDefinitionSpec runs every registration-time structural
// check and returns one human-readable problem description per defect found
// (empty when the spec is valid). Order mirrors the brief: schema
// compilation, stable-version format, duplicate versions, defaultVersion
// resolution, image tagging, then resource bounds.
func validateComponentDefinitionSpec(spec *datupletv1.ComponentDefinitionSpec) []string {
	var problems []string

	seen := make(map[string]bool, len(spec.Versions))
	for _, v := range spec.Versions {
		if v.ConfigSchema != "" {
			if _, err := validate.CompileSchema(v.ConfigSchema); err != nil {
				problems = append(problems, fmt.Sprintf("version %q: configSchema does not compile: %v", v.Version, err))
			}
		}

		if !v.Prerelease && !datupletv1.IsStableVersion(v.Version) {
			problems = append(problems, fmt.Sprintf("version %q: stable versions must use vMAJOR.MINOR.PATCH", v.Version))
		}

		if seen[v.Version] {
			problems = append(problems, fmt.Sprintf("duplicate version %q", v.Version))
		}
		seen[v.Version] = true

		if !v.Prerelease && !hasNonLatestTag(v.Image) {
			problems = append(problems, fmt.Sprintf("version %q: image %q must pin a non-latest tag", v.Version, v.Image))
		}

		if v.Resources != nil {
			problems = append(problems, resourceBoundProblems(v.Version, v.Resources)...)
		}
	}

	if spec.DefaultVersion != "" {
		if _, ok := spec.FindVersion(spec.DefaultVersion); !ok {
			problems = append(problems, fmt.Sprintf("defaultVersion %q is not present in versions", spec.DefaultVersion))
		}
	}

	return problems
}

// hasNonLatestTag reports whether image pins a tag other than "latest". A
// tag is only introduced by a ':' in the final path segment (after the last
// '/'); an earlier ':' belongs to a registry host:port
// (e.g. "registry.example.com:5000/component" has no tag at all, while
// "registry.example.com:5000/component:v1.2.3" does). This intentionally
// diverges from the CRD's belt-and-suspenders CEL rule
// ("self.image.contains(':') && !self.image.endsWith(':latest')"), which
// mishandles registry ports the same way this function used to.
func hasNonLatestTag(image string) bool {
	lastSlash := strings.LastIndex(image, "/")
	finalSegment := image[lastSlash+1:]
	return strings.Contains(finalSegment, ":") && !strings.HasSuffix(image, ":latest")
}

// resourceBoundProblems reports every resource name for which the version's
// default limit exceeds its max ceiling.
func resourceBoundProblems(version string, res *datupletv1.ComponentResources) []string {
	var problems []string
	for name, def := range res.Default.Limits {
		maxQty, ok := res.Max[name]
		if !ok {
			continue
		}
		if def.Cmp(maxQty) > 0 {
			problems = append(problems, fmt.Sprintf("version %q: resources.default limit for %q (%s) exceeds resources.max (%s)",
				version, name, def.String(), maxQty.String()))
		}
	}
	return problems
}

// SetupWithManager sets up the controller with the Manager.
func (r *ComponentDefinitionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&datupletv1.ComponentDefinition{}).
		Complete(r)
}
