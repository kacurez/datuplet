package controllers

import (
	"context"
	stderrors "errors"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PipelineReconciler reconciles a Pipeline object
type PipelineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=datuplet.io,resources=pipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=datuplet.io,resources=pipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=datuplet.io,resources=pipelines/finalizers,verbs=update

// Reconcile handles Pipeline reconciliation (validation).
func (r *PipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Pipeline instance
	pipeline := &datupletv1.Pipeline{}
	if err := r.Get(ctx, req.NamespacedName, pipeline); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Pipeline")
		return ctrl.Result{}, err
	}

	// Skip if already processed this generation
	if pipeline.Status.ObservedGeneration == pipeline.Generation &&
		(pipeline.Status.Phase == datupletv1.PipelinePhaseReady ||
			pipeline.Status.Phase == datupletv1.PipelinePhaseInvalid) {
		return ctrl.Result{}, nil
	}

	// Validate the pipeline
	validationErr := r.validatePipeline(ctx, pipeline)

	// Update status
	pipeline.Status.ObservedGeneration = pipeline.Generation
	if validationErr != nil {
		pipeline.Status.Phase = datupletv1.PipelinePhaseInvalid
		pipeline.Status.Message = validationErr.Error()
		pipeline.Status.Conditions = r.setCondition(pipeline.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "ValidationFailed",
			Message:            validationErr.Error(),
			ObservedGeneration: pipeline.Generation,
		})
		logger.Info("Pipeline validation failed", "error", validationErr.Error())
	} else {
		pipeline.Status.Phase = datupletv1.PipelinePhaseReady
		pipeline.Status.Message = "Pipeline is valid and ready to run"
		pipeline.Status.Conditions = r.setCondition(pipeline.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "ValidationPassed",
			Message:            "Pipeline is valid and ready to run",
			ObservedGeneration: pipeline.Generation,
		})
		logger.Info("Pipeline validation passed")
	}

	if err := r.Status().Update(ctx, pipeline); err != nil {
		logger.Error(err, "Failed to update Pipeline status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// validatePipeline validates the Pipeline spec using validate.ValidateTyped —
// the same semantic checks the pipeline-api save path runs — so the kubectl
// path never diverges into a second validation dialect. The first Finding
// (if any) becomes the returned error.
func (r *PipelineReconciler) validatePipeline(_ context.Context, pipeline *datupletv1.Pipeline) error {
	findings := validate.ValidateTyped(pipeline, nil, nil)
	if len(findings) == 0 {
		return nil
	}
	return stderrors.New(findings[0].Message)
}

// setCondition updates or appends a condition.
func (r *PipelineReconciler) setCondition(conditions []metav1.Condition, newCondition metav1.Condition) []metav1.Condition {
	now := metav1.Now()
	newCondition.LastTransitionTime = now

	for i, c := range conditions {
		if c.Type == newCondition.Type {
			if c.Status != newCondition.Status {
				conditions[i] = newCondition
			} else {
				// Keep the old transition time if status hasn't changed
				newCondition.LastTransitionTime = c.LastTransitionTime
				conditions[i] = newCondition
			}
			return conditions
		}
	}

	return append(conditions, newCondition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&datupletv1.Pipeline{}).
		Complete(r)
}
