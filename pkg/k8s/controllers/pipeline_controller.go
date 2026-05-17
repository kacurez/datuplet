package controllers

import (
	"context"
	"fmt"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

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

// validatePipeline validates the Pipeline spec.
func (r *PipelineReconciler) validatePipeline(_ context.Context, pipeline *datupletv1.Pipeline) error {
	spec := &pipeline.Spec

	// Validate stages
	if len(spec.Stages) == 0 {
		return fmt.Errorf("pipeline must have at least one stage")
	}

	seenComponentNames := make(map[string]bool)
	for i, stage := range spec.Stages {
		if stage.Name == "" {
			return fmt.Errorf("stage %d: name is required", i)
		}

		if len(stage.Components) == 0 {
			return fmt.Errorf("stage %s: must have at least one component", stage.Name)
		}

		for j, comp := range stage.Components {
			if comp.Name == "" {
				return fmt.Errorf("stage %s, component %d: name is required", stage.Name, j)
			}

			if seenComponentNames[comp.Name] {
				return fmt.Errorf("duplicate component name: %s", comp.Name)
			}
			seenComponentNames[comp.Name] = true

			if comp.Image == "" {
				return fmt.Errorf("stage %s, component %s: image is required", stage.Name, comp.Name)
			}

			// Validate inputs
			if comp.Inputs != nil {
				// Validate bucket names
				for _, bucket := range comp.Inputs.Buckets {
					if err := validateTableName(bucket); err != nil {
						return fmt.Errorf("stage %s, component %s: invalid input bucket name %q: %w",
							stage.Name, comp.Name, bucket, err)
					}
				}

				// Validate table specifications
				for _, tableSpec := range comp.Inputs.Tables {
					if tableSpec.Bucket == "" {
						return fmt.Errorf("stage %s, component %s: input table bucket is required",
							stage.Name, comp.Name)
					}
					if tableSpec.Table == "" {
						return fmt.Errorf("stage %s, component %s: input table name is required",
							stage.Name, comp.Name)
					}
					if err := validateTableName(tableSpec.Bucket); err != nil {
						return fmt.Errorf("stage %s, component %s: invalid input bucket name %q: %w",
							stage.Name, comp.Name, tableSpec.Bucket, err)
					}
					if err := validateTableName(tableSpec.Table); err != nil {
						return fmt.Errorf("stage %s, component %s: invalid input table name %q: %w",
							stage.Name, comp.Name, tableSpec.Table, err)
					}
				}
			}

			// Validate outputs
			if comp.Outputs != nil {
				// Check for exclusive DefaultBucket mode
				hasDefaultBucket := comp.Outputs.DefaultBucket != ""
				hasBuckets := len(comp.Outputs.Buckets) > 0
				hasTables := len(comp.Outputs.Tables) > 0

				if hasDefaultBucket && (hasBuckets || hasTables) {
					return fmt.Errorf("stage %s, component %s: defaultBucket cannot be combined with explicit buckets or tables",
						stage.Name, comp.Name)
				}

				// Validate defaultBucket
				if hasDefaultBucket {
					if err := validateTableName(comp.Outputs.DefaultBucket); err != nil {
						return fmt.Errorf("stage %s, component %s: invalid defaultBucket name %q: %w",
							stage.Name, comp.Name, comp.Outputs.DefaultBucket, err)
					}
				}

				// Validate explicit buckets
				for _, bucketSpec := range comp.Outputs.Buckets {
					if bucketSpec.Name == "" {
						return fmt.Errorf("stage %s, component %s: output bucket name is required",
							stage.Name, comp.Name)
					}
					if err := validateTableName(bucketSpec.Name); err != nil {
						return fmt.Errorf("stage %s, component %s: invalid output bucket name %q: %w",
							stage.Name, comp.Name, bucketSpec.Name, err)
					}
				}

				// Validate explicit tables
				for _, tableSpec := range comp.Outputs.Tables {
					if tableSpec.Name == "" {
						return fmt.Errorf("stage %s, component %s: output table name is required",
							stage.Name, comp.Name)
					}
					if tableSpec.Bucket == "" {
						return fmt.Errorf("stage %s, component %s: output table bucket is required",
							stage.Name, comp.Name)
					}
					if err := validateTableName(tableSpec.Name); err != nil {
						return fmt.Errorf("stage %s, component %s: invalid output table name %q: %w",
							stage.Name, comp.Name, tableSpec.Name, err)
					}
					if err := validateTableName(tableSpec.Bucket); err != nil {
						return fmt.Errorf("stage %s, component %s: invalid output bucket name %q: %w",
							stage.Name, comp.Name, tableSpec.Bucket, err)
					}
				}
			}
		}
	}

	return nil
}

// validateTableName validates that a table name is a logical identifier.
// Table names must not contain slashes, colons, or whitespace.
func validateTableName(table string) error {
	if table == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	for _, ch := range table {
		if ch == '/' || ch == ':' || ch == ' ' || ch == '\t' || ch == '\n' {
			return fmt.Errorf("table name must not contain '/', ':', or whitespace")
		}
	}
	return nil
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
