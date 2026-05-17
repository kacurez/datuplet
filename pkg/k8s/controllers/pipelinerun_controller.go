package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/lib/status"
	"github.com/google/uuid"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// DefaultGatewayImage is the default image for the data gateway sidecar.
	PipelineRunDefaultGatewayImage = "datuplet/gateway:latest"

	// GatewayPort is the gRPC port the gateway listens on.
	PipelineRunGatewayPort = 50051

	// JobTTLSecondsAfterFinished is the TTL for completed component jobs.
	JobTTLSecondsAfterFinished = int32(3600)

	// PipelineRunRequeueInterval is the requeue interval when execution is in progress.
	PipelineRunRequeueInterval = 5 * time.Second

)

// shortID returns the first 8 chars of id, or the whole string if shorter.
// Used to build K8s resource names; avoids panicking on unexpectedly short IDs.
func shortID(id string) string {
	if len(id) < 8 {
		return id
	}
	return id[:8]
}

// PipelineRunReconciler reconciles a PipelineRun object
type PipelineRunReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	GatewayImage string

	// The fields below drive commit Job construction.
	// The TableCommit CRD and tablecommit-operator have been removed;
	// the PipelineRun controller builds commit Jobs directly.

	// TableCommitImage is the image used for the spawned commit Jobs.
	TableCommitImage string
	// LakekeeperURL is the catalog REST base URL the spawned commit
	// container points at (e.g.
	// http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog).
	// Required in production K8s; empty in tests.
	LakekeeperURL string
	// PipelineAPIURL is the base URL of pipeline-api. The operator derives the
	// JWKS URL (PipelineAPIURL + "/api/v1/auth/jwks.json") and injects it into
	// every DG sidecar configMap + commit Job env. Empty disables the injection
	// (dev paths).
	PipelineAPIURL string
	// S3 long-lived credentials are not used. Commit Jobs use the run-token
	// JWT and lakekeeper-vended STS credentials exclusively.

	Clientset kubernetes.Interface
}

// +kubebuilder:rbac:groups=datuplet.io,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=datuplet.io,resources=pipelineruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=datuplet.io,resources=pipelineruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=datuplet.io,resources=pipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch

// Reconcile handles PipelineRun reconciliation.
func (r *PipelineRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the PipelineRun instance
	pipelineRun := &datupletv1.PipelineRun{}
	if err := r.Get(ctx, req.NamespacedName, pipelineRun); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PipelineRun")
		return ctrl.Result{}, err
	}

	// Update observed generation
	if pipelineRun.Status.ObservedGeneration != pipelineRun.Generation {
		pipelineRun.Status.ObservedGeneration = pipelineRun.Generation
	}

	// Cancel propagation: when the PipelineRun itself carries
	// `datuplet.io/cancel=true` (admin-driven cancel via `kubectl annotate`),
	// propagate the annotation to every owned component pod so DG's cancel
	// watcher observes it. The pipeline-api cancel path patches pods directly
	// before deleting the CRD, but admin/kubectl-driven cancels never touch
	// pipeline-api — without this, those cancels would only stop new pods
	// being created, not drain in-flight ones.
	if pipelineRun.Annotations["datuplet.io/cancel"] == "true" {
		r.propagateCancelToPods(ctx, pipelineRun)
	}

	// Handle based on current phase
	switch pipelineRun.Status.Phase {
	case "":
		// New PipelineRun - initialize
		return r.handlePending(ctx, pipelineRun)
	case datupletv1.PipelineRunPhasePending:
		// Start execution
		return r.handlePending(ctx, pipelineRun)
	case datupletv1.PipelineRunPhaseRunning:
		// Continue execution
		return r.handleRunning(ctx, pipelineRun)
	case datupletv1.PipelineRunPhaseSucceeded, datupletv1.PipelineRunPhaseFailedUser, datupletv1.PipelineRunPhaseFailedApplication:
		// Terminal state - nothing to do
		return ctrl.Result{}, nil
	default:
		logger.Info("Unknown phase", "phase", pipelineRun.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// handlePending initializes the PipelineRun and starts execution.
func (r *PipelineRunReconciler) handlePending(ctx context.Context, pr *datupletv1.PipelineRun) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Pipeline
	pipeline := &datupletv1.Pipeline{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      pr.Spec.PipelineRef.Name,
		Namespace: pr.Namespace,
	}, pipeline); err != nil {
		if errors.IsNotFound(err) {
			pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
			pr.Status.Message = fmt.Sprintf("Pipeline '%s' not found", pr.Spec.PipelineRef.Name)
			if err := r.Status().Update(ctx, pr); err != nil {
				logger.Error(err, "Failed to update PipelineRun status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Pipeline")
		return ctrl.Result{}, err
	}

	// Check if Pipeline is ready
	if pipeline.Status.Phase == datupletv1.PipelinePhaseInvalid {
		// Pipeline is invalid - fail the run
		pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
		pr.Status.Message = fmt.Sprintf("Pipeline '%s' is invalid: %s", pipeline.Name, pipeline.Status.Message)
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	if pipeline.Status.Phase != datupletv1.PipelinePhaseReady {
		// Pipeline not ready yet - requeue and wait
		logger.Info("Pipeline not ready yet, waiting", "pipeline", pipeline.Name, "phase", pipeline.Status.Phase)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// Determine run ID: use spec value if provided, otherwise generate
	runID := pr.Spec.RunID
	if runID == "" {
		runID = uuid.New().String()
	} else {
		// Validate user-provided RunID is a valid UUID
		if _, err := uuid.Parse(runID); err != nil {
			pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
			pr.Status.Message = fmt.Sprintf("invalid spec.runId: must be a valid UUID, got %q", runID)
			if err := r.Status().Update(ctx, pr); err != nil {
				logger.Error(err, "Failed to update PipelineRun status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// Initialize status
	pr.Status.Phase = datupletv1.PipelineRunPhaseRunning
	pr.Status.RunID = runID
	now := metav1.Now()
	pr.Status.StartTime = &now

	// Initialize stage statuses
	pr.Status.StageStatuses = make([]datupletv1.StageStatus, len(pipeline.Spec.Stages))
	for i, stage := range pipeline.Spec.Stages {
		pr.Status.StageStatuses[i] = datupletv1.StageStatus{
			Name:  stage.Name,
			Phase: datupletv1.StagePhasePending,
		}
	}

	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
		return ctrl.Result{}, err
	}

	logger.Info("Initialized PipelineRun", "runID", runID)
	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// handleRunning continues the PipelineRun execution.
func (r *PipelineRunReconciler) handleRunning(ctx context.Context, pr *datupletv1.PipelineRun) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Pipeline
	pipeline := &datupletv1.Pipeline{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      pr.Spec.PipelineRef.Name,
		Namespace: pr.Namespace,
	}, pipeline); err != nil {
		logger.Error(err, "Failed to get Pipeline")
		return ctrl.Result{}, err
	}

	// Find the current stage to execute
	currentStageIdx := -1
	for i, stageStatus := range pr.Status.StageStatuses {
		if stageStatus.Phase != datupletv1.StagePhaseSucceeded {
			currentStageIdx = i
			break
		}
	}

	// All stages completed
	if currentStageIdx == -1 {
		pr.Status.Phase = datupletv1.PipelineRunPhaseSucceeded
		pr.Status.Message = "All stages completed successfully"
		now := metav1.Now()
		pr.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		logger.Info("PipelineRun completed successfully")
		return ctrl.Result{}, nil
	}

	stage := &pipeline.Spec.Stages[currentStageIdx]
	stageStatus := &pr.Status.StageStatuses[currentStageIdx]
	pr.Status.CurrentStage = stage.Name

	// Handle stage based on its phase
	switch stageStatus.Phase {
	case datupletv1.StagePhasePending:
		// Start the stage
		return r.startStage(ctx, pr, pipeline, currentStageIdx)
	case datupletv1.StagePhaseRunning:
		// Check component status
		return r.checkStageComponents(ctx, pr, pipeline, currentStageIdx)
	case datupletv1.StagePhaseCommitting:
		// Check TableCommit status
		return r.checkStageCommits(ctx, pr, pipeline, currentStageIdx)
	case datupletv1.StagePhaseFailedUser:
		// Stage failed due to user error - fail the entire run
		pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
		pr.Status.Message = fmt.Sprintf("Stage '%s' failed: %s", stage.Name, stageStatus.Message)
		now := metav1.Now()
		pr.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	case datupletv1.StagePhaseFailedApplication:
		// Stage failed due to application error - fail the entire run
		pr.Status.Phase = datupletv1.PipelineRunPhaseFailedApplication
		pr.Status.Message = fmt.Sprintf("Stage '%s' failed: %s", stage.Name, stageStatus.Message)
		now := metav1.Now()
		pr.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// startStage creates Jobs for all components in the stage.
func (r *PipelineRunReconciler) startStage(ctx context.Context, pr *datupletv1.PipelineRun, pipeline *datupletv1.Pipeline, stageIdx int) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	stage := &pipeline.Spec.Stages[stageIdx]
	stageStatus := &pr.Status.StageStatuses[stageIdx]

	logger.Info("Starting stage", "stage", stage.Name)

	// Initialize component statuses
	stageStatus.ComponentStatuses = make([]datupletv1.ComponentStatus, len(stage.Components))
	for i, comp := range stage.Components {
		stageStatus.ComponentStatuses[i] = datupletv1.ComponentStatus{
			Name:  comp.Name,
			Phase: datupletv1.ComponentPhasePending,
		}
	}

	// Create Jobs for each component
	for i := range stage.Components {
		comp := &stage.Components[i]
		compStatus := &stageStatus.ComponentStatuses[i]

		job, configMap, err := r.buildComponentJob(ctx, pr, pipeline, comp)
		if err != nil {
			logger.Error(err, "Failed to build component job", "component", comp.Name)
			compStatus.Phase = datupletv1.ComponentPhaseFailedApplication
			compStatus.Message = err.Error()
			stageStatus.Phase = datupletv1.StagePhaseFailedApplication
			stageStatus.Message = fmt.Sprintf("Failed to build job for component '%s': %v", comp.Name, err)
			if err := r.Status().Update(ctx, pr); err != nil {
				logger.Error(err, "Failed to update PipelineRun status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}

		// Set owner reference
		if err := ctrl.SetControllerReference(pr, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set controller reference for job")
			return ctrl.Result{}, err
		}
		if err := ctrl.SetControllerReference(pr, configMap, r.Scheme); err != nil {
			logger.Error(err, "Failed to set controller reference for configmap")
			return ctrl.Result{}, err
		}

		// Create ConfigMap
		if err := r.Create(ctx, configMap); err != nil && !errors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create gateway config map", "component", comp.Name)
			return ctrl.Result{}, err
		}

		// Create Job
		if err := r.Create(ctx, job); err != nil && !errors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create job", "component", comp.Name)
			return ctrl.Result{}, err
		}

		compStatus.Phase = datupletv1.ComponentPhaseRunning
		compStatus.PodName = job.Name
		now := metav1.Now()
		compStatus.StartTime = &now

		logger.Info("Created Job for component", "component", comp.Name, "job", job.Name)
	}

	stageStatus.Phase = datupletv1.StagePhaseRunning
	now := metav1.Now()
	stageStatus.StartTime = &now

	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// checkStageComponents checks the status of all component Jobs in a stage.
func (r *PipelineRunReconciler) checkStageComponents(ctx context.Context, pr *datupletv1.PipelineRun, pipeline *datupletv1.Pipeline, stageIdx int) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	stage := &pipeline.Spec.Stages[stageIdx]
	stageStatus := &pr.Status.StageStatuses[stageIdx]

	allSucceeded := true
	anyFailedUser := false
	anyFailedApp := false
	var failedComponent string
	var failedMessage string

	for i := range stage.Components {
		compStatus := &stageStatus.ComponentStatuses[i]

		if compStatus.Phase == datupletv1.ComponentPhaseSucceeded {
			continue
		}
		if compStatus.Phase == datupletv1.ComponentPhaseFailedUser {
			anyFailedUser = true
			failedComponent = compStatus.Name
			failedMessage = compStatus.Message
			continue
		}
		if compStatus.Phase == datupletv1.ComponentPhaseFailedApplication {
			anyFailedApp = true
			failedComponent = compStatus.Name
			failedMessage = compStatus.Message
			continue
		}

		allSucceeded = false

		// Check Job status. Use componentJobName so this stays in sync
		// with buildComponentJob — changing the scheme in one place
		// without the other makes the poller look for the wrong object
		// and leaves components "running" forever.
		jobName := componentJobName(pr, stage.Name, compStatus.Name)

		job := &batchv1.Job{}
		if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pr.Namespace}, job); err != nil {
			if errors.IsNotFound(err) {
				logger.Info("Job not found, waiting...", "job", jobName)
				continue
			}
			logger.Error(err, "Failed to get Job", "job", jobName)
			return ctrl.Result{}, err
		}

		// Observe Secret mount/resolve state and surface it as a condition.
		r.updateSecretsResolvedCondition(ctx, pr, pipeline, job)

		// Try to extract exit code from pod
		exitCode := r.extractExitCodeFromJob(ctx, job)

		// Check Job conditions
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
				compStatus.Phase = datupletv1.ComponentPhaseSucceeded
				// Job completed successfully - exit code is 0 by definition
				zero := int32(0)
				compStatus.ExitCode = &zero
				now := metav1.Now()
				compStatus.CompletionTime = &now

				// Extract status message from pod logs
				if msg := r.extractComponentStatusMessage(ctx, job, 0); msg != "" {
					compStatus.Message = msg
				}

				logger.Info("Component succeeded", "component", compStatus.Name, "message", compStatus.Message)
			}
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				compStatus.ExitCode = exitCode

				// Classify failure type based on exit code
				if exitCode != nil {
					failureType := status.ClassifyExitCode(int(*exitCode))
					if failureType == status.FailureTypeUser {
						compStatus.Phase = datupletv1.ComponentPhaseFailedUser
						anyFailedUser = true
					} else {
						compStatus.Phase = datupletv1.ComponentPhaseFailedApplication
						anyFailedApp = true
					}

					// Try to extract status message from pod logs
					msg := r.extractComponentStatusMessage(ctx, job, int(*exitCode))
					if msg != "" {
						compStatus.Message = msg
					} else {
						compStatus.Message = condition.Message
					}
				} else {
					// No exit code available - treat as application error
					compStatus.Phase = datupletv1.ComponentPhaseFailedApplication
					compStatus.Message = condition.Message
					anyFailedApp = true
				}

				failedComponent = compStatus.Name
				failedMessage = compStatus.Message
				now := metav1.Now()
				compStatus.CompletionTime = &now
				logger.Info("Component failed", "component", compStatus.Name,
					"phase", compStatus.Phase, "exitCode", exitCode, "message", compStatus.Message)
			}
		}
	}

	// Check final state - first failed component determines stage failure type
	if anyFailedUser || anyFailedApp {
		if anyFailedUser {
			stageStatus.Phase = datupletv1.StagePhaseFailedUser
		} else {
			stageStatus.Phase = datupletv1.StagePhaseFailedApplication
		}
		stageStatus.Message = fmt.Sprintf("Component '%s' failed: %s", failedComponent, failedMessage)
		now := metav1.Now()
		stageStatus.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if allSucceeded {
		// All components done - start committing
		return r.startStageCommits(ctx, pr, pipeline, stageIdx)
	}

	// Still running - keep requeue cadence; log Update failures (next tick will retry).
	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
	}
	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// startStageCommits schedules a `batch/v1.Job` per (stage, bucket).
// The TableCommit CRD and tablecommit-operator have been removed; the
// PipelineRun controller creates commit Jobs directly. The builder lives
// in pipelinerun_commit_jobs.go (`buildCommitJob`); owner reference points
// at the PipelineRun so kubectl-driven deletion + GC stay consistent.
// K8s native Job machinery (BackoffLimit, TTLSecondsAfterFinished) handles
// retry and cleanup.
func (r *PipelineRunReconciler) startStageCommits(ctx context.Context, pr *datupletv1.PipelineRun, pipeline *datupletv1.Pipeline, stageIdx int) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	stage := &pipeline.Spec.Stages[stageIdx]
	stageStatus := &pr.Status.StageStatuses[stageIdx]

	logger.Info("Starting commits for stage", "stage", stage.Name)

	// Collect unique buckets to commit (commits happen at bucket level)
	// For each bucket, track the write mode (if any output uses FULL_LOAD, use FULL_LOAD)
	type bucketInfo struct {
		writeMode datupletv1.WriteMode
	}
	bucketsToCommit := make(map[string]bucketInfo)
	for _, comp := range stage.Components {
		if comp.Outputs == nil {
			continue
		}

		// Include defaultBucket mode
		if comp.Outputs.DefaultBucket != "" {
			info := bucketsToCommit[comp.Outputs.DefaultBucket]
			writeMode := strings.ToUpper(comp.Outputs.DefaultWriteMode)
			if writeMode == "FULL_LOAD" {
				info.writeMode = datupletv1.WriteModeFullLoad
			} else if info.writeMode == "" {
				info.writeMode = datupletv1.WriteModeAppend
			}
			bucketsToCommit[comp.Outputs.DefaultBucket] = info
		}

		// Include explicit bucket outputs
		for _, bucketSpec := range comp.Outputs.Buckets {
			info := bucketsToCommit[bucketSpec.Name]
			writeMode := strings.ToUpper(bucketSpec.WriteMode)
			if writeMode == "FULL_LOAD" {
				info.writeMode = datupletv1.WriteModeFullLoad
			} else if info.writeMode == "" {
				info.writeMode = datupletv1.WriteModeAppend
			}
			bucketsToCommit[bucketSpec.Name] = info
		}

		// Include explicit table outputs (aggregate by bucket)
		for _, tableSpec := range comp.Outputs.Tables {
			info := bucketsToCommit[tableSpec.Bucket]
			writeMode := strings.ToUpper(tableSpec.WriteMode)
			// Default to APPEND, but if any output uses FULL_LOAD, use FULL_LOAD
			if writeMode == "FULL_LOAD" {
				info.writeMode = datupletv1.WriteModeFullLoad
			} else if info.writeMode == "" {
				info.writeMode = datupletv1.WriteModeAppend
			}
			bucketsToCommit[tableSpec.Bucket] = info
		}
	}

	// Create commit Job for each bucket. The PipelineRun.Status.TableCommits
	// surface stays — it's already exposed to UI/pipeline-api consumers.
	// We just stop pointing at TableCommit CRDs and start pointing at the
	// commit Job names instead.
	pr.Status.TableCommits = nil
	for bucket, info := range bucketsToCommit {
		job := r.buildCommitJob(pr, bucket, info.writeMode)

		// Owner-ref to the PipelineRun: kubectl-driven delete or run
		// reaper sweeps the commit Job along with its sibling component
		// Jobs / ConfigMaps / run-token Secret.
		if err := ctrl.SetControllerReference(pr, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set controller reference for commit Job")
			return ctrl.Result{}, err
		}

		if err := r.Create(ctx, job); err != nil && !errors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create commit Job", "bucket", bucket)
			return ctrl.Result{}, err
		}

		pr.Status.TableCommits = append(pr.Status.TableCommits, datupletv1.TableCommitRef{
			Name:   job.Name,
			Bucket: bucket,
			Phase:  datupletv1.TableCommitPhasePending,
		})

		logger.Info("Created commit Job", "job", job.Name, "bucket", bucket)
	}

	// No buckets to commit (stage has no output declarations).
	// Mark stage Succeeded directly.
	if len(bucketsToCommit) == 0 {
		stageStatus.Phase = datupletv1.StagePhaseSucceeded
		now := metav1.Now()
		stageStatus.CompletionTime = &now
		logger.Info("Stage succeeded (no output buckets to commit)", "stage", stage.Name)
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
	}

	stageStatus.Phase = datupletv1.StagePhaseCommitting

	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// checkStageCommits inspects the commit Jobs scheduled for a stage and
// folds their results back into pr.Status. Job failures are always
// classified as FailedApplication — same contract the deleted
// TableCommit CRD documented.
func (r *PipelineRunReconciler) checkStageCommits(ctx context.Context, pr *datupletv1.PipelineRun, _ *datupletv1.Pipeline, stageIdx int) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	stageStatus := &pr.Status.StageStatuses[stageIdx]

	allSucceeded := true
	anyFailed := false
	var failedBucket string
	var failedMessage string

	for i, ref := range pr.Status.TableCommits {
		job, ok, err := r.commitJobLookup(ctx, pr, ref.Bucket)
		if err != nil {
			logger.Error(err, "Failed to get commit Job", "bucket", ref.Bucket)
			return ctrl.Result{}, err
		}
		if !ok {
			logger.Info("Commit Job not found, waiting...", "bucket", ref.Bucket)
			allSucceeded = false
			continue
		}

		phase := commitJobPhase(job)
		pr.Status.TableCommits[i].Phase = phase

		switch phase {
		case datupletv1.TableCommitPhaseSucceeded:
			// Good. The DUPLET_STATUS_MESSAGE log-tail is opportunistic
			// for success — no need to surface per-bucket success blurbs
			// up to the run unless we eventually want to.
		case datupletv1.TableCommitPhaseFailedApplication:
			anyFailed = true
			failedBucket = ref.Bucket
			// Pull the structured DUPLET_STATUS_MESSAGE from the commit
			// Pod's logs so the operator's `kubectl describe` shows the
			// real failure rather than the opaque Job condition.
			exitCode := r.extractCommitJobExitCode(ctx, job)
			code := -1
			if exitCode != nil {
				code = int(*exitCode)
			}
			if msg := r.extractCommitJobStatusMessage(ctx, job, code); msg != "" {
				failedMessage = msg
			}
		default:
			allSucceeded = false
		}
	}

	// Check final state - commit Job failures are always application errors
	if anyFailed {
		stageStatus.Phase = datupletv1.StagePhaseFailedApplication
		if failedMessage != "" {
			stageStatus.Message = fmt.Sprintf("Commit Job for bucket '%s' failed: %s", failedBucket, failedMessage)
		} else {
			stageStatus.Message = fmt.Sprintf("Commit Job for bucket '%s' failed", failedBucket)
		}
		now := metav1.Now()
		stageStatus.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if allSucceeded {
		// All commits done - stage succeeded
		stageStatus.Phase = datupletv1.StagePhaseSucceeded
		now := metav1.Now()
		stageStatus.CompletionTime = &now
		logger.Info("Stage completed successfully", "stage", stageStatus.Name)
	}

	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// SetupWithManager sets up the controller with the Manager.
// Both component Jobs and commit Jobs are owned directly by the
// PipelineRun, so a single `Owns(&batchv1.Job{})` watch covers both.
func (r *PipelineRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&datupletv1.PipelineRun{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

// propagateCancelToPods patches every pod labelled
// `datuplet.io/run-id=<runID>` in the PipelineRun's namespace with
// annotation `datuplet.io/cancel=true`. DG's cancel watcher polls the
// kubelet downward-API projection of pod annotations and triggers a
// graceful shutdown when it sees the annotation flip.
//
// Best-effort: a patch failure on any individual pod is logged and we
// keep going. The reconcile loop itself never errors on a cancel
// propagation failure — the cancel pathway is informational, not
// load-bearing for correctness (a future delete of the PipelineRun
// would force-kill the pod regardless).
//
// Mirrors pkg/pipelineapi/runbackend.K8sBackend.annotateRunPodsCancelled
// — kept here so admin/kubectl-driven cancels (which never touch
// pipeline-api) still drain in-flight pods cleanly.
func (r *PipelineRunReconciler) propagateCancelToPods(ctx context.Context, pr *datupletv1.PipelineRun) {
	logger := log.FromContext(ctx)
	if pr.Status.RunID == "" {
		// Nothing to look up by yet — pods are labelled with the run-id,
		// which the reconciler stamps onto Status during handlePending.
		// A cancel arriving before handlePending fires will be picked
		// up on the next reconcile pass once the run-id exists.
		return
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(pr.Namespace),
		client.MatchingLabels{"datuplet.io/run-id": pr.Status.RunID},
	); err != nil {
		logger.V(1).Info("cancel: list pods failed (best-effort)", "err", err.Error())
		return
	}
	if len(pods.Items) == 0 {
		return
	}
	patchBytes := []byte(`{"metadata":{"annotations":{"datuplet.io/cancel":"true"}}}`)
	for i := range pods.Items {
		pod := &pods.Items[i]
		// Skip pods that already carry the annotation — avoids burning
		// API server budget on every requeue.
		if pod.Annotations["datuplet.io/cancel"] == "true" {
			continue
		}
		if err := r.Patch(ctx, pod, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
			logger.V(1).Info("cancel: patch pod failed (best-effort)",
				"pod", pod.Name, "err", err.Error())
			continue
		}
	}
}
