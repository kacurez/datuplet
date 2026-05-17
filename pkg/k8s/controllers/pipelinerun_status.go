package controllers

import (
	"context"
	"fmt"
	"io"
	"strings"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/lib/status"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// updateSecretsResolvedCondition inspects the given job's latest pod and
// events and, when a signal is present, sets the SecretsResolved condition
// on pr.Status. No-op when the pipeline has no secretsRef or when the pod
// has not produced an observable signal yet.
func (r *PipelineRunReconciler) updateSecretsResolvedCondition(
	ctx context.Context,
	pr *datupletv1.PipelineRun,
	pipeline *datupletv1.Pipeline,
	job *batchv1.Job,
) {
	if pipeline.Spec.SecretsRef == nil {
		return
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		return
	}
	if len(pods.Items) == 0 {
		return
	}
	pod := &pods.Items[len(pods.Items)-1]

	// FailedMount on the datuplet-secrets volume -> the Secret object is missing.
	// (We list namespace-wide and filter in memory; indexing involvedObject.name
	// would require a manager-level FieldIndexer and isn't worth it for the event
	// volume of a pipeline namespace.)
	events := &corev1.EventList{}
	if err := r.List(ctx, events, client.InNamespace(pod.Namespace)); err == nil {
		for _, e := range events.Items {
			if e.InvolvedObject.Name != pod.Name {
				continue
			}
			if e.Reason == "FailedMount" && strings.Contains(e.Message, "datuplet-secrets") {
				meta.SetStatusCondition(&pr.Status.Conditions, metav1.Condition{
					Type:    datupletv1.PipelineRunSecretsResolved,
					Status:  metav1.ConditionFalse,
					Reason:  datupletv1.PipelineRunReasonSecretsRefMissing,
					Message: e.Message,
				})
				return
			}
		}
	}

	// Gateway init container observations: terminated non-zero before Ready
	// means a missing key file (or unmarshal error); Ready means it resolved
	// everything and served its first GetConfig.
	for _, c := range pod.Status.InitContainerStatuses {
		if c.Name != "gateway" {
			continue
		}
		if c.LastTerminationState.Terminated != nil && c.LastTerminationState.Terminated.ExitCode != 0 {
			meta.SetStatusCondition(&pr.Status.Conditions, metav1.Condition{
				Type:    datupletv1.PipelineRunSecretsResolved,
				Status:  metav1.ConditionFalse,
				Reason:  datupletv1.PipelineRunReasonSecretNotFound,
				Message: c.LastTerminationState.Terminated.Message,
			})
			return
		}
		if c.Ready {
			meta.SetStatusCondition(&pr.Status.Conditions, metav1.Condition{
				Type:   datupletv1.PipelineRunSecretsResolved,
				Status: metav1.ConditionTrue,
				Reason: datupletv1.PipelineRunReasonSecretsResolved,
			})
			return
		}
	}
}

// extractExitCodeFromJob extracts the exit code from the first pod of a job.
func (r *PipelineRunReconciler) extractExitCodeFromJob(ctx context.Context, job *batchv1.Job) *int32 {
	logger := log.FromContext(ctx)

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		logger.V(1).Info("Failed to list pods for job", "job", job.Name, "error", err)
		return nil
	}

	if len(pods.Items) == 0 {
		return nil
	}

	// Use the last pod (most recent attempt) to get the exit code
	return extractExitCodeFromPod(&pods.Items[len(pods.Items)-1], "component")
}

// extractExitCodeFromPod extracts the exit code for a named container from a pod.
func extractExitCodeFromPod(pod *corev1.Pod, containerName string) *int32 {
	// Check regular container statuses
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == containerName && cs.State.Terminated != nil {
			code := cs.State.Terminated.ExitCode
			return &code
		}
	}
	// Check init container statuses
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Name == containerName && cs.State.Terminated != nil {
			code := cs.State.Terminated.ExitCode
			return &code
		}
	}
	return nil
}

// extractComponentStatusMessage fetches pod logs and extracts a status message.
func (r *PipelineRunReconciler) extractComponentStatusMessage(ctx context.Context, job *batchv1.Job, exitCode int) string {
	if r.Clientset == nil {
		return ""
	}

	logs, err := r.getPodLogs(ctx, job.Namespace, job.Name, "component")
	if err != nil {
		return ""
	}

	return status.ExtractStatusMessage(logs, exitCode)
}

// getPodLogs fetches logs from the last pod (most recent attempt) of a job for a specific container.
func (r *PipelineRunReconciler) getPodLogs(ctx context.Context, namespace, jobName, containerName string) (string, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{"job-name": jobName}); err != nil {
		return "", err
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pods found for job %s", jobName)
	}

	podName := pods.Items[len(pods.Items)-1].Name
	logStream, err := r.Clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
	}).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer logStream.Close()

	logBytes, err := io.ReadAll(logStream)
	if err != nil {
		return "", err
	}

	return string(logBytes), nil
}
