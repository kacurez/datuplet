package controllers

import (
	"context"
	"fmt"
	"io"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/lib/status"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// updateSecretsResolvedCondition sets the SecretsResolved condition based on
// whether the run references any $[name] secrets. When it does, the per-run
// snapshot was validated + created at admission (snapshotRunSecrets), so the
// condition is True/Resolved. When it references none, the condition is left
// absent. The False/SnapshotMissing case is set at admission on the
// missing-key failure path, not here.
func (r *PipelineRunReconciler) updateSecretsResolvedCondition(pr *datupletv1.PipelineRun, pipeline *datupletv1.Pipeline) {
	if len(validate.ReferencedSecrets(pipeline)) == 0 {
		return
	}
	meta.SetStatusCondition(&pr.Status.Conditions, metav1.Condition{
		Type:   datupletv1.PipelineRunSecretsResolved,
		Status: metav1.ConditionTrue,
		Reason: datupletv1.PipelineRunReasonSecretsResolved,
	})
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

// extractImageIDFromJob returns the observed
// containerStatuses[name=="component"].imageID from the job's most recent
// pod, or "" if not yet reported (pod/job not found, or the image hasn't
// been pulled yet — the kubelet only populates ImageID once the container
// has started).
func (r *PipelineRunReconciler) extractImageIDFromJob(ctx context.Context, job *batchv1.Job) string {
	logger := log.FromContext(ctx)

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		logger.V(1).Info("Failed to list pods for job", "job", job.Name, "error", err)
		return ""
	}
	if len(pods.Items) == 0 {
		return ""
	}

	return extractImageIDFromPod(&pods.Items[len(pods.Items)-1], "component")
}

// extractImageIDFromPod returns the imageID reported for a named container,
// or "" if the container status isn't present yet.
func extractImageIDFromPod(pod *corev1.Pod, containerName string) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == containerName {
			return cs.ImageID
		}
	}
	return ""
}

// recordComponentImageID copies imageID into the status.components[] entry
// matching name — ONCE. A component instance's imageID is "first observed,
// frozen thereafter": once set, a later reconcile observing a different
// value (e.g. after a pod recreation) must never overwrite it (RFC 026
// §4.3: observed runtime fields are appended, not repeatedly rewritten).
// No-op if imageID is empty or no matching, not-yet-recorded entry exists.
func recordComponentImageID(pr *datupletv1.PipelineRun, name, imageID string) {
	if imageID == "" {
		return
	}
	for i := range pr.Status.Components {
		if pr.Status.Components[i].Name == name && pr.Status.Components[i].ImageID == "" {
			pr.Status.Components[i].ImageID = imageID
			return
		}
	}
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
