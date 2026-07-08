package controllers

import (
	"context"
	"fmt"
	"io"

	"github.com/datuplet/datuplet/pkg/lib/status"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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
