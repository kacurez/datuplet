package preview

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const jobNameLabel = "batch.kubernetes.io/job-name"

// NewClientsetLogTailer tails the newest pod of a Job. A Job without pods
// (already garbage-collected) tails to "".
func NewClientsetLogTailer(cs kubernetes.Interface) LogTailer { return &clientsetLogTailer{cs: cs} }

type clientsetLogTailer struct{ cs kubernetes.Interface }

func (t *clientsetLogTailer) Tail(ctx context.Context, namespace, jobName, container string, lines int64) (string, error) {
	pods, err := t.cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{jobNameLabel: jobName}).String(),
	})
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("list pods of job %s: %w", jobName, err)
	}
	var newest *corev1.Pod
	for i := range pods.Items {
		if newest == nil || newest.CreationTimestamp.Before(&pods.Items[i].CreationTimestamp) {
			newest = &pods.Items[i]
		}
	}
	if newest == nil {
		return "", nil
	}
	out, err := t.cs.CoreV1().Pods(namespace).GetLogs(newest.Name, &corev1.PodLogOptions{
		Container: container, TailLines: &lines,
	}).DoRaw(ctx)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("logs of %s/%s: %w", newest.Name, container, err)
	}
	return string(out), nil
}
