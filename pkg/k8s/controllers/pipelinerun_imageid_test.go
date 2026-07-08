package controllers

// RFC 026 P2 (Task R7): the run controller observes the pulled component
// image digest off the pod's containerStatuses and appends it to the frozen
// status.components[] entry (spec §4.3: "observed runtime fields (imageID)
// are appended later as pods report"). Capture is once-only: a later
// reconcile must never overwrite an already-recorded imageID.

import (
	"context"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// componentPod builds a Pod that looks like the one a component Job would
// spawn: labelled job-name so the poller's `client.MatchingLabels{"job-name":
// ...}` list finds it, carrying a "component" containerStatus with the given
// imageID.
func componentPod(name, jobName, imageID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{"job-name": jobName},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:    "component",
				ImageID: imageID,
				State:   corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// TestImageID_CapturedFromPodOnce_NeverOverwritten covers task R7 (a): once
// the pod reports containerStatuses[name=="component"].imageID, it is copied
// into the matching status.components[i].imageID; a later reconcile that
// observes a DIFFERENT imageID on the same pod (e.g. re-read after a
// transient update) must NOT clobber the recorded value.
func TestImageID_CapturedFromPodOnce_NeverOverwritten(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("comp-a", "v1.0.0")
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDef("comp-a", ver("v1.0.0", "img-a:v1"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r) // admission: resolvedSpec + components[0] frozen
	reconcileOnce(t, r) // startStage: builds the component Job

	got := getRun(t, c)
	jobName := componentJobName(got, "extract", "c1")

	pod := componentPod("c1-pod", jobName, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatalf("Create pod: %v", err)
	}

	reconcileOnce(t, r) // checkStageComponents: observes the pod's imageID
	got = getRun(t, c)
	if len(got.Status.Components) != 1 {
		t.Fatalf("Components len = %d, want 1", len(got.Status.Components))
	}
	if got.Status.Components[0].ImageID != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("Components[0].ImageID = %q, want the observed digest", got.Status.Components[0].ImageID)
	}

	// A later reconcile observes a DIFFERENT imageID on the same pod (e.g.
	// the pod object is re-read after some unrelated field changed). The
	// already-recorded imageID must be left untouched.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "c1-pod", Namespace: "default"}, pod); err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	pod.Status.ContainerStatuses[0].ImageID = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("Update pod status: %v", err)
	}

	reconcileOnce(t, r) // second observation
	got = getRun(t, c)
	if got.Status.Components[0].ImageID != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("Components[0].ImageID = %q, want unchanged first-observed digest (idempotent, no overwrite)", got.Status.Components[0].ImageID)
	}
}

// TestImageID_EmptyContainerStatus_LeavesImageIDUnset covers the case where
// the pod exists but the "component" container hasn't reported an imageID
// yet (image still pulling) — status.components[i].imageID must stay empty,
// not get clobbered with "".
func TestImageID_EmptyContainerStatus_LeavesImageIDUnset(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("comp-a", "v1.0.0")
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDef("comp-a", ver("v1.0.0", "img-a:v1"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r) // admission
	reconcileOnce(t, r) // startStage

	got := getRun(t, c)
	jobName := componentJobName(got, "extract", "c1")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "c1-pod", Namespace: "default", Labels: map[string]string{"job-name": jobName}},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "component",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
			}},
		},
	}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatalf("Create pod: %v", err)
	}

	reconcileOnce(t, r)
	got = getRun(t, c)
	if got.Status.Components[0].ImageID != "" {
		t.Fatalf("Components[0].ImageID = %q, want empty (not yet reported)", got.Status.Components[0].ImageID)
	}
	stageStatus := got.Status.StageStatuses[0]
	if stageStatus.ComponentStatuses[0].Phase != datupletv1.ComponentPhaseRunning {
		t.Fatalf("component phase = %q, want Running (still waiting to pull)", stageStatus.ComponentStatuses[0].Phase)
	}
}
