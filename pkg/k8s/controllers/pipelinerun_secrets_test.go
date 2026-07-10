package controllers

// RFC 026 P1.5 Task S3: per-run secret snapshot at admission.
//
// handlePending snapshots ONLY the $[name] keys the pipeline references from
// the managed project Secret (datuplet-project-secrets) into a per-run Secret
// (datuplet-runsecrets-<shortID>). The snapshot is ownerRef'd to the run,
// immutable for its life (rotation-exact), and mounted at the existing
// gateway-sidecar secrets path. A missing key fails the run FailedUser at
// admission with zero Jobs and zero snapshot.

import (
	"context"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// pipelineRefSecret returns a valid Pipeline whose single component references
// the given $[name] secret in component.config and writes to a valid bucket
// (so it passes validate.ValidateTyped).
func pipelineRefSecret(name, secretRef string) *datupletv1.Pipeline {
	cfg := `{"api_key":"$[` + secretRef + `]"}`
	return &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:      "c1",
					Component: "comp-a",
					Config:    apiextensionsv1.JSON{Raw: []byte(cfg)},
					Outputs:   &datupletv1.OutputSpec{DefaultBucket: "output-bucket"},
				}},
			}},
		},
	}
}

// pipelineNoSecret returns a valid Pipeline with no $[name] references.
func pipelineNoSecret(name string) *datupletv1.Pipeline {
	return &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:      "c1",
					Component: "comp-a",
					Outputs:   &datupletv1.OutputSpec{DefaultBucket: "output-bucket"},
				}},
			}},
		},
	}
}

func pendingRun(name, runID string) *datupletv1.PipelineRun {
	return &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: datupletv1.PipelineRunSpec{
			PipelineRef: datupletv1.PipelineRef{Name: "p1"},
			RunID:       runID,
		},
	}
}

func projectSecret(data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: datupletv1.ProjectSecretsName, Namespace: "default"},
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	}
}

func reconcilePR(t *testing.T, r *PipelineRunReconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// (a) referenced key present -> snapshot created with ONLY that key, ownerRef
// set, and the component Job mounts the snapshot Secret by name.
func TestAdmission_SnapshotCreated_ReferencedKeyOnly(t *testing.T) {
	scheme := newTestScheme(t)
	const runID = "00000000-0000-0000-0000-0000000000aa"

	pipeline := pipelineRefSecret("p1", "api_token")
	pr := pendingRun("pr1", runID)
	proj := projectSecret(map[string][]byte{
		"api_token": []byte("s3cr3t"),
		"unrelated": []byte("nope"),
	})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr, proj, compDef("comp-a", ver("v1.0.0", "datuplet/test:latest"))).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{Client: fakeClient, APIReader: fakeClient, Scheme: scheme, Registry: ComponentRegistry{Reader: fakeClient}}

	// Admission.
	reconcilePR(t, r, "pr1")

	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}
	if got.Status.Phase != datupletv1.PipelineRunPhaseRunning {
		t.Fatalf("phase = %q, want Running", got.Status.Phase)
	}

	snap := &corev1.Secret{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: runSecretsName(got), Namespace: "default"}, snap); err != nil {
		t.Fatalf("Get snapshot Secret %q: %v", runSecretsName(got), err)
	}
	if len(snap.Data) != 1 {
		t.Fatalf("snapshot has %d keys, want exactly 1 (only referenced key): %v", len(snap.Data), keysOf(snap.Data))
	}
	if string(snap.Data["api_token"]) != "s3cr3t" {
		t.Errorf("snapshot api_token = %q, want s3cr3t", string(snap.Data["api_token"]))
	}
	if _, ok := snap.Data["unrelated"]; ok {
		t.Error("snapshot must not contain unreferenced key 'unrelated'")
	}

	// OwnerRef -> PipelineRun, controller.
	if len(snap.OwnerReferences) != 1 {
		t.Fatalf("snapshot ownerRefs = %d, want 1", len(snap.OwnerReferences))
	}
	or := snap.OwnerReferences[0]
	if or.Kind != "PipelineRun" || or.Name != "pr1" || or.Controller == nil || !*or.Controller {
		t.Errorf("snapshot ownerRef = %+v, want controller ref to PipelineRun/pr1", or)
	}

	// Drive the stage so the component Job is built, then assert it mounts
	// the snapshot Secret by name.
	reconcilePR(t, r, "pr1")

	jobName := componentJobName(got, "extract", "c1")
	job := &batchv1.Job{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: jobName, Namespace: "default"}, job); err != nil {
		t.Fatalf("Get component Job %q: %v", jobName, err)
	}
	if !jobMountsSecret(job, runSecretsName(got)) {
		t.Errorf("component Job does not mount snapshot Secret %q; volumes=%v", runSecretsName(got), job.Spec.Template.Spec.Volumes)
	}
}

// (b) referenced key missing -> run FailedUser, message names the key, and
// ZERO Jobs + ZERO snapshot are created.
func TestAdmission_MissingKey_FailsUser_NoJobsNoSnapshot(t *testing.T) {
	scheme := newTestScheme(t)
	const runID = "00000000-0000-0000-0000-0000000000bb"

	pipeline := pipelineRefSecret("p1", "api_token")
	pr := pendingRun("pr1", runID)
	// Project secret exists but lacks api_token.
	proj := projectSecret(map[string][]byte{"other": []byte("x")})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr, proj, compDef("comp-a", ver("v1.0.0", "datuplet/test:latest"))).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{Client: fakeClient, APIReader: fakeClient, Scheme: scheme, Registry: ComponentRegistry{Reader: fakeClient}}

	reconcilePR(t, r, "pr1")

	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}
	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
		t.Fatalf("phase = %q, want FailedUser", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Message, "api_token") {
		t.Errorf("message %q does not name the missing key 'api_token'", got.Status.Message)
	}

	// Zero snapshot.
	snap := &corev1.Secret{}
	err := fakeClient.Get(context.Background(), types.NamespacedName{Name: runSecretsName(got), Namespace: "default"}, snap)
	if err == nil {
		t.Error("snapshot Secret was created on a missing-key admission failure; want none")
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error getting snapshot: %v", err)
	}

	// Zero Jobs.
	jobs := &batchv1.JobList{}
	if err := fakeClient.List(context.Background(), jobs); err != nil {
		t.Fatalf("List Jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Errorf("expected 0 Jobs on admission failure, found %d", len(jobs.Items))
	}
}

// (c) rotation isolation: mutating the project Secret after the snapshot
// exists does not change the mounted Secret name or the snapshot content.
func TestAdmission_RotationIsolation_SnapshotImmutable(t *testing.T) {
	scheme := newTestScheme(t)
	const runID = "00000000-0000-0000-0000-0000000000cc"

	pipeline := pipelineRefSecret("p1", "api_token")
	pr := pendingRun("pr1", runID)
	proj := projectSecret(map[string][]byte{"api_token": []byte("v1")})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr, proj, compDef("comp-a", ver("v1.0.0", "datuplet/test:latest"))).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{Client: fakeClient, APIReader: fakeClient, Scheme: scheme, Registry: ComponentRegistry{Reader: fakeClient}}

	// Admission snapshots v1.
	reconcilePR(t, r, "pr1")

	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}
	wantName := runSecretsName(got)

	// Rotate the managed project Secret to v2.
	live := &corev1.Secret{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: datupletv1.ProjectSecretsName, Namespace: "default"}, live); err != nil {
		t.Fatalf("Get project Secret: %v", err)
	}
	live.Data["api_token"] = []byte("v2")
	if err := fakeClient.Update(context.Background(), live); err != nil {
		t.Fatalf("Update project Secret: %v", err)
	}

	// Reconcile the next stage.
	reconcilePR(t, r, "pr1")

	// Snapshot content unchanged (still v1).
	snap := &corev1.Secret{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: wantName, Namespace: "default"}, snap); err != nil {
		t.Fatalf("Get snapshot Secret: %v", err)
	}
	if string(snap.Data["api_token"]) != "v1" {
		t.Errorf("snapshot api_token = %q after rotation, want v1 (immutable for run life)", string(snap.Data["api_token"]))
	}

	// Mounted name unchanged.
	jobName := componentJobName(got, "extract", "c1")
	job := &batchv1.Job{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: jobName, Namespace: "default"}, job); err != nil {
		t.Fatalf("Get component Job: %v", err)
	}
	if !jobMountsSecret(job, wantName) {
		t.Errorf("component Job does not mount the original snapshot %q; volumes=%v", wantName, job.Spec.Template.Spec.Volumes)
	}
}

// (d) no refs -> no snapshot, no mount, SecretsResolved condition absent.
func TestAdmission_NoRefs_NoSnapshotNoMountNoCondition(t *testing.T) {
	scheme := newTestScheme(t)
	const runID = "00000000-0000-0000-0000-0000000000dd"

	pipeline := pipelineNoSecret("p1")
	pr := pendingRun("pr1", runID)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr, compDef("comp-a", ver("v1.0.0", "datuplet/test:latest"))).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{Client: fakeClient, APIReader: fakeClient, Scheme: scheme, Registry: ComponentRegistry{Reader: fakeClient}}

	reconcilePR(t, r, "pr1")

	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}
	if got.Status.Phase != datupletv1.PipelineRunPhaseRunning {
		t.Fatalf("phase = %q, want Running", got.Status.Phase)
	}

	// Condition absent.
	for _, c := range got.Status.Conditions {
		if c.Type == datupletv1.PipelineRunSecretsResolved {
			t.Errorf("SecretsResolved condition present (%+v); want absent when no refs", c)
		}
	}

	// No snapshot.
	snap := &corev1.Secret{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: runSecretsName(got), Namespace: "default"}, snap); err == nil {
		t.Error("snapshot Secret created for a pipeline with no secret refs; want none")
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error getting snapshot: %v", err)
	}

	// Drive the stage; the Job must not mount any snapshot Secret.
	reconcilePR(t, r, "pr1")
	jobName := componentJobName(got, "extract", "c1")
	job := &batchv1.Job{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: jobName, Namespace: "default"}, job); err != nil {
		t.Fatalf("Get component Job: %v", err)
	}
	if jobMountsSecret(job, runSecretsName(got)) {
		t.Errorf("component Job mounts a snapshot Secret when pipeline has no refs; volumes=%v", job.Spec.Template.Spec.Volumes)
	}
}

// (e) partial-admission re-reconcile: a prior admission created the snapshot,
// but the status Update that flips the run to Running failed, so the run is
// still Pending. On the NEXT reconcile the project Secret has been rotated —
// the referenced key removed. The snapshot's existence must be authoritative:
// the run must NOT be flipped to FailedUser, and the snapshot content must be
// unchanged. (Test (c) only rotates AFTER the run is Running; this covers the
// admission re-reconcile gap where the project Secret would otherwise be
// re-read.)
func TestAdmission_PartialAdmission_SnapshotExists_ProjectKeyRemoved_NotFailed(t *testing.T) {
	scheme := newTestScheme(t)
	const runID = "00000000-0000-0000-0000-0000000000ee"

	pipeline := pipelineRefSecret("p1", "api_token")
	pr := pendingRun("pr1", runID)

	// Pre-existing snapshot from a prior (partial) admission, holding v1. Its
	// name is derived from the run-id exactly as snapshotRunSecrets derives it.
	snapName := runSecretsName(&datupletv1.PipelineRun{Status: datupletv1.PipelineRunStatus{RunID: runID}})
	preSnap := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: snapName, Namespace: "default"},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"api_token": []byte("v1")},
	}
	// Project Secret has since been rotated: the referenced key is GONE. Under
	// the old (project-Secret-first) order this would drive a FailedUser.
	proj := projectSecret(map[string][]byte{"unrelated": []byte("x")})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr, proj, preSnap, compDef("comp-a", ver("v1.0.0", "datuplet/test:latest"))).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{Client: fakeClient, APIReader: fakeClient, Scheme: scheme, Registry: ComponentRegistry{Reader: fakeClient}}

	// Re-reconcile the still-Pending run.
	reconcilePR(t, r, "pr1")

	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}
	// The run must NOT be failed — the existing snapshot is authoritative.
	if got.Status.Phase == datupletv1.PipelineRunPhaseFailedUser {
		t.Fatalf("run flipped to FailedUser on re-admission despite a valid pre-existing snapshot; message=%q", got.Status.Message)
	}
	if got.Status.Phase != datupletv1.PipelineRunPhaseRunning {
		t.Fatalf("phase = %q, want Running", got.Status.Phase)
	}

	// Snapshot content unchanged (still v1); the rotated project Secret must not
	// touch it.
	snap := &corev1.Secret{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: snapName, Namespace: "default"}, snap); err != nil {
		t.Fatalf("Get snapshot Secret: %v", err)
	}
	if string(snap.Data["api_token"]) != "v1" {
		t.Errorf("snapshot api_token = %q, want v1 (unchanged; project rotation must not touch it)", string(snap.Data["api_token"]))
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// jobMountsSecret reports whether the pod template has a volume backed by the
// named Secret.
func jobMountsSecret(job *batchv1.Job, secretName string) bool {
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Secret != nil && v.Secret.SecretName == secretName {
			return true
		}
	}
	return false
}
