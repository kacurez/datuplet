package controllers

// RFC 021 Task 7: After all components in a stage exit 0, the stage
// transitions Running -> Succeeded directly. No commit Job is created.

import (
	"context"
	"strings"
	"testing"
	"time"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	kfake "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// newTestScheme returns a runtime.Scheme with the datupletv1 types and
// the standard K8s batch + core types registered.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := datupletv1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if err := batchv1.AddToScheme(s); err != nil {
		t.Fatalf("batchv1.AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	return s
}

// TestStageRunningToSucceededDirect: after all components in a stage exit 0
// the stage transitions Running -> Succeeded directly (RFC 021).
// No table-commit Job must be created.
func TestStageRunningToSucceededDirect(t *testing.T) {
	scheme := newTestScheme(t)

	// One pipeline, one stage, one component.
	pipeline := &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: "default",
		},
		Status: datupletv1.PipelineStatus{Phase: datupletv1.PipelinePhaseReady},
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:      "c1",
					Component: "comp-a",
					Version:   "v1.0.0",
				}},
			}},
		},
	}

	// PipelineRun already in Running/stage-Running state, component succeeded.
	// The frozen resolvedSpec is what handleRunning reads (never the live Pipeline).
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr1",
			Namespace: "default",
		},
		Spec: datupletv1.PipelineRunSpec{
			PipelineRef: datupletv1.PipelineRef{Name: "p1"},
		},
		Status: datupletv1.PipelineRunStatus{
			Phase:        datupletv1.PipelineRunPhaseRunning,
			RunID:        "00000000-0000-0000-0000-000000000001",
			ResolvedSpec: pipeline.Spec.DeepCopy(),
			StageStatuses: []datupletv1.StageStatus{{
				Name:  "extract",
				Phase: datupletv1.StagePhaseRunning,
				ComponentStatuses: []datupletv1.ComponentStatus{{
					Name:  "c1",
					Phase: datupletv1.ComponentPhaseSucceeded,
				}},
			}},
		},
	}

	// Pre-existing component Job (already complete) so the controller can
	// read it during checkStageComponents. Labels match what
	// componentJobName generates.
	jobName := mustJobName(t, pr, "c1")
	compJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: "default",
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr, compJob).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	// Reconcile once: should advance the stage to Succeeded.
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pr1", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Reload the PipelineRun.
	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}

	if len(got.Status.StageStatuses) == 0 {
		t.Fatal("StageStatuses is empty after reconcile")
	}
	stagePhase := got.Status.StageStatuses[0].Phase
	if stagePhase != datupletv1.StagePhaseSucceeded {
		t.Errorf("stage phase = %q, want %q (Running->Succeeded direct, no Committing)", stagePhase, datupletv1.StagePhaseSucceeded)
	}

	// No table-commit Jobs must exist.
	jobList := &batchv1.JobList{}
	if err := fakeClient.List(context.Background(), jobList,
		client.InNamespace("default"),
		client.MatchingLabels{"app.kubernetes.io/component": "table-commit"},
	); err != nil {
		t.Fatalf("List Jobs: %v", err)
	}
	if len(jobList.Items) != 0 {
		t.Errorf("expected 0 table-commit Jobs, found %d", len(jobList.Items))
	}
}

// TestPipelineRunReconcile_InvalidPipeline_FailsUser_NoJobs: run admission
// runs validate.ValidateTyped on the fetched Pipeline itself — it does not
// gate on pipeline.Status.Phase (which the fake Pipeline below deliberately
// leaves unset). An invalid Pipeline must fail the run FailedUser with the
// first finding in the message, before any component Job is created.
func TestPipelineRunReconcile_InvalidPipeline_FailsUser_NoJobs(t *testing.T) {
	scheme := newTestScheme(t)

	pipeline := &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: "default",
		},
		// Status.Phase intentionally left unset ("") — the run controller
		// must not rely on the Pipeline controller having reconciled.
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:      "c1",
					Component: "comp-a",
					Outputs: &datupletv1.OutputSpec{
						DefaultBucket: "Bad_Bucket",
					},
				}},
			}},
		},
	}

	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr1",
			Namespace: "default",
		},
		Spec: datupletv1.PipelineRunSpec{
			PipelineRef: datupletv1.PipelineRef{Name: "p1"},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pr1", Namespace: "default"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}

	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, datupletv1.PipelineRunPhaseFailedUser)
	}
	if !strings.Contains(got.Status.Message, "Bad_Bucket") {
		t.Errorf("message = %q, want it to contain the first validate.Finding text about %q", got.Status.Message, "Bad_Bucket")
	}

	jobList := &batchv1.JobList{}
	if err := fakeClient.List(context.Background(), jobList, client.InNamespace("default")); err != nil {
		t.Fatalf("List Jobs: %v", err)
	}
	if len(jobList.Items) != 0 {
		t.Errorf("expected 0 Jobs created, found %d", len(jobList.Items))
	}
}

const testPreviewID = "22222222-2222-2222-2222-222222222222"

func inlineSpecRun() *datupletv1.PipelineRun {
	return &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr1", Namespace: "default"},
		Spec: datupletv1.PipelineRunSpec{
			RunID:        testPreviewID,
			PipelineSpec: singleStagePipeline("data-generator", "v1.0.0").Spec.DeepCopy(),
			Preview:      &datupletv1.PreviewSpec{ID: testPreviewID, RowLimit: 1000, TimeoutSeconds: 60},
		},
	}
}

func reconcilePR1(t *testing.T, r *PipelineRunReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pr1", Namespace: "default"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// An inline-spec PipelineRun is admitted without any Pipeline CR existing.
func TestHandlePending_InlineSpec(t *testing.T) {
	scheme := newTestScheme(t)
	pr := inlineSpecRun()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pr, compDef("data-generator", ver("v1.0.0", "img-dg:v1"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcilePR1(t, r)

	got := getRun(t, c)
	if got.Status.Phase != datupletv1.PipelineRunPhaseRunning {
		t.Fatalf("phase = %q (message %q), want Running", got.Status.Phase, got.Status.Message)
	}
	if got.Status.ResolvedSpec == nil || len(got.Status.ResolvedSpec.Stages) != 1 {
		t.Fatalf("ResolvedSpec = %+v, want one stage", got.Status.ResolvedSpec)
	}
	if got.Status.ResolvedSpec.Stages[0].Name != "extract" {
		t.Errorf("ResolvedSpec.Stages[0].Name = %q, want %q", got.Status.ResolvedSpec.Stages[0].Name, "extract")
	}
	if got.Status.PipelineGeneration != 0 {
		t.Errorf("PipelineGeneration = %d, want 0 for an inline spec", got.Status.PipelineGeneration)
	}
}

func TestHandlePending_NoRefNoSpec_FailedUser(t *testing.T) {
	scheme := newTestScheme(t)
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr1", Namespace: "default"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pr).WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcilePR1(t, r)

	got := getRun(t, c)
	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
		t.Fatalf("phase = %q, want FailedUser", got.Status.Phase)
	}
	if got.Status.Message != "spec.pipelineRef or spec.pipelineSpec is required" {
		t.Errorf("message = %q", got.Status.Message)
	}
}

// timedOutPreviewRun returns a Running preview run past its deadline whose
// first stage ("prep") succeeded and whose second stage ("extract", component
// c1 at s1-c0) is in flight, plus the Job name derived for c1.
func timedOutPreviewRun(t *testing.T) (*datupletv1.PipelineRun, string) {
	t.Helper()
	pr := inlineSpecRun()
	pr.UID = types.UID("pr1-uid")
	started := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	spec := pr.Spec.PipelineSpec.DeepCopy()
	spec.Stages = append([]datupletv1.StageSpec{{
		Name:       "prep",
		Components: []datupletv1.ComponentSpec{{Name: "c0", Component: "data-generator", Version: "v1.0.0"}},
	}}, spec.Stages...)
	pr.Status = datupletv1.PipelineRunStatus{
		Phase:        datupletv1.PipelineRunPhaseRunning,
		RunID:        testPreviewID,
		StartTime:    &started,
		ResolvedSpec: spec,
	}
	jobName := mustJobName(t, pr, "c1")
	pr.Status.StageStatuses = []datupletv1.StageStatus{
		{
			Name:              "prep",
			Phase:             datupletv1.StagePhaseSucceeded,
			ComponentStatuses: []datupletv1.ComponentStatus{{Name: "c0", Phase: datupletv1.ComponentPhaseSucceeded}},
		},
		{
			Name:  "extract",
			Phase: datupletv1.StagePhaseRunning,
			ComponentStatuses: []datupletv1.ComponentStatus{{
				Name:    "c1",
				Phase:   datupletv1.ComponentPhaseRunning,
				PodName: jobName,
			}},
		},
	}
	return pr, jobName
}

// A Running preview past its deadline is cancelled: pods annotated, the
// current stage's Jobs deleted, the run FailedUser with a timeout message.
func TestHandleRunning_PreviewDeadline(t *testing.T) {
	cases := []struct {
		name          string
		clientset     kubernetes.Interface
		wantComponent string
	}{
		// The fake clientset serves "fake logs" for every pod log request.
		{name: "last log captured", clientset: kfake.NewSimpleClientset(), wantComponent: "preview timed out; last log: fake logs"},
		{name: "no logs available", clientset: nil, wantComponent: "preview timed out"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			pr, jobName := timedOutPreviewRun(t)
			if want := "run-" + testPreviewID + "-s1-c0"; jobName != want {
				t.Fatalf("Job name = %q, want %q", jobName, want)
			}
			job := ownedBy(t, scheme, pr, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"}})
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: jobName + "-abcde", Namespace: "default",
				Labels: map[string]string{"datuplet.io/run-id": testPreviewID, "job-name": jobName},
			}}
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(pr, job, pod).WithStatusSubresource(pr).Build()
			r := registryReconciler(c, scheme)
			r.Clientset = tc.clientset

			reconcilePR1(t, r)

			got := getRun(t, c)
			if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
				t.Fatalf("phase = %q, want FailedUser", got.Status.Phase)
			}
			const wantMsg = "preview timed out after 60s during stage extract"
			if got.Status.Message != wantMsg {
				t.Errorf("message = %q, want %q", got.Status.Message, wantMsg)
			}
			if got.Status.CompletionTime == nil {
				t.Error("CompletionTime not set")
			}

			ss := got.Status.StageStatuses[1]
			if ss.Phase != datupletv1.StagePhaseFailedUser || ss.Message != wantMsg || ss.CompletionTime == nil {
				t.Errorf("stage status = {phase %q, message %q, completion %v}, want FailedUser/%q/set",
					ss.Phase, ss.Message, ss.CompletionTime, wantMsg)
			}
			cs := ss.ComponentStatuses[0]
			if cs.Phase != datupletv1.ComponentPhaseFailedUser || cs.CompletionTime == nil {
				t.Errorf("component status = {phase %q, completion %v}, want FailedUser/set", cs.Phase, cs.CompletionTime)
			}
			if cs.Message != tc.wantComponent {
				t.Errorf("component message = %q, want %q", cs.Message, tc.wantComponent)
			}

			err := c.Get(context.Background(), types.NamespacedName{Name: jobName, Namespace: "default"}, &batchv1.Job{})
			if !errors.IsNotFound(err) {
				t.Errorf("stage Job still present (err=%v), want deleted", err)
			}
			gotPod := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: "default"}, gotPod); err != nil {
				t.Fatalf("Get pod: %v", err)
			}
			if gotPod.Annotations["datuplet.io/cancel"] != "true" {
				t.Errorf("pod cancel annotation = %q, want true", gotPod.Annotations["datuplet.io/cancel"])
			}
		})
	}
}

// A Job under the derived name that another run controls is neither read nor
// deleted when the preview deadline passes; the run fails FailedApplication.
func TestHandleRunning_PreviewDeadline_ForeignJobUntouched(t *testing.T) {
	scheme := newTestScheme(t)
	pr, jobName := timedOutPreviewRun(t)
	job := ownedBy(t, scheme, foreignRun(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"}})
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pr, job).WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)
	r.Clientset = kfake.NewSimpleClientset()

	before := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: jobName, Namespace: "default"}, before); err != nil {
		t.Fatalf("Get Job: %v", err)
	}

	reconcilePR1(t, r)

	assertForeignFailure(t, c)
	assertUnchanged(t, c, before)
}

// Within its deadline, a preview run keeps running.
func TestHandleRunning_PreviewWithinDeadline(t *testing.T) {
	scheme := newTestScheme(t)
	pr := inlineSpecRun()
	started := metav1.NewTime(time.Now().Add(-10 * time.Second))
	pr.Status = datupletv1.PipelineRunStatus{
		Phase:        datupletv1.PipelineRunPhaseRunning,
		RunID:        testPreviewID,
		StartTime:    &started,
		ResolvedSpec: pr.Spec.PipelineSpec.DeepCopy(),
		StageStatuses: []datupletv1.StageStatus{{
			Name:              "extract",
			Phase:             datupletv1.StagePhaseRunning,
			ComponentStatuses: []datupletv1.ComponentStatus{{Name: "c1", Phase: datupletv1.ComponentPhaseRunning}},
		}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pr).WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcilePR1(t, r)

	if got := getRun(t, c); got.Status.Phase != datupletv1.PipelineRunPhaseRunning {
		t.Fatalf("phase = %q (message %q), want Running", got.Status.Phase, got.Status.Message)
	}
}

// Any accepted spec.runId spelling is stored in canonical form, so every name
// derived from it is a valid DNS-1123 label.
func TestAdmission_CanonicalizesRunID(t *testing.T) {
	const want = "11111111-aaaa-4444-8888-111111111111"
	for _, in := range []string{
		"11111111-AAAA-4444-8888-111111111111",
		"11111111aaaa44448888111111111111",
		"urn:uuid:11111111-aaaa-4444-8888-111111111111",
	} {
		t.Run(in, func(t *testing.T) {
			scheme := newTestScheme(t)
			pr := runFor("p1")
			pr.Spec.RunID = in
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(singleStagePipeline("comp-a", "v1.0.0"), pr, compDef("comp-a", ver("v1.0.0", "img-a:v1"))).
				WithStatusSubresource(pr).Build()
			r := registryReconciler(c, scheme)

			reconcileOnce(t, r) // admission
			got := getRun(t, c)
			if got.Status.RunID != want {
				t.Fatalf("status.runId = %q, want %q", got.Status.RunID, want)
			}
			reconcileOnce(t, r) // startStage
			name := mustJobName(t, got, "c1")
			if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
				t.Errorf("Job name %q is not a DNS-1123 label: %v", name, errs)
			}
			getJob(t, c, name)
		})
	}
}

// foreignRun is a different PipelineRun (distinct UID) used as the controller
// of objects that sit under names this test's run will derive.
func foreignRun() *datupletv1.PipelineRun {
	return &datupletv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{
		Name: "other", Namespace: "default", UID: types.UID("other-uid"),
	}}
}

func ownedBy(t *testing.T, scheme *runtime.Scheme, owner *datupletv1.PipelineRun, obj client.Object) client.Object {
	t.Helper()
	if err := ctrl.SetControllerReference(owner, obj, scheme); err != nil {
		t.Fatalf("SetControllerReference: %v", err)
	}
	return obj
}

func assertForeignFailure(t *testing.T, c client.Client) {
	t.Helper()
	got := getRun(t, c)
	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedApplication {
		t.Fatalf("phase = %q (message %q), want FailedApplication", got.Status.Phase, got.Status.Message)
	}
	if !strings.Contains(got.Status.Message, "belongs to another PipelineRun") {
		t.Errorf("message = %q, want it to contain %q", got.Status.Message, "belongs to another PipelineRun")
	}
}

func assertUnchanged(t *testing.T, c client.Client, before client.Object) {
	t.Helper()
	after := before.DeepCopyObject().(client.Object)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(before), after); err != nil {
		t.Fatalf("foreign %T %q gone: %v", before, before.GetName(), err)
	}
	if after.GetResourceVersion() != before.GetResourceVersion() {
		t.Errorf("foreign %T %q modified: resourceVersion %q -> %q", before, before.GetName(), before.GetResourceVersion(), after.GetResourceVersion())
	}
	if !metav1.IsControlledBy(after, foreignRun()) {
		t.Errorf("foreign %T %q no longer controlled by the other run", before, before.GetName())
	}
}

func TestReconcile_ForeignOwnedObjectsFailRun(t *testing.T) {
	const runID = "33333333-3333-4333-8333-333333333333"
	jobName := "run-" + runID + "-s0-c0"
	cases := []struct {
		name    string
		foreign client.Object
	}{
		{"Job", &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"}}},
		{"ConfigMap", &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "gateway-config-" + jobName, Namespace: "default"}}},
		{"Secret", &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "datuplet-runsecrets-" + runID, Namespace: "default"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			pr := pendingRun("pr1", runID)
			pr.UID = types.UID("pr1-uid")
			foreign := ownedBy(t, scheme, foreignRun(), tc.foreign)
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(pipelineRefSecret("p1", "api_token"), pr,
					projectSecret(map[string][]byte{"api_token": []byte("v1")}),
					compDef("comp-a", ver("v1.0.0", "img-a:v1")), foreign).
				WithStatusSubresource(pr).Build()
			r := registryReconciler(c, scheme)

			before := foreign.DeepCopyObject().(client.Object)
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(foreign), before); err != nil {
				t.Fatalf("Get foreign: %v", err)
			}
			for i := 0; i < 3 && getRun(t, c).Status.Phase != datupletv1.PipelineRunPhaseFailedApplication; i++ {
				reconcileOnce(t, r)
			}

			assertForeignFailure(t, c)
			assertUnchanged(t, c, before)
		})
	}
}

// The snapshot lookup misses, then a foreign-owned Secret appears before the
// Create lands: the AlreadyExists path must check ownership too.
func TestSnapshot_CreateRaceForeignOwned(t *testing.T) {
	const runID = "44444444-4444-4444-8444-444444444444"
	snapName := "datuplet-runsecrets-" + runID
	scheme := newTestScheme(t)
	pr := pendingRun("pr1", runID)
	pr.UID = types.UID("pr1-uid")
	foreign := ownedBy(t, scheme, foreignRun(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: snapName, Namespace: "default"},
	}).(*corev1.Secret)

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipelineRefSecret("p1", "api_token"), pr,
			projectSecret(map[string][]byte{"api_token": []byte("v1")}),
			compDef("comp-a", ver("v1.0.0", "img-a:v1"))).
		WithStatusSubresource(pr).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Secret); ok && obj.GetName() == snapName {
					if err := cl.Create(ctx, foreign.DeepCopy()); err != nil {
						return err
					}
					return errors.NewAlreadyExists(corev1.Resource("secrets"), snapName)
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r)

	assertForeignFailure(t, c)
	got := &corev1.Secret{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: snapName, Namespace: "default"}, got); err != nil {
		t.Fatalf("Get foreign Secret: %v", err)
	}
	if !metav1.IsControlledBy(got, foreignRun()) || len(got.Data) != 0 {
		t.Errorf("foreign Secret modified: ownerRefs=%v data=%v", got.OwnerReferences, got.Data)
	}
}
