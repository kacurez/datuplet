package controllers

import (
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// minimalCommitPR returns a PipelineRun ready for buildCommitJob —
// runID populated so the job name suffix is non-empty.
func minimalCommitPR() *datupletv1.PipelineRun {
	pr := &datupletv1.PipelineRun{}
	pr.Name = "pr1"
	pr.Namespace = "default"
	pr.Status.RunID = "00000000-0000-0000-0000-000000000001"
	return pr
}

// findCommitVolume returns the named volume from podSpec.Volumes or nil.
func findCommitVolume(podSpec *corev1.PodSpec, name string) *corev1.Volume {
	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Name == name {
			return &podSpec.Volumes[i]
		}
	}
	return nil
}

// findCommitMount returns the mount for name on container or nil.
func findCommitMount(c *corev1.Container, name string) *corev1.VolumeMount {
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].Name == name {
			return &c.VolumeMounts[i]
		}
	}
	return nil
}

// findCommitEnv returns the env var with name on container or nil.
func findCommitEnv(c *corev1.Container, name string) *corev1.EnvVar {
	for i := range c.Env {
		if c.Env[i].Name == name {
			return &c.Env[i]
		}
	}
	return nil
}

// TestBuildCommitJob_LakekeeperEnvWired ensures the LAKEKEEPER_URL env
// var is wired on the spawned commit container. Warehouse and project_id
// come from the validated JWT claims, not operator-injected env vars.
func TestBuildCommitJob_LakekeeperEnvWired(t *testing.T) {
	r := &PipelineRunReconciler{
		LakekeeperURL:  "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog",
		PipelineAPIURL: "http://pipeline-api.datuplet.svc.cluster.local:8081",
	}
	job := r.buildCommitJob(minimalCommitPR(), "raw", datupletv1.WriteModeAppend)
	c := &job.Spec.Template.Spec.Containers[0]

	// LAKEKEEPER_URL must be present.
	e := findCommitEnv(c, "LAKEKEEPER_URL")
	if e == nil {
		t.Fatal("LAKEKEEPER_URL env missing")
	}
	if e.Value != "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog" {
		t.Errorf("LAKEKEEPER_URL = %q", e.Value)
	}

	// PIPELINE_API_JWKS_URL must be derived from PipelineAPIURL.
	j := findCommitEnv(c, "PIPELINE_API_JWKS_URL")
	if j == nil {
		t.Fatal("PIPELINE_API_JWKS_URL env missing")
	}
	wantJWKS := "http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json"
	if j.Value != wantJWKS {
		t.Errorf("PIPELINE_API_JWKS_URL = %q, want %q", j.Value, wantJWKS)
	}

	// Warehouse/project env vars must NOT appear; they come from the JWT.
	for _, name := range []string{"WAREHOUSE_NAME", "WAREHOUSE_ROOT", "LAKEKEEPER_PROJECT_ID"} {
		if e := findCommitEnv(c, name); e != nil {
			t.Errorf("%s must not appear on commit Job (comes from validated JWT, got %q)", name, e.Value)
		}
	}
}

// TestBuildCommitJob_LakekeeperEnv_OmittedWhenUnset documents that an
// operator with no lakekeeper fields set still emits a coherent Job.
// Warehouse/project env vars are never injected by the operator.
func TestBuildCommitJob_LakekeeperEnv_OmittedWhenUnset(t *testing.T) {
	r := &PipelineRunReconciler{}
	job := r.buildCommitJob(minimalCommitPR(), "raw", datupletv1.WriteModeAppend)
	c := &job.Spec.Template.Spec.Containers[0]
	for _, name := range []string{"LAKEKEEPER_URL", "WAREHOUSE_NAME", "WAREHOUSE_ROOT", "LAKEKEEPER_PROJECT_ID", "PIPELINE_API_JWKS_URL"} {
		if e := findCommitEnv(c, name); e != nil {
			t.Errorf("%s should be absent when reconciler field is empty (got %q)", name, e.Value)
		}
	}
}

func TestBuildCommitJob_NoRunTokenRef_NoMount(t *testing.T) {
	r := &PipelineRunReconciler{}
	job := r.buildCommitJob(minimalCommitPR(), "raw", datupletv1.WriteModeAppend)

	if vol := findCommitVolume(&job.Spec.Template.Spec, "datuplet-runtoken"); vol != nil {
		t.Error("datuplet-runtoken volume must be absent when RunTokenRef is nil")
	}
	c := &job.Spec.Template.Spec.Containers[0]
	if m := findCommitMount(c, "datuplet-runtoken"); m != nil {
		t.Error("datuplet-runtoken mount must be absent when RunTokenRef is nil")
	}
	if e := findCommitEnv(c, "RUN_TOKEN_PATH"); e != nil {
		t.Error("RUN_TOKEN_PATH env must be absent when RunTokenRef is nil")
	}
}

func TestBuildCommitJob_WithRunTokenRef_MountsSecretAndPassesPath(t *testing.T) {
	r := &PipelineRunReconciler{}
	pr := minimalCommitPR()
	pr.Spec.RunTokenRef = &datupletv1.RunTokenRef{Name: "runtoken-abc"}

	job := r.buildCommitJob(pr, "raw", datupletv1.WriteModeAppend)

	vol := findCommitVolume(&job.Spec.Template.Spec, "datuplet-runtoken")
	if vol == nil {
		t.Fatal("datuplet-runtoken volume missing")
	}
	if vol.Secret == nil || vol.Secret.SecretName != "runtoken-abc" {
		t.Errorf("SecretName = %v, want runtoken-abc", vol.Secret)
	}
	if vol.Secret.DefaultMode == nil || *vol.Secret.DefaultMode != 0o440 {
		t.Error("DefaultMode must be 0o440")
	}
	// Only the singular `token` key should be projected; extra keys on
	// the Secret must not leak to the container. Single per-run JWT.
	if len(vol.Secret.Items) != 1 || vol.Secret.Items[0].Key != "token" || vol.Secret.Items[0].Path != "token" {
		t.Errorf("Items = %v, want [{token token}]", vol.Secret.Items)
	}

	c := &job.Spec.Template.Spec.Containers[0]
	m := findCommitMount(c, "datuplet-runtoken")
	if m == nil {
		t.Fatal("datuplet-runtoken mount missing on commit container")
	}
	if m.MountPath != "/var/run/secrets/datuplet-runtoken" || !m.ReadOnly {
		t.Errorf("mount = %+v, want /var/run/secrets/datuplet-runtoken readOnly", m)
	}

	env := findCommitEnv(c, "RUN_TOKEN_PATH")
	if env == nil {
		t.Fatal("RUN_TOKEN_PATH env missing")
	}
	if env.Value != "/var/run/secrets/datuplet-runtoken/token" {
		t.Errorf("RUN_TOKEN_PATH = %q", env.Value)
	}

	// fsGroup is required so the non-root commit process can read the
	// projected group-readable file.
	if job.Spec.Template.Spec.SecurityContext == nil ||
		job.Spec.Template.Spec.SecurityContext.FSGroup == nil {
		t.Fatal("podSpec.SecurityContext.FSGroup must be set when RunTokenRef is configured")
	}
}

// TestBuildCommitJob_OwnerLabelsWired pins the labels the migration
// promised: bucket, run-id and pipelinerun all surface so the operator
// can list commit Jobs by pipeline run without per-CRD lookups.
func TestBuildCommitJob_OwnerLabelsWired(t *testing.T) {
	r := &PipelineRunReconciler{}
	pr := minimalCommitPR()
	pr.Name = "kbc-services-pr1"
	job := r.buildCommitJob(pr, "raw_data", datupletv1.WriteModeAppend)

	tests := []struct{ key, want string }{
		{"app.kubernetes.io/component", "table-commit"},
		{"datuplet.io/bucket", "raw-data"}, // underscore → dash for K8s validation
		{"datuplet.io/run-id", pr.Status.RunID},
		{"datuplet.io/pipelinerun", "kbc-services-pr1"},
	}
	for _, tt := range tests {
		got, ok := job.Labels[tt.key]
		if !ok {
			t.Errorf("label %q missing on Job", tt.key)
			continue
		}
		if got != tt.want {
			t.Errorf("Job label %q = %q, want %q", tt.key, got, tt.want)
		}
	}
}

// TestBuildCommitJob_CoreEnvAlwaysPresent: RUN_ID, BUCKET, WRITE_MODE
// must always make it onto the container regardless of optional
// reconciler config — these are what the commit binary itself reads.
func TestBuildCommitJob_CoreEnvAlwaysPresent(t *testing.T) {
	r := &PipelineRunReconciler{}
	pr := minimalCommitPR()
	job := r.buildCommitJob(pr, "raw", datupletv1.WriteModeFullLoad)
	c := &job.Spec.Template.Spec.Containers[0]

	tests := []struct{ name, want string }{
		{"RUN_ID", pr.Status.RunID},
		{"BUCKET", "raw"},
		{"WRITE_MODE", string(datupletv1.WriteModeFullLoad)},
	}
	for _, tt := range tests {
		e := findCommitEnv(c, tt.name)
		if e == nil {
			t.Errorf("%s env missing", tt.name)
			continue
		}
		if e.Value != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, e.Value, tt.want)
		}
	}
}

// TestBuildCommitJob_DefaultsWriteModeAppend: callers that hand an
// empty WriteMode should get APPEND. Mirrors the deleted CRD's
// kubebuilder default so existing pipelines without explicit modes
// keep behaving identically.
func TestBuildCommitJob_DefaultsWriteModeAppend(t *testing.T) {
	r := &PipelineRunReconciler{}
	pr := minimalCommitPR()
	job := r.buildCommitJob(pr, "raw", "")
	c := &job.Spec.Template.Spec.Containers[0]
	e := findCommitEnv(c, "WRITE_MODE")
	if e == nil {
		t.Fatal("WRITE_MODE env missing")
	}
	if e.Value != string(datupletv1.WriteModeAppend) {
		t.Errorf("WRITE_MODE = %q, want APPEND", e.Value)
	}
}

// TestBuildCommitJob_BackoffLimitZero pins the no-retry contract: a
// commit failure must surface immediately as JobFailed, not after up
// to N kubelet restarts. K8s native machinery replaces the deleted
// CRD's reconciliation loop, so this is the seat-belt that keeps
// failure mode parity.
func TestBuildCommitJob_BackoffLimitZero(t *testing.T) {
	r := &PipelineRunReconciler{}
	job := r.buildCommitJob(minimalCommitPR(), "raw", datupletv1.WriteModeAppend)
	if job.Spec.BackoffLimit == nil {
		t.Fatal("BackoffLimit must be set explicitly to 0")
	}
	if *job.Spec.BackoffLimit != 0 {
		t.Errorf("BackoffLimit = %d, want 0 (commit Jobs must not retry)", *job.Spec.BackoffLimit)
	}
}

// TestCommitJobPhase_Mapping validates the commit-Job → phase mapping
// the controller folds back into PipelineRun.Status.TableCommits[i].Phase.
func TestCommitJobPhase_Mapping(t *testing.T) {
	tests := []struct {
		name     string
		job      *batchv1.Job
		expected datupletv1.TableCommitPhase
	}{
		{
			name:     "no conditions, no active pods → Pending",
			job:      &batchv1.Job{},
			expected: datupletv1.TableCommitPhasePending,
		},
		{
			name: "active pod → Running",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{Active: 1},
			},
			expected: datupletv1.TableCommitPhaseRunning,
		},
		{
			name: "JobComplete=True → Succeeded",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{{
						Type:   batchv1.JobComplete,
						Status: corev1.ConditionTrue,
					}},
				},
			},
			expected: datupletv1.TableCommitPhaseSucceeded,
		},
		{
			name: "JobFailed=True → FailedApplication (commit failures are always app errors)",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{{
						Type:   batchv1.JobFailed,
						Status: corev1.ConditionTrue,
					}},
				},
			},
			expected: datupletv1.TableCommitPhaseFailedApplication,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := commitJobPhase(tt.job)
			if got != tt.expected {
				t.Errorf("commitJobPhase = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestCommitJobName_StableScheme pins the (run, bucket) → Job name
// scheme. Two PipelineRuns with different RunIDs must not collide
// even when the bucket is shared.
func TestCommitJobName_StableScheme(t *testing.T) {
	prA := &datupletv1.PipelineRun{}
	prA.Status.RunID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	prB := &datupletv1.PipelineRun{}
	prB.Status.RunID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

	if commitJobName(prA, "raw") == commitJobName(prB, "raw") {
		t.Fatal("commit Job names must differ across runs")
	}

	// Underscore -> dash so the bucket fragment is K8s-valid.
	got := commitJobName(prA, "raw_data")
	if got != "tc-raw-data-aaaaaaaa" {
		t.Errorf("commit Job name = %q, want tc-raw-data-aaaaaaaa", got)
	}

	// Sanity: avoid metav1 import drift in dependents.
	_ = metav1.Now
}

// TestBuildCommitJob_NoS3EnvOnPod verifies the negative contract: the
// spawned commit Job MUST NOT carry long-lived S3 credentials or
// operator-injected warehouse/project env vars. Commit Pods use only the
// run-token JWT + lakekeeper-vended STS creds.
func TestBuildCommitJob_NoS3EnvOnPod(t *testing.T) {
	r := &PipelineRunReconciler{
		LakekeeperURL: "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog",
	}
	pr := minimalCommitPR()
	pr.Namespace = "datuplet-00000000-0000-0000-0000-000000000abc"
	job := r.buildCommitJob(pr, "raw", datupletv1.WriteModeAppend)
	c := &job.Spec.Template.Spec.Containers[0]

	forbidden := []string{
		"S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_ENDPOINT",
		"S3_BUCKET", "S3_USE_PATH_STYLE",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"WAREHOUSE_NAME", "WAREHOUSE_ROOT", "LAKEKEEPER_PROJECT_ID",
	}
	for _, name := range forbidden {
		if e := findCommitEnv(c, name); e != nil {
			t.Errorf("commit Pod MUST NOT carry %s env var, got %q", name, e.Value)
		}
	}
}
