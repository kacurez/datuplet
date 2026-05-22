package controllers

import (
	"context"
	"fmt"
	"strings"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/lib/status"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The PipelineRun controller schedules a `batch/v1.Job` per (run, stage,
// bucket) directly when a stage's component Pods complete. The TableCommit
// CRD and tablecommit-operator have been removed. Owner ref points at the
// PipelineRun so kubectl-driven deletion + GC stay consistent. K8s native
// Job machinery (BackoffLimit, TTLSecondsAfterFinished) handles retry
// and cleanup.
//
// This file holds the Job builder + helpers. The spawned Pod's env vars,
// run-token mount, and warehouse-root derivation flow in as parameters
// rather than being read off a CRD.

const (
	// DefaultTableCommitImage is the default image for table-commit jobs.
	DefaultTableCommitImage = "datuplet/iceberg-job:latest"

	// CommitJobTTLSecondsAfterFinished is the TTL for completed commit jobs.
	// Mirrors the old TableCommit reconciler's value so observability
	// (logs / `kubectl describe`) gives roughly the same window the
	// migration replaces.
	CommitJobTTLSecondsAfterFinished = int32(600)

	// commitContainerName is the name of the container running the
	// iceberg-job binary in --mode=table-commit (default mode). Kept
	// distinct from the PipelineRun's "component" container constant so
	// log/exit-code lookups can disambiguate.
	//
	// Why "tablecommit" and not "iceberg-job": this name pre-dates the
	// pkg/tablecommit → pkg/icebergjob rename. It is referenced by
	// extractExitCodeFromPod (CLAUDE.md non-obvious-conventions) which
	// looks for the "tablecommit" container by name; changing it would
	// break exit-code extraction without a paired operator + extractor
	// update. Leave it as-is for backward compatibility.
	commitContainerName = "tablecommit"

	// commitRunTokenMountPath is the in-container directory the spawned
	// commit job reads its run-scoped JWT map from. Identical to the
	// path used by the gateway sidecar in pipelinerun_jobs.go — kept as
	// a separate constant so a future rename on the sidecar side
	// doesn't accidentally drift the commit-job side.
	commitRunTokenMountPath = "/var/run/secrets/datuplet-runtoken"
	// commitRunTokenKey is the projected file name of the per-run JWT
	// under commitRunTokenMountPath. The projected file is the SINGLE
	// `token` (raw JWT, no JSON wrapping).
	commitRunTokenKey = "token"
	// commitRunTokenFSGroup is the GID that owns the projected Secret
	// files; matches secretsMountFSGroup used by applyRunTokenMount in
	// pipelinerun_jobs.go.
	commitRunTokenFSGroup int64 = 65532
)

// commitJobName generates the Job name for a (PipelineRun, bucket).
// Same scheme as the old tablecommit-operator's jobName so existing
// log-tail tooling that greps for "tc-<bucket>-<runprefix>" keeps
// working across the migration.
func commitJobName(pr *datupletv1.PipelineRun, bucket string) string {
	bucketSan := strings.ReplaceAll(bucket, "_", "-")
	sessionID := pr.Status.RunID
	if len(sessionID) > 8 {
		sessionID = sessionID[:8]
	}
	return fmt.Sprintf("tc-%s-%s", bucketSan, sessionID)
}

// buildCommitJob constructs the batch/v1.Job spec for a single
// (PipelineRun, bucket, writeMode) tuple. Ported from
// tablecommit_controller.go's buildJob with the only structural change
// being that bucket/runID/writeMode/runTokenRef now flow in as
// parameters instead of being read off a CRD.
func (r *PipelineRunReconciler) buildCommitJob(pr *datupletv1.PipelineRun, bucket string, writeMode datupletv1.WriteMode) *batchv1.Job {
	jobName := commitJobName(pr, bucket)
	ttl := CommitJobTTLSecondsAfterFinished

	image := r.TableCommitImage
	if image == "" {
		image = DefaultTableCommitImage
	}

	if writeMode == "" {
		writeMode = datupletv1.WriteModeAppend
	}

	// Build environment for the commit container
	var env []corev1.EnvVar

	env = append(env,
		corev1.EnvVar{Name: "RUN_ID", Value: pr.Status.RunID},
		corev1.EnvVar{Name: "BUCKET", Value: bucket},
		corev1.EnvVar{Name: "WRITE_MODE", Value: string(writeMode)},
	)

	// TableCommit talks directly to lakekeeper for metadata commits. The
	// reconciler is configured with the catalog URL via flags/env. Warehouse
	// + project_id come from the validated JWT claims; the operator does not
	// inject them as env vars.
	if r.LakekeeperURL != "" {
		env = append(env, corev1.EnvVar{Name: "LAKEKEEPER_URL", Value: r.LakekeeperURL})
	}
	// Inject JWKS URL so the commit Job can validate the mounted run-token
	// JWT before using any claims.
	if r.PipelineAPIURL != "" {
		env = append(env, corev1.EnvVar{
			Name:  "PIPELINE_API_JWKS_URL",
			Value: strings.TrimSuffix(r.PipelineAPIURL, "/") + "/api/v1/auth/jwks.json",
		})
	}

	// S3_ENDPOINT / S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY /
	// S3_USE_PATH_STYLE are intentionally absent. Commit Job Pods carry
	// no long-lived S3 credentials. All data-plane access goes through
	// lakekeeper-vended STS credentials obtained via the run-token JWT.

	// When the PipelineRun has a RunTokenRef, point the commit binary
	// at the mounted file via RUN_TOKEN_PATH. The projected file is a
	// SINGLE raw JWT. Missing file is benign — pipelines without auth
	// wiring still run; lakekeeper-with-OIDC rejects with a clear 401.
	if pr.Spec.RunTokenRef != nil {
		env = append(env, corev1.EnvVar{
			Name:  "RUN_TOKEN_PATH",
			Value: commitRunTokenMountPath + "/" + commitRunTokenKey,
		})
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pr.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/component": "table-commit",
				"app.kubernetes.io/part-of":   "datuplet",
				"datuplet.io/bucket":          strings.ReplaceAll(bucket, "_", "-"),
				"datuplet.io/run-id":          pr.Status.RunID,
				"datuplet.io/pipelinerun":     pr.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            int32Ptr(0),
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/component": "table-commit",
						"datuplet.io/run-id":          pr.Status.RunID,
						"datuplet.io/pipelinerun":     pr.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Tolerations:   r.RuntimeTolerations, // nil-safe: omitted from Pod when nil
					Containers: []corev1.Container{
						{
							Name:  commitContainerName,
							Image: image,
							// PullAlways so each iteration of the loop (RFC 020) gets the
							// freshly-pushed commit-job image rather than a cached one.
							ImagePullPolicy: corev1.PullAlways,
							Env:             env,
						},
					},
				},
			},
		},
	}

	applyCommitRunTokenMount(&job.Spec.Template.Spec, &job.Spec.Template.Spec.Containers[0], pr)

	return job
}

// applyCommitRunTokenMount adds the run-token Secret volume to podSpec
// and mounts it on the commit container. No-op when pr.Spec.RunTokenRef
// is nil — the container then runs without auth and a lakekeeper that
// requires JWTs rejects it loudly rather than silently dropping the token.
//
// Unlike the PipelineRun's component flow, the commit Pod has no
// gateway sidecar; the run-token file goes directly on the commit
// container. The file is a SINGLE raw JWT — the container reads it once
// and attaches `Authorization: Bearer …` on every lakekeeper REST call.
func applyCommitRunTokenMount(podSpec *corev1.PodSpec, container *corev1.Container, pr *datupletv1.PipelineRun) {
	if pr.Spec.RunTokenRef == nil {
		return
	}
	if podSpec.SecurityContext == nil {
		podSpec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if podSpec.SecurityContext.FSGroup == nil {
		fs := commitRunTokenFSGroup
		podSpec.SecurityContext.FSGroup = &fs
	}

	optional := false
	mode := int32(0o440)
	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name: "datuplet-runtoken",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  pr.Spec.RunTokenRef.Name,
				Optional:    &optional, // FailedMount if the Secret is missing
				DefaultMode: &mode,     // root:fsGroup, group-readable only
				// Project the SINGLE per-run JWT (raw string, no JSON
				// wrapping) at .../runtoken/token. The commit binary
				// reads the file once and attaches the contents as
				// `Authorization: Bearer …` on every lakekeeper REST call.
				Items: []corev1.KeyToPath{
					{Key: commitRunTokenKey, Path: commitRunTokenKey},
				},
			},
		},
	})

	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      "datuplet-runtoken",
		MountPath: commitRunTokenMountPath,
		ReadOnly:  true,
	})
}

// commitJobPhase maps a Job's status to the TableCommitPhase enum the
// PipelineRun.Status.TableCommits[i].Phase field already exposes — kept
// identical to the old CRD's surface so UI/pipeline-api consumers don't
// need a migration.
func commitJobPhase(job *batchv1.Job) datupletv1.TableCommitPhase {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return datupletv1.TableCommitPhaseSucceeded
		}
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return datupletv1.TableCommitPhaseFailedApplication
		}
	}
	if job.Status.Active > 0 || job.Status.Succeeded > 0 || job.Status.Failed > 0 {
		return datupletv1.TableCommitPhaseRunning
	}
	return datupletv1.TableCommitPhasePending
}

// extractCommitJobExitCode pulls the exit code from the most recent
// commit Pod attached to job. Returns nil when no Pod is around or the
// commit container hasn't terminated yet.
func (r *PipelineRunReconciler) extractCommitJobExitCode(ctx context.Context, job *batchv1.Job) *int32 {
	logger := log.FromContext(ctx)

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		logger.V(1).Info("Failed to list pods for commit job", "job", job.Name, "error", err)
		return nil
	}
	if len(pods.Items) == 0 {
		return nil
	}
	return extractExitCodeFromPod(&pods.Items[len(pods.Items)-1], commitContainerName)
}

// extractCommitJobStatusMessage fetches the commit Pod's logs and
// applies the same DUPLET_STATUS_MESSAGE: log-tail extraction the
// component path uses, so per-bucket failure messages surface in
// PipelineRun status.
func (r *PipelineRunReconciler) extractCommitJobStatusMessage(ctx context.Context, job *batchv1.Job, exitCode int) string {
	if r.Clientset == nil {
		return ""
	}
	logs, err := r.getPodLogs(ctx, job.Namespace, job.Name, commitContainerName)
	if err != nil {
		return ""
	}
	return status.ExtractStatusMessage(logs, exitCode)
}

// commitJobLookup returns the commit Job for (pr, bucket) along with a
// flag indicating whether it exists. Caller distinguishes a real
// IsNotFound (returned as `false, nil`) from a transient error.
func (r *PipelineRunReconciler) commitJobLookup(ctx context.Context, pr *datupletv1.PipelineRun, bucket string) (*batchv1.Job, bool, error) {
	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: commitJobName(pr, bucket), Namespace: pr.Namespace}, job)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return job, true, nil
}
