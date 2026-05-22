package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pipelineconfig "github.com/datuplet/datuplet/pkg/pipeline/config"
	"gopkg.in/yaml.v3"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// componentJobName returns the K8s Job name for (pr, stage, component).
// Keep this the single source of truth for the name scheme — both the
// builder (buildComponentJob) and the status-polling path in
// pipelinerun_controller.go derive the Job name from here so changes
// can't drift and leave one side looking for the wrong object.
//
// Include shortID(pr.Status.RunID) in the suffix so two runs of the
// same pipeline (e.g. "kbc-services-pipeline" and
// "kbc-services-pipeline2") don't collide on the truncated prefix.
func componentJobName(pr *datupletv1.PipelineRun, stageName, componentName string) string {
	return fmt.Sprintf("datuplet-%s-%s-%s-%s",
		pr.Name[:min(15, len(pr.Name))],
		stageName[:min(10, len(stageName))],
		componentName[:min(10, len(componentName))],
		shortID(pr.Status.RunID))
}

// buildComponentJob builds the Job + gateway ConfigMap for a single component.
func (r *PipelineRunReconciler) buildComponentJob(_ context.Context, pr *datupletv1.PipelineRun, pipeline *datupletv1.Pipeline, comp *datupletv1.ComponentSpec) (*batchv1.Job, *corev1.ConfigMap, error) {
	stageIdx := -1
	for i, stage := range pipeline.Spec.Stages {
		for _, c := range stage.Components {
			if c.Name == comp.Name {
				stageIdx = i
				break
			}
		}
	}
	if stageIdx == -1 {
		return nil, nil, fmt.Errorf("component not found in pipeline")
	}

	stage := &pipeline.Spec.Stages[stageIdx]

	jobName := componentJobName(pr, stage.Name, comp.Name)
	configMapName := fmt.Sprintf("gateway-config-%s", jobName)

	// Generate gateway config
	gatewayConfig := r.generateGatewayConfig(pr, pipeline, comp)

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: pr.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/component":  "gateway-config",
				"app.kubernetes.io/part-of":    "datuplet",
				"datuplet.io/pipelinerun":      pr.Name,
				"datuplet.io/run-id": pr.Status.RunID,
			},
		},
		Data: map[string]string{
			"gateway.yaml": gatewayConfig,
		},
	}

	// Build environment for component
	// Use 127.0.0.1 instead of localhost to force IPv4 (K8s pods may default to IPv6)
	env := []corev1.EnvVar{
		{
			Name:  "DATUPLET_GATEWAY_ADDR",
			Value: fmt.Sprintf("127.0.0.1:%d", PipelineRunGatewayPort),
		},
	}

	// Build gateway arguments. Per-table vended creds are fetched from
	// lakekeeper via the lakekeeper_url field in the config file
	// (--minio is the storage-mode flag for the sidecar's local file
	// emit path).
	//
	// --run-token-path points at the sidecar-only mount of the SINGLE
	// per-run JWT. --pod-annotations-path points at the downward-API
	// volume mounted by applyCancelAnnotationMount. When the operator sets
	// datuplet.io/cancel=true on the pod, the gateway's poll loop sees the
	// annotation and exits cleanly.
	gatewayArgs := []string{
		"--minio",
		"--config", "/config/gateway.yaml",
		"--addr", fmt.Sprintf(":%d", PipelineRunGatewayPort),
		"--run-token-path", runTokenMountPath + "/" + runTokenKey,
		"--pod-annotations-path", cancelAnnotationMountPath + "/" + cancelAnnotationFile,
	}

	gatewayEnv := r.buildGatewaySidecarEnv(pr)

	ttl := JobTTLSecondsAfterFinished
	restartPolicyAlways := corev1.ContainerRestartPolicyAlways

	gatewayImage := r.GatewayImage
	if gatewayImage == "" {
		gatewayImage = PipelineRunDefaultGatewayImage
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pr.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/component":  "component",
				"app.kubernetes.io/part-of":    "datuplet",
				"datuplet.io/pipelinerun":      pr.Name,
				"datuplet.io/component":        comp.Name,
				"datuplet.io/run-id": pr.Status.RunID,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            int32Ptr(1),
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/component":  "component",
						"datuplet.io/pipelinerun":      pr.Name,
						"datuplet.io/component":        comp.Name,
						"datuplet.io/run-id": pr.Status.RunID,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Tolerations:   r.RuntimeTolerations, // nil-safe: omitted from Pod when nil
					// Native sidecar in initContainers with restartPolicy: Always
					InitContainers: []corev1.Container{
						{
							Name:            "gateway",
							Image:           gatewayImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            gatewayArgs,
							Env:             gatewayEnv,
							RestartPolicy:   &restartPolicyAlways,
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: int32(PipelineRunGatewayPort),
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "gateway-config",
									MountPath: "/config",
									ReadOnly:  true,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "component",
							Image:           comp.Image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env:             env,
							// Component container hardening: makes the
							// sidecar-only run-token mount a real defense.
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: boolPtr(false),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
								SeccompProfile: &corev1.SeccompProfile{
									Type: corev1.SeccompProfileTypeRuntimeDefault,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "gateway-config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: configMapName,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Add resource requirements if specified
	if comp.Resources != nil {
		job.Spec.Template.Spec.Containers[0].Resources = *comp.Resources
	}

	// Mount the referenced Secret on the gateway sidecar (only) when secretsRef is set.
	applySecretsMount(
		&job.Spec.Template.Spec,
		&job.Spec.Template.Spec.InitContainers[0],
		pipeline,
	)

	// Mount the referenced Secret on the gateway sidecar (only) when runTokenRef is set.
	applyRunTokenMount(
		&job.Spec.Template.Spec,
		&job.Spec.Template.Spec.InitContainers[0],
		pr,
	)

	// Expose pod annotations to the gateway sidecar via the downward API
	// so DG can poll for an in-band cancel signal (`datuplet.io/cancel=true`).
	// The volume doesn't need special permissions; mounted read-only on the
	// gateway sidecar only (component containers don't need cancel awareness).
	applyCancelAnnotationMount(
		&job.Spec.Template.Spec,
		&job.Spec.Template.Spec.InitContainers[0],
	)

	// Pod-level automount of the ServiceAccount token is disabled
	// unconditionally. Nothing in the gateway sidecar or component container
	// talks to the K8s API; removing the token closes an unused bypass path
	// even for pipelines without secretsRef or runTokenRef.
	automount := false
	job.Spec.Template.Spec.AutomountServiceAccountToken = &automount

	return job, configMap, nil
}

// secretsMountPath is the in-container directory where pkg/lib/secrets.FileProvider
// looks up $[name] references.
const secretsMountPath = "/var/run/secrets/datuplet"

// secretsMountFSGroup is the GID the gateway process runs as so it can read files
// from the Secret volume when DefaultMode is 0440.
const secretsMountFSGroup = int64(65532)

// applySecretsMount augments podSpec with the Secret volume + gateway mount + pod
// security context needed to deliver Pipeline.spec.secretsRef.Name to the gateway
// sidecar. No-op when SecretsRef is nil.
func applySecretsMount(podSpec *corev1.PodSpec, gatewayContainer *corev1.Container, pipeline *datupletv1.Pipeline) {
	if pipeline.Spec.SecretsRef == nil {
		return
	}

	// Volume-owned files get the fsGroup; combined with DefaultMode 0440 the
	// gateway process (runAsGroup matching fsGroup) can read them while the
	// component — which has no mount — still cannot.
	if podSpec.SecurityContext == nil {
		podSpec.SecurityContext = &corev1.PodSecurityContext{}
	}
	fsGroup := secretsMountFSGroup
	podSpec.SecurityContext.FSGroup = &fsGroup

	optional := false
	mode := int32(0o440)
	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name: "datuplet-secrets",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  pipeline.Spec.SecretsRef.Name,
				Optional:    &optional, // FailedMount if absent
				DefaultMode: &mode,     // root:fsGroup, readable by group
			},
		},
	})

	// Gateway sidecar only — NOT the component container. Do not use subPath:
	// atomic Secret updates (future rotation) only propagate to non-subPath mounts.
	gatewayContainer.VolumeMounts = append(gatewayContainer.VolumeMounts, corev1.VolumeMount{
		Name:      "datuplet-secrets",
		MountPath: secretsMountPath,
		ReadOnly:  true,
	})

	runAsNonRoot := true
	uid := int64(65532)
	gid := int64(65532)
	if gatewayContainer.SecurityContext == nil {
		gatewayContainer.SecurityContext = &corev1.SecurityContext{}
	}
	gatewayContainer.SecurityContext.RunAsNonRoot = &runAsNonRoot
	gatewayContainer.SecurityContext.RunAsUser = &uid
	gatewayContainer.SecurityContext.RunAsGroup = &gid
}

// runTokenMountPath is the in-container directory where the gateway sidecar
// reads the run-scoped JWT written by pipeline-api into a K8s Secret.
const runTokenMountPath = "/var/run/secrets/datuplet-runtoken"

// runTokenKey is the Secret data key projected to a file at runTokenMountPath.
// The value is a SINGLE per-run JWT (raw string, no JSON wrapping).
// DG + TableCommit read this singular file.
const runTokenKey = "token"

// cancelAnnotationFile is the relative path inside the downward-API
// volume where the pod's annotations are projected. The gateway sidecar
// polls this file and looks for `datuplet.io/cancel: "true"` to decide
// whether to drain + exit cleanly.
const cancelAnnotationVolume = "podinfo"
const cancelAnnotationMountPath = "/etc/podinfo"
const cancelAnnotationFile = "annotations"

// applyRunTokenMount augments podSpec with the run-token Secret volume + gateway
// mount + pod/sidecar security context needed to deliver the JWT named by
// PipelineRun.spec.runTokenRef.Name to the gateway sidecar only. No-op when
// RunTokenRef is nil. Idempotent with applySecretsMount for fsGroup and the
// gateway SecurityContext (only sets fields when nil).
func applyRunTokenMount(podSpec *corev1.PodSpec, gatewayContainer *corev1.Container, pr *datupletv1.PipelineRun) {
	if pr.Spec.RunTokenRef == nil {
		return
	}

	if podSpec.SecurityContext == nil {
		podSpec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if podSpec.SecurityContext.FSGroup == nil {
		fs := secretsMountFSGroup
		podSpec.SecurityContext.FSGroup = &fs
	}

	optional := false
	mode := int32(0o440)
	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name: "datuplet-runtoken",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  pr.Spec.RunTokenRef.Name,
				Optional:    &optional, // FailedMount if Secret absent
				DefaultMode: &mode,     // root:fsGroup, readable by group only
				// Project the SINGLE per-run JWT (raw string, no JSON
				// wrapping) as a file named `token` under
				// runTokenMountPath. DG + TableCommit open
				// /var/run/secrets/datuplet-runtoken/token and attach
				// the contents as `Authorization: Bearer …` on every
				// lakekeeper call.
				Items: []corev1.KeyToPath{
					{Key: runTokenKey, Path: runTokenKey},
				},
			},
		},
	})

	// Gateway sidecar only — not the component container. No subPath so
	// future atomic Secret updates propagate.
	gatewayContainer.VolumeMounts = append(gatewayContainer.VolumeMounts, corev1.VolumeMount{
		Name:      "datuplet-runtoken",
		MountPath: runTokenMountPath,
		ReadOnly:  true,
	})

	if gatewayContainer.SecurityContext == nil {
		gatewayContainer.SecurityContext = &corev1.SecurityContext{}
	}
	if gatewayContainer.SecurityContext.RunAsNonRoot == nil {
		t := true
		gatewayContainer.SecurityContext.RunAsNonRoot = &t
	}
	if gatewayContainer.SecurityContext.RunAsUser == nil {
		u := int64(65532)
		gatewayContainer.SecurityContext.RunAsUser = &u
	}
	if gatewayContainer.SecurityContext.RunAsGroup == nil {
		g := int64(65532)
		gatewayContainer.SecurityContext.RunAsGroup = &g
	}
}

// applyCancelAnnotationMount adds a downward-API volume that exposes
// the pod's annotations as a flat `key="value"\n` file at
// /etc/podinfo/annotations on the gateway sidecar. When the cancel
// pathway sets `datuplet.io/cancel=true` on the pod, the projected
// file is re-rendered (typically within 60s per the kubelet's
// projection cadence) and DG's poll loop sees the new value and exits
// cleanly.
//
// Idempotent and side-effect-free for non-K8s code paths — the volume
// is always added, but if the gateway binary doesn't enable the cancel
// poller, the file is simply unread.
func applyCancelAnnotationMount(podSpec *corev1.PodSpec, gatewayContainer *corev1.Container) {
	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name: cancelAnnotationVolume,
		VolumeSource: corev1.VolumeSource{
			DownwardAPI: &corev1.DownwardAPIVolumeSource{
				Items: []corev1.DownwardAPIVolumeFile{
					{
						Path: cancelAnnotationFile,
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.annotations",
						},
					},
				},
			},
		},
	})
	gatewayContainer.VolumeMounts = append(gatewayContainer.VolumeMounts, corev1.VolumeMount{
		Name:      cancelAnnotationVolume,
		MountPath: cancelAnnotationMountPath,
		ReadOnly:  true,
	})
}

// generateGatewayConfig creates the gateway configuration YAML.
func (r *PipelineRunReconciler) generateGatewayConfig(pr *datupletv1.PipelineRun, pipeline *datupletv1.Pipeline, comp *datupletv1.ComponentSpec) string {
	// Convert config - handle both Config map and ConfigJSON
	config := make(map[string]any)
	for k, v := range comp.Config {
		config[k] = v
	}
	if comp.ConfigJSON != "" {
		var jsonConfig map[string]any
		if err := json.Unmarshal([]byte(comp.ConfigJSON), &jsonConfig); err == nil {
			for k, v := range jsonConfig {
				config[k] = v
			}
		}
	}

	// Build gateway settings
	gw := pipeline.Spec.Gateway
	chunkSize := gw.ChunkSize
	if chunkSize == 0 {
		chunkSize = 32 * 1024 * 1024
	}
	bufferSize := gw.BufferSize
	if bufferSize == 0 {
		bufferSize = 64 * 1024 * 1024
	}
	rowGroupSize := gw.RowGroupSize
	if rowGroupSize == 0 {
		rowGroupSize = bufferSize
	}
	targetFileSize := gw.TargetFileSize
	if targetFileSize == 0 {
		targetFileSize = 128 * 1024 * 1024
	}

	// Build config structure as a map for proper YAML marshaling
	configMap := make(map[string]any)
	configMap["mode"] = "minio"
	configMap["run_id"] = pr.Status.RunID

	// Add bucket-based fields
	inputBuckets := make([]string, 0)
	outputBuckets := make([]string, 0)
	inputBucketMap := make(map[string]bool)
	outputBucketMap := make(map[string]bool)

	// Extract input buckets and tables
	if comp.Inputs != nil {
		// Add bucket-level access grants
		for _, bucket := range comp.Inputs.Buckets {
			if bucket != "" && !inputBucketMap[bucket] {
				inputBuckets = append(inputBuckets, bucket)
				inputBucketMap[bucket] = true
			}
		}

		// Add buckets from explicit table inputs
		for _, tableSpec := range comp.Inputs.Tables {
			if tableSpec.Bucket != "" && !inputBucketMap[tableSpec.Bucket] {
				inputBuckets = append(inputBuckets, tableSpec.Bucket)
				inputBucketMap[tableSpec.Bucket] = true
			}
		}
	}

	// Extract output buckets from Outputs
	if comp.Outputs != nil {
		// DefaultBucket mode
		if comp.Outputs.DefaultBucket != "" && !outputBucketMap[comp.Outputs.DefaultBucket] {
			outputBuckets = append(outputBuckets, comp.Outputs.DefaultBucket)
			outputBucketMap[comp.Outputs.DefaultBucket] = true
		}

		// Explicit bucket outputs
		for _, bucketSpec := range comp.Outputs.Buckets {
			if bucketSpec.Name != "" && !outputBucketMap[bucketSpec.Name] {
				outputBuckets = append(outputBuckets, bucketSpec.Name)
				outputBucketMap[bucketSpec.Name] = true
			}
		}

		// Buckets from explicit table outputs
		for _, tableSpec := range comp.Outputs.Tables {
			if tableSpec.Bucket != "" && !outputBucketMap[tableSpec.Bucket] {
				outputBuckets = append(outputBuckets, tableSpec.Bucket)
				outputBucketMap[tableSpec.Bucket] = true
			}
		}
	}

	if len(inputBuckets) > 0 {
		configMap["input_buckets"] = inputBuckets
	}
	if len(outputBuckets) > 0 {
		configMap["output_buckets"] = outputBuckets
	}

	// Build input_tables array
	if comp.Inputs != nil && len(comp.Inputs.Tables) > 0 {
		inputTables := make([]map[string]any, 0, len(comp.Inputs.Tables))
		for _, tableSpec := range comp.Inputs.Tables {
			if tableSpec.Bucket != "" && tableSpec.Table != "" {
				entry := map[string]any{
					"bucket": tableSpec.Bucket,
					"table":  tableSpec.Table,
				}
				if tableSpec.LogicalName != "" {
					entry["as"] = tableSpec.LogicalName
				}
				// Resolve incremental read config (mutually exclusive: SinceDays > Since > SinceSnapshot)
				switch {
				case tableSpec.SinceDays != nil && *tableSpec.SinceDays > 0:
					cutoff := time.Now().UTC().Add(-time.Duration(*tableSpec.SinceDays) * 24 * time.Hour)
					entry["since_timestamp_ms"] = cutoff.UnixMilli()
					if tableSpec.TimestampColumn != "" {
						entry["timestamp_column"] = tableSpec.TimestampColumn
					}
				case tableSpec.Since != "":
					d, err := pipelineconfig.ParseSinceDuration(tableSpec.Since)
					if err != nil {
						fmt.Printf("WARNING: component %s, input table %s.%s: invalid since %q: %v (ignoring incremental config)\n",
							comp.Name, tableSpec.Bucket, tableSpec.Table, tableSpec.Since, err)
					} else {
						entry["since_timestamp_ms"] = time.Now().Add(-d).UnixMilli()
						if tableSpec.TimestampColumn != "" {
							entry["timestamp_column"] = tableSpec.TimestampColumn
						}
					}
				case tableSpec.SinceSnapshot != nil:
					entry["since_snapshot"] = *tableSpec.SinceSnapshot
				}
				inputTables = append(inputTables, entry)
			}
		}
		if len(inputTables) > 0 {
			configMap["input_tables"] = inputTables
		}
	}

	// Build output_tables array
	if comp.Outputs != nil && len(comp.Outputs.Tables) > 0 {
		outputTables := make([]map[string]any, 0, len(comp.Outputs.Tables))
		for _, tableSpec := range comp.Outputs.Tables {
			if tableSpec.Bucket != "" && tableSpec.Name != "" {
				writeMode := tableSpec.WriteMode
				if writeMode == "" {
					writeMode = "APPEND"
				}
				entry := map[string]any{
					"name":       tableSpec.Name,
					"bucket":     tableSpec.Bucket,
					"write_mode": writeMode,
				}
				if tableSpec.LogicalName != "" {
					entry["logical_name"] = tableSpec.LogicalName
				}
				if len(tableSpec.PartitionFields) > 0 {
					pfs := make([]map[string]string, len(tableSpec.PartitionFields))
					for i, pf := range tableSpec.PartitionFields {
						pfs[i] = map[string]string{
							"source_column": pf.SourceColumn,
							"transform":     pf.Transform,
						}
					}
					entry["partition_fields"] = pfs
				}
				outputTables = append(outputTables, entry)
			}
		}
		if len(outputTables) > 0 {
			configMap["output_tables"] = outputTables
		}
	}

	// Add default bucket if specified
	if comp.Outputs != nil && comp.Outputs.DefaultBucket != "" {
		configMap["default_bucket"] = comp.Outputs.DefaultBucket
		writeMode := comp.Outputs.DefaultWriteMode
		if writeMode == "" {
			writeMode = "FULL_LOAD" // Default for DefaultBucket mode
		}
		configMap["default_write_mode"] = writeMode
	}

	// Add gateway settings
	configMap["gateway"] = map[string]int64{
		"chunk_size":       chunkSize,
		"buffer_size":      bufferSize,
		"row_group_size":   rowGroupSize,
		"target_file_size": targetFileSize,
	}

	// Long-lived S3 credentials (s3_endpoint, s3_access_key,
	// s3_secret_key, s3_bucket_name) are not injected into the DG
	// sidecar config. The gateway uses lakekeeper-vended STS credentials
	// for all storage access.

	// Component config section
	componentMap := make(map[string]any)

	// Component config
	if len(config) > 0 {
		componentMap["config"] = config
	}

	// Build component.outputs with processors (matches Docker orchestrator format)
	if comp.Outputs != nil && len(comp.Outputs.Processors) > 0 {
		processors := make([]map[string]any, len(comp.Outputs.Processors))
		for i, p := range comp.Outputs.Processors {
			proc := map[string]any{"type": p.Type}
			if len(p.Columns) > 0 {
				proc["columns"] = p.Columns
			}
			processors[i] = proc
		}
		outputsMap := map[string]any{"processors": processors}
		componentMap["outputs"] = outputsMap
	}

	if len(componentMap) > 0 {
		configMap["component"] = componentMap
	}

	// The gateway sidecar talks directly to lakekeeper. Warehouse +
	// project_id come from the validated JWT claims; the operator does
	// not inject them into the configMap.
	if r.LakekeeperURL != "" {
		configMap["lakekeeper_url"] = r.LakekeeperURL
		if r.PipelineAPIURL != "" {
			configMap["pipeline_api_jwks_url"] = strings.TrimSuffix(r.PipelineAPIURL, "/") + "/api/v1/auth/jwks.json"
		}
	}

	// Tell the gateway where the mounted Secret files live (Task 10 wires the
	// corresponding volume + mount on the gateway sidecar). Only set when the
	// pipeline actually references a Secret.
	if pipeline.Spec.SecretsRef != nil {
		configMap["secrets_dir"] = secretsMountPath
	}

	// Marshal to YAML
	yamlBytes, err := yaml.Marshal(configMap)
	if err != nil {
		return fmt.Sprintf(`mode: minio
run_id: %s
lakekeeper_url: %s
gateway:
  chunk_size: %d
  buffer_size: %d
  row_group_size: %d
  target_file_size: %d
`,
			pr.Status.RunID,
			r.LakekeeperURL,
			chunkSize,
			bufferSize,
			rowGroupSize,
			targetFileSize)
	}

	return string(yamlBytes)
}

// buildGatewaySidecarEnv returns the env-var slice attached to every
// gateway sidecar this reconciler spawns. Centralised so per-iteration
// (DATUPLET_ITERATION_ID) and instrumentation (DATUPLET_GATEWAY_DEBUG,
// DATUPLET_GATEWAY_PROFILING + Pyroscope auth) plumbing live in one
// place rather than scattered through the Pod-spec builder.
func (r *PipelineRunReconciler) buildGatewaySidecarEnv(pr *datupletv1.PipelineRun) []corev1.EnvVar {
	// Gateway environment. No long-lived S3 credentials — per-write
	// vended STS creds come from lakekeeper.
	// DG validates JWT.run_id == os.Getenv("RUN_ID") as a Secret-swap
	// defence, so RUN_ID must be projected onto the sidecar.
	env := []corev1.EnvVar{
		{Name: "RUN_ID", Value: pr.Status.RunID},
	}

	// Optional debug logging on the gateway sidecar. Set via the operator's
	// own env (DATUPLET_GATEWAY_DEBUG -> reconciler field -> per-PipelineRun
	// sidecar env). Off by default; flip via helm value.
	if r.GatewayDebug {
		env = append(env, corev1.EnvVar{
			Name:  "DATUPLET_GATEWAY_DEBUG",
			Value: "true",
		})
	}

	// Pyroscope profiling. Requires both flag + address; auth is
	// optional. See PipelineRunReconciler doc for why creds are plain.
	if r.GatewayProfilingEnabled && r.GatewayProfilingServerAddress != "" {
		env = append(env,
			corev1.EnvVar{Name: "DATUPLET_GATEWAY_PROFILING", Value: "true"},
			corev1.EnvVar{Name: "PYROSCOPE_SERVER_ADDRESS", Value: r.GatewayProfilingServerAddress},
			// Downward API: surface pod namespace as a Pyroscope tag.
			corev1.EnvVar{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			}},
		)
		if r.GatewayProfilingUsername != "" {
			env = append(env,
				corev1.EnvVar{Name: "PYROSCOPE_USERNAME", Value: r.GatewayProfilingUsername},
			)
		}
		if r.GatewayProfilingPassword != "" {
			env = append(env,
				corev1.EnvVar{Name: "PYROSCOPE_PASSWORD", Value: r.GatewayProfilingPassword},
			)
		}
	}

	return env
}

func int32Ptr(i int32) *int32 {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}
