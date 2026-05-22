package controllers

import (
	"context"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	corev1 "k8s.io/api/core/v1"
)

// minimalPipeline returns a Pipeline with one stage and one component,
// no SecretsRef, no inputs, no outputs — just enough for buildComponentJob.
func minimalPipeline() *datupletv1.Pipeline {
	return &datupletv1.Pipeline{
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:  "c1",
					Image: "datuplet/test:latest",
				}},
			}},
		},
	}
}

// minimalPipelineRun returns a PipelineRun with name, namespace, and RunID set.
func minimalPipelineRun() *datupletv1.PipelineRun {
	pr := &datupletv1.PipelineRun{}
	pr.Name = "pr1"
	pr.Namespace = "default"
	pr.Status.RunID = "run-1"
	return pr
}

func TestBuildComponentJob_AutomountDisabled_NoSecretsRef(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}

	am := job.Spec.Template.Spec.AutomountServiceAccountToken
	if am == nil {
		t.Fatal("AutomountServiceAccountToken is nil; required to be false unconditionally")
	}
	if *am != false {
		t.Errorf("AutomountServiceAccountToken = %v, want false", *am)
	}
}

func TestBuildComponentJob_AutomountDisabled_WithSecretsRef(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pipeline.Spec.SecretsRef = &datupletv1.SecretsRef{Name: "mysecrets"}
	pr := minimalPipelineRun()

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}

	am := job.Spec.Template.Spec.AutomountServiceAccountToken
	if am == nil || *am != false {
		t.Errorf("AutomountServiceAccountToken must be false even with SecretsRef set; got %v", am)
	}
}

func TestBuildComponentJob_ComponentContainerHardened(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}

	containers := job.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("got %d app containers, want 1", len(containers))
	}
	sc := containers[0].SecurityContext
	if sc == nil {
		t.Fatal("component SecurityContext is nil; hardening is required on every component container")
	}

	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation != false {
		t.Errorf("AllowPrivilegeEscalation = %v, want explicit false", sc.AllowPrivilegeEscalation)
	}

	if sc.Capabilities == nil {
		t.Fatal("Capabilities is nil")
	}
	if len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != corev1.Capability("ALL") {
		t.Errorf("Capabilities.Drop = %v, want [ALL]", sc.Capabilities.Drop)
	}
	if len(sc.Capabilities.Add) != 0 {
		t.Errorf("Capabilities.Add = %v, want empty", sc.Capabilities.Add)
	}

	if sc.SeccompProfile == nil {
		t.Fatal("SeccompProfile is nil")
	}
	if sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("SeccompProfile.Type = %v, want RuntimeDefault", sc.SeccompProfile.Type)
	}
}

func TestBuildComponentJob_RunTokenMounted_OnGatewayOnly(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()
	pr.Spec.RunTokenRef = &datupletv1.RunTokenRef{Name: "runtoken-pr1"}

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}

	// Volume with the right Secret name, items, and mode must be present.
	var vol *corev1.Volume
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Name == "datuplet-runtoken" {
			vol = &job.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	if vol == nil {
		t.Fatal("datuplet-runtoken volume not found on Pod")
	}
	if vol.Secret == nil {
		t.Fatal("datuplet-runtoken volume is not a Secret source")
	}
	if vol.Secret.SecretName != "runtoken-pr1" {
		t.Errorf("SecretName = %q, want runtoken-pr1", vol.Secret.SecretName)
	}
	if vol.Secret.Optional == nil || *vol.Secret.Optional != false {
		t.Errorf("Optional = %v, want explicit false (FailedMount if absent)", vol.Secret.Optional)
	}
	if vol.Secret.DefaultMode == nil || *vol.Secret.DefaultMode != 0o440 {
		t.Errorf("DefaultMode = %v, want 0o440", vol.Secret.DefaultMode)
	}
	if len(vol.Secret.Items) != 1 || vol.Secret.Items[0].Key != "token" || vol.Secret.Items[0].Path != "token" {
		t.Errorf("Items = %v, want [{Key: token, Path: token}] (single per-run JWT)", vol.Secret.Items)
	}

	// Gateway sidecar must have the mount at the documented path.
	gw := &job.Spec.Template.Spec.InitContainers[0]
	if gw.Name != "gateway" {
		t.Fatalf("first init container is %q, want gateway", gw.Name)
	}
	var gwMount *corev1.VolumeMount
	for i := range gw.VolumeMounts {
		if gw.VolumeMounts[i].Name == "datuplet-runtoken" {
			gwMount = &gw.VolumeMounts[i]
			break
		}
	}
	if gwMount == nil {
		t.Fatal("gateway sidecar missing datuplet-runtoken mount")
	}
	if gwMount.MountPath != "/var/run/secrets/datuplet-runtoken" {
		t.Errorf("MountPath = %q, want /var/run/secrets/datuplet-runtoken", gwMount.MountPath)
	}
	if !gwMount.ReadOnly {
		t.Error("MountPath must be ReadOnly")
	}

	// Component container must NOT have the mount.
	comp := &job.Spec.Template.Spec.Containers[0]
	for _, m := range comp.VolumeMounts {
		if m.Name == "datuplet-runtoken" {
			t.Fatal("component container must not have the run-token mount")
		}
	}

	// Pod fsGroup must be set so the gateway (runAsGroup matching fsGroup)
	// can read the 0440-mode files.
	if job.Spec.Template.Spec.SecurityContext == nil || job.Spec.Template.Spec.SecurityContext.FSGroup == nil {
		t.Fatal("Pod SecurityContext.FSGroup is nil; gateway cannot read mount at mode 0440")
	}
	if *job.Spec.Template.Spec.SecurityContext.FSGroup != 65532 {
		t.Errorf("FSGroup = %d, want 65532", *job.Spec.Template.Spec.SecurityContext.FSGroup)
	}

	// Gateway sidecar runAsNonRoot / runAsUser / runAsGroup must be set.
	if gw.SecurityContext == nil {
		t.Fatal("gateway SecurityContext is nil")
	}
	if gw.SecurityContext.RunAsNonRoot == nil || *gw.SecurityContext.RunAsNonRoot != true {
		t.Errorf("gateway RunAsNonRoot = %v, want true", gw.SecurityContext.RunAsNonRoot)
	}
	if gw.SecurityContext.RunAsUser == nil || *gw.SecurityContext.RunAsUser != 65532 {
		t.Errorf("gateway RunAsUser = %v, want 65532", gw.SecurityContext.RunAsUser)
	}
	if gw.SecurityContext.RunAsGroup == nil || *gw.SecurityContext.RunAsGroup != 65532 {
		t.Errorf("gateway RunAsGroup = %v, want 65532", gw.SecurityContext.RunAsGroup)
	}
}

func TestBuildComponentJob_RunTokenNil_NoVolume(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()
	// RunTokenRef deliberately nil

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "datuplet-runtoken" {
			t.Error("datuplet-runtoken volume must not be added when RunTokenRef is nil")
		}
	}
}

func TestBuildComponentJob_RunTokenAndSecretsRef_BothCoexist(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pipeline.Spec.SecretsRef = &datupletv1.SecretsRef{Name: "my-secrets"}
	pr := minimalPipelineRun()
	pr.Spec.RunTokenRef = &datupletv1.RunTokenRef{Name: "runtoken-pr1"}

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}

	volumeNames := map[string]bool{}
	for _, v := range job.Spec.Template.Spec.Volumes {
		volumeNames[v.Name] = true
	}
	if !volumeNames["datuplet-secrets"] {
		t.Error("SecretsRef volume 'datuplet-secrets' missing when both refs are set")
	}
	if !volumeNames["datuplet-runtoken"] {
		t.Error("RunTokenRef volume 'datuplet-runtoken' missing when both refs are set")
	}

	gw := &job.Spec.Template.Spec.InitContainers[0]
	gwMounts := map[string]bool{}
	for _, m := range gw.VolumeMounts {
		gwMounts[m.Name] = true
	}
	if !gwMounts["datuplet-secrets"] || !gwMounts["datuplet-runtoken"] {
		t.Errorf("gateway missing mounts: %v", gwMounts)
	}
}

// TestComponentJobName_IncludesRunIDSuffix pins the Job/CM name scheme
// so two PipelineRuns of pipelines whose names share the truncated
// 15-char prefix still get distinct Jobs. Codex caught this on
// feat/002-slice-e-multi-tenant-tg: the controller's Create returns
// AlreadyExists silently on a collision and the second run reuses the
// first run's stale ConfigMap.
func TestComponentJobName_IncludesRunIDSuffix(t *testing.T) {
	prA := minimalPipelineRun()
	prA.Name = "kbc-services-pipeline"
	prA.Status.RunID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	prB := minimalPipelineRun()
	prB.Name = "kbc-services-pipeline2" // same 15-char prefix as prA
	prB.Status.RunID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

	nameA := componentJobName(prA, "extract", "c1")
	nameB := componentJobName(prB, "extract", "c1")
	if nameA == nameB {
		t.Fatalf("expected distinct Job names for two runs; both = %q", nameA)
	}
	if !strings.HasSuffix(nameA, "aaaaaaaa") || !strings.HasSuffix(nameB, "bbbbbbbb") {
		t.Errorf("run-id suffix missing: nameA=%q nameB=%q", nameA, nameB)
	}
}

// TestGenerateGatewayConfig_LakekeeperURL: when the reconciler carries
// LakekeeperURL, the gateway config YAML embeds it so the sidecar can
// reach the catalog directly.
func TestGenerateGatewayConfig_LakekeeperURL(t *testing.T) {
	r := &PipelineRunReconciler{LakekeeperURL: "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog"}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()
	pr.Namespace = "datuplet-proj-a"

	y := r.generateGatewayConfig(pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if !strings.Contains(y, "lakekeeper_url: http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog") {
		t.Errorf("generated YAML did not carry lakekeeper URL; got:\n%s", y)
	}
}

// TestGenerateGatewayConfig_LakekeeperURL_OmittedWhenUnset: a reconciler
// without LakekeeperURL emits a config without the field. Useful for
// transitional / test deploys.
func TestGenerateGatewayConfig_LakekeeperURL_OmittedWhenUnset(t *testing.T) {
	r := &PipelineRunReconciler{} // zero value
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()
	pr.Namespace = "datuplet-proj-a"

	y := r.generateGatewayConfig(pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if strings.Contains(y, "lakekeeper_url") {
		t.Errorf("generated YAML should omit lakekeeper_url when unset; got:\n%s", y)
	}
}

// TestGenerateGatewayConfig_PipelineAPIJWKSURL: when both LakekeeperURL
// and PipelineAPIURL are set, the operator injects pipeline_api_jwks_url
// into the DG sidecar configMap. The path is trimmed of any trailing slash
// on PipelineAPIURL and ends in "/api/v1/auth/jwks.json".
func TestGenerateGatewayConfig_PipelineAPIJWKSURL(t *testing.T) {
	r := &PipelineRunReconciler{
		LakekeeperURL:  "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog",
		PipelineAPIURL: "http://pipeline-api.datuplet.svc.cluster.local:8081",
	}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()
	pr.Namespace = "datuplet-proj-a"

	y := r.generateGatewayConfig(pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	want := "pipeline_api_jwks_url: http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json"
	if !strings.Contains(y, want) {
		t.Errorf("generated YAML should contain %q; got:\n%s", want, y)
	}
}

// TestGenerateGatewayConfig_PipelineAPIJWKSURL_TrimsTrailingSlash:
// trailing slash on the operator's PipelineAPIURL must not produce a
// double-slash in the JWKS URL.
func TestGenerateGatewayConfig_PipelineAPIJWKSURL_TrimsTrailingSlash(t *testing.T) {
	r := &PipelineRunReconciler{
		LakekeeperURL:  "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog",
		PipelineAPIURL: "http://pipeline-api.datuplet.svc.cluster.local:8081/",
	}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()
	pr.Namespace = "datuplet-proj-a"

	y := r.generateGatewayConfig(pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if strings.Contains(y, "8081//api") {
		t.Errorf("trailing-slash hygiene broken — double slash in JWKS URL; got:\n%s", y)
	}
	want := "pipeline_api_jwks_url: http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json"
	if !strings.Contains(y, want) {
		t.Errorf("generated YAML should contain %q; got:\n%s", want, y)
	}
}

// TestBuildComponentJobHasRuntimeTolerations verifies that RuntimeTolerations
// set on the reconciler are propagated to the component Job's PodSpec.
func TestBuildComponentJobHasRuntimeTolerations(t *testing.T) {
	r := &PipelineRunReconciler{
		RuntimeTolerations: []corev1.Toleration{{Key: "k", Operator: "Equal", Value: "v", Effect: "NoSchedule"}},
	}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}
	if len(job.Spec.Template.Spec.Tolerations) != 1 {
		t.Fatalf("Tolerations len = %d, want 1", len(job.Spec.Template.Spec.Tolerations))
	}
	if job.Spec.Template.Spec.Tolerations[0].Key != "k" {
		t.Fatalf("Tolerations[0].Key = %q, want %q", job.Spec.Template.Spec.Tolerations[0].Key, "k")
	}
}

// TestBuildComponentJobNilTolerationsOmitsField verifies that a reconciler
// with no RuntimeTolerations leaves spec.tolerations nil (not an empty slice).
func TestBuildComponentJobNilTolerationsOmitsField(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}
	if job.Spec.Template.Spec.Tolerations != nil {
		t.Fatalf("Tolerations = %v, want nil", job.Spec.Template.Spec.Tolerations)
	}
}

// TestGenerateGatewayConfig_PipelineAPIJWKSURL_OmittedWhenUnset: a
// reconciler without PipelineAPIURL emits a config without the field
// (dev paths without JWT validation).
func TestGenerateGatewayConfig_PipelineAPIJWKSURL_OmittedWhenUnset(t *testing.T) {
	r := &PipelineRunReconciler{LakekeeperURL: "http://lakekeeper:8181/catalog"} // no PipelineAPIURL
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()
	pr.Namespace = "datuplet-proj-a"

	y := r.generateGatewayConfig(pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if strings.Contains(y, "pipeline_api_jwks_url") {
		t.Errorf("generated YAML should omit pipeline_api_jwks_url when PipelineAPIURL is unset; got:\n%s", y)
	}
}

func TestBuildGatewaySidecarEnvIncludesIterationIDWhenImageMatchesIterForm(t *testing.T) {
	r := &PipelineRunReconciler{
		GatewayImage: "ttl.sh/datuplet-gateway-iter-abc1234:24h",
	}
	env := r.buildGatewaySidecarEnv(minimalPipelineRun())
	got := envVarValue(env, "DATUPLET_ITERATION_ID")
	if got != "abc1234" {
		t.Errorf("DATUPLET_ITERATION_ID = %q, want %q", got, "abc1234")
	}
}

func TestBuildGatewaySidecarEnvOmitsIterationIDWhenImageHasNoIterTag(t *testing.T) {
	r := &PipelineRunReconciler{
		GatewayImage: "datuplet/gateway:latest",
	}
	env := r.buildGatewaySidecarEnv(minimalPipelineRun())
	if got := envVarValue(env, "DATUPLET_ITERATION_ID"); got != "" {
		t.Errorf("expected DATUPLET_ITERATION_ID absent, got %q", got)
	}
}

// TestComponentJobUsesPullAlwaysOnAllContainers: RFC 020 iteration loop
// requires that every re-run of a build cycle picks up the freshly-pushed
// image rather than a kubelet-cached one. PullAlways must be set on:
//   - the gateway sidecar (init container)
//   - the component container
func TestComponentJobUsesPullAlwaysOnAllContainers(t *testing.T) {
	r := &PipelineRunReconciler{
		GatewayImage: "ttl.sh/datuplet-gateway-iter-abc1234:24h",
	}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	job, _, err := r.buildComponentJob(context.Background(), pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}

	// Gateway sidecar (first init container).
	initContainers := job.Spec.Template.Spec.InitContainers
	if len(initContainers) == 0 {
		t.Fatal("no init containers on component Job")
	}
	gw := initContainers[0]
	if gw.Name != "gateway" {
		t.Fatalf("first init container is %q, want gateway", gw.Name)
	}
	if gw.ImagePullPolicy != corev1.PullAlways {
		t.Errorf("gateway sidecar ImagePullPolicy = %q, want PullAlways", gw.ImagePullPolicy)
	}

	// Component container.
	containers := job.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		t.Fatal("no containers on component Job")
	}
	comp := containers[0]
	if comp.ImagePullPolicy != corev1.PullAlways {
		t.Errorf("component container ImagePullPolicy = %q, want PullAlways", comp.ImagePullPolicy)
	}
}

func envVarValue(envs []corev1.EnvVar, name string) string {
	for _, e := range envs {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
