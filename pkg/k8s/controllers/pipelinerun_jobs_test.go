package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/datagateway"
	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// minimalPipeline returns a Pipeline with one stage and one component,
// no inputs, no outputs — just enough for buildComponentJob.
func minimalPipeline() *datupletv1.Pipeline {
	return &datupletv1.Pipeline{
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
}

// minimalPipelineRun returns an admitted PipelineRun: RunID set, and the
// minimalPipeline frozen into Status.ResolvedSpec + Status.Components (as
// handlePending would after resolve-&-freeze). buildComponentJob reads the
// resolved image/version from the snapshot, so unit tests must supply it.
func minimalPipelineRun() *datupletv1.PipelineRun {
	pr := &datupletv1.PipelineRun{}
	pr.Name = "pr1"
	pr.Namespace = "default"
	pr.Status.RunID = "run-1"
	pr.Status.ResolvedSpec = minimalPipeline().Spec.DeepCopy()
	pr.Status.Components = []datupletv1.ResolvedComponentStatus{{
		Name:      "c1",
		Component: "comp-a",
		Version:   "v1.0.0",
		Image:     "datuplet/test:latest",
	}}
	return pr
}

func TestBuildComponentJob_AutomountDisabled_NoSecretsRef(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	job, _, err := r.buildComponentJob(context.Background(), pr, &pipeline.Spec.Stages[0].Components[0])
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

func TestBuildComponentJob_ComponentContainerHardened(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	job, _, err := r.buildComponentJob(context.Background(), pr, &pipeline.Spec.Stages[0].Components[0])
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

	job, _, err := r.buildComponentJob(context.Background(), pr, &pipeline.Spec.Stages[0].Components[0])
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

	job, _, err := r.buildComponentJob(context.Background(), pr, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "datuplet-runtoken" {
			t.Error("datuplet-runtoken volume must not be added when RunTokenRef is nil")
		}
	}
}

// mustJobName returns componentJobName(pr, comp), failing the test on error.
func mustJobName(t *testing.T, pr *datupletv1.PipelineRun, comp string) string {
	t.Helper()
	name, err := componentJobName(pr, comp)
	if err != nil {
		t.Fatalf("componentJobName(%q): %v", comp, err)
	}
	return name
}

// positionalRun returns a run whose resolved spec has stage 0 {a, b} and
// stage 1 {c}.
func positionalRun(runID string) *datupletv1.PipelineRun {
	pr := &datupletv1.PipelineRun{}
	pr.Name = "pr1"
	pr.Status.RunID = runID
	pr.Status.ResolvedSpec = &datupletv1.PipelineSpec{Stages: []datupletv1.StageSpec{
		{Name: "extract", Components: []datupletv1.ComponentSpec{{Name: "a"}, {Name: "b"}}},
		{Name: "load", Components: []datupletv1.ComponentSpec{{Name: "c"}}},
	}}
	return pr
}

func TestComponentJobName_PositionalAndStable(t *testing.T) {
	const runID = "11111111-2222-3333-4444-555555555555"
	pr := positionalRun(runID)
	for comp, want := range map[string]string{
		"a": "run-" + runID + "-s0-c0",
		"b": "run-" + runID + "-s0-c1",
		"c": "run-" + runID + "-s1-c0",
	} {
		got := mustJobName(t, pr, comp)
		if got != want {
			t.Errorf("componentJobName(%q) = %q, want %q", comp, got, want)
		}
		if errs := validation.IsDNS1123Label(got); len(errs) > 0 {
			t.Errorf("componentJobName(%q) = %q is not a DNS-1123 label: %v", comp, got, errs)
		}
		if again := mustJobName(t, pr, comp); again != got {
			t.Errorf("componentJobName(%q) not stable: %q then %q", comp, got, again)
		}
	}
}

func TestComponentJobName_FullRunIDNoPrefixCollision(t *testing.T) {
	a := mustJobName(t, positionalRun("aaaaaaaa-0000-0000-0000-000000000001"), "a")
	b := mustJobName(t, positionalRun("aaaaaaaa-0000-0000-0000-000000000002"), "a")
	if a == b {
		t.Fatalf("runs sharing an 8-char prefix got the same Job name %q", a)
	}
}

func TestComponentJobName_UnknownComponent(t *testing.T) {
	pr := positionalRun("11111111-2222-3333-4444-555555555555")
	if name, err := componentJobName(pr, "nope"); err == nil {
		t.Errorf("unknown component: got %q, want error", name)
	}
	pr.Status.ResolvedSpec = nil
	if name, err := componentJobName(pr, "a"); err == nil {
		t.Errorf("nil ResolvedSpec: got %q, want error", name)
	}
}

func TestComponentJobName_NoUserText(t *testing.T) {
	pr := &datupletv1.PipelineRun{}
	pr.Name = "my-secret-pipeline"
	pr.Status.RunID = "11111111-2222-3333-4444-555555555555"
	pr.Status.ResolvedSpec = &datupletv1.PipelineSpec{Stages: []datupletv1.StageSpec{
		{Name: "ingest", Components: []datupletv1.ComponentSpec{{Name: "extract-orders"}}},
	}}
	got := mustJobName(t, pr, "extract-orders")
	for _, s := range []string{"my-secret-pipeline", "ingest", "extract-orders"} {
		if strings.Contains(got, s) {
			t.Errorf("Job name %q contains user text %q", got, s)
		}
	}
}

func TestRunSecretsName_FullRunID(t *testing.T) {
	pr := positionalRun("11111111-2222-3333-4444-555555555555")
	if got, want := runSecretsName(pr), "datuplet-runsecrets-"+pr.Status.RunID; got != want {
		t.Errorf("runSecretsName = %q, want %q", got, want)
	}
}

// TestGenerateGatewayConfig_NestedConfig verifies that a component's
// structured Config (a nested tables list) survives into gateway.yaml as
// native YAML nesting — not as an escaped JSON string embedded in the
// output (RFC 026 P1, B6).
func TestGenerateGatewayConfig_NestedConfig(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	comp := &datupletv1.ComponentSpec{
		Name:      "c1",
		Component: "comp-a",
		Config: apiextensionsv1.JSON{
			Raw: []byte(`{"tables":[{"name":"facts","random":{"schema":{"id":"int"}}}]}`),
		},
	}

	y, err := r.generateGatewayConfig(pr, pipeline, comp)
	if err != nil {
		t.Fatalf("generateGatewayConfig: %v", err)
	}
	if strings.Contains(y, `"tables"`) {
		t.Errorf("config appears to be escaped/embedded JSON, not native YAML nesting; got:\n%s", y)
	}

	// Decode the generated YAML and walk the structure to prove "tables"
	// is a real nested sequence under component.config — not merely a
	// substring that happens to appear somewhere in the document (e.g.
	// flat, or under an unrelated key).
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(y), &doc); err != nil {
		t.Fatalf("generated YAML did not parse: %v\ngot:\n%s", err, y)
	}

	component, ok := doc["component"].(map[string]any)
	if !ok {
		t.Fatalf("doc[\"component\"] is not a map[string]any; got %T (%v)\ndoc:\n%s", doc["component"], doc["component"], y)
	}
	configSection, ok := component["config"].(map[string]any)
	if !ok {
		t.Fatalf("component[\"config\"] is not a map[string]any; got %T (%v)\ndoc:\n%s", component["config"], component["config"], y)
	}
	tables, ok := configSection["tables"].([]any)
	if !ok {
		t.Fatalf("component.config[\"tables\"] is not a []any (not a nested sequence); got %T (%v)\ndoc:\n%s", configSection["tables"], configSection["tables"], y)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 entry under component.config.tables, got %d\ndoc:\n%s", len(tables), y)
	}
	firstTable, ok := tables[0].(map[string]any)
	if !ok {
		t.Fatalf("component.config.tables[0] is not a map[string]any; got %T (%v)\ndoc:\n%s", tables[0], tables[0], y)
	}
	if name, _ := firstTable["name"].(string); name != "facts" {
		t.Errorf("expected component.config.tables[0].name == \"facts\", got %v\ndoc:\n%s", firstTable["name"], y)
	}
}

// TestGenerateGatewayConfig_OutputTableColumns proves the operator carries
// each output table's explicit column mapping (name+type) into the gateway
// sidecar's output_tables[].columns entries, and — just as importantly —
// omits the "columns" key entirely for a table that declares no mapping
// (the absence is the producer's signal to infer, so a present-but-empty
// key would be a different, wrong thing to emit). The sibling
// partition_fields hop is exercised by the same fixture/assertions since
// it is a trivial addition to the same table-driven shape.
func TestGenerateGatewayConfig_OutputTableColumns(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()
	pr := minimalPipelineRun()

	comp := &datupletv1.ComponentSpec{
		Name:      "c1",
		Component: "comp-a",
		Outputs: &datupletv1.OutputSpec{
			Tables: []datupletv1.OutputTableSpec{
				{
					Name: "with_columns", Bucket: "curated", WriteMode: "APPEND",
					Columns: []datupletv1.ColumnSpec{
						{Name: "id", Type: "int"},
						{Name: "note", Type: "string"},
					},
				},
				{
					Name: "no_columns", Bucket: "curated", WriteMode: "APPEND",
				},
				{
					Name: "with_partition", Bucket: "curated", WriteMode: "APPEND",
					PartitionFields: []datupletv1.PartitionFieldSpec{
						{SourceColumn: "created", Transform: "day"},
						{SourceColumn: "id", Transform: "bucket[16]"},
						{SourceColumn: "id", Transform: "truncate[4]"},
						{SourceColumn: "note", Transform: "void"},
					},
				},
			},
		},
	}

	y, err := r.generateGatewayConfig(pr, pipeline, comp)
	if err != nil {
		t.Fatalf("generateGatewayConfig: %v", err)
	}

	var doc map[string]any
	if err := yaml.Unmarshal([]byte(y), &doc); err != nil {
		t.Fatalf("generated YAML did not parse: %v\ngot:\n%s", err, y)
	}
	rawTables, ok := doc["output_tables"].([]any)
	if !ok || len(rawTables) != 3 {
		t.Fatalf("output_tables missing or wrong length; got %T (%v)\ndoc:\n%s", doc["output_tables"], doc["output_tables"], y)
	}
	byName := make(map[string]map[string]any, len(rawTables))
	for _, rt := range rawTables {
		tbl, ok := rt.(map[string]any)
		if !ok {
			t.Fatalf("output_tables entry is not a map[string]any; got %T\ndoc:\n%s", rt, y)
		}
		name, _ := tbl["name"].(string)
		byName[name] = tbl
	}

	// WITH columns: name+type carried through, in declared order.
	withCols, ok := byName["with_columns"]
	if !ok {
		t.Fatalf("output_tables missing entry %q\ndoc:\n%s", "with_columns", y)
	}
	cols, ok := withCols["columns"].([]any)
	if !ok || len(cols) != 2 {
		t.Fatalf("with_columns.columns missing or wrong length; got %T (%v)\ndoc:\n%s", withCols["columns"], withCols["columns"], y)
	}
	if first, _ := cols[0].(map[string]any); first["name"] != "id" || first["type"] != "int" {
		t.Errorf("columns[0] = %v, want {name: id, type: int}\ndoc:\n%s", first, y)
	}
	if second, _ := cols[1].(map[string]any); second["name"] != "note" || second["type"] != "string" {
		t.Errorf("columns[1] = %v, want {name: note, type: string}\ndoc:\n%s", second, y)
	}

	// WITHOUT columns: the "columns" key must be absent entirely (not
	// present-but-empty) so the gateway/SDK sees "no explicit mapping".
	noCols, ok := byName["no_columns"]
	if !ok {
		t.Fatalf("output_tables missing entry %q\ndoc:\n%s", "no_columns", y)
	}
	if v, present := noCols["columns"]; present {
		t.Errorf("no_columns table should omit the \"columns\" key entirely; got %v\ndoc:\n%s", v, y)
	}

	// Sibling hop, included because it's trivially covered by the same
	// fixture/assertion shape: partition_fields carried through the same way.
	withPart, ok := byName["with_partition"]
	if !ok {
		t.Fatalf("output_tables missing entry %q\ndoc:\n%s", "with_partition", y)
	}
	pfs, ok := withPart["partition_fields"].([]any)
	if !ok || len(pfs) != 4 {
		t.Fatalf("with_partition.partition_fields missing or wrong length; got %T (%v)\ndoc:\n%s", withPart["partition_fields"], withPart["partition_fields"], y)
	}
	if pf, _ := pfs[0].(map[string]any); pf["source_column"] != "created" || pf["transform"] != "day" {
		t.Errorf("partition_fields[0] = %v, want {source_column: created, transform: day}\ndoc:\n%s", pf, y)
	}
	// bucket/truncate/void reach the gateway verbatim, in order.
	for i, want := range [][2]string{{"id", "bucket[16]"}, {"id", "truncate[4]"}, {"note", "void"}} {
		if pf, _ := pfs[i+1].(map[string]any); pf["source_column"] != want[0] || pf["transform"] != want[1] {
			t.Errorf("partition_fields[%d] = %v, want {source_column: %s, transform: %s}", i+1, pf, want[0], want[1])
		}
	}
}

// TestGenerateGatewayConfig_URLWiring: LakekeeperURL and PipelineAPIURL on the
// reconciler are embedded into the gateway config YAML (the sidecar reaches the
// catalog directly and validates the run token against the JWKS URL); an unset
// field is omitted, and a trailing slash on PipelineAPIURL must not produce a
// double slash in the JWKS URL.
func TestGenerateGatewayConfig_URLWiring(t *testing.T) {
	const (
		lakekeeperURL = "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog"
		jwksWant      = "pipeline_api_jwks_url: http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json"
	)
	cases := []struct {
		name   string
		r      *PipelineRunReconciler
		want   string
		forbid string
	}{
		{
			name: "lakekeeper url embedded",
			r:    &PipelineRunReconciler{LakekeeperURL: lakekeeperURL},
			want: "lakekeeper_url: " + lakekeeperURL,
		},
		{
			name:   "lakekeeper url omitted when unset",
			r:      &PipelineRunReconciler{},
			forbid: "lakekeeper_url",
		},
		{
			name: "jwks url embedded",
			r: &PipelineRunReconciler{
				LakekeeperURL:  lakekeeperURL,
				PipelineAPIURL: "http://pipeline-api.datuplet.svc.cluster.local:8081",
			},
			want: jwksWant,
		},
		{
			name: "jwks url trims trailing slash",
			r: &PipelineRunReconciler{
				LakekeeperURL:  lakekeeperURL,
				PipelineAPIURL: "http://pipeline-api.datuplet.svc.cluster.local:8081/",
			},
			want:   jwksWant,
			forbid: "8081//api",
		},
		{
			name:   "jwks url omitted when pipeline api url unset",
			r:      &PipelineRunReconciler{LakekeeperURL: "http://lakekeeper:8181/catalog"},
			forbid: "pipeline_api_jwks_url",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := minimalPipeline()
			pr := minimalPipelineRun()
			pr.Namespace = "datuplet-proj-a"

			y, err := tc.r.generateGatewayConfig(pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
			if err != nil {
				t.Fatalf("generateGatewayConfig: %v", err)
			}
			if tc.want != "" && !strings.Contains(y, tc.want) {
				t.Errorf("generated YAML should contain %q; got:\n%s", tc.want, y)
			}
			if tc.forbid != "" && strings.Contains(y, tc.forbid) {
				t.Errorf("generated YAML should not contain %q; got:\n%s", tc.forbid, y)
			}
		})
	}
}

// TestBuildComponentJob_RuntimeTolerations: RuntimeTolerations set on the
// reconciler are propagated to the component Job's PodSpec; with none set,
// spec.tolerations stays nil (not an empty slice).
func TestBuildComponentJob_RuntimeTolerations(t *testing.T) {
	cases := []struct {
		name        string
		tolerations []corev1.Toleration
		wantKey     string // empty: Tolerations must be nil
	}{
		{
			name:        "propagated",
			tolerations: []corev1.Toleration{{Key: "k", Operator: "Equal", Value: "v", Effect: "NoSchedule"}},
			wantKey:     "k",
		},
		{name: "nil omits field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &PipelineRunReconciler{RuntimeTolerations: tc.tolerations}
			pipeline := minimalPipeline()
			pr := minimalPipelineRun()

			job, _, err := r.buildComponentJob(context.Background(), pr, &pipeline.Spec.Stages[0].Components[0])
			if err != nil {
				t.Fatalf("buildComponentJob: %v", err)
			}
			got := job.Spec.Template.Spec.Tolerations
			if tc.wantKey == "" {
				if got != nil {
					t.Fatalf("Tolerations = %v, want nil", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("Tolerations len = %d, want 1", len(got))
			}
			if got[0].Key != tc.wantKey {
				t.Fatalf("Tolerations[0].Key = %q, want %q", got[0].Key, tc.wantKey)
			}
		})
	}
}

// TestBuildGatewaySidecarEnv_IterationID proves the DATUPLET_ITERATION_ID env
// wiring; the tag parsing itself is pinned by TestIterTagFromImage.
func TestBuildGatewaySidecarEnv_IterationID(t *testing.T) {
	cases := []struct {
		name         string
		gatewayImage string
		want         string // empty: the env var must be absent
	}{
		{"set when image matches iter form", "ttl.sh/datuplet-gateway-iter-abc1234:24h", "abc1234"},
		{"omitted when image has no iter tag", "datuplet/gateway:latest", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &PipelineRunReconciler{GatewayImage: tc.gatewayImage}
			env := r.buildGatewaySidecarEnv(minimalPipelineRun())
			if got := envVarValue(env, "DATUPLET_ITERATION_ID"); got != tc.want {
				t.Errorf("DATUPLET_ITERATION_ID = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBuildComponentJob_RuntimePullPolicy pins the gateway sidecar and
// component container pull policies. The RFC 020 iteration loop needs the
// gateway to pick up a freshly pushed image (default PullAlways), and the
// component policy is registry-driven (prerelease -> Always; see
// TestFreeze_ComponentPullPolicy_RegistryDriven). The operator-wide
// DATUPLET_RUNTIME_PULL_POLICY override (e2e/kind sets IfNotPresent so K8s uses
// pre-loaded images) must win on BOTH containers: if it does not reach a
// "dev" prerelease component, K8s tries to pull a non-existent datuplet/* repo
// from Docker Hub -> ImagePullBackOff.
func TestBuildComponentJob_RuntimePullPolicy(t *testing.T) {
	cases := []struct {
		name          string
		gatewayImage  string
		runtimePolicy corev1.PullPolicy
		version       string
		wantGateway   corev1.PullPolicy
		wantComponent corev1.PullPolicy
	}{
		{
			name:          "default policy, prerelease component",
			gatewayImage:  "ttl.sh/datuplet-gateway-iter-abc1234:24h",
			version:       "dev",
			wantGateway:   corev1.PullAlways,
			wantComponent: corev1.PullAlways,
		},
		{
			name:          "override applies to gateway and stable component",
			gatewayImage:  "datuplet/gateway:latest",
			runtimePolicy: corev1.PullIfNotPresent,
			version:       "v1.0.0",
			wantGateway:   corev1.PullIfNotPresent,
			wantComponent: corev1.PullIfNotPresent,
		},
		{
			name:          "override wins over prerelease component Always (ImagePullBackOff incident)",
			gatewayImage:  "datuplet/gateway:latest",
			runtimePolicy: corev1.PullIfNotPresent,
			version:       "dev",
			wantGateway:   corev1.PullIfNotPresent,
			wantComponent: corev1.PullIfNotPresent,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &PipelineRunReconciler{GatewayImage: tc.gatewayImage, RuntimePullPolicy: tc.runtimePolicy}
			pipeline := minimalPipeline()
			pr := minimalPipelineRun()
			pr.Status.Components[0].Version = tc.version

			job, _, err := r.buildComponentJob(context.Background(), pr, &pipeline.Spec.Stages[0].Components[0])
			if err != nil {
				t.Fatalf("buildComponentJob: %v", err)
			}
			initContainers := job.Spec.Template.Spec.InitContainers
			if len(initContainers) == 0 {
				t.Fatal("no init containers on component Job")
			}
			gw := initContainers[0]
			if gw.Name != "gateway" {
				t.Fatalf("first init container is %q, want gateway", gw.Name)
			}
			if gw.ImagePullPolicy != tc.wantGateway {
				t.Errorf("gateway sidecar ImagePullPolicy = %q, want %q", gw.ImagePullPolicy, tc.wantGateway)
			}
			containers := job.Spec.Template.Spec.Containers
			if len(containers) == 0 {
				t.Fatal("no containers on component Job")
			}
			if got := containers[0].ImagePullPolicy; got != tc.wantComponent {
				t.Errorf("component container ImagePullPolicy = %q, want %q", got, tc.wantComponent)
			}
		})
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

func TestOperatorPassesVariantColumn(t *testing.T) {
	r := &PipelineRunReconciler{}
	comp := &datupletv1.ComponentSpec{
		Name:      "c1",
		Component: "comp-a",
		Outputs: &datupletv1.OutputSpec{Tables: []datupletv1.OutputTableSpec{{
			Name: "t", Bucket: "curated", WriteMode: "APPEND",
			Columns: []datupletv1.ColumnSpec{{Name: "id", Type: "long"}, {Name: "payload", Type: "variant"}},
		}}},
	}
	y, err := r.generateGatewayConfig(minimalPipelineRun(), minimalPipeline(), comp)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		OutputTables []struct {
			Columns []struct {
				Name string `yaml:"name"`
				Type string `yaml:"type"`
			} `yaml:"columns"`
		} `yaml:"output_tables"`
	}
	if err := yaml.Unmarshal([]byte(y), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.OutputTables) != 1 || len(doc.OutputTables[0].Columns) != 2 || doc.OutputTables[0].Columns[1].Name != "payload" || doc.OutputTables[0].Columns[1].Type != "variant" {
		t.Fatalf("variant column not passed through unchanged:\n%s", y)
	}
}

func TestBuildComponentJob_PreviewBackoffZero(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()

	pr := minimalPipelineRun()
	job, _, err := r.buildComponentJob(context.Background(), pr, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 1 {
		t.Errorf("non-preview BackoffLimit = %v, want 1", ptrVal(job.Spec.BackoffLimit))
	}

	pr = minimalPipelineRun()
	pr.Spec.Preview = &datupletv1.PreviewSpec{ID: testPreviewID, RowLimit: 1000, TimeoutSeconds: 300}
	job, _, err = r.buildComponentJob(context.Background(), pr, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("preview BackoffLimit = %v, want 0", ptrVal(job.Spec.BackoffLimit))
	}
}

func ptrVal(p *int32) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestGenerateGatewayConfig_PreviewBlock(t *testing.T) {
	r := &PipelineRunReconciler{}
	pipeline := minimalPipeline()

	pr := minimalPipelineRun()
	y, err := r.generateGatewayConfig(pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("generateGatewayConfig: %v", err)
	}
	if strings.Contains(y, "preview") {
		t.Errorf("non-preview config should not contain a preview block:\n%s", y)
	}

	pr.Spec.Preview = &datupletv1.PreviewSpec{ID: testPreviewID, RowLimit: 1000, TimeoutSeconds: 300}
	y, err = r.generateGatewayConfig(pr, pipeline, &pipeline.Spec.Stages[0].Components[0])
	if err != nil {
		t.Fatalf("generateGatewayConfig: %v", err)
	}
	for _, want := range []string{"preview:", "row_limit: 1000", "id: " + testPreviewID} {
		if !strings.Contains(y, want) {
			t.Errorf("preview config missing %q:\n%s", want, y)
		}
	}
	if strings.Contains(y, "timeout") {
		t.Errorf("timeout must not reach the gateway config:\n%s", y)
	}
}

// TestGatewayConfig_CarriesComponentInstanceName: the gateway YAML carries the
// component's instance name and the gateway loader keeps it (it keys
// per-component operations such as compaction).
func TestGatewayConfig_CarriesComponentInstanceName(t *testing.T) {
	r := &PipelineRunReconciler{}
	for _, name := range []string{"compact-a", "compact-b"} {
		comp := &datupletv1.ComponentSpec{Name: name, Component: "iceberg-compactor"}
		y, err := r.generateGatewayConfig(minimalPipelineRun(), minimalPipeline(), comp)
		if err != nil {
			t.Fatalf("generateGatewayConfig: %v", err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(y), &doc); err != nil {
			t.Fatalf("generated YAML did not parse: %v", err)
		}
		if doc["component_name"] != name {
			t.Errorf("component_name = %v, want %q\ndoc:\n%s", doc["component_name"], name, y)
		}
		p := filepath.Join(t.TempDir(), "gateway.yaml")
		if err := os.WriteFile(p, []byte(y), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := datagateway.LoadConfigFile(p, "", "")
		if err != nil {
			t.Fatalf("LoadConfigFile: %v", err)
		}
		if cfg.ComponentName != name {
			t.Errorf("loaded ComponentName = %q, want %q", cfg.ComponentName, name)
		}
	}
}
