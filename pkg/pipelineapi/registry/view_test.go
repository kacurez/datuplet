package registry

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

// schemaWithSecret mirrors pkg/pipeline/validate/schema_test.go's fixture: an
// apiKey field annotated x-datuplet-secret. Used to prove that View.Resolve
// delegates to validate.StaticRegistry (which populates the unexported
// rawConfigSchema) rather than hand-constructing a ResolvedComponent — a
// hand-built ResolvedComponent would silently skip the x-datuplet-secret walk
// in ValidateConfig because rawConfigSchema would be empty.
const schemaWithSecret = `{
  "type": "object",
  "properties": {
    "apiKey": { "type": "string", "x-datuplet-secret": true }
  }
}`

func newFakeClient(objs ...client.Object) client.Client {
	return fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(objs...).Build()
}

func componentDef(name, version, configSchema string) *datupletv1.ComponentDefinition {
	return &datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: datupletv1.ComponentDefinitionSpec{
			DefaultVersion: version,
			Versions: []datupletv1.VersionSpec{
				{Version: version, Image: "datuplet/" + name + ":" + version, ConfigSchema: configSchema},
			},
		},
	}
}

func TestView_Resolve_UnknownComponent(t *testing.T) {
	v := NewView(newFakeClient(), time.Minute)
	rc, findings := v.Resolve("does-not-exist", "")
	if rc != nil {
		t.Fatalf("expected nil ResolvedComponent, got %+v", rc)
	}
	if len(findings) != 1 || findings[0].Message == "" {
		t.Fatalf("expected a single finding, got %+v", findings)
	}
}

func TestView_Resolve_KnownComponent(t *testing.T) {
	v := NewView(newFakeClient(componentDef("http-fetch", "v1.0.0", "")), time.Minute)
	rc, findings := v.Resolve("http-fetch", "")
	if len(findings) != 0 {
		t.Fatalf("unexpected findings: %+v", findings)
	}
	if rc == nil || rc.Version != "v1.0.0" || rc.Image != "datuplet/http-fetch:v1.0.0" {
		t.Fatalf("unexpected ResolvedComponent: %+v", rc)
	}
}

// TestView_Resolve_DelegatesToStaticRegistry_SecretRefSchema is the critical
// regression test from the R9 handoff: View.Resolve MUST delegate to
// validate.StaticRegistry so the x-datuplet-secret config-schema check still
// fires. A hand-constructed ResolvedComponent would leave rawConfigSchema
// empty and this check would silently no-op.
func TestView_Resolve_DelegatesToStaticRegistry_SecretRefSchema(t *testing.T) {
	v := NewView(newFakeClient(componentDef("http-fetch", "v1.0.0", schemaWithSecret)), time.Minute)

	pipelineYAML := []byte(`apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: etl
spec:
  stages:
    - name: extract
      components:
        - name: c1
          component: http-fetch
          version: v1.0.0
          config:
            apiKey: "plaintext-not-a-ref"
          outputs:
            defaultBucket: raw
            defaultWriteMode: APPEND
`)
	_, findings, err := validate.ValidatePipeline(pipelineYAML, v)
	if err != nil {
		t.Fatalf("ValidatePipeline: %v", err)
	}
	var found bool
	for _, f := range findings {
		if f.Path == "stages[0].components[0].config.apiKey" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a finding at stages[0].components[0].config.apiKey (x-datuplet-secret unmet), got %+v", findings)
	}
}

// TestView_List_ReturnsComponentDefinitions asserts List surfaces the raw CRs
// (name, spec) the catalog handlers serialize.
func TestView_List_ReturnsComponentDefinitions(t *testing.T) {
	v := NewView(newFakeClient(
		componentDef("http-fetch", "v1.0.0", ""),
		componentDef("sql-transform", "v2.0.0", ""),
	), time.Minute)

	defs, err := v.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("List returned %d items, want 2", len(defs))
	}
}

// TestView_TTLCache asserts List/Resolve share a snapshot refreshed at most
// once per ttl: two calls within the ttl window hit the k8s client once;
// a call after the ttl elapses triggers a second List.
func TestView_TTLCache(t *testing.T) {
	var listCalls int
	base := newFakeClient(componentDef("http-fetch", "v1.0.0", ""))
	counting := countingListClient{Client: base, calls: &listCalls}

	v := NewView(counting, 10*time.Second)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	v.now = func() time.Time { return now }

	if _, err := v.List(context.Background()); err != nil {
		t.Fatalf("List #1: %v", err)
	}
	if _, err := v.List(context.Background()); err != nil {
		t.Fatalf("List #2: %v", err)
	}
	if listCalls != 1 {
		t.Fatalf("listCalls = %d after two calls within ttl, want 1", listCalls)
	}

	now = now.Add(11 * time.Second)
	if _, err := v.List(context.Background()); err != nil {
		t.Fatalf("List #3: %v", err)
	}
	if listCalls != 2 {
		t.Fatalf("listCalls = %d after ttl elapsed, want 2", listCalls)
	}
}

// countingListClient wraps a client.Client, counting calls to List.
type countingListClient struct {
	client.Client
	calls *int
}

func (c countingListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	*c.calls++
	return c.Client.List(ctx, list, opts...)
}
