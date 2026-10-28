package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
)

// fakeComponentRegistry is a test double for apihttp.ComponentRegistry.
// Resolve is embedded from validate.StaticRegistry — the same production
// resolution code the real registry.View delegates to — so these tests
// exercise real Resolve semantics, not a hand-rolled stand-in. List returns
// the definitions in the order given, for deterministic JSON assertions.
type fakeComponentRegistry struct {
	validate.StaticRegistry
	defs []datupletv1.ComponentDefinition
}

func newFakeComponentRegistry(defs ...datupletv1.ComponentDefinition) *fakeComponentRegistry {
	static := make(validate.StaticRegistry, len(defs))
	for _, d := range defs {
		static[d.Name] = d
	}
	return &fakeComponentRegistry{StaticRegistry: static, defs: defs}
}

func (f *fakeComponentRegistry) List(_ context.Context) ([]datupletv1.ComponentDefinition, error) {
	return f.defs, nil
}

func componentDefFixture(name string) datupletv1.ComponentDefinition {
	return datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: datupletv1.ComponentDefinitionSpec{
			DisplayName:    "Display " + name,
			Description:    "does things",
			DefaultVersion: "v1.0.0",
			Versions: []datupletv1.VersionSpec{
				{Version: "v1.0.0", Image: "datuplet/" + name + ":v1.0.0", ConfigSchema: `{"type":"object"}`},
				{Version: "dev", Image: "datuplet/" + name + ":dev", Prerelease: true},
			},
		},
	}
}

// componentDefFixtureWithIO is componentDefFixture plus an explicit io
// declaration (RFC 027 §4.3).
func componentDefFixtureWithIO(name string, io *datupletv1.ComponentIO) datupletv1.ComponentDefinition {
	d := componentDefFixture(name)
	d.Spec.IO = io
	return d
}

func newComponentsServer(reg apihttp.ComponentRegistry) (*httptest.Server, func()) {
	srv := apihttp.NewServer(nil).
		WithUserResolver(stubResolver{}).
		WithRegistry(reg)
	ts := httptest.NewServer(srv.Handler())
	return ts, ts.Close
}

func TestHandleListComponents_ReturnsCatalog(t *testing.T) {
	ts, cleanup := newComponentsServer(newFakeComponentRegistry(componentDefFixture("http-fetch")))
	defer cleanup()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/components")
	if err != nil {
		t.Fatalf("GET /api/v1/components: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body []struct {
		Name           string `json:"name"`
		DisplayName    string `json:"displayName"`
		Description    string `json:"description"`
		Deprecated     bool   `json:"deprecated"`
		DefaultVersion string `json:"defaultVersion"`
		Versions       []struct {
			Version      string `json:"version"`
			Prerelease   bool   `json:"prerelease"`
			Image        string `json:"image"`
			ConfigSchema string `json:"configSchema"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("len(body) = %d, want 1", len(body))
	}
	c := body[0]
	if c.Name != "http-fetch" || c.DisplayName != "Display http-fetch" || c.DefaultVersion != "v1.0.0" {
		t.Errorf("unexpected summary: %+v", c)
	}
	if len(c.Versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(c.Versions))
	}
	// The catalog list must NOT leak configSchema — that's detail-only.
	for _, v := range c.Versions {
		if v.ConfigSchema != "" {
			t.Errorf("list response leaked configSchema for version %q", v.Version)
		}
	}
}

func TestHandleListComponents_Unauthenticated(t *testing.T) {
	// selectiveResolver{allow: false} (shared with run_handlers_test.go) is
	// wired but rejects every request — this proves the route is registered
	// AND the auth.WithUser gate runs, unlike TestHandleListComponents_NotConfigured
	// below where the route is unregistered entirely (no resolver at all).
	srv := apihttp.NewServer(nil).
		WithUserResolver(&selectiveResolver{allow: false}).
		WithRegistry(newFakeComponentRegistry())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/components")
	if err != nil {
		t.Fatalf("GET /api/v1/components: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (no resolver wired)", resp.StatusCode)
	}
}

func TestHandleListComponents_NotConfigured(t *testing.T) {
	srv := apihttp.NewServer(nil).WithUserResolver(stubResolver{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/components")
	if err != nil {
		t.Fatalf("GET /api/v1/components: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusNotFound {
		t.Errorf("status = %d, want 404 (no registry wired -> route unregistered)", resp.StatusCode)
	}
}

func TestHandleGetComponent_Detail(t *testing.T) {
	ts, cleanup := newComponentsServer(newFakeComponentRegistry(componentDefFixture("http-fetch")))
	defer cleanup()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/components/http-fetch")
	if err != nil {
		t.Fatalf("GET /api/v1/components/http-fetch: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body struct {
		Name     string `json:"name"`
		Versions []struct {
			Version      string `json:"version"`
			ConfigSchema string `json:"configSchema"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Name != "http-fetch" {
		t.Fatalf("name = %q, want http-fetch", body.Name)
	}
	var gotSchema bool
	for _, v := range body.Versions {
		if v.Version == "v1.0.0" {
			if v.ConfigSchema != `{"type":"object"}` {
				t.Errorf("configSchema = %q, want the raw schema string", v.ConfigSchema)
			}
			gotSchema = true
		}
	}
	if !gotSchema {
		t.Fatalf("v1.0.0 not found in versions: %+v", body.Versions)
	}
}

// TestComponentsIO covers the io declaration on the list and detail
// responses: a component without one defaults to optional/optional, an
// explicit declaration is passed through.
func TestComponentsIO(t *testing.T) {
	type ioBody struct {
		Name string `json:"name"`
		IO   struct {
			Inputs  string `json:"inputs"`
			Outputs string `json:"outputs"`
		} `json:"io"`
	}
	explicit := componentDefFixtureWithIO("data-generator", &datupletv1.ComponentIO{Inputs: "none", Outputs: "required"})
	tests := []struct {
		name        string
		def         datupletv1.ComponentDefinition
		path        string
		list        bool // the path returns an array of components
		wantInputs  string
		wantOutputs string
	}{
		{"list defaults to optional without io declaration", componentDefFixture("http-fetch"), "/api/v1/components", true, "optional", "optional"},
		{"list passes explicit io through", explicit, "/api/v1/components", true, "none", "required"},
		{"get passes explicit io through", explicit, "/api/v1/components/data-generator", false, "none", "required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts, cleanup := newComponentsServer(newFakeComponentRegistry(tc.def))
			defer cleanup()

			resp, err := stdhttp.Get(ts.URL + tc.path)
			if err != nil {
				t.Fatalf("GET %s: %v", tc.path, err)
			}
			defer resp.Body.Close()

			var got ioBody
			if tc.list {
				var body []ioBody
				if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if len(body) != 1 {
					t.Fatalf("len(body) = %d, want 1", len(body))
				}
				got = body[0]
			} else if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.IO.Inputs != tc.wantInputs || got.IO.Outputs != tc.wantOutputs {
				t.Errorf("io = %+v, want %s/%s", got.IO, tc.wantInputs, tc.wantOutputs)
			}
		})
	}
}

func TestHandleGetComponent_NotFound(t *testing.T) {
	ts, cleanup := newComponentsServer(newFakeComponentRegistry(componentDefFixture("http-fetch")))
	defer cleanup()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/components/does-not-exist")
	if err != nil {
		t.Fatalf("GET /api/v1/components/does-not-exist: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}
