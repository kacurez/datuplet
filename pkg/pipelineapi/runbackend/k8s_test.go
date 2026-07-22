package runbackend_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/runbackend"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

func TestK8sBackendTriggerRunReturnsRunIDAndNamespace(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()

	projectID := uuid.New()
	expectedNS := "datuplet-" + projectID.String()

	be := runbackend.NewK8sBackend(runbackend.K8sOpts{
		Client:      c,
		RunInserter: stubInserter{},
		ProjectNS:   stubProjectNS{},
		Minter:      stubMinter{},
		Audience:    "test-aud",
	})

	// Use a pipeline doc with explicit table refs so
	// TableCapabilitiesFromPipeline produces at least one cap and the
	// Minter is exercised. Bucket-level defaults don't generate per-table
	// grants — only explicit table outputs do.
	parsed, err := config.Parse(minimalPipelineDoc())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	resp, err := be.TriggerRun(context.Background(), runbackend.TriggerRequest{
		ProjectID:    projectID,
		PipelineName: "p",
		Doc:          parsed,
	})
	if err != nil {
		t.Fatalf("TriggerRun: %v", err)
	}
	if resp.RunID == uuid.Nil {
		t.Error("RunID is zero")
	}
	if resp.Namespace == "" {
		t.Error("Namespace is empty")
	}
	if resp.Namespace != expectedNS {
		t.Errorf("Namespace = %q, want %q", resp.Namespace, expectedNS)
	}
}

// TestK8sBackendTriggerRunAppliesDocAsCR asserts TriggerRun renders the
// TriggerRequest.Doc into a Pipeline CR via config.DocToCR: the applied
// object's ObjectMeta.Name and Spec match DocToCR(doc), living in the
// project namespace.
func TestK8sBackendTriggerRunAppliesDocAsCR(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	projectID := uuid.New()
	expectedNS := "datuplet-" + projectID.String()

	be := runbackend.NewK8sBackend(runbackend.K8sOpts{
		Client:      c,
		RunInserter: stubInserter{},
		ProjectNS:   stubProjectNS{},
		Minter:      stubMinter{},
		Audience:    "test-aud",
	})

	parsed, err := config.Parse(minimalPipelineDoc())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := be.TriggerRun(context.Background(), runbackend.TriggerRequest{
		ProjectID:    projectID,
		PipelineName: "p",
		Doc:          parsed,
	}); err != nil {
		t.Fatalf("TriggerRun: %v", err)
	}

	got := &datupletv1.Pipeline{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "p", Namespace: expectedNS}, got); err != nil {
		t.Fatalf("Get Pipeline CR: %v", err)
	}
	want := config.DocToCR(parsed)
	if got.Name != want.Name {
		t.Errorf("CR name = %q, want %q", got.Name, want.Name)
	}
	if got.Namespace != expectedNS {
		t.Errorf("CR namespace = %q, want %q", got.Namespace, expectedNS)
	}
	if !reflect.DeepEqual(got.Spec, want.Spec) {
		t.Errorf("CR spec = %+v, want %+v", got.Spec, want.Spec)
	}
}

// TestK8sBackend_WarehouseResolver_PopulatesSpec asserts that TriggerRun
// passes the warehouse returned by WarehouseResolver into the RunSpec that
// reaches the Minter.
func TestK8sBackend_WarehouseResolver_PopulatesSpec(t *testing.T) {
	const wantWarehouse = "lakekeeper-wh-xyz"

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	captureMinter := &capturingMinter{}

	be := runbackend.NewK8sBackend(runbackend.K8sOpts{
		Client:      c,
		RunInserter: stubInserter{},
		ProjectNS:   stubProjectNS{},
		Minter:      captureMinter,
		Audience:    "test-aud",
		WarehouseResolver: func(_ context.Context, _ string) (string, error) {
			return wantWarehouse, nil
		},
	})

	parsed, err := config.Parse(minimalPipelineDoc())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = be.TriggerRun(context.Background(), runbackend.TriggerRequest{
		ProjectID:    uuid.New(),
		PipelineName: "p",
		Doc:          parsed,
	})
	if err != nil {
		t.Fatalf("TriggerRun: %v", err)
	}
	if captureMinter.lastSpec.Warehouse != wantWarehouse {
		t.Errorf("RunSpec.Warehouse = %q, want %q", captureMinter.lastSpec.Warehouse, wantWarehouse)
	}
}

// TestK8sBackend_WarehouseResolver_ErrorPropagates asserts that when
// WarehouseResolver returns an error, TriggerRun surfaces it wrapped.
func TestK8sBackend_WarehouseResolver_ErrorPropagates(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()

	be := runbackend.NewK8sBackend(runbackend.K8sOpts{
		Client:      c,
		RunInserter: stubInserter{},
		ProjectNS:   stubProjectNS{},
		Minter:      stubMinter{},
		Audience:    "test-aud",
		WarehouseResolver: func(_ context.Context, _ string) (string, error) {
			return "", fmt.Errorf("lakekeeper unavailable")
		},
	})

	parsed, err := config.Parse(minimalPipelineDoc())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = be.TriggerRun(context.Background(), runbackend.TriggerRequest{
		ProjectID:    uuid.New(),
		PipelineName: "p",
		Doc:          parsed,
	})
	if err == nil {
		t.Fatal("expected error from WarehouseResolver, got nil")
	}
	if !strings.Contains(err.Error(), "lakekeeper unavailable") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "lakekeeper unavailable")
	}
}

// TestK8sBackend_WarehouseResolver_NilDegrades asserts that a nil
// WarehouseResolver still allows TriggerRun to succeed with empty warehouse.
func TestK8sBackend_WarehouseResolver_NilDegrades(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	captureMinter := &capturingMinter{}

	be := runbackend.NewK8sBackend(runbackend.K8sOpts{
		Client:            c,
		RunInserter:       stubInserter{},
		ProjectNS:         stubProjectNS{},
		Minter:            captureMinter,
		Audience:          "test-aud",
		WarehouseResolver: nil, // intentionally nil
	})

	parsed, err := config.Parse(minimalPipelineDoc())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = be.TriggerRun(context.Background(), runbackend.TriggerRequest{
		ProjectID:    uuid.New(),
		PipelineName: "p",
		Doc:          parsed,
	})
	if err != nil {
		t.Fatalf("TriggerRun with nil resolver: %v", err)
	}
	if captureMinter.lastSpec.Warehouse != "" {
		t.Errorf("RunSpec.Warehouse = %q, want empty (soft-degrade)", captureMinter.lastSpec.Warehouse)
	}
}

// minimalPipelineDoc is the envelope-free doc-shape (RFC 027 §3) fed to
// config.Parse to derive TriggerRequest.Doc. It declares one output table so
// TableCapabilitiesFromPipeline produces a cap and the Minter is exercised.
func minimalPipelineDoc() []byte {
	return []byte(`{
  "name": "p",
  "stages": [
    {
      "name": "extract",
      "components": [
        {
          "name": "c1",
          "component": "datuplet/test:latest",
          "outputs": {"tables": [{"bucket": "events", "name": "users", "writeMode": "APPEND"}]}
        }
      ]
    }
  ]
}`)
}

// --- stubs ---

type stubInserter struct{}

func (stubInserter) Insert(_ context.Context, opts store.CreateRunOpts) (*store.Run, error) {
	return &store.Run{ID: opts.ID, ProjectID: opts.ProjectID, PipelineID: opts.PipelineID, Phase: "Pending"}, nil
}
func (stubInserter) MarkFailed(_ context.Context, _ uuid.UUID, _ error) {}

type stubProjectNS struct{}

func (stubProjectNS) Ensure(_ context.Context, id uuid.UUID) (string, error) {
	return "datuplet-" + id.String(), nil
}

type stubMinter struct{}

func (stubMinter) MintRun(_ context.Context, _ tokens.RunSpec) (string, error) {
	return "test-token", nil
}

// capturingMinter records the last RunSpec it received so tests can assert
// on the fields (e.g. Warehouse) that TriggerRun passes to the Minter.
type capturingMinter struct {
	lastSpec tokens.RunSpec
}

func (m *capturingMinter) MintRun(_ context.Context, spec tokens.RunSpec) (string, error) {
	m.lastSpec = spec
	return "test-token", nil
}
