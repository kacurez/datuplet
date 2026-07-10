package projectgate_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate/projectgatetest"
)

func testGate(allow bool, checkErr error) *projectgate.Gate {
	return &projectgate.Gate{
		LakekeeperProjectIDFor: func(_ context.Context, _ uuid.UUID) (string, error) {
			return "lk-proj-1", nil
		},
		Authorizer: projectgatetest.FakeAuthorizer{Allow: allow, Err: checkErr},
		WarehouseFor: func(_ context.Context, lkPID string) (string, error) {
			if lkPID != "lk-proj-1" {
				return "", errors.New("wrong project")
			}
			return "wh1", nil
		},
	}
}

func TestQualifiedWarehouse_HappyPath(t *testing.T) {
	wh, gerr := testGate(true, nil).QualifiedWarehouse(context.Background(), uuid.NewString(), uuid.NewString())
	if gerr != nil {
		t.Fatalf("gerr = %+v, want nil", gerr)
	}
	if wh != "lk-proj-1/wh1" {
		t.Fatalf("warehouse = %q, want %q", wh, "lk-proj-1/wh1")
	}
}

func TestAuthorize_ReturnsBothIDs(t *testing.T) {
	pidStr := uuid.NewString()
	pid, lkPID, gerr := testGate(true, nil).Authorize(context.Background(), uuid.NewString(), pidStr)
	if gerr != nil {
		t.Fatalf("gerr = %+v", gerr)
	}
	if pid.String() != pidStr || lkPID != "lk-proj-1" {
		t.Fatalf("got (%s, %s), want (%s, lk-proj-1)", pid, lkPID, pidStr)
	}
}

func TestAuthorize_InvalidPID(t *testing.T) {
	_, _, gerr := testGate(true, nil).Authorize(context.Background(), uuid.NewString(), "../../etc")
	if gerr == nil || gerr.Status != http.StatusBadRequest || gerr.Kind != "bad_request" {
		t.Fatalf("gerr = %+v, want 400 bad_request", gerr)
	}
}

func TestAuthorize_Forbidden(t *testing.T) {
	_, _, gerr := testGate(false, nil).Authorize(context.Background(), uuid.NewString(), uuid.NewString())
	if gerr == nil || gerr.Status != http.StatusForbidden || gerr.Kind != "forbidden" {
		t.Fatalf("gerr = %+v, want 403 forbidden", gerr)
	}
}

func TestAuthorize_AuthzUnavailable(t *testing.T) {
	_, _, gerr := testGate(false, authz.ErrAuthzUnavailable).Authorize(context.Background(), uuid.NewString(), uuid.NewString())
	if gerr == nil || gerr.Status != http.StatusServiceUnavailable {
		t.Fatalf("gerr = %+v, want 503", gerr)
	}
}

func TestAuthorize_NoLakekeeperProject(t *testing.T) {
	g := testGate(true, nil)
	g.LakekeeperProjectIDFor = func(_ context.Context, _ uuid.UUID) (string, error) { return "", nil }
	_, _, gerr := g.Authorize(context.Background(), uuid.NewString(), uuid.NewString())
	if gerr == nil || gerr.Status != http.StatusServiceUnavailable {
		t.Fatalf("gerr = %+v, want 503 (project authz not yet provisioned)", gerr)
	}
}

func TestWarehouse_NoneRegistered(t *testing.T) {
	g := testGate(true, nil)
	g.WarehouseFor = func(_ context.Context, _ string) (string, error) { return "", errors.New("none registered") }
	_, gerr := g.Warehouse(context.Background(), "lk-proj-1")
	if gerr == nil || gerr.Status != http.StatusServiceUnavailable || gerr.Kind != "unavailable" {
		t.Fatalf("gerr = %+v, want 503 unavailable", gerr)
	}
}

func TestAuthorize_NilDepsSoftDegrade(t *testing.T) {
	// authzr is nil in authz-disabled deployments (OPENFGA_MODEL_VERSION
	// unset — cmd/pipeline-api/main.go:163/181); the gate must 503, never panic.
	g := testGate(true, nil)
	g.Authorizer = nil
	_, _, gerr := g.Authorize(context.Background(), uuid.NewString(), uuid.NewString())
	if gerr == nil || gerr.Status != http.StatusServiceUnavailable {
		t.Fatalf("gerr = %+v, want 503 when Authorizer is nil", gerr)
	}
}

func TestAuthorize_CheckErrorInternal(t *testing.T) {
	// A non-ErrAuthzUnavailable error from Check maps to 500 internal,
	// distinct from the 503 unavailable path.
	_, _, gerr := testGate(false, errors.New("boom")).Authorize(context.Background(), uuid.NewString(), uuid.NewString())
	if gerr == nil || gerr.Status != http.StatusInternalServerError || gerr.Kind != "internal" {
		t.Fatalf("gerr = %+v, want 500 internal", gerr)
	}
}

func TestWarehouse_NilResolver(t *testing.T) {
	g := testGate(true, nil)
	g.WarehouseFor = nil
	_, gerr := g.Warehouse(context.Background(), "lk-proj-1")
	if gerr == nil || gerr.Status != http.StatusServiceUnavailable || gerr.Kind != "unavailable" {
		t.Fatalf("gerr = %+v, want 503 unavailable when WarehouseFor is nil", gerr)
	}
}
