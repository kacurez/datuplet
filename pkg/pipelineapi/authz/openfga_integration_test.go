//go:build integration

package authz

// Integration tests for OpenFGAAuthorizer against a real OpenFGA container.
//
// Run with:
//   go test -v -tags=integration ./pkg/pipelineapi/authz/... -run TestOpenFGA
//
// Or via Make:
//   make test-authz-integration
//
// These tests require Docker. They are excluded from the default
// `go test ./...` run by the `integration` build tag.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	openfgaImage = "openfga/openfga:v1.15.0"
	openfgaPort  = "8080/tcp"
)

// startOpenFGA starts an openfga container and returns its base HTTP URL.
// The container is registered for cleanup via t.Cleanup.
func startOpenFGA(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        openfgaImage,
			ExposedPorts: []string{openfgaPort},
			Cmd:          []string{"run"},
			WaitingFor:   wait.ForHTTP("/healthz").WithPort(openfgaPort),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start openfga container: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(ctx); err != nil {
			t.Logf("warn: terminate openfga container: %v", err)
		}
	})

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("get container host: %v", err)
	}
	port, err := c.MappedPort(ctx, openfgaPort)
	if err != nil {
		t.Fatalf("get mapped port: %v", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// createStore creates an OpenFGA store and returns its ID.
func createStore(t *testing.T, baseURL string) string {
	t.Helper()
	body := `{"name":"test-store"}`
	resp, err := http.Post(baseURL+"/stores", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatalf("parse create store response: %v", err)
	}
	id, ok := result["id"].(string)
	if !ok {
		t.Fatalf("create store response missing id: %s", b)
	}
	return id
}

// uploadMinimalModel uploads a minimal OpenFGA authorization model and returns
// its model ID. Uses a simplified model (user + document:reader) sufficient
// for round-trip tests without importing the full collaboration-4.3 DSL parser.
func uploadMinimalModel(t *testing.T, baseURL, storeID string) string {
	t.Helper()
	// Minimal model: user type + project type with viewer and editor relations.
	modelJSON := `{
		"schema_version": "1.1",
		"type_definitions": [
			{"type": "user"},
			{
				"type": "project",
				"relations": {
					"viewer": {
						"this": {}
					},
					"editor": {
						"union": {
							"child": [
								{"this": {}},
								{"computedUserset": {"relation": "viewer"}}
							]
						}
					}
				},
				"metadata": {
					"relations": {
						"viewer": {
							"directly_related_user_types": [{"type": "user"}]
						},
						"editor": {
							"directly_related_user_types": [{"type": "user"}]
						}
					}
				}
			}
		]
	}`
	resp, err := http.Post(
		fmt.Sprintf("%s/stores/%s/authorization-models", baseURL, storeID),
		"application/json",
		strings.NewReader(modelJSON),
	)
	if err != nil {
		t.Fatalf("upload model: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatalf("parse upload model response: %v", err)
	}
	id, ok := result["authorization_model_id"].(string)
	if !ok {
		t.Fatalf("upload model response missing authorization_model_id: %s", b)
	}
	return id
}

// newTestAuthorizer creates an OpenFGAAuthorizer pointed at the test server.
func newTestAuthorizer(t *testing.T, baseURL, storeID, modelID string) *OpenFGAAuthorizer {
	t.Helper()
	a, err := NewOpenFGAAuthorizer(baseURL, storeID, modelID, "", 10*time.Second)
	if err != nil {
		t.Fatalf("NewOpenFGAAuthorizer: %v", err)
	}
	return a
}

// TestOpenFGA_RoundTrip verifies: Write → Check (allowed) → Delete → Check (denied).
func TestOpenFGA_RoundTrip(t *testing.T) {
	baseURL := startOpenFGA(t)
	storeID := createStore(t, baseURL)
	modelID := uploadMinimalModel(t, baseURL, storeID)
	authzr := newTestAuthorizer(t, baseURL, storeID, modelID)
	ctx := context.Background()

	proj := ProjectObject("proj-roundtrip")
	user := UserObject("alice")

	// Write the tuple.
	if err := authzr.WriteTuples(ctx, []Tuple{{
		User:     user.String(),
		Relation: "viewer",
		Object:   proj,
	}}); err != nil {
		t.Fatalf("WriteTuples: %v", err)
	}

	// Check should be allowed.
	allowed, err := authzr.Check(ctx, user.String(), "viewer", proj)
	if err != nil {
		t.Fatalf("Check after Write: %v", err)
	}
	if !allowed {
		t.Error("Check after Write: expected allowed=true, got false")
	}

	// Delete the tuple.
	if err := authzr.DeleteTuples(ctx, []Tuple{{
		User:     user.String(),
		Relation: "viewer",
		Object:   proj,
	}}); err != nil {
		t.Fatalf("DeleteTuples: %v", err)
	}

	// Check should now be denied.
	allowed, err = authzr.Check(ctx, user.String(), "viewer", proj)
	if err != nil {
		t.Fatalf("Check after Delete: %v", err)
	}
	if allowed {
		t.Error("Check after Delete: expected allowed=false, got true")
	}
}

// TestOpenFGA_Timeout verifies that a context deadline exceeded returns
// ErrAuthzUnavailable, not false.
func TestOpenFGA_Timeout(t *testing.T) {
	baseURL := startOpenFGA(t)
	storeID := createStore(t, baseURL)
	modelID := uploadMinimalModel(t, baseURL, storeID)

	// Use a 1ns deadline — will always expire before the network call completes.
	a, err := NewOpenFGAAuthorizer(baseURL, storeID, modelID, "", 1*time.Nanosecond)
	if err != nil {
		t.Fatalf("NewOpenFGAAuthorizer: %v", err)
	}

	_, err = a.Check(context.Background(), "user:oidc~alice", "viewer", ProjectObject("proj-timeout"))
	if err == nil {
		t.Fatal("expected error from Check with 1ns deadline, got nil")
	}
	if !errors.Is(err, ErrAuthzUnavailable) {
		t.Errorf("expected ErrAuthzUnavailable, got: %v", err)
	}
}

// TestOpenFGA_ListObjects verifies that ListObjects returns objects for which
// a user has a specific relation.
func TestOpenFGA_ListObjects(t *testing.T) {
	baseURL := startOpenFGA(t)
	storeID := createStore(t, baseURL)
	modelID := uploadMinimalModel(t, baseURL, storeID)
	authzr := newTestAuthorizer(t, baseURL, storeID, modelID)
	ctx := context.Background()

	user := UserObject("bob")
	projA := ProjectObject("proj-list-a")
	projB := ProjectObject("proj-list-b")
	projC := ProjectObject("proj-list-c")

	// Grant bob viewer on A and B (not C).
	if err := authzr.WriteTuples(ctx, []Tuple{
		{User: user.String(), Relation: "viewer", Object: projA},
		{User: user.String(), Relation: "viewer", Object: projB},
	}); err != nil {
		t.Fatalf("WriteTuples: %v", err)
	}
	_ = projC // intentionally not granted

	objs, err := authzr.ListObjects(ctx, user.String(), "viewer", TypeProject)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}

	objSet := make(map[string]bool)
	for _, o := range objs {
		objSet[o.String()] = true
	}
	if !objSet[projA.String()] {
		t.Errorf("ListObjects missing %s", projA)
	}
	if !objSet[projB.String()] {
		t.Errorf("ListObjects missing %s", projB)
	}
	if objSet[projC.String()] {
		t.Errorf("ListObjects should not include %s (no grant)", projC)
	}
}

// TestOpenFGA_BatchCheck verifies that BatchCheck returns correct results for
// a mix of allowed and denied queries.
func TestOpenFGA_BatchCheck(t *testing.T) {
	baseURL := startOpenFGA(t)
	storeID := createStore(t, baseURL)
	modelID := uploadMinimalModel(t, baseURL, storeID)
	authzr := newTestAuthorizer(t, baseURL, storeID, modelID)
	ctx := context.Background()

	user := UserObject("carol")
	projAllowed := ProjectObject("proj-batch-allowed")
	projDenied := ProjectObject("proj-batch-denied")

	if err := authzr.WriteTuples(ctx, []Tuple{{
		User:     user.String(),
		Relation: "viewer",
		Object:   projAllowed,
	}}); err != nil {
		t.Fatalf("WriteTuples: %v", err)
	}

	queries := []CheckQuery{
		{User: user.String(), Relation: "viewer", Object: projAllowed},
		{User: user.String(), Relation: "viewer", Object: projDenied},
	}
	results, errs := authzr.BatchCheck(ctx, queries)

	if errs[0] != nil {
		t.Errorf("BatchCheck[0] unexpected error: %v", errs[0])
	}
	if !results[0] {
		t.Error("BatchCheck[0]: expected allowed=true for granted tuple")
	}
	if errs[1] != nil {
		t.Errorf("BatchCheck[1] unexpected error: %v", errs[1])
	}
	if results[1] {
		t.Error("BatchCheck[1]: expected allowed=false for non-granted tuple")
	}
}
