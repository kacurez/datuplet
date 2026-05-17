package http_test

import (
	"bytes"
	"context"
	stdhttp "net/http"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

const validPipelineYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: etl
spec:
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/test:latest
          outputs:
            defaultBucket: raw
            defaultWriteMode: APPEND
`

func putYAML(t *testing.T, url string, yaml []byte, cookie *stdhttp.Cookie) *stdhttp.Response {
	t.Helper()
	req, _ := stdhttp.NewRequest("PUT", url, bytes.NewReader(yaml))
	req.Header.Set("Content-Type", "application/yaml")
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	return resp
}

func TestPutPipeline_ValidYAML(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-put-valid"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", []byte(validPipelineYAML), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}

	got, err := store.GetPipelineByName(ctx, pool, proj.ID, "etl")
	if err != nil {
		t.Fatalf("GetPipelineByName: %v", err)
	}
	if got.YAML != validPipelineYAML {
		t.Error("YAML not persisted")
	}
}

func TestPutPipeline_InvalidYAML(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-put-invalid"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/broken", []byte("not: [valid yaml"), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestListPipelines(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-list-pipelines"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "describe", authz.ProjectObject(lkID))
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "p1", validPipelineYAML)
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "p2", validPipelineYAML)

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestGetPipeline_NotMember(t *testing.T) {
	ts, pool, _, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, _ := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	// Set a lakekeeper_project_id so we pass the empty-lkID guard,
	// but do NOT seed any FGA tuple for alice — mustHaveRelation returns 403.
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, "lk-not-member"); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "secret", validPipelineYAML)

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/secret", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Errorf("status = %d, want 403 (no FGA tuple for alice)", resp.StatusCode)
	}
}

func TestDeletePipeline(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-delete-pipeline"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", validPipelineYAML)

	req, _ := stdhttp.NewRequest("DELETE", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}
}

func TestPutPipeline_NameMismatch(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-name-mismatch"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	// YAML says metadata.name: "actual"; URL says "requested". Must 400.
	yaml := `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: actual
spec:
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/test:latest
          outputs:
            defaultBucket: raw
            defaultWriteMode: APPEND
`
	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/requested", []byte(yaml), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400 (YAML name vs URL name mismatch)", resp.StatusCode)
	}
}
