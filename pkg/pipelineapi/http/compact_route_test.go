package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/storage"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

func sysReq(t *testing.T, method, url string, body string, cookie *stdhttp.Cookie) (int, map[string]any) {
	t.Helper()
	req, _ := stdhttp.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestPipelines_SystemHidden(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithK8s(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", "", []byte(validPipelineYAML))
	sysName := storage.SystemCompactPipelineName("raw", "events")
	if _, err := store.UpsertSystemPipeline(ctx, pool, proj.ID, sysName, "Compaction of raw.events", []byte(validPipelineYAML)); err != nil {
		t.Fatalf("seed system pipeline: %v", err)
	}
	base := ts.URL + "/api/v1/projects/" + proj.ID.String() + "/pipelines"

	req, _ := stdhttp.NewRequest("GET", base, nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var list []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if len(list) != 1 || list[0]["name"] != "etl" {
		t.Errorf("GET /pipelines = %v, want only etl", list)
	}

	if code, _ := sysReq(t, "GET", base+"/"+sysName, "", cookie); code != 200 {
		t.Errorf("GET system pipeline: %d, want 200", code)
	}
	if code, body := sysReq(t, "PUT", base+"/sys-x", validPipelineYAML, cookie); code != 400 || body["error"] != "reserved_name" {
		t.Errorf("PUT sys-x: %d %v, want 400 reserved_name", code, body)
	}
	if code, body := sysReq(t, "DELETE", base+"/"+sysName, "", cookie); code != 409 || body["error"] != "system_pipeline" {
		t.Errorf("DELETE system pipeline: %d %v, want 409 system_pipeline", code, body)
	}
	if code, body := sysReq(t, "POST", base+"/"+sysName+"/runs", "{}", cookie); code != 409 || body["error"] != "system_pipeline" {
		t.Errorf("trigger system pipeline: %d %v, want 409 system_pipeline", code, body)
	}
}

func fakeLakekeeperForEvents(t *testing.T) string {
	t.Helper()
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("/v1/config", func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	})
	mux.HandleFunc("/v1/namespaces/raw/tables/events", func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata-location":"s3://datuplet/m.json","metadata":{"format-version":1,
			"table-uuid":"00000000-0000-0000-0000-000000000001","location":"s3://datuplet/t","last-updated-ms":1700000000000,
			"last-column-id":1,"schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]},
			"current-schema-id":0,"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}],
			"partition-spec":[],"default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],"default-sort-order-id":0,
			"sort-orders":[{"order-id":0,"fields":[]}],"properties":{},"current-snapshot-id":-1,"snapshots":[],"refs":{}},"config":{}}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestCompactTable_RunCancellable(t *testing.T) {
	schema, err := os.ReadFile("../../../components/iceberg-maintenance/schema.json")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	reg := newFakeComponentRegistry(datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "iceberg-maintenance"},
		Spec: datupletv1.ComponentDefinitionSpec{
			DefaultVersion: "v1.0.0",
			Versions:       []datupletv1.VersionSpec{{Version: "v1.0.0", Image: "x/iceberg-maintenance:v1.0.0", ConfigSchema: string(schema)}},
			IO:             &datupletv1.ComponentIO{Inputs: "none", Outputs: "required"},
		},
	})
	lkURL := fakeLakekeeperForEvents(t)
	ts, pool, fakeAuthz, cleanup := freshServerWithK8sClient(t, fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build(),
		func(s *apihttp.Server, pool *pgxpool.Pool, authzr *authztest.Fake) *apihttp.Server {
			return s.WithRegistry(reg).
				WithStorage(&storage.Service{
					LakekeeperURL: lkURL,
					Minter:        func(context.Context) (tokens.ImpersonationToken, error) { return "tok", nil },
				}).
				WithProjectGate(&projectgate.Gate{
					LakekeeperProjectIDFor: func(ctx context.Context, id uuid.UUID) (string, error) {
						p, err := store.GetProjectByID(ctx, pool, id)
						if err != nil {
							return "", err
						}
						return p.LakekeeperProjectID, nil
					},
					Authorizer:   authzr,
					WarehouseFor: func(context.Context, string) (string, error) { return "datuplet", nil },
				})
		})
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	seedProjectAuthz(t, pool, fakeAuthz, alice.ID, proj.ID, "editor")

	code, body := sysReq(t, "POST", ts.URL+"/api/v1/storage/projects/"+proj.ID.String()+"/tables/raw/events/compact", "", cookie)
	if code != 202 {
		t.Fatalf("compact: %d %v, want 202", code, body)
	}
	runID, _ := body["run_id"].(string)

	code, run := sysReq(t, "GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs/"+runID, "", cookie)
	if code != 200 || run["pipeline_name"] != storage.SystemCompactPipelineName("raw", "events") || run["pipeline_description"] != "Compaction of raw.events" {
		t.Fatalf("get run: %d %v", code, run)
	}
	code, page := sysReq(t, "GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs", "", cookie)
	runs, _ := page["runs"].([]any)
	if code != 200 || len(runs) != 1 || runs[0].(map[string]any)["id"] != runID {
		t.Fatalf("list runs: %d %v", code, page)
	}
	if code, _ := sysReq(t, "POST", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/runs/"+runID+"/cancel", "{}", cookie); code != 202 {
		t.Fatalf("cancel: %d, want 202", code)
	}
	r, _ := store.GetRunByID(ctx, pool, uuid.MustParse(runID))
	if r.Phase != "Cancelled" {
		t.Errorf("phase = %q, want Cancelled", r.Phase)
	}
}
